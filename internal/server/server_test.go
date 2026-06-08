package server

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
)

const bufSize = 1 << 20

func testClient(t *testing.T) (kvcachev1.KVCacheClient, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	kvcachev1.RegisterKVCacheServer(gs, New(cache.NewStore(nil)))
	go func() {
		if err := gs.Serve(lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	return kvcachev1.NewKVCacheClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	}
}

func writeBlock(t *testing.T, client kvcachev1.KVCacheClient, model string, hash []byte, tokenIDs []int32, payload []byte) uint64 {
	t.Helper()
	stream, err := client.Write(context.Background())
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	err = stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Header{Header: &kvcachev1.WriteHeader{
		ModelId:   model,
		BlockHash: hash,
		TokenIds:  tokenIDs,
		TotalSize: uint64(len(payload)),
	}}})
	if err != nil {
		t.Fatalf("send header: %v", err)
	}
	if len(payload) > 0 {
		err = stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Chunk{Chunk: &kvcachev1.KVChunk{
			Data: payload,
			Last: true,
		}}})
		if err != nil {
			t.Fatalf("send payload: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	return resp.GetVersion()
}

func fetchAll(t *testing.T, client kvcachev1.KVCacheClient, req *kvcachev1.FetchRequest) ([]byte, error) {
	t.Helper()
	stream, err := client.Fetch(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var out []byte
	seenLast := false
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if !seenLast {
				t.Fatal("Fetch stream ended without last=true")
			}
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, chunk.GetData()...)
		seenLast = chunk.GetLast()
	}
}

func hashBytes(b byte) []byte {
	h := make([]byte, 32)
	h[31] = b
	return h
}

func TestFetchVerifiesModelVersionAndTokens(t *testing.T) {
	client, cleanup := testClient(t)
	defer cleanup()

	hash := hashBytes(1)
	tokens := []int32{1, 2, 3, 4}
	payload := []byte("tensor-bytes")
	version := writeBlock(t, client, "model-a", hash, tokens, payload)

	got, err := fetchAll(t, client, &kvcachev1.FetchRequest{
		ModelId:   "model-a",
		BlockHash: hash,
		Version:   version,
		TokenIds:  tokens,
	})
	if err != nil {
		t.Fatalf("Fetch hit: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}

	cases := []struct {
		name string
		req  *kvcachev1.FetchRequest
	}{
		{
			name: "wrong model",
			req:  &kvcachev1.FetchRequest{ModelId: "model-b", BlockHash: hash, Version: version, TokenIds: tokens},
		},
		{
			name: "wrong version",
			req:  &kvcachev1.FetchRequest{ModelId: "model-a", BlockHash: hash, Version: version + 1, TokenIds: tokens},
		},
		{
			name: "wrong tokens",
			req:  &kvcachev1.FetchRequest{ModelId: "model-a", BlockHash: hash, Version: version, TokenIds: []int32{1, 2, 3, 9}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fetchAll(t, client, tc.req)
			if status.Code(err) != codes.NotFound {
				t.Fatalf("Fetch code = %v, want NotFound (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestFetchRejectsBadHashLength(t *testing.T) {
	client, cleanup := testClient(t)
	defer cleanup()

	_, err := fetchAll(t, client, &kvcachev1.FetchRequest{ModelId: "m", BlockHash: []byte{1, 2, 3}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Fetch code = %v, want InvalidArgument", status.Code(err))
	}
}

// batchBlock is one demuxed block from a BatchFetch stream: its assembled bytes plus the
// per-block found/last terminal flags.
type batchBlock struct {
	data  []byte
	found bool
	last  bool
}

// batchFetchAll drains a BatchFetch stream into a per-index result map, asserting that
// every index it saw terminated with last=true.
func batchFetchAll(t *testing.T, client kvcachev1.KVCacheClient, req *kvcachev1.BatchFetchRequest) map[uint32]*batchBlock {
	t.Helper()
	stream, err := client.BatchFetch(context.Background(), req)
	if err != nil {
		t.Fatalf("BatchFetch: %v", err)
	}
	out := map[uint32]*batchBlock{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("BatchFetch Recv: %v", err)
		}
		b := out[chunk.GetIndex()]
		if b == nil {
			b = &batchBlock{}
			out[chunk.GetIndex()] = b
		}
		b.data = append(b.data, chunk.GetData()...)
		b.found = b.found || chunk.GetFound()
		if chunk.GetLast() {
			b.last = true
		}
	}
	for idx, b := range out {
		if !b.last {
			t.Fatalf("index %d never terminated with last=true", idx)
		}
	}
	return out
}

func TestBatchFetchDemuxesAndIsolatesMisses(t *testing.T) {
	client, cleanup := testClient(t)
	defer cleanup()

	model := "model-a"
	h0, h1, missing := hashBytes(1), hashBytes(2), hashBytes(9)
	tok0, tok1 := []int32{1, 2, 3, 4}, []int32{5, 6, 7, 8}
	pay0, pay1 := []byte("block-zero-bytes"), []byte("block-one-bytes")
	v0 := writeBlock(t, client, model, h0, tok0, pay0)
	writeBlock(t, client, model, h1, tok1, pay1)

	// One request mixing: a hit, another hit, an absent block, a present hash at the wrong
	// version. Order matters — the index tag must map each frame back to its slot.
	got := batchFetchAll(t, client, &kvcachev1.BatchFetchRequest{
		ModelId: model,
		Blocks: []*kvcachev1.FetchBlock{
			{BlockHash: h0, Version: v0, TokenIds: tok0},
			{BlockHash: h1},
			{BlockHash: missing},
			{BlockHash: h0, Version: v0 + 1}, // wrong version -> not found
		},
	})

	cases := []struct {
		idx       uint32
		wantFound bool
		wantData  []byte
	}{
		{0, true, pay0},
		{1, true, pay1},
		{2, false, nil},
		{3, false, nil},
	}
	for _, tc := range cases {
		b, ok := got[tc.idx]
		if !ok {
			t.Fatalf("index %d missing from response", tc.idx)
		}
		if b.found != tc.wantFound {
			t.Fatalf("index %d found = %v, want %v", tc.idx, b.found, tc.wantFound)
		}
		if string(b.data) != string(tc.wantData) {
			t.Fatalf("index %d data = %q, want %q", tc.idx, b.data, tc.wantData)
		}
	}
}

func TestBatchFetchReportsBadHashAsMiss(t *testing.T) {
	client, cleanup := testClient(t)
	defer cleanup()

	// A malformed hash must not fail the whole batch — it comes back as a per-block miss.
	got := batchFetchAll(t, client, &kvcachev1.BatchFetchRequest{
		ModelId: "m",
		Blocks:  []*kvcachev1.FetchBlock{{BlockHash: []byte{1, 2, 3}}},
	})
	b, ok := got[0]
	if !ok || b.found {
		t.Fatalf("bad-hash block: got %+v, want found=false terminal frame", b)
	}
}
