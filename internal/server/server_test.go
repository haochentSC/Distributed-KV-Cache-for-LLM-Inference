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
