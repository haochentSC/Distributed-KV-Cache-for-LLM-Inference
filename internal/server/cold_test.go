package server

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
)

// fakeCold is an in-test coldReader: it lets a test preload "cold" blocks and serve them on a hot
// miss, without the AWS SDK (that's the point of the narrow coldReader seam).
type fakeCold struct {
	mu   sync.Mutex
	m    map[cache.BlockHash]coldEntry
	gets int
}
type coldEntry struct {
	kv      []byte
	version uint64
	tokens  []int32
}

func newFakeCold() *fakeCold { return &fakeCold{m: make(map[cache.BlockHash]coldEntry)} }
func (f *fakeCold) put(h cache.BlockHash, e coldEntry) {
	f.mu.Lock()
	f.m[h] = e
	f.mu.Unlock()
}
func (f *fakeCold) Get(_ context.Context, _ string, h cache.BlockHash) ([]byte, uint64, []int32, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	e, ok := f.m[h]
	if !ok {
		return nil, 0, nil, false, nil
	}
	return e.kv, e.version, e.tokens, true, nil
}

// clientForServer wires a bufconn gRPC client to an already-built Server (so a test can install a
// cold reader first). Mirrors testClient but takes the Server.
func clientForServer(t *testing.T, srv *Server) (kvcachev1.KVCacheClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	kvcachev1.RegisterKVCacheServer(gs, srv)
	go func() {
		if err := gs.Serve(lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
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

// TestFetch_ColdReadThrough: a hot miss is served from the cold tier with the exact bytes, the
// block is re-admitted to the hot store, and a cold miss is a clean NotFound (ADR 0027).
func TestFetch_ColdReadThrough(t *testing.T) {
	store := cache.NewStore(cache.NewLRU())
	fc := newFakeCold()
	client, cleanup := clientForServer(t, New(store).WithColdReader(fc))
	defer cleanup()

	var h cache.BlockHash
	h[0] = 9
	kv := []byte("cold-tier-bytes")
	fc.put(h, coldEntry{kv: kv, version: 3, tokens: []int32{5, 6}})

	got, err := fetchAll(t, client, &kvcachev1.FetchRequest{ModelId: "m", BlockHash: h[:], TokenIds: []int32{5, 6}})
	if err != nil {
		t.Fatalf("fetch (cold hit): %v", err)
	}
	if !bytes.Equal(got, kv) {
		t.Fatalf("cold read-through bytes mismatch: got %q want %q", got, kv)
	}

	// Re-admitted, so a second Fetch is a hot hit (the cold tier isn't consulted again).
	if _, present := store.Peek("m", h); !present {
		t.Error("block should be re-admitted to the hot store after read-through")
	}
	getsAfterFirst := fc.gets
	if _, err := fetchAll(t, client, &kvcachev1.FetchRequest{ModelId: "m", BlockHash: h[:]}); err != nil {
		t.Fatalf("fetch (now hot): %v", err)
	}
	if fc.gets != getsAfterFirst {
		t.Errorf("second fetch hit the cold tier again (gets %d -> %d); re-admit didn't take", getsAfterFirst, fc.gets)
	}

	// A hash the cold tier doesn't have is a clean NotFound.
	var miss cache.BlockHash
	miss[0] = 99
	if _, err := fetchAll(t, client, &kvcachev1.FetchRequest{ModelId: "m", BlockHash: miss[:]}); status.Code(err) != codes.NotFound {
		t.Fatalf("cold miss: want NotFound, got %v", err)
	}
}

// TestFetch_ColdVersionMismatch: a pinned version that doesn't match the cold copy must NOT be
// served (and must NOT re-admit) — upholding ADR 0016 across the tier boundary.
func TestFetch_ColdVersionMismatch(t *testing.T) {
	store := cache.NewStore(cache.NewLRU())
	fc := newFakeCold()
	client, cleanup := clientForServer(t, New(store).WithColdReader(fc))
	defer cleanup()

	var h cache.BlockHash
	h[0] = 4
	fc.put(h, coldEntry{kv: []byte("v3-bytes"), version: 3, tokens: []int32{1}})

	// Client pins version 2; cold has version 3 -> miss, not a mis-served block.
	_, err := fetchAll(t, client, &kvcachev1.FetchRequest{ModelId: "m", BlockHash: h[:], Version: 2})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("version mismatch: want NotFound, got %v", err)
	}
	if _, present := store.Peek("m", h); present {
		t.Error("a mismatched cold read must not re-admit the block")
	}
}
