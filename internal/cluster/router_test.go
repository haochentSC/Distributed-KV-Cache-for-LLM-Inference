package cluster_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
	"github.com/haochentSC/distributed-kv-cache/internal/server"
)

// key makes a deterministic 32-byte routing key (stand-in for a real block_hash[0]).
func key(i int) []byte {
	h := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
	return h[:]
}

// startShard runs a real in-process cache server on a loopback port and returns its
// Member plus the server handle (so a test can kill it). Stopped on cleanup.
func startShard(t *testing.T, id string) (cluster.Member, *grpc.Server) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	kvcachev1.RegisterKVCacheServer(gs, server.New(cache.NewStore(nil)))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop) // idempotent: safe even if a test already called Stop
	return cluster.Member{ID: id, Addr: lis.Addr().String()}, gs
}

func TestOwnerConnEmptyRing(t *testing.T) {
	r := cluster.New(64)
	if conn, ok := r.OwnerConn(key(0)); ok || conn != nil {
		t.Fatalf("empty ring should resolve no owner, got (%v, %v)", conn, ok)
	}
}

// TestOwnerConnDeterministic: a key always resolves to the same connection, and every
// key resolves to one of the live shards. Repeated calls are stable.
func TestOwnerConnDeterministic(t *testing.T) {
	a, _ := startShard(t, "A")
	b, _ := startShard(t, "B")
	c, _ := startShard(t, "C")
	r := cluster.New(64)
	r.SetMembers([]cluster.Member{a, b, c})
	defer r.Close()

	for i := 0; i < 500; i++ {
		c1, ok1 := r.OwnerConn(key(i))
		c2, ok2 := r.OwnerConn(key(i))
		if !ok1 || !ok2 {
			t.Fatalf("key %d: not resolved (%v,%v)", i, ok1, ok2)
		}
		if c1 != c2 {
			t.Fatalf("key %d: unstable owner %p then %p", i, c1, c2)
		}
	}
}

// TestRoundTripThroughOwnerConn: the conn the router hands back actually works — a Write
// then Lookup+Fetch against the resolved owner round-trips the bytes. Proves routing and
// a real shard integrate, not just that the map returns something.
func TestRoundTripThroughOwnerConn(t *testing.T) {
	a, _ := startShard(t, "A")
	b, _ := startShard(t, "B")
	c, _ := startShard(t, "C")
	r := cluster.New(64)
	r.SetMembers([]cluster.Member{a, b, c})
	defer r.Close()

	ctx := context.Background()
	h := key(42)
	conn, ok := r.OwnerConn(h)
	if !ok {
		t.Fatal("no owner for key")
	}
	want := []byte("hello-kv-bytes")
	if err := writeBlock(ctx, conn, "m1", h, want); err != nil {
		t.Fatalf("write: %v", err)
	}

	cl := kvcachev1.NewKVCacheClient(conn)
	lr, err := cl.Lookup(ctx, &kvcachev1.LookupRequest{ModelId: "m1", BlockHashes: [][]byte{h}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(lr.Blocks) != 1 || !lr.Blocks[0].HasEntry {
		t.Fatalf("expected hit, got %+v", lr.Blocks)
	}

	got, err := fetchBlock(ctx, cl, "m1", h)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("fetched %q, want %q", got, want)
	}
}

// TestReconcileClosesRemoved: dropping a member re-homes its keys onto survivors, closes
// the dropped connection (no leak), and does NOT re-dial the survivors (idempotent adds).
func TestReconcileClosesRemoved(t *testing.T) {
	a, _ := startShard(t, "A")
	b, _ := startShard(t, "B")
	c, _ := startShard(t, "C")
	r := cluster.New(64)
	r.SetMembers([]cluster.Member{a, b, c})
	defer r.Close()

	const samples = 2000
	pre := map[*grpc.ClientConn]struct{}{}
	for i := 0; i < samples; i++ {
		conn, ok := r.OwnerConn(key(i))
		if !ok {
			t.Fatalf("key %d unresolved before remove", i)
		}
		pre[conn] = struct{}{}
	}
	if len(pre) != 3 {
		t.Fatalf("keys spread over %d conns, want 3", len(pre))
	}

	r.SetMembers([]cluster.Member{a, b}) // drop C

	post := map[*grpc.ClientConn]struct{}{}
	for i := 0; i < samples; i++ {
		conn, ok := r.OwnerConn(key(i))
		if !ok {
			t.Fatalf("key %d homeless after remove", i)
		}
		post[conn] = struct{}{}
	}
	if len(post) != 2 {
		t.Fatalf("keys spread over %d conns after remove, want 2", len(post))
	}

	// The conn that disappeared from routing must have been Closed (state Shutdown).
	var removed *grpc.ClientConn
	for conn := range pre {
		if _, still := post[conn]; !still {
			removed = conn
		}
	}
	if removed == nil {
		t.Fatal("no connection was dropped")
	}
	if st := removed.GetState(); st != connectivity.Shutdown {
		t.Errorf("dropped conn state = %v, want Shutdown (leaked?)", st)
	}
	// Survivors must be the SAME conns, not re-dialed.
	for conn := range post {
		if _, existed := pre[conn]; !existed {
			t.Error("a survivor connection was re-dialed on reconcile")
		}
	}
}

// TestSetMembersIdempotent: calling with the current set changes nothing — same conn.
func TestSetMembersIdempotent(t *testing.T) {
	a, _ := startShard(t, "A")
	b, _ := startShard(t, "B")
	r := cluster.New(64)
	members := []cluster.Member{a, b}
	r.SetMembers(members)
	defer r.Close()

	before, _ := r.OwnerConn(key(7))
	r.SetMembers(members) // no-op
	after, ok := r.OwnerConn(key(7))
	if !ok || before != after {
		t.Fatalf("idempotent SetMembers changed routing: %p -> %p (ok=%v)", before, after, ok)
	}
}

// TestDownedShardDegradesToMiss: the router does not know a shard is down (lazy conns), so
// OwnerConn still resolves it — and the RPC against it errors. That error is the caller's
// degrade-to-miss signal; routing itself must not fail (ADR 0016).
func TestDownedShardDegradesToMiss(t *testing.T) {
	m, gs := startShard(t, "solo")
	r := cluster.New(64)
	r.SetMembers([]cluster.Member{m})
	defer r.Close()

	conn, ok := r.OwnerConn(key(1))
	if !ok {
		t.Fatal("expected an owner")
	}
	gs.Stop() // shard goes away, but membership is unchanged

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cl := kvcachev1.NewKVCacheClient(conn)
	if _, err := cl.Lookup(ctx, &kvcachev1.LookupRequest{ModelId: "m", BlockHashes: [][]byte{key(1)}}); err == nil {
		t.Fatal("expected RPC error against a downed shard (caller maps it to a miss)")
	}
	// Router keeps routing — downness is the caller's concern, not a routing failure.
	if _, ok := r.OwnerConn(key(1)); !ok {
		t.Fatal("router should still resolve the owner; it cannot know the shard is down")
	}
}

// --- helpers ---

func writeBlock(ctx context.Context, conn *grpc.ClientConn, model string, h, data []byte) error {
	st, err := kvcachev1.NewKVCacheClient(conn).Write(ctx)
	if err != nil {
		return err
	}
	hdr := &kvcachev1.WriteHeader{ModelId: model, BlockHash: h, TotalSize: uint64(len(data))}
	if err := st.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Header{Header: hdr}}); err != nil {
		return err
	}
	chunk := &kvcachev1.KVChunk{Data: data, Last: true}
	if err := st.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Chunk{Chunk: chunk}}); err != nil {
		return err
	}
	_, err = st.CloseAndRecv()
	return err
}

func fetchBlock(ctx context.Context, cl kvcachev1.KVCacheClient, model string, h []byte) ([]byte, error) {
	st, err := cl.Fetch(ctx, &kvcachev1.FetchRequest{ModelId: model, BlockHash: h})
	if err != nil {
		return nil, err
	}
	var out []byte
	for {
		chunk, err := st.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, chunk.GetData()...)
	}
}
