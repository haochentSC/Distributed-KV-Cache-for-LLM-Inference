package coord_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
	"github.com/haochentSC/distributed-kv-cache/internal/coord"
)

func key(i int) []byte {
	h := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
	return h[:]
}

// TestDriveRouterAppliesSnapshots: DriveRouter feeds each membership snapshot into the
// router's SetMembers seam. Pre-load + close the channel so the loop runs to completion
// synchronously (no sleeps): after the LAST snapshot only "a" remains, so every key routes
// to "a". This exercises the etcd->router wiring without needing etcd.
func TestDriveRouterAppliesSnapshots(t *testing.T) {
	r := cluster.New(64)
	defer r.Close()

	ch := make(chan []cluster.Member, 2)
	ch <- []cluster.Member{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: "127.0.0.1:2"}}
	ch <- []cluster.Member{{ID: "a", Addr: "127.0.0.1:1"}} // b leaves
	close(ch)

	coord.DriveRouter(context.Background(), ch, r) // drains both, returns on close

	for i := 0; i < 200; i++ {
		owner, ok := r.Owner(key(i))
		if !ok || owner != "a" {
			t.Fatalf("key %d: owner=%q ok=%v, want sole survivor a", i, owner, ok)
		}
	}
}

// TestDriveRouterStopsOnContext: DriveRouter returns promptly when ctx is cancelled even
// with no snapshots pending. (time.After is only a failure deadline, not success-path sync.)
func TestDriveRouterStopsOnContext(t *testing.T) {
	r := cluster.New(64)
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan []cluster.Member) // never written
	done := make(chan struct{})
	go func() { coord.DriveRouter(ctx, ch, r); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DriveRouter did not return after ctx cancel")
	}
}

// TestEtcdRegisterAndWatch is the ACCEPTANCE test for the two guided methods (Register +
// WatchMembers). It SKIPS when no etcd is reachable on localhost:2379, and FAILS until the
// methods are implemented. Run a single-node etcd first:
//
//	docker run -d --name kvc-etcd -p 2379:2379 \
//	  quay.io/coreos/etcd:v3.5.17 etcd \
//	  --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
//
// then: go test ./internal/coord/ -run TestEtcdRegisterAndWatch -v
func TestEtcdRegisterAndWatch(t *testing.T) {
	cli := dialTestEtcd(t)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	snaps, err := cli.WatchMembers(ctx)
	if err != nil {
		t.Fatalf("WatchMembers: %v", err)
	}

	// Unique id per run so stale keys from a prior run can't confuse the assertions.
	id := fmt.Sprintf("node-itest-%d", time.Now().UnixNano())
	release, err := cli.Register(ctx, id, "10.0.0.1:50051", 5)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	waitSnapshot(t, snaps, func(m []cluster.Member) bool { return containsID(m, id) },
		"registered node to appear in a snapshot")

	release() // graceful deregister: lease revoked -> key deleted -> node leaves

	waitSnapshot(t, snaps, func(m []cluster.Member) bool { return !containsID(m, id) },
		"node to disappear after release")
}

// --- helpers ---

func dialTestEtcd(t *testing.T) *coord.Client {
	t.Helper()
	cli, err := coord.Dial([]string{"localhost:2379"}, 2*time.Second)
	if err != nil {
		t.Skipf("etcd not configured (%v); skipping integration test", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		t.Skipf("etcd not reachable on localhost:2379 (%v); skipping. Start one with docker (see test doc).", err)
	}
	return cli
}

func waitSnapshot(t *testing.T, ch <-chan []cluster.Member, pred func([]cluster.Member) bool, what string) {
	t.Helper()
	deadline := time.After(8 * time.Second)
	for {
		select {
		case snap, ok := <-ch:
			if !ok {
				t.Fatalf("snapshot channel closed while waiting for %s", what)
			}
			if pred(snap) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func containsID(m []cluster.Member, id string) bool {
	for _, x := range m {
		if x.ID == id {
			return true
		}
	}
	return false
}
