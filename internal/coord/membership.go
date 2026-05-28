// Package coord integrates the cache with etcd for the LINEARIZABLE metadata tier —
// cluster membership now, and shard-ownership leases in Sub-stage C. Per ADR 0013 only
// metadata goes through etcd's Raft; cache DATA stays eventually consistent and never
// touches etcd. Membership uses two etcd primitives:
//
//   - Lease: a TTL'd token. A node Puts its membership key bound to a lease and keepalives
//     it. If the node dies, keepalives stop, the lease expires, and etcd auto-deletes the
//     key — failure detection for free, which the watch turns into a ring removal.
//   - Watch: clients subscribe to the /kvcache/members/ prefix and rebuild membership on
//     every change, feeding each snapshot into Router.SetMembers (the ADR 0019 seam). So
//     the Phase-2 static driver becomes an etcd-driven one with no routing-code change.
//
// Schema is MEMBERSHIP ONLY (ADR 0020): the ring is deterministic from the member set
// (ADR 0018), so every client recomputes an identical ring — no per-vnode ownership is
// published.
package coord

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
)

// MembersPrefix is the etcd key prefix each live node publishes itself under:
// /kvcache/members/<node-id> -> <addr>.
const MembersPrefix = "/kvcache/members/"

// memberKey returns the etcd key for a node id. (Strip MembersPrefix from a watched key to
// recover the node id.)
func memberKey(nodeID string) string { return MembersPrefix + nodeID }

// Client wraps an etcd client for the cache's coordination needs.
type Client struct {
	etcd *clientv3.Client
}

// Dial connects to etcd (e.g. endpoints = []string{"localhost:2379"}). [scaffold]
func Dial(endpoints []string, dialTimeout time.Duration) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Client{etcd: cli}, nil
}

// Close releases the underlying etcd client. [scaffold]
func (c *Client) Close() error { return c.etcd.Close() }

// Ping checks etcd is reachable (a cheap Get). Useful for readiness and to let tests skip
// when no etcd is running. [scaffold]
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.etcd.Get(ctx, "ping-probe")
	return err
}

// Register publishes this node into membership under a lease and keeps the lease alive in
// the background until ctx is cancelled or the returned release func is called. When the
// node dies (keepalive stops), the lease expires and etcd deletes the key — automatic
// failure detection that the membership watch turns into a ring removal.
//
// TODO(hc) [guided — the lease lifecycle, implement it]:
//  1. Grant a lease: lr, err := c.etcd.Grant(ctx, ttlSeconds). Pick a TTL short enough that
//     a dead node leaves the ring quickly, long enough to survive a GC pause / brief blip
//     (~10s is a fine start). NOTE: this TTL vs the partition-detection window becomes the
//     split-brain knob in Sub-stage C — leave yourself a comment.
//  2. Put the key under the lease:
//     c.etcd.Put(ctx, memberKey(nodeID), addr, clientv3.WithLease(lr.ID)).
//  3. Keepalive: kaCh, err := c.etcd.KeepAlive(ctx, lr.ID). You MUST drain kaCh in a
//     goroutine — etcd sends an ack per interval; if nobody reads it the channel fills and
//     keepalive stalls (the node would silently drop out). When kaCh closes, the lease is
//     lost — log it (in Sub-stage C this means "I am no longer primary").
//  4. Return a release func that Revokes the lease (use a fresh short context, NOT ctx,
//     since ctx may already be cancelled during shutdown) so a graceful drain removes the
//     node immediately instead of waiting out the TTL (Sub-stage D).
func (c *Client) Register(ctx context.Context, nodeID, addr string, ttlSeconds int64) (release func(), err error) {
	lr, err := c.etcd.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, fmt.Errorf("grant lease: %w", err)
	}
	if _, err := c.etcd.Put(ctx, memberKey(nodeID), addr, clientv3.WithLease(lr.ID)); err != nil {
		_, _ = c.etcd.Revoke(context.Background(), lr.ID) // don't orphan the lease
		return nil, fmt.Errorf("put member key: %w", err)
	}

	// KeepAlive returns a channel of acks we MUST drain, or it back-pressures and the lease
	// silently lapses. Tie it to a cancellable context so release() can stop it.
	kaCtx, kaCancel := context.WithCancel(ctx)
	kaCh, err := c.etcd.KeepAlive(kaCtx, lr.ID)
	if err != nil {
		kaCancel()
		_, _ = c.etcd.Revoke(context.Background(), lr.ID)
		return nil, fmt.Errorf("keepalive: %w", err)
	}
	go func() {
		for range kaCh { // drain acks; nothing to do per-ack
		}
		// Channel closed. If we didn't cancel it ourselves, the lease was lost (crash blip,
		// partition) — the node should treat itself as unhealthy (matters in Sub-stage C).
		if kaCtx.Err() == nil {
			log.Printf("coord: keepalive for %s ended unexpectedly — lease lost", nodeID)
		}
	}()

	var once sync.Once
	release = func() {
		once.Do(func() {
			kaCancel() // stop keepalive
			// Revoke on a FRESH context: ctx is likely already cancelled during shutdown,
			// and we want the key deleted now rather than waiting out the TTL.
			rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rcancel()
			if _, err := c.etcd.Revoke(rctx, lr.ID); err != nil {
				log.Printf("coord: revoke lease for %s: %v", nodeID, err)
			}
		})
	}
	return release, nil
}

// WatchMembers yields the current membership as a fresh, complete snapshot: one immediately
// on subscribe, then a new one on every change under MembersPrefix. The driver feeds each
// snapshot straight into Router.SetMembers (ADR 0019), so a join/leave becomes a ring
// Add/Remove. The channel is closed when ctx is done or the watch ends.
//
// TODO(hc) [guided — the watch loop, implement it]:
//  1. Read the initial state with a prefix Get:
//     resp, err := c.etcd.Get(ctx, MembersPrefix, clientv3.WithPrefix()).
//     Build the first snapshot from resp.Kvs (node id = key with MembersPrefix trimmed;
//     value = addr) and send it. CAPTURE resp.Header.Revision.
//  2. Start the watch AT revision+1 so no event between the Get and the Watch is missed or
//     double-applied:
//     wch := c.etcd.Watch(ctx, MembersPrefix, clientv3.WithPrefix(),
//     clientv3.WithRev(resp.Header.Revision+1)).
//  3. Keep an authoritative map[string]string{id->addr} in the loop. For each WatchResponse,
//     apply ev.Type: PUT -> set, DELETE -> delete (ev.Kv.Key still carries the id). After
//     applying a batch, send a fresh snapshot of the whole map. (Re-emit the FULL set, not
//     deltas — SetMembers reconciles to "exactly this set", so a snapshot is the natural fit;
//     say why in a comment.)
//  4. Do the loop in a goroutine writing to an out channel; close the out channel when ctx is
//     done or wch closes. Return the receive end.
func (c *Client) WatchMembers(ctx context.Context) (<-chan []cluster.Member, error) {
	// Initial state. Capturing the revision lets us start the watch exactly after it, with
	// no gap and no double-apply between the Get and the Watch.
	resp, err := c.etcd.Get(ctx, MembersPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("initial members get: %w", err)
	}
	members := make(map[string]string, len(resp.Kvs)) // node id -> addr (authoritative)
	for _, kv := range resp.Kvs {
		members[strings.TrimPrefix(string(kv.Key), MembersPrefix)] = string(kv.Value)
	}
	startRev := resp.Header.Revision + 1

	out := make(chan []cluster.Member)
	go func() {
		defer close(out)
		if !sendSnapshot(ctx, out, members) { // initial snapshot
			return
		}
		wch := c.etcd.Watch(ctx, MembersPrefix, clientv3.WithPrefix(), clientv3.WithRev(startRev))
		for wresp := range wch { // closes when ctx is cancelled
			if err := wresp.Err(); err != nil {
				log.Printf("coord: members watch error: %v", err)
				return
			}
			for _, ev := range wresp.Events {
				id := strings.TrimPrefix(string(ev.Kv.Key), MembersPrefix)
				switch ev.Type {
				case clientv3.EventTypePut:
					members[id] = string(ev.Kv.Value)
				case clientv3.EventTypeDelete: // lease expiry or explicit revoke
					delete(members, id)
				}
			}
			if !sendSnapshot(ctx, out, members) {
				return
			}
		}
	}()
	return out, nil
}

// sendSnapshot emits the current member map as a fresh, COMPLETE set (not deltas) — because
// Router.SetMembers reconciles to "exactly this set" (ADR 0019), a snapshot is the natural
// unit. Returns false if ctx is done so the caller can stop.
func sendSnapshot(ctx context.Context, out chan<- []cluster.Member, members map[string]string) bool {
	snap := make([]cluster.Member, 0, len(members))
	for id, addr := range members {
		snap = append(snap, cluster.Member{ID: id, Addr: addr})
	}
	select {
	case out <- snap:
		return true
	case <-ctx.Done():
		return false
	}
}

// DriveRouter consumes membership snapshots and applies each to the router via SetMembers,
// blocking until ctx is cancelled or the channel closes. This single line replaces the
// Phase-2 static driver: the same Router.SetMembers seam (ADR 0019), now etcd-driven. It is
// deliberately trivial — the interesting work is in WatchMembers. [scaffold]
func DriveRouter(ctx context.Context, snaps <-chan []cluster.Member, r *cluster.Router) {
	for {
		select {
		case <-ctx.Done():
			return
		case snap, ok := <-snaps:
			if !ok {
				return
			}
			r.SetMembers(snap)
		}
	}
}
