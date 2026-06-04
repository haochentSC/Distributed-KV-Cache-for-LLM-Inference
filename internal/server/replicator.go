package server

// replicator.go — the PRIMARY side of RF=2 replication (Phase 3 Sub-stage B, ADR 0021).
//
// When this node is the primary for a block (the ring owner the client routed to), the
// Write handler hands the stored block to the Replicator, which forwards a copy to the
// block's replica(s) over the Replicate RPC. It is ASYNC and BEST-EFFORT by design:
//   - The client was already acked once the PRIMARY stored the block (ADR 0013 — cache
//     data is eventually consistent), so replication must never sit on the ack path.
//   - A dropped or failed replication just means the replica misses one block; on a
//     primary failure that block is a recompute, never a correctness violation. So the
//     queue is bounded and we DROP under pressure rather than block (ADR 0017 backpressure).
//
// Placement (which peer is the replica) comes from the SAME consistent-hash ring the
// client routes on, via the shared Router: OwnersN(routingKey, rf) returns [primary,
// replica, ...]; we forward to every owner that is not ourselves. routingKey is the prompt
// ROOT, not the block hash (prefix-affinity, ADR 0014/0021) — that is why ReplicaJob
// carries it.

import (
	"context"
	"io"
	"log"

	"google.golang.org/grpc"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
)

// replicateChunkBytes mirrors the Write/Fetch 1 MiB framing (ADR 0012) so all three paths
// stay under gRPC's 4 MB message cap with the same arithmetic.
const replicateChunkBytes = 1 << 20

// replicaQueueDepth bounds the in-flight replication backlog. Bounded because replication
// is droppable (see file header): a full queue means a slow/down replica, and we'd rather
// drop a copy than stall writes. Tune against payload size if memory becomes a concern.
const replicaQueueDepth = 256

// ReplicaJob is one block to copy to its replica(s). It carries everything the replica
// needs to store an identical entry at the primary's version. KV aliases the stored
// entry's buffer (the Store published it as an immutable snapshot, so sharing is safe —
// nobody mutates it).
type ReplicaJob struct {
	Hash          cache.BlockHash
	ModelID       string
	KV            []byte
	TokenIDs      []int32
	TenantID      string
	RecomputeCost float64
	Version       uint64 // the version the PRIMARY assigned; the replica stores under it
	RoutingKey    []byte // prompt root that selects the replica (empty => use Hash[:])
}

// Replicator forwards freshly-written blocks from this primary to their replica(s).
// One per cache-server. Construct with NewReplicator, then Run it in a goroutine.
type Replicator struct {
	self    string          // this node's ring ID — never replicate to ourselves
	rf      int             // replication factor (2 = primary + 1 replica)
	router  *cluster.Router // shared ring + connection pool (OwnersN, ConnFor)
	queue   chan ReplicaJob
	metrics metricsSink // never nil; noopMetrics until WithMetrics is called (ADR 0025)
}

// NewReplicator builds a Replicator. self must be the SAME node ID this server registers in
// the ring (so OwnersN can recognize "me"); router is the same Router the etcd membership
// watch drives (so the server's view of placement matches the clients'). [scaffold]
func NewReplicator(self string, rf int, router *cluster.Router) *Replicator {
	return &Replicator{
		self:    self,
		rf:      rf,
		router:  router,
		queue:   make(chan ReplicaJob, replicaQueueDepth),
		metrics: noopMetrics{},
	}
}

// WithMetrics installs a metrics sink (main wires the process-wide *metrics.Metrics). A nil m
// is ignored, keeping the noop default. Returns the Replicator for chaining.
func (r *Replicator) WithMetrics(m metricsSink) *Replicator {
	if m != nil {
		r.metrics = m
	}
	return r
}

// QueueDepth reports the current replication backlog (0..replicaQueueDepth). main polls it on a
// ticker for the replication_queue_depth gauge — a rising depth is the proxy for replication lag
// in this async-drop design. len on a buffered channel is a safe, lock-free snapshot.
func (r *Replicator) QueueDepth() int { return len(r.queue) }

// Enqueue schedules job for async replication. It is NON-BLOCKING: if the queue is full it
// DROPS the job (and should log it), because blocking here would stall the Write ack on a
// slow replica — the opposite of the "ack on primary" design.
func (r *Replicator) Enqueue(job ReplicaJob) {
	select {
	case r.queue <- job:
	default:
		// Drop > block: a slow replica must not stall the Write ack path (ADR 0017). A dropped
		// copy is at worst a future recompute, never a correctness violation (ADR 0013).
		r.metrics.Replica("dropped")
		log.Printf("replicator: queue full, dropping block %x v%d", job.Hash[:6], job.Version)
	}
}

// Run drains the queue and forwards each job until ctx is cancelled. Start once from main:
// `go repl.Run(ctx)`.
func (r *Replicator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-r.queue:
			r.forward(ctx, job)
		}
	}
}

// forward ships one job to each replica — the OwnersN(routingKey, rf) entries that are not
// self — via the Replicate RPC, carrying job.Version so the replica stores under the
// primary's version (loop-free: Replicate never re-forwards).
func (r *Replicator) forward(ctx context.Context, job ReplicaJob) {
	routingKey := job.RoutingKey
	if len(routingKey) == 0 {
		// Single-block / no-root fallback: a lone block's placement is by its own hash. This
		// keeps Replicator usable from callers that don't yet propagate a prompt root.
		routingKey = job.Hash[:]
	}
	ids := r.router.OwnersN(routingKey, r.rf)
	for _, id := range ids {
		if id == r.self {
			continue // never replicate to myself (I am the primary that just stored this)
		}
		conn, ok := r.router.ConnFor(id)
		if !ok {
			// Member is in the ring but not yet pooled (a race against a join, or a flapping
			// remove). Drop this copy rather than block — the next write will retry placement.
			continue
		}
		if err := r.sendOne(ctx, conn, job); err != nil {
			// Best-effort: every error path is log-and-continue. A failed copy is a future
			// recompute, not a fault to surface (ADR 0013 + ADR 0017).
			r.metrics.Replica("error")
			log.Printf("replicator: forward to %s for %x v%d: %v", id, job.Hash[:6], job.Version, err)
		} else {
			r.metrics.Replica("forwarded")
		}
	}
}

// sendOne streams one ReplicaJob to a single peer over the Replicate RPC. Factored out of
// forward so the per-peer logic — header, chunks, ack — is one linear function and so
// future call sites (rebalance/backfill) can reuse it.
func (r *Replicator) sendOne(ctx context.Context, conn *grpc.ClientConn, job ReplicaJob) error {
	client := kvcachev1.NewKVCacheClient(conn)
	stream, err := client.Replicate(ctx)
	if err != nil {
		return err
	}
	hdr := &kvcachev1.WriteHeader{
		ModelId:       job.ModelID,
		BlockHash:     job.Hash[:],
		TokenIds:      job.TokenIDs,
		TenantId:      job.TenantID,
		RecomputeCost: job.RecomputeCost,
		TotalSize:     uint64(len(job.KV)),
		Version:       job.Version,    // authoritative — the replica stores under this
		RoutingKey:    job.RoutingKey, // pass-through; the replica doesn't re-place but having
		// it on the wire keeps Replicate symmetric with Write for inspection/debugging.
	}
	if err := stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Header{Header: hdr}}); err != nil {
		return err
	}
	for off := 0; off < len(job.KV); off += replicateChunkBytes {
		end := off + replicateChunkBytes
		if end > len(job.KV) {
			end = len(job.KV)
		}
		chunk := &kvcachev1.KVChunk{Data: job.KV[off:end], Last: end == len(job.KV)}
		if err := stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Chunk{Chunk: chunk}}); err != nil {
			return err
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil && err != io.EOF {
		return err
	}
	return nil
}
