package coldtier

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Default worker-pool sizing. Spills are large (multi-MB) but best-effort, so a small pool with
// a bounded queue is enough: it smooths bursts and drops the overflow rather than stalling the
// evictor (which is what calls Spill, under a stripe lock).
const (
	defaultSpillWorkers = 4
	defaultSpillQueue   = 256
	spillPutTimeout     = 30 * time.Second // a stuck PutObject must not pin a worker forever
)

// s3API is the slice of the S3 client this tier uses, declared as an interface so the Put/Get
// paths could be tested against a fake without a live bucket (the unit tests use Memory instead;
// this keeps the door open and documents the exact surface we depend on).
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Tier implements Tier over an S3 bucket. Region and credentials come from the default chain
// (env / EC2 instance role) — there are no static credentials anywhere (ADR 0004).
type S3Tier struct {
	client s3API
	bucket string

	jobs    chan spillJob
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	dropped atomic.Int64 // spills shed because the queue was full (best-effort; observable)
}

type spillJob struct {
	model    string
	hash     [32]byte
	version  uint64
	tokenIDs []int32
	kv       []byte
}

// NewS3 builds an S3-backed cold tier on bucket and starts the spill worker pool. The ctx bounds
// the workers' lifetime (Close also stops them). LoadDefaultConfig resolves region+creds from the
// environment / instance role.
func NewS3(ctx context.Context, bucket string) (*S3Tier, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return newS3WithClient(ctx, s3.NewFromConfig(cfg), bucket), nil
}

// newS3WithClient is the injectable constructor (tests / NewS3 share it).
func newS3WithClient(ctx context.Context, client s3API, bucket string) *S3Tier {
	wctx, cancel := context.WithCancel(ctx)
	t := &S3Tier{
		client: client,
		bucket: bucket,
		jobs:   make(chan spillJob, defaultSpillQueue),
		cancel: cancel,
	}
	for i := 0; i < defaultSpillWorkers; i++ {
		t.wg.Add(1)
		go t.worker(wctx)
	}
	return t
}

// Spill enqueues the block for an async upload. NON-BLOCKING: if the queue is full we drop this
// spill (and count it) rather than block the evictor — a dropped spill is a future recompute, not
// a violation (ADR 0013).
func (t *S3Tier) Spill(model string, hash [32]byte, version uint64, tokenIDs []int32, kv []byte) {
	job := spillJob{model: model, hash: hash, version: version, tokenIDs: tokenIDs, kv: kv}
	select {
	case t.jobs <- job:
	default:
		if n := t.dropped.Add(1); n == 1 || n%100 == 0 {
			log.Printf("coldtier: spill queue full, dropped %d block(s) (best-effort; they become recomputes)", n)
		}
	}
}

// worker drains the spill queue, framing and uploading each block. It runs until ctx is cancelled
// AND the queue is drained (Close closes jobs so the range ends).
func (t *S3Tier) worker(ctx context.Context) {
	defer t.wg.Done()
	for job := range t.jobs {
		blob := encode(job.version, job.tokenIDs, job.kv) // off the hot path — copies here, not under lock
		putCtx, cancel := context.WithTimeout(ctx, spillPutTimeout)
		key := objectKey(job.model, job.hash)
		_, err := t.client.PutObject(putCtx, &s3.PutObjectInput{
			Bucket: &t.bucket,
			Key:    &key,
			Body:   bytes.NewReader(blob),
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			// Best-effort: log and move on. The block is simply not in cold storage, so a later
			// Fetch for it misses and recomputes — never wrong bytes.
			log.Printf("coldtier: spill %s failed: %v", key, err)
		}
	}
}

// Get fetches and decodes a block. A NoSuchKey is a clean miss (ok=false, err=nil); any other
// error is returned so the caller can log it, but it too is served as a miss (never wrong bytes).
func (t *S3Tier) Get(ctx context.Context, model string, hash [32]byte) ([]byte, uint64, []int32, bool, error) {
	key := objectKey(model, hash)
	out, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &t.bucket, Key: &key})
	if err != nil {
		var nsk *types.NoSuchKey
		var nf *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &nf) {
			return nil, 0, nil, false, nil // clean miss
		}
		return nil, 0, nil, false, err
	}
	defer out.Body.Close()
	blob, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, 0, nil, false, err
	}
	version, tokenIDs, kv, err := decode(blob)
	if err != nil {
		return nil, 0, nil, false, err
	}
	return kv, version, tokenIDs, true, nil
}

// Dropped reports how many spills were shed because the queue was full (best-effort visibility).
func (t *S3Tier) Dropped() int64 { return t.dropped.Load() }

// Close stops the workers, draining whatever is already queued.
func (t *S3Tier) Close() error {
	close(t.jobs) // let workers finish the in-flight queue
	t.wg.Wait()
	t.cancel()
	return nil
}
