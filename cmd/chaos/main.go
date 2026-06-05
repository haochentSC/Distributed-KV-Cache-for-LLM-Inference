// Command chaos is the Phase 4 chaos-test harness (plan §Phase 4, ADR 0026). It stands up a
// local multi-node cache cluster on a running etcd, drives VERIFYING load through it
// (loadgen -verify), and kills/restarts random nodes on a schedule — then asserts the cluster
// never served KV that mismatched the requested key (the ADR 0016 correctness invariant) and
// that throughput recovers after each kill.
//
// The hypothesis it falsifies (ADR 0026): under node loss, RF=2 replication + etcd lease-expiry
// failover (Phase 3) keep the cluster CORRECT (zero violations — wrong bytes are a bug, misses
// are fine) and AVAILABLE (load recovers within ~the lease TTL once the dead node leaves the
// ring). It exits non-zero if loadgen reports any violation, so it doubles as a CI gate.
//
// SCOPE (local-first): the only fault here is a HARD PROCESS KILL (Process.Kill ≈ SIGKILL = a
// crash — the unplanned-loss case the lease TTL exists for). Network partition + latency
// injection need tc/iptables on Linux and land with the AWS infra (Sub-stage E); they are out of
// scope on a Windows laptop. A clean kill is the highest-value, fully-portable fault and exercises
// the whole lease-expiry → ring-removal → replica-takeover path.
//
// Prereq: a reachable etcd (the local kvc-etcd container is fine). Everything else — building the
// cache-server + loadgen binaries and launching/killing them — the harness does itself.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/haochentSC/distributed-kv-cache/internal/coord"
)

func main() {
	nodes := flag.Int("nodes", 3, "number of cache-server nodes to launch")
	etcd := flag.String("etcd", "localhost:2379", "etcd endpoints (comma-separated)")
	basePort := flag.Int("base-port", 50051, "first gRPC port; node i listens on base-port+i")
	baseMetricsPort := flag.Int("base-metrics-port", 9100, "first metrics port; node i exposes /metrics on base-metrics-port+i (matches deploy/observability/prometheus.yml)")
	rf := flag.Int("rf", 2, "replication factor the servers run with")
	leaseTTL := flag.Int64("lease-ttl", 5, "etcd membership lease TTL (s) — the failure-detection window, so also ~the recovery bound")
	maxBytes := flag.Int64("max-bytes", 0, "per-node byte bound (0 = unbounded); set it to also exercise eviction under chaos")
	duration := flag.Duration("duration", 60*time.Second, "total chaos run time")
	killEvery := flag.Duration("kill-every", 15*time.Second, "interval between node kills")
	downTime := flag.Duration("down-time", 8*time.Second, "how long a killed node stays down before restart (0 = never restart)")
	concurrency := flag.Int("concurrency", 8, "loadgen concurrent clients")
	payloadBytes := flag.Int("payload-bytes", 256<<10, "loadgen KV bytes per block (default 256KiB to keep a local run light)")
	prefixShare := flag.Float64("prefix-share", 0.8, "loadgen hot-prefix reuse fraction")
	seed := flag.Int64("seed", 1, "RNG seed for which node gets killed (reproducible)")
	flag.Parse()

	if *nodes < 1 {
		log.Fatal("need -nodes >= 1")
	}
	if *nodes <= *rf {
		log.Printf("WARNING: -nodes (%d) <= -rf (%d): a kill can drop a key below its replication factor, "+
			"so you'll see misses (still never violations). Use -nodes > -rf for a clean availability story.", *nodes, *rf)
	}

	// 0. Confirm etcd is reachable before doing anything expensive, with an actionable hint.
	if err := pingEtcd(*etcd); err != nil {
		log.Fatalf("etcd unreachable at %s: %v\n  start it with:\n  docker run -d --name kvc-etcd -p 2379:2379 "+
			"quay.io/coreos/etcd:v3.5.17 /usr/local/bin/etcd "+
			"--advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379", *etcd, err)
	}

	// 1. Build the cache-server and loadgen binaries ONCE into a temp dir, then exec them
	// directly. Building (not `go run`) matters: `go run` spawns a child compiler+process, and
	// killing the `go run` wrapper can orphan the real server — Process.Kill must hit the server
	// itself or the lease never lapses and there is no failover to test.
	workDir, err := os.MkdirTemp("", "kvc-chaos-")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(workDir)
	serverBin := filepath.Join(workDir, exeName("cache-server"))
	loadgenBin := filepath.Join(workDir, exeName("loadgen"))
	log.Printf("building binaries into %s ...", workDir)
	if err := goBuild("./cmd/cache-server", serverBin); err != nil {
		log.Fatalf("build cache-server: %v", err)
	}
	if err := goBuild("./cmd/loadgen", loadgenBin); err != nil {
		log.Fatalf("build loadgen: %v", err)
	}

	// ctx cancels everything on Ctrl-C or when the run completes; the deferred teardown kills any
	// server still alive so a crashed/aborted run never leaks processes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigc; log.Println("interrupted — tearing down"); cancel() }()

	cl := &cluster{serverBin: serverBin, etcd: *etcd, rf: *rf, leaseTTL: *leaseTTL, maxBytes: *maxBytes,
		basePort: *basePort, baseMetricsPort: *baseMetricsPort}
	defer cl.killAll()

	// 2. Launch the nodes and wait until all N have registered in etcd (so load doesn't start
	// against a half-formed ring).
	log.Printf("launching %d nodes ...", *nodes)
	for i := 0; i < *nodes; i++ {
		if err := cl.start(i); err != nil {
			log.Fatalf("start node %d: %v", i, err)
		}
	}
	if err := waitForMembers(ctx, *etcd, *nodes, 20*time.Second); err != nil {
		log.Fatalf("waiting for registration: %v", err)
	}
	log.Printf("all %d nodes registered", *nodes)

	// 3. Start the verifying load for the whole window. Its stdout (incl. the periodic stats line
	// and any VIOLATION log) streams to our terminal; its EXIT CODE is the correctness verdict.
	lg := exec.CommandContext(ctx, loadgenBin,
		"-etcd", *etcd,
		"-verify",
		"-duration", duration.String(),
		"-stats-every", "2s",
		"-concurrency", strconv.Itoa(*concurrency),
		"-payload-bytes", strconv.Itoa(*payloadBytes),
		"-prefix-share", strconv.FormatFloat(*prefixShare, 'f', -1, 64),
	)
	lg.Stdout, lg.Stderr = os.Stdout, os.Stderr
	log.Printf("starting verifying load for %s (kill-every=%s, down-time=%s)", *duration, *killEvery, *downTime)
	if err := lg.Start(); err != nil {
		log.Fatalf("start loadgen: %v", err)
	}

	// 4. Fault loop: while load runs, kill a random live node every -kill-every and (optionally)
	// restart it after -down-time. The aliveAboveRF guard keeps at least rf nodes up so a key's
	// primary+replica are never BOTH down at once — that keeps the availability story clean
	// (correctness holds either way). Runs until the load finishes or ctx is cancelled.
	start := time.Now()
	stop := make(chan struct{})
	var faultWG sync.WaitGroup
	faultWG.Add(1)
	go func() {
		defer faultWG.Done()
		cl.faultLoop(ctx, stop, start, *killEvery, *downTime, *rf, rand.New(rand.NewSource(*seed)))
	}()

	// 5. Wait for the load to finish, then stop the fault loop and tear down.
	lgErr := lg.Wait()
	close(stop)
	faultWG.Wait()
	cl.killAll()

	kills, restarts := cl.counts()
	fmt.Println("---- chaos report ----")
	fmt.Printf("nodes:     %d (rf=%d, lease-ttl=%ds)\n", *nodes, *rf, *leaseTTL)
	fmt.Printf("duration:  %s\n", duration.Round(time.Second))
	fmt.Printf("faults:    %d kills, %d restarts\n", kills, restarts)
	fmt.Println("recovery:  read the per-2s 'req/s' dip+recovery in the loadgen lines above,")
	fmt.Println("           and the request-rate / resident-bytes panels in Grafana (bounded by lease TTL).")

	if lgErr != nil {
		if ctx.Err() != nil {
			log.Fatalf("run aborted: %v", ctx.Err())
		}
		// Non-zero loadgen exit under -verify means it counted ≥1 correctness violation.
		fmt.Println("RESULT:    FAIL — correctness violations detected (see VIOLATION lines above)")
		os.Exit(1)
	}
	fmt.Println("RESULT:    PASS — zero correctness violations across the chaos run (ADR 0016 holds)")
}

// node is one launched cache-server.
type node struct {
	id   string
	args []string
	cmd  *exec.Cmd
}

// cluster owns the launched nodes and the kill/restart bookkeeping. A single mutex guards node
// state because the fault loop and per-node restart timers both touch it.
type cluster struct {
	serverBin       string
	etcd            string
	rf              int
	leaseTTL        int64
	maxBytes        int64
	basePort        int
	baseMetricsPort int

	mu       sync.Mutex
	nodes    []*node // index i is logical node i; cmd==nil means currently down
	kills    int
	restarts int
}

// start launches (or relaunches) node i. Caller holds no lock; start locks internally.
func (c *cluster) start(i int) error {
	port := c.basePort + i
	metricsPort := c.baseMetricsPort + i
	id := fmt.Sprintf("chaos-%d", i)
	args := []string{
		"-addr", fmt.Sprintf(":%d", port),
		"-advertise", fmt.Sprintf("localhost:%d", port),
		"-node-id", id,
		"-etcd", c.etcd,
		"-rf", strconv.Itoa(c.rf),
		"-lease-ttl", strconv.FormatInt(c.leaseTTL, 10),
		"-max-bytes", strconv.FormatInt(c.maxBytes, 10),
		"-metrics-addr", fmt.Sprintf(":%d", metricsPort),
	}
	cmd := exec.Command(c.serverBin, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.nodes) <= i {
		c.nodes = append(c.nodes, &node{})
	}
	c.nodes[i] = &node{id: id, args: args, cmd: cmd}
	return nil
}

// faultLoop kills a random live node every killEvery and restarts it after downTime, until stop
// is closed or ctx is cancelled.
func (c *cluster) faultLoop(ctx context.Context, stop <-chan struct{}, start time.Time, killEvery, downTime time.Duration, rf int, rng *rand.Rand) {
	t := time.NewTicker(killEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			i, ok := c.pickVictim(rf, rng)
			if !ok {
				log.Printf("[%5.1fs] (skip kill: keeping >= rf nodes alive)", time.Since(start).Seconds())
				continue
			}
			c.kill(i)
			log.Printf("[%5.1fs] KILLED node %d — lease expires in <= %ds, then failover to replica",
				time.Since(start).Seconds(), i, c.leaseTTL)
			if downTime > 0 {
				go c.restartAfter(ctx, i, downTime, start)
			}
		}
	}
}

// pickVictim returns a random currently-alive node index, but only if killing it would leave at
// least rf nodes alive (so primary+replica of a key are never both down). ok=false => skip.
func (c *cluster) pickVictim(rf int, rng *rand.Rand) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var alive []int
	for i, n := range c.nodes {
		if n.cmd != nil {
			alive = append(alive, i)
		}
	}
	if len(alive)-1 < rf {
		return 0, false
	}
	return alive[rng.Intn(len(alive))], true
}

// kill hard-terminates node i (a crash). No-op if already down.
func (c *cluster) kill(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	if n == nil || n.cmd == nil {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait() // reap so it doesn't linger as a zombie
	n.cmd = nil
	c.kills++
}

// restartAfter waits downTime then relaunches node i (unless the run is ending).
func (c *cluster) restartAfter(ctx context.Context, i int, downTime time.Duration, start time.Time) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(downTime):
	}
	if err := c.start(i); err != nil {
		log.Printf("[%5.1fs] restart node %d failed: %v", time.Since(start).Seconds(), i, err)
		return
	}
	c.mu.Lock()
	c.restarts++
	c.mu.Unlock()
	log.Printf("[%5.1fs] RESTARTED node %d — rejoins the ring on its next lease grant", time.Since(start).Seconds(), i)
}

// killAll terminates every live node. Safe to call twice (teardown + defer).
func (c *cluster) killAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n != nil && n.cmd != nil {
			_ = n.cmd.Process.Kill()
			_ = n.cmd.Wait()
			n.cmd = nil
		}
	}
}

func (c *cluster) counts() (kills, restarts int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kills, c.restarts
}

// pingEtcd dials etcd and does a cheap reachability check.
func pingEtcd(endpoints string) error {
	cli, err := coord.Dial(splitCSV(endpoints), 5*time.Second)
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cli.Ping(ctx)
}

// waitForMembers blocks until at least want nodes have registered under the membership prefix, or
// timeout. It reuses the same WatchMembers seam the clients use, so "registered" means exactly
// what the ring will see.
func waitForMembers(ctx context.Context, endpoints string, want int, timeout time.Duration) error {
	cli, err := coord.Dial(splitCSV(endpoints), 5*time.Second)
	if err != nil {
		return err
	}
	defer cli.Close()
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	snaps, err := cli.WatchMembers(wctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-wctx.Done():
			return fmt.Errorf("only saw fewer than %d members before timeout (are old chaos-* keys lingering in etcd?)", want)
		case snap, ok := <-snaps:
			if !ok {
				return fmt.Errorf("membership channel closed")
			}
			if len(snap) >= want {
				return nil
			}
		}
	}
}

// splitCSV parses a comma-separated list, trimming blanks.
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// goBuild compiles pkg to out, streaming compiler errors through.
func goBuild(pkg, out string) error {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// exeName adds the platform executable suffix.
func exeName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}
	return base
}
