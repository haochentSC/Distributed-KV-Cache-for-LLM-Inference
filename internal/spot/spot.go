// Package spot polls the EC2 instance-metadata service for the Spot interruption notice
// and turns it into a graceful-drain trigger. AWS gives ~2 minutes between the notice and
// the reclaim; that is plenty of time to (a) revoke the etcd membership lease so clients
// stop routing here, then (b) GracefulStop the gRPC server. This package is the bridge
// between AWS's signal and the existing shutdown path in cmd/cache-server/main.go.
//
// Why this lives in its own package: the cache logic must compile and run with no AWS
// dependency (local dev, integration tests, anywhere outside EC2). The cache server wires
// this in only when -spot is set, and the package itself is just an HTTP poll — no AWS SDK,
// no auth, no IAM. The IMDSv2 token dance is the only AWS-specific bit.
//
// Reference: AWS docs "Spot Instance interruption notices" — a 200 OK from
// http://169.254.169.254/latest/meta-data/spot/instance-action means the interruption is
// scheduled; any other status means no notice yet.
package spot

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	// imdsBase is the EC2 instance-metadata endpoint. Link-local, not internet-routable.
	imdsBase = "http://169.254.169.254"
	// tokenPath is IMDSv2 — required on modern AMIs. v1 still works but v2 is the only
	// path that survives the IMDSv1-disabled hardening many orgs apply.
	tokenPath = "/latest/api/token"
	// noticePath returns 200 ONCE the interruption is scheduled, 404 otherwise.
	noticePath = "/latest/meta-data/spot/instance-action"
	// pollInterval keeps the poll under the 2-minute notice window with margin. Going much
	// faster wastes CPU/network; much slower risks missing the notice in a partial poll.
	pollInterval = 5 * time.Second
	// tokenTTL is how long the IMDSv2 token is valid. We refresh per poll for simplicity.
	tokenTTL = "60"
	// httpTimeout caps each IMDS request so a flaky metadata service can't stall the watcher.
	httpTimeout = 2 * time.Second
)

// Watch polls IMDS until either an interruption notice arrives or ctx is cancelled. On a
// notice it calls onInterrupt EXACTLY ONCE and returns; onInterrupt is the hook the cache
// server uses to trigger the same drain path SIGTERM uses (revoke lease, GracefulStop).
//
// Not running on EC2 is a graceful no-op: the very first poll fails (no link-local route),
// the function logs once and continues polling. If you want it to exit instead, cancel ctx.
func Watch(ctx context.Context, onInterrupt func()) {
	client := &http.Client{Timeout: httpTimeout}
	var loggedUnreachable bool
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			notice, err := checkOnce(ctx, client)
			if err != nil {
				if !loggedUnreachable {
					// Log only the first failure — on dev machines this would log every 5 s
					// forever. The watcher staying alive is intentional: an EC2 metadata
					// blip shouldn't disable Spot handling permanently.
					log.Printf("spot: IMDS unreachable (this is normal off EC2): %v", err)
					loggedUnreachable = true
				}
				continue
			}
			loggedUnreachable = false
			if notice {
				log.Printf("spot: interruption notice received — triggering drain")
				onInterrupt()
				return
			}
		}
	}
}

// checkOnce does one IMDSv2 token + notice poll. Returns (true, nil) iff the notice
// endpoint returns 200; any non-200 (typically 404) is (false, nil). Transport errors
// surface as err so Watch can log them once.
func checkOnce(ctx context.Context, client *http.Client) (bool, error) {
	token, err := fetchToken(ctx, client)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imdsBase+noticePath, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the conn can be reused
	return resp.StatusCode == http.StatusOK, nil
}

func fetchToken(ctx context.Context, client *http.Client) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, imdsBase+tokenPath, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", tokenTTL)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
