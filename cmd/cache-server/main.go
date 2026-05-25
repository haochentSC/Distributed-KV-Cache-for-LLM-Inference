// Command cache-server runs a single KV-cache shard over gRPC.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
	"github.com/haochentSC/distributed-kv-cache/internal/server"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	flag.Parse()

	store := cache.NewStore(cache.NoopPolicy{})
	srv := server.New(store)

	gs := grpc.NewServer()
	kvcachev1.RegisterKVCacheServer(gs, srv)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	go func() {
		log.Printf("kv-cache server listening on %s", *addr)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	// TODO(hc) Phase 3: wire graceful drain to SIGTERM and the EC2 Spot interruption notice.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	gs.GracefulStop()
}
