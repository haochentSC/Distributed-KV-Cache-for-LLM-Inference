GOPATH_BIN := $(shell go env GOPATH)/bin
export PATH := $(PATH):$(GOPATH_BIN)
MODULE := github.com/haochentSC/distributed-kv-cache

.PHONY: tools proto proto-go proto-python build test vet fmt run-server run-loadgen tidy demo plots

tools: ## install the protoc Go plugins
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

proto: proto-go proto-python ## generate Go and Python gRPC code from the .proto

proto-go: ## generate Go gRPC code from the .proto
	protoc --go_out=. --go_opt=module=$(MODULE) \
	       --go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
	       proto/kvcache/v1/kvcache.proto

proto-python: ## generate Python gRPC code from the .proto
	python -m grpc_tools.protoc -Iproto \
	       --python_out=connector/src/kvcache_proto \
	       --grpc_python_out=connector/src/kvcache_proto \
	       proto/kvcache/v1/kvcache.proto

tidy: ## sync go.mod/go.sum
	go mod tidy

build: ## build all binaries
	go build ./...

test: ## run tests with the race detector
	go test ./... -race

vet:
	go vet ./...

fmt:
	gofmt -w .

run-server: ## run the cache server
	go run ./cmd/cache-server

run-loadgen: ## run the synthetic load generator
	go run ./cmd/loadgen

demo: ## 3-node local cluster + verified load + node kills (0 violations or non-zero exit)
	sh scripts/demo-chaos.sh

plots: ## regenerate docs/img/*.png from the committed benchmark JSONs
	python scripts/plots/make_all.py
