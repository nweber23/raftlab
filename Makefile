.PHONY: run build test test-chaos lint fmt

# Serve the cluster visualizer on http://localhost:8080 (make run ADDR=127.0.0.1:9000 to move it).
ADDR ?= 127.0.0.1:8080
run:
	go run ./cmd/raftlab -addr $(ADDR)

build:
	go build -o bin/ ./cmd/...

# Every test run uses the race detector.
test:
	go test -race -count=1 ./...

# Many seeds of the chaos test; a failure prints its seed. Reproduce with
#   CHAOS_SEED=<n> go test -race -run Chaos -v ./internal/kv
CHAOS_SEEDS ?= 100
test-chaos:
	CHAOS_SEEDS=$(CHAOS_SEEDS) go test -race -count=1 -run Chaos -timeout 30m -v ./internal/kv

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...
