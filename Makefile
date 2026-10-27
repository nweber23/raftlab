.PHONY: test lint fmt

# Every test run uses the race detector.
test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...