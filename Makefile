.PHONY: build test test-race test-integration lint bench cover

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

lint:
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found in PATH, running go vet..."; \
		go vet ./...; \
	fi

bench:
	go test -bench=. -benchmem -run=^$$ ./...

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out
