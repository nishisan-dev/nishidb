.PHONY: build test test-race test-integration lint bench cover

build:
	go build ./...

# Suíte completa, INCLUINDO os gates de alocação (testing.AllocsPerRun), que
# são //go:build !race por construção. É este alvo que protege o orçamento de
# alocações por evento — rode-o antes de commitar.
test:
	go test ./...

# Detector de corrida. NÃO cobre os gates de alocação: a instrumentação do
# -race altera a contagem, então eles ficam de fora do build e se anunciam
# como SKIP. Os dois alvos são complementares, nenhum substitui o outro.
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
