.PHONY: build test test-race test-integration lint bench bench-micro cover

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

# Micro-benchmarks integrados, nos mesmos parâmetros dos baselines de
# benchmark/baselines/ (por isso os benchtimes fixos: comparação com benchstat
# exige a mesma contagem de iterações).
#
# NISHIDB_BENCH_DIR precisa apontar para DISCO REAL — os benchmarks de escrita
# appendam um record por iteração e geram centenas de MiB por rodada, com
# fallocate de verdade. Sem a variável eles pulam; em tmpfs travariam a máquina.
bench-micro:
	@if [ -z "$$NISHIDB_BENCH_DIR" ]; then \
		echo "aviso: NISHIDB_BENCH_DIR não definida — os benchmarks de escrita no Store real vão pular"; \
	fi
	go test ./benchmark/micro/ -run='^$$' -bench='BenchmarkApply' -benchmem -benchtime=20000x -count=6
	go test ./benchmark/micro/ -run='^$$' -bench='BenchmarkQuery|BenchmarkFullScan' -benchmem -benchtime=2000x -count=6

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out
