.PHONY: build test test-race test-integration test-python lint bench bench-micro cover

# `go build ./...` sozinho DESCARTA o binário: o módulo tem dois packages
# main (cmd/nishidb e benchmark/ingest-bench), e sem -o o toolchain só
# verifica que os dois compilam, sem deixar nada em disco. O `./...` continua
# aqui como verificação — pega quebra em qualquer pacote, não só no CLI —, e a
# segunda chamada é quem de fato emite `./nishidb`, com os três ldflags do
# buildinfo carimbados na hora do build. É a MESMA linha que o `release.yml`
# (Task A2) herda ao chamar `make build`; não crie um segundo comando de
# build fora daqui, ou os dois divergem em silêncio.
LDFLAGS := -X github.com/nishisan-dev/nishidb/internal/buildinfo.Versao=$(shell git describe --tags --always --dirty) \
           -X github.com/nishisan-dev/nishidb/internal/buildinfo.Commit=$(shell git rev-parse --short HEAD) \
           -X github.com/nishisan-dev/nishidb/internal/buildinfo.Data=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

build:
	go build ./...
	go build -ldflags "$(LDFLAGS)" -o nishidb ./cmd/nishidb

# Suíte completa, INCLUINDO os gates de medida — os de alocação
# (testing.AllocsPerRun) e o de vazão da ingestão pelo CLI —, que são
# //go:build !race por construção. É este alvo que protege o orçamento de
# alocações por evento e o custo por evento do comando: rode-o antes de
# commitar.
test:
	go test ./...

# Detector de corrida. NÃO cobre os gates de medida: a instrumentação do -race
# altera a contagem de alocações e infla o Apply sem inflar o checkpoint, então
# eles ficam de fora do build e se anunciam como SKIP. Os dois alvos são
# complementares, nenhum substitui o outro.
test-race:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

# scripts/nishidb-stats.py é código de produção — o painel que o operador lê
# em cima de um incidente —, e até a Task A3 o teste dele
# (nishidb-stats_test.py) só rodava pela mão de quem lembrasse de chamar
# `python3` direto: nenhum Makefile nem workflow o executava, então a
# próxima alteração no painel podia quebrar a exibição da versão sem que
# NADA acusasse. O alvo segue o molde do `lint` logo abaixo: ferramenta
# ausente FALHA alto e nomeia o que falta, não vira SKIP silencioso — é o
# mesmo defeito do débito 53 (gate que passa porque a ferramenta não está lá)
# por uma porta nova, em Python em vez de em Go.
test-python:
	@if command -v python3 > /dev/null; then \
		python3 scripts/nishidb-stats_test.py; \
	else \
		echo "erro: python3 não está no PATH — o teste do painel NÃO rodou." >&2; \
		exit 1; \
	fi

# Os dois tetos vão desligados também aqui, e a duplicação com o `issues:` do
# .golangci.yml é deliberada: o modo de falha deste gate é o SILÊNCIO — linter
# estourado no teto absorve apontamento novo sem mexer no total —, e `make lint`
# é o comando que a doutrina do projeto cita. Repetir a intenção na linha de
# comando custa dois flags e garante que quem roda o alvo vê o número real (390
# nesta branch), não o truncado. O porquê longo está no .golangci.yml.
#
# O ramo `else` FALHA, e falha alto. Ele já caiu para `go vet ./...` devolvendo
# código 0, e essa é a outra metade da cegueira do débito 53: um gate que passa
# quando a ferramenta está AUSENTE não é gate, é carimbo — foi assim que o
# projeto atravessou o M1 achando que tinha lint, porque a única pista era uma
# linha de aviso rolando no meio da saída. `go vet` também não substitui o que
# se perde: ele cobre um subconjunto de `govet` e não conhece `errcheck`,
# `revive` nem `staticcheck`, que sozinhos respondem por 388 dos 390
# apontamentos da linha de base — enquanto o `govet` não responde por NENHUM
# deles. O fallback trocava o gate por uma checagem que hoje não acusa nada.
# Quem não tem o binário precisa descobrir pelo exit code, com o nome do que
# instalar na mão.
#
# A VERSÃO da instrução de instalação é fixa, e não `@latest`, porque esta linha
# é a única instrução de instalação do linter no repositório inteiro: é ela que
# decide qual binário mede a linha de base. Versão do instrumento é a mesma
# dimensão que os tetos — configuração que muda o número —, e com ela solta o
# próximo desenvolvedor instala outra, mede outro total e a comparação "390
# antes, 390 depois" volta a mentir, que é o defeito dos débitos 53 e 59
# reaberto por outra porta. Subir a versão é ato deliberado: re-medir a linha de
# base e atualizar o débito 53 na mesma passada.
#
# O alvo é verificado por internal/gates/lint_test.go, que o EXECUTA com o PATH
# controlado. Gate sem verificação de si mesmo é o mesmo carimbo, um nível
# acima: sem esse teste, `exit 1` virava `exit 0` e a suíte seguia verde.
lint:
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run --max-issues-per-linter=0 --max-same-issues=0; \
	else \
		echo "erro: golangci-lint não está no PATH — o gate de lint NÃO rodou." >&2; \
		echo "  instale: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2" >&2; \
		echo '  e ponha $$(go env GOPATH)/bin no PATH' >&2; \
		exit 1; \
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
