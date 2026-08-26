.PHONY: build test test-race test-integration test-python test-python-it lint-python lint bench bench-micro cover build-jdbc test-jdbc-unit test-jdbc

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

# ------------------------------------------------------------------ driver JDBC
#
# O repositório passou a ser poliglota na Fase 7, e o §97 continua valendo para
# o `nishidb`: o binário é único e sem runtime externo. O .jar é artefato de
# CLIENTE, não do engine.
#
# `make test` NÃO chama nada daqui, de propósito: a suíte Go não pode passar a
# exigir uma JVM instalada para rodar. Quem mexe no driver chama estes alvos.
JDBC_DIR := clients/jdbc

# O ramo `else` falha ALTO e nomeia o que falta, como o alvo `lint`: um gate que
# passa quando a ferramenta está AUSENTE não é gate, é carimbo (débito 53). O
# Maven não entra na conta — o wrapper o baixa, na versão pinada.
define exige-java
	@if ! command -v java >/dev/null 2>&1; then \
		echo "erro: java não está no PATH — o driver JDBC exige JDK 21 ou mais novo." >&2; \
		echo "  Ubuntu/Debian: apt install openjdk-21-jdk" >&2; \
		exit 1; \
	fi
endef

build-jdbc:
	$(exige-java)
	cd $(JDBC_DIR) && ./mvnw -B -DskipTests package

# Só os unitários: não sobem servidor, não pedem porta, rodam em qualquer
# máquina com JDK. Os de integração contra o binário real ficam no test-jdbc.
test-jdbc-unit:
	$(exige-java)
	cd $(JDBC_DIR) && ./mvnw -B test

# Integração de VERDADE: sobe o binário com TLS e credencial, ingere um corpus
# com um campo de cada tipo e roda o driver contra ele. É o alvo que o CI chama,
# e o que separa "o driver faz o que eu achei que o servidor emite" de "o driver
# fala com o servidor".
#
# Ele seta NISHIDB_IT_REQUIRED=1, e é isso que torna impossível o pior desfecho:
# harness quebrado com suíte verde e zero teste de integração executado.
test-jdbc:
	$(exige-java)
	./scripts/jdbc-it.sh

# scripts/nishidb-stats.py é código de produção — o painel que o operador lê
# em cima de um incidente —, e até a Task A3 o teste dele
# (nishidb-stats_test.py) só rodava pela mão de quem lembrasse de chamar
# `python3` direto: nenhum Makefile nem workflow o executava, então a
# próxima alteração no painel podia quebrar a exibição da versão sem que
# NADA acusasse. O alvo segue o molde do `lint` logo abaixo: ferramenta
# ausente FALHA alto e nomeia o que falta, não vira SKIP silencioso — é o
# mesmo defeito do débito 53 (gate que passa porque a ferramenta não está lá)
# por uma porta nova, em Python em vez de em Go.
#
# A partir do client Python (clients/python), o alvo roda DUAS suítes: o painel
# e o client. As duas com `python3` puro e ZERO `pip install` — é a mesma
# promessa do binário único aplicada ao ferramental, e é o que permite ao job
# `test` do CI chamá-lo sem etapa de instalação nenhuma. O PYTHONPATH aponta
# para o src layout do pacote, que não precisa estar instalado para ser testado.
test-python:
	@if command -v python3 > /dev/null; then \
		python3 scripts/nishidb-stats_test.py && \
		PYTHONPATH=$(PY_DIR)/src python3 -m unittest discover -s $(PY_DIR)/tests -p '*_test.py'; \
	else \
		echo "erro: python3 não está no PATH — os testes Python NÃO rodaram." >&2; \
		exit 1; \
	fi

# ---------------------------------------------------------------- client Python
#
# O irmão do driver JDBC: artefato de CLIENTE, não do engine. Zero dependência
# de runtime (só a stdlib), cobrado por internal/gates/python_test.go.
PY_DIR := clients/python

define exige-python
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "erro: python3 não está no PATH — o client Python exige Python 3.10 ou mais novo." >&2; \
		echo "  Ubuntu/Debian: apt install python3" >&2; \
		exit 1; \
	fi
endef

# Integração de VERDADE: sobe o binário com TLS e credencial, ingere um corpus
# com um campo de cada tipo e roda o client contra ele. Mesma doutrina do
# test-jdbc, inclusive o NISHIDB_IT_REQUIRED=1 que torna impossível o pior
# desfecho: harness quebrado com suíte verde e zero teste de integração rodado.
test-python-it:
	$(exige-python)
	./scripts/python-it.sh

# Lint e tipagem do client. Exigem as ferramentas do extra `dev` instaladas, e
# por isso NÃO entram no `test-python`, que tem de rodar sem instalar nada.
lint-python:
	$(exige-python)
	cd $(PY_DIR) && python3 -m ruff check . && python3 -m ruff format --check . && python3 -m mypy

# Os dois tetos vão desligados também aqui, e a duplicação com o `issues:` do
# .golangci.yml é deliberada: o modo de falha deste gate é o SILÊNCIO — linter
# estourado no teto absorve apontamento novo sem mexer no total —, e `make lint`
# é o comando que a doutrina do projeto cita. Repetir a intenção na linha de
# comando custa dois flags e garante que quem roda o alvo vê o número real, não
# o truncado. Qual é o número corrente NÃO se escreve aqui: a fonte é
# .golangci-baseline, que é o que o CI lê, e todo PR que fecha um apontamento o
# abaixa. Comentário com o número dentro diverge do arquivo no PR seguinte — foi
# o que aconteceu com o "390" que estava nesta linha. O porquê longo está no
# .golangci.yml.
#
# O ramo `else` FALHA, e falha alto. Ele já caiu para `go vet ./...` devolvendo
# código 0, e essa é a outra metade da cegueira do débito 53: um gate que passa
# quando a ferramenta está AUSENTE não é gate, é carimbo — foi assim que o
# projeto atravessou o M1 achando que tinha lint, porque a única pista era uma
# linha de aviso rolando no meio da saída. `go vet` também não substitui o que
# se perde: ele cobre um subconjunto de `govet` e não conhece `errcheck`,
# `revive` nem `staticcheck`, que sozinhos respondem por quase toda a linha de
# base — enquanto o `govet` não responde por NENHUM deles. O fallback trocava o gate por uma checagem que hoje não acusa nada.
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
