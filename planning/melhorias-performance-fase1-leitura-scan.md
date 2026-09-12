# Plano — Melhorias de Performance Fase 1 (Caminho de Leitura/Scan)

## Contexto

O projeto já tem gates de medida (allocs/vazão) e teto de memória rígido (8M
entidades < 1 GiB RSS). As oportunidades reais **não são de alocação** (o
caminho feliz já é zero-aloc), mas de **syscalls**, **varreduras redundantes**
e **locks/contenção**. Esta fase cobre o caminho de leitura/scan.

## Itens

### 1.1 — Eliminar duplicação de liveness no full scan (maior retorno)

**Problema:** no full scan, `liveIterator.Next` (store.go:2231-2260) já resolve
`locs.Get` + `prazoVencido` + `purgada` para decidir liveness. Depois
`GetByRowID` (store.go:1413-1438) **repete** exatamente essas três + `aposMu.RLock`
+ `entra/sai`. Cada linha viva paga 2× o trabalho.

**Solução:**
- `liveIterator` expõe `Packed() uint64` (location já resolvida).
- Novo `Store.GetByRowIDPacked(id, packed, buf)` que pula `locs.Get`/
  `prazoVencido`/`purgada` (já decididos) e mantém `aposMu.RLock` +
  `ReadRecord` + checagem `rec.RowID == id`.
- `candidateSource` (candidate.go:33-46) usa o caminho packed quando a fonte é
  full scan.
- Sem mudança de semântica; `aposMu` permanece (barreira de aposentadoria).

**Testes:** `TestGetByRowIDAndLiveness` (fakestorage_test.go:425),
`TestScanLiveAscending` (fakestorage_test.go:493); novo teste de que
`GetByRowIDPacked` respeita `aposMu` (rotação de segmento durante scan).
Bench: `internal/query/bench_test.go` (full scan).

### 1.2 — Batch de locks por candidato (agressivo: `vida` + `aposMu`)

**Problema:** cada candidato indexado paga `vida.RLock` + `aposMu.RLock` +
`locs.mu.RLock` (store.go:1414-1421).

**Solução:**
- `Store` expõe `AbreLote()`/`FechaLote()` que seguram `vida.RLock` +
  `aposMu.RLock` por lote.
- `candidateSource` adquire por lote de 64 candidatos (cadência `BatchSize`),
  em vez de por linha.

**Risco documentado:** alarga a seção crítica contra rotação de segmento; lote
pequeno (64) mitiga. Teste específico de rotação durante scan.

### 1.3 — `Coleta` sem reset de todos os slots por linha

**Problema:** `predicado.go:41-44` zera `Present` de **todos** os slots por
linha, mesmo os que a query não toca. Varredura O(nSlots) por linha.

**Solução:** esquema de geração/epoch. `Slot` ganha `gen uint32`; `Present`
derivado de `gen == genAtual` (epoch incrementado por linha). Elimina a
varredura de reset. Atualizar consumidores (`Evaluate`, executor). Garantir
zero-alocação (gates).

**Testes:** `mecanica_test.go` e gates de alloc (`alloc_gate_test.go`).

### 1.4 — Batch de `pread` no fetch de candidatos

**Problema:** `segment.go:323-371` faz um `pread` de 4 KiB por record.

**Solução:** variante batch de `ReadRecord` que lê N records contíguos num
único `pread`. Agrupa só quando `seg`+`off` contíguos; fallback para `pread`
individual.

## Ordem de execução (commits atômicos)

1. **1.1** — `GetByRowIDPacked` + `liveIterator.Packed()` + `candidateSource`.
2. **1.3** — geração/epoch no `Coleta`.
3. **1.2** — `AbreLote`/`FechaLote` + batch de locks.
4. **1.4** — batch de `pread`.

Cada item: commit com teste de regressão + docs/`§` no mesmo commit.

## Validação

- `make test` + `make test-race` verdes; `make lint` sem tetos novos (ratchet
  bidirecional).
- Benchmarks: `internal/query/bench_test.go` (full scan) e
  `internal/query/executor/topn_bench_test.go`; comparar antes/depois.
