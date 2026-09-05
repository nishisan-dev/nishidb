# Contadores mantidos na escrita — plano de implementação

> **Para quem executa:** SUB-SKILL OBRIGATÓRIA: `superpowers:subagent-driven-development`
> (recomendada) ou `superpowers:executing-plans`, tarefa a tarefa. Os passos usam
> `- [ ]` para acompanhamento. Ao aprovar, copiar este arquivo para
> `planning/contadores-mantidos-na-escrita-plano.md` (convenção do projeto) e
> trabalhar em worktree (`superpowers:using-git-worktrees`, entrar no worktree
> ANTES de criar a branch `feature/contadores-mantidos-na-escrita`).

## Estado da execução (04/09/2026)

Executado inteiro, na ordem do plano, na branch
`feature/contadores-mantidos-na-escrita`, um commit por tarefa. O aceite com os
números e as mutações está em [`docs/aceite-contadores.md`](../docs/aceite-contadores.md).

| Bloco | Estado | Validação |
|---|---|---|
| **A1** `Denso32` e atômicos de 32 bits | **feito** (`ff55597`) | o `denseFile` recusa a abertura com o outro tamanho de entrada; gate de alocação com contraparte sob `-race` |
| **A2** chave composta no `valuekey` | **feito** (`6820e25`) | golden de bytes; `ab\|c` ≠ `a\|bc` |
| **A3** `AvaliadorDePayload` e `CompilaCondicao` | **feito** (`b03a21a`) | as regras de TTL passaram a compilar pelo mesmo caminho sem editar um teste; gate de alocação |
| **B1** dicionário de grupos | **feito** (`3782d22`) | mutação do fsync: vermelha, revertida, verde |
| **B2** vetor, dobra e transição | **feito** (`9eb4422`) | três mutações do plano mais a do early-return (aplicada no aceite: quatro testes vermelhos, contra o "verde" que o plano previa) |
| **B3** seção do manifest | **feito** (`4ea5fdb`) | tag **`0x0001`, ignorável** — a tabela "Decisões" venceu o mapa de arquivos, que ainda diz `0x8005`; dataset sem contadores gera os mesmos bytes |
| **C1** abertura, `applyRecord` 7c, checkpoint | **feito** (`129b4ad`) | crash converge nas duas variantes (sem checkpoint, e checkpoint interrompido depois do msync); duas mutações |
| **C2** gate de alocação do `Put` real | **feito** (`31d4d37`) | 1,0 sem contadores, 1,0 com dois — o primeiro teto sobre o `Put` real |
| **C3** auditoria sob snapshot | **feito** (`707a073`) | duas mutações; a cópia fora do `wmu` precisou de seam determinístico, e isso está registrado |
| **C4** aposentadoria por `refs == 0` | **feito** (`b24e3ff`) | duas mutações; a de `total` no lugar de `refs` só aparece com o instante de esvaziamento ignorado junto |
| **D1** bloco `counters:` no schema | **feito** (`b6a4fef`) | tabela de recusas, um caso por linha |
| **D2** compilação das definições | **feito** (`a677cbc`) | recusa `merge: sum` no `where` e no `groupBy` |
| **E1** tabelas virtuais no motor | **feito** (`08da307`) | `_counters` corre pelo pipeline com métricas, top-N e orçamento |
| **E2** `_counters.<nome>` | **feito** (`480c3c2`) | as consultas da proposta; zero alocação por linha (gate) |
| **E3** servidor e CLI | **feito** (`3811a53`) | todo chamador de `storage.Open` passa o callback; a irmã não |
| **F1** métricas e `/v1/stats` | **feito** (`ea53dd2`) | oito séries nascem com zero, mesma estrutura nos dois lados |
| **F2** rotas ADMIN | **feito** (`1dc6d4b`) | fail-closed: 403 no loopback, 401/403 sem credencial ou com READ |
| **F3** `nishidb counters` | **feito** (`b4f5b5a`) | exit 3 na divergência, 5 na inconclusiva, local e remoto |
| **G1** documentação | **feito** (`021603e`) | diagrama validado pelo servidor (o validador discrimina); débitos 107–111 |
| **G2** bancada e aceite | **feito** | em regime, dois contadores custam +0,5 µs por evento com zero alocação; carga fria paga ≈ 0,6 ms por grupo novo; dobra 38–52 ms por 1M linhas; reconstrução 0,8 s por 1M por contador. Ponta a ponta com o binário sobre a amostra: sete passos verdes, exceto o SIGTERM no meio da auditoria, que em 1 010 linhas não tem janela (coberto por `TestAuditoriaRespeitaOContexto`) |
| **G3** fechamento | **feito** | `make test`, `make test-race`, `make lint` (baseline igualada), `make test-integration`, `make build`; `git status` sem resíduo de mutação |

**Fora de escopo, como o plano fixou:** mudanças no produtor, calibração do
prazo de 48 h, coluna `refs` na tabela, `_counters` no `/v1/metadata`,
reciclagem de ids.

**Objetivo:** saber, sem varrer o dataset, quantas entidades vivas satisfazem uma
condição, agrupadas por uma chave declarada no YAML — o contador é alimentado pela
TRANSIÇÃO de estado de cada linha dentro do `applyRecord`, lido por SQL em
`_counters.<nome>`, auditado por varredura sob snapshot, e esquece grupos vazios
por `refs == 0` (nunca por `total == 0`).

**Arquitetura:** por contador, um vetor denso `rowId → (condição | groupId)` em
mmap (4 bytes por linha, cópia do `denseFile` da `rowindex`) mais um dicionário de
grupos append-only com fsync por entrada (cópia do `fielddict`); os totais vivem em
heap e são DOBRA do vetor na abertura, antes do replay — é isso que torna o
contador consistente através de crash sem depender do estado anterior que o replay
não tem. A tabela `_counters.<nome>` é um Store virtual compilado do mesmo YAML de
dataset que o `_meta` usa, registrado no motor sob namespace.

**Spec:** `planning/contadores-mantidos-na-escrita.md` (a proposta que este plano
implementa; as decisões que ela deixou em aberto estão fechadas na seção
"Decisões" abaixo).

**Stack:** Go 1.25+ (CI pina 1.26.5), `yaml.v3`, mmap/fallocate Linux; nada novo.

## Contexto

O caso motivador é externo (plataforma de accounting RADIUS): o alarme de queda de
autenticação precisa de "quantos serviços existem por OLT" e "quantos estão
autenticados por OLT", a cada minuto. Uma varredura completa custa 17 s em 8M
linhas e segura um snapshot de leitura por esse tempo; `GROUP BY` continua recusado
por decisão de produto. A proposta é um **índice derivado**: mantido na escrita,
lido em tempo constante, com deriva detectável por auditoria. A forma óbvia (somar
por mensagem) foi refutada na proposta porque Kafka entrega pelo menos uma vez e
`merge: sum` já é declaradamente não idempotente.

### O que a leitura do código estabeleceu (e mudou no desenho)

- **Refutado: "o replay reaplica a ponta e o contador volta ao ponto certo
  sozinho" (proposta §6.2, momento 4).** No replay, `prevPayload` só existe quando
  a location está estritamente ATRÁS do cursor
  ([store_recovery.go:411-424](internal/storage/store_recovery.go:411)); no crash
  típico ela já está à frente e o estado anterior é `nil`. Consequência: o
  contador **não pode** ser mantido por delta sobre o payload antigo. O delta sai
  do próprio vetor denso (`antiga := vetor[rowID]`), e os totais são dobra do vetor
  na abertura, ANTES do replay. Com isso `totals == fold(vetor)` vale
  independentemente de a página do vetor ter sido descarregada antes do crash.
- **Refutado: "existe gate que quebra se alocar" no write path real.**
  `TestGateAllocsIngestFake` mede o dublê; `BenchmarkApplyIngestReal` é benchmark
  sem teto ([gates_test.go:98-121](benchmark/micro/gates_test.go:98)). Um gate
  sobre o `Put` real é pré-requisito (tarefa C2).
- **Refutado: CLAUDE.md diz que há fase de CLEAR depois do commit.** Não há desde a
  §5.2 do subsistema 1 ([store.go:1924-1931](internal/storage/store.go:1924)). O
  contador entra ao lado da agenda (7b), depois do commit, e o CLAUDE.md é
  corrigido na tarefa G1.
- **Confirmado: a lápide passa pelo `applyRecord` com payload reduzido** (só
  `_writtenAt`, [expiracao.go:171-182](internal/storage/expiracao.go:171)); o
  copy-forward da compaction e a expiração oportunista também passam por ele
  ([compaction.go:217-231](internal/storage/compaction.go:217)). Os três caminhos
  de saída de cena (sobrescrita, expiração, compaction) convergem no mesmo ponto.
- **Confirmado: o roteamento de namespace virtual é um único `if` em
  [engine.go:95-97](internal/query/engine.go:95)**, e `_meta` compila a tabela
  virtual de um YAML de dataset ([meta.go:113-193](internal/query/meta/meta.go:113)).
  O `_counters` entra por um registro genérico de tabelas virtuais no motor.
- **Confirmado: a key da v1 é uma coluna string única**, e o planner só faz
  `PointLookup` sobre ela. Um `groupBy` composto não tem coluna que identifique a
  linha; por isso toda tabela `_counters.<nome>` ganha a coluna `groupKey` (texto
  canônico do grupo) como key, ao lado das colunas abertas e tipadas do `groupBy`.
- **Confirmado: `valuekey.Append` codifica UM valor sem prefixo de tamanho**
  ([valuekey.go:58-69](internal/index/valuekey/valuekey.go:58)); concatenar
  componentes seria ambíguo. A chave composta ganha codificação própria no mesmo
  pacote (dono único), com prefixo de tamanho por componente e golden de bytes.
- **Confirmado: `schema.Compile` precisa do dicionário que o `storage.Open` cria, e
  o replay precisa das definições dos contadores.** A saída é um callback em
  `storage.Options` chamado DEPOIS de `_writtenAt`/`_expiresIn`/indexados serem
  registrados ([store.go:1128-1193](internal/storage/store.go:1128)) e ANTES do
  ramo de recovery ([store.go:1195](internal/storage/store.go:1195)).
- **Confirmado: uma coluna chamada `group` não é problema** (o lexer não a
  reconhece; `TestColunaChamadaOrderContinuaValida`). As colunas do `groupBy`
  saem com os nomes reais por legibilidade, não por restrição.
- **Refutado (pela validação adversarial): um vetor gravado em place fica à
  frente do log durável numa cauda rasgada.** `escritas`/`prazos`/agenda aceitam
  isso porque o resíduo deles é benigno na leitura; no contador seria número
  publicado errado. O vetor ganha OVERLAY em heap como o `Locations`
  ([rowindex.go:120-232](internal/storage/rowindex/rowindex.go:120)): `Publish`
  no `applyRecord`, `ApplyOverlay` no checkpoint DEPOIS do `segSync` — a página
  do vetor nunca referencia transição de record não durável.
- **Refutado: `ScanLive(snap)` + `GetByRowID(snap)` servem à auditoria.** O
  iterador lê a location CORRENTE e o bit de lápide corrente
  ([store.go:2259-2275](internal/storage/store.go:2259)), não o journal; e a
  visibilidade filtra por prazo vencido, que o contador (por definição) só
  desconta quando a LÁPIDE é gravada. A auditoria ganha enumerador próprio no
  pacote `storage`: `vers.Em(id, snap.Epoca)` senão `locs.Get(id)`, pula só
  ausente/lápide, lê o record sob `aposMu.RLock`. "Viva" para o contador é "com
  location e sem lápide", e isso fica escrito.
- **Refutado: aposentar por `refs == 0` + prazo basta.** Se a transição que
  esvaziou o grupo estiver numa cauda rasgada, o estado durável ainda aponta
  para um grupo aposentado. Regra adicional: aposentar só se a última mexida do
  grupo já foi coberta por um checkpoint (`ultimaMexida[g] <= mexidasCobertas`).
- **Refutado: a seção do manifest deve ser crítica.** Como "seção ausente ⇒
  reconstruir" já é o comportamento certo, uma tag crítica só serviria para
  bloquear o rollback do piloto (`nishidb-update.sh`). A tag é IGNORÁVEL
  (`< 0x8000`): o binário anterior abre o dataset, ignora a seção, e o binário
  novo reconstrói ao voltar.
- **Confirmado: `schema.Compile` chamado entre `resolveCamposIndexados` e o
  ramo de recovery produz os mesmos FieldIDs de hoje** — o dicionário está no
  estado exato em que o `Open` o devolve; nada entre esses pontos toca nomes.

## Decisões (fecham a seção 11 da proposta)

| Item | Decisão |
|---|---|
| 11.3 backfill | **Reconstruir na abertura.** Contador novo, ou com `groupBy`/`where`/`maxGroups` diferentes do manifest, ou cujo vetor nasceu/não cobre as linhas, é reconstruído varrendo as linhas vivas, com log `contador.construido` nomeando motivo e duração. Contador removido do YAML tem os arquivos apagados com log `contador.removido`. |
| 11.6 auditoria | **Rota ADMIN síncrona sob snapshot.** `POST /v1/counters/audit` congela os totais e abre o snapshot no mesmo instante (sob `wmu` por microssegundos), varre sem `wmu`, compara. `POST /v1/counters/rebuild` reconstrói (segura `wmu` pela varredura — documentado). CLI `nishidb counters` com `--server` (rotas) ou `--config` (offline, flock). |
| 6.5 ciclo de vida | **Entregue neste ciclo (bloco C5).** Grupo com `refs == 0` há mais de `retainEmptyGroups` é APOSENTADO: registro no dicionário (fsync), id nunca reemitido, some do `_counters`. Coletor próprio por varredura linear dos grupos, a cada minuto. |
| 11.2 defaults | `maxGroups: 65536` (declarável até 2^31−1), `retainEmptyGroups: 7d` (aceita `never`). Teto atingido em runtime: a linha entra, o contador a trata como "sem grupo", `nishidb_counter_groups_refused_total` sobe e um WARN sai uma vez por contador por subida. |
| tipos do `groupBy` | `string`, `int64`, `boolean`, com tipo DECLARADO (não `auto`), coluna de topo ou extraída, nunca a key nem a sequence. Outros tipos são recusados na subida com mensagem nomeando os três. |
| coluna key da tabela | `groupKey` (string): componentes na ordem do `groupBy` separados por `\|`; string escapa `\` → `\\` e `\|` → `\\|`; int64 decimal; boolean `true`/`false`. Um componente só sai sem separador. |
| snapshot | `_counters` lê o PRESENTE: os totais não são versionados pelo journal do snapshot. Documentado na arquitetura e no README. |
| durabilidade do vetor | Overlay em heap por contador (`Publish` no `applyRecord`, `ApplyOverlay` no checkpoint depois do `segSync`): a página do vetor nunca fica à frente do log durável, e `totals == fold(vetor)` vale em qualquer janela de crash. Custo: um mapa bounded pelo intervalo de checkpoint, como o das locations. |
| "viva" para o contador | Linha com location e SEM lápide. Linha com prazo vencido e ainda sem lápide continua contada até o coletor chegar (a proposta §5.5 já assume isso: "o total cai quando o coletor chega"). A auditoria usa essa definição, não a de visibilidade da consulta. |
| reconstrução | Decidida ANTES da recovery a partir do manifest (seção ausente, colunas ou `where` diferentes, vetor nascido com linhas, vetor que não cobre `NextRowID`). Reconstruir recria os DOIS arquivos (dicionário e vetor): chaves de outras colunas não podem poluir um dicionário cujos ids nunca são reemitidos. Mudança de `maxGroups` NÃO reconstrói — só é recusada se menor que os grupos em uso. |
| manifest | Seção IGNORÁVEL (`tagContadores = 0x0001`): nome, colunas, `where` por contador. |
| aposentadoria segura | Aposentar exige `refs == 0`, prazo vencido E `ultimaMexida[g] <= mexidasCobertas` (contador monotônico de transições; `mexidasCobertas` é gravado no checkpoint, sob `wmu`, depois do `Msync` do vetor). |
| histórico | Dataset em `ModoHistorico` recusa `counters:` na subida (a retenção purga por marca-d'água sem `applyRecord`; o contador nunca decrementaria). |
| sem § novo | Como o `_meta`, o contador nasce sem seção na spec da raiz; `specsDoMarcador` não muda. |

## Restrições globais

- Repositório inteiro em PT-BR: identificadores, godoc, testes, logs, commits.
- Zero alocação por evento no caminho quente: buffers do writer com `buf[:0]` e
  reatribuição do retorno; `m[string(b)]` para lookup em mapa sem alocar.
- Ordem de locks `recPassadaMu → retPassadaMu → vida → wmu → aposMu`; `snapMu`,
  `selMu` e o lock do dicionário de grupos são FOLHA. Nada tomado sob folha toma
  `wmu`.
- Métricas: nomes só por constantes de `internal/metrics/nomes.go`; `Familia` e
  `Nome` por constante; label nunca vem de entrada do cliente; série que alarma
  nasce com zero.
- Gates que continuam verdes sem edição: `TestCitacoesDeSecaoResolvem`,
  `TestAmostrasDeMetricaTemFamiliaENome`, `TestQuickstartDoREADMEExecuta`
  (não inserir bloco ```` ```console ```` ou ```` ```yaml ```` antes da linha 845
  do README), `TestTodoCodigoDoProtocoloTemSqlStateNoDriver` e
  `TestTodoCodigoDoProtocoloTemExcecaoNoClientPython` (nenhum código de erro novo
  no wire), gate de lint (`.golangci-baseline` igualado no mesmo commit, nunca
  escrito em prosa).
- Doutrina de verificação: toda invariante nova tem mutação aplicada (vermelho),
  revertida (verde), e os dois relatados no commit/aceite.
- `make test` E `make test-race` antes de cada commit; `make lint` com baseline
  igualada.

## Mapa de arquivos

| Arquivo | Responsabilidade |
|---|---|
| `internal/storage/mmap/region.go` | `AtomicLoad32`/`AtomicStore32` (único pacote com unsafe) |
| `internal/storage/rowindex/rowindex.go` | `denseFile` com tamanho de entrada parametrizado; `Denso32` exportado |
| `internal/index/valuekey/valuekey.go` | `AppendComposta`/`DecodeComposta` + golden |
| `internal/predicado/avaliador.go` (novo) | `AvaliadorDePayload`: programa + slots + reader, `Avalia(payload)` |
| `internal/predicado/regra.go` | `CompilaCondicao` extraída de `CompilaRegras` |
| `internal/storage/contador/` (novo) | `dicionario.go`, `contador.go`, `chavetexto.go`, `auditoria.go`, `aposentadoria.go` + testes |
| `internal/storage/checkpoint/manifest.go` | tag 0x8005, `ContadorInfo`, `Manifest.Contadores` |
| `internal/storage/store.go` | `Options.CompilaContadores`, abertura, passo 7c do `applyRecord`, Close |
| `internal/storage/contadores.go` (novo) | `adotaOuConstroiContadores`, reconstrução, auditoria sob snapshot, laço de aposentadoria, estatísticas |
| `internal/storage/store_checkpoint.go` | `traceStep("msyncContadores")` |
| `internal/schema/contador.go` (novo) | `ContadorSpec`, parse do bloco `counters:`, validação |
| `internal/schema/spec.go` | `rawSpec.Counters`, `DatasetSpec.Contadores`, mensagem do prefixo `_` |
| `internal/contadores/` (novo) | `compila.go` (spec → `contador.Definicao`), `tabela.go` (schema sintético + fonte virtual), `abertura.go` (callback) |
| `internal/query/engine.go` | `RegistraVirtual` + roteamento por namespace |
| `internal/query/meta/meta.go` | mensagem de namespace desconhecido lista os existentes |
| `internal/server/server.go`, `historico.go` | wiring do callback e do registro das tabelas |
| `internal/server/contadores.go` (novo) | rotas `/v1/counters/audit` e `/v1/counters/rebuild` |
| `internal/server/metricas_dataset.go`, `documentos.go` | séries e `/v1/stats` da mesma estrutura |
| `internal/metrics/nomes.go` | constantes novas + `LabelCounter` |
| `internal/protocolo/contadores.go` (novo), `documentos.go` | tipos de wire |
| `internal/cli/counters.go` (novo), `cli.go` | subcomando, uso, exit codes |
| `README.md`, `docs/architecture.md`, `docs/diagrams/contadores_flow.puml`, `docs/debitos.md`, `docs/aceite-contadores.md`, `CLAUDE.md`, `.gitignore` | documentação e aceite |

---

## Bloco A — Fundações de formato

### Tarefa A1: `Denso32` na `rowindex` (vetor denso de 4 bytes)

**Arquivos:**
- Modificar: `internal/storage/mmap/region.go` (ao lado de `AtomicLoad64`, linha 269)
- Modificar: `internal/storage/rowindex/rowindex.go:15-118`
- Criar: `internal/storage/rowindex/denso32.go`
- Testar: `internal/storage/mmap/region_test.go`, `internal/storage/rowindex/denso32_test.go`

**Interfaces produzidas:**
```go
// mmap
func (r *Region) AtomicLoad32(off int64) uint32
func (r *Region) AtomicStore32(off int64, v uint32)
// rowindex
func OpenDenso32(path string, magic [4]byte, minRows uint64) (*Denso32, error)
func (d *Denso32) Get(rowID types.RowID) (v uint32, gravado bool) // false além do arquivo
func (d *Denso32) Set(rowID types.RowID, v uint32) error          // writer-only, cresce por doubling
func (d *Denso32) Nasceu() bool
func (d *Denso32) Cobre(limite types.RowID) bool                  // arquivo tem entrada para limite-1
func (d *Denso32) Zera() error                                    // memset das entradas (reconstrução)
func (d *Denso32) Msync() error
func (d *Denso32) Close() error
func (d *Denso32) SizeBytes() int64
```

- [ ] **Passo 1: teste do atômico de 32 bits no mmap**

```go
func TestAtomicos32EnderecamMeiaWord(t *testing.T) {
	r := abreRegiaoDeTeste(t, 4096) // helper já existente no pacote; se não houver, mmap.Open num t.TempDir()
	r.AtomicStore32(64, 0xAABBCCDD)
	r.AtomicStore32(68, 0x11223344)
	if got := r.AtomicLoad32(64); got != 0xAABBCCDD { t.Fatalf("off 64: %#x", got) }
	if got := r.AtomicLoad32(68); got != 0x11223344 { t.Fatalf("off 68: %#x", got) }
	// As duas meias-words compõem a word de 64 em little-endian: prova que o
	// endereçamento de 32 não pisa na vizinha.
	if got := r.AtomicLoad64(64); got != 0x11223344AABBCCDD { t.Fatalf("word: %#x", got) }
}
```

- [ ] **Passo 2: rodar e ver falhar** — `go test ./internal/storage/mmap/ -run TestAtomicos32 -count=1` → falha de compilação (`AtomicStore32` indefinido).

- [ ] **Passo 3: implementar em `region.go`** (mesmo padrão de `word`/`AtomicLoad64`: `RLock`, bounds check exigindo múltiplo de 4, `exigeMutavel`, `atomic.LoadUint32((*uint32)(unsafe.Pointer(&r.data[off])))`). Godoc: "existe para o vetor de grupos do contador (4 bytes por linha); a agenda e os demais continuam em 64".

- [ ] **Passo 4: verde** — `go test ./internal/storage/mmap/ -count=1`.

- [ ] **Passo 5: parametrizar `denseFile`** — `entrySize` deixa de ser const global: vira campo `entry int64` do `denseFile`; `openDense(path, magic, minRows, entry)`; `entryOff` vira método; `load/store` ganham as variantes de 32 quando `entry == 4`. Os cinco tipos existentes passam `8`. O byte 5 do header já grava o tamanho, então um arquivo de 8 aberto como 4 (ou vice-versa) é recusado pela checagem que já existe (`rowindex.go:83`).

- [ ] **Passo 6: teste do `Denso32`**

```go
func TestDenso32GravaLeECresce(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenDenso32(filepath.Join(dir, "g.idx"), [4]byte{'T','S','T','1'}, 512)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	if !d.Nasceu() { t.Fatal("arquivo novo tem de nascer") }
	if _, ok := d.Get(10); ok { t.Fatal("entrada nunca gravada tem de sair gravado=false") }
	if err := d.Set(100_000, 1<<31|7); err != nil { t.Fatal(err) } // além do piso: cresce
	if v, ok := d.Get(100_000); !ok || v != 1<<31|7 { t.Fatalf("got %v %v", v, ok) }
	if !d.Cobre(100_001) || d.Cobre(1<<40) { t.Fatal("Cobre errado") }
	if err := d.Zera(); err != nil { t.Fatal(err) }
	if v, _ := d.Get(100_000); v != 0 { t.Fatal("Zera não zerou") }
}

func TestDenso32RecusaArquivoDeOitoBytes(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenEscritas(filepath.Join(dir, "x.idx"), 512); e.Close()
	if _, err := OpenDenso32(filepath.Join(dir, "x.idx"), escritasMagic, 512); err == nil {
		t.Fatal("arquivo com entrySize 8 aberto como 4 tem de ser recusado")
	}
}
```

- [ ] **Passo 7: vermelho → implementar `denso32.go` → verde.** `Zera` usa `WithBytes` em blocos, sem alocar o arquivo inteiro.

- [ ] **Passo 8: gate de alocação**: `TestDenso32NaoAlocaNoCaminhoQuente` (`//go:build !race`, `testing.AllocsPerRun(1000, …)` sobre `Get`+`Set` em entrada já coberta; teto 0). Contraparte `_race.go` com `t.Skip` explícito.

- [ ] **Passo 9: suíte do pacote + lint + commit**

```bash
go test ./internal/storage/... -count=1 && make lint
git add internal/storage/mmap internal/storage/rowindex
git commit -m "rowindex: vetor denso de 4 bytes por linha (Denso32) e atômicos de 32 bits no mmap"
```

### Tarefa A2: chave composta no `valuekey`

**Arquivos:**
- Modificar: `internal/index/valuekey/valuekey.go`, `valuekey_test.go`

**Interfaces produzidas:**
```go
// AppendComposta grava, para cada componente, `u16 LE tamanho | chave simples`.
// dst é do chamador e o retorno tem de ser reatribuído (mesmo contrato do Append).
func AppendComposta(dst []byte, vs []types.Value) ([]byte, error)
// DecodeComposta decodifica em dst (reusado) os componentes; os Bytes das strings
// apontam para dentro de chave (zero-copy).
func DecodeComposta(chave []byte, dst []types.Value) ([]types.Value, error)
```

- [ ] **Passo 1: golden de bytes** — em `TestChaveCanonicaEhGoldenDeDisco` adicionar uma tabela irmã `TestChaveCompostaEhGoldenDeDisco` com hex literal:

```go
{"olt só", []types.Value{str("A")}, "0200" + "0941"},
{"olt,slot,port", []types.Value{str("A"), i64(1), i64(9)}, "0200 0941" + "0900 010100000000000000" + "0900 010900000000000000"},
{"ambiguidade: ab|c ≠ a|bc", …} // os dois hex têm de ser diferentes
```
(sem espaços no literal; `str`/`i64` são helpers do teste). Mais `TestDecodeCompostaDesfazOAppend` (round-trip para string/int64/bool) e `TestAppendCompostaRespeitaOBufferDoChamador` (ponteiro e cap iguais, como o existente).

- [ ] **Passo 2: vermelho → implementar → verde.** `AppendComposta` chama `Append` por componente num scratch interno? NÃO: para não alocar, grava o prefixo de 2 bytes como placeholder, chama `Append(dst, v)`, e volta para preencher o tamanho. `DecodeComposta` lê `[tipo]` e decide: int64/bool → 8 bytes LE em `I64`; string → resto do componente em `Bytes`.

- [ ] **Passo 3: commit** — `valuekey: chave composta com prefixo de tamanho por componente, golden de disco`.

### Tarefa A3: `AvaliadorDePayload` no `predicado`

**Arquivos:**
- Criar: `internal/predicado/avaliador.go`, `avaliador_test.go`
- Modificar: `internal/predicado/regra.go:77-120` (extrair `CompilaCondicao`)

**Interfaces produzidas:**
```go
// CompilaCondicao resolve uma condição solta pelo MESMO caminho do WHERE e das
// regras de TTL: parse, bind contra o schema, recusa de campo de sistema, programa.
func CompilaCondicao(rotulo, condicao string, sc *schema.Compiled) (*Programa, binder.Slots, error)

// AvaliadorDePayload avalia um programa direto sobre um payload. Tem scratch
// próprio (slots + reader) e por isso é de UM dono: o writer usa o seu, a
// auditoria instancia outro.
type AvaliadorDePayload struct{ prog *Programa; slots []Slot; fr encoding.FieldReader }
func NovoAvaliadorDePayload(prog *Programa, ids []types.FieldID) *AvaliadorDePayload
func (a *AvaliadorDePayload) Avalia(payload []byte) (bool, error)
```

- [ ] **Passo 1: teste** — compila `authenticated = true AND slot > 0` contra um `schema.Compiled` de teste (o pacote já tem helper para regras; reusar), monta payload com `encoding.AppendField`, afirma true/false, e afirma que campo ausente compara false. `TestCompilaCondicaoRecusaCampoDeSistema` (`_writtenAt`), reaproveitando o caso que `CompilaRegras` já cobre.

- [ ] **Passo 2: vermelho → implementar → verde.** `CompilaRegras` passa a chamar `CompilaCondicao(fmt.Sprintf("ttl[%d]", i), r.When, sc)` — os testes de regra continuam verdes sem edição.

- [ ] **Passo 3: gate** — `TestAvaliadorDePayloadNaoAloca` (`AllocsPerRun`, teto 0, `!race`).

- [ ] **Passo 4: commit** — `predicado: avaliador sobre payload e compilação de condição compartilhada com as regras de TTL`.

---

## Bloco B — O motor do contador (`internal/storage/contador`)

### Tarefa B1: dicionário de grupos

**Arquivos:**
- Criar: `internal/storage/contador/dicionario.go`, `dicionario_test.go`

**Formato de `grupos.dict`** (cópia deliberada do `fielddict` v2): header 8 bytes
(`magic "NDBG"`, version 1); entradas `[u16 len][len bytes chave composta][crc32c]`
com id implícito 1-based; registro de aposentadoria `[u16 0][u32 id][crc32c]`
(discriminado por `len == 0`, como o hint de tipo do fielddict). Toda entrada é
fsyncada ANTES de o id ser devolvido — nenhuma entrada do vetor pode apontar para
um id que a reabertura não resolva. Abertura escaneia o prefixo válido por CRC e
trunca a cauda rasgada.

**Interfaces produzidas:**
```go
type Dicionario struct{ … }
func AbreDicionario(path string, teto uint32) (*Dicionario, error)
// Interna devolve o id do grupo; cria (com fsync) se novo. ErrTetoDeGrupos quando
// o número de grupos VIVOS já é o teto. Nunca aloca quando a chave já existe
// (lookup por m[string(chave)]).
func (d *Dicionario) Interna(chave []byte) (uint32, error)
func (d *Dicionario) Chave(id uint32) (chave []byte, vivo bool)   // aposentado → vivo=false
func (d *Dicionario) PorChave(chave []byte) (uint32, bool)
func (d *Dicionario) PorTexto(texto []byte) (uint32, bool)         // groupKey → id
func (d *Dicionario) Aposenta(id uint32) error                     // registro + fsync; remove de PorChave/PorTexto
func (d *Dicionario) Limite() uint32                               // maior id emitido + 1
func (d *Dicionario) Vivos() uint32
func (d *Dicionario) Aposentados() uint32
func (d *Dicionario) SizeBytes() int64
func (d *Dicionario) Close() error
var ErrTetoDeGrupos = errors.New("contador: teto de grupos atingido")
```
O mapa chave→id e o slice de chaves são protegidos por um `sync.RWMutex` FOLHA:
o writer só toma `Lock` ao internar um grupo NOVO ou aposentar; leitores
(`_counters`, auditoria) tomam `RLock`. O texto (`groupKey`) é calculado uma vez
por grupo na internação e guardado ao lado da chave (tarefa B2 define
`TextoDaChave`).

- [ ] **Passo 1: testes**
  - `TestDicionarioInternaEReabre`: interna 3 chaves, fecha, reabre, ids e chaves iguais.
  - `TestDicionarioTruncaCaudaRasgada`: escreve meio registro no fim do arquivo à mão, reabre, `Limite()` inalterado e o arquivo encolheu.
  - `TestDicionarioAposentaENuncaReemite`: aposenta o id 2, reabre, `Chave(2)` vivo=false, `Interna(mesma chave)` devolve id 4.
  - `TestDicionarioRecusaOTeto`: teto 2, terceira chave → `ErrTetoDeGrupos`; aposentar uma libera uma vaga.
  - `TestDicionarioNaoAlocaNoLookup` (`!race`, teto 0 sobre `Interna` de chave existente e `PorChave`).
  - **Mutação obrigatória**: remover o `f.Sync()` de `Interna` e provar que um teste fica vermelho — `TestDicionarioFsyncaAntesDeDevolverOId` usa um seam `aposEscrita func()` (como `agenda.aposEscrita`) que injeta pânico entre o write e o sync, e confere na reabertura que a entrada NÃO está lá (com o sync, o pânico só pode acontecer depois dela durável).

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `contador: dicionário de grupos append-only com fsync por entrada e aposentadoria`.

### Tarefa B2: o `Contador` — vetor, dobra, transição, texto da chave

**Arquivos:**
- Criar: `internal/storage/contador/contador.go`, `chavetexto.go`, `contador_test.go`, `chavetexto_test.go`, `alloc_gate_test.go` (+ `_race`)

**Interfaces produzidas:**
```go
// Avaliador é o `where` compilado. nil = sem condição (conta toda linha com grupo).
type Avaliador interface{ Avalia(payload []byte) (bool, error) }

type Definicao struct {
	Nome        string
	Colunas     []types.FieldID   // na ordem DECLARADA do groupBy
	Nomes       []string          // nomes das colunas, mesma ordem (para a tabela virtual e mensagens)
	Tipos       []types.FieldType // string | int64 | bool, mesma ordem
	Where       string            // texto normalizado; "" = sem condição
	NovoAvaliador func() Avaliador // nil quando Where == ""
	MaxGroups   uint32
	RetainEmptyGroups time.Duration // 0 = never
}

type Contador struct{ … }

const SemGrupo uint32 = 0
const bitCondicao uint32 = 1 << 31

// Abre abre (ou cria) dicionário e vetor em dir = indexes/contadores/<nome>.
// recria=true renomeia os arquivos existentes para *.old e nasce vazio — é a
// reconstrução por divergência de parâmetros, decidida pelo Store a partir do
// manifest ANTES da recovery.
func Abre(dir string, def Definicao, minRows uint64, recria bool) (*Contador, error)
func (c *Contador) Def() Definicao
// Dobra recomputa totais e refs a partir do vetor em DISCO, pelo tamanho do
// arquivo (nextRowID ainda é zero antes da recovery). Entrada com id aposentado
// ou além do dicionário é órfã: reescrita para SemGrupo (o Open não tem
// leitores), contada em Orfas e logada em WARN. UMA vez na abertura, antes do
// replay; pulada quando recria=true.
func (c *Contador) Dobra() error
// Transiciona é o único ponto de escrita: antiga := vetor[rowID] (overlay, senão
// mmap); nova := Deriva(payload). lapide=true força nova = SemGrupo. Writer-only.
// Zero alocação com grupo existente. Incrementa o grupo novo ANTES de decrementar
// o antigo (um leitor somando nunca subestima). Roda nos dois modos do
// applyRecord — write e replay.
func (c *Contador) Transiciona(rowID types.RowID, payload []byte, lapide bool) error
// Deriva é a função pura (grupo, condição) → entrada; exposta para a auditoria,
// que a chama com scratch PRÓPRIO (av Avaliador e vals são do chamador).
func (c *Contador) Deriva(payload []byte, av Avaliador, vals []types.Value, chaveBuf []byte) (entrada uint32, chaveBuf2 []byte, err error)
// ApplyOverlay drena o overlay para o mmap. Só no checkpoint, DEPOIS do segSync.
func (c *Contador) ApplyOverlay() error
// CobreMexidas registra que todas as transições até n estão duráveis (record E
// página). Chamado no checkpoint depois do Msync, sob wmu.
func (c *Contador) CobreMexidas(n uint64)
func (c *Contador) Transicoes() uint64
// Congela devolve cópias de totais e refs e o limite de ids — para a auditoria,
// sob wmu.
func (c *Contador) Congela() (totais, refs []int64, limite uint32)
// Leitura (concorrente ao writer):
func (c *Contador) Limite() uint32
func (c *Contador) Grupo(id uint32) (chave []byte, texto []byte, total, refs int64, vivo bool)
func (c *Contador) PorTexto(texto []byte) (uint32, bool)
func (c *Contador) Estatisticas() Estatisticas
func (c *Contador) Msync() error
func (c *Contador) Close() error
func (c *Contador) Nasceu() bool
func (c *Contador) Cobre(limite types.RowID) bool

type Estatisticas struct {
	Nome string; Grupos, Aposentados, Teto uint32
	Recusados, Orfas uint64 // grupos recusados pelo teto; entradas órfãs vistas na dobra
	Bytes int64            // vetor + dicionário
}
```
Totais e refs são `[]atomic.Int64` indexados por id. Crescem por `append` SÓ
dentro de `Interna` de grupo novo, sob o `Lock` do dicionário; leitores indexam
sob `RLock` (nunca guardam o header do slice fora do lock); as `Add` do writer
em entradas existentes são atômicas e sem lock — o writer é um só, então nunca
há `append` concorrente a um `Add`. Por grupo há ainda `ultimaMexida []uint64`
(o valor de `transicoes` na última transição que o tocou) e `vazioDesde`
(tarefa C4). O overlay é `over map[types.RowID]uint32`, writer-only (não precisa
de lock: `Transiciona`, `Dobra` e `ApplyOverlay` rodam sob `wmu`); `clear(over)`
depois de drenar, para não realocar.

**Derivação por linha, sem alocar:** um `encoding.FieldReader` percorre o payload
UMA vez, por merge linear contra `colunasOrdenadas` (cópia ordenada de
`Colunas` com o índice de volta à ordem declarada), preenchendo
`c.valores []types.Value` (scratch do writer) e marcando presença; qualquer
coluna ausente ⇒ `SemGrupo` (a proposta §6.3: entidade sem a coluna não é
contada). Presentes todas: `valuekey.AppendComposta(c.chaveBuf[:0], c.valores)`,
`c.chaveBuf = chave`, `id, err := c.dict.Interna(chave)`; `ErrTetoDeGrupos` ⇒
`SemGrupo` + `recusados.Add(1)` + WARN uma vez. Condição: `c.avaliador.Avalia(payload)`
(nil ⇒ true). Delta:

```go
antiga := c.entrada(rowID) // overlay, senão mmap; além do arquivo = SemGrupo
if antiga == nova { return nil }
c.transicoes++
if g := nova &^ bitCondicao; g != SemGrupo {           // inc ANTES do dec
	c.refs[g].Add(1)
	if nova&bitCondicao != 0 { c.totais[g].Add(1) }
	c.ultimaMexida[g] = c.transicoes
	c.tocou(g) // aposentadoria (C4): limpa vazioDesde
}
if g := antiga &^ bitCondicao; g != SemGrupo && g < c.dict.Limite() { // guarda contra id órfão
	c.refs[g].Add(-1)
	if antiga&bitCondicao != 0 { c.totais[g].Add(-1) }
	c.ultimaMexida[g] = c.transicoes
	if c.refs[g].Load() == 0 { c.esvaziou(g, agora) }
}
c.over[rowID] = nova // Publish no overlay; o mmap só muda no ApplyOverlay
return nil
```

**`chavetexto.go`:** `func TextoDaChave(dst []byte, chave []byte, scratch []types.Value) ([]byte, []types.Value, error)` — renderiza `groupKey` como decidido acima. Golden: `TestTextoDaChaveEhGolden` com `A|1|9`, `a\|b|true`, `x\\y`.

- [ ] **Passo 1: testes de transição** (payloads montados com `encoding.AppendField`; o "schema" é só a lista de FieldIDs, sem YAML):
  - `TestTransicaoNovaLinhaContaNoGrupo`: refs 1, total 1 (cond true) / total 0 (cond false).
  - `TestReprocessarOMesmoEventoNaoMexe`: duas chamadas iguais; total e refs inalterados. (É a segunda linha da tabela da proposta §5.5.)
  - `TestMudarDeGrupoAjustaAsDuasPontas`: A→B: refs/total de A caem, de B sobem, no mesmo `Transiciona`.
  - `TestLapideDescontaEPreservaOGrupo`: total 1→0, refs 1→0, grupo continua `vivo` com total 0 (proposta §5.4, decisão 2).
  - `TestRessurreicaoVoltaAContar`: lápide depois Put → total 1.
  - `TestLinhaSemColunaDeAgrupamentoNaoConta`: payload sem `olt` ⇒ refs de todos os grupos inalterados.
  - `TestTetoDeGruposRecusaEConta`: teto 1; segunda OLT ⇒ `Recusados == 1`, linha entra como SemGrupo.
  - `TestDobraReproduzOsTotais`: 1.000 transições aleatórias + `ApplyOverlay` + `Msync`; `Dobra` numa segunda instância aberta sobre os mesmos arquivos devolve exatamente os mesmos totais/refs. **Este é o teste que sustenta a consistência através de crash.**
  - `TestOverlayNaoVaiAoMmapAntesDoApply`: transições sem `ApplyOverlay`; uma segunda instância sobre os arquivos dobra ZERO. Depois do `ApplyOverlay`, dobra os totais. (Mutação: `Transiciona` gravando direto no `Denso32` → vermelho.)
  - `TestDobraTrataIdAposentadoComoOrfa`: reescreve a entrada para SemGrupo e conta em `Orfas`.
  - `TestCobreMexidasAcompanhaAsTransicoes`: `Transicoes()` sobe por transição efetiva (não por reprocessamento), `CobreMexidas` é monotônico.
  - **Mutações obrigatórias, relatadas no commit**: (1) remover o decremento de `refs` na saída de grupo → `TestMudarDeGrupoAjustaAsDuasPontas` vermelho; (2) remover o `total--` da lápide → `TestLapideDescontaEPreservaOGrupo` vermelho; (3) trocar `antiga == nova` por `true` → `TestReprocessarOMesmoEventoNaoMexe` continua verde (é o esperado: a mutação equivalente é remover o early-return e ver `TestDobraReproduzOsTotais`… continuar verde também — registrar que essa mutação é inofensiva por construção, e por quê).

- [ ] **Passo 2: vermelho → implementar → verde.**

- [ ] **Passo 3: gate** — `TestContadorNaoAlocaNoCaminhoQuente`: `AllocsPerRun(10_000, Transiciona)` alternando dois grupos já internados sobre linhas já presentes no overlay (o mapa não cresce; é o mesmo regime do overlay das locations), teto 0. Contraparte `_race`.

- [ ] **Passo 4: commit** — `contador: vetor de grupos, dobra na abertura e transição por delta do próprio vetor`.

### Tarefa B3: seção do manifest

**Arquivos:**
- Modificar: `internal/storage/checkpoint/manifest.go` (consts 91-157, structs 201-246, `TamanhoDaCaudaTLV` 258-267, `appendCauda` 314-335, `decodeCauda` 439-470, novo `decodeContadores`), `manifest_tlv_test.go`, `manifest_test.go`

**Interfaces produzidas:**
```go
// tagContadores é IGNORÁVEL (< 0x8000), e a política é deliberada: a seção
// existe para o binário NOVO decidir entre adotar e reconstruir, e "seção
// ausente ⇒ reconstruir" já é a resposta certa. Torná-la crítica só bloquearia
// o rollback (nishidb-update.sh) sem proteger nada: o binário anterior ignora a
// seção, grava manifest sem ela, e o binário novo reconstrói ao voltar.
const tagContadores = 0x0001
type ContadorInfo struct {
	Nome    string
	Colunas []uint16 // FieldIDs na ordem declarada
	Where   string   // texto normalizado; "" = sem condição
}
// Manifest.Contadores []ContadorInfo — nil no dataset sem contadores (seção omitida)
```
Layout da seção: `u16 n | por contador: u16 lenNome | nome | u16 nCols | nCols×u16 |
u32 lenWhere | where`. `maxGroups` NÃO viaja: mudá-lo não muda o conteúdo do
contador (só é recusado quando menor que os grupos em uso). O decode de tag
ignorável desconhecida continua pulando bytes (`decodeCauda`); a seção nova
entra em `appendCauda` em ordem crescente de tag (ela é a menor: vem antes de
todas as críticas).

- [ ] **Passo 1: testes** — round-trip encode/decode com dois contadores (um sem `where`); `TestManifestSemContadoresNaoEmiteASecao` (bytes idênticos ao de antes — o golden existente continua verde); `TestSecaoDeContadoresTruncadaEhRecusada`; ajustar o teste de reserva de `TamanhoDaCaudaTLV`.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `checkpoint: seção crítica dos contadores no manifest`.

---

## Bloco C — Integração no `Store`

### Tarefa C1: abertura, `applyRecord`, checkpoint, adoção/reconstrução, Close

**Arquivos:**
- Modificar: `internal/storage/store.go` (`Options` ~113-195; `Store` struct; `Open` 1094-1281; `applyRecord` 1907-1931; `Close` ~2510-2560)
- Modificar: `internal/storage/store_checkpoint.go:125-135` e a montagem do `Manifest` (~231)
- Criar: `internal/storage/contadores.go`, `contadores_test.go`

**Interfaces produzidas:**
```go
// Options
// CompilaContadores é chamado com o dicionário DEPOIS de os campos de sistema e
// indexados terem sido registrados e ANTES do replay da recovery. Ele devolve as
// definições dos contadores do YAML — e é dentro dele que o schema é compilado,
// porque as definições precisam do schema e o replay precisa das definições.
// Um dataset cujo manifest declara contadores e cuja abertura não fornece o
// callback é RECUSADO: abrir sem as definições deixaria o vetor parado enquanto o
// Put anda, e a divergência só apareceria na auditoria.
CompilaContadores func(dict *fielddict.Dict) ([]contador.Definicao, error)

// Store
func (s *Store) Contadores() []*contador.Contador
func (s *Store) EstatisticasDeContadores() []contador.Estatisticas
```

- [ ] **Passo 1: testes de integração no Store** (`contadores_test.go`, usando o helper de abertura de dataset de teste do pacote e um `Definicao` montado à mão com FieldIDs do dicionário):
  - `TestContadorAcompanhaPutLapideECopyForward`: Put A(cond) → total 1; Put A(¬cond) → 0; Put A(cond) → 1; `Expira` → 0 e refs 0; `Compact` (copy-forward) → inalterado.
  - `TestContadorSobreviveAoCheckpointEReabertura`: 100 linhas, Checkpoint, Close, Open → totais iguais (adoção, sem log de construção).
  - `TestContadorRecuperaDoTailSemCheckpoint`: 100 linhas, Checkpoint, +50 escritas (algumas mudando de grupo, algumas lápides), fecha SEM checkpoint (simula crash via o seam que o pacote já usa nos testes de recovery), reabre → totais iguais aos de uma instância de referência que nunca caiu. **Duas variantes, e as duas têm de convergir**: (a) crash sem nenhum checkpoint depois das 50 escritas — o mmap do vetor está em S, o replay aplica tudo; (b) checkpoint INTERROMPIDO depois de `applyOverlayContadores`+`msyncContadores` e antes do manifest (seam `writeTrace`/`traceStep`) — o mmap está à frente de S com records duráveis, o replay reaplica e vê `antiga == nova`. É a prova do argumento "dobra antes do replay".
  - `TestContadorNovoEhConstruidoNaAbertura`: dataset com 100 linhas SEM contador; reabre com contador → totais certos, log `contador.construido` com motivo "o dataset não tinha este contador".
  - `TestContadorComWhereDiferenteEhReconstruido`: mesmo com `where` mudado → motivo "os parâmetros mudaram", dicionário e vetor recriados (os `*.old` ficam até o fim do Open e são apagados depois do checkpoint).
  - `TestMudarMaxGroupsNaoReconstroi` e `TestMaxGroupsMenorQueOsGruposEmUsoEhRecusado` (`ErrInvalidArgument` nomeando o contador, os grupos em uso e o teto).
  - `TestContadorRemovidoDoYamlEhApagado`: reabre sem o contador → diretório some, log `contador.removido`, manifest sem a seção.
  - `TestAberturaSemCallbackRecusaDatasetComContadores` (`ErrDatasetCorrupt` com mensagem nomeando os contadores).
  - `TestContadorEhRecusadoNoHistorico`.
  - `TestOrdemDeLocks…`: a auditoria e o `_counters` nunca tomam `wmu` a partir do lock do dicionário — coberto estruturalmente (o lock do dicionário só existe dentro de `contador`, que não conhece o Store).
  - **Mutações obrigatórias (as três de proposta §10)**: (1) `Expira` deixa de chamar `applyRecord` para o contador (comentar o passo 7c no ramo de lápide) → `TestContadorAcompanhaPutLapideECopyForward` vermelho; (2) a expiração oportunista da compaction grava a lápide sem `applyRecord` → teste próprio `TestExpiracaoOportunistaDescontaDoContador` vermelho; (3) `Dobra` chamada DEPOIS do replay → `TestContadorRecuperaDoTailSemCheckpoint` (variante msyncada) vermelho.

- [ ] **Passo 2: implementar `Open`** na ordem (validada contra `store_recovery.go` e `store_checkpoint.go`):
  1. depois de `resolveCamposIndexados` (linha 1190) e antes do `switch` (1195): `defs, err := s.compilaContadores(opts, m)` — chama o callback (se `opts.CompilaContadores == nil` e `m != nil && len(m.Contadores) > 0` ⇒ `ErrDatasetCorrupt` nomeando os contadores); recusa `ModoHistorico` com defs (molde `validaOpcoesDoModo`, store.go:2629).
  2. **Decidir a reconstrução ANTES da recovery**, por contador: `recria = m == nil || seção ausente || colunas/where diferem de m.Contadores[nome]`; `contador.Abre(dir, def, minRows, recria)`. Se `!recria` e (`Nasceu()` com `m.NextRowID > 0` ou `!Cobre(m.NextRowID)`) ⇒ marcar `recria` também (o vetor não descreve as linhas) — nesse caso o `Abre` já aconteceu; chamar `Zera()`. Guardar `s.contadores` e `s.contadoresARecriar`. Contadores no manifest que não estão nas defs ⇒ `os.RemoveAll` do diretório + log `contador.removido`.
  3. **Dobra** (`c.Dobra()`, pelo tamanho do arquivo) para cada contador SEM reconstrução pendente.
  4. o ramo de recovery roda como hoje — o replay passa pelo passo 7c (os dois modos).
  5. logo depois do ramo e ANTES de `abreIndicesDeSegmento`: para cada contador em `contadoresARecriar`, `s.reconstroiContador(c)` — varredura de `[0, nextRowID)` por `locs.Get`, pula ausente e lápide, `segR.ReadRecord`, `Transiciona` (idempotente com o que o replay já aplicou: o delta é contra a entrada corrente); log `contador.construido` (motivo, linhas, grupos, duração), molde de `agenda.go:62-87`.
  6. `abreIndicesDeSegmento`, `adotaOuConstroiAgenda`, `carregaOffsets`, `Checkpoint()` — como hoje; o checkpoint grava a seção nova e aplica o overlay.

- [ ] **Passo 3: `applyRecord` passo 7c** — logo depois do bloco da agenda (1919-1923):

```go
	// 7c. contadores, DEPOIS do commit, pelo mesmo argumento da agenda: derivados,
	// fora da durabilidade do §6.1, sob o mesmo wmu. O delta sai do PRÓPRIO vetor
	// do contador (entrada antiga → nova), nunca de prevPayload — no replay ele
	// não existe (store_recovery.go). A lápide entra como "sem grupo".
	for _, c := range s.contadores {
		if err := c.Transiciona(a.rowID, a.payload, a.flags&types.FlagTombstone != 0); err != nil {
			return err
		}
	}
```

- [ ] **Passo 4: checkpoint** — dois passos novos: `traceStep("applyOverlayContadores", …)` logo DEPOIS de `applyOverlay` das locations (isto é, depois do `segSync`: é o que garante que o mmap do vetor só recebe transições de records duráveis), e `traceStep("msyncContadores", …)` entre `msyncVencimentos` e `msyncSequences`, erro NÃO sticky (godoc igual ao da agenda). Depois do msync, ainda sob `wmu`: `c.CobreMexidas(c.Transicoes())`. `Manifest.Contadores` montado de `c.Def()` a cada checkpoint. **Close**: parar o laço de aposentadoria (C4) ao lado de `ParaExpiracao`; `Close()` dos contadores ao lado da agenda; sem msync no Close (o que não foi checkpointado volta pelo replay, como hoje).

- [ ] **Passo 5: verde na suíte do storage** (`go test ./internal/storage/... -count=1`), depois `make test` e `make test-race`.

- [ ] **Passo 6: commit** — `storage: contadores abrem antes do replay, transicionam no applyRecord e viajam no manifest`.

### Tarefa C2: gate de alocação do `Put` real

**Arquivos:**
- Criar: `internal/storage/put_alloc_gate_test.go` (`//go:build !race`) e `put_alloc_gate_race_test.go`

- [ ] **Passo 1: medir ANTES de ligar contadores** — na branch, com o dataset de teste do pacote (segmento pequeno em `t.TempDir()`), `testing.AllocsPerRun(2000, func(){ Put(key existente, payload igual) })` e registrar o número no godoc do gate como "medido em <data>, sem contadores: N". Se N > 0, o gate nasce com teto N e o próximo passo prova que contadores não o sobem.

- [ ] **Passo 2: gate com contadores** — mesmo Put num dataset com dois contadores (um com `where`, um sem), grupos já internados ⇒ teto igual a N. Teste separado para o caminho "grupo novo" documentando que ele aloca (mapa + chave) e é limitado por `maxGroups`.

- [ ] **Passo 3: commit** — `storage: gate de alocação sobre o Put real, com e sem contadores`.

### Tarefa C3: auditoria sob snapshot e reconstrução em operação

**Arquivos:**
- Criar: `internal/storage/contador/auditoria.go` (tipos + comparação), `internal/storage/contadores.go` (`AuditaContadores`, `ReconstroiContadores`), testes

**Interfaces produzidas:**
```go
// contador
type Divergencia struct{ ID uint32; Texto string; Mantido, Recalculado, RefsMantido, RefsRecalculado int64 }
type Relatorio struct {
	Nome string; Grupos uint32
	Mantido, Recalculado int64      // soma dos totais
	Divergentes int                 // grupos com total OU refs diferentes
	Amostra []Divergencia           // as primeiras 20
	LinhasVarridas uint64; Duracao time.Duration
}
// Store
func (s *Store) AuditaContadores(ctx context.Context) ([]contador.Relatorio, error)
func (s *Store) ReconstroiContadores(ctx context.Context) ([]contador.Estatisticas, error)
```

Mecânica de `AuditaContadores`: `entra()`; `wmu.Lock()`; `snap := s.AbreSnapshot()`
(dentro do `wmu` de propósito: fecha a corrida entre ler a época e registrar o
leitor, `snapshot.go:176-181`); `c.Congela()` de cada contador; `wmu.Unlock()`.
Depois, sem `wmu`, **enumerador próprio** — NÃO `ScanLive`/`GetByRowID`, que
leem a location corrente e filtram por prazo vencido: para `id` em
`[snap.BaseRowID, snap.LimiteRowID)`: `packed := s.vers.Em(id, snap.Epoca)`,
senão `s.locs.Get(id)`; pula só ausente ou bit de lápide; sob `aposMu.RLock`,
`segR.ReadRecord(seg, off, buf)` e confere `rec.RowID == id`; rederiva com
`c.Deriva(payload, avaliadorDaAuditoria, scratch, chaveBuf)`; acumula em slices
por id (do tamanho do limite congelado; id ausente do dicionário ⇒ divergência
própria "grupo não internado"). Ao fim compara com a cópia e monta o relatório;
`FechaSnapshot`. `ErrSnapshotExpirado` ⇒ a auditoria é INCONCLUSIVA: devolve o
erro, nunca um relatório (o CLI já o traduz para exit 5). Godoc nomeia a
definição: "viva = com location e sem lápide", e por que a visibilidade da
consulta (prazo vencido) não serve de oráculo. `ReconstroiContadores` = sob
`wmu`: `Zera()` + varredura como na abertura; documentar que segura o writer
pela varredura inteira.

- [ ] **Passo 1: testes**
  - `TestAuditoriaSemDivergenciaComIngestaoConcorrente`: goroutine escrevendo Puts (mudando grupos) E lápides via `Expira` enquanto a auditoria roda; `Divergentes == 0` em todas as rodadas. Prova que o par (snapshot, cópia T0) é consistente — inclusive contra lápide gravada durante a varredura, que `ScanLive` pularia.
  - `TestAuditoriaContaLinhaVencidaSemLapide`: linha com prazo vencido, coletor parado; a consulta não a vê, a auditoria a conta e bate com o mantido (é a definição, escrita no godoc).
  - `TestAuditoriaDetectaDeriva`: injeta deriva via seam de teste (`c.corrompeParaTeste(id, +1)`) ⇒ `Divergentes == 1`, amostra nomeia o grupo.
  - `TestAuditoriaInconclusivaQuandoOSnapshotEstoura`: `SnapshotMaxBytes` mínimo + escrita concorrente ⇒ erro `ErrSnapshotExpirado`, sem relatório.
  - `TestReconstruirZeraADeriva`.
  - **Mutações obrigatórias**: (1) tirar a cópia dos totais de dentro do `wmu` (copiar depois de soltar) ⇒ `TestAuditoriaSemDivergenciaComIngestaoConcorrente` vermelho (intermitente é vermelho: `-count=20`); (2) trocar o enumerador por `ScanLive` ⇒ o mesmo teste vermelho pelas lápides concorrentes, e `TestAuditoriaContaLinhaVencidaSemLapide` vermelho.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `storage: auditoria dos contadores sob snapshot, sem segurar o writer, e reconstrução em operação`.

### Tarefa C4: ciclo de vida — aposentadoria de grupos vazios

**Arquivos:**
- Criar: `internal/storage/contador/aposentadoria.go`, testes; laço em `internal/storage/contadores.go`

**Interfaces produzidas:**
```go
// contador
func (c *Contador) AposentaVazios(agora time.Time) (aposentados int, err error) // writer-only
// Store: goroutine própria (como rodaExpiracao), cadência 1 min, sob wmu por passada.
```
Regra: `vazioDesde[g]` é definido quando `refs[g]` chega a 0 (`esvaziou`) e limpado
quando sobe (`tocou`); na abertura, todo grupo vivo com `refs == 0` recebe
`vazioDesde = agora` (o reinício só ESTENDE a visibilidade, nunca a encurta). Um
grupo sai quando `RetainEmptyGroups > 0 && refs == 0 && agora − vazioDesde ≥ RetainEmptyGroups`
**E `ultimaMexida[g] <= mexidasCobertas`** — a transição que esvaziou o grupo já
está durável em record E em página (coberta por um checkpoint). Sem essa cláusula,
uma cauda rasgada deixaria em disco uma linha viva apontando para um grupo
aposentado, e a dobra da reabertura a contaria como órfã. Aposentar =
`dict.Aposenta(id)` (fsync) + `aposentados.Add(1)`. Grupo aposentado some do
`_counters` (`vivo == false`) e a mesma chave, se reaparecer, interna um id NOVO.

- [ ] **Passo 1: testes**
  - `TestGrupoZeradoComRefsNaoEhAposentado`: 500 linhas na OLT X, nenhuma satisfaz o `where`; `total == 0`; relógio avança 30 dias; `AposentaVazios` ⇒ 0. (É o cenário da proposta §6.5 que a regra intuitiva erra.)
  - `TestGrupoSemRefsEhAposentadoDepoisDoPrazo`: lápides em todas as linhas; antes do prazo ⇒ continua; depois ⇒ aposentado; `_counters` (via `Grupo`) `vivo=false`; a OLT reaparece ⇒ id novo.
  - `TestReinicioEstendeAVisibilidade`: refs 0 há 6 dias, reabre, `vazioDesde` = agora.
  - `TestRetainNeverNuncaAposenta`.
  - `TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida`: lápides esvaziam o grupo; relógio avança além do prazo; SEM checkpoint ⇒ `AposentaVazios` devolve 0; `Checkpoint()` ⇒ aposentado.
  - **Mutações obrigatórias**: (1) (proposta §10) trocar a condição `refs == 0` por `total == 0` ⇒ `TestGrupoZeradoComRefsNaoEhAposentado` vermelho; (2) remover a cláusula `ultimaMexida <= mexidasCobertas` ⇒ `TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida` vermelho. Relatar os dois.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `contador: aposentadoria de grupos por refs==0 com prazo de visibilidade`.

---

## Bloco D — Schema: o bloco `counters:`

### Tarefa D1: parse e validação no `schema`

**Arquivos:**
- Criar: `internal/schema/contador.go`, `contador_test.go`
- Modificar: `internal/schema/spec.go` (`rawSpec` +`Counters yaml.Node`; `DatasetSpec` +`Contadores []ContadorSpec`; chamada em `validate` depois do bloco de `schema.fields` e da key; mensagem do prefixo `_` em 669-673 passa a "os espaços de nome do produto (`_meta`, `_counters`)")

**Interfaces produzidas:**
```go
const (
	MaxGroupsPadrao         = 65_536
	MaxGroupsTetoFisico     = 1<<31 - 1           // bit 31 do vetor é a condição
	RetainEmptyGroupsPadrao = 7 * 24 * time.Hour
	NamespaceContadores     = "_counters"
	ColunaChaveDoContador   = "groupKey"
	ColunaTotalDoContador   = "total"
)
type ContadorSpec struct {
	Name              string
	GroupBy           []string
	Where             string        // "" = sem condição
	MaxGroups         uint32
	RetainEmptyGroups time.Duration // 0 = never
}
func contadoresDoNo(n *yaml.Node) ([]rawContador, error)                       // bloco() por item: name, groupBy, where, maxGroups, retainEmptyGroups
func validaContadores(raw []rawContador, fields map[string]FieldSpec, key, seq string) ([]ContadorSpec, error)
```
Recusas (todas `specErrf`, fail-fast, itens em ordem de arquivo; nomes de colunas
ordenados antes de qualquer mensagem que liste vários):
`counters` vazio; `name` fora de `[a-z0-9_]+` ou repetido ou igual a `fields`?
(não — o namespace é outro; recusar só repetição); `groupBy` ausente/vazio/com
repetição/coluna não declarada em `schema.fields`/coluna `auto`/tipo fora de
{string,int64,boolean}/coluna igual à key ou à sequence/coluna chamada `groupKey`
ou `total` (colide com a tabela); `where` vazio quando presente; `maxGroups` zero,
negativo ou acima do teto físico; `retainEmptyGroups` zero explícito (apontar para
`never`, como o TTL faz), abaixo de 1 min. `where` NÃO é validado aqui (precisa
do schema compilado — D2), mesma nota de `validaRegrasTTL`.

- [ ] **Passo 1: testes** — um caso por recusa acima (tabela), mais `TestCountersAceitaAFormaDaProposta` (o YAML da proposta §5.1 com dois contadores) e `TestCountersAplicaOsDefaults`.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `schema: bloco counters com validação na subida`.

### Tarefa D2: compilação das definições (`internal/contadores/compila.go`)

**Arquivos:**
- Criar: `internal/contadores/compila.go`, `compila_test.go`, `abertura.go`

**Interfaces produzidas:**
```go
// Compila resolve as specs contra o schema compilado: nomes → FieldID, tipos
// efetivos, `where` pelo CompilaCondicao (recusa campo de sistema), e recusa
// `where` ou groupBy sobre coluna com merge: sum (Acumula) — a idempotência do
// contador depende de o estado ser função do último evento.
func Compila(spec *schema.DatasetSpec, sc *schema.Compiled) ([]contador.Definicao, error)

// Compilador prepara o callback de abertura do storage. Dentro do callback o
// schema é compilado (spec.Compile(dict)) — no MESMO ponto em que hoje é compilado
// depois do Open, só que antes do replay. Compilado() devolve o resultado.
type Compilador struct{ … }
func NovoCompilador(spec *schema.DatasetSpec) *Compilador
func (c *Compilador) Callback() func(*fielddict.Dict) ([]contador.Definicao, error)
func (c *Compilador) Compilado() *schema.Compiled
```
`Definicao.Where` é o texto do YAML com espaços colapsados (`strings.Join(strings.Fields(w), " ")`)
— é o que vai ao manifest e decide reconstrução.

- [ ] **Passo 1: testes** — `TestCompilaRecusaWhereSobreCampoSomado` (`merge: sum`), `TestCompilaRecusaWhereComCampoDeSistema`, `TestCompilaResolveColunaExtraida` (groupBy sobre coluna de `extract`), `TestCompiladorDevolveOMesmoSchemaQueOCallbackCompilou`.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `contadores: compilação das definições e callback de abertura`.

---

## Bloco E — Consulta: `_counters.<nome>`

### Tarefa E1: registro genérico de tabelas virtuais no motor

**Arquivos:**
- Modificar: `internal/query/engine.go` (`registro` +`virtuais map[string]map[string]*registro`; `RegistraVirtual`; `Execute` 91-98), `internal/query/meta/meta.go:66-72`, `engine_test.go`

**Interfaces produzidas:**
```go
// RegistraVirtual associa `namespace.tabela`, sob o dataset, a um schema e um
// store — o mesmo pipeline (binder → planner → executor, com métricas, top-N e
// orçamento) que um dataset de verdade usa. `_meta` continua fora do registro:
// ele descreve o dataset alvo por consulta e não tem store fixo.
func (e *Engine) RegistraVirtual(dataset, namespace, tabela string, sch *schema.Compiled, st Store) error
```
`Execute`: `if strings.HasPrefix(stmt.Namespace, "_")`: `_meta` ⇒ como hoje;
senão procura `reg.virtuais[stmt.Namespace][stmt.Dataset]`; ausente ⇒ erro
`namespace %q não existe; existem _meta e _counters` (namespace desconhecido) ou
`_counters não tem tabela %q neste dataset; existem: …` (tabela desconhecida,
lista ordenada). O binder confere `stmt.Dataset == sc.Name` de graça porque o
schema sintético tem `name: <tabela>`. Erro do binder é renomeado para
`_counters.<tabela>` como o `meta.renomeia` faz (extrair helper compartilhado).

- [ ] **Passo 1: testes** — `TestExecuteRoteiaTabelaVirtualRegistrada` (com um `fakestorage`), `TestNamespaceDesconhecidoListaOsExistentes`, `TestTabelaVirtualDesconhecidaListaAsDoDataset`, `TestMetaContinuaRespondendo`. Ajustar `TestNamespaceDesconhecidoERecusado` (meta) à mensagem nova.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `query: registro de tabelas virtuais por namespace; _counters entra pelo mesmo pipeline`.

### Tarefa E2: a tabela virtual (`internal/contadores/tabela.go`)

**Arquivos:**
- Criar: `internal/contadores/tabela.go`, `tabela_test.go`, `alloc_gate_test.go`

**Interfaces produzidas:**
```go
// Leitor é o que a tabela precisa do contador (satisfeito por *contador.Contador).
type Leitor interface {
	Limite() uint32
	Grupo(id uint32) (chave, texto []byte, total, refs int64, vivo bool)
	PorTexto(texto []byte) (uint32, bool)
}
func Tabela(def contador.Definicao, l Leitor) (*schema.Compiled, query.Store, error)
// Registra liga todos os contadores do Store no motor, sob o dataset.
func Registra(eng *query.Engine, dataset string, st *storage.Store) error
```
Schema sintético (string YAML como `especDaTabela` do meta): `name: <nome>`,
`key.field: groupKey`, `schema.include: [groupKey, <colunas…>, total]`, tipos das
colunas de `def.Tipos` (`boolean` para `TypeBool`). Cache: um `*schema.Compiled`
por contador, construído em `Tabela` (não há `sync.Once` global).

Fonte (`query.Store`): `AbreSnapshot()` devolve `types.Snapshot{LimiteRowID: l.Limite()}`
(congela o conjunto de grupos por consulta; `Epoca`/`Instante`/`BaseRowID`
ficam zero e são ignorados pela fonte — godoc no molde do `meta.fonte.AbreSnapshot`);
`ScanLive(snap)` itera `1..LimiteRowID-1`, pulando `vivo == false`;
`GetByRowID(snap, id, buf)` codifica: `groupKey` (texto), as colunas (por
`valuekey.DecodeComposta` da chave, zero-copy), `total`. **Zero alocação por
linha**: o texto e a chave vêm do dicionário (memória estável), o payload é
montado em `buf[:0]` com `encoding.AppendField` em ordem de FieldID;
`GetByKey(snap, key, buf)` ⇒ `PorTexto(key)` ⇒ mesmo `record`. `Indexado` false;
`Lookup` erro de rede de segurança (como o meta). RowID = id do grupo (crescente).

- [ ] **Passo 1: testes ponta a ponta no pacote** (harness igual ao `meta_test.linhas`, mas via `query.Engine` com `RegistraVirtual`): as consultas da proposta §5.2/§5.3:
  - `SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5`
  - `SELECT olt, total FROM _counters.autenticados_por_olt WHERE olt = 'PRCTA_G1M10'`
  - `SELECT groupKey, total FROM _counters.autenticados_por_olt WHERE groupKey = 'PRCTA_G1M10'` (point lookup — afirmar pelo plano/métrica que foi `PointLookupPlan`)
  - `SELECT olt, slot, port, total FROM _counters.autenticados_por_pon WHERE olt = 'PRCTA_G1M10' ORDER BY total ASC LIMIT 3` (colunas tipadas: `slot` sai como número no NDJSON)
  - `TestGrupoZeradoApareceComTotalZero` e `TestGrupoAposentadoNaoAparece`.
  - `TestTabelaDoContadorNaoAlocaPorLinha` (gate).

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `contadores: tabela virtual _counters.<nome> com groupKey, colunas tipadas e total`.

### Tarefa E3: wiring no servidor e no CLI

**Arquivos:**
- Modificar: `internal/server/server.go:336-426` (`abreDataset`: `NovoCompilador` → `Options.CompilaContadores` → `compilado := comp.Compilado()` no lugar de `spec.Compile(st.FieldDict())`; depois de `eng.Register`: `contadores.Registra(s.eng, spec.Name, st)`; log `contadores.declarados` com nome/groupBy/where/teto por contador, como `ttl.regras`)
- Modificar: `internal/cli/cli.go:248-279` (`abreDataset` idem) e `cli.go:302` (segunda abertura), `internal/cli/query.go:107` (`contadores.Registra` depois do `Register`), `internal/cli/offsets.go:85` (callback nas `Options`), `benchmark/ingest-bench/harness.go:1592` (idem). **Todo chamador de `storage.Open` de um dataset base passa o callback** — a guarda do C1 ("manifest com contadores sem callback ⇒ recusa") é o que pega o chamador esquecido; `historico.go:84` (irmã) NÃO passa, e a irmã nunca tem a seção.
- Testar: `internal/server/query_test.go` (`TestCountersRespondePelaRotaDeQuery`, no molde de `TestMetaFieldsRespondePelaRotaDeQuery:916`), `internal/cli/query_test.go` (consulta local a `_counters`)

- [ ] **Passo 1: testes** — HTTP: sobe servidor de teste com YAML contendo `counters`, ingere via o caminho de teste existente, consulta `_counters.x` e afirma linhas + moldura de fim; JDBC/Python NÃO mudam (é SQL). CLI: `nishidb query --config … "SELECT … FROM _counters.x"` devolve NDJSON.

- [ ] **Passo 2: vermelho → implementar → verde.** Conferir que `abreHistorico` (irmã) NÃO recebe callback e que a irmã sobe (`TestHistorico…` existentes verdes).

- [ ] **Passo 3: `make test`, `make test-race`, `make lint` → commit** — `server, cli: contadores compilados na abertura e _counters registrado no motor`.

---

## Bloco F — Operação

### Tarefa F1: métricas e `/v1/stats`

**Arquivos:**
- Modificar: `internal/metrics/nomes.go` (bloco "Contadores mantidos na escrita", com a razão de cada série; `LabelCounter = "counter"` com a nota "valor vem do YAML, conjunto fechado — nunca do cliente"), `internal/server/metricas_dataset.go` (`amostrasDeContadores(d) []metrics.Amostra` lendo `d.st.EstatisticasDeContadores()` UMA vez), `internal/protocolo/documentos.go` (`DatasetStats.Counters []ContadorStats`), `internal/server/documentos.go` (mesma chamada)
- Testar: `internal/server/metricas_dataset_test.go`, `internal/gates` continua verde

Séries (todas por `dataset`,`counter`, nascem com zero na subida via coletor que
emite sempre): `nishidb_counter_groups` (medidor, vivos), `nishidb_counter_groups_max`
(medidor), `nishidb_counter_groups_refused_total` (contador), `nishidb_counter_groups_retired_total`
(contador), `nishidb_counter_orphan_entries` (medidor, da última dobra),
`nishidb_counter_bytes` (medidor), `nishidb_counter_audit_divergent_groups`
(medidor, da última auditoria; 0 até a primeira), `nishidb_counter_audit_last_success_timestamp`
(medidor, 0 até a primeira).

- [ ] **Passo 1: teste** — `TestMetricasDeContadorNascemComZero` (scrape antes de qualquer evento tem as oito séries por contador com 0), `TestStatsEMetricsLeemOMesmoContador` (o número em `/v1/stats` é o do scrape).

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `metrics: séries dos contadores, por coletor, e o bloco em /v1/stats`.

### Tarefa F2: rotas ADMIN

**Arquivos:**
- Criar: `internal/server/contadores.go`, `contadores_test.go`; `internal/protocolo/contadores.go`
- Modificar: `internal/server/autenticacao.go:138-148` (`rt.registra("/v1/counters/audit", config.RoleAdmin, s.handleAuditoriaDeContadores)`, idem `rebuild`)

**Wire (sem código de erro novo):**
```go
type AuditoriaContador struct {
	Name string `json:"name"`; Groups uint32 `json:"groups"`
	Maintained, Recomputed int64 `json:"maintained","recomputed"`
	DivergentGroups int `json:"divergentGroups"`
	Sample []DivergenciaDeGrupo `json:"sample,omitempty"`
	RowsScanned uint64 `json:"rowsScanned"`; DurationMs int64 `json:"durationMs"`
}
type DivergenciaDeGrupo struct{ GroupKey string; Maintained, Recomputed, RefsMaintained, RefsRecomputed int64 }
type AuditoriaDataset struct{ Name string; Counters []AuditoriaContador }
type RespostaAuditoriaDeContadores struct{ Datasets []AuditoriaDataset }
type RebuildContador struct{ Name string; Groups uint32; DurationMs int64 }
type RebuildDataset struct{ Name string; Counters []RebuildContador }
type RespostaRebuildDeContadores struct{ Datasets []RebuildDataset }
```
Handlers no molde de `handleCheckpoint` (`mutacoes.go:161-198`): método POST,
`leRequisicaoDataset`, `datasetsAlvo`, `processaDatasets`, `ctxDeMutacao(r)` (a
varredura é longa e tem de honrar SIGTERM), `erroDeMutacao` (+ `ErrSnapshotExpirado`
⇒ 409 `SNAPSHOT_TOO_OLD`, código já existente). Dataset sem contadores ⇒ lista
vazia, não erro. Log `contadores.auditoria` com divergentes por contador.

- [ ] **Passo 1: testes** — 405 no GET com corpo JSON; 403 no listener loopback; auditoria devolve zero divergência após ingestão; `rebuild` responde e o writer volta a aceitar Put; SIGTERM (cancelar `vidaCtx`) interrompe a auditoria com erro de contexto.

- [ ] **Passo 2: vermelho → implementar → verde → commit** — `server: rotas ADMIN de auditoria e reconstrução dos contadores`.

### Tarefa F3: subcomando `nishidb counters`

**Arquivos:**
- Criar: `internal/cli/counters.go`, `counters_test.go`
- Modificar: `internal/cli/cli.go` (case `"counters"`; bloco `COUNTERS` no texto `uso` e linha na sinopse)

Sintaxe: `nishidb counters (--config F | --server ADDR [--user --password-file --tls-ca]) --dataset D [--audit | --rebuild]`.
Sem ação: tabela `contador  grupos  teto  aposentados  recusados` (local: `EstatisticasDeContadores`;
remoto: `/v1/stats`). `--audit`: a tabela da proposta §5.6 (`contador grupos mantido
recalculado divergência`), com a amostra de divergências abaixo quando houver, e
exit 3 (`ErrDatasetCorrupt`) quando `Divergentes > 0` — divergência é corrupção
de índice derivado, e o exit code é o que um script de operação lê. `--rebuild`:
tabela `contador grupos duração`. `--config` com servidor no ar ⇒ exit 3 + dica
`dicaDeServidor` (já existe). Saída via `escreveSaida` (tabwriter, uma escrita).

- [ ] **Passo 1: testes** — `TestOUsoAnunciaCounters`; `TestCountersLocalListaEAudita` (dataset de teste); `TestCountersAuditaComDivergenciaSaiComExitDataset` (deriva injetada pelo seam); `TestCountersRemotoChamaARota` (httptest, como os testes de `postaMutacao`); `TestCountersComServidorNoArSaiComExit3`.

- [ ] **Passo 2: vermelho → implementar → verde → `make test` + `make test-race` + `make lint` → commit** — `cli: subcomando counters (lista, auditoria, reconstrução) local e remoto`.

---

## Bloco G — Documentação, bancada e aceite

### Tarefa G1: documentação no mesmo estado do código

**Arquivos:**
- `README.md`: em `### 2. Definição do dataset`, DEPOIS do bloco YAML existente da linha 319, um bloco ```` ```yaml ```` com `counters:` (proposta §5.1) — posição segura para `TestQuickstartDoREADMEExecuta` (só `yamls[0..1]` são posicionais; conferir com o gate antes de commitar); nova seção `### Contadores mantidos na escrita` depois de `### Ver o estado do servidor no ar` (linha ~1137) com os exemplos de consulta em ```` ```bash ```` (não `console`), a tabela `_counters`, `groupKey`, a nota "lê o presente, não é versionado pelo snapshot", o `nishidb counters` e as rotas; na tabela de SQL suportado, a linha de namespaces virtuais lista `_meta` e `_counters`.
- `docs/architecture.md`: seção "Contadores mantidos na escrita" (o quê, os quatro momentos, por que a dobra vem antes do replay, o resíduo da cauda rasgada, o ciclo de vida por `refs`, a auditoria sob snapshot, a tabela virtual), com números só depois de medidos (G2); incorporar `docs/diagrams/contadores_flow.puml` pelo padrão `uml.nishisan.dev/proxy?src=…/main/docs/diagrams/contadores_flow.puml` (validar pelo servidor, ver memória `plantuml-ausente-validar-pelo-servidor`); a lista "O que ainda não existe" perde o item se houver.
- `docs/debitos.md`: itens 107+ no fim da seção "Sem milestone": (a) `AbreSnapshot` fora do `wmu` nas CONSULTAS lê a época e registra o leitor em dois passos (`snapshot.go:176-181`) — a auditoria fecha a corrida tomando o `wmu`; as consultas não (pré-existente, achado na validação deste ciclo); (b) fsync por grupo novo no nível PON/splitter (custo não medido); (c) `_counters` fora do isolamento de snapshot; (d) `Lookup` indexado na tabela virtual (hoje `WHERE olt = …` em contador composto é varredura dos grupos); (e) `ScanLive(snap)` não é versionado para lápides — a consulta longa pode perder uma linha lapidada durante a varredura (pré-existente; a auditoria não depende dele).
- `CLAUDE.md`: corrigir "índices secundários fazem SET antes e CLEAR depois do commit" para "SET antes do commit; não há CLEAR (spec §5.2 do subsistema 1)"; acrescentar o contador à lista de pacotes (`internal/storage/contador`, `internal/contadores`) e a invariante "contador é delta do próprio vetor, nunca de `prevPayload`; dobra antes do replay".
- `.gitignore`: `samples/` (a amostra não é versionada, proposta §12).
- `internal/cli/evidencias_test.go:53-58`: acrescentar `docs/aceite-contadores.md` (G2 cria).

- [ ] **Passo 1: escrever; rodar `go test ./internal/gates/ ./internal/cli/ -count=1` (citações, evidências, quickstart).**
- [ ] **Passo 2: commit** — `docs: contadores mantidos na escrita — README, arquitetura, diagrama, débitos, CLAUDE.md`.

### Tarefa G2: bancada e aceite

**Arquivos:**
- Modificar: `benchmark/micro/apply_bench_test.go` (variante `BenchmarkApplyIngestRealComContadores`: mesmo corpus, dois contadores sobre campos do corpus RADIUS; exige `NISHIDB_BENCH_DIR` em disco real)
- Criar: `docs/aceite-contadores.md`

- [ ] **Passo 1: rodar** `NISHIDB_BENCH_DIR=<disco real> go test ./benchmark/micro/ -run='^$' -bench='BenchmarkApplyIngestReal' -benchmem -benchtime=20000x -count=6` sem e com contadores; `benchstat`. Critério: allocs/op iguais; custo por evento reportado (esperado: microssegundos — uma passada extra pelo payload por contador).
- [ ] **Passo 2: medir a dobra e a reconstrução** num dataset de 1M linhas sintéticas (o pacote `benchmark` tem gerador): tempo de `Dobra` (esperado: dezenas de ms) e de reconstrução (esperado: ordem do full scan).
- [ ] **Passo 3: `docs/aceite-contadores.md`** — no molde de `docs/aceite-indice-por-segmento.md`: critérios (um por invariante da proposta §10), os testes que provam cada um, as **mutações aplicadas com o vermelho e o verde de cada uma**, os números da bancada com origem, e o que ficou como débito.
- [ ] **Passo 4:** `docs/architecture.md` recebe os números com a origem; commit — `aceite: contadores mantidos na escrita — bancada e prova por mutação`.

### Tarefa G3: fechamento

- [ ] `make test`, `make test-race`, `make lint` (baseline igualada), `make test-integration`, `make build`.
- [ ] `git status` limpo de resíduo de mutação (classe 3 da doutrina).
- [ ] Copiar este plano para `planning/contadores-mantidos-na-escrita-plano.md` com a tabela "Estado da execução" preenchida (formato do M6), e atualizar o cabeçalho de `planning/contadores-mantidos-na-escrita.md` de "proposta" para "implementado, ver o plano".
- [ ] `superpowers:finishing-a-development-branch`: merge em `main` por PR.

## Ordem de execução e commits

A1 → A2 → A3 → B1 → B2 → B3 → C1 → C2 → C3 → C4 → D1 → D2 → E1 → E2 → E3 → F1 → F2 → F3 → G1 → G2 → G3.
Um commit por tarefa (o projeto aceita commits por bloco lógico). D1/D2 podem ser
feitos antes de C para destravar E3 mais cedo, mas C1 depende só de A/B.

## Fora de escopo, com razão

- **Mudanças no produtor** (proposta §8: publicar com chave; `_SEQ` no primeiro nível): repositório externo. Sem elas o contador está certo sobre um estado possivelmente errado — registrado no README como pré-requisito do caso de uso.
- **Calibrar o prazo de 48 h** (proposta §11.1): é regra de TTL do dataset, já existente; decisão de operação.
- **Coluna `refs`/`rows` na tabela**: a proposta mantém `refs` interno; sai pela auditoria e pelas stats.
- **`_counters` no `/v1/metadata`** e no `DatabaseMetaData` do JDBC: o `_meta` também não está; é ciclo próprio.
- **Reciclagem de ids** (proposta §6.5): só aposentadoria; reciclar exige prova de ausência de referência.

## Verificação

**Por tarefa, antes do commit:** `make test`, `make test-race`, `make lint` (com
`.golangci-baseline` lido do arquivo e igualado no mesmo commit).

**Gates que continuam verdes sem edição:** `TestCitacoesDeSecaoResolvem`,
`TestAmostrasDeMetricaTemFamiliaENome`, `TestQuickstartDoREADMEExecuta`,
`TestTodoCodigoDoProtocoloTemSqlStateNoDriver`,
`TestTodoCodigoDoProtocoloTemExcecaoNoClientPython`, `TestChaveCanonicaEhGoldenDeDisco`
(a chave simples não muda), os testes de manifest existentes (dataset sem
contadores gera bytes idênticos), toda a suíte de `internal/storage`
(recovery, compaction, expiração, snapshot) e de `internal/query/meta`.

**Gates que mudam, e como:** `TestNamespaceDesconhecidoERecusado` (mensagem lista
os dois namespaces); `TestOUsoAnuncia…` ganha `Counters`; o teste de reserva de
`TamanhoDaCaudaTLV`; `evidencias_test.go` ganha o aceite novo. Nada afrouxa.

**Diferenciais (os testes que decidem se o plano está certo):**
`TestDobraReproduzOsTotais`, `TestContadorRecuperaDoTailSemCheckpoint` (duas
variantes), `TestAuditoriaSemDivergenciaComIngestaoConcorrente`,
`TestGrupoZeradoComRefsNaoEhAposentado`, `TestReprocessarOMesmoEventoNaoMexe`,
os gates de alocação (Denso32, dicionário, contador, avaliador, tabela, Put real).

**Mutações obrigatórias (vermelho → verde relatados no aceite):** fsync do
dicionário; decremento de refs na troca de grupo; decremento de total na lápide;
`Transiciona` gravando direto no mmap em vez do overlay; lápide da expiração sem
`applyRecord`; expiração oportunista sem `applyRecord`; dobra depois do replay;
cópia dos totais fora do `wmu` na auditoria; enumerador da auditoria trocado por
`ScanLive`; `total == 0` no lugar de `refs == 0` na aposentadoria; aposentadoria
sem a cláusula de cobertura pelo checkpoint.

**Ponta a ponta com o binário** (`make build`): (1) YAML da proposta §5.1 sobre
um dataset com a amostra `samples/` ingerida por `nishidb ingest`; (2)
`nishidb query --config … "SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5"`;
(3) subir o servidor, repetir com `--server`, e `nishidb counters --server … --audit`
com zero divergência; (4) parar com SIGTERM no meio de uma auditoria e ver o
processo encerrar; (5) reiniciar e ver `contador` adotado sem reconstrução;
(6) mudar o `where` no YAML, reiniciar, ver `contador.construido` no log e a
auditoria zerada; (7) `/metrics` com as séries dos dois contadores.
