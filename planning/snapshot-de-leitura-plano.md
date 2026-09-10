# Plano — Snapshot de leitura com isolamento (C2 + C6)

## Context

O bloco **C2** do ciclo M6 nunca foi feito, e está registrado como **débito 94**:
não existe `Store.Snapshot()`. O efeito tem duas metades, e as duas doem hoje:

1. **O relógio anda debaixo da varredura.** `prazoVencido`
   ([store.go:1863](internal/storage/store.go:1863)) chama `s.agora()` a cada
   row testada. Uma linha viva quando a consulta começou some do meio do
   resultado quando a varredura a alcança.
2. **Índice e varredura discordam do conjunto.** O full scan fixa o teto de
   rowId na abertura ([store.go:2174](internal/storage/store.go:2174)); o
   caminho por índice não fixa nada — a fonte do segmento corrente lê o `.widx`
   que a ingestão continua escrevendo. A MESMA consulta devolve conjuntos
   diferentes conforme o planner escolha um ou outro.

Enquanto o teto de prazo era 30s, a janela era estreita. O `query.maxTimeout`
da fase de consulta longa (débito 63) a esticou para a duração inteira de um
relatório: **a consulta longa entrou em produção sem a garantia que a torna
útil.**

O escopo aqui vai além do C2 original por decisão explícita: entra também o
**C6 — isolamento de snapshot**, que o ciclo M6 tinha deixado de fora. Com ele,
o resultado passa a corresponder a **um instante** do banco: cada linha é
entregue na versão vigente em T0, não na versão corrente.

**Resultado pretendido:** uma consulta de oito minutos devolve exatamente o que
devolveria se o banco tivesse congelado no instante em que ela abriu —
independentemente do plano escolhido, da ingestão em curso, do coletor de
expiração, da compaction e da retenção.

## Preço que esta escolha cobra

O C6 transforma a fase de "propagar um instante" em **versionamento de leitura**.
Ele acrescenta uma estrutura em heap proporcional a (taxa de escrita × duração
da consulta mais longa viva), o que ataca a tensão central do produto — 8M de
entidades em menos de 1 GiB de RSS. Por isso o teto de memória e a recusa
nomeada (`SNAPSHOT_TOO_OLD`) **não são acessórios da fase, são parte dela**:
sem eles, uma consulta longa esquecida derruba o processo. Está tudo na etapa
E4, e a etapa não é opcional.

## Três achados que mudam o desenho de 27/08

O bloco C2 foi escrito antes do índice por segmento. Ler o código de hoje mudou
três coisas — e a primeira barateia a fase inteira.

**1. As versões antigas já estão em disco. Não existe before-image a copiar.**
O plano M6 estimava o C6 como "journal de before-images no commit point do
writer". Não é preciso: os segmentos são append-only, e uma atualização apenas
appenda um record novo e publica a nova location — o record anterior continua
fisicamente no segmento até a compaction aposentá-lo. Mais: a location anterior
já está **na mão do writer**, no `applyRecord`, no exato passo que decrementa a
liveness do segmento antigo ([store.go:1698](internal/storage/store.go:1698)).
O journal guarda `(rowID, packedAnterior, época)` — 24 bytes por atualização, sem
payload. O leitor lê a versão antiga por `ReadRecord(segAntigo, offAntigo)`.

Consequência boa: `escritas` e `prazos` da versão antiga **não precisam ser
guardados**. São índices derivados cuja fonte da verdade é o campo de sistema
do payload — `instanteDeEscrita` ([store.go:1826](internal/storage/store.go:1826))
os extrai do record antigo lido, que é exatamente o que o `applyRecord` faz.

**2. O copy-forward da compaction NÃO pode versionar.** `reapendaSeVivo` chama
`applyRecord` com `hadPrevious: true`
([compaction.go:218](internal/storage/compaction.go:218)). Se o journal fosse
alimentado dentro do `applyRecord` sem discriminar, a compaction criaria
entradas apontando para o segmento que ela está aposentando — e o pin
bloquearia a própria passada que o gerou, em laço. A distinção é semântica e
tem de ser explícita: **Put e Expira mudam o conteúdo visível e versionam; o
copy-forward só move bytes e não versiona.** O leitor com snapshot segue a
location corrente, que a compaction acabou de atualizar para o mesmo conteúdo.

**3. A compaction apaga payload de row que o snapshot ainda vê.** A descoberta
oportunista da expiração ([compaction.go:186](internal/storage/compaction.go:186))
testa `prazoVencido` com o relógio ATUAL e, se venceu, grava a lápide no destino
em vez de copiar o payload. Sob um snapshot com relógio congelado, essa row
ainda é visível — e o payload dela morre ali, sem nenhuma entrada de journal que
o recupere. **A compaction tem de usar o instante do snapshot mais antigo vivo
como relógio dessa decisão.** Fecha o furo sem pin nenhum: a row é copiada
normalmente e o snapshot lê a location nova.

## Arquitetura

### O tipo de valor e o registro

```go
// internal/storage/snapshot.go
type Snapshot struct {
    Epoch      uint64      // versão: o que estava publicado em T0
    Instant    time.Time   // relógio congelado da expiração
    LimitRowID types.RowID // teto: rows nascidas depois não aparecem
    BaseRowID  types.RowID // piso: a purga não abre buraco por baixo
    id         uint64      // handle do registro de snapshots vivos
}

func (s *Store) OpenSnapshot() Snapshot
func (s *Store) CloseSnapshot(Snapshot)
```

O snapshot **viaja em `Options`, nunca em campo do `Store`** — dois leitores
concorrentes têm snapshots diferentes, e um campo os fundiria num só. É o ponto
que o plano M6 já marcava como o que costuma ser errado.

`OpenSnapshot` registra; `CloseSnapshot` desregistra e dispara a poda. O par é
o que dá ao writer a **menor época viva** (um atômico), que governa três
decisões: se vale versionar, o que podar do journal, e qual relógio a compaction
usa.

> **Sobre `TestSnapshotNaoAlocaNoOpen`, do plano M6:** com registro, abrir um
> snapshot aloca uma entrada. O teste como estava escrito não pode passar e não
> deve ser forçado a passar. O gate que continua valendo — e que é o que
> importa — é **zero alocação por linha** no scan.

### O journal de versões

Pacote novo `internal/storage/versions`, dono único da estrutura, com testes
próprios. Heap, não mmap (é estado de sessão, morre com o processo).

```go
type Journal struct { ... }

func (c *Journal) Retain(row types.RowID, packedAnterior uint64, epoca uint64)
func (c *Journal) At(row types.RowID, epoca uint64) (packed uint64, achou bool)
func (c *Journal) Prune(menorEpocaViva uint64) (entradas int, bytes int64)
func (c *Journal) Pinned(seg uint32) bool
func (c *Journal) Bytes() int64
```

**A regra de busca, que é onde um erro passaria despercebido:** ao publicar a
versão nova de `R` na época `P`, registra-se `{P, packedAnterior}`. O leitor com
época `E < P` procura a **menor** época registrada para `R` que seja maior que
`E`, e usa o `packedAnterior` dela — essa é a versão vigente em `E`. Sem
nenhuma entrada com época maior que `E`, a location corrente é a certa.

O pin é **por segmento, por refcount**: `Retain` incrementa o contador do
segmento do `packedAnterior`, `Prune` decrementa. A compaction pula segmento com
contador acima de zero. Refcount preciso, e não um piso conservador de segmento,
porque com `maxTimeout` de minutos um piso pararia a compaction pela duração
inteira de cada relatório.

### Ordem de locks

`verMu` (do journal) é **folha e nunca aninhado**: o leitor consulta o journal,
solta, e só então toma `aposMu.RLock` para ler os bytes. É seguro porque o pin
pertence ao **snapshot vivo**, não à entrada consultada — enquanto o snapshot
existe, o segmento não pode ser aposentado. A ordem publicada passa a ser
`vida → wmu → aposMu`, com `verMu` e `selMu` folhas.

### Propagação até o executor

Os únicos consumidores fora de `internal/storage` são
[candidate.go](internal/query/executor/candidate.go:40) e o harness de bench.
Por isso **muda-se a assinatura, sem criar métodos paralelos no molde de
`LookupBitmapEm`** — duas
portas para a mesma pergunta é o que o repositório condena em toda parte. E
`LookupBitmapEm`, do plano M6, não existe mais: o D5 colapsou os dois lookups
em `Lookup`.

- `executor.Store` ganha o `Snapshot` nos três métodos de leitura.
- `executor.Open` abre o snapshot **antes** de `fonteDoPlano`
  ([executor.go:231](internal/query/executor/executor.go:231)); o `Close` do
  iterador o fecha, em todos os caminhos de saída.
- `ScanLive` perde o cálculo do teto — quem o fixa passa a ser o snapshot, para
  os quatro planos.
- `candidateSource.advance` filtra `id >= LimitRowID` e `id < BaseRowID`
  **antes** do fetch, o que dá teto e piso uniformes a bitmap, hash, interseção
  e união de graça.
- Acompanham a assinatura: `fakestorage` (senão os gates de alocação passam a
  medir outra coisa) e `query/meta/source.go`, que ignora o snapshot com a razão
  no godoc — o `_meta` já é um retrato por construção.

## Etapas

Cada etapa é um commit, na ordem. As três primeiras entregam correção; a quarta
é o que impede a correção de virar um modo de falha novo.

### E0 — Caracterização (nenhuma linha de produto muda)

Quatro fatos que hoje são opinião viram asserção vermelha: a row que vence no
meio da varredura e some do resultado; o plano indexado que enxerga row nascida
depois do `Open` enquanto o full scan não; o valor lido depois da atualização
concorrente; a compaction que lapida uma row que a consulta ainda deveria ver.
Roda sob a tag `integration` onde precisar de disco real.

### E1 — Snapshot de valor, relógio congelado, teto e piso

`storage.Snapshot`, `OpenSnapshot`/`CloseSnapshot`, registro de vivos com a
menor época atômica. `prazoVencido` ganha o parâmetro `em time.Time` e deixa de
chamar `s.agora()`; os cinco pontos de liveness passam o instante do snapshot —
`reapendaSeVivo` é a exceção tratada em E3. Propagação até o executor conforme
acima. **Fecha o C2 do débito 94.**

Testes: `TestOsDoisPlanosEnxergamOMesmoConjuntoDeRows`,
`TestRowQueVenceNoMeioDaVarreduraContinuaVisivelAteOFimDaQuery`,
`TestRowNascidaDepoisDoOpenNaoApareceNemPeloIndice`,
`TestPurgaDoHistoricoNaoAbreBuracoNaVarreduraEmVoo`,
`TestDoisLeitoresConcorrentesTemSnapshotsIndependentes`.

> `TestOsDoisPlanosEnxergamOMesmoConjuntoDeRows` tem de ser construído sobre
> **row nascida depois do Open** e **row vencida no meio** — que é o que o
> snapshot fecha. Escrito com um update de valor, ele passa por sorte em E1 e
> vira um oráculo que não mede o que o nome diz.

### E2 — Isolamento: journal de versões e pin

Pacote `internal/storage/versions`. `recordApply` ganha o discriminador de
versionamento; Put e Expira versionam, copy-forward não. Leitura versionada no
`GetByRowID` e no `lookup` por key. Pin por refcount consultado por
`candidatos(ratio)` ([compaction.go:689](internal/storage/compaction.go:689)).

Testes: `TestLinhaAtualizadaDuranteAConsultaEhEntregueNaVersaoDeT0`,
`TestDuasAtualizacoesNaMesmaLinhaEntregamAVersaoMaisAntigaEEnaoAIntermediaria`,
`TestCopyForwardNaoCriaVersaoEPortantoNaoPinaOSegmentoQueEstaAposentando`,
`TestSegmentoComVersaoRetidaNaoEhEleitoPelaCompaction`,
`TestPodaLiberaOPinQuandoOUltimoSnapshotFecha`,
`TestVerMuNuncaEhAninhadoComAposMu`.

### E3 — Compaction e retenção sob o piso de snapshot

A descoberta oportunista da expiração passa a usar o instante do snapshot mais
antigo vivo (achado 3). A retenção não avança a marca-d'água além do piso vivo,
e o que não pode avançar aparece no log e na métrica em vez de silenciar.

Testes: `TestCompactionNaoLapidaLinhaVisivelAoSnapshotMaisAntigo`,
`TestRetencaoNaoUltrapassaOPisoDoSnapshotVivoEDizIssoNoLog`,
`TestSemSnapshotVivoACompactionVoltaAUsarORelogioCorrente`.

### E4 — Teto, recusa nomeada e contrato de erro

`query.snapshot.maxBytes` (ponteiro, para distinguir ausente de explícito;
recusa na subida se fora de faixa, nunca arredonda). Estourado o teto, o
snapshot **mais antigo** é invalidado e a consulta que o usa falha na próxima
leitura com `SNAPSHOT_TOO_OLD` — a ingestão nunca para por causa de uma
consulta. Sentinela `ErrSnapshotExpirado` + `errors.Is` em `codigoDoErro`,
código de wire estável, SQLSTATE no driver JDBC e exceção mapeada no client
Python (o gate cobra os três).

Testes: `TestSnapshotQueEstouraOTetoEhInvalidadoEAIngestaoContinua`,
`TestConsultaComSnapshotInvalidadoFalhaComCodigoProprio`,
`TestTetoAusenteUsaODefaultETetoForaDeFaixaRecusaNaSubida`, mais os gates de
paridade dos clients.

### E5 — Observabilidade e documentação

`consultaEmVoo` ([inflight.go:31](internal/server/inflight.go:31)) ganha a época e o
instante do snapshot, alimentando `/v1/stats` e o `nishidb status`. Métricas
novas, todas por constante em `internal/metrics/names.go`, nascendo com zero na
subida: `nishidb_query_snapshot_oldest_seconds` (prevista no M6 e nunca criada),
`nishidb_query_snapshot_bytes`, `nishidb_query_snapshot_expired_total`,
`nishidb_compaction_segmentos_pinados`. Atualização de `docs/architecture.md`,
`docs/debitos.md` (fecha o 94) e do diagrama de sequência da consulta, no mesmo
commit.

### E6 — Bancada

O journal está no caminho quente do writer. A fase não pode regredir a
ingestão: rodada de `make bench-micro` contra disco real, comparada com a
baseline em `benchmark/baselines/`, e o gate de zero alocação por linha na
leitura reafirmado com o snapshot no caminho.

## Verificação

```bash
make test && make test-race && make test-integration && make lint
```

Ponta a ponta contra o binário real, com ingestão concorrente:

```bash
NISHIDB_BENCH_DIR=/var/tmp/nishidb-bench make bench-micro
```

E o diferencial que prova a fase no piloto: uma consulta longa com `ORDER BY`
sobre milhões de candidatos, rodando enquanto o dataset ingere, comparada com o
mesmo SQL sobre um dataset parado — os dois resultados têm de ser idênticos
linha a linha.

**A doutrina de verificação vale para cada teste nomeado acima:** aplique a
mutação, veja vermelho, reverta, veja verde, e relate os dois. As mutações que
importam nesta fase: apagar o filtro de teto no `candidateSource`; fazer o
copy-forward versionar; devolver a versão intermediária em vez da mais antiga
na busca do journal; usar o relógio corrente na descoberta oportunista da
compaction.

## O que fica de fora, e precisa estar no godoc

- **Consulta que atravessa reinício** não sobrevive: o journal é heap e morre
  com o processo. É a semântica pretendida, não uma lacuna.
- **A tabela histórica não versiona** — linhas só nascem, nunca são atualizadas.
  Para ela valem só o teto, o piso e o relógio.
- **`_meta` ignora o snapshot** por já ser um retrato do materializer.

Aprovado, o plano é copiado para `planning/` conforme a convenção do projeto.
