# M4 — Sustentar o dataset vivo: compaction, disco e operação (§53-55, §92, §96)

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` ou `superpowers:executing-plans` para executar tarefa a tarefa. Os passos usam checkbox (`- [ ]`).

**Goal:** fazer o disco parar de crescer para sempre — compaction de segmentos rodando concorrente com a ingestão Kafka, com a curva visível em métrica antes, durante e depois.

**Architecture:** copy-forward pelo writer. A compaction varre o segmento candidato com `segment.ScanFrom`, e para cada record cuja location **ainda o nomeia** re-apenda o payload pelo caminho normal de escrita, sob o mutex do writer. Terminada a varredura, nenhuma location aponta para a origem: o tally do segmento é **zerado por autoridade da varredura** e o arquivo é aposentado por rename para nome não-canônico, que o `ListSegments` já ignora.

**Tech Stack:** Go 1.25, stdlib apenas. **Nenhuma dependência nova.**

**Spec:** `nishidb-spec-proposal.md` §53-55 (compaction), §78 (`storage.compaction`), §92 (métricas), §96 (CLI). Roadmap: [`planning/roadmap-m4-m5-m6.md`](roadmap-m4-m5-m6.md).

---

## Global Constraints

Valem para **todas** as tarefas.

1. **§54 — compaction não carrega segmento em memória.** Streaming record a record, buffer reutilizado. Nenhum `[]types.Record` acumulado, nenhuma leitura de segmento inteiro.
2. **§128 `NO multi-writer v1`.** Toda mutação de índice continua passando pelo writer único, sob `s.wmu`. `Publish`, `ApplyOverlay` e `segTally` permanecem writer-only.
3. **Durabilidade por um caminho só.** `checkpoint.escreveAtomico` ([manifest.go:250](../internal/storage/checkpoint/manifest.go:250)) é o único caminho de durabilidade provado do projeto, e o comentário lá proíbe duplicá-lo. A compaction não escreve manifest por conta própria: ela chama `Checkpoint()`.
4. **Nenhuma dependência nova** (§97: binário único).
5. **Idioma e estilo:** comentários, erros e nomes de teste em PT-BR, no estilo argumentativo existente — o comentário explica **por quê**, cita `§NN` e o débito que fecha. Sentinela por pacote + `errors.Is` na fronteira. Stdlib `testing`, zero testify.
6. **`go test ./...`, `go test ./... -race` e o lint limpos** antes de cada commit. A regra continua: **0 apontamentos no código novo**.
   **O lint roda com os tetos desligados** — `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0` — e a linha de base é **390**, não 114. Os 114 que a documentação do M2 e da Fase 6 citam são um número **truncado** pelos caps default (`--max-issues-per-linter=50`, `--max-same-issues=3`): `revive` reporta 50 com teto e **81** sem. Um número truncado não serve de linha de base, porque o linter estourado no teto **absorve o apontamento novo em silêncio** — e a comparação "114 antes, 114 depois" continuaria verde. Ver a Task 0.
7. **Rotas mutantes não existem neste milestone** (decisão D3 do roadmap). A compaction é background regido por config; `POST /v1/compact` é do M5, atrás do papel ADMIN.

---

## 1. Contexto

A 500 ev/s contínuos, o §86 projeta 30–43 GB escritos por dia contra 5,6–8 GB de estado vivo (§84), num disco recomendado de 50–100 GB (§85). Segmentos são append-only: todo update apenda um record novo e deixa o anterior como garbage, e **nada no código devolve esse espaço**. São 2 a 3 dias até ENOSPC.

Metade da compaction já está no repositório como primitiva provada:

| Peça do §53-55 | Onde | Papel aqui |
|---|---|---|
| Varredura streaming, buffer reutilizado, CRC validado, para em corrupção sem erro | `Reader.ScanFrom` — [segment.go:399](../internal/storage/segment/segment.go:399) | É literalmente o laço do §54 |
| `SizeBytes`/`TotalRecords`/`LiveRecords`/`LiveBytes` por segmento, persistidos no manifest | `checkpoint.SegmentInfo` — [manifest.go:79](../internal/storage/checkpoint/manifest.go:79) | Insumo da seleção de candidatos do §53 |
| Publicação atômica de location num u64 | `Pack`/`Unpack`/`Publish` — [rowindex.go:29](../internal/storage/rowindex/rowindex.go:29) | É o "atomically update locations" do §55 |
| Aplicação de estado num code path só | `applyRecord` — [store.go:783](../internal/storage/store.go:783) | O copy-forward entra por ele, não por um segundo caminho |
| Quiesce e ciclo de vida | `vida` RWMutex + `entra/sai` + `Close` — [store.go:153](../internal/storage/store.go:153) | A goroutine de compaction se junta a ele |
| `ListSegments` **ignora nome não-canônico de propósito** | [segment.go:252](../internal/storage/segment/segment.go:252) | É o mecanismo de aposentadoria, já pronto |

O trabalho é **ciclo de vida de arquivo e coordenação com o writer**, não motor novo.

---

## 2. Decisões fechadas

| # | Decisão | Escolha | Alternativa rejeitada |
|---|---|---|---|
| **D1** | Destino dos records vivos | **Copy-forward pelo segmento corrente**: re-apendar pelo `segW.Append` normal | Construir arquivo novo com ID próprio. Esbarra em três restrições reais: `checkHeader` amarra o `segmentID` ao nome do arquivo ([segment.go:70](../internal/storage/segment/segment.go:70)), `openNew` usa `O_EXCL`, e IDs vêm de `segID+1` no writer. Copy-forward faz as três desaparecerem e reusa rotação, tally e durabilidade já provadas |
| **D2** | Critério de "vivo" | **A location ainda nomeia este record** (`Unpack(locs.Get(rowID)) == (seg, off)`) | "Row visível". Um record **tombstoned** precisa ser copiado também: a location aponta para ele, e não copiá-lo deixaria a location pendurada num arquivo removido. Recuperar tombstone exige limpar location e primary, que é trabalho do TTL (M6) |
| **D3** | Corrida com a ingestão | **Verificação e append na MESMA seção crítica de `s.wmu`** | Verificar fora do lock e apendar depois. Um update entre os dois faria o payload **antigo** vencer o novo — perda de dado silenciosa. É o motivo de existir `reapendaSeVivo` em vez de chamar `Put` |
| **D4** | Tally do segmento esvaziado | **Zerado por autoridade da varredura completa** | Confiar nos decrementos. Não fecha: o tally é limite superior (débitos 38 e 45), então os decrementos param no resíduo órfão e nunca alcançam zero. A varredura viu **todos** os records e é a única fonte exata — é exatamente o que [manifest.go:73](../internal/storage/checkpoint/manifest.go:73) prevê ao dizer "só uma varredura do segmento reconstrói o tally" |
| **D5** | Aposentadoria do arquivo | **Rename para `segment-%08d.dat.retired`**, depois checkpoint, depois unlink | Unlink direto. Abre janela: o manifest vigente ainda lista o segmento, e `Open` trata segmento listado e ausente como `ErrDatasetCorrupt` ([store_recovery.go:57](../internal/storage/store_recovery.go:57)). O rename mantém os bytes em disco até o manifest concordar |
| **D6** | Segmento corrente como candidato | **Nunca.** Só segmentos selados (`id != CurrentSegment`) | Compactar o segmento em que se está apendando é copiar dentro de si mesmo |
| **D7** | Cancelamento | Goroutine própria com `context` + `WaitGroup`, **cancelada e aguardada por `Close` ANTES do `vida.Lock()`** | Segurar `entra()` pela varredura inteira. `Close` toma `vida.Lock()` e ficaria bloqueado por minutos |
| **D8** | `garbage_bytes` na métrica | Publicado como **limite inferior**, com a semântica no `Ajuda` | Deixar fora. `live` é limite superior, logo `total - live` é limite **inferior** do garbage: "há pelo menos isto de lixo" é afirmação verdadeira e acionável |

---

## 3. Estrutura de arquivos

| Arquivo | Responsabilidade |
|---|---|
| `internal/storage/compaction.go` **(novo)** | `reapendaSeVivo`, `compactaSegmento`, `aposentaSegmento`, seleção de candidatos. Todo o motor num arquivo só |
| `internal/storage/compaction_test.go` **(novo)** | Unidade: liveness, corrida com update, tally zerado |
| `internal/storage/compaction_crash_test.go` **(novo)** | As duas janelas de crash do D5, com SIGKILL real |
| `internal/storage/store.go` | `Stats` ganha campos de disco; ciclo de vida da goroutine; `Close` cancela e aguarda |
| `internal/storage/store_recovery.go` | Regras de reparo do `.retired` (restaurar e varrer órfão) |
| `internal/config/config.go` | `storage.compaction.{enabled,minimumLiveRatio}` |
| `internal/server/metricas_dataset.go` | `storage_bytes`, `garbage_bytes`, séries de compaction |
| `internal/kafka/writer.go` | Débito 56 — checkpoint por tempo |
| `internal/cli/`, `internal/server/`, `internal/protocolo/` | §96 leitura: `status`, `datasets`, `inspect` |

---

## Fase A′ — o que a revisão adversarial cobrou

Três correções levantadas na execução da Fase A. **Duas são erro deste plano**, não do código, e estão marcadas como tal. Entram antes de qualquer coisa nova.

### Task 0: o gate de lint precisa enxergar

**Files:** Modify `.golangci.yml`, `Makefile` (alvo `lint`), `docs/debitos.md`

A doutrina do projeto — "0 apontamentos no código novo, com os 114 do M1 conferidos antes e depois" — foi verificada com um instrumento cego desde o M2. Os 114 são **truncados** pelos caps default (`--max-issues-per-linter=50`, `--max-same-issues=3`). Medido nesta branch: `revive` sai **50** com teto e **81** sem; `staticcheck`, 12 e 13. O total real é **390**.

O defeito não é o número errado, é o gate inoperante: com `revive` já estourado no teto, um apontamento **novo** de `revive` não muda a contagem — entra no lugar de outro que a truncagem descartou. "114 antes, 114 depois" continuaria verde com código novo sujo. Pior, o conjunto de 114 **não é estável**: duas rodadas no mesmo commit devolvem seleções diferentes.

- [ ] **Step 1: Provar o defeito**

Run: `golangci-lint run 2>&1 | tail -3`
Run: `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0 2>&1 | tail -3`
Expected: os totais por linter divergem — é a prova de que o gate trunca.

- [ ] **Step 2: Desligar os tetos na configuração**

Em `.golangci.yml`, acrescentar, com comentário explicando que teto silencioso torna a comparação de baseline inútil:

```yaml
issues:
  max-issues-per-linter: 0
  max-same-issues: 0
```

- [ ] **Step 3: Confirmar a linha de base estável**

Run: `golangci-lint run 2>&1 | tail -8` duas vezes seguidas.
Expected: **390**, idêntico nas duas, com os mesmos totais por linter.

- [ ] **Step 4: Registrar em `docs/debitos.md`**

Corrigir o débito 53, que afirma 114 — o número está errado e a razão importa mais que ele. Registrar item novo: as alegações de "0 apontamentos no código novo" do M2 e da Fase 6 foram feitas com o instrumento truncado e **não foram re-verificadas**.

- [ ] **Step 5: Commit**

```bash
git add .golangci.yml Makefile docs/debitos.md
git commit -m "Tira o teto do lint, que fazia a baseline esconder apontamento novo"
```

---

### Task 1b: `Stats()` não pode bloquear atrás do writer — **erro deste plano**

**Files:** Modify `internal/storage/store.go`, `internal/storage/store_checkpoint.go`; Test: `internal/storage/store_test.go`

O passo 3 da Task 1 prescreveu `s.wmu.Lock()` para ler `segTally` e `segSize`. Está **errado**, e o próprio godoc de `Stats` dizia por quê antes de ser alterado: `Stats` é seguro para leitores concorrentes porque o segmento corrente vem de um **espelho atômico**, não do writer.

`Checkpoint` segura `wmu` do início ao fim, e o histograma dele vai até 30s. Com o lock, todo scrape de `/metrics` e toda chamada de `/v1/stats` param atrás do checkpoint — ou seja, o painel cega exatamente sob a pressão que ele existe para observar. Num milestone cujo objetivo é tornar a curva de disco visível, é a regressão mais cara possível.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestStatsNaoBloqueiaAtrasDoCheckpoint fixa a propriedade que o godoc de
// Stats sempre prometeu: leitor concorrente não espera o writer. O checkpoint
// segura wmu do início ao fim e pode levar dezenas de segundos; um Stats que
// o aguarde cega o /metrics e o /v1/stats justamente sob pressão.
func TestStatsNaoBloqueiaAtrasDoCheckpoint(t *testing.T) {
	s := openStore(t, Options{})
	popula(t, s, 2000)

	liberado := make(chan struct{})
	s.SetTraceStepParaTeste(func(passo string) {
		if passo == "msyncLocations" {
			<-liberado // segura o checkpoint DENTRO da seção crítica do writer
		}
	})
	go func() { _ = s.Checkpoint() }()
	esperaCheckpointEntrar(t, s)

	pronto := make(chan Stats, 1)
	go func() { pronto <- s.Stats() }()
	select {
	case <-pronto:
	case <-time.After(2 * time.Second):
		close(liberado)
		t.Fatal("Stats bloqueou atrás do checkpoint: o painel cega sob pressão")
	}
	close(liberado)
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestStatsNaoBloqueiaAtrasDoCheckpoint -v`
Expected: FAIL por timeout de 2s — é o defeito.

- [ ] **Step 3: Implementar espelhos atômicos**

Trocar a iteração de mapa por três espelhos, no mesmo padrão que `curSegID` já usa neste arquivo:

```go
	// Espelhos atômicos dos agregados do tally. Existem para que Stats NUNCA
	// tome s.wmu: o Checkpoint o segura do início ao fim, e o histograma dele
	// vai até 30s — um Stats atrás desse lock apagaria /metrics e /v1/stats
	// exatamente sob a pressão que eles existem para mostrar. Mapa Go não se
	// lê sem sincronizar (fatal error, não corrida benigna), então o que
	// atravessa a fronteira é o agregado, não o mapa.
	segCount     atomic.Uint32
	liveBytesAll atomic.Uint64
	diskBytesAll atomic.Int64
```

O writer atualiza `segCount`/`liveBytesAll` nos mesmos pontos em que já mexe no tally (`applyRecord`, incremento e decremento), e o checkpoint recarrega os três na reconciliação com `ListSegments`. Custo no hot path: duas somas atômicas por Put, **zero alocação** — os gates de `AllocsPerRun` seguem valendo.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -run TestStats -v` → PASS
Run: `go test ./... && go test ./... -race && golangci-lint run`
Run: `go test ./benchmark/micro/ -run TestGate -v` — os gates de alocação **têm** de continuar verdes.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/
git commit -m "Devolve ao Stats a leitura que não espera o writer"
```

---

### Task 0b: os dois resíduos do gate de lint

**Files:** Modify `Makefile` (alvo `lint`), `planning/roadmap-m4-m5-m6.md`, `.golangci.yml`, `docs/debitos.md`; Create `internal/gates/lint_test.go`

Dois pontos que a Task 0 deixou registrados e não fechou.

> **Reprovada e reparada.** A primeira rodada entregou o alvo corrigido **sem prova de si mesmo**: nenhum teste executava `make lint`, e trocar `exit 1` por `exit 0` devolvia o defeito com um caractere sem a suíte piscar. Um gate que não é verificado é o mesmo carimbo que ele existe para fechar, um nível acima. O reparo é `internal/gates/lint_test.go`, que EXECUTA o alvo com o PATH controlado. A rodada também deixou o texto do fallback vivo, no PRESENTE, em `.golangci.yml` e no débito 53 — os dois lugares para onde o comentário do próprio alvo aponta — e instruía `@latest`, que é a versão do instrumento solta: outra dimensão da mesma configuração que os tetos, e por ela a comparação "390 antes, 390 depois" volta a mentir.

**O fallback silencioso.** O alvo `lint` cai para `go vet ./...` e sai com **código 0** quando o `golangci-lint` não está no PATH. É a outra metade da cegueira que o débito 53 descreve: um gate que **passa quando a ferramenta está ausente** não é gate — foi exatamente assim que o projeto atravessou o M1 achando que tinha lint. Deve falhar alto, nomeando o que instalar.

**O 114 sobrevivente.** [roadmap-m4-m5-m6.md:285](roadmap-m4-m5-m6.md:285) ainda cita os 114 como linha de base viva. O número foi retirado de circulação pela Task 0; deixá-lo num documento de planejamento é convidar a próxima revisão a se apoiar nele.

- [x] **Step 1: Provar o fallback**

Run: `PATH=/usr/bin:/bin make lint; echo "exit=$?"` (com o `golangci-lint` fora do PATH)
Expected: exit **0** — o gate passa sem ter rodado. É o defeito.

- [x] **Step 2: Fazer o alvo falhar alto**

O `else` do alvo passa a imprimir o que falta **no stderr** e sair **não-zero**, em vez de rodar `go vet` e devolver 0. `go vet` não substitui o lint: cobre um subconjunto e não conhece `errcheck`, `revive` nem `staticcheck`. A instrução de instalação nomeia uma versão **fixa**, não `@latest` — é a única instrução de instalação do linter no repositório, e é ela que decide qual binário mede a linha de base.

- [x] **Step 3: Confirmar**

Run: `PATH=/usr/bin:/bin make lint; echo "exit=$?"` → exit **não-zero**, com a mensagem nomeando o binário.
Run: `make lint` (PATH normal) → roda e devolve **390**.

> **Errata:** este Step prescrevia exit **1** e o número está errado. O `exit 1` é do **shell da receita**; o GNU make reporta `[lint] Error 1` e sai com **2**. O que o gate promete — e o que o teste afirma — é **não-zero**, não o literal 1. Escrever o número exato num critério de aceite sem medi-lo é a mesma classe de erro que os 114.

- [x] **Step 4: Corrigir o roadmap**

Substituir a citação dos 114 pela regra vigente (390, tetos em 0), apontando para o débito 59.

- [x] **Step 4b: Fixar o alvo com teste** *(reparo)*

`internal/gates/lint_test.go` executa `make lint` com o PATH substituído por um diretório temporário vazio e afirma: exit não-zero, a recusa **no stderr** nomeando o binário e o `go install`, nada de `go vet` em canal nenhum. Com um dublê de `golangci-lint` no PATH, afirma que o alvo o chama com os **dois tetos em 0** — a duplicação com o `.golangci.yml` é load-bearing e não estava fixada por nada. Mais dois: a versão instruída é fixa e bate com a que o débito 53 registra, e a linha de base citada no Makefile, no `.golangci.yml` e no roadmap é a mesma que o débito 53 mede.

- [x] **Step 4c: Fechar os resíduos de texto** *(reparo)*

`.golangci.yml` e o débito 53 afirmavam **no presente** um fallback que este trabalho removeu. Os dois vão para o passado, e o débito 53 ganha a errata da metade fechada mais o instrumento da medida (golangci-lint v2.12.2 / go1.26.5) ao lado dos 390.

- [x] **Step 5: Commit**

```bash
git add Makefile planning/roadmap-m4-m5-m6.md
git commit -m "Faz o alvo de lint falhar quando a ferramenta falta, em vez de passar"

# reparo da reprovação, em commit NOVO por cima:
git add internal/gates/ Makefile .golangci.yml docs/debitos.md planning/
git commit -m "Põe o alvo de lint sob teste e tira o fallback dos textos que ainda o afirmam"
```

---

### Task 1c: pinar o detector de espelhos — **Task 1b continua reprovada**

**Files:** Modify `internal/storage/store_liveness_espelho_test.go`, `internal/storage/export_test.go`; possivelmente `internal/storage/store_recovery.go`

O núcleo da Task 1b está certo: `Stats()` lê atômicos e não toma mais `wmu`. O que a rodada de reparo acrescentou — `conferaEspelhos()` em `store_checkpoint.go`, mais a semeadura de `segCount`/`liveBytesAll` em `recoverFrom` — **não é fixado por teste nenhum**. A revisão adversarial derrubou quatro mutantes:

| Mutante | O que faz | Suíte |
|---|---|---|
| **M7** | Remove a chamada `s.conferaEspelhos()` do `checkpoint()` | verde |
| **M8** | Tira o termo `disco == s.diskBytesAll.Load()` da guarda | verde |
| **M14** | Move a conferência para **depois** da recarga dos espelhos | verde |
| **M16** | M14 + remover a semeadura de `recoverFrom` | verde, **com o underflow de volta** |

O **M16** é o grave. A ordem — conferir **antes** de tocar nos espelhos — é a única propriedade que faz o detector valer alguma coisa, e é o argumento central da mensagem de commit. Com a conferência depois da recarga ela nunca acha nada, porque a recarga acabou de consertar o que ela iria comparar. Somado à remoção da semeadura, o underflow de `uint64` que o próprio commit mediu (`liveBytesAll = 18446744073709551544` contra 568 nos tallies) volta à produção sem uma única falha.

Um detector que ninguém prova disparando é pior que detector nenhum: lê-se como proteção e não protege.

- [ ] **Step 1: Escrever o teste que mata M7, M8 e M14**

Um subteste por espelho. O hook de corrupção entra em `export_test.go`, no estilo do `zeraTallyDoSegmento` que já existe lá.

```go
// TestConferenciaDeEspelhosAcusaCadaEspelhoDivergente prova que o detector
// DISPARA, e prova para os três espelhos. Sem isto ele é decoração: remover a
// chamada do checkpoint, ou cegá-la para um dos termos, deixa a suíte verde —
// medido em três mutantes na revisão da Task 1b.
func TestConferenciaDeEspelhosAcusaCadaEspelhoDivergente(t *testing.T) {
	for _, c := range []struct {
		nome     string
		corrompe func(*Store)
	}{
		{"segCount", func(s *Store) { s.segCount.Add(1) }},
		{"liveBytesAll", func(s *Store) { s.liveBytesAll.Add(4096) }},
		{"diskBytesAll", func(s *Store) { s.diskBytesAll.Add(4096) }},
	} {
		t.Run(c.nome, func(t *testing.T) {
			s := openStore(t, Options{})
			popula(t, s, 50)
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint de base: %v", err)
			}
			antes := s.EspelhosDivergentesForTest()
			s.CorrompeEspelhoParaTeste(c.corrompe) // sob s.wmu
			if err := s.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if got := s.EspelhosDivergentesForTest(); got != antes+1 {
				t.Errorf("divergências = %d, queria %d: o detector não acusou %s", got, antes+1, c.nome)
			}
		})
	}
}
```

- [ ] **Step 2: Escrever o teste que mata M16 — oráculo DIRETO da recovery**

O oráculo atual é indireto: `TestRecoverySemeiaOEspelhoQueOStatsPublica` só pega o defeito **através** do contador de divergência, e sua asserção de `LiveBytesUpperBound` passa nas duas implementações, porque o `Checkpoint` incondicional do `Open` recarrega o absoluto antes de qualquer leitura. É preciso ler os espelhos **ao fim de `recoverFrom`, antes desse Checkpoint**.

```go
// TestEspelhosSaemDaRecoveryJaSemeados lê os espelhos ANTES do Checkpoint que
// o Open dispara. Sem esse ponto de leitura o defeito é invisível: a recarga
// conserta o absoluto e a asserção de Stats passa dos dois jeitos — foi assim
// que o underflow de uint64 (liveBytesAll = 1,8e19 contra 568 nos tallies)
// sobreviveu à suíte inteira no mutante composto M16.
func TestEspelhosSaemDaRecoveryJaSemeados(t *testing.T) {
	dir := datasetComManifest(t, 200)
	var noFimDaRecovery espelhos
	s := openStoreCom(t, dir, Options{
		TraceRecoveryParaTeste: func(e espelhos) { noFimDaRecovery = e },
	})
	defer s.Close()
	if noFimDaRecovery.vivo == 0 || noFimDaRecovery.vivo > uint64(noFimDaRecovery.disco) {
		t.Errorf("liveBytesAll = %d ao fim da recovery, fora de (0, %d]: espelho não semeado",
			noFimDaRecovery.vivo, noFimDaRecovery.disco)
	}
	if noFimDaRecovery.segmentos == 0 {
		t.Error("segCount = 0 ao fim da recovery: espelho não semeado")
	}
}
```

- [ ] **Step 3: Rodar e confirmar que os dois falham**

Run: `go test ./internal/storage/ -run 'TestConferenciaDeEspelhos|TestEspelhosSaemDaRecovery' -v`
Expected: FAIL — os hooks de teste ainda não existem.

- [ ] **Step 4: Implementar só os hooks**

`CorrompeEspelhoParaTeste` e o ponto de trace ao fim de `recoverFrom`. **Nenhuma mudança de comportamento de produção** — o código já está correto; o que falta é o oráculo.

- [ ] **Step 5: Re-matar os quatro mutantes, um a um**

Aplicar M7, M8, M14 e M16 numa cópia descartável (`git archive` para fora do repo) e confirmar que **cada um** agora fica vermelho. Um mutante que sobreviver reabre a tarefa.

- [ ] **Step 6: Decidir sobre `espelhosDivergentes`**

O contador só tem `Log.Warn` e acessor de teste — nenhuma exposição ao operador. Num milestone cujo objetivo é o §92, ou ele ganha série em `metricas_dataset.go`, ou uma linha em `docs/debitos.md` dizendo quando ganha. **Escolher uma e executar**, não deixar as duas em aberto.

- [ ] **Step 7: Commit**

```bash
git add internal/storage/
git commit -m "Prova que a conferência de espelhos dispara, em vez de supô-lo"
```

---

### Task 2b: fixar `storage_bytes` contra o disco real — **Task 2 continua reprovada**

**Files:** Modify `internal/server/metricas_dataset_test.go`, `internal/server/query_test.go` (helper `servidorComDados`)

A revisão adversarial provou por mutação: trocar o valor publicado de `st.StorageBytes` para `st.LiveBytesUpperBound` — ou para `st.RowCount` — deixa **a suíte inteira verde**. A única asserção é `bytes.Valor > 0`, que qualquer número positivo satisfaz. É o defeito do débito 6 outra vez: medir sem oráculo.

O mutante sobrevivente é exatamente a "métrica que mente" que o comentário do arquivo declara estar evitando — publicar o tally de liveness sob um nome que promete tamanho de arquivo.

Bloqueio a remover: `servidorComDados` ([query_test.go:98](../internal/server/query_test.go:98)) descarta o ambiente e não expõe o diretório do dataset.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestStorageBytesBateComOsArquivosEmDisco é o oráculo que faltava: a série
// tem de casar com a soma dos segment-*.dat, não apenas ser positiva. Sem
// isto, publicar o tally de liveness no lugar do tamanho de arquivo passa
// despercebido — e era esse o mutante que sobrevivia à suíte inteira.
func TestStorageBytesBateComOsArquivosEmDisco(t *testing.T) {
	srv, dir := servidorComDadosEDiretorio(t)
	amostra, ok := achaAmostra(coletaAmostras(t, srv), metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}
	var emDisco int64
	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		emDisco += fi.Size()
	}
	if emDisco == 0 {
		t.Fatal("nenhum segmento em disco: o teste não estaria provando nada")
	}
	if int64(amostra.Valor) != emDisco {
		t.Errorf("storage_bytes = %d, arquivos em disco somam %d", int64(amostra.Valor), emDisco)
	}
}
```

- [ ] **Step 2: Provar que o teste mata o mutante**

Trocar `Valor: float64(st.StorageBytes)` por `float64(st.LiveBytesUpperBound)` em [metricas_dataset.go:95](../internal/server/metricas_dataset.go:95), rodar o teste, **confirmar FAIL**, e reverter. Um teste que não mata este mutante não fecha a Task 2.

- [ ] **Step 3: Implementar**

Expor o diretório no helper (`servidorComDadosEDiretorio`, com `servidorComDados` delegando a ele para não tocar nos chamadores existentes).

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/server/
git commit -m "Ancora o storage_bytes no disco real, não num maior-que-zero"
```

---

## Fase A — tornar a curva visível

Entrega sozinha e vai primeiro. Com 2 a 3 dias de margem, ter o gráfico é o que separa uma janela de manutenção de um incidente.

### Task 1: `Stats` passa a contar disco e liveness

**Files:**
- Modify: `internal/storage/store.go` (struct `Stats` ~linha 99; método `Stats()` ~linha 1231)
- Test: `internal/storage/store_test.go`

**Interfaces:**
- Produces: `Stats{RowCount, SegmentsTotal, CurrentSegmentID, StorageBytes, LiveBytesUpperBound, PrimaryLoadFactor, OverlayLen}` — consumido pelas Tasks 2, 8 e 10.

Hoje `SegmentsTotal` é derivado do ID corrente com o comentário `// IDs monotônicos desde 1, sem remoção no M1` ([store.go:1245](../internal/storage/store.go:1245)) — premissa que a compaction quebra. E não há campo nenhum de bytes.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestStatsContaBytesDeDiscoEDeLiveness prova que Stats deixou de derivar a
// contagem de segmentos do ID corrente. A compaction remove segmentos, e o ID
// é monotônico: continuar derivando faria o painel reportar segmento que não
// existe mais.
func TestStatsContaBytesDeDiscoEDeLiveness(t *testing.T) {
	st := abreStoreParaTeste(t)
	for i := range 100 {
		key := []byte(fmt.Sprintf("k-%03d", i))
		if _, _, err := st.Put(key, 1, 0, payloadDeTeste(t), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	got := st.Stats()
	if got.StorageBytes <= 0 {
		t.Errorf("StorageBytes = %d, queria > 0", got.StorageBytes)
	}
	if got.SegmentsTotal != 1 {
		t.Errorf("SegmentsTotal = %d, queria 1", got.SegmentsTotal)
	}
	if got.LiveBytesUpperBound == 0 || got.LiveBytesUpperBound > uint64(got.StorageBytes) {
		t.Errorf("LiveBytesUpperBound = %d fora de (0, %d]", got.LiveBytesUpperBound, got.StorageBytes)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestStatsContaBytesDeDiscoEDeLiveness -v`
Expected: FAIL — `got.StorageBytes undefined`.

- [ ] **Step 3: Implementar**

Acrescentar ao struct `Stats`:

```go
	// StorageBytes é a soma EXATA do tamanho dos arquivos de segmento. É
	// exato mesmo com o tally de liveness sendo limite superior (débitos 38
	// e 45), porque não depende dele: é tamanho de arquivo. É a série que
	// antecede o ENOSPC.
	StorageBytes int64
	// LiveBytesUpperBound é a soma do tally por segmento. NÃO é contagem
	// exata: é limite SUPERIOR, pelo mesmo contrato de
	// checkpoint.SegmentInfo. O nome carrega a ressalva de propósito —
	// chamá-lo de LiveBytes convidaria a próxima revisão a tratá-lo como
	// exato e a dimensionar compaction por ele.
	LiveBytesUpperBound uint64
```

Em `Stats()`, trocar a derivação por contagem real do tally (que o checkpoint já mantém alinhado com `ListSegments`) e acumular os bytes. `s.segTally` é writer-only, então a leitura acontece sob `s.wmu`:

```go
	s.wmu.Lock()
	var (
		storageBytes int64
		liveBytes    uint64
		segmentos    uint32
	)
	for id, t := range s.segTally {
		segmentos++
		liveBytes += t.liveBytes
		if info, ok := s.segSize[id]; ok {
			storageBytes += info
		}
	}
	s.wmu.Unlock()
```

`s.segSize map[uint32]int64` é novo, alimentado no mesmo ponto do checkpoint que já chama `ListSegments` ([store_checkpoint.go:103](../internal/storage/store_checkpoint.go:103)) e semeado pela recovery do manifest.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -run TestStatsContaBytes -v` → PASS
Run: `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/storage/store.go internal/storage/store_checkpoint.go internal/storage/store_test.go
git commit -m "Dá ao Stats a conta de disco que a compaction vai precisar"
```

---

### Task 2: publicar `nishidb_storage_bytes` e `nishidb_storage_garbage_bytes`

**Files:**
- Modify: `internal/server/metricas_dataset.go`
- Test: `internal/server/metricas_dataset_test.go`

O comentário atual ([metricas_dataset.go:16](../internal/server/metricas_dataset.go:16)) deixa os **três** de fora com um argumento correto — tally impreciso publicado como exato é métrica que mente. **O argumento não alcança `storage_bytes`**, que é tamanho de arquivo, e alcança `garbage_bytes` só pela metade: como `live` é limite superior, `total - live` é limite **inferior** do garbage.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestMetricaDeDiscoEhPublicadaComSemanticaDeLimite prova que storage_bytes
// sai como número exato e que garbage_bytes se declara limite INFERIOR no
// texto de ajuda. Publicar o segundo sem a ressalva repetiria o defeito que o
// comentário do M2 evitou.
func TestMetricaDeDiscoEhPublicadaComSemanticaDeLimite(t *testing.T) {
	srv := servidorDeTeste(t)
	amostras := coletaAmostras(t, srv)
	bytes, ok := achaAmostra(amostras, metrics.MetStorageBytes)
	if !ok {
		t.Fatal("nishidb_storage_bytes não foi publicada")
	}
	if bytes.Valor <= 0 {
		t.Errorf("storage_bytes = %v, queria > 0", bytes.Valor)
	}
	lixo, ok := achaAmostra(amostras, metrics.MetStorageGarbageBytes)
	if !ok {
		t.Fatal("nishidb_storage_garbage_bytes não foi publicada")
	}
	if !strings.Contains(lixo.Ajuda, "pelo menos") {
		t.Errorf("Ajuda = %q; precisa declarar que é limite inferior", lixo.Ajuda)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/server/ -run TestMetricaDeDisco -v` → FAIL: métrica ausente.

- [ ] **Step 3: Implementar**

No coletor existente, acrescentar as duas amostras e **reescrever o comentário do topo** para dizer o que ficou de fora e por quê (só `live_bytes`, até a compaction reconstruir o tally):

```go
			metrics.Amostra{
				Familia: metrics.MetStorageBytes,
				Nome:    metrics.MetStorageBytes,
				Ajuda:   "bytes ocupados pelos segmentos do dataset em disco",
				Tipo:    metrics.TipoMedidor,
				Labels:  rotulo,
				Valor:   float64(st.StorageBytes),
			},
			metrics.Amostra{
				Familia: metrics.MetStorageGarbageBytes,
				Nome:    metrics.MetStorageGarbageBytes,
				Ajuda:   "bytes de garbage nos segmentos: há PELO MENOS isto de lixo (limite inferior, porque a liveness é limite superior — débitos 38 e 45)",
				Tipo:    metrics.TipoMedidor,
				Labels:  rotulo,
				Valor:   float64(uint64(st.StorageBytes) - min(uint64(st.StorageBytes), st.LiveBytesUpperBound)),
			},
```

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/server/ -v` → PASS
Run: `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/server/metricas_dataset.go internal/server/metricas_dataset_test.go
git commit -m "Publica a curva de disco, que é o alarme que antecede o ENOSPC"
```

---

## Fase B — a compaction

### Task 3: config `storage.compaction`

**Files:**
- Modify: `internal/config/config.go` (struct `Storage` ~linha 39; `validate` ~linha 166)
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces: `Config.Compaction() (habilitada bool, minimumLiveRatio float64)` — consumido pela Task 7.

Doutrina do débito 49: forma validada no **parse**, comportamento na **ativação**. Default do §53: `minimumLiveRatio: 0.50`.

- [ ] **Step 1: Escrever os testes que falham**

```go
func TestCompactionHerdaODefaultDoParagrafo53(t *testing.T) {
	cfg := carregaConfig(t, "storage:\n  path: /tmp/x\n")
	if _, ratio := cfg.Compaction(); ratio != 0.50 {
		t.Errorf("minimumLiveRatio = %v, queria 0.50 (§53)", ratio)
	}
}

// TestMinimumLiveRatioForaDeZeroUmEhRecusado: ratio >= 1 elegeria TODO
// segmento, inclusive os cheios de dado vivo — compaction perpétua que só
// queima I/O. Ratio <= 0 nunca elegeria nenhum, e a chave mentiria.
func TestMinimumLiveRatioForaDeZeroUmEhRecusado(t *testing.T) {
	for _, v := range []string{"0", "1", "1.5", "-0.2"} {
		yaml := "storage:\n  path: /tmp/x\n  compaction:\n    minimumLiveRatio: " + v + "\n"
		if _, err := carregaConfigErr(t, yaml); err == nil {
			t.Errorf("minimumLiveRatio: %s foi aceito; queria recusa nomeando a chave", v)
		}
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/config/ -run TestCompaction -run TestMinimumLiveRatio -v` → FAIL.

- [ ] **Step 3: Implementar**

Acrescentar ao struct `Storage`:

```go
		Compaction struct {
			Enabled          *bool    `yaml:"enabled"`
			MinimumLiveRatio *float64 `yaml:"minimumLiveRatio"`
		} `yaml:"compaction"`
```

`Enabled` é ponteiro pelo mesmo motivo de `server.enabled`: ausente vale **true** — o §78 traz a chave ligada no exemplo, e exigir declaração explícita para ter compaction inverteria o default seguro. Validar `MinimumLiveRatio` na faixa aberta `(0, 1)`, com chave e valor no erro.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/config/ -v` → PASS
Run: `go test ./... && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "Dá leitor ao storage.compaction, que era aceito e ignorado"
```

---

### Task 4: `reapendaSeVivo` — o núcleo, e a corrida que ele fecha

**Files:**
- Create: `internal/storage/compaction.go`
- Test: `internal/storage/compaction_test.go`

**Interfaces:**
- Consumes: `applyRecord(recordApply)` ([store.go:783](../internal/storage/store.go:783)), `rowindex.Unpack`, `encoding.BuildRecord`.
- Produces: `func (s *Store) reapendaSeVivo(seg, off uint32, rec types.Record) (copiado bool, n int, err error)` — consumido pela Task 5.

Esta é a tarefa em que a corretude se decide. Verificar liveness fora do lock e apendar depois faz o payload **antigo** vencer um update concorrente.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente é o teste que
// justifica a existência de reapendaSeVivo. Se a verificação de liveness e o
// append não estiverem na MESMA seção crítica do writer, um update chegado no
// meio perde para a cópia — e o dataset volta no tempo em silêncio.
func TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente(t *testing.T) {
	st := abreStoreParaTeste(t)
	key := []byte("k-1")
	if _, _, err := st.Put(key, 1, 0, payloadCom(t, "v", "antigo"), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	packed, _ := st.LocationDeTeste(rowIDDe(t, st, key))
	seg, off, _ := rowindex.Unpack(packed)

	// O update entra ANTES da tentativa de cópia: a location deixa de nomear
	// (seg, off) e a cópia tem de ser recusada.
	if _, _, err := st.Put(key, 2, 0, payloadCom(t, "v", "novo"), nil); err != nil {
		t.Fatalf("Put do update: %v", err)
	}
	rec := recordEm(t, st, seg, off)
	copiado, _, err := st.ReapendaSeVivoParaTeste(seg, off, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if copiado {
		t.Fatal("a cópia foi aceita depois do update: o payload antigo venceria o novo")
	}
	got := valorDe(t, st, key, "v")
	if got != "novo" {
		t.Errorf("valor = %q, queria \"novo\"", got)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestCopyForwardNaoRessuscita -v` → FAIL: `ReapendaSeVivoParaTeste undefined`.

- [ ] **Step 3: Implementar**

Criar `internal/storage/compaction.go`:

```go
// reapendaSeVivo re-apenda o record de (seg, off) no segmento corrente se, e
// somente se, a location da row ainda o nomear.
//
// A verificação e o append acontecem na MESMA seção crítica de s.wmu, e isso
// é a corretude inteira desta tarefa: entre ler a location e apendar, a
// ingestão pode ter gravado um update. Fora do lock, a cópia do payload
// ANTIGO seria publicada depois do novo e o dataset voltaria no tempo, sem
// erro e sem log.
//
// Liveness aqui é "a location nomeia este record" (§55), não "a row é
// visível": record TOMBSTONED também é copiado, porque a location aponta para
// ele e não copiá-lo a deixaria pendurada num arquivo removido. Recuperar
// tombstone exige limpar location e primary, que é trabalho do TTL (Fase 9).
func (s *Store) reapendaSeVivo(seg, off uint32, rec types.Record) (bool, int, error) {
	if err := s.entra(); err != nil {
		return false, 0, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return false, 0, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	packed, had := s.locs.Get(rec.RowID)
	if !had {
		return false, 0, nil
	}
	curSeg, curOff, flags := rowindex.Unpack(packed)
	if curSeg != seg || curOff != off {
		return false, 0, nil // a row já migrou sozinha; copiar seria regredir
	}
	// flags vêm da LOCATION, não do record: a location é a autoridade de
	// visibilidade, e é ela que o §44 consulta.
	buf, err := encoding.BuildRecord(s.compactBuf[:0], rec.RowID, rec.Sequence, flags, rec.Key, rec.Payload)
	s.compactBuf = buf
	if err != nil {
		return false, 0, err
	}
	novoSeg, novoOff, err := s.segW.Append(buf)
	if err != nil {
		return false, 0, s.failWriter(err)
	}
	s.curSegID.Store(novoSeg)
	// prevPayload é o PRÓPRIO payload: é o que faz o decremento de liveness
	// do applyRecord descontar exatamente o tamanho do record de origem. Os
	// bitmaps ficam sem trabalho pela mesma razão — o SET liga os bits que já
	// estavam ligados e o CLEAR não acha diferença.
	if err := s.applyRecord(recordApply{
		rowID:       rec.RowID,
		key:         rec.Key,
		seq:         rec.Sequence,
		flags:       flags,
		payload:     rec.Payload,
		prevPayload: rec.Payload,
		hadPrevious: true,
		countsAsNew: false,
		seg:         novoSeg,
		off:         novoOff,
		recLen:      len(buf),
	}); err != nil {
		return false, 0, s.failWriter(err)
	}
	return true, len(buf), nil
}
```

Expor `ReapendaSeVivoParaTeste`, `LocationDeTeste` e `recordEm` em `internal/storage/export_test.go`, seguindo o que já existe lá.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -run TestCopyForward -v` → PASS
Run: `go test ./internal/storage/ -race -count=1` — o `-race` é o que prova a seção crítica.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction.go internal/storage/compaction_test.go internal/storage/export_test.go
git commit -m "Copia record vivo para frente sem deixar update concorrente perder"
```

---

### Task 5: `compactaSegmento` — varredura e o tally que a varredura zera

**Files:**
- Modify: `internal/storage/compaction.go`
- Test: `internal/storage/compaction_test.go`

**Interfaces:**
- Produces: `func (s *Store) compactaSegmento(ctx context.Context, seg uint32) (copiados uint64, lidos, escritos int64, err error)` — consumido pelas Tasks 6 e 7.

O ponto sutil: os decrementos do `applyRecord` **não** levam o tally da origem a zero, porque ele parte de um limite superior (débitos 38 e 45) e o resíduo órfão sobrevive a qualquer número de decrementos. Quem zera é a varredura, que viu todos os records — exatamente o que [manifest.go:73](../internal/storage/checkpoint/manifest.go:73) antecipa.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestVarreduraCompletaZeraOTallyMesmoComResiduoOrfao ataca o débito 38 de
// frente: injeta o resíduo que o replay deixa permanente e prova que a
// compaction NÃO tenta chegar a zero por decremento — ela zera por autoridade
// da varredura, que é a única leitura exata que existe.
func TestVarreduraCompletaZeraOTallyMesmoComResiduoOrfao(t *testing.T) {
	st := abreStoreParaTeste(t)
	popula(t, st, 50)
	forcaRotacaoDeSegmento(t, st)
	inflaTallyDeTeste(t, st, 1, 7, 441) // 7 records / 441 bytes órfãos

	if _, _, _, err := st.CompactaSegmentoParaTeste(context.Background(), 1); err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	live, recs := st.TallyDeTeste(1)
	if live != 0 || recs != 0 {
		t.Errorf("tally do segmento 1 = %d records / %d bytes, queria 0/0", recs, live)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestVarreduraCompletaZera -v` → FAIL.

- [ ] **Step 3: Implementar**

```go
// compactaSegmento varre seg record a record e copia para frente os que a
// location ainda nomeia (§55). Streaming puro: um record por vez, buffer
// reutilizado pelo ScanFrom, nada proporcional ao tamanho do segmento (§54).
//
// Terminada a varredura SEM erro e SEM cancelamento, nenhuma location aponta
// para seg — e é por isso que o tally dele é ZERADO aqui em vez de deduzido
// dos decrementos. O tally é limite superior por construção (débitos 38 e
// 45): os decrementos param no resíduo órfão do replay e nunca alcançam zero.
// A varredura é a única leitura exata do segmento que existe no produto, e
// esta é a linha que fecha os dois débitos.
func (s *Store) compactaSegmento(ctx context.Context, seg uint32) (uint64, int64, int64, error) {
	var (
		copiados          uint64
		lidos, escritos   int64
	)
	_, err := s.segR.ScanFrom(seg, segment.HeaderSize, func(off uint32, rec types.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		lidos += int64(encoding.RecordHeaderSize + len(rec.Key) + len(rec.Payload))
		copiou, n, err := s.reapendaSeVivo(seg, off, rec)
		if err != nil {
			return err
		}
		if copiou {
			copiados++
			escritos += int64(n)
		}
		return nil
	})
	if err != nil {
		return copiados, lidos, escritos, err
	}
	s.wmu.Lock()
	if t := s.segTally[seg]; t != nil {
		t.liveRecords = 0
		t.liveBytes = 0
	}
	s.wmu.Unlock()
	return copiados, lidos, escritos, nil
}
```

O cancelamento sai pelo erro do callback, que o `ScanFrom` propaga — e o tally **não** é zerado nesse caminho, porque a varredura não foi completa.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -run TestVarredura -v` → PASS
Run: `go test ./... && go test ./... -race`

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction.go internal/storage/compaction_test.go
git commit -m "Zera o tally pela varredura, que é a única leitura exata do segmento"
```

---

### Task 6: aposentar o segmento, e as duas janelas de crash

**Files:**
- Modify: `internal/storage/compaction.go`, `internal/storage/store_recovery.go`
- Create: `internal/storage/compaction_crash_test.go`

**Interfaces:**
- Produces: `func (s *Store) aposentaSegmento(seg uint32) error`.

A sequência do D5, com o que cada crash deixa:

| Ponto | Estado em disco | Reparo |
|---|---|---|
| Depois do `Checkpoint` de locations, antes do rename | Origem íntegra, listada, tally zerado | Nada a fazer: a próxima passada varre, não acha nada vivo e aposenta |
| **Depois do rename, antes do checkpoint** | Manifest lista `seg`, arquivo é `.retired` | **`Open` renomeia de volta** — segmento listado tem de existir ([store_recovery.go:57](../internal/storage/store_recovery.go:57)) |
| Depois do checkpoint, antes do unlink | Manifest não lista `seg`, `.retired` órfão em disco | **`Open` varre e remove** `.retired` não listado |

- [ ] **Step 1: Escrever os testes que falham**

```go
// TestCrashEntreORenameEOCheckpointRestauraOSegmento cobre a única janela em
// que a aposentadoria produz estado inconsistente: o manifest ainda lista o
// segmento e o arquivo já não tem nome canônico. Sem o reparo, o Open sai com
// ErrDatasetCorrupt num dataset que está inteiro.
func TestCrashEntreORenameEOCheckpointRestauraOSegmento(t *testing.T) {
	dir := datasetDeTeste(t, 200)
	renomeiaParaRetired(t, dir, 1)

	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v — o reparo do .retired não rodou", err)
	}
	defer st.Close()
	if _, err := os.Stat(filepath.Join(dir, "data", "segment-00000001.dat")); err != nil {
		t.Errorf("segmento 1 não foi restaurado: %v", err)
	}
}

// TestRetiredOrfaoNaoListadoEhRemovidoNoOpen: crash entre o checkpoint e o
// unlink deixa bytes em disco que ninguém mais referencia. Sem a varredura,
// a compaction devolveria menos disco a cada ciclo do que promete.
func TestRetiredOrfaoNaoListadoEhRemovidoNoOpen(t *testing.T) {
	dir := datasetDeTeste(t, 200)
	criaRetiredOrfao(t, dir, 99)

	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	if _, err := os.Stat(filepath.Join(dir, "data", "segment-00000099.dat.retired")); !os.IsNotExist(err) {
		t.Error(".retired órfão sobreviveu ao Open")
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestCrashEntreORename -run TestRetiredOrfao -v` → FAIL.

- [ ] **Step 3: Implementar**

Em `compaction.go`:

```go
// sufixoAposentado marca o segmento cujos records vivos já migraram. O nome
// deixa de ser canônico de propósito: ListSegments ignora não-canônicos
// (segment.go:252), então o próximo manifest já não o lista — sem que os
// bytes tenham saído do disco. É o que fecha a janela entre o unlink e o
// manifest, em que o segmento listado e ausente vira ErrDatasetCorrupt.
const sufixoAposentado = ".retired"

func (s *Store) aposentaSegmento(seg uint32) error {
	dir := filepath.Join(s.opts.Dir, "data")
	canonico := filepath.Join(dir, fmt.Sprintf("segment-%08d.dat", seg))
	aposentado := canonico + sufixoAposentado
	// 1. locations duráveis ANTES de qualquer coisa: até o ApplyOverlay, as
	//    republicações da varredura vivem só no heap.
	if err := s.Checkpoint(); err != nil {
		return err
	}
	// 2. rename: sai do listing, continua em disco.
	if err := os.Rename(canonico, aposentado); err != nil {
		return fmt.Errorf("storage: aposentar segmento %d: %w", seg, err)
	}
	// 3. checkpoint de novo: o manifest é reconstruído do ListSegments, que
	//    já não enxerga o arquivo.
	if err := s.Checkpoint(); err != nil {
		return err
	}
	// 4. só agora os bytes saem.
	if err := os.Remove(aposentado); err != nil {
		return fmt.Errorf("storage: remover segmento aposentado %d: %w", seg, err)
	}
	return syncDir(dir)
}
```

Em `store_recovery.go`, antes da checagem de segmento listado e ausente, rodar `reparaAposentados(dir, m.Segments)`: restaura o `.retired` cujo ID está no manifest, remove o que não está.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -run TestCrash -run TestRetired -v` → PASS
Run: `go test ./... && go test ./... -race`

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction.go internal/storage/store_recovery.go internal/storage/compaction_crash_test.go
git commit -m "Aposenta o segmento por rename, e ensina o Open a reparar as duas janelas"
```

---

### Task 7: seleção de candidatos e o ciclo de vida da goroutine

**Files:**
- Modify: `internal/storage/compaction.go`, `internal/storage/store.go` (`Close` ~linha 1268), `internal/server/server.go`
- Test: `internal/storage/compaction_test.go`

**Interfaces:**
- Produces: `func (s *Store) IniciaCompaction(ctx context.Context, ratio float64)` e `func (s *Store) ParaCompaction()`.

- [ ] **Step 1: Escrever os testes que falham**

```go
// TestSegmentoCorrenteNuncaEhCandidato: compactar o segmento em que se está
// apendando é copiar dentro de si mesmo — o destino é a própria origem.
func TestSegmentoCorrenteNuncaEhCandidato(t *testing.T) {
	st := abreStoreParaTeste(t)
	popula(t, st, 10)
	for _, c := range st.CandidatosParaTeste(0.99) {
		if c == st.Stats().CurrentSegmentID {
			t.Fatalf("segmento corrente %d foi eleito candidato", c)
		}
	}
}

// TestCloseCancelaEAguardaACompaction: Close toma vida.Lock(), que só é
// concedido sem operação em voo. Se a goroutine não for cancelada e aguardada
// ANTES disso, Close trava pelo tempo da varredura — ou pior, o Close vence e
// desmapeia o índice sob os pés dela.
func TestCloseCancelaEAguardaACompaction(t *testing.T) {
	st := abreStoreParaTeste(t)
	popula(t, st, 20000)
	st.IniciaCompaction(context.Background(), 0.99)

	feito := make(chan error, 1)
	go func() { feito <- st.Close() }()
	select {
	case err := <-feito:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close não retornou: a compaction não foi cancelada nem aguardada")
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/storage/ -run TestSegmentoCorrente -run TestCloseCancela -v` → FAIL.

- [ ] **Step 3: Implementar**

Seleção pelo §53, lendo o tally como limite superior — o que apenas **adia**, nunca antecipa indevidamente:

```go
// candidatos elege segmentos cujo liveBytes/sizeBytes fica abaixo do
// minimumLiveRatio (§53). Lê o tally como LIMITE SUPERIOR: um segmento cuja
// liveness está superestimada simplesmente não é eleito ainda, que é a
// direção segura registrada no débito 38 — subestimar faria a compaction
// descartar dado vivo, superestimar só adia.
func (s *Store) candidatos(ratio float64) []uint32 {
	corrente := s.curSegID.Load()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var out []uint32
	for id, t := range s.segTally {
		if id == corrente {
			continue // D6: não se compacta o segmento em que se apenda
		}
		tam, ok := s.segSize[id]
		if !ok || tam <= 0 {
			continue
		}
		if float64(t.liveBytes)/float64(tam) < ratio {
			out = append(out, id)
		}
	}
	slices.Sort(out) // determinismo: mesma eleição produz a mesma ordem
	return out
}
```

O laço de background dorme entre passadas, respeita `ctx`, e **para na primeira falha do writer** (modo falho sticky). Em `Close`, antes do `vida.Lock()`:

```go
	// A compaction é cancelada e AGUARDADA antes do quiesce: Close só recebe
	// vida.Lock() quando nenhuma operação está em voo, e a varredura é uma
	// operação longa. Sem o join aqui, Close esperaria a passada inteira.
	s.ParaCompaction()
```

Ligar no servidor a partir da config da Task 3.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./internal/storage/ -v` → PASS
Run: `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction.go internal/storage/store.go internal/server/server.go internal/storage/compaction_test.go
git commit -m "Elege candidato pelo §53 e dá à compaction um ciclo de vida que o Close encerra"
```

---

### Task 8: as três séries de compaction do §92

**Files:**
- Modify: `internal/storage/compaction.go`, `internal/server/metricas_dataset.go`
- Test: `internal/server/metricas_dataset_test.go`

`MetCompactionRunning`, `MetCompactionBytesRead` e `MetCompactionBytesWritten` já existem como nome sem alimentador ([nomes.go:48](../internal/metrics/nomes.go:48)), com o comentário `// sem alimentador até o M4` — que sai nesta tarefa.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestSeriesDeCompactionGanhamAlimentador prova que as três séries do §92
// deixaram de ser nome sem número. bytes_read e bytes_written são
// CONTADORES: o painel precisa da taxa, e um medidor que zera a cada passada
// não a produz.
func TestSeriesDeCompactionGanhamAlimentador(t *testing.T) {
	srv := servidorComCompaction(t)
	rodaUmaPassada(t, srv)
	amostras := coletaAmostras(t, srv)
	for _, nome := range []string{
		metrics.MetCompactionRunning,
		metrics.MetCompactionBytesRead,
		metrics.MetCompactionBytesWritten,
	} {
		a, ok := achaAmostra(amostras, nome)
		if !ok {
			t.Errorf("%s não foi publicada", nome)
			continue
		}
		if nome != metrics.MetCompactionRunning && a.Tipo != metrics.TipoContador {
			t.Errorf("%s = %v, queria contador", nome, a.Tipo)
		}
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha**

Run: `go test ./internal/server/ -run TestSeriesDeCompaction -v` → FAIL.

- [ ] **Step 3: Implementar**

`running` é medidor 0/1; `bytes_read`/`bytes_written` são contadores monotônicos alimentados pelos retornos de `compactaSegmento`. Remover o comentário `// sem alimentador até o M4` de `nomes.go`.

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction.go internal/server/metricas_dataset.go internal/metrics/nomes.go internal/server/metricas_dataset_test.go
git commit -m "Dá alimentador às três séries de compaction do §92"
```

---

## Fase C — operação

### Task 9: débito 56 — checkpoint por tempo no laço do consumidor

**Files:**
- Modify: `internal/kafka/writer.go` (~linha 269)
- Test: `internal/kafka/writer_test.go`

O checkpoint — e o commit de offset que vem depois dele (§48) — é disparado **por evento**. Terminado o consumo, o offset commitado para no último checkpoint e o `nishidb_kafka_lag` congela. Medido na aceite da Fase 6: **lag de 499.999 parado com o consumidor em dia**.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestCheckpointPorTempoDestravaOLagComIngestaoOciosa reproduz o débito 56:
// sem evento novo, o checkpoint por contagem nunca dispara e o offset
// commitado congela. Usa relógio injetado — esperar o intervalo de verdade
// tornaria o teste lento e instável.
func TestCheckpointPorTempoDestravaOLagComIngestaoOciosa(t *testing.T) {
	w, relogio := writerComRelogioDeTeste(t, 5*time.Second)
	produz(t, w, 10) // abaixo do limiar por contagem
	antes := w.OffsetCommitado()

	relogio.Avanca(6 * time.Second)
	w.Tick()

	if depois := w.OffsetCommitado(); depois <= antes {
		t.Errorf("offset commitado = %d, continua em %d: o disparo por tempo não rodou", depois, antes)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha** → FAIL.

- [ ] **Step 3: Implementar.** Disparo por contagem **ou** por tempo, o que vier primeiro, usando `storage.checkpoint.interval` — a mesma regra que o `nishidb ingest` já aplica.

- [ ] **Step 4: Rodar** `go test ./... && go test ./... -race`

- [ ] **Step 5: Commit**

```bash
git add internal/kafka/
git commit -m "Destrava o lag congelado: checkpoint também por tempo, não só por evento"
```

---

### Task 10: §96 — `status`, `datasets` e `inspect`, remotos

**Files:**
- Modify: `internal/cli/cli.go` (dispatch ~linha 113), `internal/server/documentos.go`, `internal/protocolo/documentos.go`
- Create: `internal/cli/operacao.go`, `internal/cli/operacao_test.go`

Nascem **remotos** (`--server`), pelo argumento do flock exclusivo: se forem só locais, repetem o débito 54 — o operador teria que derrubar o servidor para perguntar o status dele. `/v1/stats` e `/v1/metadata` já são a base, e `internal/cli/query_remota.go` é o molde do cliente. **Só leitura** (D3): `checkpoint` e `compact` são do M5, atrás de ADMIN.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestStatusRemotoFuncionaComOServidorNoAr é o critério que separa este
// comando do débito 54: ele tem de responder COM o servidor de pé, que é
// justamente quando o flock barra o caminho local.
func TestStatusRemotoFuncionaComOServidorNoAr(t *testing.T) {
	srv := servidorDeTeste(t)
	var out bytes.Buffer
	code := cli.Roda([]string{"status", "--server", srv.Endereco()}, &out, io.Discard)
	if code != 0 {
		t.Fatalf("exit = %d, queria 0: %s", code, out.String())
	}
	if !strings.Contains(out.String(), "radius_sessions") {
		t.Errorf("saída não nomeia o dataset: %q", out.String())
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha** → FAIL: comando não registrado.

- [ ] **Step 3: Implementar.** `status` resume storage, rows, segmentos e ingestão; `datasets` lista; `inspect <dataset>` detalha colunas e índices a partir do `/v1/metadata`. Reusar `normalizaEndereco` e a tradução código→exit code de `query_remota.go:124`. Atualizar o texto de uso em `cli.go:58`.

- [ ] **Step 4: Rodar** `go test ./... && go test ./... -race && golangci-lint run`

- [ ] **Step 5: Commit**

```bash
git add internal/cli/ internal/server/ internal/protocolo/
git commit -m "Entrega os comandos de leitura do §96 pelo caminho que o flock não barra"
```

---

### Task 11: o gate que prova a compaction — disco em plateau

**Files:**
- Modify: `benchmark/ingest-bench/harness.go` (`DiskBytes` ~linha 289 e ~492), `benchmark/ingest-bench/main.go`
- Test: `benchmark/ingest-bench/harness_test.go`

**Esta é a prova que importa.** A compaction pode passar em todo teste unitário e não devolver disco em regime. O harness já mede `DiskBytes`; falta a série temporal e o gate.

- [ ] **Step 1: Escrever o teste que falha**

```go
// TestGateDeDiscoReprovaCrescimentoLinear afere o gate contra a série que a
// AUSÊNCIA de compaction produz. Um gate que não reprova o append-only puro
// não está medindo nada — é o mesmo erro do oráculo do débito 6.
func TestGateDeDiscoReprovaCrescimentoLinear(t *testing.T) {
	linear := []int64{1e9, 2e9, 3e9, 4e9, 5e9, 6e9}
	if g := avaliaGateDeDisco(linear); g.Status != GateFail {
		t.Errorf("gate = %v para crescimento linear, queria fail", g.Status)
	}
	plateau := []int64{1e9, 1.8e9, 2.1e9, 2.15e9, 2.1e9, 2.2e9}
	if g := avaliaGateDeDisco(plateau); g.Status != GatePass {
		t.Errorf("gate = %v para plateau, queria pass", g.Status)
	}
}
```

- [ ] **Step 2: Rodar e confirmar a falha** → FAIL.

- [ ] **Step 3: Implementar.** Amostrar `DiskBytes` ao longo do STEADY e cobrar que o crescimento da **segunda metade** da janela fique abaixo de uma fração do da primeira. Usar o `GateStatus` de três estados que a errata do M1 introduziu — gate não avaliado **não** é gate aprovado. Carga: updates sobre **key set fixo**, que é o padrão que gera garbage sem gerar rows novas.

- [ ] **Step 4: Rodar** `go test ./benchmark/... -v`

- [ ] **Step 5: Commit**

```bash
git add benchmark/ingest-bench/
git commit -m "Cobra da rodada que o disco estabilize, não que a compaction rode"
```

---

### Task 12: documentação, diagramas e a rodada de aceite

**Files:**
- Modify: `README.md`, `docs/architecture.md`, `docs/debitos.md`
- Create: `docs/diagrams/compaction_flow.puml`, `docs/aceite-m4.md`

- [ ] **Step 1: Rodar o aceite com Kafka real**, no molde de `docs/aceite-fase6.md`, com o gate da Task 11 ligado. Registrar o artefato em `benchmark/baselines/`.

- [ ] **Step 2: `compaction_flow.puml`** — varredura, republicação condicional, as três etapas da aposentadoria e os dois pontos de reparo do `Open`. Validar com `plantuml -checkonly -failfast2` **e** pelo `uml.nishisan.dev`, como manda a nota do débito 39. URL do proxy fixando `main`.

- [ ] **Step 3: `docs/debitos.md`** — fechar **21**, **34**, **38**, **45**, **56** e a parte de leitura do **17**, cada um nomeando onde fechou. Abrir os itens novos que a fase produzir. **Sem renumerar nada.**

- [ ] **Step 4: `README.md`** — seção de compaction, `storage.compaction` na config de exemplo, os três comandos novos, e a linha "Próximos" refletindo a ordem do roadmap (M5: auth/TLS → JDBC).

- [ ] **Step 5: Conferir a suíte inteira e a documentação**

Run: `go test ./... && go test ./... -race && golangci-lint run`
Run: `go test ./internal/cli/ -run TestEvidenciasCitadasNosDocumentosExistem -v`

- [ ] **Step 6: Commit**

```bash
git add README.md docs/
git commit -m "Fecha o M4: documentação, diagrama e a rodada com Kafka real"
```

---

## 4. Correções pontuais a incorporar

Prosa que não bate com o código, encontrada na varredura desta fase. Entram na tarefa mais próxima, não em commit próprio:

- [config.go:71](../internal/config/config.go:71) e [:245](../internal/config/config.go:245) afirmam que a subida avisa sobre `storage.durability` "como já faz". **Não há campo nem aviso.** Acrescentar a chave à lista de `config.sem_efeito` fecha parte do débito 50 — vai junto da Task 3.
- [manifest.go:35](../internal/storage/checkpoint/manifest.go:35) descreve 12 bytes por segmento; o `encode` grava 36 desde os stats de liveness, e o `make` de capacidade em [:110](../internal/storage/checkpoint/manifest.go:110) usa o `*12` desatualizado. Vai junto da Task 1.

## 5. O que este milestone NÃO entrega

- **Recuperação de tombstone.** Record tombstoned é copiado para frente (D2). Limpar location e primary é Fase 9.
- **Rotas mutantes.** `POST /v1/compact` e `/v1/checkpoint` são do M5, atrás de ADMIN (D3).
- **`storage.durability`** (§47) continua sem implementação — ganha o aviso, não o comportamento. Débito 50 segue aberto.
