# Disciplina de mutação — índice por segmento

Este documento é o produto de uma tarefa que não escreveu funcionalidade
nenhuma. Ele existe para responder a uma pergunta que nenhuma suíte verde
responde sozinha: **se a invariante fosse quebrada, a suíte ficaria vermelha?**

É a doutrina do débito 53 aplicada aos testes em vez das ferramentas: *"gate que
passa com a ferramenta ausente não é gate, é carimbo"*. Um teste que passa com o
defeito presente é a mesma coisa, e neste ciclo ele apareceu **vinte vezes** —
mais do que qualquer outra classe de achado, e nenhuma delas encontrada por
leitura.

O procedimento, por mutação: aplicar no código de produção, rodar `make test`,
anotar **qual** teste reprovou e **com que mensagem literal**, `git checkout` da
mutação. Nada foi commitado com mutação aplicada. Onde uma mutação reprovou de
menos — ou de mais —, o teste foi corrigido e a correção está registrada aqui.

---

## 1. As sete mutações canônicas

Linha de base: `make test` verde, 43 pacotes `ok`, exit 0.

| # | Mutação | Onde | Reprovou | Mensagem literal |
|---|---|---|---|---|
| M1 | o `Set` volta a varrer a cadeia antes de inserir | `segindex/wip.go` | `TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento` — **e nenhum teste de corretude** | `pior caso de 20000 acessos com n=10000, teto 4 — a cadeia voltou a crescer com o segmento` |
| M2 | o merge k-way não descarta as cabeças empatadas | `segindex/union.go` | `TestUneMergeiaFaixasSobrepostasEDeduplica`, `TestUneNaoConcatenaQuandoAsFaixasSeTocam`, `TestUneEhEquivalenteAUniaoOrdenada`, `TestMergeIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas`, mais 3 em `internal/storage` | `drena = [10 20 30 30 40 50], quero sem repetir o 30` |
| M3 | `disjoint` devolve sempre `true` | `segindex/union.go` | os quatro acima mais `TestUneMergePropagaOPrimeiroErro`, mais 3 em `internal/storage` | `estratégia = "concatenacao", quero "merge"` |
| M4 | o `Open` adota o `.widx` em vez de descartá-lo | `storage/indices.go` | `TestOWIPNaoEhAdotadoAtravesDeUmRestart`, `TestOsWIPsQueNaoSaoDoCorrenteSaemNaAberturaESoQuemTemDadoEntraNaFila`, `TestOWIPOrfaoDeUmSegmentoJaSeladoSaiSemMexerNoSelado`, `TestNomeIlegivelNoEspacoDoWIPSaiComoDatasetIlegivel`, `TestOWIPDeUmaExecucaoAnteriorEhDescartadoNaAbertura`, `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` | `Open: segindex: formato inválido: […]/segment-00000001-00001.widx não começa com "NDBW"` |
| M4' | o `Open` adota o `.widx` **de segmento não corrente** | `storage/indices.go` | `TestOsWIPsQueNaoSaoDoCorrenteSaemNaAberturaESoQuemTemDadoEntraNaFila`, `TestOWIPOrfaoDeUmSegmentoJaSeladoSaiSemMexerNoSelado` | `Stat(segment-00009999-00001.widx) = <nil>, quero IsNotExist: o .widx de um segmento que não é o corrente tem de sair na abertura` |
| M5 | a aposentadoria não remove o `.sidx` | `storage/indices.go` | `TestAAposentadoriaLevaOIndiceJunto`, `TestAJanelaNaoDeixaIndiceDeSegmentoAposentado`, `TestAAposentadoriaNaoDesmapeiaOIndiceSobUmaVarreduraEmVoo` | `Stat(segment-00000001.sidx) = <nil>, quero IsNotExist — o índice tem de sair com o .dat` |
| M6 | a reconstrução emite as postings na ordem de leitura, sem ordenar | `segindex/writer.go` | `TestOEscritorAgrupaOrdenaEDeduplicaAsPostings`, `TestGoldenDoSelado`, `TestSelarPreservaExatamenteOQueOWIPRespondia`, `TestOSeladoDevolveOQueOEscritorRecebeu`, mais 8 em `internal/storage` incluindo `TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP` | `secoes[0].fieldID = 7, quero 3 (campo 3 antes do 7)` |
| M7 | crc divergente devolve erro em vez de degradar | `segindex/sealed.go` | `TestCRCDivergenteNaoDerrubaAConsulta`, `TestOCrcDivergenteContaUmDegrauEEnumeraSempre`, `TestOGaugeDeEstadosEhCegoAIndicePodre`, `TestStatsEMetricsLeemAMesmaEstrutura` | `OpenSealed não deve falhar por crc: segindex: crc divergente: […] (calculado c11ea0ba, gravado 3e1ea0ba)` |

**Nenhuma das sete escapou.** Uma reprovou **de mais**, e está corrigida
(seção 2). A M5 — que o plano marcava como a mais provável de escapar, porque a
órfã não muda resposta nenhuma e só faz o disco não voltar — foi pega por três
testes independentes: o do `.sidx` em disco, o da janela e o da contagem de
vidas do mapeamento.

### M4: o plano nomeou o teste errado para o próprio enunciado

O plano escreve *"o `Open` adota o `.widx` de segmento **não corrente**"* e manda
esperar `TestOWIPNaoEhAdotadoAtravesDeUmRestart`. Os dois não casam: aquele teste
corrompe o `.widx` do segmento **corrente** e reabre. A mutação literal do
enunciado não o toca.

As duas metades foram exercidas, e as duas estão cobertas. A primeira, pelo teste
nomeado. A segunda, por dois outros, que juntos travam os casos 2, 3 e 4 da §2.5:

- `TestOsWIPsQueNaoSaoDoCorrenteSaemNaAberturaESoQuemTemDadoEntraNaFila`
- `TestOWIPOrfaoDeUmSegmentoJaSeladoSaiSemMexerNoSelado`

Não há buraco; há um ponteiro impreciso no plano, registrado aqui para não virar
"esta mutação escapa" numa leitura futura.

---

## 2. O sinal da M1, que é o ponto do exercício

A §6.5 da spec manda procurar **uma** coisa em todo o plano: uma mutação que
produza resultados **certos** e apenas **lentos**, e que reprove **só** no gate de
custo. É a M1 — fazer o `Set` varrer a cadeia antes de inserir não muda o `Lookup`,
não muda os bytes do `.sidx` selado, não muda resposta de consulta nenhuma. Só
devolve o O(n²) que este subsistema inteiro existe para matar.

O que a rodada mostrou, medido:

- **o gate de custo reprova**, e reprova três vezes. Em `n=10000`,
  `pior caso de 20000 acessos com n=10000, teto 4`. Em `n=100000`,
  `pior caso de 200000 acessos`. Em `n=1000000` ele nem termina:
  `panic: test timed out after 10m0s`, com
  `TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento/n=1000000` na lista de testes em
  execução. A demonstração literal da forma quadrática;
- **`internal/storage` passa inteiro** (25,8 s, zero FAIL) — todas as invariantes
  de estado, recovery, aposentadoria, janela e reconstrução seguem verdes;
- **`internal/server` estoura o prazo** em `TestClienteQueDesisteLiberaAVaga`,
  com a pilha em `WIP.Set → w.le → Region.AtomicLoad64`. Não é reprovação de
  corretude: é a regressão de desempenho ficando grande o bastante para virar
  falta de vivacidade num teste que ingere de verdade. Vale registrar como o que
  é — sintoma de custo, não de resposta errada.

### O que reprovou de mais, e o conserto

`TestParaCadaVarreTodasAsPostingsUmaVez` ficou vermelho sob a M1:

```
wip_test.go:311: ForEach visitou 5 postings, quero 6
wip_test.go:319: ForEach viu (h=100,row=10) 1 vezes, quero 2
```

Ele comparava o total visitado contra `len(postadas)` — a lista que o **teste**
gravou. Isso amarra o contrato de `ForEach` ("visita cada posting do array uma
vez") à política de escrita do `Set` ("o array é append-only, dois Set com o mesmo
par deixam duas entradas"), que são coisas diferentes: a primeira é observável
pela selagem, a segunda não é observável por consumidor nenhum, porque o
`Writer` deduplica de qualquer forma.

O oráculo passou a ser `w.Postings()` — o que o **WIP** diz que tem no array. O
teste continua pegando o defeito que lhe cabe, e mais apertado, porque agora
compara contra a verdade da estrutura em vez da expectativa do autor. Conferido
por mutação em `ForEach` (parar a cadeia um elo antes):

```
wip_test.go:337: ForEach visitou 3 postings e o array tem 6
wip_test.go:348: ForEach não visitou (h=300,row=42)
```

Com o conserto, a M1 reaplicada deixa o pacote inteiro verde fora dos gates de
custo — 82 `PASS`, zero `FAIL` — e só o gate de custo vermelho. **É a suíte sabendo separar um bug de uma
regressão de desempenho**, que é o que a §6.5 pede.

### O gate irmão é cego à M1, e é correto que seja

`TestOCustoDaSondagemNaoAcompanhaONumeroDeValoresDistintos` passa sob a M1 com a
média **idêntica** à da linha de base: 3,848 / 3,980 / 3,780 acessos por `Set` nas
três escalas. A razão é estrutural, não folga de teto: naquele gate todo valor é
novo, então a cadeia do slot está vazia e a varredura acrescentada não executa
uma iteração sequer.

É exatamente por isso que o outro gate põe **todas** as postings no mesmo valor —
o caso que derrubou o piloto, com ~3.724 postings por valor. Dois gates de custo
com corpus trocado teriam medido, os dois, o melhor caso.

---

## 3. O corpus do ciclo — mais de setenta mutações

As sete acima são as que o plano previu. Ao longo das dezessete tarefas,
implementadores e revisores aplicaram muitas outras, e elas valem mais juntas do
que espalhadas pelos relatórios.

### Fase A — o pacote `segindex` (tarefas 1 a 5)

| Mutação | Reprovou | Trecho |
|---|---|---|
| desligar o dedup do `Writer` | `TestOEscritorAgrupaOrdenaEDeduplicaAsPostings` | `valor 100: count = 4, quero 3` |
| inverter a prioridade do sort (hash antes de campo) | idem | `secoes[0].fieldID = 7, quero 3` |
| `sealedIter.Next` copia o delta antes de decodificar | `TestIterarSeladoNaoAlocaPorLinha` | `AllocsPerRun = 1000, quero 0` — uma alocação por linha |
| neutralizar os dois guardas de tabela de seções | `TestSeladoRecusaTabelaDeSecoesForaDeOrdem`, `TestPostingsForaDaFaixaDegradamEmVezDePanicar` | `panic: slice bounds out of range [:4100] with capacity 120` |
| o `Set` varre a cadeia (M1, forma canônica) | gate de custo, e só ele | `pior caso de 10001 acessos com n=10000, teto 4` |
| a mesma varredura por `AtomicLoad64`, escapando do contador | `TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador` | `Set lê o mapeamento por AtomicLoad64 direto […] Use w.le.` |
| `postCount` publicado DEPOIS da cabeça | `TestOWIPAtendeLeitorConcorrenteDuranteOCrescimento` | `Lookup encolheu de 943 para 0` |
| tirar o `slices.Compact` do `Lookup` | ordem estrita | `Lookup = [10 10 30 40 50], quero [10 30 40 50]` |
| tirar o `IsSorted`/`Sort` do `Lookup` | ordem estrita | `Lookup = [50 10 30 10 40], quero [10 30 40 50]` |
| materialização exata trocada por `append` | `TestLookupNoWIPAlocaExatamenteUmaVez` | `AllocsPerRun(Lookup) = 8, quero exatamente 1` |
| o rehash não desloca as postings | `TestOWIPCresceSemPerderPosting` | `Lookup(3) = [0], quero [1] — o crescimento perdeu a posting` |
| merge sem dedup (M2) | as três de `Union` | `drena = [10 20 30 30 40 50]` |
| concatenar sempre (M3) | as três de `Union` | `drena = [10 30 50 20 30 40]` |
| `concatIter.Close` só fecha `fontes[idx:]` | `TestConcatIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas` | `f1 (já esgotada, cursor passou dela) não foi fechada` |
| `mergeIter.Close` só fecha o que o heap guarda | `TestMergeIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas` | `f2 (já esgotou e saiu do heap) não foi fechada` |
| o descarte da fonte vazia não fecha | `TestUneFechaAsFontesVaziasDescartadasAntesDeCombinar` | `a fonte vazia não foi fechada no descarte` |
| o merge não captura o erro no ramo esgotado | `TestUneMergePropagaOPrimeiroErro` | `it.Err() == nil` com fonte que falhou |

### Fase B — a posse pelo `Store` (tarefas 6 a 9)

| Mutação | Reprovou | Trecho |
|---|---|---|
| a reconstrução decide liveness pela location | `TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP`, `TestAReconstrucaoNaoConsultaAsLocations` | `selagem e reconstrução produziram arquivos diferentes: 6724 bytes contra 5628` |
| remover a classificação do nome ilegível | `TestNomeIlegivelNoEspacoDoWIPSaiComoDatasetIlegivel` | `quero ErrDatasetCorrupt (exit 4); sem a sentinela isto vira exit 1` |
| desligar a guarda do segmento corrente | `TestConstruirOIndiceDoSegmentoCorrenteEhRecusado` | `ConstroiIndexOfSegment(corrente) = <nil>, quero ErrInvalidArgument` |
| aceitar subconjunto de campos na reconstrução | à época, o teste de recusa; hoje `TestConstruirComCampoNaoIndexadoEhRecusado` e `TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam` | `= <nil>, quero ErrInvalidArgument` |
| `aplicaIndexSets` pula o campo não alterado | `TestUpdateQueCruzaSegmentoLevaAPostingDoCampoInalterado`, `TestAPostingDoCampoInalteradoSobreviveASelagemEAoRestart` | `a posting do campo inalterado não migrou com o record` |
| `Close` desmonta o mapeamento incondicionalmente | `TestAAposentadoriaNaoDesmapeiaOIndiceSobUmaVarreduraEmVoo` | `fatal error: fault [signal SIGSEGV]` em `sealedIter.Next → readDelta` |
| o pread não trata `ErrNotExist` do segmento aposentado | `TestSegmentoAposentadoEntreAListagemEOPreadViraFonteVazia` | consulta abortada |
| `cleanLegacyGlobalIndices` inerte | `TestDatasetLegadoAbreEOsIndicesGlobaisSaemDoDisco` | `IndexBytes 21.037.056 ≥ 20.996.096`, os dois diretórios sobrevivem |
| `decode` do manifest só aceita v3 | idem | `o manifest legado montado à mão não é legível: version desconhecida 1` |
| `decode` não pula as entradas de bitmap | `TestSecaoDeHashesLegadaEhDescartadaSemRecusarODataset` | `seção kafka inesperada (6291458)` |

### Fases C e D — planner, janela, laço e métricas (tarefas 10 a 16)

| Mutação | Reprovou | Trecho |
|---|---|---|
| a união da cobertura removida da reconstrução | `TestReconstruirSoOsCamposQueFaltamPreservaOsQueJaEstavam` | seção preservada some |
| os dois sítios da cobertura divergem | idem | `mapeado (...)` — segmento eternamente pendente |
| cobrar zero pelo `.sidx` não construído | `TestAJanelaConvergeEmVezDeMoer` | `pendentes = [1 2 3 4] logo depois da expulsão` |
| fronteira da janela pelo record mais ANTIGO | `TestFronteiraDaJanelaUsaORecordMaisNovoDoSegmento` | o segmento que atravessa perde o índice |
| cobrar pelo `maior .sidx` em disco (o furo do desenho aprovado) | `TestAJanelaConvergeEmVezDeMoer` | `pendentes = [1] logo depois da expulsão` |
| remover a guarda de bootstrap | `TestAJanelaConvergeEmVezDeMoer` | `pendentes = [1 2 3 4 5], quer só o mais novo selado` |
| orçamento caminhando do mais velho para o mais novo | idem | `pendente = 1, quer o mais novo selado (5)` |
| `inPontoFixo` sempre `false` | `TestSegmentoSemCampoIndexadoNaoVoltaParaAFila`, `TestOPontoFixoCaiQuandoOIndiceSaiDeDisco` | `uma passada seguinte reconstruiu 2 segmentos que já estavam no ponto fixo` |
| ponto fixo como booleano "já tentei" | `TestOPontoFixoCaiQuandoOIndiceSaiDeDisco` | `a passada depois da expulsão reconstruiu 0 segmentos, quero 1` |
| teto de 1 segmento por passada | `TestAReconstrucaoConvergeEDeixaAFilaVazia`, `TestOLacoDeReconstrucaoDrenaAFilaSozinho` | fila não esgota |
| temporário de nome fixo | `TestCadaEscritaUsaUmTemporarioProprio`, `TestDoisEscritoresConcorrentesNaoPublicamArquivoRasgado` | crc divergente de verdade |
| `Close` sem `ForRebuild` | `TestCloseCancelaEEsperaAReconstrucao` | goroutine órfã |
| `ForRebuild` sem `Wait` | `TestSegundaPartidaDaReconstrucaoNaoSobeSegundaGoroutine` | o `Para` nunca volta |
| `segmentLeftDataset` sempre `false` | `TestOSegmentoAposentadoNoMeioDaPassadaNaoDerrubaOLaco`, `TestOErroDoSegmentoSumidoEhOQueOLacoTolera` | o laço para de vez |
| `publicaSealed` sem conferir a geração | `TestOMapeamentoAbertoAntesDaReescritaNaoEhPublicado` | o cache serve o arquivo antigo |
| uma chamada de `Construi` por campo | `TestAReconstrucaoResolveTodosOsCamposPendentesNaMesmaLeitura` | `o segmento foi varrido 2 vezes` |
| `windowOfDataset` sem a guarda do materializado | `TestAReconstrucaoSobeNosDoisModosComAJanelaCerta` | janela finita no materializado |
| primeira passada não imediata | `TestOLacoDeReconstrucaoDrenaAFilaSozinho` | a fila só drena no segundo ciclo |
| **laço com janela infinita, expulsão com 6 h** | **nada — VERDE** | ver seção 4 |
| o coletor só emite o estado quando a contagem ≠ 0 | `TestAsSeriesDeIndiceExistemAntesDoPrimeiroEvento`, `TestOsLabelsDeIndiceSaoConjuntosFechados` | `a fila que nasce vazia é indistinguível da fila que nunca foi publicada` |
| `fora` contado como `pendente` | `TestOsQuatroEstadosParticionamOsSegmentos` | `estado fora = 0 depois de a janela expulsar índices` |
| `estado="pendente"` ↔ `estado="fora"` no mapa | `TestStatsEMetricsLeemAMesmaEstrutura` | `/metrics diz 5 e /v1/stats diz 3` |
| `indexStats` mantém contador próprio | idem | `/metrics diz 5 e /v1/stats diz 6 — duas fontes para o mesmo fato divergem` |
| `modo="enumeracao"` como constante zero | idem | `/metrics diz 0 e /v1/stats diz 10` |
| `crc_falhas` como constante zero | idem | `/metrics diz 0 e /v1/stats diz 2` |
| `reconstrucoes` como constante zero | idem | `/metrics diz 0 e /v1/stats diz 7` |
| o `/v1/stats` zera `CRCFailures` | idem | `o bloco index saiu com CRCFailures = 0` |
| o valor do rótulo vem do nome do dataset | `TestOsLabelsDeIndiceSaoConjuntosFechados` | `trouxe estado="d", que não está na enumeração fechada` |
| a enumeração deixa de ser contada | `TestOsSegmentosConsultadosSeparamIndiceDeEnumeracao` | `4 por índice + 0 por enumeração = 4, e o dataset tem 6 segmentos` |
| o crc divergente degrada mas não conta | `TestOCrcDivergenteContaUmDegrauEEnumeraSempre` | `crc_falhas subiu 0 ao abrir um .sidx corrompido, quer 1` |
| `crc_falhas` sobe a cada consulta (taxa em vez de degrau) | idem | `crc_falhas subiu [0 0 0] em três consultas ao MESMO segmento podre, queria [1 0 0]` |
| `index_bytes` perde o filtro e soma os `.dat` | `TestOsBytesDeIndiceContamExatamenteOsArquivosDeIndice` | `index_bytes = 81920 e os arquivos de índice em data/ somam 57344 blocos` |
| `index_bytes` devolve zero | idem | `nishidb_index_bytes = 0 num dataset com .sidx e .widx em disco` |
| `history_index_bytes` lê o próprio base | `TestOsBytesDeIndiceVivemNoRotuloDaIrma` | `= 1.68e+07 e o índice em disco da IRMÃ soma 106496 blocos` |
| `historyStats` lê `base.st` em vez de `base.irma.st` | idem | as três asserções falam |
| publicar os estados no caminho de erro | `TestAPassadaQueFalhaPreservaORetratoAnterior` | `a passada interrompida publicou {Sealed:4 …} por cima de {Sealed:5 …}` |
| `reconstrucoes` conta passadas em vez de segmentos publicados | `TestAsReconstrucoesSaoContadasPorSegmentoPublicado` | `reconstrucoes subiu 1 e a passada publicou 5 segmento(s)` |
| `recFeitas` volta a contar a visita | idem | `subiu 2 para 2 segmento(s) que foram varridos e NÃO publicaram arquivo nenhum` |
| `reconstrucao_ativa` ecoa `secCorrentes` | `TestOMedidorDaReconstrucaoNaoEhOContadorDeCorrentes` | `= true num Store cujo laço nunca subiu, com corrente = 1` |
| `reconstrucao_ativa` **e** `RebuildRunning` ecoam `States.Current` (os dois lados) | `TestASerieDaReconstrucaoDesceQuandoOLacoMorre` | `nishidb_index_reconstrucao_ativa = 1 com o laço MORTO e corrente = 1` |
| a `Ajuda` volta a prometer taxa | `TestAAjudaDoContadorDeCrcNaoApontaSerieQueNaoDiscrimina` | `ainda afirma que a falha não é memorizada` |
| `Family` vazia de volta | `TestOsCabecalhosSaemNomeadosParaCadaFamilia` | `cabeçalho com nome de métrica VAZIO: "# HELP  Linhas gravadas…"` |
| citação morta plantada em `indices_metrics.go` | `TestAsCitacoesDeTesteNosComentariosExistem` | nomeia arquivo e nome |

---

## 4. As que escaparam — a taxonomia

Vinte vezes neste ciclo um teste passou com o defeito presente. **Todas as vinte
foram achadas por mutação; nenhuma por leitura**, e várias sobreviveram a revisões
inteiras em tier alto. É o achado central do ciclo, e ele não estava no plano.

Escrever isso como lista de anedotas não serve ao próximo ciclo. O que serve é dar
**nome** a cada forma de cegueira e escrever **a pergunta** que o autor do teste
deveria ter feito.

### 4.1 Oráculo autocumprido

*O oráculo é produzido pelo mesmo código que ele deveria conferir.*

O golden do `.sidx` foi gerado pelo próprio `Writer` que ele verifica: ele trava
desvio futuro, mas não prova acerto nenhum — se o escritor errasse a aritmética de
offsets, o golden gravaria o erro e passaria para sempre. O remédio foi um
decodificador local no teste, que **segue** os offsets gravados em vez de
recalculá-los pela mesma fórmula.

Mesma família: a primeira versão do teste de rótulo de `history_index_bytes`
comparava as duas séries **entre si** — as duas alimentadas pela mesma
proveniência errada. E a comparação `/metrics` × `/v1/stats` não vê a mutação
quando ela alimenta os **dois lados** da mesma fonte: foi assim que
`reconstrucao_ativa` ecoando `States.Current` passou verde nos dois canos.

> **Pergunta:** de onde vem o número que eu chamo de "certo"? Se ele vem do código
> em julgamento — direta ou indiretamente —, o teste é um espelho. O oráculo tem de
> vir de fora: outra implementação, o disco medido por `du`, uma tabela escrita à
> mão, ou uma superfície que o defeito não alcança.

### 4.2 Gate que mede o melhor caso

*O corpus escolhido não exercita o caminho que o gate anuncia medir.*

O teto de sondagem do `Set` afirmava `1 <= 4` nas três escalas, porque todas as
postings estavam no mesmo valor e o diretório encontrava o slot na primeira
tentativa. O gate central do ciclo provava menos do que anunciava, e um `Set` que
varresse a cadeia inteira passaria por ele. O remédio foi contar **acessos ao
mapeamento**, e não sondas de diretório — mais um gate de AST
(`TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador`) que impede a varredura de se
esconder do contador.

Simetricamente, `TestOCustoDaSondagemNaoAcompanhaONumeroDeValoresDistintos` é cego
à M1, e por construção: com todo valor novo, a cadeia está vazia. São dois gates
com corpus **oposto** de propósito.

> **Pergunta:** qual defeito este corpus tornaria observável, e qual ele esconde?
> Se a resposta certa é a mesma para "algoritmo bom" e "algoritmo ruim", o corpus
> está medindo o melhor caso.

### 4.3 Concorrência vácua

*O teste tem duas goroutines e nenhuma janela.*

O gate de leitor concorrente ao crescimento tinha asserções finais quase
tautológicas: quem fazia o trabalho era o bloqueio, não a asserção. Só depois de
apertar o WIP em 16 slots (nove rehashes na janela medida) e de acrescentar uma
**barreira de não-vacuidade** — o escritor espera o leitor publicar um comprimento
intermediário — o teste passou a discriminar. E foi ele, escrito para outro
defeito, que pegou a inversão de ordem de publicação que duas revisões não viram:
`Lookup encolheu de 3309 para 0`.

> **Pergunta:** se eu remover o mecanismo de sincronização do produto, este teste
> falha — ou ele nunca chega a criar a janela? Uma barreira que force o
> entrelaçamento é o que separa um teste concorrente de um teste com duas
> goroutines.

### 4.4 Interrupção antes de haver o que vazar

*O teste para no ponto em que o defeito ainda não é observável.*

Os dois testes de `Close` do `Union` passavam com o bug simulado. No `concatIter`,
interromper depois de um único `Next()` deixa `idx == 0`, então "fechar só
`fontes[idx:]`" ainda fecha tudo. No `mergeIter`, depois de um `Next()` as duas
fontes continuam **dentro** do heap, então "fechar só o heap" também fecha tudo. O
conserto foi traçar a sequência de emissão e interromper no ponto exato em que uma
fonte esgota e **sai** da estrutura.

Mesmo padrão: a primeira versão do teste de segmento aposentado removia só o
`.dat`, e o descritor cacheado mantinha o inode vivo — o pread continuava
funcionando e nada era encenado.

> **Pergunta:** no instante em que eu interrompo, o estado que o defeito corrompe
> já existe? Vazamento, órfão e uso-após-liberação precisam que a coisa tenha
> saído da estrutura antes de o teste olhar.

### 4.5 Cobertura pela metade

*O teste exercita um dos ramos e afirma sobre os dois.*

`TestUnePropagaOPrimeiroErro` usava faixas **disjuntas**, então só exercitava o
`concatIter` — o caminho de MERGE, que é o que o dataset materializado sempre usa,
ficou sem regressão para a invariante mais dura do produto ("erro nunca vira
resultado menor"). Hoje há um teste por caminho.

Igual: a metade "falha no meio do laço" do teste de retrato de estados nunca era
exercitada, porque nada no teste fazia `segmentPendente` falhar; e o teste de
migração de índice legado plantava os diretórios sob um dataset **v3**, sem nunca
abrir um legado de verdade.

> **Pergunta:** quantos caminhos a função tem, e quantos este teste percorre? Se a
> asserção fala de "todos" e a montagem produz um, o resto está descoberto.

### 4.6 Helper que mede outra grandeza

*O teste e o produto usam unidades diferentes para o mesmo número.*

O teste do moinho da janela calculava o teto sobre `info.Size()` — tamanho
aparente — enquanto o produto conta **blocos alocados**. Um `.sidx` de teste tem
centenas de bytes e ocupa um bloco inteiro, então o teto ficava abaixo do custo de
um único índice e **tudo** era expulso nas duas implementações. A mutação só ficou
vermelha depois de o helper passar a usar `blocosOf`.

> **Pergunta:** o número que o helper calcula é o **mesmo** que o produto calcula,
> pela mesma via? Bytes contra blocos, aparente contra alocado, e "o que o teste
> gravou" contra "o que a estrutura diz que tem" são pares que passam verdes nos
> dois desenhos.

### 4.7 Fixture sem variedade e sem folga

*Todos os elementos são iguais, então o estimador é constante por construção.*

A pior das vinte, porque não é calibragem — é cegueira a uma **classe** inteira. O
teste de convergência da janela media a convergência num regime em que todos os
`.sidx` ocupavam **um bloco**: o estimador "maior `.sidx` em disco" era invariante
à expulsão, e o não-monotônico que o revisor traçou (um `.sidx` atípico de 40 M
entre quatro de 4 M) não podia acontecer. A fixture precisa das duas coisas:
**variedade** (um elemento atípico) e **folga** (espaço para caber a soma dos
pequenos mais uma vaga), senão até o estimador errado recusa pelo motivo errado.

A mesma classe apareceu no ponto fixo do laço: só uma fixture com **dois campos**
— um presente nos records, outro em nenhum — produz o estado em que o ponto fixo e
a fila-superconjunto se contradizem.

> **Pergunta:** existe, dentro desta fixture, um par de entradas que se comportem
> diferente? Se todos os elementos são intercambiáveis, o teste mede uma constante.

### 4.8 Bancada em zero

*Metade dos números observados vale zero, e zero é indistinguível de zero.*

`TestStatsEMetricsLeemAMesmaEstrutura` rodava sobre um dataset **materializado**
onde 5 dos 10 números eram zero, e a guarda de não-trivialidade cobria só os
quatro que não eram. Trocar os rótulos `pendente` e `fora` passava **verde**;
publicar `modo="enumeracao"` como constante zero, também. Cinco dos onze achados
daquela revisão eram o mesmo defeito de bancada, e a correção foi uma só: um
dataset histórico com janela estreita, em que os dez números são distintos e
diferentes de zero.

Mesma família: `publicar zeros` e `não publicar` produzem o mesmo estado
observável quando não houve passada boa antes — a asserção passava por vacuidade.

> **Pergunta:** algum número que este teste compara vale zero? Se vale, a
> comparação `0 == 0` aprova qualquer fio congelado. A bancada tem de produzir um
> valor distinto para **cada** série que ela pretende amarrar.

### 4.9 Oráculo de prosa por substring

*O teste procura uma palavra e aceita qualquer texto que a contenha.*

Os testes de texto de `Ajuda` conferiam substring, e passavam com qualquer redação
que **mencionasse** a palavra certa — inclusive as duas redações erradas que este
ciclo produziu ("leia `rate()`", "olhe a enumeração"). Reescritos como
**propriedade**: a `Ajuda` do contador de crc não pode conter **nenhuma** das
outras famílias de índice, e a lista sai da constante, não de nomes escritos no
teste — uma série nova entra na proibição sozinha.

> **Pergunta:** que texto ERRADO passaria neste teste? Se existe um, a asserção é
> sobre a presença de uma palavra e não sobre o que o texto afirma.

### 4.10 Correlação perfeita na bancada saudável

*Os dois fios que o teste compara são estruturalmente iguais no cenário testado.*

O caso mais fino do ciclo, e ele merece nome próprio porque nenhuma das perguntas
acima o pega. Num servidor saudável, `reconstrucao_ativa` e `States.Current` são
sempre iguais: existe um segmento corrente, o laço sobe sempre que há campo
indexado, e `Current` só é publicado quando há campo indexado. Nenhuma bancada
**saudável** separa os dois — e o cenário que o revisor sugeriu para separá-los não
existe no código (sem campo indexado, `Current` é 0, não 1).

O cenário que discrimina é o **laço morto**: sabotar o `.sidx` depois da subida, de
modo que a passada seguinte falhe, o laço morra e o retrato anterior fique de pé —
`corrente=1` com `reconstrucao_ativa=0`. E a espera tem de ser pelo **log**
(`reconstrucao.parou`), não pelo medidor, porque o medidor é o que está em
julgamento.

> **Pergunta:** existe algum estado alcançável em que estes dois números divergem?
> Se só o estado de FALHA os separa, o teste tem de encenar a falha — e esperar por
> algo que não seja o número em julgamento.

### As duas que a mutação não pegou, e por quê

Duas entradas do corpus **não** são "teste cego": são falhas da própria mutação, e
ficam registradas para que ninguém as conte como cobertura.

- a primeira tentativa de mutar `index_bytes` (devolvendo o `IndexBytes` inteiro)
  **recursou infinitamente** em vez de reprovar. Refeita como "perde o filtro e
  soma os `.dat`", aí o oráculo de disco pegou;
- duas das mutações do decode legado são pegas pela **pré-condição do helper de
  cirurgia**, não pelo corpo do teste — o que é cobertura real, mas de outro
  lugar, e o relatório da tarefa diz isso com precisão.

---

## 5. O que a suíte NÃO alcança

No estilo que o godoc de `durabilizaODiretorio` já usa: o limite escrito é melhor
que o limite presumido.

### Durabilidade física

Nenhum teste deste repositório afirma que um `fsync` levou a entrada de diretório
ao meio físico — de nenhum `fsync`, e o índice por segmento não é exceção. O
kernel não devolve observável para isso e o `SIGKILL` não descarta page cache. O
que a suíte alcança é a **existência** e a **posição** da chamada. Quem trocar o
corpo de `durabilizaODiretorio` por um `return nil` não fica vermelho; fica com
uma função chamada "durabiliza" que não durabiliza, e é a leitura que pega.

O mesmo vale para o `.sidx`: `TestCadaEscritaUsaUmTemporarioProprio` e
`TestDoisEscritoresConcorrentesNaoPublicamArquivoRasgado` provam que a publicação é
atômica **do ponto de vista do namespace** (tmp único + rename), não que os bytes
chegaram ao dispositivo antes do rename.

### O índice podre não tem série (débito 103)

Medido em duas bancadas idênticas, uma com dois `.sidx` corrompidos: **o gauge de
estados é literalmente idêntico nos dois casos**. O arquivo reprovado no crc
continua contado como `selado`, porque a fila de pendência deriva da tabela de
seções e ela não confere crc — de propósito, já que conferi-lo custaria até 2,25 GB
de leitura por arquivo a cada passada.

A enumeração **sobe** (medido: saudável 5/0, podre 3/2), mas no MESMO balde de um
segmento fora da janela, sem rótulo que separe a causa: ela conta "quantos
responderam sem índice", nunca "quantos deveriam ter índice e não tinham".
Consequência operacional: **nenhuma série responde "há corrupção agora"**. O único
sinal é o WARN `indice.selado_corrompido` no log, e o segmento corrompido responde
por enumeração para sempre — correto, e mais caro.

`TestOGaugeDeEstadosEhCegoAIndicePodre` afirma as duas metades, e o enunciado dele
foi **apertado por medição**: a primeira redação dizia que a enumeração também era
idêntica, e o teste a reprovou.

### O gate de citações cobre onze arquivos, não o repositório

`TestAsCitacoesDeTesteNosComentariosExistem` confere que todo nome de teste citado em
comentário existe de fato — **nos onze arquivos do índice por segmento**. Rodado sobre o
repositório inteiro ele acusa **mais de vinte citações mortas** em
`internal/query/planner`, `internal/schema`, `internal/query/meta`,
`internal/query/executor` e outros, herdadas da remoção dos motores globais e da
unificação da superfície de índice. Limpá-las é trabalho de outros donos; o limite
está escrito no godoc do gate em vez de fingido.

Segunda limitação do mesmo gate: uma citação a teste de **outro pacote** é aceita
se o nome existir em qualquer arquivo da árvore, porque resolver o pacote exigiria
carregar o repositório inteiro.

Este documento entrou na lista de
`TestEvidenciasCitadasNosDocumentosExistem` — ele é feito de citações, e sem o
gate seria a superfície mais fácil de mentir do ciclo. **Cinco** dos nomes que o
diário registrou já não existiam quando este texto foi escrito, todos por rename
de tarefas posteriores, e o gate os apontou um a um. Que ele não é carimbo foi
conferido por mutação: um nome de teste inventado, plantado neste arquivo, reprova
nomeando o documento e o nome — `[…] cita […], que não existe na suíte`.

### A selagem sob o `wmu` a cada rotação (débito 102)

O laço de reconstrução lê records por `ScanFrom` e não toma o `wmu`
(`TestAPassadaDeReconstrucaoNaoTomaOWmu`). Quem lê slot a slot é a **selagem**, sob
`wmu`, a cada rotação de segmento — pré-existente a este ciclo e não tocado por
ele. Nenhum teste daqui mede a pausa que ela impõe à ingestão; o que existe é o
argumento estrutural de que ela é por segmento, não por dataset.

### O gate de AST protege dois nomes contra um seletor

`TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador` inspeciona `Set` e `probeOrCreate`
procurando `AtomicLoad64`. A mesma varredura movida para um auxiliar novo, ou feita
por `WithBytes`, escaparia. É limite honesto e documentado, não defeito: a forma
canônica da regressão reprova por dois caminhos independentes (o gate de custo e
este), e fechar o buraco exigiria uma análise de alcance que o pacote não tem.

### Medidor que discrimina por corrida, não por construção

`TestOMedidorDaReconstrucaoSobeAntesDaGoroutine` pega "o medidor nunca sobe" e "não
desce no `Para`". Contra a mutação "`Store(true)` movido para dentro da goroutine"
ele discrimina **por corrida** — é a mesma limitação dos testes irmãos da compaction
e da retenção, e fechá-la exigiria um seam que não existe.

### Uma falha sem causa raiz

`TestCompactionEmDatasetReabertoDevolveDiscoDeVerdade` falhou **uma vez** durante a
tarefa 8 e passou nas quatro execuções seguintes. O argumento de por que aquele
pacote não pode ter o defeito investigado é sólido (ele não liga Readers nem
SQLReaders, logo `Lookup` nunca é chamado e nenhuma vida do mapeamento é
adquirida), mas a causa da falha em si continua sem nome. Falha sem causa é item de
triagem, não item para descartar.

---

## 6. O saldo

- as **sete** mutações do plano foram aplicadas, medidas e revertidas; **nenhuma
  escapou**;
- **uma** reprovou de mais — `TestParaCadaVarreTodasAsPostingsUmaVez` amarrava o
  contrato da varredura à política de escrita — e está corrigida, com a mutação de
  `ForEach` como critério de aceitação;
- a M1 reprova no gate de custo e em nenhum teste de corretude, que é o sinal que
  a §6.5 manda procurar;
- o corpus do ciclo tem mais de setenta mutações, e **vinte** delas encontraram
  testes que aprovavam código quebrado. Nenhuma foi encontrada por leitura.

A pergunta que atravessa o documento inteiro, e que o ciclo aprendeu a fazer antes
de considerar um teste pronto, não é *"o código está certo?"*. É:

> **este teste reprovaria se ele não estivesse?**
