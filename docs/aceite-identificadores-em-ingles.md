# Aceite — identificadores em inglês (fechamento antecipado)

Fechamento em 11/09/2026, na branch `refactor/identificadores-em-ingles`, sobre
o binário `v0.7.0-166-gfcd5e64-dirty` (o `fcd5e64` é o último commit dos
renames à mão; o `-dirty` são os arquivos deste fechamento). Registro de
rodada: não é reescrito.

O ciclo foi planejado para seis fases
([`planning/identificadores-em-ingles.md`](../planning/identificadores-em-ingles.md)
e [`planning/identificadores-em-ingles-plano.md`](../planning/identificadores-em-ingles-plano.md))
e foi **encerrado antes do fim por decisão de custo**: as Fases 0, 1 e metade
da Fase 2 rodaram pelo mecanismo do plano (ferramentas, workflow de agentes,
gates); o restante da Fase 2 foi feito à mão, em lotes, na IDE; as Fases 3, 4 e
5 não rodaram; a Fase 6 é este documento. O que ficou em PT-BR passa a ser
renomeado **incrementalmente**, pela regra registrada no `CLAUDE.md`: a
feature que toca um identificador PT-BR o renomeia no mesmo PR.

## Os números

Inventário por `go/types` (`scripts/renameinv`), antes e depois. "Sinal PT-BR"
é uma heurística de medição, não um gate: acento, termo PT-BR da tabela de
domínio do glossário ou terminação portuguesa em palavra de seis letras ou
mais, com os empréstimos que a prosa PT-BR do repositório usa (`payload`,
`dataset`, `checkpoint`…) descontados. Ela subestima abreviações (`tam`, `qtd`)
e superestima palavras que existem nas duas línguas; serve para comparar os
dois inventários pela mesma régua, não para afirmar zero.

| Medida | Início (05/09, `inventario-inicial.tsv`) | Fim (11/09, `inventario-final.tsv`) |
|---|---|---|
| Declarações nomeadas no módulo | 38 036 | 40 011 |
| Nomes de teste (`Test*`, `Benchmark*`, `Fuzz*`), fora da régua | 2 407 | 2 467 |
| Declarações com sinal PT-BR | 6 528 (18,3 % das não-teste) | 2 429 (6,5 %) |
| — locais | 3 209 | 1 195 |
| — funções | 808 | 283 |
| — parâmetros | 704 | 193 |
| — campos | 553 | 297 |
| — métodos | 511 | 198 |
| — constantes | 375 | 126 |
| — tipos | 252 | 100 |
| — variáveis de pacote | 101 | 34 |
| — métodos de interface | 15 | 3 |
| Baseline de lint (`.golangci-baseline`) | 368 | 333 |

Onde o resíduo mora (fim): `internal/storage` 580, `internal/server` 434,
`internal/cli` 317, `internal/kafka` 196, `internal/materializer` 140,
`benchmark/ingest-bench` 98, `internal/query/executor` 98, `internal/query` 83.
Metade do que sobrou são locais.

## O que rodou pelo mecanismo do plano

| Fase | Entrega | PR |
|---|---|---|
| 0 — preparação | glossário normativo, norma no `CLAUDE.md`, `scripts/renameinv`, `scripts/ifacerename`, `scripts/renamecite`, `scripts/rename-apply.sh`, gates de glossário e de ferramentas | #51 |
| 1 — pacotes e arquivos | seis pacotes e 267 arquivos `.go` renomeados por `gopls` e `git mv`, com o mapeamento em TSV | #52 |
| 2 — G0 e G1 | doze pacotes-folha; primeira rodada do workflow (tradutores, juiz, refutadores, aplicador, verificador, crítico de completude) | #53 |
| 2 — G2 | índices, protocolo, parser e o storage de segundo nível; nasce a fase de prosa e o inventário passa a ver campos de struct anônima | #54 |
| 2 — G3 | schema, config, fakestorage, binder, planner e o contador; o gate de citação pendurada passa a ler os grupos de interface | #55 |

Cada PR carrega no corpo os contratos conferidos por extração (tags YAML e
JSON, nomes de métrica, nomes em disco) e as mutações relatadas por tarefa:
vermelho com o defeito, verde sem ele. Os TSV em
`planning/identificadores-em-ingles/mapeamento-2-*.tsv` guardam o motivo de
cada nome; o gate `TestRenamedIdentifiersAreNotCitedByTheirOldNames` os lê
como dado e é o que impede um nome velho de sobreviver em godoc, documento ou
diagrama.

## O que foi feito à mão

Catorze commits entre 10/09 e 11/09 (`ef9734e..fcd5e64`) renomearam, na IDE e
em lote, funções, métodos, tipos, constantes e variáveis dos pacotes que o
workflow ainda não tinha alcançado (`cli`, `server`, `kafka`, `storage`,
`metakafka`, `materializer`, `query`, `counters`, `predicate`, benchmarks) e o
método público `Client.conecta → Client.connect` do client Python. O registro
de intenção está em
[`planning/rename_pt_functions_plan.md`](../planning/rename_pt_functions_plan.md)
e [`planning/checkpoint_variaveis.md`](../planning/checkpoint_variaveis.md).

Para que esses renames entrassem no mesmo regime dos do workflow, o
fechamento extraiu do diff os pares velho → novo de declarações de pacote
(linha `-` e `+` adjacentes, mesma espécie, mesma assinatura sem os
identificadores), validou cada par contra a árvore — o nome novo declarado no
mesmo pacote, o velho declarado em lugar nenhum — e gravou os 912 que
passaram em `mapeamento-2-manual.tsv` (486 funções, 172 métodos, 109
constantes, 87 tipos, 55 variáveis, mais três pares que a assinatura refluída
escondeu, acrescentados à mão). Onze ficaram de fora com o motivo relatado
(nome velho ainda vivo em outro pacote, ou curto demais para citação segura).
Locais, parâmetros e campos renomeados em lote não têm par extraível do diff
e ficaram fora do TSV: a prosa que os cita é o resíduo incremental.

Com o TSV, `scripts/renamecite` reescreveu 286 citações em 100 arquivos —
godoc que ainda dizia `// validaCertificadoNaSubida confere…`, rótulos de
`t.Errorf`, `docs/architecture.md`, `docs/debitos.md`, o `CLAUDE.md`
(`Engine.RegistraVirtual`) e planos antigos. Duas coisas a ferramenta não
resolve sozinha e ficaram registradas: `fechandoStore` existia em `cli` e em
`server` com nomes novos diferentes (`closingStorage`, `closingStore`), e o
godoc do segundo foi corrigido à mão; e o próprio registro De → Para do
usuário colapsou em `X → X` na primeira passada — foi restaurado e entrou na
lista de exclusão do `renamecite` e do gate, ao lado do plano do ciclo.

Duas correções de tradução no fechamento: `validateCertificateOnUpload` virou
`validateCertificateAtStartup` ("na subida" é a subida do processo, não um
upload), e o parâmetro `context Context` de `countersLocal` voltou a `amb`,
como nas outras dezenove funções da `cli` que recebem o mesmo tipo.

## Verificação

| Comando | Resultado |
|---|---|
| `make build` | verde |
| `make test` | verde, 50 pacotes `ok`, 2 sem testes |
| `make test-race` | verde |
| `make lint` | 333 apontamentos = `.golangci-baseline` (errcheck 278, revive 42, staticcheck 13) |
| `go vet ./...`, `gofmt -l .` | limpos |
| `TestRenamedIdentifiersAreNotCitedByTheirOldNames` | verde, com mais de dois mil nomes velhos procurados (os TSV do workflow mais o manual) — a primeira rodada acusou duas citações, corrigidas acima |
| `TestInterfaceMethodRenamesAreChargedLikeAnyOtherLeaf`, `TestGlossaryHasBothColumnsAndNoDuplicateTerm`, `TestCitacoesDeSecaoResolvem`, `TestEvidenciasCitadasNosDocumentosExistem` | verdes |

## O que ficou de fora, e a regra daqui em diante

- **Fase 2, resíduo** (2 429 declarações com sinal PT-BR, metade locais):
  renomear quando a feature tocar o arquivo, no mesmo PR, e gravar o par em
  `mapeamento-2-manual.tsv` — é o que faz o gate de citação pendurada e o
  `renamecite` conhecerem o nome novo. Termo novo entra no glossário.
- **Fase 3 — mensagens, chaves de log e contratos** (config YAML, wire,
  métricas, nomes em disco como `prazos.idx`): não rodou. Mensagens de erro
  e de log novas nascem em inglês; as existentes mudam quando o código ao
  redor mudar. Contratos só mudam com decisão explícita e registrada, porque
  a partir da primeira release passam a ter compatibilidade a considerar.
- **Fase 4 — nomes de teste** (2 467): ficam em PT-BR até o teste ser
  reescrito; o formato sujeito-ação-condição vale nas duas línguas.
- **Fase 5 — clients**: o Python teve `connect`; o JDBC não foi tocado.
- **Fase 6 — deploy**: nada a implantar — não houve mudança de contrato, o
  piloto não precisa ser recriado.
- As ferramentas (`scripts/renameinv`, `scripts/renamecite`,
  `scripts/ifacerename`, `scripts/rename-apply.sh`) e os gates ficam no
  repositório: são o mecanismo do incremento, não sobra do ciclo.

Débito 116 em [`debitos.md`](debitos.md) registra o resíduo com os números
desta tabela e o critério de entrada.

## Método

O inventário é `go/types` (`scripts/renameinv`), nunca grep por acento. A
régua de "sinal PT-BR" é a mesma nos dois inventários e está descrita acima
com as suas limitações. Os pares do fechamento à mão foram extraídos do diff
com validação contra a árvore atual e conferidos por amostra antes da passada
de prosa; o diff da passada foi lido inteiro antes do commit, e o único
arquivo que ela corrompeu (o registro De → Para) foi restaurado do commit
anterior. Nenhum número deste documento foi copiado de relatório de agente:
todos vêm de comandos rodados sobre a árvore no dia do fechamento.
