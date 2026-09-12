# Aceite — schema declarado por dataset e catálogo para o JDBC

Rodada de aceite em 11/09/2026, na branch `feature/schema-declarado-e-catalogo-jdbc`,
sobre o código da branch no commit `3680c6b` (`v0.7.0-224-g3680c6b`). Atende a
[`planning/schema-declarado-e-catalogo-jdbc.md`](../planning/schema-declarado-e-catalogo-jdbc.md).
Registro de rodada: não é reescrito.

## O critério

A proposta nasceu de três fatos do código, e o aceite tem de ser lido contra os
três:

1. **O driver não escondia as tabelas virtuais — ele não sabia que existiam.**
   O `/v1/metadata` publicava os datasets base e suas colunas, e nada mais;
   `_counters.<nome>`, `_meta.fields`, `_meta.counters` e `_meta.kafka` não
   atravessavam o fio.
2. **As tabelas virtuais estavam presas ao dataset da requisição.** `FROM
   _counters.x` não nomeia dataset nenhum e cai no default da conexão, de modo
   que, com dois datasets no servidor, os contadores de um eram inalcançáveis
   da conexão do outro. Era limitação de então, não consequência da proposta.
3. **A gramática do `FROM` tem dois níveis, e isso é decisão de produto**
   (`internal/query/parser/parser.go`: "nome no FROM tem no máximo dois níveis").
   Qualquer coisa que vire schema consome um nível, e não sobra um terceiro.

O fato 3 é a restrição dura, e é ela que decide o mapeamento JDBC: com dois
níveis, o de cima é o **schema** e o catálogo (`TABLE_CAT`) fica vazio — não há
onde pendurar um terceiro nome.

Daí as seis peças que o ciclo tinha de entregar: `schemaName` obrigatório e
único; colisão de nome irrepresentável por construção; um catálogo que seja
fonte única do que cada dataset publica; a `_meta.tables` respondendo isso por
SQL; o `/v1/metadata` publicando o mesmo, aditivamente; e o driver JDBC montando
a árvore a partir dele.

## Critérios e as evidências

| Critério | Provado por |
|---|---|
| `schemaName` é obrigatório no YAML do dataset e **o dataset sem a chave não sobe** — sem default derivado do nome e sem ausência aceita | `TestSpecExigeSchemaName` (que ainda alimenta de volta ao `Parse` o exemplo que a recusa mostra), `TestSpecAceitaSchemaName` |
| A gramática é `[a-z0-9_]+` e o prefixo `_` é reservado; vazio escrito à mão também é recusado | `TestSpecRecusaSchemaNameInvalido` (4 subcasos) |
| O valor chega ao compilado, que é o que o motor carrega depois da subida; a tabela irmã do histórico **herda** o schema do base | `TestCompiladoCarregaOSchemaName`, `TestDerivaHistoricoHerdaOSchemaDoDatasetBase` |
| As quatro specs sintéticas (as tabelas virtuais) usam um schema reservado que nenhum YAML de usuário consegue declarar | `TestArquivoDeDatasetRecusaOSchemaReservadoDasTabelasVirtuais`, `TestSpecSinteticaAtravessaAValidacaoComOSchemaReservado` |
| O schema é **único no servidor**, e a recusa nomeia o schema e os DOIS datasets | `TestDoisDatasetsNoMesmoSchemaSaoRecusados`, `TestDatasetsComSchemasProprioSobem` (a contrapartida: sem ela, recusar todo segundo dataset passaria) |
| A tabela irmã não conta como um segundo dataset na unicidade | `TestATabelaIrmaDoHistoricoNaoContaComoSegundoDataset` |
| A regra vale pela MESMA porta na subida do servidor e nos comandos locais que leem o diretório | `TestDoisDatasetsNoMesmoSchemaRecusamASubida`, `TestQueryLocalRecusaDiretorioComSchemaRepetido`, `TestOffsetsRecusaDiretorioComSchemaRepetido`, `TestOffsetsRecusaDatasetNaoDeclaradoNoDiretorio` |
| Nome de contador não pode ter o prefixo reservado, nem ser o nome do dataset, nem o da tabela irmã do histórico — e a recusa da irmã não compara contra o literal `_history` | `TestCountersRecusaPrefixoReservado`, `TestCountersRecusaNomeDoProprioDataset`, `TestCountersRecusaNomeDaTabelaHistorica` (4 casos, três deles com `suffix:` trocado ou sem bloco `history`) |
| O catálogo lista a base como `TABLE` no schema declarado, os contadores como `VIEW` no mesmo schema e as tabelas de meta como `VIEW` no `_meta` | `TestCatalogoListaABaseOsContadoresEAsTabelasDeMeta` |
| A ordem do catálogo é estável (não é a do mapa) | `TestCatalogoTemOrdemEstavel` (64 voltas comparando a lista inteira) |
| A `_meta.fields`, que não vem de registro, entra uma vez só — e um registro homônimo dela, que o `Execute` nunca alcança, não vira segunda linha | `TestCatalogoNaoListaDuasVezesAFieldsSombreada` |
| Cada view publica as colunas DA PRÓPRIA view, e não as do dataset | `TestCatalogoDaACadaViewAsColunasDaPropriaView` (que falha se o fixture convergir para o dublê e a comparação deixar de provar algo) |
| A linha publica o `namespace` que abre a tabela, e ele é o endereço que o `Execute` aceita — não só um valor plausível | `TestCatalogoPublicaONamespaceQueAbreAView` (monta o `FROM` e executa) |
| `_meta.tables` responde por SQL que tabelas existem, **incluindo ela mesma**, com o schema, o namespace, a espécie e a contagem de colunas de cada uma | `TestMetaTablesListaOCatalogo`, `TestMetaTablesSeInclui`, `TestMetaTablesPublicaOSchemaDeCadaTabela`, `TestMetaTablesNamespaceEhNuloSoNaTabelaBase`, `TestMetaTablesContaAsColunasDeCadaTabela` |
| A key é o ENDEREÇO (`namespace.name`) e não o nome, porque um contador chamado `tables` é aceito e teria o mesmo nome | `TestMetaTablesSeparaHomonimasPeloEndereco` |
| `description` é publicada e sai NULL em toda linha, porque nada declara descrição de tabela — nenhum texto foi inventado | `TestMetaTablesPublicaDescriptionNulaEnquantoNadaADeclara` |
| A tabela não aloca por linha, nem na varredura nem no point lookup | `TestTabelaMetaTablesNaoAlocaPorLinha` |
| A fiação existe nos TRÊS caminhos que registram um dataset consultável, e cada teste acusa o seu | `TestMetaTablesRespondePelaRotaDeConsultaDoServidor`, `TestMetaTablesRespondeNaTabelaIrmaAbertaPeloServidor`, `TestQueryLocalRespondeMetaTablesComOCatalogoDoDataset` |
| `FROM <schema>.<tabela>` resolve **o dataset dono**, inclusive um diferente do handle da conexão, na base e nas views | `TestSchemaQualificadoResolveODatasetCerto` (confere pelo CONTEÚDO: a base do harness está vazia, e "zero linhas" passaria igual abrindo a errada) |
| O atalho `_ns.<nome>` continua significando "no dataset da requisição", e não virou busca global | `TestAtalhoSemSchemaUsaODatasetDaRequisicao` |
| Qualificador que não é schema declarado — antes aceito e descartado em silêncio — passa a ser **recusado**, e as três recusas são erro de bind com a posição do token ofensor | `TestQualificadorQueNaoEhSchemaEhRecusado`, `TestSchemaDesconhecidoListaOsExistentes`, `TestSchemaConhecidoSemATabelaListaAsDoSchema` |
| A lista que a recusa oferece não entrega os espaços de nome do MOTOR como endereço | `TestRecusaDeSchemaNaoOfereceOsNamespacesDoMotor` (confere pelo PREFIXO `_`, e antes de o `_meta` existir no catálogo) |
| A resolução do `FROM` qualificado não cresce com o dicionário do dataset | `TestResolucaoQualificadaNaoAlocaPorCampoDoDataset` (em BYTES; ver "A bancada") |
| Os limites de consulta (`query.maxRows`, `query.timeout`, `query.maxTimeout`) são do dataset **LIDO**, e não do handle | `TestOsLimitesSaoDoDatasetLidoENaoDoHandle` (os dois lados: o handle alto não abre o dataset de teto baixo, e o handle baixo não fecha o de teto alto) |
| O `/v1/metadata` publica `schema` e `tables` com a enumeração inteira, cada coluna já com o código `jdbc` do servidor | `TestMetadataPublicaOSchemaEOCatalogoDeTabelas` (lista inteira e em ordem, não um `Contains`) |
| Os dois canais — SQL e HTTP — não podem discordar, porque saem do mesmo catálogo | `TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo` (no dataset base E na irmã, cujas listas são diferentes) |
| Os campos são **aditivos**: um cliente compilado antes deles lê o documento como lia, e `columns` continua sendo a lista completa da base | `TestOClienteAnteriorAosCamposNovosContinuaLendoOMetadata` (contra um tipo anterior escrito à MÃO; derivá-lo do tipo corrente faria o oráculo acompanhar a mudança que ele vigia) |
| O driver JDBC monta a árvore: base como `TABLE` no schema declarado, contador como `VIEW` no mesmo schema, tabelas de meta como `VIEW` no `_meta`, todas com `TABLE_SCHEM` preenchido | `NishiDatabaseMetaDataTest#listaBaseEViewsComSchema`, `NishiDatabaseMetaDataTest#osSchemasSaemDistintosEOrdenados` |
| O filtro de tipo e o de padrão de schema funcionam como o JDBC manda | `NishiDatabaseMetaDataTest#filtraPorTipoDeTabela`, `NishiDatabaseMetaDataTest#filtraPeloPadraoDeSchema`, `NishiDatabaseMetaDataTest#oPadraoLikeCasaComoOJdbcManda` |
| `getColumns` cobre as views pelo mesmo documento, com `ORDINAL_POSITION` por TABELA | `NishiDatabaseMetaDataTest#colunasDaViewSaemComTipoJdbc`, `NishiDatabaseMetaDataTest#oOrdinalPositionEhPorTabelaEContaAColunaDescartada` |
| A chave primária e o índice saem no mesmo schema da tabela que o `getTables` publica, e honram o schema recebido | `NishiDatabaseMetaDataTest#aChaveDoDatasetSaiComoChavePrimariaNoSchemaDoDataset`, `NishiDatabaseMetaDataTest#oIndiceSaiComARessalvaDeQueEhDeclaradoENoSchemaDoDataset`, `NishiDatabaseMetaDataTest#aChaveEOIndiceHonramOSchemaRecebido`, `NishiDatabaseMetaDataTest#aChaveDoServidorSemSchemaSaiPelaStringVazia` |
| Servidor anterior aos campos novos: a árvore de sempre, nenhuma view inventada, nada lançado | `NishiDatabaseMetaDataTest#metadataSemTablesDegradaSemErro` (com fixture só seu) |
| Contra o binário REAL: os schemas são os do servidor, os tipos são `TABLE` e `VIEW`, e o `FROM` montado a partir do par (schema, nome) que a árvore devolveu executa | `DriverIT#aSequenciaQueUmaFerramentaFazAoAbrirAConexao` |

## As mutações: o vermelho e o verde de cada uma

Doutrina de verificação: o defeito foi aplicado, a suíte ficou vermelha, o
defeito foi revertido, a suíte voltou ao verde. Nenhuma ficou no código
(conferido por `git status` antes de cada commit).

| Mutação aplicada | Reprovou em | O que a falha disse |
|---|---|---|
| O `schemaName` ausente vira default derivado do nome do dataset | `TestSpecExigeSchemaName` e `TestSpecRecusaSchemaNameInvalido` | dois testes, não um: sob o default, o `schemaName: ""` escrito à mão também passa a ser aceito — a mesma falha de desenho por outro ângulo |
| A recusa deixa de mostrar um exemplo copiável | `TestSpecExigeSchemaName` | "a mensagem não traz um `schemaName: <valor>` entre acentos graves" — recusa que não ensina a correção só transfere o trabalho |
| A cópia do `SchemaName` some do `Compile` e do `DeriveHistory` | `TestCompiladoCarregaOSchemaName`, `TestDerivaHistoricoHerdaOSchemaDoDatasetBase` | `SchemaName` da irmã = `""`, quer o do base |
| A guarda do `Load` contra o schema reservado é removida | `TestArquivoDeDatasetRecusaOSchemaReservadoDasTabelasVirtuais` | um YAML de usuário com o sentinela das tabelas virtuais foi aceito |
| O sentinela volta a ser um nome legal de usuário | `TestSpecSinteticaAtravessaAValidacaoComOSchemaReservado` | "sem o prefixo reservado, o sentinela vira um schemaName que um dataset de usuário também pode declarar" |
| A unicidade de schema vira "o último vence" | `TestDoisDatasetsNoMesmoSchemaSaoRecusados`, `TestDoisDatasetsNoMesmoSchemaRecusamASubida` e `TestQueryLocalRecusaDiretorioComSchemaRepetido` | as TRÊS camadas caem com a mutação num lugar só — é a prova de que existe UMA verificação, e não três |
| O `offsets` volta a abrir um arquivo só, fora da porta comum | `TestOffsetsRecusaDiretorioComSchemaRepetido` | `code = 0, quer 1`: o comando respondeu normalmente num diretório ambíguo |
| As mensagens das duas recusas de contador são TROCADAS entre si (sem mudar qual `if` dispara) | `TestCountersRecusaPrefixoReservado` e `TestCountersRecusaNomeDoProprioDataset` | os dois caem: o assert olha o MOTIVO, e não a existência do erro |
| A recusa do nome da tabela irmã é desarmada | `TestCountersRecusaNomeDaTabelaHistorica` | `name: events` + `history:` + `counters: [{name: events_history}]` era aceito e publicava o mesmo par duas vezes num arquivo só |
| O catálogo é ordenado só pelo grupo, deixando as views em ordem de mapa | `TestCatalogoTemOrdemEstavel` | vermelho em posições DIFERENTES a cada rodada (`por_pon` antes de `por_olt` numa volta, `kafka` antes de `counters` em outra) |
| As tabelas do `_meta` saem no schema do dataset | `TestCatalogoListaABaseOsContadoresEAsTabelasDeMeta`, `TestMetaTablesSeInclui`, `TestMetaTablesPublicaOSchemaDeCadaTabela`, `TestMetaTablesSeparaHomonimasPeloEndereco` | a lista saiu `VIEW:telecom.fields`, `VIEW:telecom.counters`, `VIEW:telecom.kafka` |
| As colunas de uma view passam a ser as do dataset (`reg.sch` no lugar de `v.sch`) | `TestCatalogoDaACadaViewAsColunasDaPropriaView` | nas duas views, um contador e uma tabela de meta |
| O schema publicado da view sai da spec sintética (`v.sch.SchemaName`) | `TestCatalogoListaABaseOsContadoresEAsTabelasDeMeta` e mais dois | a lista saiu com `VIEW:_virtual.por_olt` |
| O catálogo é congelado no REGISTRO, e não montado a cada consulta | seis testes da `_meta.tables` | a tabela some da própria listagem e leva junto tudo que foi registrado depois dela |
| Cada uma das TRÊS chamadas de `RegisterMetaTables` é apagada, uma de cada vez | um teste distinto de cada vez, e só ele | `_meta não tem tabela "tables" no dataset "d"` / `"eventos_history"` / `"servicos"` — as três fiações são independentes e os três testes sabem disso |
| A linha da `_meta.tables` é montada com `fmt.Sprintf` | `TestTabelaMetaTablesNaoAlocaPorLinha` | `allocs/linha = 2.0, quer 0` (e o mesmo no point lookup) |
| A comparação dataset/`FROM` roda ANTES da resolução do schema | os 4 testes de roteamento | `dataset "por_olt" desconhecido (este handle é de "eventos")` — exatamente a mensagem de antes do ciclo |
| A resolução ignora o schema declarado e casa só pelo nome da tabela | `TestQualificadorQueNaoEhSchemaEhRecusado` e `TestSchemaConhecidoSemATabelaListaAsDoSchema` | o qualificador inventado voltou a abrir a base, e a lista vazou as tabelas do outro schema |
| O filtro do prefixo `_` sai da lista que a recusa oferece | `TestRecusaDeSchemaNaoOfereceOsNamespacesDoMotor` | `existem: _meta, rede, telecom` — a recusa oferecendo como endereço um espaço de nome do motor |
| A resolução volta a pedir o catálogo COMPLETO (colunas inclusive) | `TestResolucaoQualificadaNaoAlocaPorCampoDoDataset` | `3336 bytes com 40 campos e 35568 com 4.000` — e a forma NÃO qualificada, no mesmo teste, ficou verde como controle |
| Os limites voltam a sair do handle | `TestOsLimitesSaoDoDatasetLidoENaoDoHandle` | `status = 200, quer 400`: o teto do dataset lido deixou de valer |
| O `/v1/metadata` monta `tables` pelos REGISTROS do motor, e não pelo catálogo | `TestMetadataPublicaOSchemaEOCatalogoDeTabelas`, `TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo` | a lista do registro é **plausível** — erra só a `_meta.fields`, que o catálogo acrescenta à parte |
| Os campos novos somem do JSON (`json:"-"`) | os dois testes acima, e o da aditividade ficou VERDE | é o ponto: um documento sem os campos novos é o que um cliente anterior vê, e ele continua lendo `name`, `key`, os limites e `columns` |
| O schema da linha do contador sai da spec sintética, no canal HTTP | `TestMetadataPublicaOSchemaEOCatalogoDeTabelas` | `{Name:por_status Schema:_virtual Namespace:_counters}`; e `TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo` continuou VERDE, como tem de continuar — um oráculo de concordância não é oráculo de correção |
| `TABLE_SCHEM` nulo nas linhas `VIEW` do driver | 3 testes JUnit | `VIEW\|null\|por_olt` no lugar de `VIEW\|telecom\|por_olt` — e o SQL que a ferramenta monta passa a cair no dataset da conexão |
| `acceptsType` devolve sempre `true` | 3 testes JUnit | pedir `{"TABLE"}` trouxe as views, e pedir `{"VIEW"}` trouxe as bases |
| A degradação quebra (o driver assume que `tables` existe) | `NishiDatabaseMetaDataTest#metadataSemTablesDegradaSemErro` | árvore vazia contra o servidor anterior aos campos novos |
| A deduplicação das linhas do `_meta` é removida | 4 testes JUnit | cada tabela do `_meta` saiu uma vez por dataset do servidor |
| `ORDINAL_POSITION` é contado fora do laço por tabela, ou incrementado depois do filtro de coluna | `NishiDatabaseMetaDataTest#oOrdinalPositionEhPorTabelaEContaAColunaDescartada` | `telecom\|por_olt\|olt\|5` no lugar de `1`, e `telecom\|eventos\|status\|1` no lugar de `\|2` |
| O nome do teste JUnit citado na arquitetura ganha um sufixo; e, numa segunda mutação, a classe citada é trocada por outra | `TestEvidenciasCitadasNosDocumentosExistem` | "cita ⟨classe⟩#⟨método⟩, que não existe na suíte", nas duas — o nome ofensor não é reproduzido aqui de propósito, porque este documento também é varrido pelo gate |
| A leitura da anotação `@Test` do gate de evidências quebra | `TestEvidenciasCitadasNosDocumentosExistem` | `só 0 testes JUnit indexados: a leitura dos clients quebrou` — o piso próprio, provando que o gate não passa por vacuidade |

### E este documento também é cobrado

Ele entrou na lista de `TestEvidenciasCitadasNosDocumentosExistem`
(`internal/cli/evidence_test.go`) no mesmo commit em que nasceu, e o gate foi
exercitado contra ele nas duas grafias que a tabela acima usa:

| Mutação no PRÓPRIO documento | O que a falha disse |
|---|---|
| O nome de teste Go da linha dos limites de consulta tem duas letras trocadas no fim | `aceite-schema-declarado.md cita ⟨o nome mutado⟩, que não existe na suíte` |
| A CLASSE do primeiro teste JUnit citado é trocada por outra classe de teste que EXISTE na suíte, mantido o nome do método | `aceite-schema-declarado.md cita ⟨outra classe⟩#listaBaseEViewsComSchema, que não existe na suíte` |

Verde depois de reverter as duas, e `git status` limpo antes do commit. A
segunda mutação é a que importa: ela prova que a citação JUnit é conferida
**qualificada** — trocar a classe por outra que existe não a salva, porque o
índice do gate é `Classe#metodo`.

Os nomes mutados não aparecem escritos acima, e a razão é um efeito colateral
que vale saber: **um documento varrido pelo gate não pode citar o nome que ele
está dizendo não existir**. A primeira redação desta seção reproduzia as
mensagens de falha na íntegra, e o gate reprovou o documento por causa das
próprias mensagens — acusando, sem mutação nenhuma, que ele já estava sendo
lido.

## A bancada

**O custo do `FROM` qualificado.** Rodada de 11/09/2026, saída crua em
[`benchmark/baselines/from-qualified-20260911.txt`](../benchmark/baselines/from-qualified-20260911.txt):
Intel Core 7 150U (12 threads), Linux 6.8, go1.26.7, `-benchmem -count=3`,
mediana das três.

| campos declarados | `FROM <nome>` | `FROM <schema>.<nome>` | fator | B/op qualificado |
|---|---|---|---|---|
| 40 | 2.368 ns | 3.508 ns | 1,5x | 2.216 |
| 400 | 2.317 ns | 3.577 ns | 1,5x | 2.216 |
| 4.000 | 3.040 ns | 4.778 ns | 1,6x | 2.216 |

O que a tabela registra é a propriedade, não o número: **os bytes por consulta
da forma qualificada são constantes no tamanho do dicionário** (2.216 nos três
tamanhos, contra 1.584 da forma simples), e é essa igualdade que o gate
`TestResolucaoQualificadaNaoAlocaPorCampoDoDataset` trava. O fator residual de
~1,5x é a enumeração em si — varrer os datasets registrados e montar as três
strings por linha —, que é o que a feature custa.

**Os números de ANTES não são desta rodada, e não podem ser**: o defeito já não
existe. Eles vêm da medida da Task 7 deste ciclo (11/09/2026, mesmo hardware,
`BenchmarkExecuteFromQualified`), onde as duas metades foram medidas juntas, na
mesma passada: com 4.000 campos a forma qualificada custava **479 µs e 35.520
B/op** contra ~4,2 µs e ~1,5 KiB da simples — **113x** —, porque a resolução
pedia o catálogo COMPLETO, com as colunas montadas e ordenadas, só para ler três
strings por linha. Depois de separar a enumeração da montagem de colunas: 84x
mais rápido e 16x menos lixo. É esta seção a origem dos números citados em
[`docs/architecture.md`](architecture.md), seção "O `FROM` qualificado pelo
schema".

A forma qualificada é a que uma ferramenta gráfica gera sozinha a partir da
árvore — foi o que o spike mediu —, de modo que a regressão estava exatamente no
caminho quente do cliente-alvo.

**O custo em bytes do `/v1/metadata`.** Medido contra binário real, mesmo corpus,
os dois binários, num dataset de 301 colunas: 86.950 → 207.955 bytes (2,39x),
com a repartição por bloco e a extrapolação para o dataset-alvo. Não se repete
aqui: está no **débito 119** de [`docs/debitos.md`](debitos.md), que é onde o
critério de entrada mora.

## O que o DBeaver disse, e o que ele ainda não disse

Esta é a evidência que nenhum teste Go dá, e ela está pela metade. **A metade
que existe** é o spike da Task 1, rodado pelo usuário em 11/09/2026 com um jar
descartável que falsificava só o driver, contra o servidor de então. Três
perguntas, três respostas:

1. **A árvore aparece com o schema do dataset em cima, com as pastas Tables e
   Views?** Sim.
2. **O SELECT que a ferramenta gera é QUALIFICADO?** Sim —
   `FROM telecom.autenticados_por_pon`. É a resposta que decidiu o ciclo: é por
   ela que o `TABLE_SCHEM` é o campo que paga a feature, e é por ela que o
   `FROM <schema>.<tabela>` (Task 7) tinha de existir no servidor.
3. **A `SYSTEM TABLE` aparece ao lado das outras?** Não. O DBeaver **esconde**
   objeto de sistema por default, e a opção que o revela não foi achada na
   interface. A separação aconteceria por ocultação, não por pasta ao lado —
   e tabela que existe para ser descoberta não pode nascer invisível. Foi o que
   mudou o desenho no meio do ciclo: as tabelas de meta saíram do schema do
   dataset e viraram o schema `_meta`, tipadas `VIEW`.

Naquele spike o SELECT qualificado ainda falhava no servidor
(`dataset X desconhecido (este handle é de Y)`), como se esperava: o jar
falsificava o driver, e o roteamento era a Task 7.

**A metade que faltava** quando esta seção foi escrita, e que fechou em
12/09/2026 (o desfecho está logo abaixo): abrir o
DBeaver contra o **driver real** desta branch e repetir as três perguntas. Não
há sessão gráfica na árvore em que o ciclo rodou. O que existe no lugar é o
`DriverIT#aSequenciaQueUmaFerramentaFazAoAbrirAConexao`, que faz contra o
binário real a sequência mecânica — `getCatalogs`, `getSchemas`,
`getTableTypes`, `getTables` — e **executa o `FROM` montado a partir do par
(schema, nome) que a própria árvore devolveu**, de modo que um `TABLE_SCHEM`
nulo vira `SELECT id FROM null.todos_os_tipos` e o servidor recusa. O que ele
não cobre é a suposição de interface: se a ferramenta monta a árvore como o
spike mediu, e se ela qualifica o `FROM` com o `TABLE_SCHEM` deste driver.

### O desfecho: 12/09/2026, contra o driver desta branch

O aceite pelo DBeaver real aconteceu em 12/09/2026, rodado pelo usuário contra
o driver desta branch e um servidor de verdade. Três fatos foram verificados, e
é só isso que este registro afirma:

- **a árvore monta, com os dois schemas** — o do dataset e o `_meta`;
- **o SELECT que a própria ferramenta gera executa**;
- **o duplo clique nas views abre os dados.**

O item 1 do "o que fica em aberto" está fechado por aqui.

### O defeito que este aceite encontrou

É o achado mais valioso do ciclo, e ele só apareceu porque alguém abriu a
ferramenta. O `SqlScanner` do driver tem DOIS ramos que capturam o nome do
dataset depois do `FROM` — o do identificador NU e o do identificador ENTRE
ASPAS —, e só o primeiro verificava se vinha um ponto depois do nome. O ramo
das aspas capturava sempre: com `"_meta".counters`, que é um nome QUALIFICADO
com a primeira parte citada, o driver mandava `dataset: "_meta"` no corpo, e o
servidor recusava um dataset que não existe.

A consequência é exatamente a que o duplo clique expõe: **nenhuma view do
`_meta` abria**. A árvore montava inteira e não abria nada — o modo de falha
que o `getTables` verde esconde, porque montar a árvore e abrir uma tabela são
caminhos diferentes.

Corrigido em `44a3f70`: o ramo das aspas passa a aplicar a mesma regra, com o
ponto procurado a partir do índice logo APÓS a aspa de fechamento (procurar do
miolo tiraria o palpite de `FROM "_meta.counters"`, que é um identificador só e
cuja recusa é do servidor). As cinco formas entram no `SqlScannerTest` —
`SqlScannerTest#nomeQualificadoComAPrimeiraParteCitadaAbandonaOPalpite`,
`SqlScannerTest#nomeQualificadoCitadoInteiroContinuaSendoUmIdentificadorSo`,
`SqlScannerTest#nomeCitadoSemPontoDepoisContinuaSendoPalpiteValido`,
`SqlScannerTest#aContagemDePlaceholdersNaoMudaComAFormaDoNome` e
`SqlScannerTest#apenasOPrimeiroFromContaTambemComNomeCitado` —, e o caminho
inteiro ganhou um caso contra o binário real,
`DriverIT#oSqlComCadaParteCitadaDaArvoreExecuta`, que monta o endereço a partir
da árvore e das aspas que o metadata declara.

**E o que ele ensina vale mais que a correção: o defeito atravessou nove
revisões e toda a verificação deste ciclo.** A razão é precisa, e vale escrever
por inteiro. O `SqlScanner` ERA exercitado contra o binário real: o
`DriverIT#aSequenciaQueUmaFerramentaFazAoAbrirAConexao` monta o `FROM` a partir
do par (schema, nome) que a árvore devolveu. Só que ele o monta CONCATENANDO as
duas partes com um ponto, sem aspas — e essa é justamente a forma que o ramo do
identificador NU já tratava certo. A ferramenta de verdade escreve a outra: ela
cita cada parte com o `getIdentifierQuoteString`, que o próprio IT confere ser
`"`. O ramo das aspas, portanto, não era percorrido por teste nenhum, e todo o
resto da verificação correu pelo CLI, que não passa pelo scanner.

Nenhuma quantidade de revisão do que ERA testado encontraria isso. Não é
nenhuma das classes que a doutrina nomeia — não é texto que mente nem citação
que não resolve —, e a forma dela é a do achado nº 2 desta mesma rodada: **um
teste que monta ele mesmo a entrada do cliente prova a forma que o teste
escreveu, não a que o cliente escreve.** Onde houver um cliente que GERA a
entrada, a entrada tem de sair dele.

## Três achados de método, que valem mais que o código

**1. Um gate de alocação passou com o defeito presente, porque contava
alocações e o defeito mudava só o tamanho.** A primeira versão do gate do `FROM`
qualificado usava `testing.AllocsPerRun`. Montar a lista de colunas de uma
tabela é um `make` com a capacidade já conhecida mais um `sort`: um dicionário
cem vezes maior aloca o mesmo NÚMERO de vezes e cem vezes mais BYTES. O gate foi
mutado de volta para o catálogo completo e **passou verde**. Reescrito para medir
bytes (`bytesPerRun`, sobre `TotalAlloc`), a mesma mutação o derruba. A
regressão que ele deixaria passar era de até 113x, na forma de `FROM` que a
ferramenta gráfica sempre gera. A lição é a do repositório, mais estreita:
**escolher a grandeza que o defeito move, e não a que é mais fácil de medir.**

**2. "A lista INTEIRA" não é prova se a lista não passa pelo ramo que importa.**
O teste do `/v1/metadata` comparava a enumeração inteira, em ordem, e se
anunciava como tal — mas o fixture não declarava contador nenhum, de modo que
o ramo que publica o schema de uma view no schema do DATASET nunca era
percorrido. A mutação que faz esse ramo publicar o schema sintético passava por
aquele canal. O fixture ganhou um contador, e a mutação passou a acusar. É o
mesmo defeito que o ciclo anterior encontrou no teste de injetividade dos
moldes, que só virou prova quando passou a listar também os moldes que o parser
RECUSA: **uma asserção exaustiva sobre um domínio que não contém o caso
interessante é exaustiva sobre nada.**

**3. A classe nº 1 da doutrina — texto normativo que afirma barreira que o
código não estabelece — foi o achado mais frequente do ciclo, de longe.** Ela
apareceu na revisão de SETE das oito tarefas revisadas (2, 3, 5, 6, 7, 8 e 9;
só a 4 escapou, e a revisão dela era sobre asserts fracos). A forma é sempre a
mesma: um godoc escrito no presente do indicativo sobre um mecanismo que a
tarefa SEGUINTE ia construir, e que fica falso por dois dias — ou, pior, um
godoc que a própria tarefa falsifica no commit seguinte ao que o escreveu (o
`Engine.Catalog` prometeu duas vezes "passa a sair daqui na tarefa seguinte", e
as duas vezes a tarefa seguinte era a que estava rodando). Dois hábitos saem
daqui, e são o que este ciclo tem a ensinar ao próximo:

- **texto sobre o que ainda não existe vai no futuro, e nomeia o destino** — foi
  o remédio aplicado em todos os casos, e ele sobrevive à revisão;
- **abrir a fonte antes de afirmar** é a única verificação que funciona, porque
  nenhum gate pega esta classe: os gates fecham a citação que não resolve
  (`§NN` inexistente, nome de teste inexistente), não a frase que resolve para
  um mecanismo que não faz o que ela diz.

## Mudanças de contrato registradas

**Todo YAML de dataset parou de abrir até ganhar a linha `schemaName`.** É
quebra dura de configuração, tomada com o custo à vista: não há release
publicada, e as duas alternativas foram consideradas e recusadas — um default
derivado do nome entregaria `servicos.servicos`, e a ausência entregaria a
schema nula que o cliente mostra. A chave existe justamente para desfazer a
ambiguidade entre datasets, e um default a reintroduziria.

**As tabelas de meta mudaram de lugar no meio do ciclo.** O desenho aprovado as
punha em `telecom._fields`, tipadas `SYSTEM TABLE`. Depois do spike (razão 3
acima) elas passaram a morar no schema `_meta`, tipadas `VIEW`. O SQL não mudou:
`_meta.<nome>` é o atalho de sempre e continua sendo o único endereço que essas
tabelas têm.

**`FROM <qualquercoisa>.<tabela>` deixou de ser aceito.** Antes do ciclo o
qualificador era lido e descartado, e `FROM telecom.x`, `FROM inventado.x` e
`FROM x` eram a MESMA consulta. Agora o qualificador que não é schema declarado
é recusado, nomeando os que existem. Quem dependia do descarte silencioso
quebra — e é o que se quer, porque ele não podia estar certo.

**`_counters.<nome>` e `_meta.<nome>` continuam valendo**, como abreviação do
dataset da requisição. São contrato publicado; o que mudou é que a forma
canônica na documentação passou a ser a qualificada.

**Os limites de consulta passaram a ser do dataset lido.** Uma conexão com
`maxRows` alto deixou de ser porta para ler, sob aquele teto, um dataset cujo
YAML declara teto baixo.

## Verificação da rodada

Rodado no fechamento, em 11/09/2026, sobre o commit `d20b9a5`:

| Comando | Resultado |
|---|---|
| `make test` | 51 pacotes `ok`, nenhum `FAIL` (com os gates de medida) |
| `make test-race` | 51 pacotes `ok`, nenhum `FAIL`, nenhum `DATA RACE` |
| `make lint` | **333** apontamentos (errcheck 278, revive 42, staticcheck 13), IGUAL a `.golangci-baseline` — a saída != 0 é o esperado enquanto a baseline > 0 |
| `make test-jdbc` | **217 testes, 0 falhas, 0 SKIP** — o `DriverIT` rodou contra o binário real, com TLS e credencial |

`make test-integration` não foi rodado: está quebrado desde antes deste ciclo
(`undefined: bancada` em `internal/kafka/integration_test.go`, resíduo do ciclo
de identificadores em inglês), e nenhuma tarefa o tocou.

## O que fica em aberto

1. ~~**O aceite pelo DBeaver real está pendente** (seção acima). É o único item
   do plano que nenhum teste substitui.~~ **FECHADO em 12/09/2026**: a árvore
   monta com os dois schemas, o SELECT gerado pela ferramenta executa e o duplo
   clique nas views abre os dados. Ele cobrou o preço de ser o único item sem
   teste substituto — encontrou o defeito do `SqlScanner` que deixava toda view
   do `_meta` sem abrir (`44a3f70`). O relato está em "O desfecho: 12/09/2026".
2. **O piloto não foi tocado nesta rodada.** O `service_view.yaml` precisa ganhar
   a linha `schemaName` antes de qualquer subida com este binário — sem ela o
   dataset não abre. Fica como o primeiro passo de quem for fazer o deploy.
3. **Três débitos abertos pelo ciclo**, todos com critério de entrada:
   **117** (a tabela irmã publica catálogos diferentes conforme o processo que a
   atende), **118** (o driver clampa `maxRows`/`setQueryTimeout` pelo dataset da
   conexão enquanto o servidor aplica os do dataset lido) e **119** (o
   `/v1/metadata` publica as colunas da base duas vezes).
4. **O label de métrica continua sendo o do handle.** `nishidb_query_total{dataset=…}`
   conta no dataset da requisição, e desde a Task 7 os LIMITES já são do dono.
   As duas séries descrevem coisas diferentes, e a divergência ficou mais
   visível do que estava. É decisão à parte, não regressão.
5. **A tradução do `internal/server` não é deste ciclo.** O código NOVO nasceu em
   inglês e o que este ciclo INTRODUZIU em PT-BR foi renomeado no fechamento
   (`planning/identificadores-em-ingles/mapeamento-2-manual.tsv`). O que ficou
   são identificadores PRÉ-EXISTENTES que as linhas tocadas apenas usam —
   `dataset.nome`, `byNome`, `prazo`, `longa`, `contextoOfConsulta`, `specIrma`
   —, e traduzi-los é migração de pacote, não edição de feature: o `internal/server`
   é majoritariamente PT-BR (o débito 116 mede 434 declarações com sinal PT-BR ali), e renomear cinco
   deles deixaria o módulo mais misturado do que está. Fica declarado aqui em vez
   de passar em silêncio.
6. **`make test-integration` continua quebrado**, e já estava antes do ciclo
   (`undefined: bancada` em `internal/kafka/integration_test.go`, resíduo do
   ciclo de identificadores). Nenhuma tarefa o tocou, por instrução.
7. **As mensagens de erro novas nasceram em PT-BR, e isso é divergência
   declarada.** O critério do débito 116 diz que "mensagem e chave de log novas
   nascem em inglês", e o ciclo não o seguiu: as dezenas de mensagens que ele
   introduziu — as recusas do `keyFormat`, as da validação dos contadores contra
   o schema, as do `FROM` qualificado e as falhas internas da `_meta.tables` —
   saíram todas em PT-BR. A decisão foi seguir a BASE, pela exceção que o
   próprio critério de idioma prevê: o wire de erro deste servidor é PT-BR de
   ponta a ponta, e traduzir só as novas deixaria o cliente lendo duas línguas
   na mesma sessão, que é pior do que ler uma língua que o critério não
   prefere. **Quatro delas atravessam o fio até os clients** — as recusas de
   roteamento por schema (`internal/query/engine.go`: schema inexistente,
   tabela inexistente no schema, e o par ambíguo), mais o complemento de
   `wholeQuotedNameHint` (`internal/query/binder/binder.go`, commit `5e6f204`)
   que nomeia a forma certa quando um nome qualificado é citado INTEIRO entre
   aspas. As quatro saem por `binder.ErrorAt` e chegam ao JDBC e ao Python como
   `message` de um erro de bind. A tradução delas é trabalho do ciclo do débito
   116, junto com o resto do wire, e não deste: feita aqui, seria mudança de
   contrato de saída sem a decisão que o próprio débito exige. Fica registrado
   para que a divergência seja lida como escolha, e não como descuido.

   **Atualização de 12/09/2026:** esse complemento passou a sair também pela
   recusa de **dataset desconhecido** do `POST /v1/query`
   (`internal/server/query.go`), que é o ponto em que o cliente JDBC é
   recusado — o driver lê o nome do `FROM` e o manda no corpo, então
   `FROM "telecom.service_view"` morre no 404 antes de o SQL ser analisado, e o
   bind nunca roda. É a MESMA função, agora exportada como
   `binder.DottedDatasetNameHint`, e não uma segunda redação da regra. O texto
   passou a enunciar a regra das aspas em vez de afirmar que o chamador as
   escreveu: no caminho HTTP o nome chega num campo do corpo, e o servidor não
   tem como saber se ele veio de um `FROM` citado inteiro.
