# Aceite — formato declarável do `groupKey`

Rodada de aceite em 11/09/2026, na branch `feature/formato-declaravel-do-groupkey`,
sobre o binário `v0.7.0-175-g1148c0e-dirty`. Atende a issue #38. Registro de rodada: não é reescrito.

## O critério

A issue pediu três coisas, e cada uma tem de ser lida separadamente:

1. **declarar o formato do `groupKey`** por contador — separador escolhido e
   número com zero à esquerda, ou um molde com as colunas nomeadas;
2. o texto formatado é o que sai na coluna **e o que `WHERE groupKey = '…'`
   aceita como busca direta** — ou seja, não basta renderizar, tem de continuar
   sendo point lookup;
3. **sem ambiguidade**: dois grupos distintos nunca podem produzir o mesmo
   texto; e **mudar o formato não pode exigir reconstruir** o contador.

## Critérios e as evidências

| Critério | Provado por |
|---|---|
| O molde é declarado por contador e chega até o texto: `{olt}.{slot:02}` rende `BR_SPOAM_OLT.08` | `TestMoldeDeclaradoEhGolden`, `TestCompilaLevaOMoldeDeclaradoAteADefinicao` |
| Quem não declara vê o texto de sempre — colunas na ordem do groupBy, separadas por `|` | `TestMoldeDefaultEhOSeparadorHistorico`, `TestTextoDaChaveEhGolden` |
| O texto formatado é a chave de BUSCA, e a busca continua sendo point lookup | `TestTabelaComMoldeRespondePeloNomeFormatado` (confere a contagem de point lookups do motor), `TestQueryLocalConsultaPeloGroupKeyFormatado` |
| Ponta a ponta com o binário real: YAML com molde, ingestão, consulta pelo nome do inventário | `TestQueryLocalConsultaPeloGroupKeyFormatado` |
| Não-ambiguidade: todo molde que o `Parse` ACEITA é injetivo, exaustivamente sobre um alfabeto com o escape, os separadores e a string vazia | `TestRenderizacaoEhInjetiva` |
| As recusas na subida, uma por regra, com o caminho do YAML na mensagem | `TestParseRecusaMoldeInvalido` (22 casos), `TestCountersRecusaMoldeInvalido`, `TestCountersAceitaOMoldeDoGroupKey` |
| A lista de chaves do bloco `counters:` cresceu e continua recusando chave desconhecida | `TestCountersRecusaChaveDesconhecidaDepoisDoKeyFormat` |
| **Mudar o molde não reconstrói**, e o texto novo passa a valer na mesma abertura | `TestMudarOMoldeNaoReconstroiEReRenderizaOTexto` |
| A colisão é recusada ANTES de a chave ir ao disco, e a abertura recusa duas chaves com o mesmo texto | `TestColisaoRecusaAntesDeAChaveIrAoDisco`, `TestAberturaRecusaDoisGruposComOMesmoTexto` |
| A guarda não entra no caminho quente | `TestGrupoConhecidoNaoPassaPelaGuarda`, `TestContadorNaoAlocaNoCaminhoQuente` |
| A renderização não aloca | `TestAppendRespeitaOBufferDoChamador`, `TestTextoDaChaveRespeitaOsBuffersDoChamador`, `TestTabelaDoContadorNaoAlocaPorLinha` |
| `_meta.counters` diz o molde efetivo, com o default expandido | `TestMetaCountersMostraOMoldeEfetivo`, `TestTabelaMetaCountersNaoAlocaPorLinha` |

## As mutações: o vermelho e o verde de cada uma

Doutrina de verificação: o defeito foi aplicado, a suíte ficou vermelha, o
defeito foi revertido, a suíte voltou ao verde. Nenhuma ficou no código
(conferido por `git status` antes de cada commit).

| Mutação aplicada | Reprovou em | O que a falha disse |
|---|---|---|
| Literal do molde pode conter `\` (R5 removida) | `TestParseRecusaMoldeInvalido` (2 casos) e **`TestRenderizacaoEhInjetiva`** | o molde `{a}\{c}` passou a compilar e o teste de injetividade achou a colisão sozinho: `("x\","y")` e `("x","\y")` rendem os dois `x\\\y` |
| Coluna do groupBy pode ficar de fora do molde (R1) | `TestParseRecusaMoldeInvalido` | `{olt}` foi aceito num groupBy de duas colunas — dois grupos distintos renderiam o mesmo texto |
| Dois marcadores podem ficar colados (R2) | `TestParseRecusaMoldeInvalido` e `TestRenderizacaoEhInjetiva` | `{olt}{slot}` compilou, e a injetividade caiu junto |
| Literal depois de int64 pode começar com dígito ou `-` (R3) | `TestParseRecusaMoldeInvalido` (2 casos) | `{slot}0{olt}` e `{slot}-{olt}` compilaram: o decimal deixa de ter fim reconhecível |
| Escapar todos os caracteres do literal seguinte, não só o primeiro | `TestMoldeDeclaradoEhGolden` | `x-x-y` virou `\x-x-y` |
| Escapar também os caracteres do literal de prefixo | `TestMoldeDeclaradoEhGolden` (2 casos) | `OLT-BR_SPOAM_OLT.08` virou `OLT-BR_SP\OAM_\O\L\T.08` — o oposto do que a issue pede |
| Molde entra em `sameDefinition` / `CounterInfo` | `TestMudarOMoldeNaoReconstroiEReRenderizaOTexto` | trocar o molde passou a reconstruir 1 contador, varrendo as linhas vivas sem motivo |
| Guarda de colisão movida para DEPOIS do `Intern` | `TestColisaoRecusaAntesDeAChaveIrAoDisco` | o `grupos.dict` cresceu de 18 para 35 bytes e o grupo ganhou id: a chave colidente ficou durável, e a próxima abertura não abriria mais |
| Guarda de colisão removida da abertura | `TestAberturaRecusaDoisGruposComOMesmoTexto` | dois grupos com o mesmo texto abriram, e o segundo sobrescreveu o primeiro no mapa de busca |
| Atalho `ByKey` removido (render por linha) | `TestGrupoConhecidoNaoPassaPelaGuarda`, `TestContadorNaoAlocaNoCaminhoQuente`, e mais 10 testes | cada linha repetida passou a colidir com o próprio grupo |

## O que a bancada revelou, e ficou

**O golden não enxergava a regra que ele existia para fixar.** Os sete casos de
`TestTextoDaChaveEhGolden` passavam com e sem o escape do separador no ÚLTIMO
componente — nenhum deles tinha string com `|` na última posição. O caso
`["a","b|c"]` entrou ANTES da implementação, com o valor de hoje
(`a|b\|c`), e é ele que registra a mudança para `a|b|c`.

**O teste de injetividade também não enxergava.** Na primeira forma ele listava
só moldes válidos, e por isso passou verde com a R5 removida: o molde perigoso
nem chegava a ser exercitado. A lista passou a incluir os moldes que o `Parse`
recusa, com a invariante escrita como "todo molde ACEITO é injetivo" — e nessa
forma ele achou a colisão do `\` sozinho.

## Mudança de contrato registrada

O texto do `groupKey` sob o molde default mudou num caso: o ÚLTIMO componente
string deixou de escapar `|`. A regra nova escapa o primeiro caractere do
literal que SEGUE o marcador, e o último componente não tem literal depois —
ali não há fronteira a proteger. `["PRCTA","a|b"]` saía `PRCTA|a\|b` e passa
a sair `PRCTA|a|b`.

A decisão é deliberada: contadores são todos posteriores à `v0.7.0`, nenhuma
release expõe `groupKey`, nenhum cliente (JDBC ou Python) o consome e o piloto
não usa `counters:`.
