# Doutrina de verificação

Como este repositório prova que uma invariante está de pé. Nasceu do ciclo do
índice por segmento, onde **vinte vezes** um teste passou com o defeito
presente — o padrão dominante, encontrado por mutação, nunca por leitura.

## O princípio

**Teste que passa com o defeito presente não prova nada.**

Um teste só é evidência quando a mutação do defeito o deixa vermelho. Passar
verde com o defeito no lugar é o mesmo que não existir — pior, porque dá a
impressão de cobertura.

## As quatro classes de defeito que o ciclo pagou caro

1. **Teste que passa com o defeito presente** (20×). Golden autocumprido
   (gerado pelo escritor que ele deveria conferir); teto de sondagem nunca
   exercitado; teste concorrente vacuamente concorrente; bancada de métricas
   onde 5 dos 10 números eram zero, permitindo trocar dois rótulos sem ficar
   vermelho; teste de convergência cego porque a fixture não tinha variedade
   **e folga**.

2. **Texto normativo que mente** (5+). Godoc, comentário ou doc afirmando uma
   barreira ou comportamento que o código não estabelece. O compilador não lê
   prosa; só um revisor lendo a fonte pega. **Abra a fonte antes de afirmar.**

3. **Resíduo de mutação**. Agentes de revisão deixaram mutação na árvore:
   constante hardcoded, lock acrescentado. Confira `git status` antes de
   commitar, e desconfie de constante ou lock que não estava no diff.

4. **Citação que não resolve**. `§NN` apontando para seção que não existe, e
   amostra de métrica sem `Familia` — o `# HELP`/`# TYPE` sai com nome vazio e
   o Prometheus recusa o scrape inteiro, não só a linha.

## O método

Para toda invariante nova:

1. Escreva o teste.
2. **Aplique a mutação** — introduza o defeito que o teste deveria pegar.
3. **Veja vermelho** — o teste tem de falhar.
4. **Reverta** a mutação.
5. **Veja verde** — o teste volta a passar.
6. **Relate os dois** (vermelho e verde) na revisão.

Se o passo 3 não ficar vermelho, o teste não prova a invariante: volte ao
passo 1. Um teste de convergência que passa com o defeito presente não prova
nada.

## O que é gateado

Dois gates fecham as classes baratas, lendo o documento/código como dado e
comparando duas extrações — a mesma doutrina dos outros gates de
`internal/gates`:

- `TestCitacoesDeSecaoResolvem` — toda citação `§NN`/`§NN.M` em docs, README,
  CLAUDE.md, planning e comentários Go resolve em uma seção ou item das specs
  declaradas. O `.go` é lido por AST (só comentários) e o `.md` pula os blocos
  cercados: `§` dentro de string literal ou de exemplo não é citação.
- `TestAmostrasDeMetricaTemFamiliaENome` — todo literal `metrics.Amostra{...}`
  em código de produção declara `Familia` e `Nome` por constante, nunca string
  literal, e o `Nome` pertence à `Familia` (a mesma constante, ou ela mais um
  sufixo de histograma). "Todo" inclui a forma elidida `[]metrics.Amostra{{…}}`,
  que é quase 40% dos literais.

Eles não julgam se a citação aponta para o lugar certo — isso é revisão. Eles
fecham a classe barata: citar o que não existe.

## O gate também é código

Os dois gates acima nasceram, eles próprios, na classe 1. O de família só
enxergava o literal com o tipo escrito e era cego à forma elidida — a dominante
no repositório —, e entre os literais invisíveis estavam justamente os quatro do
coletor Kafka que o commit da doutrina tinha acabado de consertar: eles podiam
regredir com o gate verde. O de citação lia `.go` como bytes crus, procurando
`§` em prosa, contra a doutrina de ler código como DADO que ele mesmo pregava.

Nenhum dos dois foi pego por leitura. Os dois foram pegos aplicando a mutação
**na forma dominante do repositório**, e não só na forma que o autor do teste
tinha na cabeça. É a lição operacional da classe 1 aplicada ao ferramental:
mutar o caso que você escreveu prova o caso que você escreveu.

## O que nenhum gate alcança

- Se o teste prova o critério (não só existe).
- Se o texto normativo diz a verdade sobre o código.
- Se a mutação foi revertida por inteiro.

Esses três continuam sendo trabalho de revisão humana. A doutrina existe para
que a revisão saiba o que procurar.
