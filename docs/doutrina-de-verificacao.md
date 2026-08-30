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
  planning e comentários Go resolve em uma seção ou item de spec.
- `TestAmostrasDeMetricaTemFamiliaENome` — todo literal `metrics.Amostra{...}`
  em código de produção declara `Familia` e `Nome` por constante, nunca string
  literal.

Eles não julgam se a citação aponta para o lugar certo — isso é revisão. Eles
fecham a classe barata: citar o que não existe.

## O que nenhum gate alcança

- Se o teste prova o critério (não só existe).
- Se o texto normativo diz a verdade sobre o código.
- Se a mutação foi revertida por inteiro.

Esses três continuam sendo trabalho de revisão humana. A doutrina existe para
que a revisão saiba o que procurar.
