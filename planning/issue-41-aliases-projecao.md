# Issue #41 — aliases por consulta

Plano aprovado em 14/09/2026 para a
[issue #41](https://github.com/nishisan-dev/nishidb/issues/41).

## Contrato aprovado

- Aceitar `campo AS alias` e `campo alias` para campos simples, caminhos
  aninhados e tabelas virtuais. O alias muda o nome de saída, preservando
  valor, tipo, nulabilidade e ordem da projeção.
- Numerar **qualquer** repetição de nome com `_1`, `_2` etc., preservando
  todas as colunas. Essa decisão substitui tanto a recusa de aliases
  repetidos da issue quanto a recusa anterior de `SELECT a, a`.
- Aliases declarados na consulta, com ou sem `AS`, têm prioridade sobre
  nomes sem alias; entre aliases
  explícitos iguais, vence o primeiro. Reservar todos os nomes pedidos
  antes de escolher o menor índice livre para cada repetição.
- `ORDER BY` resolve nomes finais, inclusive sufixos, antes do schema.
  Caminhos qualificados continuam sendo referências a campos aninhados.
- `WHERE` e referências de origem da projeção resolvem somente no schema,
  incluindo aliases YAML. Não há encadeamento de aliases na projeção.
- Aliases distinguem maiúsculas e minúsculas. Palavras reservadas exigem
  aspas duplas; a regra inclui o vocabulário contextual do SQL v1, sem
  mudar nomes de campos já aceitos. Recusar alias vazio e em `*`/`pai.*`.
- O nome final `nishidb` continua reservado no stream HTTP. Um campo com
  esse nome pode ser consultado sob outro alias.

| Projeção | Saída |
|---|---|
| `a AS equipamento, b` | `equipamento, b` |
| `a, a` | `a, a_1` |
| `b, a AS b` | `b_1, b` |
| `a AS x, b AS x` | `x, x_1` |
| `a AS x, b AS x, c AS x_1` | `x, x_2, x_1` |

## Implementação

1. Acrescentar alias e posição à referência de coluna na AST. Reconhecer
   `AS` no parser, sem torná-lo keyword global. Preservar a recusa de
   `SELECT DISTINCT campo` antes de consumir um alias implícito.
2. Resolver campos e expandir curingas no binder, finalizar os nomes e
   só então resolver `ORDER BY`. Reutilizar slots de campos repetidos.
3. Usar `BoundColumn.Name` como fonte do nome final para executor, encoder
   e metadados. Resolver tudo uma vez por consulta, sem custo por linha.
4. Validar os clients existentes contra o servidor real, sem mudar sua
   implementação nem o formato do protocolo.
5. Atualizar a documentação normativa e registrar as provas de aceite.

## Verificação

Cobrir as duas grafias, aspas e escapes, recusas com posição, aliases YAML,
campos aninhados, `_meta`, `_counters`, colisões antes/depois do limiar do
mapa e reutilização da AST. Usar dados com ordens diferentes para provar a
prioridade dos aliases no `ORDER BY` e a independência do `WHERE`.

Verificar metadados e valores em resultados com linhas e vazios, acesso por
nome no JDBC/Python, zero alocação por linha e preservação do orçamento das
consultas existentes. Rodar suíte Go normal e com race, unitários e
integrações dos clients, lint Go com a baseline e lint Python. Aplicar
mutações controladas, observar vermelho, reverter e observar verde.

Sem migração de dados, mudança de storage, configuração YAML ou versão
do protocolo.
