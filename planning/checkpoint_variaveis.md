# Checkpoint Técnico - Refatoração (Português -> Inglês)

## Estado Atual da Arquitetura
- A base de código está com os testes e build íntegros.
- Foram realizadas alterações de nomenclatura em nível de **Funções**, traduzindo os identificadores de português para inglês em toda a pasta `internal/` e arquivos associados (`benchmark/`, `cli`, `server`, etc).
- Os commits foram mantidos de forma atômica e agrupados por pacote, garantindo facilidade para revertabilidade.
- O plano aprovado sobre as funções está documentado em `planning/rename_pt_functions_plan.md`.

## Decisões Já Tomadas
- Substituições são realizadas através de ferramentas semânticas (como `gofmt -r`) para não modificar comentários de código ou textos puros sem intenção.
- Textos puros, labels expostas ao usuário e comentários em português devem permanecer inalterados, focando apenas no código executável e identificadores.

## Pendências Abertas
- Renomeação de **variáveis** escritas em português (ex: `caminho`, `campo`, `acorda`, `ajustado`, `alvo`, etc) para o inglês.

## Próximos Passos Sugeridos
1. Extrair os identificadores das variáveis (via AST ou RegExp) de forma isolada em scripts locais.
2. Formular um documento de planejamento (`/plan`) focado em agrupamentos de variáveis (ex: pacote por pacote).
3. Aplicar o plano validando as modificações pacote por pacote para evitar bugs lógicos complexos (shadowing, conflitos de dependência).
