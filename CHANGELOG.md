# Changelog

Formato inspirado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).
Este é o primeiro registro do arquivo — entradas anteriores não foram
retroativamente reconstruídas; a partir daqui, mudança de contrato observável
entra aqui no mesmo commit que o código.

## [Não lançado]

### Alterado (BREAKING)

- `index: hash` e `index: bitmap` foram substituídos por `index: true`; a
  subida recusa as grafias antigas com exit 3. Com o índice por segmento
  único (ver `planning/spec-subsistema-1-indice-por-segmento.md`, decisão D5),
  as duas grafias construiriam exatamente a mesma coisa — aceitá-las como
  sinônimo deixaria o operador escrevendo uma distinção que não existe mais.
  A mensagem de erro ensina a grafia nova. Datasets existentes com `index:
  hash` ou `index: bitmap` precisam trocar para `index: true` antes de
  atualizar o binário.
