// Package protocol é o contrato do protocolo remoto do §67-69: o corpo da
// requisição de consulta, a moldura da resposta NDJSON, os corpos de
// /v1/metadata e /v1/stats e a tabela de tipos do §73.
//
// Mora em pacote próprio, fora de internal/server, porque tem DOIS falantes: o
// servidor, que produz, e o `nishidb query --server`, que consome — e amanhã o
// driver JDBC da Fase 7, que é cliente deste mesmo formato. Com os tipos
// morando dentro do servidor, o cliente redeclararia o formato e as duas
// definições divergiriam no primeiro campo acrescentado, sem nada em camada
// nenhuma reclamar.
//
// O pacote depende apenas da stdlib, de internal/types e de internal/typeparse
// — este último de propósito: um parâmetro que entra pelo protocolo e um
// literal escrito na query devem ser interpretados byte a byte da mesma forma,
// e a única maneira de garantir isso é os dois passarem pelos MESMOS parsers.
package protocol

import (
	"encoding/json"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ContentTypeNDJSON é o Content-Type da resposta de /v1/query (§69).
const ContentTypeNDJSON = "application/x-ndjson"

// ContentTypeJSON é o Content-Type da requisição e das respostas que não são
// stream (/v1/metadata, /v1/stats e os erros que saem antes da primeira linha).
const ContentTypeJSON = "application/json"

// MaxRequestBody é o teto do corpo de /v1/query.
//
// Existe porque o §128 proíbe fila sem limite, e um POST sem teto é
// exatamente isso: um cliente que envia 2 GiB de SQL faz o servidor alocar
// 2 GiB antes de descobrir que a query é inválida. 1 MiB é folga de sobra para
// uma query com parâmetros — o SQL da v1 não tem JOIN nem subquery.
const MaxRequestBody = 1 << 20

// Request é o corpo de POST /v1/query (§68).
//
// MaxRows e TimeoutMs são PONTEIROS para separar "ausente" de "zero": ausente
// herda o teto do dataset, e zero é um pedido explícito de nada, que precisa
// ser recusado com a mesma clareza de um negativo.
type Request struct {
	Dataset    string  `json:"dataset"`
	SQL        string  `json:"sql"`
	Parameters []Param `json:"parameters"`
	MaxRows    *int    `json:"maxRows"`
	TimeoutMs  *int    `json:"timeoutMs"`
}

// Param é um parâmetro posicional do §68.
//
// Value é RawMessage, e não `any`, por duas razões. A primeira é precisão: o
// `any` do encoding/json decodifica todo número como float64, e um int64 acima
// de 2^53 chegaria ao motor com o valor ERRADO e sem erro nenhum. A segunda é
// que o tipo DECLARADO manda — guardar os bytes crus é o que permite recusar
// {"type":"int64","value":3.5} em vez de arredondar por conta própria.
type Param struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// Os quatro tipos de wire aceitos em Param.Type.
//
// A lista é curta porque é o contrato de binder.coerceParam, não uma escolha
// deste pacote: o binder recusa parâmetro já em representação binária para
// manter UM caminho de coerção e UM conjunto de testes. Endereços, prefixos e
// timestamps viajam como string e são convertidos pela mesma tabela dos
// literais SQL — que é o que o §73 já pressupõe ao mapear ipv4 para VARCHAR.
const (
	WireTypeString  = "string"
	WireTypeInt64   = "int64"
	WireTypeDouble  = "double"
	WireTypeBoolean = "boolean"
)

// TypeUnknown é o nome de tipo de uma coluna auto que o dataset ainda não
// observou. Não é erro: é a ausência de evento que fixasse o tipo.
const TypeUnknown = "unknown"

// FrameKey é a chave reservada que distingue uma linha de MOLDURA de uma
// linha de dados.
//
// A escolha de um discriminador na raiz do objeto é o que permite às linhas de
// dados serem exatamente o que o §69 mostra — sem envelope, sem custo de
// serialização a mais. O preço é que uma coluna de saída com este nome tornaria
// a row indistinguível da moldura; por isso o servidor RECUSA a query, antes do
// primeiro byte, quando a projeção produz uma coluna chamada assim.
const FrameKey = "nishidb"

// Os três valores de FrameKey.
const (
	FrameSchema = "schema"
	FrameEnd    = "end"
	FrameError  = "error"
)

// SchemaLine é a PRIMEIRA linha da resposta de /v1/query.
//
// Ela existe porque o NDJSON não carrega tipos e o ResultSetMetaData do §71
// precisa deles. Derivá-los do dado não funciona: uma coluna que é NULL em
// todas as linhas não revela tipo nenhum, e mesmo uma que revela só o faria
// depois da primeira row — tarde demais para quem já está consumindo o stream.
type SchemaLine struct {
	Nishidb string   `json:"nishidb"`
	Dataset string   `json:"dataset"`
	Columns []Column `json:"columns"`
}

// Column descreve uma coluna da projeção.
//
// Os três números são o que o ResultSetMetaData do §71 precisa além do tipo, e
// saem do descritor único (DescriptorOf): o driver não os recalcula, e por
// isso um tipo novo no servidor não exige jar novo no cliente.
//
// Nullable é o único que NÃO sai do tipo: vem do executor, que é quem tem o
// campo compilado na mão. false só para a chave e para a sequence — as duas
// colunas que o materializer garante não-nulas recusando o evento que as traga
// ausentes ou nulas.
//
// SEM omitempty, de propósito. precision:0 é o caso que carrega informação —
// "sem teto declarado", que é a resposta para string, json e bytes — e com
// omitempty ele sumiria justamente onde é a decisão. Ausência e zero são
// indistinguíveis para quem decodifica, mas não para quem lê o contrato.
type Column struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	JDBC        string `json:"jdbc"`
	Nullable    bool   `json:"nullable"`
	Precision   int    `json:"precision"`
	Scale       int    `json:"scale"`
	DisplaySize int    `json:"displaySize"`
}

// EndLine é a ÚLTIMA linha de uma resposta bem-sucedida.
//
// Truncated distingue "acabou" de "parou no teto de query.maxRows". Sem essa
// distinção o cliente apresentaria um resultado incompleto como completo, que é
// uma resposta errada — e é o motivo de o erro de teto sair depois das rows em
// vez de virar status HTTP.
type EndLine struct {
	Nishidb   string `json:"nishidb"`
	Rows      int64  `json:"rows"`
	Truncated bool   `json:"truncated"`
}

// ErrorLine é a última linha quando a consulta falha DEPOIS de a primeira linha
// já ter saído.
//
// É a resposta ao problema estrutural do streaming: o status HTTP foi enviado
// junto com o primeiro byte, então uma query que quebra na linha 10.000 não tem
// mais status para usar. Rows diz quantas linhas válidas o cliente recebeu
// antes da falha.
type ErrorLine struct {
	Nishidb string `json:"nishidb"`
	Code    string `json:"code"`
	Rows    int64  `json:"rows"`
	Message string `json:"message"`
}

// ErrorBody é a resposta de erro que sai ANTES da primeira linha, com o status
// HTTP correspondente.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail é o erro em si. Line e Column são 1-based e só aparecem para
// erro de sintaxe ou de bind, que são os que têm posição no texto da query — é
// a mesma informação que o CLI já imprime com o cursor sob o trecho ofensor.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
}

// Códigos de erro do protocolo. São estáveis: o cliente casa por código, nunca
// pelo texto da mensagem, que é prosa para humanos e pode ser reescrita.
const (
	CodeBadRequest          = "BAD_REQUEST"
	CodeBadParameter        = "BAD_PARAMETER"
	CodeMaxRowsAboveCeiling = "MAX_ROWS_ABOVE_LIMIT"
	CodeReservedColumn      = "RESERVED_COLUMN"
	CodeSyntax              = "SYNTAX"
	CodeBind                = "BIND"
	// CodeUnsupportedClause separa "isto não existe nesta versão" de
	// "você digitou errado", que o CodeSyntax misturava.
	//
	// A distinção existe para a ferramenta de SQL: `ORDER BY` saía como
	// "conteúdo inesperado após o fim da query", e quem escreveu a cláusula
	// num DBeaver ia procurar o erro de digitação em vez de descobrir que a
	// v1 não ordena. A mensagem nomeia a cláusula, e o driver JDBC traduz
	// este código para SQLState 0A000 (feature not supported), não para os
	// da classe 42 (sintaxe).
	CodeUnsupportedClause = "UNSUPPORTED_CLAUSE"
	CodeUnknownDataset    = "UNKNOWN_DATASET"
	CodeMethod            = "METHOD_NOT_ALLOWED"
	CodeMediaType         = "UNSUPPORTED_MEDIA_TYPE"
	CodeBodyTooLarge      = "REQUEST_TOO_LARGE"
	CodeTooManyQueries    = "TOO_MANY_QUERIES"
	// CodeTooManyLongQueries é a recusa do SUB-POOL de consulta longa
	// (runtime.query.maxLongRunning), e tem código próprio em vez de reusar
	// CodeTooManyQueries porque a AÇÃO do cliente é outra.
	//
	// TOO_MANY_QUERIES diz "o servidor está cheio, tente de novo em instantes"
	// — e é verdade, porque as consultas que ocupam o pool geral duram
	// milissegundos. TOO_MANY_LONG_QUERIES diz "as vagas de consulta LONGA
	// estão ocupadas", e quem as ocupa pode estar lá por minutos: repetir em
	// um segundo é quase certo que falha de novo, e a alternativa útil é
	// baixar o `timeoutMs` para caber no prazo padrão do dataset e entrar pelo
	// pool geral. Com um código só, o cliente não teria como distinguir os dois
	// conselhos, e o driver publicaria o mesmo SQLState para os dois.
	CodeTooManyLongQueries = "TOO_MANY_LONG_QUERIES"
	CodeTimeout            = "TIMEOUT"
	CodeMaxRows            = "MAX_ROWS_EXCEEDED"
	// CodeRowTooLarge é a linha que não coube no orçamento de buffer da consulta
	// (runtime.query.maxBufferPerQuery, §80).
	//
	// Tem código próprio, e não CodeInternal, porque não é defeito do servidor: é
	// o teto de memória sendo aplicado, e existe ação do cliente — projetar
	// menos colunas, ou o operador subir a chave. Enquanto o StreamWriter
	// crescia em silêncio (débito 28) não havia erro nenhum para nomear, e o
	// sintoma aparecia como RSS num processo que ninguém ligava àquela consulta.
	//
	// Sai SEMPRE na linha de fecho, depois das rows: quando a linha grande
	// aparece, o status HTTP já foi enviado junto com o schema.
	CodeRowTooLarge = "ROW_TOO_LARGE"
	// CodeSnapshotExpired é a consulta cujo universo o servidor não consegue
	// mais reconstruir: o journal de versões do isolamento estourou o teto de
	// memória (runtime.query.snapshotMaxBytes) e as versões que ela veria
	// foram descartadas.
	//
	// Tem código próprio, e não CodeInternal, pela mesma razão do ROW_TOO_LARGE:
	// não é defeito do servidor, é um teto sendo aplicado, e existe ação do
	// cliente — repetir a consulta (a nova abre um snapshot atual e passa a
	// ver o estado corrente), estreitá-la para terminar antes, ou pedir ao
	// operador para subir a chave.
	//
	// Não degradar para a versão corrente é o ponto: as duas respostas são
	// indistinguíveis no wire e só uma é verdadeira. Quem pediu isolamento
	// precisa saber quando não o recebeu.
	CodeSnapshotExpired = "SNAPSHOT_TOO_OLD"
	CodeDatasetCorrupt  = "DATASET_CORRUPT"
	CodeInternal        = "INTERNAL"

	// CodeUnauthenticated e CodeForbidden são do middleware de autenticação e
	// papéis do §74-77 (Fase 11, Task B3) — os dois primeiros códigos deste
	// pacote que não vêm da consulta em si. CodeUnauthenticated sai com 401 e
	// o header WWW-Authenticate (RFC 7617); CodeForbidden sai com 403. Nenhum
	// dos dois nomeia CREDENTIAL nem ROLE na mensagem de forma a distinguir
	// "usuário não existe" de "senha errada" — essa distinção é dado que o
	// protocolo deliberadamente NÃO expõe (ver internal/server/auth.go).
	CodeUnauthenticated = "UNAUTHENTICATED"
	CodeForbidden       = "FORBIDDEN"

	// CodeWriterFailed e CodeCompactionBusy são das rotas mutantes do §96 (Fase
	// 11, Task B5, decisão D3 do roadmap): POST /v1/checkpoint e POST
	// /v1/compact. CodeWriterFailed sai com 500 — o writer está em modo falho
	// sticky (§14.14), e só reabrir o dataset o tira de lá; o CLI mapeia este
	// código para o MESMO ExitWriter(4) que o caminho local já usa para o
	// mesmo sentinela (internal/types.ErrWriterFailed), coerência com a
	// tabela de exit codes. CodeCompactionBusy sai com 409 — já existe uma
	// passada de compaction em voo, do laço automático ou de outra chamada
	// manual concorrente (storage.ErrCompactionBusy); não é retentável na
	// hora, mas é retentável em instantes, o que o distingue de um erro de
	// uso — mesmo assim ele NÃO ganha um exit code próprio (ver o
	// comentário de codigoDoProtocolo no CLI).
	CodeWriterFailed   = "WRITER_FAILED"
	CodeCompactionBusy = "COMPACTION_BUSY"
)

// TypeName é o nome do tipo como o operador o escreve no YAML (§17).
//
// TypeNull vira "unknown" em vez de "null": no wire, "null" descreveria um
// VALOR ausente, e aqui o que é desconhecido é o TIPO da coluna — um campo auto
// que nenhum evento fixou ainda.
func TypeName(t types.FieldType) string {
	if JDBCName(t) == JDBCOther {
		return TypeUnknown
	}
	return t.String()
}
