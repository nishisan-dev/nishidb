// Package types define o vocabulário compartilhado entre os subsistemas de
// storage, materializer e query do NishiDB: os tipos de dados, os sentinelas
// de erro que os consumidores classificam e as poucas interfaces que
// PROVEDORES DISTINTOS precisam satisfazer identicamente.
//
// O pacote depende apenas da stdlib e assim deve permanecer — é o que
// permite que o dublê de teste de um subsistema não importe a implementação
// que dubla.
//
// O critério de admissão é estreito e vale para as duas categorias que não
// são dados puros:
//
//   - interfaces: só entram as que têm mais de um provedor REAL (o Store e o
//     seu dublê). Interface com provedor único continua no consumidor.
//   - constantes: só entram limites de FORMATO que provedor e dublê precisam
//     aplicar identicamente. Constante de implementação (tamanho de buffer,
//     política interna) fica no pacote que a usa.
//
// O teste é sempre o mesmo: se um dublê precisasse importar a implementação
// que dubla para respeitar a regra, a regra mora aqui.
package types

import (
	"errors"
	"math"
)

// MaxIndexedValueBytes é o teto de um valor que pode entrar num índice
// bitmap: o dicionário de valores codifica o comprimento em u16.
//
// É limite de FORMATO, e por isso mora aqui e não no pacote do índice: o
// dublê de teste do storage precisa recusar o mesmo valor que o Store real
// recusa, senão um teste passa no dublê e explode na integração — que é
// exatamente a divergência que um dublê existe para expor. As alternativas
// eram o dublê importar a implementação do índice, recriando o acoplamento
// que os sentinelas eliminaram, ou repetir o número nos dois lados, onde
// ele envelheceria calado.
const MaxIndexedValueBytes = math.MaxUint16

// Sentinelas que os consumidores do storage precisam classificar.
//
// Moram aqui, e não no pacote que os produz, porque quem os interpreta é o
// materializer — e o dublê de teste dele precisa DEVOLVÊ-LOS. Com os
// sentinelas dentro de internal/storage, tanto o materializer quanto o seu
// fake passariam a importar a implementação que dublam, que é justamente o
// acoplamento evitado ao declarar as interfaces no consumidor.
var (
	// ErrInvalidArgument indica violação dos argumentos de uma operação
	// (key vazia, payload malformado, campo desconhecido). O evento é ruim;
	// o writer segue utilizável.
	ErrInvalidArgument = errors.New("storage: argumento inválido")

	// ErrWriterFailed indica writer em modo falho (sticky): o dataset
	// precisa ser reaberto para voltar a aceitar escrita. É o que separa
	// "pule este evento" de "pare a ingestão" — sem essa distinção, a
	// política de log-and-skip por evento vira um skip infinito e a
	// ingestão descarta tudo em silêncio.
	ErrWriterFailed = errors.New("storage: writer em modo falho (reabra o dataset)")
)

// FieldType identifica o tipo lógico de um campo (spec §17-18) e é também a
// tag de tipo no wire format do payload (spec §33).
type FieldType uint8

const (
	// TypeNull é reservado no wire format: nunca é codificado em payload
	// materializado (NULL = campo ausente). Existe apenas para representar
	// null em Value durante o processamento de patches.
	TypeNull       FieldType = 0x00
	TypeInt64      FieldType = 0x01 // 8 bytes LE
	TypeDouble     FieldType = 0x02 // 8 bytes IEEE-754 LE
	TypeBool       FieldType = 0x03 // 1 byte (0/1)
	TypeTimestamp  FieldType = 0x04 // 8 bytes LE, epoch millis
	TypeIPv4       FieldType = 0x05 // 4 bytes, network order
	TypeIPv6       FieldType = 0x06 // 16 bytes
	TypeIPv4Prefix FieldType = 0x07 // 4 bytes addr + 1 byte prefix len
	TypeIPv6Prefix FieldType = 0x08 // 16 bytes addr + 1 byte prefix len
	TypeString     FieldType = 0x09 // varlen, UTF-8
	TypeBytes      FieldType = 0x0A // varlen
	TypeJSON       FieldType = 0x0B // varlen, JSON UTF-8 cru
)

// FieldID identifica um campo no field dictionary do dataset (spec §32).
// O valor 0 é reservado; IDs válidos começam em 1.
type FieldID uint16

// RowID é o identificador interno estável de uma entidade (spec §11).
// Monotônico, nunca reutilizado.
type RowID uint64

// Flags de um Record. Bit layout é contrato entre storage e consumidores.
const (
	// FlagTombstone marca o record como tombstone (Fase 9; M1 grava sempre 0).
	FlagTombstone uint8 = 1 << 0
)

// RowIDIterator percorre rowIds em ordem CRESCENTE — a ordem é
// pré-condição das interseções e uniões do planner.
//
// Mora aqui, e não no consumidor, porque dois provedores precisam satisfazer
// a MESMA interface: o Store real e o fake do ramo query. Uma interface
// redeclarada em cada consumidor nunca é satisfeita por dois provedores com
// tipos de retorno diferentes, e é isso que permite as asserções de
// compilação dos dois lados.
type RowIDIterator interface {
	Next() bool
	RowID() RowID
	Err() error
	Close() error
}

// Value é uma união sem boxing para valores tipados no hot path.
// Nunca usar interface{} para valores.
//
//   - int64, timestamp, bool (0/1): I64
//   - double: math.Float64bits em I64
//   - string, bytes, json, ipv4 (4B), ipv6 (16B), prefixes (5B/17B): Bytes
type Value struct {
	Type  FieldType
	I64   int64
	Bytes []byte
}

// Record é a visão decodificada de um registro materializado.
//
// Key e Payload são zero-copy sobre o buffer fornecido pelo caller na
// leitura; válidos somente até a próxima chamada que reutilize o buffer.
// Payload contém os campos codificados em ordem crescente de FieldID,
// sem duplicatas e sem campos null (NULL = campo ausente).
type Record struct {
	RowID    RowID
	Sequence int64
	Flags    uint8
	Key      []byte
	Payload  []byte
}
