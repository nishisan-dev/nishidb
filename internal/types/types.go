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
	"strconv"
	"time"
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

	// ErrTooLarge indica que alguma parte do evento excedeu um teto de
	// FORMATO do storage: key acima de MaxKeySize, record acima de
	// MaxRecordSize, valor acima de MaxIndexedValueBytes num campo indexado.
	//
	// Existe como sentinela próprio porque esses casos SÃO culpa do evento e
	// até o M1 caíam no balde de STORAGE_ERROR, que é o balde do "bug nosso
	// ou dataset avariado". A consequência era concreta: um produtor emitindo
	// um atributo de 70 KiB num campo com bitmap fazia o `ingest` sair com
	// exit 7 (perda de dado) a cada evento, e num daemon, onde não há exit
	// code, o operador teria de escolher entre avançar o offset (perda
	// definitiva e invisível) e não avançar (livelock por evento envenenado).
	// Com o sentinela, esses eventos são governados pela política do §101
	// como qualquer outro evento ruim.
	ErrTooLarge = errors.New("storage: acima do teto de formato")
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
	// TypeDateTime é data+hora SEM fuso na origem, resolvida para instante
	// pelo fuso do host (time.Local) na ingestão. Fisicamente idêntico a
	// TypeTimestamp; a tag é distinta para que o nome declarado no YAML
	// chegue ao /v1/metadata e ao JDBC sem um sistema de tipos sombra.
	TypeDateTime FieldType = 0x0C // 8 bytes LE, epoch millis UTC
	// TypeTime é hora do dia, e NÃO é um instante: não tem data, não tem
	// fuso e não compara com timestamp. Tag própria porque reusar a de
	// timestamp faria a query casar sem erro e a saída emitir uma data de
	// 1970 plausível para um valor que nunca foi uma data.
	TypeTime FieldType = 0x0D // 8 bytes LE, millis desde a meia-noite
)

// MillisPerDay é o teto exclusivo de TypeTime.
const MillisPerDay int64 = 86_400_000

// fieldTypeNames mapeia a tag para o nome do tipo no vocabulário do YAML.
var fieldTypeNames = [...]string{
	TypeNull:       "null",
	TypeInt64:      "int64",
	TypeDouble:     "double",
	TypeBool:       "boolean",
	TypeTimestamp:  "timestamp",
	TypeIPv4:       "ipv4",
	TypeIPv6:       "ipv6",
	TypeIPv4Prefix: "ipv4_prefix",
	TypeIPv6Prefix: "ipv6_prefix",
	TypeString:     "string",
	TypeBytes:      "bytes",
	TypeJSON:       "json",
	TypeDateTime:   "datetime",
	TypeTime:       "time",
}

// String devolve o nome do tipo como o operador o escreveu no YAML. Sem
// ele, toda mensagem que interpola um FieldType imprime a tag numérica do
// wire — "conversão para 1 falhou" em vez de "conversão para int64 falhou".
func (t FieldType) String() string {
	if int(t) < len(fieldTypeNames) && fieldTypeNames[t] != "" {
		return fieldTypeNames[t]
	}
	return "FieldType(0x" + strconv.FormatUint(uint64(t), 16) + ")"
}

// CarriesI64 diz se o valor do tipo mora em Value.I64 (largura fixa) em vez
// de Value.Bytes. É a fonte ÚNICA dessa distinção.
//
// Existe porque quem a enumerava à mão a escrevia como um switch com
// `default:` caindo no ramo dos bytes — e uma tag nova de largura fixa
// esquecida ali não erra alto: os dois lados têm Bytes nil, bytes.Equal
// devolve true para valores DIFERENTES, e o diff conclui que o campo não
// mudou. Sem erro, sem log, sem métrica. Acrescentar tag agora é mexer aqui.
func (t FieldType) CarriesI64() bool {
	switch t {
	case TypeInt64, TypeDouble, TypeBool, TypeTimestamp, TypeDateTime, TypeTime:
		return true
	}
	return false
}

// Presence é o que o materializer precisa saber sobre uma key, e são TRÊS
// estados, não dois.
//
// Enquanto "ausente" e "expirada" chegavam colapsados num único ok=false, o
// stale check do §25 não rodava para a row expirada — o materializer a tratava
// como key nova e a recriava a partir de um evento ANTIGO, que é exatamente a
// ressurreição que o §58 diz que o tombstone deve impedir (débitos 24 e 25).
// A informação sempre existiu no lookup; era descartada uma linha depois.
//
// Mora em types porque dois provedores precisam concordar sobre ela: o Store
// real e o dublê de storage.
type Presence uint8

const (
	// PresenceAbsent marca a key que nunca existiu neste dataset.
	PresenceAbsent Presence = iota
	// PresenceLive marca a key cujo estado corrente é legível.
	PresenceLive
	// PresenceExpired marca a entidade que existiu e não está mais visível —
	// lápide publicada ou prazo vencido. RowID e Sequence do record devolvido
	// são VÁLIDOS, e é isso que faz o stale check funcionar; só o Payload
	// deixou de descrever um estado vivo.
	PresenceExpired
)

// Exists reporta se a key já foi vista, viva ou não.
func (p Presence) Exists() bool { return p != PresenceAbsent }

func (p Presence) String() string {
	switch p {
	case PresenceAbsent:
		return "ausente"
	case PresenceLive:
		return "viva"
	case PresenceExpired:
		return "expirada"
	}
	return "Presenca(" + strconv.FormatUint(uint64(p), 10) + ")"
}

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

// Snapshot é o universo que UMA consulta enxerga, do primeiro ao último byte
// do resultado.
//
// É tipo de VALOR, e viaja em parâmetro — nunca em campo do Store. A
// distinção não é estilística: dois leitores concorrentes têm snapshots
// diferentes, e um campo no Store os fundiria num só, de modo que a consulta
// que abriu depois arrastaria a que abriu antes para o seu próprio universo.
//
// Os três limites respondem a três formas diferentes de o dataset mudar
// debaixo de uma varredura em curso, e nenhum deles substitui os outros:
//
//   - Epoca congela a VERSÃO de cada linha. Sem ela a consulta vê o mesmo
//     conjunto de linhas do começo ao fim, mas mistura estados: as que a
//     varredura alcançou antes de uma atualização saem velhas e as de depois
//     saem novas, e o resultado não corresponde a instante nenhum do banco.
//   - Instante congela o relógio da expiração. Sem ele, prazoVencido consulta
//     s.agora() a cada row testada e uma linha viva quando a consulta abriu
//     desaparece do MEIO do resultado.
//   - LimiteRowID torna o teto EXPLÍCITO e comum aos quatro planos. Ele não
//     conserta um defeito ativo, e dizer que conserta seria repetir o que o
//     débito 94 afirma sem que o código sustente: as fontes do índice por
//     segmento já fixam o seu teto na CONSTRUÇÃO — o WIP do corrente
//     materializa a fatia de rowIds no Lookup e a fonte de todas as locations
//     nasce com `fim` carregado —, e a construção acontece no Open. O que o
//     campo entrega é que o teto passe a vir de UMA leitura, no início da
//     consulta, em vez de uma por fonte: fonte nova não precisa redescobrir a
//     regra, e o filtro do executor pode aplicá-la uniformemente.
//   - BaseRowID congela o piso. Sem ele, a purga do histórico avança a
//     marca-d'água durante a varredura e abre buraco por baixo.
type Snapshot struct {
	// Epoch é a versão do dataset em T0: a consulta enxerga tudo que foi
	// publicado ATÉ ela, e nada do que veio depois. É o que dá isolamento —
	// uma linha atualizada com a consulta em voo é entregue na versão desta
	// época, não na corrente.
	Epoch uint64
	// Instant é o relógio da expiração para esta consulta.
	Instant time.Time
	// LimitRowID é EXCLUSIVO: rowIds a partir dele nasceram depois da
	// consulta e não pertencem ao universo dela.
	LimitRowID RowID
	// BaseRowID é INCLUSIVO: rowIds abaixo dele já tinham sido purgados
	// quando a consulta abriu.
	BaseRowID RowID
}
