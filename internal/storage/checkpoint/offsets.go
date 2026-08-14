package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
)

// OffsetsFileName é o nome literal que o §50 e o §83 dão ao arquivo.
const OffsetsFileName = "kafka.offsets"

var (
	// ErrNoOffsets indica ausência do arquivo — dataset que nunca consumiu
	// de Kafka, ou dataset alimentado só por `nishidb ingest`.
	ErrNoOffsets = errors.New("checkpoint: offsets ausentes")

	// ErrCorruptOffsets indica magic, version, CRC ou tamanho inconsistente.
	//
	// NÃO é tratado como dataset corrompido pelo Store: os dados continuam
	// íntegros e as consultas têm de subir (§51). O que se perde é a POSIÇÃO,
	// e a decisão de o que fazer com isso é do operador (ver a regra do
	// RowCount na subida da ingestão).
	ErrCorruptOffsets = errors.New("checkpoint: offsets corrompidos")
)

var offsetsMagic = [4]byte{'N', 'D', 'B', 'K'}

const offsetsVersion = 1

// Origem identifica o FLUXO de onde os offsets vieram.
//
// Existe porque um offset é um número sem significado fora do seu tópico. Com
// topic, groupId e bootstrapServers mutáveis no YAML e sem conferência contra
// o que está persistido, apontar o dataset para outro tópico — ou para outro
// cluster com o mesmo NOME de tópico, que é o cenário normal de promoção
// teste→produção — faria o consumer dar seek num offset de outro fluxo:
// começaria no meio, pulando ou reprocessando uma janela arbitrária, sem erro
// em camada nenhuma.
type Origem struct {
	Topic     string
	GroupID   string
	ClusterID string
	Bootstrap string
}

// SourceOffsets é o conteúdo do checkpoint/kafka.offsets.
type SourceOffsets struct {
	Origem Origem
	// Offsets mapeia partição → PRÓXIMO offset a consumir (a mesma convenção
	// do offset commitado no Kafka: o que já foi processado é offset-1).
	Offsets map[int32]int64
}

// CanonizaBootstrap devolve a forma comparável de uma lista de brokers:
// ordenada e sem espaços. Sem canonizar, reordenar a lista no YAML pareceria
// troca de cluster e recusaria a subida sem motivo.
func CanonizaBootstrap(brokers []string) string {
	limpos := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if b = strings.TrimSpace(b); b != "" {
			limpos = append(limpos, b)
		}
	}
	slices.Sort(limpos)
	return strings.Join(slices.Compact(limpos), ",")
}

// Divergencia nomeia o PRIMEIRO campo em que as duas origens diferem, ou ""
// se são a mesma. O nome do campo é o que o operador precisa: "recusei subir"
// sem dizer o que mudou obriga a comparar YAML com binário na mão.
//
// ClusterID vazio de um dos lados NÃO conta como divergência: nem todo broker
// expõe o id do cluster, e recusar a subida por um dado que o próprio Kafka
// não deu seria transformar limitação de ambiente em falha de configuração.
func (o Origem) Divergencia(outra Origem) string {
	switch {
	case o.Topic != outra.Topic:
		return "topic"
	case o.GroupID != outra.GroupID:
		return "consumer.groupId"
	case o.Bootstrap != outra.Bootstrap:
		return "bootstrapServers"
	case o.ClusterID != "" && outra.ClusterID != "" && o.ClusterID != outra.ClusterID:
		return "cluster"
	}
	return ""
}

// Particoes devolve as partições em ordem crescente.
func (s SourceOffsets) Particoes() []int32 {
	out := slices.Collect(maps.Keys(s.Offsets))
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Clone copia o mapa, para que o estado do writer não vaze por referência.
func (s SourceOffsets) Clone() SourceOffsets {
	out := SourceOffsets{Origem: s.Origem, Offsets: make(map[int32]int64, len(s.Offsets))}
	maps.Copy(out.Offsets, s.Offsets)
	return out
}

// Layout do kafka.offsets (little-endian):
//
//	0   4  magic "NDBK"
//	4   1  version = 1
//	5   3  reservado
//	       topicLen u16     | topic
//	       groupLen u16     | groupId
//	       clusterLen u16   | cluster id ("" quando o broker não expõe)
//	       bootstrapLen u16 | bootstrap canônico (ordenado, vírgula)
//	    4  partCount u32
//	       por partição: partition i32 | offset i64
//	    4  crc32c sobre tudo acima
func (s SourceOffsets) encode() []byte {
	parts := s.Particoes()
	b := make([]byte, 0, 24+len(s.Origem.Topic)+len(s.Origem.GroupID)+
		len(s.Origem.ClusterID)+len(s.Origem.Bootstrap)+12*len(parts)+4)
	b = append(b, offsetsMagic[:]...)
	b = append(b, offsetsVersion, 0, 0, 0)
	b = appendTexto(b, s.Origem.Topic)
	b = appendTexto(b, s.Origem.GroupID)
	b = appendTexto(b, s.Origem.ClusterID)
	b = appendTexto(b, s.Origem.Bootstrap)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(parts)))
	// Ordem crescente de partição: dois checkpoints do mesmo estado produzem
	// arquivos idênticos byte a byte, e um diff de dois arquivos é
	// ferramenta de diagnóstico.
	for _, p := range parts {
		b = binary.LittleEndian.AppendUint32(b, uint32(p))
		b = binary.LittleEndian.AppendUint64(b, uint64(s.Offsets[p]))
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func appendTexto(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func (r *reader) texto() string {
	n := int(r.u16())
	return string(r.take(n))
}

func decodeOffsets(raw []byte) (SourceOffsets, error) {
	var s SourceOffsets
	if len(raw) < 8 || [4]byte(raw[0:4]) != offsetsMagic {
		return s, fmt.Errorf("%w: magic inválida", ErrCorruptOffsets)
	}
	if raw[4] != offsetsVersion {
		return s, fmt.Errorf("%w: version desconhecida %d", ErrCorruptOffsets, raw[4])
	}
	if len(raw) < 12 {
		return s, fmt.Errorf("%w: truncado", ErrCorruptOffsets)
	}
	body, tail := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return s, fmt.Errorf("%w: CRC32C não confere", ErrCorruptOffsets)
	}

	r := &reader{b: body, pos: 8}
	s.Origem.Topic = r.texto()
	s.Origem.GroupID = r.texto()
	s.Origem.ClusterID = r.texto()
	s.Origem.Bootstrap = r.texto()
	n := r.u32()
	// Sanidade antes de alocar: 12 bytes por partição.
	if int(n) > len(body)/12 {
		return s, fmt.Errorf("%w: partCount %d impossível", ErrCorruptOffsets, n)
	}
	s.Offsets = make(map[int32]int64, n)
	for i := uint32(0); i < n; i++ {
		p := int32(r.u32())
		off := int64(r.u64())
		if _, dup := s.Offsets[p]; dup {
			return s, fmt.Errorf("%w: partição %d duplicada", ErrCorruptOffsets, p)
		}
		s.Offsets[p] = off
	}
	// Byte a mais no fim também é corrupção: sem esta checagem, um arquivo
	// truncado pela metade e completado com lixo passaria pelo CRC de um
	// prefixo válido.
	if r.bad || r.pos != len(body) {
		return s, fmt.Errorf("%w: tamanho inconsistente (pos=%d body=%d)", ErrCorruptOffsets, r.pos, len(body))
	}
	return s, nil
}

// WriteOffsets grava o kafka.offsets atomicamente, pelo mesmo caminho de
// durabilidade do manifest.
//
// O CHAMADOR é responsável pela ordem: este arquivo tem de ser escrito DEPOIS
// do manifest.bin. São dois renames sem transação entre eles, e a ordem é o
// que determina o estado possível após um crash entre os dois — offset ATRÁS
// dos dados (reprocessa, inócuo) contra offset ADIANTE dos dados (perda
// definitiva). O §48 já exige exatamente essa direção.
func WriteOffsets(path string, s SourceOffsets) error {
	return escreveAtomico(path, s.encode())
}

// LoadOffsets lê o kafka.offsets. Ausência devolve ErrNoOffsets.
func LoadOffsets(path string) (SourceOffsets, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return SourceOffsets{}, fmt.Errorf("%w: %s", ErrNoOffsets, path)
	}
	if err != nil {
		return SourceOffsets{}, err
	}
	return decodeOffsets(raw)
}
