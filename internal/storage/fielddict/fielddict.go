// Package fielddict implementa o dicionário append-only de nomes de campos
// (spec §32): fieldID implícito pela ordem das entries, nomes nunca
// repetidos nos records.
//
// Durabilidade: uma entry nova é fsyncada ANTES de LookupOrCreate devolver o
// id — nenhum record em segmento pode referenciar um fieldID que o replay do
// tail não consiga resolver.
package fielddict

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Layout do arquivo (v2):
//
//	header (8 bytes):   magic "NDBD" | version u8 | reserved[3]
//	entry de campo:     nameLen u16 LE (>=1) | typeHint u8 | name | crc32c u32
//	                    (CRC sobre nameLen..name; fieldID implícito, 1-based)
//	registro de hint:   0x0000 u16 LE | fieldID u16 LE | typeHint u8 | crc32c u32
//	                    (9 bytes fixos; CRC sobre os 5 primeiros)
//
// Os dois registros se distinguem por nameLen: 0 é impossível numa entry de
// campo (nome vazio é recusado), então serve de discriminador sem gastar um
// byte de tag.
//
// O typeHint da ENTRY continua sendo escrito como 0 ("não declarado"): o tipo
// não é conhecido quando o campo nasce. Quem o fixa é um registro de hint
// posterior, e o ÚLTIMO registro de um fieldID vence.
//
// Por que registro append-only e não reescrita in-place do byte da entry:
// o Open trunca o arquivo na primeira entry com CRC inválido. Uma reescrita
// in-place rasgada por crash (entre o byte de tipo e o CRC) deixaria uma
// entry inválida no MEIO do arquivo, e a abertura seguinte apagaria todos os
// campos posteriores — campos cujos IDs já estão em records duráveis. Com
// append-only, o rasgo só pode estar no tail: perde-se o hint que ainda não
// foi fsyncado, e o campo volta a fixar o tipo na próxima observação.
const (
	headerSize    = 8
	entryOverhead = 3 + 4 // nameLen+typeHint antes do nome, CRC depois

	hintRecordSize = 9 // nameLen(0) u16 | fieldID u16 | typeHint u8 | crc u32

	dictVersion = 2
)

var (
	dictMagic  = [4]byte{'N', 'D', 'B', 'D'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// ErrTypeHintConflict indica tentativa de fixar um tipo diferente do já
// fixado para o campo. É a materialização da regra R11: o tipo de um campo
// auto é decidido na PRIMEIRA observação e não muda depois — senão o mesmo
// campo teria significados diferentes conforme a ordem de chegada dos
// eventos, e nenhuma query sobre ele seria confiável.
var ErrTypeHintConflict = errors.New("fielddict: tipo do campo já fixado com outro valor")

// dictFile é a superfície de arquivo usada pelo dicionário. É interface
// para permitir que testes verifiquem a ORDEM das chamadas de durabilidade
// (a invariante fsync-before-first-use do §4/§14.6).
type dictFile interface {
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Close() error
}

// Dict é o dicionário de campos. LookupOrCreate é exclusivo do writer;
// Lookup/Name são seguros para leitores concorrentes. O mapeamento em heap
// é bounded pela cardinalidade de NOMES de campos (dezenas/centenas), não
// pelo número de entidades.
type Dict struct {
	mu     sync.RWMutex
	f      dictFile
	size   int64
	byName map[string]types.FieldID
	byID   []string // índice 0 = fieldID 1
	// hints é paralelo a byID (índice 0 = fieldID 1); types.TypeNull = não
	// fixado. Array e não map porque a cardinalidade é a mesma de byID, que o
	// Open já conhece.
	hints []types.FieldType
}

// Open cria ou reabre o dicionário, validando o prefixo por CRC e truncando
// qualquer tail rasgado por crash.
func Open(path string) (*Dict, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("fielddict: abrir %s: %w", path, err)
	}
	d := &Dict{f: f, byName: make(map[string]types.FieldID)}

	raw, err := os.ReadFile(path)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("fielddict: ler %s: %w", path, err)
	}
	if len(raw) == 0 {
		var h [headerSize]byte
		copy(h[0:4], dictMagic[:])
		h[4] = dictVersion
		if _, err := f.WriteAt(h[:], 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("fielddict: escrever header: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		d.size = headerSize
		return d, nil
	}
	if len(raw) < headerSize || [4]byte(raw[0:4]) != dictMagic {
		f.Close()
		return nil, fmt.Errorf("fielddict: header inválido em %s", path)
	}
	if raw[4] != dictVersion {
		f.Close()
		// Quem lê isto é um operador com o processo caído: a mensagem precisa
		// dizer QUAL arquivo, o que ele é, e o que fazer. Não há conversor de
		// v1 (decisão QP-D2, design §4): o v1 não tem onde guardar o tipo
		// fixado dos campos auto, e ler um v1 como v2 faria todo campo auto
		// parecer nunca observado — em silêncio, e só na query.
		return nil, fmt.Errorf("fielddict: %s está na version %d, esperada %d: "+
			"não há migração do formato antigo — recrie o dataset a partir da origem",
			path, raw[4], dictVersion)
	}

	// Scan do prefixo válido; primeiro registro inválido/incompleto →
	// truncate. Os hints são aplicados na ordem do arquivo, então o último
	// registro de um campo vence.
	pos := headerSize
	for pos < len(raw) {
		rest := raw[pos:]
		if len(rest) < entryOverhead {
			break
		}
		if binary.LittleEndian.Uint16(rest[0:2]) == 0 {
			// Registro de hint.
			if len(rest) < hintRecordSize {
				break
			}
			body := rest[:5]
			crc := binary.LittleEndian.Uint32(rest[5:9])
			if crc32.Checksum(body, castagnoli) != crc {
				break
			}
			id := types.FieldID(binary.LittleEndian.Uint16(body[2:4]))
			t := types.FieldType(body[4])
			// Hint de campo inexistente só apareceria com arquivo corrompido
			// de forma que o CRC não pega; ignorar é mais seguro que indexar
			// fora do array.
			if id >= 1 && int(id) <= len(d.byID) {
				d.hints[id-1] = t
			}
			pos += hintRecordSize
			continue
		}
		nameLen := int(binary.LittleEndian.Uint16(rest[0:2]))
		if len(rest) < entryOverhead+nameLen {
			break
		}
		body := rest[:3+nameLen]
		crc := binary.LittleEndian.Uint32(rest[3+nameLen : 3+nameLen+4])
		if crc32.Checksum(body, castagnoli) != crc {
			break
		}
		name := string(body[3 : 3+nameLen])
		d.byID = append(d.byID, name)
		d.byName[name] = types.FieldID(len(d.byID))
		d.hints = append(d.hints, types.TypeNull)
		pos += entryOverhead + nameLen
	}
	if int64(pos) != int64(len(raw)) {
		if err := f.Truncate(int64(pos)); err != nil {
			f.Close()
			return nil, fmt.Errorf("fielddict: truncar tail rasgado em %d: %w", pos, err)
		}
	}
	d.size = int64(pos)
	return d, nil
}

// LookupOrCreate devolve o id do campo, criando (com fsync) se necessário.
// Exclusivo do writer.
func (d *Dict) LookupOrCreate(name []byte) (types.FieldID, error) {
	if id, ok := d.Lookup(name); ok {
		return id, nil
	}
	if len(name) == 0 {
		return 0, fmt.Errorf("fielddict: nome de campo vazio")
	}
	if len(name) > math.MaxUint16 {
		return 0, fmt.Errorf("fielddict: nome de campo com %d bytes excede u16", len(name))
	}
	if len(d.byID) >= math.MaxUint16 {
		return 0, fmt.Errorf("fielddict: limite de %d campos atingido", math.MaxUint16)
	}

	entry := make([]byte, 0, entryOverhead+len(name))
	entry = binary.LittleEndian.AppendUint16(entry, uint16(len(name)))
	entry = append(entry, 0) // typeHint: 0 = não declarado (reservado)
	entry = append(entry, name...)
	entry = binary.LittleEndian.AppendUint32(entry, crc32.Checksum(entry, castagnoli))

	if _, err := d.f.WriteAt(entry, d.size); err != nil {
		return 0, fmt.Errorf("fielddict: append de %q: %w", name, err)
	}
	if err := d.f.Sync(); err != nil {
		return 0, fmt.Errorf("fielddict: fsync de %q: %w", name, err)
	}

	d.mu.Lock()
	d.size += int64(len(entry))
	d.byID = append(d.byID, string(name))
	d.hints = append(d.hints, types.TypeNull)
	id := types.FieldID(len(d.byID))
	d.byName[string(name)] = id
	d.mu.Unlock()
	return id, nil
}

// TypeHint devolve o tipo FIXADO do campo (R11). ok=false quando o campo não
// existe ou quando nunca teve tipo fixado — os dois casos significam "não há
// tipo contra o qual interpretar valores deste campo", e o binder trata os
// dois igual. Seguro para leitores concorrentes.
func (d *Dict) TypeHint(id types.FieldID) (types.FieldType, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if id < 1 || int(id) > len(d.hints) {
		return types.TypeNull, false
	}
	t := d.hints[id-1]
	return t, t != types.TypeNull
}

// SetTypeHint fixa o tipo do campo na primeira observação e o torna DURÁVEL
// antes de retornar, pela mesma razão que o nome (§14.6): nenhum record em
// segmento pode referenciar um tipo que uma reabertura não consiga resolver.
//
// Idempotente — fixar o MESMO tipo de novo não escreve nada. Fixar tipo
// DIFERENTE devolve ErrTypeHintConflict e mantém o anterior. TypeNull é erro:
// não se fixa "nenhum tipo", e permitir isso travaria o campo num estado que
// nunca mais poderia ser corrigido. Exclusivo do writer, como LookupOrCreate.
func (d *Dict) SetTypeHint(id types.FieldID, t types.FieldType) error {
	if t == types.TypeNull {
		return fmt.Errorf("fielddict: tipo nulo não pode ser fixado no campo %d", id)
	}
	d.mu.RLock()
	exists := id >= 1 && int(id) <= len(d.hints)
	var current types.FieldType
	if exists {
		current = d.hints[id-1]
	}
	name := ""
	if exists {
		name = d.byID[id-1]
	}
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("fielddict: campo %d inexistente", id)
	}
	switch current {
	case t:
		return nil // idempotente: já fixado com o mesmo tipo
	case types.TypeNull: // ainda não fixado: segue
	default:
		return fmt.Errorf("%w: campo %q fixado como %s, tentativa de %s",
			ErrTypeHintConflict, name, current, t)
	}

	var rec [hintRecordSize]byte
	binary.LittleEndian.PutUint16(rec[0:2], 0) // discriminador de hint
	binary.LittleEndian.PutUint16(rec[2:4], uint16(id))
	rec[4] = byte(t)
	binary.LittleEndian.PutUint32(rec[5:9], crc32.Checksum(rec[:5], castagnoli))

	if _, err := d.f.WriteAt(rec[:], d.size); err != nil {
		return fmt.Errorf("fielddict: append do tipo de %q: %w", name, err)
	}
	// fsync ANTES de publicar em memória: invertido, um crash entre a
	// publicação e o fsync deixaria records duráveis apontando para um tipo
	// que o dicionário não conhece.
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("fielddict: fsync do tipo de %q: %w", name, err)
	}

	d.mu.Lock()
	d.size += hintRecordSize
	d.hints[id-1] = t
	d.mu.Unlock()
	return nil
}

// Lookup devolve o id do campo sem criar.
func (d *Dict) Lookup(name []byte) (types.FieldID, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.byName[string(name)] // conversão elidida em lookup de map
	return id, ok
}

// Name devolve o nome do campo pelo id.
func (d *Dict) Name(id types.FieldID) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if id < 1 || int(id) > len(d.byID) || d.byID[id-1] == "" {
		return "", false
	}
	return d.byID[id-1], true
}

// Len devolve o número de campos registrados.
func (d *Dict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.byID)
}

// SizeBytes devolve o tamanho válido do arquivo (advisory para o manifest).
func (d *Dict) SizeBytes() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.size
}

// Close fecha o arquivo do dicionário.
func (d *Dict) Close() error { return d.f.Close() }
