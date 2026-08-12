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
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Layout do arquivo:
//
//	header (8 bytes): magic "NDBD" | version u8 | reserved[3]
//	entry:            nameLen u16 LE | typeHint u8 | name | crc32c u32
//	                  (CRC sobre nameLen..name; fieldID implícito, 1-based)
const (
	headerSize    = 8
	entryOverhead = 3 + 4 // nameLen+typeHint antes do nome, CRC depois

	dictVersion = 1
)

var (
	dictMagic  = [4]byte{'N', 'D', 'B', 'D'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

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
		return nil, fmt.Errorf("fielddict: version desconhecida %d", raw[4])
	}

	// Scan do prefixo válido; primeira entry inválida/incompleta → truncate.
	pos := headerSize
	for pos < len(raw) {
		rest := raw[pos:]
		if len(rest) < entryOverhead {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(rest[0:2]))
		if nameLen == 0 || len(rest) < entryOverhead+nameLen {
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
	id := types.FieldID(len(d.byID))
	d.byName[string(name)] = id
	d.mu.Unlock()
	return id, nil
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
