package alarmdelivery

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// Cabeçalho v1: magic(4), versão(1), reserva(3), tamanho(4), CRC32C(4).
func encodeRecord(body []byte) []byte {
	b := make([]byte, recordHeaderBytes+len(body))
	copy(b, "NDAQ")
	b[4] = 1
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(body)))
	binary.LittleEndian.PutUint32(b[12:16], crc32.Checksum(body, checksumTable))
	copy(b[16:], body)
	return b
}

// eventIdentity é o que o remetente lê do corpo gravado, no MESMO
// json.Unmarshal que confere o registro. ID e At valem para todo transporte
// e são exigidos aqui; Event, Dataset, Counter e GroupKey só o Kafka usa (o
// nome do evento vai num cabeçalho, e a chave do registro é montada com os
// outros três), e quem os exige é o transporte (kafkaRecord). O formato do
// registro em disco não muda: tudo isto já está no corpo desde a v1.
type eventIdentity struct {
	ID      string    `json:"eventId"`
	At      time.Time `json:"at"`
	Event   string    `json:"event"`
	Dataset string    `json:"dataset"`
	Counter string    `json:"counter"`
	// GroupKey é ponteiro para distinguir o campo ausente do texto vazio.
	GroupKey *string `json:"groupKey"`
}

func readRecord(path string) ([]byte, eventIdentity, error) {
	var identity eventIdentity
	stat, err := os.Lstat(path)
	if err != nil {
		return nil, identity, err
	}
	if !stat.Mode().IsRegular() || stat.Size() < recordHeaderBytes || stat.Size() > maxEventBytes+recordHeaderBytes {
		return nil, identity, errors.New("invalid queue record size or file type")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, identity, err
	}
	if len(raw) < recordHeaderBytes || string(raw[:4]) != "NDAQ" || raw[4] != 1 || raw[5] != 0 || raw[6] != 0 || raw[7] != 0 || uint64(binary.LittleEndian.Uint32(raw[8:12])) != uint64(len(raw)-recordHeaderBytes) {
		return nil, identity, errors.New("invalid queue record header")
	}
	body := raw[recordHeaderBytes:]
	if crc32.Checksum(body, checksumTable) != binary.LittleEndian.Uint32(raw[12:16]) {
		return nil, identity, errors.New("queue record checksum mismatch")
	}
	if err = json.Unmarshal(body, &identity); err != nil || identity.ID == "" || identity.At.IsZero() {
		return nil, identity, errors.New("invalid queue event identity")
	}
	return body, identity, nil
}

func (q *queue) recover() error {
	// ReadDir em lotes impede que nomes de arquivos consumam o heap inteiro.
	dir, err := os.Open(q.dir)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	removed := false
	for {
		files, readErr := dir.ReadDir(256)
		for _, file := range files {
			name := file.Name()
			if strings.HasSuffix(name, ".evt.tmp") && len(name) == 28 {
				if _, err := strconv.ParseUint(name[:20], 10, 64); err != nil {
					return fmt.Errorf("unexpected queue file %q", name)
				}
				if err := os.Remove(filepath.Join(q.dir, name)); err != nil {
					return err
				}
				removed = true
				continue
			}
			if len(name) != 24 || !strings.HasSuffix(name, ".evt") {
				return fmt.Errorf("unexpected queue file %q", name)
			}
			seq, err := strconv.ParseUint(name[:20], 10, 64)
			if err != nil || seq == 0 || seq == ^uint64(0) {
				return fmt.Errorf("invalid queue sequence %q", name)
			}
			body, identity, err := readRecord(filepath.Join(q.dir, name))
			if err != nil {
				return fmt.Errorf("record %q: %w", name, err)
			}
			if len(q.pending) >= maxDiskEvents {
				return errors.New("queue exceeds maximum record count")
			}
			size := int64(len(body) + recordHeaderBytes)
			q.pending = append(q.pending, entry{seq: seq, size: size, at: identity.At})
			q.sequence = max(q.sequence, seq)
			q.stats.DiskBytes += size
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return readErr
			}
			break
		}
	}
	if removed {
		if err := atomicfile.SyncDir(q.dir); err != nil {
			return err
		}
	}
	sort.Slice(q.pending, func(i, j int) bool { return q.pending[i].seq < q.pending[j].seq })
	q.stats.DiskEvents = len(q.pending)
	q.reservedBytes = q.stats.DiskBytes
	return nil
}
