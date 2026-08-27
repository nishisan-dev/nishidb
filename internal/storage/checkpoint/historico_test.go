package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// O dataset materializado tem Modo e RowIDBase zerados e continua gravando um
// manifest v1 — é isso que preserva o rollback para quem nunca declarou
// `history`. Falhar aqui significa todo dataset em produção virar v2 no
// primeiro deploy, sem nada em troca.
func TestManifestSemHistoricoContinuaNaV1(t *testing.T) {
	m := &Manifest{Epoch: 3, NextRowID: 10}
	if n := TamanhoDaCaudaTLV(m); n != 0 {
		t.Fatalf("cauda = %d bytes, quer 0", n)
	}
	if got := m.encode()[4]; got != manifestV1 {
		t.Fatalf("version = %d, quer %d", got, manifestV1)
	}
}

func TestModoHistoricoSobreviveAoIdaEVolta(t *testing.T) {
	m := &Manifest{Epoch: 1, NextRowID: 900, Modo: 1, RowIDBase: 128}
	volta, err := decode(m.encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if volta.Modo != 1 {
		t.Errorf("Modo = %d, quer 1", volta.Modo)
	}
	if volta.RowIDBase != 128 {
		t.Errorf("RowIDBase = %d, quer 128", volta.RowIDBase)
	}
}

// A marca-d'água sozinha já leva o manifest à v2: ela muda o que é VISÍVEL, e
// um binário que a ignorasse devolveria linhas já purgadas.
func TestRowIDBaseSozinhaLevaAV2(t *testing.T) {
	m := &Manifest{RowIDBase: 7}
	if got := m.encode()[4]; got != manifestV2 {
		t.Fatalf("version = %d, quer %d", got, manifestV2)
	}
	volta, err := decode(m.encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if volta.RowIDBase != 7 {
		t.Fatalf("RowIDBase = %d, quer 7", volta.RowIDBase)
	}
}

// As duas seções convivem, e a ordem é CRESCENTE por tag: uma ordem que
// dependesse de quando cada seção foi inventada mudaria o arquivo sem que o
// conteúdo mudasse.
func TestHashesEHistoricoConvivemEmOrdemDeTag(t *testing.T) {
	m := &Manifest{
		Hashes:    []HashInfo{{FieldID: 4, SlotCount: 1024}},
		Modo:      1,
		RowIDBase: 64,
	}
	b := m.encode()
	volta, err := decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(volta.Hashes) != 1 || volta.Hashes[0].FieldID != 4 {
		t.Errorf("Hashes = %+v", volta.Hashes)
	}
	if volta.Modo != 1 || volta.RowIDBase != 64 {
		t.Errorf("Modo = %d, RowIDBase = %d", volta.Modo, volta.RowIDBase)
	}
	hash := binary.LittleEndian.AppendUint16(nil, tagHashes)
	hist := binary.LittleEndian.AppendUint16(nil, tagHistorico)
	iHash, iHist := bytes.Index(b, hash), bytes.Index(b, hist)
	if iHash < 0 || iHist < 0 || iHash > iHist {
		t.Fatalf("posições: hashes em %d, histórico em %d", iHash, iHist)
	}
}

// caudaHistorico monta a seção 0x8002 à mão, sem passar pelo encoder deste
// pacote — a mesma independência do manifestV1Golden: um teste que gerasse os
// bytes pelo próprio encoder provaria só que ele concorda consigo mesmo.
func caudaHistorico(modo uint8, base uint64, lenDeclarado uint32) []byte {
	raw := manifestV1Golden()
	body := raw[:len(raw)-4] // fora o CRC
	body[4] = 2              // tem cauda, logo é v2
	body = binary.LittleEndian.AppendUint16(body, tagHistorico)
	body = binary.LittleEndian.AppendUint32(body, lenDeclarado)
	body = append(body, modo)
	body = binary.LittleEndian.AppendUint64(body, base)
	return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
}

func carrega(t *testing.T, b []byte) (*Manifest, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestSecaoDeHistoricoEscritaAMaoEhLida(t *testing.T) {
	m, err := carrega(t, caudaHistorico(1, 4096, historicoInfoSize))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Modo != 1 || m.RowIDBase != 4096 {
		t.Fatalf("Modo = %d, RowIDBase = %d", m.Modo, m.RowIDBase)
	}
	if m.Epoch != 7 {
		t.Fatalf("o corpo não sobreviveu à cauda: %+v", *m)
	}
}

// O CRC é conferido ANTES da cauda e um adulterador o recalcula: a recusa tem
// de vir do formato da seção, não do checksum.
func TestSecaoDeHistoricoComTamanhoErradoEhRecusada(t *testing.T) {
	if _, err := carrega(t, caudaHistorico(1, 5, historicoInfoSize-1)); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("Load = %v, quer ErrCorruptManifest", err)
	}
}
