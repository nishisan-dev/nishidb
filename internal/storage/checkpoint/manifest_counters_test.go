package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"slices"
	"testing"
)

func sampleCounters() []CounterInfo {
	return []CounterInfo{
		{Name: "servicos_por_olt", Columns: []uint16{7}},
		{Name: "autenticados_por_pon", Columns: []uint16{7, 9, 10}, Where: "authenticated = true"},
	}
}

func TestManifestComContadoresRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	want.Counters = sampleCounters()
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Counters) != 2 {
		t.Fatalf("a seção dos contadores voltou com %d entradas, quer 2", len(got.Counters))
	}
	for i := range want.Counters {
		w, g := want.Counters[i], got.Counters[i]
		if g.Name != w.Name || g.Where != w.Where || !slices.Equal(g.Columns, w.Columns) {
			t.Fatalf("contador %d voltou %+v, quer %+v", i, g, w)
		}
	}
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("a cauda deslocou os segmentos: %+v", got.Segments)
	}
}

// TestManifestSemContadoresNaoEmiteASecao: o dataset sem contadores gera
// exatamente os bytes de antes — é o que mantém os goldens do formato verdes e
// o que faz um dataset que nunca declarou contador não ganhar seção nenhuma.
func TestManifestSemContadoresNaoEmiteASecao(t *testing.T) {
	m := sampleManifest()
	if n := TLVTailSize(&m); n != tlvHeaderSize+indexInfoSize {
		t.Fatalf("cauda de %d bytes num manifest sem contadores; quer só a seção do índice", n)
	}
	empty := sampleManifest()
	empty.Counters = []CounterInfo{}
	if !slices.Equal(m.encode(), empty.encode()) {
		t.Fatal("lista vazia de contadores tem de gerar os mesmos bytes que nil")
	}
	got, err := decode(m.encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Counters != nil {
		t.Fatalf("manifest sem a seção tem de voltar com Counters nil, veio %+v", got.Counters)
	}
}

func TestAReservaDaCaudaContaOsContadores(t *testing.T) {
	without := sampleManifest()
	with := sampleManifest()
	with.Counters = sampleCounters()
	growth := len(with.encode()) - len(without.encode())
	if declared := TLVTailSize(&with) - TLVTailSize(&without); growth != declared {
		t.Fatalf("a seção ocupou %d bytes; TLVTailSize declara %d", growth, declared)
	}
}

// TestASecaoDosContadoresEhIgnoravel: a política é deliberada. "Seção ausente
// ⇒ reconstruir" já é a resposta certa do binário novo, então uma tag crítica
// só serviria para bloquear o rollback do piloto. O binário anterior pula a
// seção, grava o manifest sem ela, e o novo reconstrói ao voltar.
func TestASecaoDosContadoresEhIgnoravel(t *testing.T) {
	if tagCounters&tagCritical != 0 {
		t.Fatalf("tagCounters %#04x carrega o bit crítico; ela tem de ser ignorável", tagCounters)
	}
}

// A ordem continua crescente por tag com a seção nova, que é a menor de todas
// e por isso sai ANTES das críticas.
func TestCaudaSaiEmOrdemCrescenteDeTagComContadores(t *testing.T) {
	m := sampleManifest()
	m.Mode, m.RowIDBase = 1, 500
	m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	m.Counters = sampleCounters()
	raw := m.encode()
	tlvTail := raw[len(raw)-4-TLVTailSize(&m) : len(raw)-4]

	var tags []uint16
	for pos := 0; pos+tlvHeaderSize <= len(tlvTail); {
		tag := binary.LittleEndian.Uint16(tlvTail[pos:])
		n := binary.LittleEndian.Uint32(tlvTail[pos+2:])
		tags = append(tags, tag)
		pos += tlvHeaderSize + int(n)
	}
	if len(tags) != 4 {
		t.Fatalf("a cauda tem %d registros, quer 4: %#04x", len(tags), tags)
	}
	if !slices.IsSorted(tags) {
		t.Fatalf("as tags saíram fora de ordem crescente: %#04x", tags)
	}
	if tags[0] != tagCounters {
		t.Fatalf("a seção dos contadores tinha de ser a primeira, veio %#04x", tags[0])
	}
}

// TestSecaoDosContadoresTruncadaEhRecusada: uma seção que declara mais
// contadores do que os bytes contêm é manifest corrompido, e não "menos
// contadores" — ler pela metade adotaria um contador com colunas erradas.
func TestSecaoDosContadoresTruncadaEhRecusada(t *testing.T) {
	m := sampleManifest()
	m.Counters = sampleCounters()
	raw := m.encode()
	body := raw[:len(raw)-4]
	// A seção começa logo depois do kafkaCount, no início da cauda; o payload
	// dela começa após o header TLV. Corta o último byte do nome do primeiro
	// contador e recalcula o CRC para que só a seção acuse.
	start := len(body) - TLVTailSize(&m)
	payloadStart := start + tlvHeaderSize
	tampered := slices.Clone(body)
	// u16 n=2 | u16 lenNome=16 → declara 16 bytes de nome mas só sobram 15 se
	// o tamanho do nome for inflado em 1.
	binary.LittleEndian.PutUint16(tampered[payloadStart+2:], 17)
	tampered = binary.LittleEndian.AppendUint32(tampered, crc32.Checksum(tampered, castagnoli))
	if _, err := decode(tampered); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("seção inconsistente tem de ser ErrCorruptManifest, veio %v", err)
	}
}
