package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"slices"
	"strings"
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
	with.Counters[0].DictValidBytes = 512
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
	m.StoreMode, m.RowIDBase = 1, 500
	m.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	m.Counters = sampleCounters()
	m.Counters[1].DictValidBytes = 100
	raw := m.encode()
	tlvTail := raw[len(raw)-4-TLVTailSize(&m) : len(raw)-4]

	var tags []uint16
	for pos := 0; pos+tlvHeaderSize <= len(tlvTail); {
		tag := binary.LittleEndian.Uint16(tlvTail[pos:])
		n := binary.LittleEndian.Uint32(tlvTail[pos+2:])
		tags = append(tags, tag)
		pos += tlvHeaderSize + int(n)
	}
	want := []uint16{tagCounters, tagCounterDictValidBytes, tagHistory, tagSchedule, tagPerSegmentIndex}
	if !slices.Equal(tags, want) {
		t.Fatalf("as tags da cauda saíram %#04x, quer %#04x (ordem crescente, as duas ignoráveis antes das críticas)", tags, want)
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

// withExtraTLV devolve o manifest codificado com um registro TLV a mais no fim
// da cauda e o CRC recalculado — o jeito de montar uma seção que o encode não
// produz (nome desconhecido, nome repetido, seção duplicada).
func withExtraTLV(m *Manifest, tag uint16, payload []byte) []byte {
	raw := m.encode()
	body := slices.Clone(raw[:len(raw)-4])
	body = binary.LittleEndian.AppendUint16(body, tag)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(payload)))
	body = append(body, payload...)
	return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
}

// floorEntry é uma entrada da seção 0x0002.
func floorEntry(b []byte, name string, valid uint64) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(name)))
	b = append(b, name...)
	return binary.LittleEndian.AppendUint64(b, valid)
}

// TestCounterDictFloorRoundTrip: o piso de durabilidade de cada contador volta
// do disco no contador certo, e um contador sem piso volta sem piso.
func TestCounterDictFloorRoundTrip(t *testing.T) {
	m := sampleManifest()
	m.Counters = sampleCounters()
	m.Counters[1].DictValidBytes = 4242
	got, err := decode(m.encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Counters[0].DictValidBytes != 0 || got.Counters[1].DictValidBytes != 4242 {
		t.Fatalf("pisos voltaram %d e %d, quer 0 e 4242", got.Counters[0].DictValidBytes, got.Counters[1].DictValidBytes)
	}
}

// TestCounterDictFloorSectionGoldenBytes trava o formato da seção 0x0002 em
// bytes literais, escritos à mão a partir do layout do godoc — e não
// recalculados pelo próprio encode, que tornaria o golden autocumprido. A
// ordem da cauda também está nele: a 0x0002 sai logo depois da 0x0001 e antes
// de todas as críticas.
func TestCounterDictFloorSectionGoldenBytes(t *testing.T) {
	m := sampleManifest()
	m.Counters = []CounterInfo{{Name: "ab", Columns: []uint16{7}, DictValidBytes: 0x0102030405060708}}
	raw := m.encode()
	tail := raw[len(raw)-4-TLVTailSize(&m) : len(raw)-4]
	want := []byte{
		// 0x0001, 14 bytes: n=1 | lenNome=2 "ab" | nCols=1 col=7 | lenWhere=0
		0x01, 0x00, 0x0e, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x02, 0x00, 'a', 'b', 0x01, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00,
		// 0x0002, 14 bytes: n=1 | lenNome=2 "ab" | validBytes u64 LE
		0x02, 0x00, 0x0e, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x02, 0x00, 'a', 'b', 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
		// 0x8004: formato do índice por segmento
		0x04, 0x80, 0x01, 0x00, 0x00, 0x00, 0x01,
	}
	if !slices.Equal(tail, want) {
		t.Fatalf("cauda = % x\n quer % x", tail, want)
	}
}

// TestCounterDictFloorSectionIsIgnorableByAnOlderBinary: a seção é ignorável
// pelo bit, e o que um binário anterior faz com ela é o ramo de "tag ignorável
// desconhecida". A prova troca a tag 0x0002 por uma ignorável que NENHUM
// binário conhece (0x0003), com os mesmos bytes, e decodifica: o manifest
// abre, os contadores voltam inteiros e sem piso — exatamente o que o binário
// anterior vê.
func TestCounterDictFloorSectionIsIgnorableByAnOlderBinary(t *testing.T) {
	if tagCounterDictValidBytes&tagCritical != 0 {
		t.Fatalf("tagCounterDictValidBytes %#04x carrega o bit crítico; o rollback recusaria o manifest", tagCounterDictValidBytes)
	}
	m := sampleManifest()
	m.Counters = sampleCounters()
	m.Counters[0].DictValidBytes = 77
	raw := m.encode()
	body := slices.Clone(raw[:len(raw)-4])
	start := len(body) - TLVTailSize(&m)
	sec0 := start + tlvHeaderSize + countersSectionSize(m.Counters)
	if got := binary.LittleEndian.Uint16(body[sec0:]); got != tagCounterDictValidBytes {
		t.Fatalf("a seção depois da 0x0001 é %#04x, quer a do piso", got)
	}
	binary.LittleEndian.PutUint16(body[sec0:], 0x0003)
	older := binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
	got, err := decode(older)
	if err != nil {
		t.Fatalf("o manifest com uma seção ignorável desconhecida tem de abrir: %v", err)
	}
	if len(got.Counters) != 2 || got.Counters[0].Name != m.Counters[0].Name || got.Counters[0].DictValidBytes != 0 {
		t.Fatalf("contadores = %+v; quer os dois, sem piso", got.Counters)
	}
}

// TestCounterDictFloorOfAnUndeclaredCounterIsIgnored: o piso de um nome que a
// 0x0001 não declara não tem a quem se aplicar.
func TestCounterDictFloorOfAnUndeclaredCounterIsIgnored(t *testing.T) {
	m := sampleManifest()
	m.Counters = sampleCounters()
	payload := binary.LittleEndian.AppendUint16(nil, 2)
	payload = floorEntry(payload, "servicos_por_olt", 900)
	payload = floorEntry(payload, "nao_declarado", 300)
	got, err := decode(withExtraTLV(&m, tagCounterDictValidBytes, payload))
	if err != nil {
		t.Fatal(err)
	}
	if got.Counters[0].DictValidBytes != 900 || got.Counters[1].DictValidBytes != 0 {
		t.Fatalf("pisos = %d e %d, quer 900 e 0", got.Counters[0].DictValidBytes, got.Counters[1].DictValidBytes)
	}
}

// TestCounterDictFloorSectionInconsistentIsRefused: nome repetido, seção
// duplicada e tamanho que não fecha são manifest corrompido — dois pisos para
// o mesmo dicionário não têm leitura certa.
func TestCounterDictFloorSectionInconsistentIsRefused(t *testing.T) {
	m := sampleManifest()
	m.Counters = sampleCounters()
	repeated := binary.LittleEndian.AppendUint16(nil, 2)
	repeated = floorEntry(repeated, "servicos_por_olt", 100)
	repeated = floorEntry(repeated, "servicos_por_olt", 200)
	short := binary.LittleEndian.AppendUint16(nil, 2)
	short = floorEntry(short, "servicos_por_olt", 100)
	withFloor := m
	withFloor.Counters = sampleCounters()
	withFloor.Counters[0].DictValidBytes = 50
	single := floorEntry(binary.LittleEndian.AppendUint16(nil, 1), "servicos_por_olt", 60)
	for name, raw := range map[string][]byte{
		"nome repetido":    withExtraTLV(&m, tagCounterDictValidBytes, repeated),
		"entradas a menos": withExtraTLV(&m, tagCounterDictValidBytes, short),
		"seção duplicada":  withExtraTLV(&withFloor, tagCounterDictValidBytes, single),
	} {
		_, err := decode(raw)
		if !errors.Is(err, ErrCorruptManifest) || !strings.Contains(err.Error(), "counter dictionary floor") {
			t.Errorf("%s: decode = %v; quer ErrCorruptManifest com a mensagem da seção do piso", name, err)
		}
	}
}
