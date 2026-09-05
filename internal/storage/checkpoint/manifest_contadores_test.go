package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"slices"
	"testing"
)

func contadoresDeExemplo() []ContadorInfo {
	return []ContadorInfo{
		{Nome: "servicos_por_olt", Colunas: []uint16{7}},
		{Nome: "autenticados_por_pon", Colunas: []uint16{7, 9, 10}, Where: "authenticated = true"},
	}
}

func TestManifestComContadoresRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	want.Contadores = contadoresDeExemplo()
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Contadores) != 2 {
		t.Fatalf("a seção dos contadores voltou com %d entradas, quer 2", len(got.Contadores))
	}
	for i := range want.Contadores {
		w, g := want.Contadores[i], got.Contadores[i]
		if g.Nome != w.Nome || g.Where != w.Where || !slices.Equal(g.Colunas, w.Colunas) {
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
	if n := TamanhoDaCaudaTLV(&m); n != tlvHeaderSize+indiceInfoSize {
		t.Fatalf("cauda de %d bytes num manifest sem contadores; quer só a seção do índice", n)
	}
	vazio := sampleManifest()
	vazio.Contadores = []ContadorInfo{}
	if !slices.Equal(m.encode(), vazio.encode()) {
		t.Fatal("lista vazia de contadores tem de gerar os mesmos bytes que nil")
	}
	got, err := decode(m.encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Contadores != nil {
		t.Fatalf("manifest sem a seção tem de voltar com Contadores nil, veio %+v", got.Contadores)
	}
}

func TestAReservaDaCaudaContaOsContadores(t *testing.T) {
	sem := sampleManifest()
	com := sampleManifest()
	com.Contadores = contadoresDeExemplo()
	crescimento := len(com.encode()) - len(sem.encode())
	if declarado := TamanhoDaCaudaTLV(&com) - TamanhoDaCaudaTLV(&sem); crescimento != declarado {
		t.Fatalf("a seção ocupou %d bytes; TamanhoDaCaudaTLV declara %d", crescimento, declarado)
	}
}

// TestASecaoDosContadoresEhIgnoravel: a política é deliberada. "Seção ausente
// ⇒ reconstruir" já é a resposta certa do binário novo, então uma tag crítica
// só serviria para bloquear o rollback do piloto. O binário anterior pula a
// seção, grava o manifest sem ela, e o novo reconstrói ao voltar.
func TestASecaoDosContadoresEhIgnoravel(t *testing.T) {
	if tagContadores&tagCritica != 0 {
		t.Fatalf("tagContadores %#04x carrega o bit crítico; ela tem de ser ignorável", tagContadores)
	}
}

// A ordem continua crescente por tag com a seção nova, que é a menor de todas
// e por isso sai ANTES das críticas.
func TestCaudaSaiEmOrdemCrescenteDeTagComContadores(t *testing.T) {
	m := sampleManifest()
	m.Modo, m.RowIDBase = 1, 500
	m.Agenda = &AgendaInfo{DuracaoDeFundoMs: 60_000, BlockShift: 6, Niveis: 4}
	m.Contadores = contadoresDeExemplo()
	raw := m.encode()
	cauda := raw[len(raw)-4-TamanhoDaCaudaTLV(&m) : len(raw)-4]

	var tags []uint16
	for pos := 0; pos+tlvHeaderSize <= len(cauda); {
		tag := binary.LittleEndian.Uint16(cauda[pos:])
		n := binary.LittleEndian.Uint32(cauda[pos+2:])
		tags = append(tags, tag)
		pos += tlvHeaderSize + int(n)
	}
	if len(tags) != 4 {
		t.Fatalf("a cauda tem %d registros, quer 4: %#04x", len(tags), tags)
	}
	if !slices.IsSorted(tags) {
		t.Fatalf("as tags saíram fora de ordem crescente: %#04x", tags)
	}
	if tags[0] != tagContadores {
		t.Fatalf("a seção dos contadores tinha de ser a primeira, veio %#04x", tags[0])
	}
}

// TestSecaoDosContadoresTruncadaEhRecusada: uma seção que declara mais
// contadores do que os bytes contêm é manifest corrompido, e não "menos
// contadores" — ler pela metade adotaria um contador com colunas erradas.
func TestSecaoDosContadoresTruncadaEhRecusada(t *testing.T) {
	m := sampleManifest()
	m.Contadores = contadoresDeExemplo()
	raw := m.encode()
	corpo := raw[:len(raw)-4]
	// A seção começa logo depois do kafkaCount, no início da cauda; o payload
	// dela começa após o header TLV. Corta o último byte do nome do primeiro
	// contador e recalcula o CRC para que só a seção acuse.
	inicio := len(corpo) - TamanhoDaCaudaTLV(&m)
	payloadInicio := inicio + tlvHeaderSize
	adulterado := slices.Clone(corpo)
	// u16 n=2 | u16 lenNome=16 → declara 16 bytes de nome mas só sobram 15 se
	// o tamanho do nome for inflado em 1.
	binary.LittleEndian.PutUint16(adulterado[payloadInicio+2:], 17)
	adulterado = binary.LittleEndian.AppendUint32(adulterado, crc32.Checksum(adulterado, castagnoli))
	if _, err := decode(adulterado); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("seção inconsistente tem de ser ErrCorruptManifest, veio %v", err)
	}
}
