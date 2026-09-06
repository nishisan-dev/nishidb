package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func sampleSource() Source {
	return Source{
		Topic:     "radius-accounting",
		GroupID:   "nishidb-radius",
		ClusterID: "MkU3OEVBNTcwNTJENDM2Qk",
		Bootstrap: CanonicalBootstrap([]string{"kafka03:9092", "kafka01:9092", "kafka02:9092"}),
	}
}

func sampleOffsets() SourceOffsets {
	return SourceOffsets{
		Source:  sampleSource(),
		Offsets: map[int32]int64{2: 918273, 0: 42, 1: 0},
	}
}

func TestOffsetsRoundTrip(t *testing.T) {
	want := sampleOffsets()
	path := filepath.Join(t.TempDir(), OffsetsFileName)
	if err := WriteOffsets(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadOffsets(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip divergiu:\n tem: %+v\nquer: %+v", got, want)
	}
}

func TestOffsetsVazioRoundTrip(t *testing.T) {
	want := SourceOffsets{Source: sampleSource(), Offsets: map[int32]int64{}}
	path := filepath.Join(t.TempDir(), OffsetsFileName)
	if err := WriteOffsets(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadOffsets(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip divergiu:\n tem: %+v\nquer: %+v", got, want)
	}
}

// Dois checkpoints do mesmo estado têm de produzir arquivos idênticos byte a
// byte: um diff de dois arquivos é ferramenta de diagnóstico, e ordem de mapa
// a inutilizaria.
func TestEncodeEDeterministico(t *testing.T) {
	first := sampleOffsets().encode()
	for i := 0; i < 50; i++ {
		if got := sampleOffsets().encode(); !reflect.DeepEqual(got, first) {
			t.Fatalf("encode %d divergiu", i)
		}
	}
}

func TestOffsetsAusenteEhErrNoOffsets(t *testing.T) {
	_, err := LoadOffsets(filepath.Join(t.TempDir(), OffsetsFileName))
	if !errors.Is(err, ErrNoOffsets) {
		t.Errorf("erro = %v, quer ErrNoOffsets", err)
	}
}

func TestOffsetsCorrompidoERecusado(t *testing.T) {
	good := sampleOffsets().encode()

	cases := []struct {
		name   string
		mutate func([]byte) []byte
		term   string
	}{
		{"magic", func(b []byte) []byte { c := append([]byte(nil), b...); c[0] = 'X'; return c }, "magic"},
		{"version", func(b []byte) []byte { c := append([]byte(nil), b...); c[4] = 2; return c }, "version"},
		{"crc", func(b []byte) []byte { c := append([]byte(nil), b...); c[len(c)-1] ^= 0xff; return c }, "CRC32C"},
		{"truncado", func(b []byte) []byte { return b[:9] }, "truncado"},
		{"byte extra", func(b []byte) []byte {
			// Corpo válido com um byte a mais ANTES do CRC, e CRC recalculado:
			// só a checagem estrita de tamanho pega isto.
			c := append([]byte(nil), b[:len(b)-4]...)
			c = append(c, 0x00)
			return append(c, crcOf(c)...)
		}, "tamanho inconsistente"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := decodeOffsets(c.mutate(good))
			if !errors.Is(err, ErrCorruptOffsets) {
				t.Fatalf("erro = %v, quer ErrCorruptOffsets", err)
			}
			if !containsTerm(err.Error(), c.term) {
				t.Errorf("erro = %q, quer citar %q", err, c.term)
			}
		})
	}
}

// O arquivo é escrito pelo MESMO caminho de durabilidade do manifest: tmp →
// fsync → rename → fsync do diretório. Perder um desses passos é perder a
// atomicidade sem nenhum sintoma até o primeiro corte de energia.
func TestWriteOffsetsOrdemDeDurabilidade(t *testing.T) {
	var steps []string
	writeTrace = func(s string) { steps = append(steps, s) }
	defer func() { writeTrace = nil }()

	path := filepath.Join(t.TempDir(), OffsetsFileName)
	if err := WriteOffsets(path, sampleOffsets()); err != nil {
		t.Fatal(err)
	}
	want := []string{"write", "fsync", "rename", "dirsync"}
	if !reflect.DeepEqual(steps, want) {
		t.Errorf("passos = %v, quer %v", steps, want)
	}
	// O tmp não pode sobrar: um .tmp esquecido vira lixo que o operador
	// confunde com estado.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("sobrou o arquivo temporário")
	}
}

func TestCanonizaBootstrapIgnoraOrdemEEspaco(t *testing.T) {
	a := CanonicalBootstrap([]string{"kafka03:9092", " kafka01:9092 ", "kafka02:9092"})
	b := CanonicalBootstrap([]string{"kafka01:9092", "kafka02:9092", "kafka03:9092"})
	if a != b {
		t.Errorf("%q != %q: reordenar a lista no YAML não é troca de cluster", a, b)
	}
	if a != "kafka01:9092,kafka02:9092,kafka03:9092" {
		t.Errorf("forma canônica = %q", a)
	}
}

// "Recusei subir" sem dizer O QUE mudou obriga o operador a comparar YAML com
// binário na mão.
func TestDivergenciaNomeiaOCampo(t *testing.T) {
	base := sampleSource()
	cases := []struct {
		name   string
		mutate func(Source) Source
		want   string
	}{
		{"igual", func(o Source) Source { return o }, ""},
		{"topic", func(o Source) Source { o.Topic = "outro"; return o }, "topic"},
		{"group", func(o Source) Source { o.GroupID = "outro"; return o }, "consumer.groupId"},
		{"bootstrap", func(o Source) Source { o.Bootstrap = "outro:9092"; return o }, "bootstrapServers"},
		{"cluster", func(o Source) Source { o.ClusterID = "outro"; return o }, "cluster"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := base.Divergence(c.mutate(base)); got != c.want {
				t.Errorf("Divergencia = %q, quer %q", got, c.want)
			}
		})
	}
}

// Nem todo broker expõe o id do cluster. Recusar a subida por um dado que o
// próprio Kafka não deu seria transformar limitação de ambiente em falha de
// configuração.
func TestClusterIDVazioNaoEDivergencia(t *testing.T) {
	withID := sampleSource()
	withoutID := withID
	withoutID.ClusterID = ""
	if got := withID.Divergence(withoutID); got != "" {
		t.Errorf("Divergencia = %q, quer vazio", got)
	}
	if got := withoutID.Divergence(withID); got != "" {
		t.Errorf("Divergencia = %q, quer vazio", got)
	}
}

func TestParticoesSaemOrdenadas(t *testing.T) {
	s := SourceOffsets{Offsets: map[int32]int64{7: 1, 0: 1, 3: 1, 12: 1}}
	want := []int32{0, 3, 7, 12}
	if got := s.Partitions(); !reflect.DeepEqual(got, want) {
		t.Errorf("Particoes = %v, quer %v", got, want)
	}
}

func TestCloneNaoCompartilhaOMapa(t *testing.T) {
	orig := sampleOffsets()
	c := orig.Clone()
	c.Offsets[0] = 999
	if orig.Offsets[0] == 999 {
		t.Error("Clone compartilhou o mapa com a origem")
	}
}

func containsTerm(s, term string) bool { return strings.Contains(s, term) }

// crcOf devolve o CRC32C do corpo, no formato do arquivo. Existe para o teste
// poder forjar um arquivo com corpo válido e CRC coerente — o único jeito de
// exercitar a checagem estrita de tamanho.
func crcOf(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, castagnoli))
}
