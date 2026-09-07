package fielddict

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// O requisito que justifica a tarefa: o tipo fixado de um campo auto precisa
// SOBREVIVER a restart. Sem isso, o processo reiniciado se comporta como se o
// campo nunca tivesse sido observado e o binder recusa comparação num campo
// que tem dados.
func TestTypeHintPersisteEntreAberturas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := mustCreate(t, d, "X-Attr")
	other := mustCreate(t, d, "Y-Attr")
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	got, ok := reopened.TypeHint(id)
	if !ok || got != types.TypeInt64 {
		t.Errorf("TypeHint após reabrir = %v/%v, want int64/true", got, ok)
	}
	// Campo sem hint continua sem hint — "não fixado" precisa ser
	// distinguível de "fixado como algo".
	if got, ok := reopened.TypeHint(other); ok {
		t.Errorf("campo sem hint devolveu %v/true", got)
	}
	// Os nomes seguem íntegros: o registro de hint não pode confundir o scan.
	if n, ok := reopened.Name(id); !ok || n != "X-Attr" {
		t.Errorf("Name(%d) = %q/%v", id, n, ok)
	}
	if reopened.Len() != 2 {
		t.Errorf("Len = %d, want 2", reopened.Len())
	}
}

func TestSetTypeHintIdempotente(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")

	if err := d.SetTypeHint(id, types.TypeString); err != nil {
		t.Fatal(err)
	}
	size := d.SizeBytes()
	for i := 0; i < 3; i++ {
		if err := d.SetTypeHint(id, types.TypeString); err != nil {
			t.Fatalf("repetição %d: %v", i, err)
		}
	}
	if d.SizeBytes() != size {
		t.Errorf("SizeBytes = %d após repetições, want %d (nada deveria ser escrito)",
			d.SizeBytes(), size)
	}
}

// R11 na prática: o tipo é decidido na primeira observação e não muda.
func TestSetTypeHintConflito(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "Connect-Info")
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}

	err := d.SetTypeHint(id, types.TypeString)
	if !errors.Is(err, ErrTypeHintConflict) {
		t.Fatalf("erro = %v, want ErrTypeHintConflict", err)
	}
	// A mensagem precisa dizer QUAL campo e QUAIS tipos: é o que o operador
	// lê no log de ingestão para descobrir que a fonte mudou de formato.
	for _, snippet := range []string{"Connect-Info", "int64", "string"} {
		if !strings.Contains(err.Error(), snippet) {
			t.Errorf("mensagem = %q, want conter %q", err, snippet)
		}
	}
	// O hint anterior PERMANECE: a tentativa falha, não corrompe.
	if got, ok := d.TypeHint(id); !ok || got != types.TypeInt64 {
		t.Errorf("TypeHint após conflito = %v/%v, want int64/true", got, ok)
	}
}

func TestSetTypeHintRecusaNullEIdInvalido(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")

	cases := []struct {
		name string
		id   types.FieldID
		t    types.FieldType
		msg  string
	}{
		{"tipo nulo", id, types.TypeNull, "nulo"},
		{"id zero", 0, types.TypeInt64, "inexistente"},
		{"id além do fim", types.FieldID(d.Len() + 5), types.TypeInt64, "inexistente"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := d.SetTypeHint(tc.id, tc.t)
			if err == nil {
				t.Fatal("erro esperado")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// Mesma invariante do nome (§14.6): o hint é durável ANTES de ser publicado
// em memória. Invertido, um crash entre a publicação e o fsync deixaria
// records duráveis apontando para um tipo que o dicionário não conhece.
func TestTypeHintFsyncAntesDeRetornar(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")

	rec := &orderRecorder{}
	// visibleAtSync captura se o hint JÁ estava publicado em memória no
	// instante do fsync. Precisa ser false: publicar antes de durar deixaria
	// uma janela em que o writer aceita records referenciando um tipo que uma
	// reabertura não conhece — e essa inversão não aparece na ordem
	// WriteAt/Sync, que continuaria idêntica.
	var visibleAtSync bool
	d.WrapFileForTest(func(f dictFile) dictFile {
		rec.inner = &syncProbe{inner: f, beforeSync: func() {
			_, visibleAtSync = d.TypeHint(id)
		}}
		return rec
	})
	if err := d.SetTypeHint(id, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade = %v, want %v", rec.calls, want)
	}
	if visibleAtSync {
		t.Error("o hint foi publicado em memória ANTES do fsync: um crash na janela deixaria " +
			"records duráveis apontando para um tipo que o dicionário não conhece")
	}
	if got, ok := d.TypeHint(id); !ok || got != types.TypeTimestamp {
		t.Errorf("TypeHint após retornar = %v/%v, want timestamp/true", got, ok)
	}

	// Idempotente não toca no arquivo.
	rec.calls = nil
	if err := d.SetTypeHint(id, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Errorf("SetTypeHint idempotente tocou o arquivo: %v", rec.calls)
	}
}

// syncProbe roda um gancho no INSTANTE do fsync, para observar o estado em
// memória no meio da operação — a ordem WriteAt/Sync sozinha não distingue
// "publicou antes" de "publicou depois".
type syncProbe struct {
	inner      dictFile
	beforeSync func()
}

func (p *syncProbe) WriteAt(b []byte, off int64) (int, error) { return p.inner.WriteAt(b, off) }
func (p *syncProbe) Sync() error {
	p.beforeSync()
	return p.inner.Sync()
}
func (p *syncProbe) Truncate(size int64) error { return p.inner.Truncate(size) }
func (p *syncProbe) Close() error              { return p.inner.Close() }

// O teste da decisão de formato (QP-D1): um hint rasgado no tail não pode
// derrubar campo nenhum. É por isso que o hint é registro append-only e não
// reescrita in-place do byte da entry — in-place, o rasgo cairia no MEIO do
// arquivo e o Open apagaria todos os campos posteriores.
func TestTailRasgadoDeHintNaoDerrubaCampos(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []types.FieldID
	for _, name := range []string{"a", "b", "c", "d"} {
		ids = append(ids, mustCreate(t, d, name))
	}
	if err := d.SetTypeHint(ids[0], types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	if err := d.SetTypeHint(ids[1], types.TypeString); err != nil {
		t.Fatal(err)
	}
	validSize := d.SizeBytes()
	if err := d.SetTypeHint(ids[2], types.TypeBool); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Rasga o ÚLTIMO registro de hint pela metade.
	if err := os.Truncate(path, validSize+hintRecordSize/2); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	// Todos os campos sobrevivem — é o ponto.
	if reopened.Len() != 4 {
		t.Fatalf("Len = %d após tail rasgado, want 4 (nenhum campo pode se perder)", reopened.Len())
	}
	for i, name := range []string{"a", "b", "c", "d"} {
		if n, ok := reopened.Name(ids[i]); !ok || n != name {
			t.Errorf("Name(%d) = %q/%v, want %q", ids[i], n, ok, name)
		}
	}
	// Os hints anteriores ao rasgo sobrevivem; o rasgado se perde e o campo
	// volta a fixar o tipo na próxima observação.
	if got, ok := reopened.TypeHint(ids[0]); !ok || got != types.TypeInt64 {
		t.Errorf("hint de a = %v/%v, want int64/true", got, ok)
	}
	if got, ok := reopened.TypeHint(ids[1]); !ok || got != types.TypeString {
		t.Errorf("hint de b = %v/%v, want string/true", got, ok)
	}
	if _, ok := reopened.TypeHint(ids[2]); ok {
		t.Error("hint rasgado sobreviveu: o registro não foi truncado")
	}
	// E o arquivo volta a aceitar escrita a partir do ponto truncado.
	if err := reopened.SetTypeHint(ids[2], types.TypeBool); err != nil {
		t.Fatalf("dicionário inutilizável após truncate: %v", err)
	}
}

// Semântica append-only: o ÚLTIMO registro de hint de um campo vence. Só
// acontece com arquivo montado à mão (o SetTypeHint recusa conflito), mas o
// scan precisa ser determinístico.
func TestUltimoHintVence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := mustCreate(t, d, "X")
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	size := d.SizeBytes()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Segundo registro, escrito à mão, com tipo diferente.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var rec [hintRecordSize]byte
	binary.LittleEndian.PutUint16(rec[0:2], 0)
	binary.LittleEndian.PutUint16(rec[2:4], uint16(id))
	rec[4] = byte(types.TypeString)
	binary.LittleEndian.PutUint32(rec[5:9], crc32.Checksum(rec[:5], castagnoli))
	if _, err := f.WriteAt(rec[:], size); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.TypeHint(id); !ok || got != types.TypeString {
		t.Errorf("TypeHint = %v/%v, want string/true (o último registro vence)", got, ok)
	}
}

// QP-D2: v1 é recusado, sem caminho de migração. O M1 não foi lançado e a
// regra do projeto é aplicar o definitivo sem fallback para legado —
// datasets de pré-prod anteriores precisam ser recriados.
func TestVersaoV1Recusada(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, "X")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{1}, 4); err != nil { // version = 1
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("v1 deveria ser recusado")
	}
	// Quem lê a mensagem é um operador com o processo caído: sem o arquivo,
	// as duas versões e a saída, ele não tem o que fazer com a informação.
	for _, snippet := range []string{path, "version 1", "esperada 2", "recrie o dataset"} {
		if !strings.Contains(err.Error(), snippet) {
			t.Errorf("erro = %q, want conter %q", err, snippet)
		}
	}
}

// Golden de BYTES do formato v2. Round-trip não basta para um formato
// persistente: leitor e escritor mutados da mesma maneira concordam entre si,
// a suíte fica verde, e o arquivo produzido — com o MESMO byte de version —
// é truncado pelo leitor canônico, apagando o dicionário de um dataset cujos
// fieldIDs já estão em records duráveis.
//
// Os CRCs abaixo foram calculados a partir do layout DOCUMENTADO, não do que
// o código produz. Se este teste falhar, ou o formato mudou de propósito (e
// então a version tem de mudar junto) ou uma mutação o corrompeu.
//
// Decodificação byte a byte do esperado:
//
//	4e 44 42 44   magic "NDBD"
//	02            version 2
//	00 00 00      reservado
//	01 00         nameLen = 1
//	00            typeHint da entry = 0 (não declarado ao nascer)
//	58            nome "X"
//	63 63 de 4e   crc32c sobre nameLen+typeHint+nome (01 00 00 58)
//	00 00         discriminador de registro de hint (nameLen impossível)
//	01 00         fieldID 1
//	01            tipo fixado = int64
//	48 67 58 12   crc32c sobre os 5 bytes anteriores
func TestGoldenBytesV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id := mustCreate(t, d, "X")
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	want := []byte{
		0x4e, 0x44, 0x42, 0x44, 0x02, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x58, 0x63, 0x63, 0xde, 0x4e,
		0x00, 0x00, 0x01, 0x00, 0x01, 0x48, 0x67, 0x58, 0x12,
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("bytes do fields.dict v2:\n got % x\nwant % x", got, want)
	}
}

// O hint não pode confundir o cálculo de tamanho usado pelo manifest.
func TestSizeBytesContabilizaHint(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")
	before := d.SizeBytes()
	if err := d.SetTypeHint(id, types.TypeDouble); err != nil {
		t.Fatal(err)
	}
	if got := d.SizeBytes() - before; got != hintRecordSize {
		t.Errorf("crescimento = %d bytes, want %d", got, hintRecordSize)
	}
}
