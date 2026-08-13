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
	outro := mustCreate(t, d, "Y-Attr")
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reaberto, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reaberto.Close()

	got, ok := reaberto.TypeHint(id)
	if !ok || got != types.TypeInt64 {
		t.Errorf("TypeHint após reabrir = %v/%v, want int64/true", got, ok)
	}
	// Campo sem hint continua sem hint — "não fixado" precisa ser
	// distinguível de "fixado como algo".
	if got, ok := reaberto.TypeHint(outro); ok {
		t.Errorf("campo sem hint devolveu %v/true", got)
	}
	// Os nomes seguem íntegros: o registro de hint não pode confundir o scan.
	if n, ok := reaberto.Name(id); !ok || n != "X-Attr" {
		t.Errorf("Name(%d) = %q/%v", id, n, ok)
	}
	if reaberto.Len() != 2 {
		t.Errorf("Len = %d, want 2", reaberto.Len())
	}
}

func TestSetTypeHintIdempotente(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")

	if err := d.SetTypeHint(id, types.TypeString); err != nil {
		t.Fatal(err)
	}
	tamanho := d.SizeBytes()
	for i := 0; i < 3; i++ {
		if err := d.SetTypeHint(id, types.TypeString); err != nil {
			t.Fatalf("repetição %d: %v", i, err)
		}
	}
	if d.SizeBytes() != tamanho {
		t.Errorf("SizeBytes = %d após repetições, want %d (nada deveria ser escrito)",
			d.SizeBytes(), tamanho)
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
	for _, trecho := range []string{"Connect-Info", "int64", "string"} {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("mensagem = %q, want conter %q", err, trecho)
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

	casos := []struct {
		nome string
		id   types.FieldID
		t    types.FieldType
		msg  string
	}{
		{"tipo nulo", id, types.TypeNull, "nulo"},
		{"id zero", 0, types.TypeInt64, "inexistente"},
		{"id além do fim", types.FieldID(d.Len() + 5), types.TypeInt64, "inexistente"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
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
	// visívelNoSync captura se o hint JÁ estava publicado em memória no
	// instante do fsync. Precisa ser false: publicar antes de durar deixaria
	// uma janela em que o writer aceita records referenciando um tipo que uma
	// reabertura não conhece — e essa inversão não aparece na ordem
	// WriteAt/Sync, que continuaria idêntica.
	var visívelNoSync bool
	d.WrapFileForTest(func(f dictFile) dictFile {
		rec.inner = &syncProbe{inner: f, antes: func() {
			_, visívelNoSync = d.TypeHint(id)
		}}
		return rec
	})
	if err := d.SetTypeHint(id, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade = %v, want %v", rec.calls, want)
	}
	if visívelNoSync {
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
	inner dictFile
	antes func()
}

func (p *syncProbe) WriteAt(b []byte, off int64) (int, error) { return p.inner.WriteAt(b, off) }
func (p *syncProbe) Sync() error {
	p.antes()
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
	for _, nome := range []string{"a", "b", "c", "d"} {
		ids = append(ids, mustCreate(t, d, nome))
	}
	if err := d.SetTypeHint(ids[0], types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	if err := d.SetTypeHint(ids[1], types.TypeString); err != nil {
		t.Fatal(err)
	}
	tamanhoBom := d.SizeBytes()
	if err := d.SetTypeHint(ids[2], types.TypeBool); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Rasga o ÚLTIMO registro de hint pela metade.
	if err := os.Truncate(path, tamanhoBom+hintRecordSize/2); err != nil {
		t.Fatal(err)
	}

	reaberto, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reaberto.Close()

	// Todos os campos sobrevivem — é o ponto.
	if reaberto.Len() != 4 {
		t.Fatalf("Len = %d após tail rasgado, want 4 (nenhum campo pode se perder)", reaberto.Len())
	}
	for i, nome := range []string{"a", "b", "c", "d"} {
		if n, ok := reaberto.Name(ids[i]); !ok || n != nome {
			t.Errorf("Name(%d) = %q/%v, want %q", ids[i], n, ok, nome)
		}
	}
	// Os hints anteriores ao rasgo sobrevivem; o rasgado se perde e o campo
	// volta a fixar o tipo na próxima observação.
	if got, ok := reaberto.TypeHint(ids[0]); !ok || got != types.TypeInt64 {
		t.Errorf("hint de a = %v/%v, want int64/true", got, ok)
	}
	if got, ok := reaberto.TypeHint(ids[1]); !ok || got != types.TypeString {
		t.Errorf("hint de b = %v/%v, want string/true", got, ok)
	}
	if _, ok := reaberto.TypeHint(ids[2]); ok {
		t.Error("hint rasgado sobreviveu: o registro não foi truncado")
	}
	// E o arquivo volta a aceitar escrita a partir do ponto truncado.
	if err := reaberto.SetTypeHint(ids[2], types.TypeBool); err != nil {
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
	tamanho := d.SizeBytes()
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
	if _, err := f.WriteAt(rec[:], tamanho); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reaberto, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reaberto.Close()
	if got, ok := reaberto.TypeHint(id); !ok || got != types.TypeString {
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
	if !strings.Contains(err.Error(), "version desconhecida") {
		t.Errorf("erro = %q, want conter 'version desconhecida'", err)
	}
}

// O hint não pode confundir o cálculo de tamanho usado pelo manifest.
func TestSizeBytesContabilizaHint(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "X")
	antes := d.SizeBytes()
	if err := d.SetTypeHint(id, types.TypeDouble); err != nil {
		t.Fatal(err)
	}
	if got := d.SizeBytes() - antes; got != hintRecordSize {
		t.Errorf("crescimento = %d bytes, want %d", got, hintRecordSize)
	}
}
