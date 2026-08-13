package materializer

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// fieldKV é um campo materializado (id + valor) para montar payloads de
// entrada e conferir payloads de saída.
type fieldKV struct {
	id types.FieldID
	v  types.Value
}

// encodeFields monta um payload com o codificador do storage — oráculo
// INDEPENDENTE do código sob teste (nada aqui passa por encodePatch).
func encodeFields(t *testing.T, kvs ...fieldKV) []byte {
	t.Helper()
	var out []byte
	var err error
	for _, kv := range kvs {
		out, err = encoding.AppendField(out, kv.id, kv.v)
		if err != nil {
			t.Fatalf("AppendField(%d): %v", kv.id, err)
		}
	}
	return out
}

func decodePayload(t *testing.T, payload []byte) []fieldKV {
	t.Helper()
	var r encoding.FieldReader
	r.Reset(payload)
	var got []fieldKV
	for {
		id, v, ok := r.Next()
		if !ok {
			if err := r.Err(); err != nil {
				t.Fatalf("payload de saída corrompido: %v", err)
			}
			return got
		}
		got = append(got, fieldKV{id, v})
	}
}

func valueEqual(a, b types.Value) bool {
	return a.Type == b.Type && a.I64 == b.I64 && bytes.Equal(a.Bytes, b.Bytes)
}

// wantFields confere o payload campo a campo e revalida os invariantes que o
// Put exige (ordem crescente estrita, sem duplicatas, sem TypeNull): um merge
// que produza saída fora de ordem é rejeitado só na escrita, longe daqui.
func wantFields(t *testing.T, payload []byte, want ...fieldKV) {
	t.Helper()
	if _, err := encoding.ValidatePayload(payload); err != nil {
		t.Fatalf("saída viola invariantes de payload: %v", err)
	}
	got := decodePayload(t, payload)
	if len(got) != len(want) {
		t.Fatalf("payload com %d campos, want %d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i].id != want[i].id || !valueEqual(got[i].v, want[i].v) {
			t.Errorf("campo %d: got id=%d %+v, want id=%d %+v", i, got[i].id, got[i].v, want[i].id, want[i].v)
		}
	}
}

func vstr(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func vi64(n int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: n} }
func pe(id types.FieldID, v types.Value) patchEntry {
	return patchEntry{field: &schema.CompiledField{ID: id}, val: v}
}
func peNull(id types.FieldID) patchEntry {
	return patchEntry{field: &schema.CompiledField{ID: id}, isNull: true, val: types.Value{Type: types.TypeNull}}
}

func mustMerge(t *testing.T, prev []byte, patch []patchEntry) []byte {
	t.Helper()
	out, err := mergeRecords(prev, patch, nil)
	if err != nil {
		t.Fatalf("mergeRecords: %v", err)
	}
	return out
}

func mustEncode(t *testing.T, patch []patchEntry) []byte {
	t.Helper()
	out, err := encodePatch(patch, nil)
	if err != nil {
		t.Fatalf("encodePatch: %v", err)
	}
	return out
}

const spec22YAML = `
version: 1
name: spec22
materialization:
  key:
    field: id
  sequence:
    field: _SEQ
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    status:
      type: string
    counter:
      type: int64
`

// entriesOf monta as entries de um evento pelo caminho REAL (PatchBuffer) e
// devolve também o payload materializado equivalente.
func entriesOf(t *testing.T, c *schema.Compiled, event string) ([]patchEntry, []byte) {
	t.Helper()
	b := parsePatch(t, c, event)
	kvs := make([]fieldKV, 0, len(b.entries))
	for _, e := range b.entries {
		if e.isNull {
			continue
		}
		kvs = append(kvs, fieldKV{e.field.ID, e.val})
	}
	return b.entries, encodeFields(t, kvs...)
}

// Exemplo literal da spec §22: o patch traz id/_SEQ/counter e o resultado
// precisa preservar status, que o patch não menciona.
func TestMergeSpec22Exemplo(t *testing.T) {
	c := compileYAML(t, spec22YAML)
	fid := func(name string) types.FieldID {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não compilado", name)
		}
		return f.ID
	}
	_, state := entriesOf(t, c, `{"id":"ABC","_SEQ":100,"status":"Start","counter":5000}`)
	patch, _ := entriesOf(t, c, `{"id":"ABC","_SEQ":101,"counter":9000}`)

	got := mustMerge(t, state, patch)

	want := []fieldKV{
		{fid("_SEQ"), vi64(101)},
		{fid("counter"), vi64(9000)},
		{fid("id"), vstr("ABC")},
		{fid("status"), vstr("Start")}, // preservado: ausente do patch
	}
	// A ordem esperada é a crescente de FieldID; o YAML atribui IDs por nome
	// ordenado (_SEQ, counter, id, status), então os campos do patch e o
	// preservado ficam INTERCALADOS — é o caso que pega off-by-one.
	wantFields(t, got, want...)
}

func TestMergeCampoAusenteMantemAnterior(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vstr("mantido")}, fieldKV{3, vi64(30)})
	got := mustMerge(t, prev, []patchEntry{pe(1, vi64(11)), pe(3, vi64(33))})
	wantFields(t, got, fieldKV{1, vi64(11)}, fieldKV{2, vstr("mantido")}, fieldKV{3, vi64(33)})
}

// D2/§24: null explícito remove o campo, e um patch POSTERIOR sem o campo
// mantém a ausência — não existe terceiro estado.
func TestMergeNullRemoveCampoEAusenciaEhTransitiva(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vstr("some")}, fieldKV{3, vi64(30)})

	semCampo2 := mustMerge(t, prev, []patchEntry{peNull(2)})
	wantFields(t, semCampo2, fieldKV{1, vi64(10)}, fieldKV{3, vi64(30)})

	// Segundo patch não menciona o campo 2: continua ausente.
	depois := mustMerge(t, semCampo2, []patchEntry{pe(1, vi64(11))})
	wantFields(t, depois, fieldKV{1, vi64(11)}, fieldKV{3, vi64(30)})
}

// null de campo que já não existe é no-op (não pode virar campo com TypeNull).
func TestMergeNullDeCampoInexistente(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)})
	got := mustMerge(t, prev, []patchEntry{peNull(5)})
	wantFields(t, got, fieldKV{1, vi64(10)})
}

func TestMergeSoCamposNovosNoFim(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vi64(20)})
	got := mustMerge(t, prev, []patchEntry{pe(7, vstr("novo")), pe(9, vi64(90))})
	wantFields(t, got, fieldKV{1, vi64(10)}, fieldKV{2, vi64(20)}, fieldKV{7, vstr("novo")}, fieldKV{9, vi64(90)})
}

func TestMergeSoCamposNovosNoInicio(t *testing.T) {
	prev := encodeFields(t, fieldKV{7, vi64(70)}, fieldKV{9, vi64(90)})
	got := mustMerge(t, prev, []patchEntry{pe(1, vstr("novo")), pe(2, vi64(20))})
	wantFields(t, got, fieldKV{1, vstr("novo")}, fieldKV{2, vi64(20)}, fieldKV{7, vi64(70)}, fieldKV{9, vi64(90)})
}

func TestMergeCamposIntercalados(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(1)}, fieldKV{3, vi64(3)}, fieldKV{5, vi64(5)}, fieldKV{7, vi64(7)})
	patch := []patchEntry{pe(2, vi64(20)), pe(3, vi64(30)), pe(6, vi64(60)), pe(8, vi64(80))}
	got := mustMerge(t, prev, patch)
	wantFields(t, got,
		fieldKV{1, vi64(1)}, fieldKV{2, vi64(20)}, fieldKV{3, vi64(30)}, fieldKV{5, vi64(5)},
		fieldKV{6, vi64(60)}, fieldKV{7, vi64(7)}, fieldKV{8, vi64(80)})
}

// prev vazio é o create pelo caminho do merge: só o patch entra.
func TestMergePrevVazio(t *testing.T) {
	got := mustMerge(t, nil, []patchEntry{pe(1, vi64(1)), peNull(2), pe(3, vstr("x"))})
	wantFields(t, got, fieldKV{1, vi64(1)}, fieldKV{3, vstr("x")})
}

// patch vazio preserva o estado byte a byte.
func TestMergePatchVazio(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{4, vstr("z")})
	got := mustMerge(t, prev, nil)
	wantFields(t, got, fieldKV{1, vi64(10)}, fieldKV{4, vstr("z")})
	if !bytes.Equal(got, prev) {
		t.Errorf("patch vazio deveria copiar o payload byte a byte: got %x, want %x", got, prev)
	}
}

// patch que anula TODOS os campos do prev produz payload vazio — legítimo
// (entidade sem nenhum campo além da key, que vive no record, não no payload).
func TestMergeTodosOsCamposAnulados(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vi64(20)})
	got := mustMerge(t, prev, []patchEntry{peNull(1), peNull(2)})
	wantFields(t, got)
	if len(got) != 0 {
		t.Errorf("payload = %x, want vazio", got)
	}
}

// os 11 tipos do wire format sobrevivem ao round-trip do merge: os do prev
// pela cópia crua, os do patch pela codificação.
func TestMergeTodosOsTiposRoundTrip(t *testing.T) {
	v := func(tp types.FieldType, i int64, b []byte) types.Value {
		return types.Value{Type: tp, I64: i, Bytes: b}
	}
	todos := []fieldKV{
		{1, vi64(-42)},
		{2, v(types.TypeDouble, int64(math.Float64bits(-3.5)), nil)},
		{3, v(types.TypeBool, 1, nil)},
		{4, v(types.TypeTimestamp, 1786399690000, nil)},
		{5, v(types.TypeIPv4, 0, []byte{10, 0, 0, 1})},
		{6, v(types.TypeIPv6, 0, bytes.Repeat([]byte{0xAB}, 16))},
		{7, v(types.TypeIPv4Prefix, 0, []byte{192, 168, 0, 0, 24})},
		{8, v(types.TypeIPv6Prefix, 0, append(bytes.Repeat([]byte{0x28}, 16), 64))},
		{9, vstr("")}, // string vazia: varlen com LENGTH 0
		{10, v(types.TypeBytes, 0, []byte{0x00, 0xFF, 0x7F})},
		{11, v(types.TypeJSON, 0, []byte(`{"a":[1,2]}`))},
	}

	// Metade vem do prev (cópia crua), metade do patch (codificação).
	var doPrev []fieldKV
	var patch []patchEntry
	for i, kv := range todos {
		if i%2 == 0 {
			doPrev = append(doPrev, kv)
		} else {
			patch = append(patch, pe(kv.id, kv.v))
		}
	}
	got := mustMerge(t, encodeFields(t, doPrev...), patch)
	wantFields(t, got, todos...)

	// E o inverso: os mesmos campos, agora todos sobrescritos pelo patch.
	todosPatch := make([]patchEntry, 0, len(todos))
	for _, kv := range todos {
		todosPatch = append(todosPatch, pe(kv.id, kv.v))
	}
	got2 := mustMerge(t, encodeFields(t, doPrev...), todosPatch)
	wantFields(t, got2, todos...)
}

// O buffer de saída é reutilizado entre merges: o segundo resultado não pode
// carregar resíduo do primeiro nem apontar para dentro do prev.
func TestMergeReutilizaBufferDeSaida(t *testing.T) {
	prev1 := encodeFields(t, fieldKV{1, vstr("primeiro-payload-longo")}, fieldKV{2, vi64(1)})
	out, err := mergeRecords(prev1, []patchEntry{pe(3, vi64(3))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	prev2 := encodeFields(t, fieldKV{4, vi64(4)})
	out2, err := mergeRecords(prev2, []patchEntry{pe(5, vi64(5))}, out[:0])
	if err != nil {
		t.Fatal(err)
	}
	wantFields(t, out2, fieldKV{4, vi64(4)}, fieldKV{5, vi64(5)})
}

// Trava a pré-condição de aliasing documentada no mergeRecords: buffers de
// entrada e saída são DISTINTOS no Apply, e este teste registra o que o
// contrato garante — o resultado é o mesmo do caso normal quando os arrays
// são separados, e o prev permanece intacto (o merge não escreve nele).
func TestMergeNaoEscreveNoPrev(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vstr("original")}, fieldKV{5, vi64(50)})
	copia := append([]byte(nil), prev...)

	out := make([]byte, 0, 256)
	got, err := mergeRecords(prev, []patchEntry{pe(2, vstr("novo")), pe(9, vi64(90))}, out)
	if err != nil {
		t.Fatal(err)
	}
	wantFields(t, got, fieldKV{1, vi64(10)}, fieldKV{2, vstr("novo")}, fieldKV{5, vi64(50)}, fieldKV{9, vi64(90)})

	if !bytes.Equal(prev, copia) {
		t.Errorf("o merge alterou o payload anterior:\n got  %x\n want %x", prev, copia)
	}
	// E a saída não pode ser uma janela sobre o prev: o Put recebe os dois e
	// o prev precisa sobreviver ao diff de índices.
	if len(got) > 0 && len(prev) > 0 && &got[0] == &prev[0] {
		t.Error("a saída compartilha o array do prev")
	}
}

func TestEncodePatchSoCamposDoPatch(t *testing.T) {
	got := mustEncode(t, []patchEntry{pe(1, vi64(1)), peNull(2), pe(3, vstr("x"))})
	wantFields(t, got, fieldKV{1, vi64(1)}, fieldKV{3, vstr("x")})
}

func TestEncodePatchVazio(t *testing.T) {
	got := mustEncode(t, nil)
	wantFields(t, got)
	if len(got) != 0 {
		t.Errorf("payload = %x, want vazio", got)
	}
}

// Erros de estado: nenhum deles é culpa do evento, então carregam
// ErrInternalState — a categoria que o Apply contabiliza separado das 5
// categorias §101.
func TestMergePrevCorrompido(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vstr("abc")})
	truncado := prev[:len(prev)-2] // corta o valor da string

	_, err := mergeRecords(truncado, []patchEntry{pe(9, vi64(9))}, nil)
	if err == nil {
		t.Fatal("merge sobre payload truncado deveria falhar")
	}
	if !errors.Is(err, ErrInternalState) {
		t.Errorf("erro = %v, want ErrInternalState", err)
	}
	if !errors.Is(err, encoding.ErrCorrupt) {
		t.Errorf("erro = %v, want encoding.ErrCorrupt preservado na cadeia", err)
	}
	if !strings.Contains(err.Error(), "estado anterior") {
		t.Errorf("mensagem = %q, want menção ao estado anterior", err)
	}
}

// Tag de tipo inválida no meio do prev: o FieldReader zera o span do campo
// corrente no erro, então nem meio campo pode vazar para a saída.
func TestMergePrevComTagInvalida(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)}, fieldKV{2, vi64(20)})
	prev[11+2] = 0x00 // TYPE do segundo campo vira TypeNull (proibido em payload)

	_, err := mergeRecords(prev, nil, nil)
	if !errors.Is(err, ErrInternalState) || !errors.Is(err, encoding.ErrCorrupt) {
		t.Fatalf("erro = %v, want ErrInternalState + encoding.ErrCorrupt", err)
	}
}

func TestMergePatchForaDeOrdem(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)})
	_, err := mergeRecords(prev, []patchEntry{pe(5, vi64(5)), pe(3, vi64(3))}, nil)
	if !errors.Is(err, ErrInternalState) {
		t.Fatalf("erro = %v, want ErrInternalState", err)
	}
	if !strings.Contains(err.Error(), "fora de ordem") {
		t.Errorf("mensagem = %q, want menção a fora de ordem", err)
	}
}

func TestMergePatchComIDDuplicado(t *testing.T) {
	_, err := mergeRecords(nil, []patchEntry{pe(3, vi64(3)), pe(3, vi64(4))}, nil)
	if !errors.Is(err, ErrInternalState) {
		t.Fatalf("erro = %v, want ErrInternalState (duplicata quebra a ordem estrita)", err)
	}
}

// FieldID 0 é reservado pelo dicionário (§32): o merge o recusa em vez de
// gravar um campo que nenhum nome resolve.
func TestMergePatchComIDReservado(t *testing.T) {
	_, err := mergeRecords(nil, []patchEntry{pe(0, vi64(1))}, nil)
	if !errors.Is(err, ErrInternalState) {
		t.Fatalf("erro = %v, want ErrInternalState", err)
	}
}

func TestEncodePatchForaDeOrdem(t *testing.T) {
	_, err := encodePatch([]patchEntry{pe(5, vi64(5)), pe(3, vi64(3))}, nil)
	if !errors.Is(err, ErrInternalState) {
		t.Fatalf("erro = %v, want ErrInternalState", err)
	}
}

// Value inválida vinda do patch (largura fixa errada) é erro do codificador e
// não pode ser confundida com evento ruim.
func TestEncodePatchValueInvalida(t *testing.T) {
	_, err := encodePatch([]patchEntry{pe(1, types.Value{Type: types.TypeIPv4, Bytes: []byte{1, 2}})}, nil)
	if !errors.Is(err, encoding.ErrInvalidPayload) {
		t.Fatalf("erro = %v, want encoding.ErrInvalidPayload", err)
	}
}

// O mesmo pelo caminho do MERGE: engolir o erro do codificador deixaria a
// saída sem o campo — um estado silenciosamente incompleto, que é pior que a
// falha, porque o Put aceitaria o payload truncado sem reclamar.
func TestMergeAppendFieldErroPropaga(t *testing.T) {
	prev := encodeFields(t, fieldKV{1, vi64(10)})
	inválida := types.Value{Type: types.TypeIPv4, Bytes: []byte{1, 2}} // ipv4 exige 4 bytes

	_, err := mergeRecords(prev, []patchEntry{pe(2, inválida)}, nil)
	if !errors.Is(err, encoding.ErrInvalidPayload) {
		t.Fatalf("erro = %v, want encoding.ErrInvalidPayload", err)
	}
	if !strings.Contains(err.Error(), "exige 4 bytes") {
		t.Errorf("mensagem = %q, want identificar a largura esperada", err)
	}

	// E também quando o campo inválido sobrescreve um do prev (o ramo em que o
	// cursor do prev já avançou).
	if _, err := mergeRecords(prev, []patchEntry{pe(1, inválida)}, nil); !errors.Is(err, encoding.ErrInvalidPayload) {
		t.Errorf("sobrescrita com Value inválida: erro = %v, want encoding.ErrInvalidPayload", err)
	}
}
