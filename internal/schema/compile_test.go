package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// fakeDict atribui IDs sequenciais na ordem das chamadas (como o fielddict
// real do storage) e registra a ordem para asserções de determinismo.
type fakeDict struct {
	ids   map[string]types.FieldID
	order []string
	next  types.FieldID
}

func newFakeDict() *fakeDict {
	return &fakeDict{ids: map[string]types.FieldID{}, next: 1}
}

func (d *fakeDict) LookupOrCreate(name []byte) (types.FieldID, error) {
	if id, ok := d.ids[string(name)]; ok {
		return id, nil
	}
	id := d.next
	d.next++
	d.ids[string(name)] = id
	d.order = append(d.order, string(name))
	return id, nil
}

func compileBase(t *testing.T) (*Compiled, *fakeDict) {
	t.Helper()
	s := parseOK(t, baseYAML)
	dict := newFakeDict()
	c, err := s.Compile(dict)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return c, dict
}

func TestCompileBasics(t *testing.T) {
	c, _ := compileBase(t)
	if c.Name != "radius_sessions" || c.Mode != ModeMerge || !c.IncludeAll {
		t.Errorf("Name=%q Mode=%v IncludeAll=%v", c.Name, c.Mode, c.IncludeAll)
	}
	if c.Unknown != UnknownInclude {
		t.Errorf("Unknown = %v", c.Unknown)
	}
	if c.MaxRows != 50000 {
		t.Errorf("MaxRows = %d", c.MaxRows)
	}
}

func TestCompileFieldLookupByNameAndAlias(t *testing.T) {
	c, _ := compileBase(t)
	byName, ok1 := c.FieldByName([]byte("Acct-Status-Type"))
	byAlias, ok2 := c.FieldByName([]byte("status"))
	if !ok1 || !ok2 || byName != byAlias {
		t.Fatalf("nome e alias devem resolver para o mesmo campo: %p vs %p", byName, byAlias)
	}
	if byName.Index != IndexBitmap || byName.Type != types.TypeString {
		t.Errorf("campo = %+v", byName)
	}
	if _, ok := c.FieldByName([]byte("inexistente")); ok {
		t.Error("campo inexistente não deveria resolver")
	}
}

func TestCompileRoleFlags(t *testing.T) {
	c, _ := compileBase(t)
	key, _ := c.FieldByName([]byte("id"))
	if !key.IsKey || c.Key != key {
		t.Errorf("Key: %+v, c.Key=%p", key, c.Key)
	}
	if alias, _ := c.FieldByName([]byte("service_id")); alias != key {
		t.Error("alias da key deve resolver para o mesmo campo")
	}
	seq, _ := c.FieldByName([]byte("_SEQ"))
	if !seq.IsSequence || c.Seq != seq {
		t.Errorf("Seq: %+v", seq)
	}
	ttl, _ := c.FieldByName([]byte("EventExpireDate"))
	if !ttl.IsTTL || c.TTL != ttl {
		t.Errorf("TTL: %+v", ttl)
	}
}

func TestCompileFieldByID(t *testing.T) {
	c, _ := compileBase(t)
	f, _ := c.FieldByName([]byte("NAS-Identifier"))
	got, ok := c.FieldByID(f.ID)
	if !ok || got != f {
		t.Errorf("FieldByID(%d) = %v, %v", f.ID, got, ok)
	}
	if _, ok := c.FieldByID(9999); ok {
		t.Error("ID inexistente não deveria resolver")
	}
}

func TestCompileExcluded(t *testing.T) {
	c, _ := compileBase(t)
	if !c.Excluded([]byte("debug")) {
		t.Error("debug está no exclude")
	}
	if c.Excluded([]byte("id")) {
		t.Error("id não está no exclude")
	}
}

// excludedYAML exclui um campo DECLARADO e com alias — o caso que expõe se a
// exclusão vale só para o nome.
func excludedYAML(t *testing.T) string {
	t.Helper()
	return strings.Replace(baseYAML, "  exclude:\n    - debug\n",
		"  exclude:\n    - debug\n    - Acct-Status-Type\n", 1)
}

// Campo excluído é INEXISTENTE para o runtime: não é compilado, não resolve
// por nome nem por alias e não consome FieldID persistente do dicionário.
// Antes ele era compilado e registrado em byName pelo alias, então o evento
// que trouxesse "status" materializava o campo que o operador excluiu.
func TestCompileExcludedFieldIsNotCompiled(t *testing.T) {
	s := parseOK(t, excludedYAML(t))
	dict := newFakeDict()
	c, err := s.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.FieldByName([]byte("Acct-Status-Type")); ok {
		t.Error("campo excluído não deveria resolver por nome")
	}
	if _, ok := c.FieldByName([]byte("status")); ok {
		t.Error("campo excluído não deveria resolver pelo alias")
	}
	if !c.Excluded([]byte("status")) {
		t.Error("Excluded deveria valer para o alias do campo excluído")
	}
	for _, name := range dict.order {
		if name == "Acct-Status-Type" {
			t.Error("campo excluído não deveria consumir FieldID do dicionário")
		}
	}
}

// A porta dos fundos: com unknownFields include, o campo excluído chega ao
// ResolveUnknown (já não está em byName) e não pode ser registrado ali —
// nem pelo nome, nem pelo alias.
func TestResolveUnknownRespectsExclude(t *testing.T) {
	s := parseOK(t, excludedYAML(t))
	dict := newFakeDict()
	c, err := s.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	before := len(dict.order)
	for _, name := range []string{"Acct-Status-Type", "status", "debug"} {
		f, err := c.ResolveUnknown([]byte(name))
		if err != nil || f != nil {
			t.Errorf("ResolveUnknown(%q) = %v, %v; want nil, nil", name, f, err)
		}
	}
	if len(dict.order) != before {
		t.Errorf("campo excluído registrou no dicionário: %v", dict.order[before:])
	}
}

// Sob policy reject, campo excluído é descartado em silêncio: o operador já
// declarou o que fazer com ele. Rejeitar o evento inteiro seria o oposto.
func TestResolveUnknownExcludeBeatsReject(t *testing.T) {
	yml := strings.Replace(excludedYAML(t), "unknownFields: include", "unknownFields: reject", 1)
	s := parseOK(t, yml)
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if f, err := c.ResolveUnknown([]byte("Acct-Status-Type")); err != nil || f != nil {
		t.Errorf("excluído sob reject = %v, %v; want nil, nil", f, err)
	}
	if _, err := c.ResolveUnknown([]byte("X-Vendor")); !errors.Is(err, ErrUnknownField) {
		t.Errorf("campo realmente desconhecido = %v, want ErrUnknownField", err)
	}
}

func TestCompileDeterministicIDs(t *testing.T) {
	_, d1 := compileBase(t)
	_, d2 := compileBase(t)
	if strings.Join(d1.order, ",") != strings.Join(d2.order, ",") {
		t.Errorf("ordem de registro não determinística:\n%v\n%v", d1.order, d2.order)
	}
}

func TestResolveUnknownInclude(t *testing.T) {
	c, dict := compileBase(t) // unknownFields: include
	f1, err := c.ResolveUnknown([]byte("X-Vendor-Attr"))
	if err != nil || f1 == nil {
		t.Fatalf("ResolveUnknown: %v, %v", f1, err)
	}
	if !f1.IsAuto || f1.Name != "X-Vendor-Attr" {
		t.Errorf("campo criado = %+v", f1)
	}
	// Cacheado: segunda chamada devolve o mesmo campo, sem novo ID.
	before := len(dict.order)
	f2, err := c.ResolveUnknown([]byte("X-Vendor-Attr"))
	if err != nil || f2 != f1 {
		t.Fatalf("cache: %p vs %p, err=%v", f2, f1, err)
	}
	if len(dict.order) != before {
		t.Error("segunda resolução não deveria registrar de novo no dict")
	}
	// E FieldByName também passa a resolver.
	if got, ok := c.FieldByName([]byte("X-Vendor-Attr")); !ok || got != f1 {
		t.Error("FieldByName deveria achar o campo resolvido")
	}
}

func TestResolveUnknownIgnore(t *testing.T) {
	s := parseOK(t, strings.Replace(baseYAML, "unknownFields: include", "unknownFields: ignore", 1))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.ResolveUnknown([]byte("X-Vendor-Attr"))
	if err != nil || f != nil {
		t.Errorf("ignore: f=%v err=%v, want nil,nil", f, err)
	}
}

func TestResolveUnknownReject(t *testing.T) {
	s := parseOK(t, strings.Replace(baseYAML, "unknownFields: include", "unknownFields: reject", 1))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ResolveUnknown([]byte("X-Vendor-Attr"))
	if !errors.Is(err, ErrUnknownField) {
		t.Errorf("reject: err=%v, want ErrUnknownField", err)
	}
}

// Com include explícito o universo de campos é fechado: qualquer campo fora
// da lista é ignorado (nunca unknown), independente da policy.
func TestExplicitIncludeIsClosed(t *testing.T) {
	s := parseOK(t, `
version: 1
name: closed
materialization:
  key:
    field: id
schema:
  include:
    - id
    - _SEQ
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    NotIncluded:
      type: string
`)
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.FieldByName([]byte("NotIncluded")); ok {
		t.Error("campo declarado fora do include não deveria materializar")
	}
	f, err := c.ResolveUnknown([]byte("QualquerCoisa"))
	if err != nil || f != nil {
		t.Errorf("universo fechado: f=%v err=%v, want nil,nil", f, err)
	}
}

// Campo listado no include sem declaração em fields vira auto.
func TestIncludeUndeclaredBecomesAuto(t *testing.T) {
	s := parseOK(t, `
version: 1
name: autofield
materialization:
  key:
    field: id
schema:
  include:
    - id
    - livre
  fields:
    id:
      type: string
`)
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("livre"))
	if !ok || !f.IsAuto {
		t.Errorf("campo livre = %+v, ok=%v", f, ok)
	}
}

// unknownFields: include exige include: ["*"] (com lista explícita o
// universo é fechado — a combinação é contraditória).
func TestUnknownIncludeRequiresWildcard(t *testing.T) {
	parseErr(t, `
version: 1
name: contradicao
materialization:
  key:
    field: id
schema:
  include:
    - id
  unknownFields: include
  fields:
    id:
      type: string
`, "unknownFields")
}
