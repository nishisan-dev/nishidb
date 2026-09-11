package schema

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// fakeDict atribui IDs sequenciais na ordem das chamadas (como o fielddict
// real do storage), registra a ordem para asserções de determinismo e
// espelha o tipo fixado dos campos auto (R11).
type fakeDict struct {
	ids   map[string]types.FieldID
	hints map[types.FieldID]types.FieldType
	order []string
	next  types.FieldID
	// setCalls conta as fixações persistidas: é assim que os testes provam
	// que o tipo foi ao dicionário, e não só à memória do Compiled.
	setCalls int
}

func newFakeDict() *fakeDict {
	return &fakeDict{
		ids:   map[string]types.FieldID{},
		hints: map[types.FieldID]types.FieldType{},
		next:  1,
	}
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

func (d *fakeDict) Len() int { return len(d.order) }

func (d *fakeDict) Name(id types.FieldID) (string, bool) {
	if id < 1 || int(id) > len(d.order) {
		return "", false
	}
	return d.order[id-1], true
}

func (d *fakeDict) TypeHint(id types.FieldID) (types.FieldType, bool) {
	t, ok := d.hints[id]
	return t, ok && t != types.TypeNull
}

func (d *fakeDict) SetTypeHint(id types.FieldID, t types.FieldType) error {
	if t == types.TypeNull {
		return errors.New("tipo nulo não pode ser fixado")
	}
	switch current := d.hints[id]; current {
	case t:
		return nil
	case types.TypeNull:
		d.hints[id] = t
		d.setCalls++
		return nil
	default:
		return errors.New("tipo já fixado com outro valor")
	}
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
	if !byName.Indexed || byName.Type != types.TypeString {
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
	// EventExpireDate é campo COMUM desde que o deadline absoluto vindo do
	// payload saiu: continua compilado, tipado e consultável — só não governa
	// expiração nenhuma.
	exp, _ := c.FieldByName([]byte("EventExpireDate"))
	if exp == nil || exp.Type != types.TypeTimestamp || exp.IsSystem {
		t.Errorf("EventExpireDate: %+v", exp)
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
schemaName: telecom
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
schemaName: telecom
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
schemaName: telecom
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

// ceilingYAML monta um dataset de universo aberto com teto de campos declarado.
// Depois do Compile o dicionário tem exatamente um campo (`id`), então
// maxCols: 2 deixa espaço para UM autodescoberto e recusa o seguinte.
func ceilingYAML(maxCols int, onMax string) string {
	return fmt.Sprintf(`version: 1
name: t
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  maxCols: %d
  onMaxCols: %s
  fields:
    id:
      type: string
`, maxCols, onMax)
}

// O teto de campos NÃO pode parar a ingestão: sobre uma grandeza que cresce
// sem limite, atingi-lo não é anomalia, é questão de tempo. Sob `ignore` o
// campo excedente é descartado e o EVENTO continua sendo gravado — é o mesmo
// desfecho que o campo excluído já tem, e por isso reusa a rota (nil, nil).
func TestResolveUnknownDescartaCampoAcimaDoTetoSobIgnore(t *testing.T) {
	s := parseOK(t, ceilingYAML(2, "ignore"))
	dict := newFakeDict()
	c, err := s.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	if f, err := c.ResolveUnknown([]byte("cabe")); err != nil || f == nil {
		t.Fatalf("primeiro autodescoberto = %v, %v; devia caber", f, err)
	}
	before := len(dict.order)
	f, err := c.ResolveUnknown([]byte("naoCabe"))
	if err != nil || f != nil {
		t.Errorf("campo acima do teto = %v, %v; quer nil, nil", f, err)
	}
	if len(dict.order) != before {
		t.Errorf("campo acima do teto foi registrado no dicionário: %v", dict.order[before:])
	}
	if got := c.DiscardedFieldCount(); got != 1 {
		t.Errorf("DiscardedFieldCount = %d, quer 1 — a perda precisa ser contável", got)
	}
}

// Sob `stop` a perda não é aceita: o erro sobe com sentinela própria para que
// a classificação o separe do balde de "bug nosso" e a parada seja declarada,
// e não um efeito colateral de erro sem dono.
func TestResolveUnknownRecusaAcimaDoTetoSobStop(t *testing.T) {
	s := parseOK(t, ceilingYAML(2, "stop"))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResolveUnknown([]byte("cabe")); err != nil {
		t.Fatalf("primeiro autodescoberto: %v", err)
	}
	if _, err := c.ResolveUnknown([]byte("naoCabe")); !errors.Is(err, ErrFieldCeiling) {
		t.Errorf("campo acima do teto = %v, quer ErrFieldCeiling", err)
	}
}

// Bater no teto congela o CONJUNTO de campos, não o dataset: os campos que já
// existem continuam resolvendo normalmente. Sem esta garantia, o primeiro
// campo excedente derrubaria todo evento seguinte, que é o defeito que o teto
// deveria evitar.
func TestTetoDeCamposNaoAfetaCampoJaConhecido(t *testing.T) {
	s := parseOK(t, ceilingYAML(2, "stop"))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResolveUnknown([]byte("cabe")); err != nil {
		t.Fatalf("primeiro autodescoberto: %v", err)
	}
	if _, err := c.ResolveUnknown([]byte("naoCabe")); !errors.Is(err, ErrFieldCeiling) {
		t.Fatalf("preparo: quer ErrFieldCeiling, veio %v", err)
	}
	if f, err := c.ResolveUnknown([]byte("cabe")); err != nil || f == nil {
		t.Errorf("campo já conhecido no teto = %v, %v; devia continuar resolvendo", f, err)
	}
}

// Declarar mais campos do que o próprio teto é erro de configuração, e sai na
// compilação: deixar passar produziria um dataset que nasce acima do limite
// que ele mesmo declarou.
func TestCompileRecusaCamposDeclaradosAcimaDoTeto(t *testing.T) {
	s := parseOK(t, strings.Replace(ceilingYAML(1, "ignore"),
		"    id:\n      type: string\n",
		"    id:\n      type: string\n    outro:\n      type: string\n", 1))
	if _, err := s.Compile(newFakeDict()); !errors.Is(err, ErrFieldCeiling) {
		t.Errorf("Compile = %v, quer ErrFieldCeiling", err)
	}
}

// yamlWithDeclaredIndex monta uma spec mínima com um campo `node` que
// declara `index: <grafia>` — usado pelos dois testes de D5 abaixo, que
// precisam de um YAML isolado do baseYAML porque o baseYAML está migrando
// para a grafia nova junto com o resto da suíte.
func yamlWithDeclaredIndex(spelling string) string {
	return "version: 1\nname: t\nschemaName: telecom\nmaterialization:\n  key:\n    field: id\n" +
		"schema:\n  fields:\n    id:\n      type: string\n" +
		"    node:\n      type: string\n      index: " + spelling + "\n"
}

// TestIndexTrueEhAceito prova que `index: true` marca o campo como indexado.
// Com um motor só (D5) não há mais um IndexKind de três valores a conferir: o
// campo compilado carrega um bool, e a regra do planner que o lê é uma — a que
// intersecta todos os termos elegíveis.
func TestIndexTrueEhAceito(t *testing.T) {
	s := parseOK(t, yamlWithDeclaredIndex("true"))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	f, ok := c.FieldByName([]byte("node"))
	if !ok {
		t.Fatal("campo node não compilou")
	}
	if !f.Indexed {
		t.Error("index: true não marcou o campo como indexado")
	}
}

// TestAsGrafiasAntigasSaoRecusadas. Com um motor só, hash e bitmap
// construiriam a mesma coisa — e config que declara uma escolha que o motor
// ignora é o expireAfterWrite decorativo que o projeto já carrega como débito.
// Aceitá-las como sinônimo seria pior: o operador continuaria escrevendo uma
// distinção que não existe, e o WARN vira ruído que ninguém lê.
func TestAsGrafiasAntigasSaoRecusadas(t *testing.T) {
	for _, spelling := range []string{"hash", "bitmap"} {
		t.Run(spelling, func(t *testing.T) {
			_, err := Parse([]byte(yamlWithDeclaredIndex(spelling)), BuiltinDefaults)
			if err == nil {
				t.Fatalf("index: %s foi aceito", spelling)
			}
			// A mensagem tem de dizer o que ESCREVER, e não só o que está
			// errado: quem lê isso está com o servidor fora do ar.
			if !strings.Contains(err.Error(), "index: true") {
				t.Errorf("a mensagem não ensina a grafia nova: %v", err)
			}
		})
	}
}
