package schema

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlWithRules monta uma spec mínima com a lista de regras dada, já indentada
// sob materialization.
func yamlWithRules(rules string) string {
	return "version: 1\nname: t\nschemaName: telecom\nmaterialization:\n  key:\n    field: id\n" + rules +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
}

func refusesRules(t *testing.T, rules string, snippets ...string) {
	t.Helper()
	_, err := Parse([]byte(yamlWithRules(rules)), BuiltinDefaults)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, snippet := range snippets {
		if !strings.Contains(err.Error(), snippet) {
			t.Errorf("a mensagem precisa nomear %q: %v", snippet, err)
		}
	}
}

func TestTTLListaVaziaEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl: []\n", "vazia")
}

// A regra de fundo casa com tudo: colocá-la antes de uma condicional tornaria
// a condicional inalcançável, e a ordem é semântica aqui.
func TestRegraSemWhenNoMeioEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl:\n    - expireAfterWrite: 7d\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n",
		"última")
}

func TestDuasRegrasSemWhenSaoRecusadas(t *testing.T) {
	refusesRules(t, "  ttl:\n    - expireAfterWrite: 7d\n    - expireAfterWrite: 3m\n", "última")
}

func TestRegraSemExpireAfterWriteEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl:\n    - when: \"id = 'a'\"\n", "expireAfterWrite")
}

func TestDuracaoAbaixoDaResolucaoEhRecusada(t *testing.T) {
	for _, d := range []string{"30s", "1s", "59s"} {
		refusesRules(t, "  ttl:\n    - expireAfterWrite: "+d+"\n", "resolução")
	}
	// O piso exato é aceito.
	s, err := Parse([]byte(yamlWithRules("  ttl:\n    - expireAfterWrite: 1m\n")), BuiltinDefaults)
	if err != nil {
		t.Fatalf("1m (a própria resolução) foi recusado: %v", err)
	}
	if s.Materialization.FallbackDuration != time.Minute {
		t.Errorf("duração de fundo = %v, quero 1m", s.Materialization.FallbackDuration)
	}
}

// `never` diz a intenção; `0` parece um esquecimento.
func TestZeroNumericoEhRecusadoMasNeverEhAceito(t *testing.T) {
	refusesRules(t, "  ttl:\n    - expireAfterWrite: 0\n", "never")
	s, err := Parse([]byte(yamlWithRules("  ttl:\n    - expireAfterWrite: never\n")), BuiltinDefaults)
	if err != nil {
		t.Fatalf("never foi recusado: %v", err)
	}
	if len(s.Materialization.TTLRules) != 1 || s.Materialization.TTLRules[0].Duration != 0 {
		t.Errorf("regras = %+v, quero uma regra imortal", s.Materialization.TTLRules)
	}
	if s.Materialization.FallbackDuration != 0 {
		t.Errorf("duração de fundo = %v, `never` não declara prazo", s.Materialization.FallbackDuration)
	}
}

// O gate da migração: a forma antiga tem de dar erro NOMEANDO a nova, não
// decodificar em silêncio nem morrer com a mensagem do yaml.
func TestFormaEscalarAntigaEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl:\n    expireAfterWrite: 7d\n", "materialization.ttl", "lista")
}

func TestFormaFieldTypeAntigaEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl:\n    field: exp\n    type: timestamp\n", "lista")
}

func TestChaveDesconhecidaNaRegraEhRecusada(t *testing.T) {
	refusesRules(t, "  ttl:\n    - expireAfterWrite: 7d\n      field: exp\n", "field")
}

// A ordem do YAML é preservada: primeira que casa vence, e reordenar as
// regras muda o significado.
func TestRegrasPreservamAOrdemDoArquivo(t *testing.T) {
	s, err := Parse([]byte(yamlWithRules(
		"  ttl:\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n"+
			"    - when: \"id = 'b'\"\n      expireAfterWrite: 2h\n"+
			"    - expireAfterWrite: 7d\n")), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	rules := s.Materialization.TTLRules
	if len(rules) != 3 {
		t.Fatalf("%d regras, quero 3", len(rules))
	}
	if rules[0].Duration != 3*time.Minute || rules[1].Duration != 2*time.Hour {
		t.Errorf("durações fora de ordem: %+v", rules)
	}
	if rules[2].When != "" || s.Materialization.FallbackDuration != 7*24*time.Hour {
		t.Errorf("a última é a de fundo: %+v / %v", rules[2], s.Materialization.FallbackDuration)
	}
}

func compileWithRules(t *testing.T, rules string) *Compiled {
	t.Helper()
	s, err := Parse([]byte(yamlWithRules(rules)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCompilaRegistraOCampoDePrazoQuandoHaRegras(t *testing.T) {
	c := compileWithRules(t, "  ttl:\n    - expireAfterWrite: 7d\n")
	if c.TTLField == nil {
		t.Fatal("o campo de prazo não foi registrado")
	}
	if !c.TTLField.IsSystem {
		t.Error("o campo de prazo é do writer: tem de ser de sistema")
	}
	if c.TTLField.Type != types.TypeInt64 {
		t.Errorf("tipo do prazo = %v, quero int64 — ele carrega uma DURAÇÃO em millis, e timestamp "+
			"renderizaria 3 minutos como uma data", c.TTLField.Type)
	}
	if c.WriteInstantField == nil {
		t.Error("o instante de escrita continua obrigatório: é dele que a conta do prazo parte")
	}
}

func TestSemRegrasNaoRegistraOCampoDePrazo(t *testing.T) {
	c := compileWithRules(t, "")
	if c.TTLField != nil || c.WriteInstantField != nil {
		t.Error("dataset sem regras não paga campo de sistema nenhum")
	}
}

// Quem pede "tudo" quer os campos do próprio dado, não a contabilidade
// interna do banco.
func TestCampoDePrazoFicaForaDoSelectStar(t *testing.T) {
	c := compileWithRules(t, "  ttl:\n    - expireAfterWrite: 7d\n")
	for _, f := range c.FieldsByID() {
		if f.Name == TTLFieldName {
			t.Fatal("o campo de prazo apareceria no SELECT * e no /v1/metadata")
		}
	}
	found := false
	for _, f := range c.FieldsByIDIncludingSystem() {
		if f.Name == TTLFieldName {
			found = true
		}
	}
	if !found {
		t.Error("o conjunto FÍSICO precisa dele: é o que a compaction e o storage leem")
	}
}

// A ordem do LookupOrCreate decide o mapeamento nome→id de um dataset NOVO, e
// esse mapeamento é gravado para sempre: _writtenAt primeiro, _expiresIn
// depois. Reordenar quebraria a leitura de todo dataset já criado.
func TestCampoDeSistemaNaoPodeSerDeclaradoNoSchema(t *testing.T) {
	yaml := yamlWithRules("  ttl:\n    - expireAfterWrite: 7d\n")
	yaml = strings.Replace(yaml, "    id:\n      type: string\n",
		"    id:\n      type: string\n    "+TTLFieldName+":\n      type: int64\n", 1)
	if _, err := Parse([]byte(yaml), BuiltinDefaults); err == nil {
		t.Fatal("o campo de prazo foi aceito em schema.fields")
	}
}

// O log de subida precisa mostrar a ORDEM, que é semântica, e distinguir
// `never` de um prazo zero que não existe.
func TestDescreveRegrasMostraOrdemEFundo(t *testing.T) {
	s, err := Parse([]byte(yamlWithRules(
		"  ttl:\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n"+
			"    - when: \"id = 'b'\"\n      expireAfterWrite: never\n"+
			"    - expireAfterWrite: 7d\n")), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	got := DescribeRules(s.Materialization.TTLRules)
	for _, snippet := range []string{`[0] when="id = 'a'" 3m0s`, `[1] when="id = 'b'" never`, "[2] fundo 168h0m0s"} {
		if !strings.Contains(got, snippet) {
			t.Errorf("descrição %q não contém %q", got, snippet)
		}
	}
}
