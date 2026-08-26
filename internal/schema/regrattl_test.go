package schema

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlComRegras monta uma spec mínima com a lista de regras dada, já indentada
// sob materialization.
func yamlComRegras(regras string) string {
	return "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n" + regras +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
}

func recusaRegras(t *testing.T, regras string, trechos ...string) {
	t.Helper()
	_, err := Parse([]byte(yamlComRegras(regras)), DefaultsPadrao)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, trecho := range trechos {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("a mensagem precisa nomear %q: %v", trecho, err)
		}
	}
}

func TestTTLListaVaziaEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl: []\n", "vazia")
}

// A regra de fundo casa com tudo: colocá-la antes de uma condicional tornaria
// a condicional inalcançável, e a ordem é semântica aqui.
func TestRegraSemWhenNoMeioEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl:\n    - expireAfterWrite: 7d\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n",
		"última")
}

func TestDuasRegrasSemWhenSaoRecusadas(t *testing.T) {
	recusaRegras(t, "  ttl:\n    - expireAfterWrite: 7d\n    - expireAfterWrite: 3m\n", "última")
}

func TestRegraSemExpireAfterWriteEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl:\n    - when: \"id = 'a'\"\n", "expireAfterWrite")
}

func TestDuracaoAbaixoDaResolucaoEhRecusada(t *testing.T) {
	for _, d := range []string{"30s", "1s", "59s"} {
		recusaRegras(t, "  ttl:\n    - expireAfterWrite: "+d+"\n", "resolução")
	}
	// O piso exato é aceito.
	s, err := Parse([]byte(yamlComRegras("  ttl:\n    - expireAfterWrite: 1m\n")), DefaultsPadrao)
	if err != nil {
		t.Fatalf("1m (a própria resolução) foi recusado: %v", err)
	}
	if s.Materialization.DuracaoDeFundo != time.Minute {
		t.Errorf("duração de fundo = %v, quero 1m", s.Materialization.DuracaoDeFundo)
	}
}

// `never` diz a intenção; `0` parece um esquecimento.
func TestZeroNumericoEhRecusadoMasNeverEhAceito(t *testing.T) {
	recusaRegras(t, "  ttl:\n    - expireAfterWrite: 0\n", "never")
	s, err := Parse([]byte(yamlComRegras("  ttl:\n    - expireAfterWrite: never\n")), DefaultsPadrao)
	if err != nil {
		t.Fatalf("never foi recusado: %v", err)
	}
	if len(s.Materialization.RegrasTTL) != 1 || s.Materialization.RegrasTTL[0].Duracao != 0 {
		t.Errorf("regras = %+v, quero uma regra imortal", s.Materialization.RegrasTTL)
	}
	if s.Materialization.DuracaoDeFundo != 0 {
		t.Errorf("duração de fundo = %v, `never` não declara prazo", s.Materialization.DuracaoDeFundo)
	}
}

// O gate da migração: a forma antiga tem de dar erro NOMEANDO a nova, não
// decodificar em silêncio nem morrer com a mensagem do yaml.
func TestFormaEscalarAntigaEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl:\n    expireAfterWrite: 7d\n", "materialization.ttl", "lista")
}

func TestFormaFieldTypeAntigaEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl:\n    field: exp\n    type: timestamp\n", "lista")
}

func TestChaveDesconhecidaNaRegraEhRecusada(t *testing.T) {
	recusaRegras(t, "  ttl:\n    - expireAfterWrite: 7d\n      field: exp\n", "field")
}

// A ordem do YAML é preservada: primeira que casa vence, e reordenar as
// regras muda o significado.
func TestRegrasPreservamAOrdemDoArquivo(t *testing.T) {
	s, err := Parse([]byte(yamlComRegras(
		"  ttl:\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n"+
			"    - when: \"id = 'b'\"\n      expireAfterWrite: 2h\n"+
			"    - expireAfterWrite: 7d\n")), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	regras := s.Materialization.RegrasTTL
	if len(regras) != 3 {
		t.Fatalf("%d regras, quero 3", len(regras))
	}
	if regras[0].Duracao != 3*time.Minute || regras[1].Duracao != 2*time.Hour {
		t.Errorf("durações fora de ordem: %+v", regras)
	}
	if regras[2].When != "" || s.Materialization.DuracaoDeFundo != 7*24*time.Hour {
		t.Errorf("a última é a de fundo: %+v / %v", regras[2], s.Materialization.DuracaoDeFundo)
	}
}

func compilaComRegras(t *testing.T, regras string) *Compiled {
	t.Helper()
	s, err := Parse([]byte(yamlComRegras(regras)), DefaultsPadrao)
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
	c := compilaComRegras(t, "  ttl:\n    - expireAfterWrite: 7d\n")
	if c.Prazo == nil {
		t.Fatal("o campo de prazo não foi registrado")
	}
	if !c.Prazo.IsSystem {
		t.Error("o campo de prazo é do writer: tem de ser de sistema")
	}
	if c.Prazo.Type != types.TypeInt64 {
		t.Errorf("tipo do prazo = %v, quero int64 — ele carrega uma DURAÇÃO em millis, e timestamp "+
			"renderizaria 3 minutos como uma data", c.Prazo.Type)
	}
	if c.Escrita == nil {
		t.Error("o instante de escrita continua obrigatório: é dele que a conta do prazo parte")
	}
}

func TestSemRegrasNaoRegistraOCampoDePrazo(t *testing.T) {
	c := compilaComRegras(t, "")
	if c.Prazo != nil || c.Escrita != nil {
		t.Error("dataset sem regras não paga campo de sistema nenhum")
	}
}

// Quem pede "tudo" quer os campos do próprio dado, não a contabilidade
// interna do banco.
func TestCampoDePrazoFicaForaDoSelectStar(t *testing.T) {
	c := compilaComRegras(t, "  ttl:\n    - expireAfterWrite: 7d\n")
	for _, f := range c.FieldsByID() {
		if f.Name == CampoDePrazo {
			t.Fatal("o campo de prazo apareceria no SELECT * e no /v1/metadata")
		}
	}
	achou := false
	for _, f := range c.FieldsByIDIncluindoSistema() {
		if f.Name == CampoDePrazo {
			achou = true
		}
	}
	if !achou {
		t.Error("o conjunto FÍSICO precisa dele: é o que a compaction e o storage leem")
	}
}

// A ordem do LookupOrCreate decide o mapeamento nome→id de um dataset NOVO, e
// esse mapeamento é gravado para sempre: _writtenAt primeiro, _expiresIn
// depois. Reordenar quebraria a leitura de todo dataset já criado.
func TestCampoDeSistemaNaoPodeSerDeclaradoNoSchema(t *testing.T) {
	yaml := yamlComRegras("  ttl:\n    - expireAfterWrite: 7d\n")
	yaml = strings.Replace(yaml, "    id:\n      type: string\n",
		"    id:\n      type: string\n    "+CampoDePrazo+":\n      type: int64\n", 1)
	if _, err := Parse([]byte(yaml), DefaultsPadrao); err == nil {
		t.Fatal("o campo de prazo foi aceito em schema.fields")
	}
}

// O log de subida precisa mostrar a ORDEM, que é semântica, e distinguir
// `never` de um prazo zero que não existe.
func TestDescreveRegrasMostraOrdemEFundo(t *testing.T) {
	s, err := Parse([]byte(yamlComRegras(
		"  ttl:\n    - when: \"id = 'a'\"\n      expireAfterWrite: 3m\n"+
			"    - when: \"id = 'b'\"\n      expireAfterWrite: never\n"+
			"    - expireAfterWrite: 7d\n")), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	got := DescreveRegras(s.Materialization.RegrasTTL)
	for _, trecho := range []string{`[0] when="id = 'a'" 3m0s`, `[1] when="id = 'b'" never`, "[2] fundo 168h0m0s"} {
		if !strings.Contains(got, trecho) {
			t.Errorf("descrição %q não contém %q", got, trecho)
		}
	}
}
