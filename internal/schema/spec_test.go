package schema

import (
	"strings"
	"testing"
	"time"
)

// YAML mínimo válido usado como base; testes de erro mutam trechos.
const baseYAML = `
version: 1
name: radius_sessions
materialization:
  key:
    source: payload
    field: id
    type: string
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater
schema:
  include:
    - "*"
  exclude:
    - debug
  unknownFields: include
  fields:
    id:
      type: string
      alias: service_id
    _SEQ:
      type: int64
    Acct-Status-Type:
      type: string
      alias: status
      index: true
    NAS-Identifier:
      type: string
      alias: nas
      index: true
    Framed-IP-Address:
      type: ipv4
      alias: ip
      index: true
    Event-Timestamp:
      type: timestamp
    EventExpireDate:
      type: timestamp
      alias: expiration
    Acct-Input-Octets:
      type: int64
    ratio:
      type: double
    extra:
      type: auto
query:
  maxRows: 50000
  timeout: 10s
`

func parseOK(t *testing.T, yml string) *DatasetSpec {
	t.Helper()
	s, err := Parse([]byte(yml), DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func parseErr(t *testing.T, yml, wantSubstr string) {
	t.Helper()
	_, err := Parse([]byte(yml), DefaultsPadrao)
	if err == nil {
		t.Fatalf("esperava erro contendo %q", wantSubstr)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("erro %q não contém %q", err.Error(), wantSubstr)
	}
}

func TestParseBaseYAML(t *testing.T) {
	s := parseOK(t, baseYAML)
	if s.Name != "radius_sessions" {
		t.Errorf("Name = %q", s.Name)
	}
	if s.Materialization.Key.Field != "id" {
		t.Errorf("Key.Field = %q", s.Materialization.Key.Field)
	}
	if s.Materialization.Mode != "merge" {
		t.Errorf("Mode = %q", s.Materialization.Mode)
	}
	if s.Materialization.Sequence == nil || s.Materialization.Sequence.Field != "_SEQ" {
		t.Errorf("Sequence = %+v", s.Materialization.Sequence)
	}
	if s.Schema.UnknownFields != "include" {
		t.Errorf("UnknownFields = %q", s.Schema.UnknownFields)
	}
	if got := s.Schema.Fields["Acct-Status-Type"]; got.Alias != "status" || got.Index != "true" {
		t.Errorf("Acct-Status-Type = %+v", got)
	}
	if s.Query.MaxRows != 50000 || s.Query.Timeout != 10*time.Second {
		t.Errorf("Query = %+v", s.Query)
	}
}

func TestParseDefaults(t *testing.T) {
	s := parseOK(t, `
version: 1
name: minimal
materialization:
  key:
    field: id
    type: string
schema:
  fields:
    id:
      type: string
`)
	if s.Materialization.Mode != "merge" {
		t.Errorf("Mode default = %q, want merge", s.Materialization.Mode)
	}
	if s.Schema.UnknownFields != "ignore" {
		t.Errorf("UnknownFields default = %q, want ignore", s.Schema.UnknownFields)
	}
	if s.Materialization.Key.Source != "payload" {
		t.Errorf("Key.Source default = %q, want payload", s.Materialization.Key.Source)
	}
	if s.Query.MaxRows != 100000 {
		t.Errorf("MaxRows default = %d, want 100000", s.Query.MaxRows)
	}
	if s.Query.Timeout != 30*time.Second {
		t.Errorf("Timeout default = %v, want 30s", s.Query.Timeout)
	}
	// Sem include e sem "*": include implícito = campos declarados.
	if len(s.Schema.Include) != 1 || s.Schema.Include[0] != "id" {
		t.Errorf("Include implícito = %v, want [id]", s.Schema.Include)
	}
}

func TestParseValidationErrors(t *testing.T) {
	repl := func(old, new string) string {
		if !strings.Contains(baseYAML, old) {
			t.Fatalf("baseYAML não contém %q", old)
		}
		return strings.Replace(baseYAML, old, new, 1)
	}
	cases := []struct {
		name string
		yml  string
		want string
	}{
		{"yaml invalido", "{{not yaml", "yaml"},
		{"version errada", repl("version: 1", "version: 2"), "version"},
		{"nome vazio", repl("name: radius_sessions", "name: \"\""), "name"},
		{"nome invalido", repl("name: radius_sessions", "name: Radius-Sessions"), "name"},
		{"nome com prefixo reservado", repl("name: radius_sessions", "name: _meta"), "reservado"},
		{"key ausente", repl("    field: id\n", ""), "key.field"},
		{"key tipo nao-string", repl("    field: id\n    type: string", "    field: id\n    type: int64"), "key.type"},
		{"mode invalido", repl("mode: merge", "mode: upsert"), "mode"},
		{"sequence tipo invalido", repl("    type: int64\n    strategy: greater", "    type: string\n    strategy: greater"), "sequence.type"},
		{"strategy invalida", repl("strategy: greater", "strategy: lower"), "sequence.strategy"},
		// Sequence/ttl referenciados devem estar DECLARADOS em fields
		// incondicionalmente (como a key): sob include explícito, um campo
		// não declarado compilaria como auto/TypeNull e quebraria o greater
		// do materializer.
		{"sequence nao declarada", "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  sequence:\n    field: _SEQ\nschema:\n  include: [id, _SEQ]\n  fields:\n    id:\n      type: string\n", "sequence"},
		{"sequence declarada com outro tipo", repl("    _SEQ:\n      type: int64", "    _SEQ:\n      type: string"), "_SEQ"},
		{"ttl na forma antiga", "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  ttl:\n    expireAfterWrite: 2h\nschema:\n  include: [id]\n  fields:\n    id:\n      type: string\n", "lista"},
		{"unknownFields invalido", repl("unknownFields: include", "unknownFields: drop"), "unknownFields"},
		{"tipo de campo invalido", repl("      type: double", "      type: float"), "float"},
		{"index invalido", repl("index: true", "index: btree"), "btree"},
		// D5: index: hash e index: bitmap foram as duas grafias GLOBAIS que o
		// motor único aposentou. A rejeição tem sentinela própria
		// (ErrGrafiaDeIndiceObsoleta, testada em cli.TestDatasetComGrafiaDe...
		// e em schema.TestAsGrafiasAntigasSaoRecusadas); aqui só se confere que
		// ela também sai pelo caminho comum de validação de campo, com a
		// mensagem ensinando a grafia nova.
		{"grafia hash obsoleta", repl("index: true", "index: hash"), "index: true"},
		{"grafia bitmap obsoleta", repl("index: true", "index: bitmap"), "index: true"},
		// bitmap/hash não distinguem mais nada (D5): o que sobrevive da
		// checagem antiga é só "o tipo é indexável?", e a resposta não mudou
		// para double nem para json — a UNIÃO dos dois conjuntos antigos
		// continua sem os dois.
		{"index em tipo nao indexavel (double)", repl("    ratio:\n      type: double", "    ratio:\n      type: double\n      index: true"), "ratio"},
		{"index em tipo nao indexavel (json)", repl("    extra:\n      type: auto", "    extra:\n      type: json\n      index: true"), "extra"},
		{"index na key", repl("      alias: service_id", "      alias: service_id\n      index: true"), "key"},
		{"alias duplicado", repl("      alias: nas", "      alias: status"), "status"},
		{"alias colide com nome", repl("      alias: nas", "      alias: _SEQ"), "_SEQ"},
		{"exclude sem wildcard", repl("    - \"*\"", "    - id"), "exclude"},
		{"wildcard com outros", repl("    - \"*\"", "    - \"*\"\n    - id"), "*"},
		// Excluir key/sequence/ttl compila mas quebra todo evento em runtime;
		// excluir por alias seria um no-op silencioso.
		{"exclude da key", repl("  exclude:\n    - debug", "  exclude:\n    - id"), "key"},
		{"exclude da sequence", repl("  exclude:\n    - debug", "  exclude:\n    - _SEQ"), "sequence"},
		{"exclude por alias", repl("  exclude:\n    - debug", "  exclude:\n    - status"), "alias"},
		{"maxRows negativo", repl("maxRows: 50000", "maxRows: -1"), "maxRows"},
		{"maxCols zero", repl("  unknownFields: include", "  unknownFields: include\n  maxCols: 0"), "maxCols"},
		{"maxCols negativo", repl("  unknownFields: include", "  unknownFields: include\n  maxCols: -1"), "maxCols"},
		{"maxCols acima do teto fisico", repl("  unknownFields: include", "  unknownFields: include\n  maxCols: 65536"), "65535"},
		{"onMaxCols invalido", repl("  unknownFields: include", "  unknownFields: include\n  onMaxCols: partial"), "onMaxCols"},
		{"timeout invalido", repl("timeout: 10s", "timeout: depressa"), "timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parseErr(t, tc.yml, tc.want)
		})
	}
}

// O prefixo "_" em nome de DATASET fica reservado aos espaços de nome do
// produto — o primeiro é `_meta`, a tabela virtual de introspecção. A recusa
// entra ANTES de existir dataset que o ocupe: é uma linha hoje, e depois seria
// "recrie o dataset", o custo mais alto do produto.
func TestNomeDeDatasetNaoPodeComecarComUnderscore(t *testing.T) {
	for _, nome := range []string{"_meta", "_", "_x"} {
		t.Run(nome, func(t *testing.T) {
			parseErr(t, strings.Replace(baseYAML, "name: radius_sessions", "name: "+nome, 1), "reservado")
		})
	}
}

// A regra acima vale para o nome do DATASET e para mais nada. Campo e alias
// com "_" continuam válidos, e é deliberado: o comentário de nomesDeSistema
// (spec.go) registra que reservar o prefixo para CAMPOS recusaria datasets que
// hoje sobem, porque `_SEQ` é nome de sequence legítimo e já está em uso.
// Sem esta metade, a recusa nova poderia crescer para os campos sem que nada
// acusasse.
func TestUnderscoreContinuaValidoEmCampoEAlias(t *testing.T) {
	s := parseOK(t, strings.Replace(baseYAML, "      alias: service_id", "      alias: _sid", 1))
	if s.Schema.Fields["_SEQ"].Type != "int64" {
		t.Errorf("campo _SEQ deveria continuar declarado como int64, é %q", s.Schema.Fields["_SEQ"].Type)
	}
	if got := s.Schema.Fields["id"].Alias; got != "_sid" {
		t.Errorf("alias _sid deveria ser aceito, é %q", got)
	}
}

// Até o M1 este teste se chamava TestParseSourceKafkaIsAcceptedAndIgnored e
// provava que o bloco era aceito sem validação nenhuma. Esse comportamento ERA
// o defeito (issue #1): um `topic` sem `bootstrapServers` passava limpo, e o
// operador só descobria na primeira tentativa de conexão — que no M1 nunca
// acontecia, porque ninguém lia a seção.
func TestSourceKafkaIncompletoERecusadoNoParse(t *testing.T) {
	parseErr(t, strings.Replace(baseYAML, "materialization:", `source:
  type: kafka
  kafka:
    topic: radius-accounting
materialization:`, 1), "bootstrapServers")
}

// maxCols/onMaxCols seguem a forma do maxRows: quem declara vence, quem omite
// herda o default. O default de maxCols é o teto FÍSICO do dicionário — o
// FieldID é uint16 —, porque qualquer número menor aplicaria política a quem
// não a escolheu.
func TestMaxColsPadraoEOTetoFisicoDoDicionario(t *testing.T) {
	s := parseOK(t, baseYAML)
	if s.Schema.MaxCols != 65535 {
		t.Errorf("MaxCols default = %d, quer 65535", s.Schema.MaxCols)
	}
	if s.Schema.OnMaxCols != OnMaxColsIgnore {
		t.Errorf("OnMaxCols default = %q, quer %q", s.Schema.OnMaxCols, OnMaxColsIgnore)
	}
}

func TestMaxColsDeclaradoVenceODefault(t *testing.T) {
	s := parseOK(t, strings.Replace(baseYAML,
		"  unknownFields: include",
		"  unknownFields: include\n  maxCols: 1000\n  onMaxCols: stop", 1))
	if s.Schema.MaxCols != 1000 {
		t.Errorf("MaxCols = %d, quer 1000", s.Schema.MaxCols)
	}
	if s.Schema.OnMaxCols != OnMaxColsStop {
		t.Errorf("OnMaxCols = %q, quer %q", s.Schema.OnMaxCols, OnMaxColsStop)
	}
}
