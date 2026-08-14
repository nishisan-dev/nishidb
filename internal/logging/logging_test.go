package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// O §95 fixa os nomes dos campos. slog batiza "time" e "msg" por default, e
// aceitar isso deixaria o log fora da spec por descuido de biblioteca.
func TestJSONUsaOsNomesDeCampoDoSpec95(t *testing.T) {
	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).
		Component("kafka").Dataset("radius_sessions")

	log.Info("consumer.subiu", "consumer no ar")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("linha não é JSON: %v (%q)", err, buf.String())
	}
	for _, campo := range []string{"timestamp", "level", "component", "dataset", "event", "message"} {
		if _, ok := m[campo]; !ok {
			t.Errorf("campo %q ausente; tem %v", campo, chaves(m))
		}
	}
	for _, proibido := range []string{"time", "msg"} {
		if _, ok := m[proibido]; ok {
			t.Errorf("campo %q presente; o §95 nomeia timestamp/message", proibido)
		}
	}
	if m["component"] != "kafka" || m["dataset"] != "radius_sessions" {
		t.Errorf("component/dataset = %v/%v", m["component"], m["dataset"])
	}
	if m["event"] != "consumer.subiu" || m["message"] != "consumer no ar" {
		t.Errorf("event/message = %v/%v", m["event"], m["message"])
	}
}

// Sem dataset o campo tem de sumir, não virar string vazia: um `dataset: ""`
// em toda linha de log de escopo global é ruído que atrapalha o grep.
func TestSemDatasetOCampoNaoAparece(t *testing.T) {
	var buf bytes.Buffer
	logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).
		Component("server").Info("server.subiu", "ok")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["dataset"]; ok {
		t.Errorf("dataset presente sem ter sido definido: %v", m["dataset"])
	}
}

func TestFormatoTextoTambemNomeiaOsCamposDoSpec95(t *testing.T) {
	var buf bytes.Buffer
	logging.New(&buf, logging.FormatoTexto, slog.LevelInfo).
		Component("storage").Info("recovery.divergencia", "header prevalece")

	s := buf.String()
	for _, campo := range []string{"timestamp=", "level=", "component=", "event=", "message="} {
		if !strings.Contains(s, campo) {
			t.Errorf("saída não traz %q: %s", campo, s)
		}
	}
	if strings.Contains(s, "time=") && !strings.Contains(s, "timestamp=") {
		t.Errorf("campo time= vazou: %s", s)
	}
}

// §95: "Não registrar payload completo por default. Não registrar senha."
// Este é o gate: o material sensível não pode chegar ao writer, venha ele por
// chave de atributo reconhecidamente sensível ou por valor marcado Segredo.
func TestSenhaEPayloadNaoChegamAoWriter(t *testing.T) {
	const senha = "trocar-esta-senha-agora"
	const corpo = `{"id":"A","Framed-IP-Address":"10.0.0.1"}`

	casos := []struct {
		nome  string
		attrs []any
	}{
		{"chave password", []any{"password", senha}},
		{"chave senha", []any{"senha", senha}},
		{"chave passwd", []any{"passwd", senha}},
		{"chave secret", []any{"secret", senha}},
		{"chave token", []any{"token", senha}},
		{"chave payload", []any{"payload", corpo}},
		{"chave evento", []any{"evento", corpo}},
		{"chave body", []any{"body", corpo}},
		{"valor Segredo", []any{"qualquer_coisa", logging.Segredo(senha)}},
		{"Segredo aninhado em grupo", []any{"kafka", slog.GroupValue(slog.Any("password", logging.Segredo(senha)))}},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			var buf bytes.Buffer
			logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).
				Component("kafka").Info("teste", "mensagem", c.attrs...)

			s := buf.String()
			if strings.Contains(s, senha) {
				t.Errorf("SENHA vazou para o log: %s", s)
			}
			if strings.Contains(s, "Framed-IP-Address") {
				t.Errorf("PAYLOAD vazou para o log: %s", s)
			}
			if !strings.Contains(s, logging.Redactado) {
				t.Errorf("valor não foi substituído pela marca de redação: %s", s)
			}
		})
	}
}

// A redação não pode ser tão ampla a ponto de apagar o que se precisa para
// operar: partição, offset e nome de env var têm de sobreviver.
func TestRedacaoNaoEngoleOsCamposDeOperacao(t *testing.T) {
	var buf bytes.Buffer
	logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).
		Component("kafka").Dataset("radius_sessions").
		Info("offset.commit", "commit no broker",
			"partition", 3, "offset", int64(918273), "usernameEnv", "KAFKA_USERNAME")

	s := buf.String()
	for _, quer := range []string{`"partition":3`, `"offset":918273`, "KAFKA_USERNAME"} {
		if !strings.Contains(s, quer) {
			t.Errorf("saída não traz %s: %s", quer, s)
		}
	}
}

func TestNivelFiltra(t *testing.T) {
	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatoJSON, slog.LevelWarn).Component("kafka")
	log.Info("ignorado", "não deve sair")
	if buf.Len() != 0 {
		t.Errorf("Info passou por um logger em nível Warn: %s", buf.String())
	}
	log.Warn("emitido", "deve sair")
	if !strings.Contains(buf.String(), "emitido") {
		t.Errorf("Warn não saiu: %s", buf.String())
	}
}

// Component e Dataset derivam um logger novo em vez de mutar o original: dois
// datasets compartilham o mesmo logger raiz e não podem contaminar um ao outro.
func TestDerivacaoNaoMutaOLoggerDeOrigem(t *testing.T) {
	var buf bytes.Buffer
	raiz := logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).Component("server")
	_ = raiz.Dataset("radius_sessions")

	raiz.Info("evento", "sem dataset")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["dataset"]; ok {
		t.Errorf("o logger de origem foi contaminado: dataset = %v", m["dataset"])
	}
}

func chaves(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Component e Dataset SUBSTITUEM, não acumulam.
//
// O defeito apareceu no primeiro percurso de operador: o servidor derivava
// Component("server") e o consumer derivava Component("kafka") do MESMO
// logger, e a linha saía com o campo duas vezes —
// `{"component":"server","component":"kafka"}`. Chave repetida em JSON tem
// comportamento indefinido: jq, Loki e Elasticsearch escolhem uma delas por
// critérios diferentes, então o mesmo log lido por duas ferramentas discorda
// sobre quem emitiu a linha.
func TestComponentEDatasetSubstituemEmVezDeAcumular(t *testing.T) {
	casos := []struct {
		nome   string
		deriva func(*logging.Logger) *logging.Logger
		campo  string
		quer   string
	}{
		{"component duas vezes", func(l *logging.Logger) *logging.Logger {
			return l.Component("server").Component("kafka")
		}, "component", "kafka"},
		{"dataset duas vezes", func(l *logging.Logger) *logging.Logger {
			return l.Component("kafka").Dataset("a").Dataset("b")
		}, "dataset", "b"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			var buf bytes.Buffer
			c.deriva(logging.New(&buf, logging.FormatoJSON, slog.LevelInfo)).
				Info("evento", "mensagem")

			linha := buf.String()
			if n := strings.Count(linha, `"`+c.campo+`":`); n != 1 {
				t.Errorf("campo %q apareceu %d vezes; chave repetida em JSON tem comportamento indefinido:\n%s",
					c.campo, n, linha)
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(linha), &m); err != nil {
				t.Fatal(err)
			}
			if m[c.campo] != c.quer {
				t.Errorf("%s = %v, quer %q (a derivação mais recente vence)", c.campo, m[c.campo], c.quer)
			}
		})
	}
}

// A derivação repetida não pode perder os atributos fixos do With: o consumer
// amarra partição e offset a um laço inteiro por ali.
func TestWithSobreviveANovaDerivacao(t *testing.T) {
	var buf bytes.Buffer
	logging.New(&buf, logging.FormatoJSON, slog.LevelInfo).
		Component("kafka").With("partition", 3).Dataset("d").
		Info("evento", "mensagem")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if m["partition"] != float64(3) {
		t.Errorf("partition = %v, quer 3: a derivação seguinte apagou o atributo fixo\n%s", m["partition"], buf.String())
	}
	if m["dataset"] != "d" || m["component"] != "kafka" {
		t.Errorf("component/dataset = %v/%v", m["component"], m["dataset"])
	}
}
