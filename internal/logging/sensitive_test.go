package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// TestIsSensitiveKeyConcordaComOHandler é o ponto do exercício: a função e a
// supressão do handler têm de responder a MESMA coisa para todo nome.
//
// Ela existe porque o handler não alcança tudo. A redação percorre atributos e
// grupos, mas NÃO entra num valor serializado por slog.Any — ali o
// encoding/json toma conta. Quem monta um valor estruturado a partir de nomes
// que vêm do YAML do cliente (as colunas de um enriquecimento de alarme, por
// exemplo) precisa aplicar a mesma regra na montagem, e duas listas divergiriam
// na primeira correção.
func TestIsSensitiveKeyConcordaComOHandler(t *testing.T) {
	nomes := []string{
		"password", "passwd", "senha", "secret", "segredo", "token", "apikey",
		"payload", "evento", "event_payload", "body", "record", "value",
		"PASSWORD", "Senha", "ToKeN",
		"olt", "slot", "customerId", "total", "groupKey",
		"tokenizer_erros", "senha_invalida_total", "usernameEnv", "passwordEnv",
	}
	for _, nome := range nomes {
		quero := handlerSuprime(t, nome)
		if got := logging.IsSensitiveKey(nome); got != quero {
			t.Errorf("IsSensitiveKey(%q) = %v, mas o handler %s",
				nome, got, map[bool]string{true: "suprime", false: "não suprime"}[quero])
		}
	}
}

// handlerSuprime diz se o handler de verdade troca o valor daquela chave pelo
// texto de redação — é o oráculo, e não uma segunda cópia da lista.
func handlerSuprime(t *testing.T, key string) bool {
	t.Helper()
	var buf bytes.Buffer
	logging.New(&buf, logging.FormatJSON, slog.LevelDebug).Info("teste", "msg", key, "valor-original")
	var linha map[string]any
	if err := json.Unmarshal(buf.Bytes(), &linha); err != nil {
		t.Fatal(err)
	}
	return linha[key] == logging.Redacted
}

// TestIsSensitiveKeyIgnoraCaixa: o nome de coluna vem do YAML e ninguém garante
// a caixa.
func TestIsSensitiveKeyIgnoraCaixa(t *testing.T) {
	for _, nome := range []string{"Token", "TOKEN", "tOkEn"} {
		if !logging.IsSensitiveKey(nome) {
			t.Errorf("IsSensitiveKey(%q) = false", nome)
		}
	}
}

// TestIsSensitiveKeyNaoUsaContains: um `contains` apagaria contadores que se
// quer ver, e a razão já está registrada no godoc da lista.
func TestIsSensitiveKeyNaoUsaContains(t *testing.T) {
	for _, nome := range []string{"tokenizer_erros", "senha_invalida_total", "valueOf", "recordCount"} {
		if logging.IsSensitiveKey(nome) {
			t.Errorf("IsSensitiveKey(%q) = true: a comparação virou `contains` e apagou um contador", nome)
		}
	}
	if !strings.EqualFold("token", "TOKEN") {
		t.Fatal("sanidade")
	}
}
