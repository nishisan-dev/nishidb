package query_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// LIKE ponta a ponta: parser, binder, planner e executor sobre o pipeline
// real, com os dados entrando pelo Materializer.
func TestLikeFiltraPorPrefixoSufixoEContem(t *testing.T) {
	h := semeado(t)
	casos := []struct {
		sql  string
		quer []string
	}{
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE 'jo%'", []string{"CMP-0001", "CMP-0003"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE '%ia'", []string{"CMP-0002"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE '%a%'", []string{"CMP-0001", "CMP-0002", "CMP-0003", "CMP-0004"}}, // joao, maria, joao e ana têm 'a'
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE '%ri%'", []string{"CMP-0002"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE 'ana'", []string{"CMP-0004"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE '%'", []string{"CMP-0001", "CMP-0002", "CMP-0003", "CMP-0004"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE 'jo_o'", []string{"CMP-0001", "CMP-0003"}},
		{"SELECT id FROM radius_sessions WHERE \"User-Name\" LIKE 'zzz%'", nil},
	}
	for _, tc := range casos {
		got := chaves(h.consultaOK(tc.sql), "id")
		if strings.Join(got, ",") != strings.Join(tc.quer, ",") {
			t.Errorf("%s\n  got  = %v\n  quer = %v", tc.sql, got, tc.quer)
		}
	}
}

func TestNotLikeNega(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "User-Name" NOT LIKE 'jo%'`), "id")
	quer := []string{"CMP-0002", "CMP-0004"}
	if strings.Join(got, ",") != strings.Join(quer, ",") {
		t.Errorf("got = %v, quer %v", got, quer)
	}
}

// Campo AUSENTE compara false para todo operador, e NOT LIKE não é exceção.
//
// É a intuição SQL invertida e é deliberado: sem NOT genérico na v1 a lógica
// trivalente não tem por onde vazar, e um NOT LIKE "esperto" devolveria
// justamente as rows que o IS NOT NULL nega. Quem quer as linhas sem a coluna
// escreve IS NULL.
func TestNotLikeSobreCampoAusenteEhFalso(t *testing.T) {
	h := novoHarness(t)
	h.ingere(
		`{"id":"COM","_SEQ":1,"User-Name":"joao"}`,
		`{"id":"SEM","_SEQ":1,"Connect-Info":1}`,
	)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "User-Name" NOT LIKE 'zzz%'`), "id")
	if strings.Join(got, ",") != "COM" {
		t.Errorf("got = %v, quer só COM: a row sem a coluna não pode entrar por NOT LIKE", got)
	}
	// E o IS NULL, que é o jeito de pedir aquela linha, continua valendo.
	got = chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "User-Name" IS NULL`), "id")
	if strings.Join(got, ",") != "SEM" {
		t.Errorf("IS NULL devolveu %v, quer SEM", got)
	}
}

// LIKE com parâmetro: o padrão é compilado no BIND, a partir do parâmetro que
// o bind já recebe. Sem isto, PreparedStatement do driver não teria LIKE.
func TestLikeComParametro(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "User-Name" LIKE ?`,
		types.Value{Type: types.TypeString, Bytes: []byte("jo%")}), "id")
	quer := []string{"CMP-0001", "CMP-0003"}
	if strings.Join(got, ",") != strings.Join(quer, ",") {
		t.Errorf("got = %v, quer %v", got, quer)
	}
}

// LIKE sobre coluna não textual é recusado no BIND, com a posição da coluna —
// nunca resultado vazio, que pareceria resposta.
func TestLikeSobreColunaNaoTextualEhRecusadoNoBind(t *testing.T) {
	h := semeado(t)
	_, err := h.consulta(`SELECT id FROM radius_sessions WHERE "Connect-Info" LIKE '1%'`)
	if err == nil {
		t.Fatal("LIKE sobre coluna int64 foi aceito")
	}
	if !strings.Contains(err.Error(), "LIKE só vale para coluna de texto") {
		t.Errorf("mensagem = %q, quer nomear a regra", err)
	}
}

// O LIKE não vira termo de índice e degrada para full scan — mas o bitmap do
// OUTRO termo do AND continua valendo. O planner trata nó desconhecido como
// conjunct OPACO, então isto vale sem uma linha nova lá.
func TestBitmapContinuaValendoComLikeNoMesmoAnd(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions WHERE "Acct-Status-Type" = 'Stop' AND "User-Name" LIKE 'jo%'`), "id")
	quer := []string{"CMP-0001", "CMP-0003"}
	if strings.Join(got, ",") != strings.Join(quer, ",") {
		t.Errorf("got = %v, quer %v", got, quer)
	}
	if m := h.engine.Metrics(); m.IndexScans.Load() == 0 {
		t.Error("nenhum index scan: o LIKE derrubou o bitmap do outro termo")
	}
}

// A palavra `like` continua sendo identificador em toda posição em que a
// gramática aceita um: é o contrato publicado de que as palavras de cláusula
// não viraram keywords do lexer, e a prova de que o reconhecimento por posição
// está certo.
func TestColunaChamadaLikeContinuaValida(t *testing.T) {
	h := semeado(t)
	// Não existe coluna `like` no fixture; o que se afere é que o PARSER a
	// aceita como nome, e a recusa vem do BINDER, nomeando a coluna.
	_, err := h.consulta(`SELECT id FROM radius_sessions WHERE like = 1`)
	if err == nil {
		t.Fatal("esperado erro de coluna inexistente")
	}
	if strings.Contains(err.Error(), "esperado comparador") {
		t.Errorf("o parser tratou `like` como cláusula em posição de coluna: %v", err)
	}
	if !strings.Contains(err.Error(), "like") {
		t.Errorf("mensagem = %q, quer nomear a coluna `like`", err)
	}
}
