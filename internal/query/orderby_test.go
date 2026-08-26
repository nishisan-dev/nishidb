package query_test

import (
	"strings"
	"testing"
)

func TestOrderByOrdenaNasDuasDirecoes(t *testing.T) {
	h := semeado(t)
	casos := []struct {
		sql  string
		quer []string
	}{
		{`SELECT id FROM radius_sessions ORDER BY "Acct-Session-Time"`,
			[]string{"CMP-0002", "CMP-0003", "CMP-0004", "CMP-0001"}},
		{`SELECT id FROM radius_sessions ORDER BY "Acct-Session-Time" DESC`,
			[]string{"CMP-0001", "CMP-0004", "CMP-0003", "CMP-0002"}},
		{`SELECT id FROM radius_sessions ORDER BY "User-Name"`,
			[]string{"CMP-0004", "CMP-0001", "CMP-0003", "CMP-0002"}},
		{`SELECT id FROM radius_sessions ORDER BY "User-Name" DESC`,
			[]string{"CMP-0002", "CMP-0001", "CMP-0003", "CMP-0004"}},
	}
	for _, tc := range casos {
		got := chaves(h.consultaOK(tc.sql), "id")
		if strings.Join(got, ",") != strings.Join(tc.quer, ",") {
			t.Errorf("%s\n  got  = %v\n  quer = %v", tc.sql, got, tc.quer)
		}
	}
}

// Duas chaves: a segunda decide dentro do empate da primeira.
func TestOrderByComDuasChaves(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions ORDER BY "User-Name", "Acct-Session-Time" DESC`), "id")
	// joao empata; dentro dele, 3600 (CMP-0001) vem antes de 120 (CMP-0003).
	quer := []string{"CMP-0004", "CMP-0001", "CMP-0003", "CMP-0002"}
	if strings.Join(got, ",") != strings.Join(quer, ",") {
		t.Errorf("got = %v, quer %v", got, quer)
	}
}

// O DESEMPATE por rowID é o que torna a paginação correta: sem ordem TOTAL,
// duas páginas de um resultado com empates podem se sobrepor e omitir linhas.
func TestEmpateDesempataPorRowIDCrescente(t *testing.T) {
	h := novoHarness(t)
	h.ingere(
		`{"id":"A","_SEQ":1,"User-Name":"igual","Connect-Info":1}`,
		`{"id":"B","_SEQ":1,"User-Name":"igual","Connect-Info":2}`,
		`{"id":"C","_SEQ":1,"User-Name":"igual","Connect-Info":3}`,
	)
	got := chaves(h.consultaOK(`SELECT id FROM radius_sessions ORDER BY "User-Name"`), "id")
	if strings.Join(got, ",") != "A,B,C" {
		t.Errorf("got = %v, quer A,B,C (desempate por rowID crescente)", got)
	}
	// E com DESC a chave inverte, mas o desempate NÃO: as três continuam em
	// rowID crescente entre si, porque só a comparação da CHAVE é invertida.
	got = chaves(h.consultaOK(`SELECT id FROM radius_sessions ORDER BY "User-Name" DESC`), "id")
	if strings.Join(got, ",") != "A,B,C" {
		t.Errorf("got = %v com DESC, quer A,B,C", got)
	}
}

// Campo AUSENTE vai por ÚLTIMO nas DUAS direções.
//
// Diverge de Postgres e Oracle (que invertem no DESC) e de MySQL e SQLite (que
// tratam NULL como o menor), e é deliberado: é o único default em que
// `ORDER BY x DESC LIMIT 10` significa "os 10 maiores" e não "10 linhas sem
// valor". NULLS FIRST existe para quem quiser a convenção alheia.
func TestAusenteVaiPorUltimoNasDuasDirecoes(t *testing.T) {
	h := novoHarness(t)
	h.ingere(
		`{"id":"COM-B","_SEQ":1,"User-Name":"b"}`,
		`{"id":"SEM","_SEQ":1,"Connect-Info":1}`,
		`{"id":"COM-A","_SEQ":1,"User-Name":"a"}`,
	)
	casos := []struct{ sql, quer string }{
		{`SELECT id FROM radius_sessions ORDER BY "User-Name"`, "COM-A,COM-B,SEM"},
		{`SELECT id FROM radius_sessions ORDER BY "User-Name" DESC`, "COM-B,COM-A,SEM"},
		{`SELECT id FROM radius_sessions ORDER BY "User-Name" NULLS FIRST`, "SEM,COM-A,COM-B"},
		{`SELECT id FROM radius_sessions ORDER BY "User-Name" DESC NULLS FIRST`, "SEM,COM-B,COM-A"},
	}
	for _, tc := range casos {
		got := strings.Join(chaves(h.consultaOK(tc.sql), "id"), ",")
		if got != tc.quer {
			t.Errorf("%s\n  got  = %s\n  quer = %s", tc.sql, got, tc.quer)
		}
	}
}

// LIMIT sobre ORDER BY corta um PREFIXO LEGÍTIMO da resposta ordenada, e não
// "as N primeiras por rowId, depois ordenadas" — que seria resultado errado com
// cara de certo.
func TestLimitSobreOrderByCortaOPrefixoDaOrdem(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions ORDER BY "Acct-Session-Time" DESC LIMIT 2`), "id")
	if strings.Join(got, ",") != "CMP-0001,CMP-0004" {
		t.Errorf("got = %v, quer os DOIS MAIORES", got)
	}
}

// O OFFSET é aplicado DEPOIS de ordenar. Pular antes ordenaria o conjunto
// errado e devolveria uma página que não é página de nada.
func TestOffsetSobreOrderByPulaDepoisDeOrdenar(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(
		`SELECT id FROM radius_sessions ORDER BY "Acct-Session-Time" DESC LIMIT 2 OFFSET 1`), "id")
	if strings.Join(got, ",") != "CMP-0004,CMP-0003" {
		t.Errorf("got = %v, quer a segunda página da ordem decrescente", got)
	}
}

// A ordem de saída SEM ORDER BY continua sendo rowID crescente. Não é contrato
// de API, mas os testes dependem dela e uma mudança silenciosa aqui quebraria
// todos de uma vez.
func TestOrdemDeSaidaSemOrderByContinuaSendoRowIDCrescente(t *testing.T) {
	h := semeado(t)
	got := chaves(h.consultaOK(`SELECT id FROM radius_sessions`), "id")
	if strings.Join(got, ",") != "CMP-0001,CMP-0002,CMP-0003,CMP-0004" {
		t.Errorf("got = %v", got)
	}
}

// Ordenar por coluna FORA da projeção vale: a chave entra no conjunto de slots
// a coletar como qualquer outra. Sem o b.usa() do binder, a coluna não teria
// slot e o executor ordenaria por um valor que nunca leu.
func TestOrderByPorColunaForaDaProjecao(t *testing.T) {
	h := semeado(t)
	rows := h.consultaOK(`SELECT id FROM radius_sessions ORDER BY "Acct-Session-Time" DESC`)
	if got := chaves(rows, "id"); strings.Join(got, ",") != "CMP-0001,CMP-0004,CMP-0003,CMP-0002" {
		t.Errorf("got = %v", got)
	}
	// E a coluna de ordenação NÃO vaza para a saída.
	if _, tem := rows[0]["Acct-Session-Time"]; tem {
		t.Error("a coluna de ordenação apareceu na projeção")
	}
}

func TestOrderBySobreTipoSemOrdemEhRecusado(t *testing.T) {
	h := semeado(t)
	for _, sql := range []string{
		`SELECT id FROM radius_sessions ORDER BY "NAS-IP-Address-Prefix"`,
		`SELECT id FROM radius_sessions ORDER BY 1`,
	} {
		if _, err := h.consulta(sql); err == nil {
			t.Errorf("%s foi aceito", sql)
		}
	}
}

func TestOrderByComColunaRepetidaEhRecusado(t *testing.T) {
	h := semeado(t)
	_, err := h.consulta(`SELECT id FROM radius_sessions ORDER BY "User-Name", "User-Name" DESC`)
	if err == nil {
		t.Fatal("coluna repetida no ORDER BY foi aceita")
	}
	if !strings.Contains(err.Error(), "duas vezes") {
		t.Errorf("mensagem = %q", err)
	}
}
