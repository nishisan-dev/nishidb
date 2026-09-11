package cli

import "testing"

// O --limit reescreve a query do usuário, e reescrever texto SQL só é honesto
// se quem separa cláusula de literal for a gramática de verdade. A primeira
// versão reconhecia apenas a aspa SIMPLES como delimitador — mas a aspa DUPLA
// é obrigatória para os nomes reais do dataset ("Acct-Status-Type" tem hífen),
// e um identificador citado contendo a palavra limit fazia o corte cair DENTRO
// dele: a query executada deixava de ser a query escrita.
func TestComLimitNaoCortaDentroDeLiteralNemDeIdentificador(t *testing.T) {
	casos := []struct {
		name string
		sql  string
		n    int
		want string
	}{
		{
			name: "sem limit na query",
			sql:  "SELECT id FROM t",
			n:    5,
			want: "SELECT id FROM t LIMIT 5",
		},
		{
			name: "limit da query é substituído",
			sql:  "SELECT id FROM t LIMIT 3",
			n:    5,
			want: "SELECT id FROM t LIMIT 5",
		},
		{
			name: "ponto e vírgula final",
			sql:  "SELECT id FROM t;",
			n:    2,
			want: "SELECT id FROM t LIMIT 2",
		},
		{
			name: "limit dentro de literal de string",
			sql:  "SELECT id FROM t WHERE s = 'sem limit aqui'",
			n:    5,
			want: "SELECT id FROM t WHERE s = 'sem limit aqui' LIMIT 5",
		},
		{
			name: "limit dentro de identificador entre aspas duplas",
			sql:  `SELECT "no limit aqui" FROM t`,
			n:    5,
			want: `SELECT "no limit aqui" FROM t LIMIT 5`,
		},
		{
			name: "identificador citado E cláusula de verdade",
			sql:  `SELECT "no limit aqui" FROM t LIMIT 9`,
			n:    5,
			want: `SELECT "no limit aqui" FROM t LIMIT 5`,
		},
		{
			name: "aspa dupla escapada dentro do identificador",
			sql:  `SELECT "a"" limit b" FROM t`,
			n:    5,
			want: `SELECT "a"" limit b" FROM t LIMIT 5`,
		},
		{
			name: "aspa simples escapada dentro do literal",
			sql:  `SELECT id FROM t WHERE s = 'a'' limit b'`,
			n:    5,
			want: `SELECT id FROM t WHERE s = 'a'' limit b' LIMIT 5`,
		},
		{
			name: "limit em minúsculas é cláusula",
			sql:  "SELECT id FROM t limit 3",
			n:    5,
			want: "SELECT id FROM t LIMIT 5",
		},
		{
			name: "limite não é LIMIT",
			sql:  `SELECT "limite" FROM t`,
			n:    5,
			want: `SELECT "limite" FROM t LIMIT 5`,
		},
		// Os três casos abaixo são o defeito que o OFFSET expôs, e o primeiro
		// é o grave: o corte antigo ia "da última cláusula LIMIT em diante" e
		// levava o OFFSET junto. A paginação EVAPORAVA, e o comando devolvia as
		// N PRIMEIRAS linhas como se fossem a página pedida — sem erro e sem
		// aviso, que é resultado errado com cara de certo.
		{
			name: "offset é PRESERVADO ao trocar o limit",
			sql:  "SELECT id FROM t LIMIT 5 OFFSET 10",
			n:    3,
			want: "SELECT id FROM t LIMIT 3 OFFSET 10",
		},
		{
			// Aqui o corte antigo anexava `LIMIT 3` DEPOIS do OFFSET, gerando
			// `... OFFSET 10 LIMIT 3` — ordem inválida. O usuário recebia erro
			// de sintaxe sobre um texto que não escreveu.
			name: "offset sem limit ganha o limit ANTES dele",
			sql:  "SELECT id FROM t OFFSET 10",
			n:    3,
			want: "SELECT id FROM t LIMIT 3 OFFSET 10",
		},
		{
			name: "offset com where e ponto e vírgula",
			sql:  "SELECT id FROM t WHERE a = 1 LIMIT 5 OFFSET 2;",
			n:    9,
			want: "SELECT id FROM t WHERE a = 1 LIMIT 9 OFFSET 2",
		},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			if got := withLimit(c.sql, c.n); got != c.want {
				t.Errorf("comLimit(%q, %d)\n got %q\nwant %q", c.sql, c.n, got, c.want)
			}
		})
	}
}

// Query que nem tokeniza (aspa sem fechamento) não pode ser CORTADA por
// adivinhação: sem gramática íntegra não há como distinguir cláusula de
// literal. O --limit é anexado e o erro de sintaxe sai do Execute, com a
// posição apontando para a aspa aberta — cortar ali produziria um texto que o
// usuário nunca escreveu e um erro em posição inventada.
func TestComLimitNaoCortaQueryQueNaoTokeniza(t *testing.T) {
	for _, sql := range []string{
		// A aspa abre e nunca fecha: tudo depois dela é território
		// desconhecido.
		`SELECT "a limit b FROM t`,
		// Aqui existe um LIMIT legítimo ANTES do trecho que não tokeniza.
		// Cortar nele apagaria silenciosamente o resto da query em vez de
		// deixar o erro de sintaxe aparecer.
		`SELECT a FROM t LIMIT 3 AND s = 'x`,
	} {
		if got, want := withLimit(sql, 5), sql+" LIMIT 5"; got != want {
			t.Errorf("comLimit(%q, 5)\n got %q\nwant %q", sql, got, want)
		}
	}
}
