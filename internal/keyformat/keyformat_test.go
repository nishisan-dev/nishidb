package keyformat

import (
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func str(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func i64(n int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: n} }
func boolv(b bool) types.Value {
	v := types.Value{Type: types.TypeBool}
	if b {
		v.I64 = 1
	}
	return v
}

func s(name string) Column { return Column{Name: name, Type: types.TypeString} }
func n(name string) Column { return Column{Name: name, Type: types.TypeInt64} }
func b(name string) Column { return Column{Name: name, Type: types.TypeBool} }

func render(t *testing.T, f *Format, vs ...types.Value) string {
	t.Helper()
	got, err := f.Append(nil, vs)
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

// TestMoldeDeclaradoEhGolden fixa o texto que sai de cada molde. Os esperados
// são literais: o groupKey é contrato com quem consulta, e derivá-lo da função
// faria o teste concordar com qualquer mudança.
func TestMoldeDeclaradoEhGolden(t *testing.T) {
	cases := []struct {
		name string
		mold string
		cols []Column
		vs   []types.Value
		text string
	}{
		{"o pedido da issue", "{olt}.{slot:02}", []Column{s("olt"), n("slot")},
			[]types.Value{str("BR_SPOAM_OLT"), i64(8)}, "BR_SPOAM_OLT.08"},
		{"literal nao e escapado dentro do valor", "OLT-{olt}.{slot:02}", []Column{s("olt"), n("slot")},
			[]types.Value{str("BR_SPOAM_OLT"), i64(8)}, "OLT-BR_SPOAM_OLT.08"},
		{"largura menor que o valor sai inteira", "{olt}.{slot:02}", []Column{s("olt"), n("slot")},
			[]types.Value{str("X"), i64(123)}, "X.123"},
		{"a largura conta o sinal", "{olt}.{slot:03}", []Column{s("olt"), n("slot")},
			[]types.Value{str("X"), i64(-8)}, "X.-08"},
		{"o separador escolhido e escapado no valor", "{olt}.{slot:02}", []Column{s("olt"), n("slot")},
			[]types.Value{str("BR.X"), i64(8)}, `BR\.X.08`},
		// O literal `-x-` tem dois caracteres distintos, e só o PRIMEIRO
		// protege a fronteira. Escapar os outros sujaria o texto sem nada
		// ganhar — e é o defeito que nenhum golde com literal de um caractere
		// só consegue enxergar.
		{"literal de mais de um caractere escapa so o primeiro", "{a}-x-{c}", []Column{s("a"), s("c")},
			[]types.Value{str("x"), str("y")}, "x-x-y"},
		{"o ultimo componente nao escapa o separador", "{a}.{c}", []Column{s("a"), s("c")},
			[]types.Value{str("x"), str("y.z")}, "x.y.z"},
		{"a barra invertida e sempre escapada", "{a}.{c}", []Column{s("a"), s("c")},
			[]types.Value{str(`x\y`), str(`z\w`)}, `x\\y.z\\w`},
		{"o molde reordena em relacao ao groupBy", "{slot:02}@{olt}", []Column{s("olt"), n("slot")},
			[]types.Value{str("X"), i64(8)}, "08@X"},
		{"sufixo e prefixo literais", "[{a}]", []Column{s("a")},
			[]types.Value{str("x")}, "[x]"},
		{"chaves literais", "{{{a}}}", []Column{s("a")},
			[]types.Value{str("x")}, "{x}"},
		{"boolean", "{a}/{c}", []Column{b("a"), b("c")},
			[]types.Value{boolv(true), boolv(false)}, "true/false"},
		{"um componente so escapa a barra invertida", "{a}", []Column{s("a")},
			[]types.Value{str("a|b.c")}, "a|b.c"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := Parse(c.mold, c.cols)
			if err != nil {
				t.Fatal(err)
			}
			if got := render(t, f, c.vs...); got != c.text {
				t.Fatalf("texto = %q, golden %q", got, c.text)
			}
			if f.String() != c.mold {
				t.Fatalf("String() = %q, esperado o molde declarado %q", f.String(), c.mold)
			}
		})
	}
}

// TestMoldeDefaultEhOSeparadorHistorico prova que quem não declara `keyFormat`
// continua vendo o texto de sempre — componentes separados por `|`, com o
// separador escapado dentro do valor. A exceção deliberada é o ÚLTIMO
// componente, que não escapa: ali não há fronteira a proteger.
func TestMoldeDefaultEhOSeparadorHistorico(t *testing.T) {
	cases := []struct {
		name string
		cols []Column
		vs   []types.Value
		text string
	}{
		{"um componente sai sem separador", []Column{s("olt")}, []types.Value{str("PRCTA_G1M10")}, "PRCTA_G1M10"},
		{"composto", []Column{s("olt"), n("slot"), n("port")},
			[]types.Value{str("PRCTA_G1M10"), i64(1), i64(9)}, "PRCTA_G1M10|1|9"},
		{"negativo", []Column{s("olt"), n("slot")}, []types.Value{str("A"), i64(-9)}, "A|-9"},
		{"boolean", []Column{b("a"), b("c")}, []types.Value{boolv(true), boolv(false)}, "true|false"},
		{"barra vertical escapada", []Column{s("a"), n("c")}, []types.Value{str("a|b"), i64(1)}, `a\|b|1`},
		{"barra invertida escapada", []Column{s("a")}, []types.Value{str(`x\y`)}, `x\\y`},
		{"string vazia", []Column{s("a"), n("c")}, []types.Value{str(""), i64(2)}, "|2"},
		{"o ultimo componente nao escapa", []Column{s("a"), s("c")}, []types.Value{str("a"), str("b|c")}, "a|b|c"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := Default(c.cols)
			if got := render(t, f, c.vs...); got != c.text {
				t.Fatalf("texto = %q, golden %q", got, c.text)
			}
		})
	}
}

// TestMoldeDefaultSeMostraExpandido garante que `_meta.counters` tem sempre um
// molde para mostrar, mesmo quando ninguém declarou um.
func TestMoldeDefaultSeMostraExpandido(t *testing.T) {
	if got := Default([]Column{s("olt"), n("slot"), n("port")}).String(); got != "{olt}|{slot}|{port}" {
		t.Fatalf("molde default = %q", got)
	}
	// Nome de coluna no NishiDB não é restrito a minúsculas, e o default não
	// pode recusar o que hoje funciona.
	if got := Default([]Column{s("Acct-Status-Type"), n("slot")}).String(); got != "{Acct-Status-Type}|{slot}" {
		t.Fatalf("molde default com nome fora do alfabeto = %q", got)
	}
}

func TestParseRecusaMoldeInvalido(t *testing.T) {
	cols := []Column{s("olt"), n("slot")}
	cases := []struct {
		name string
		mold string
		cols []Column
	}{
		{"vazio", "", cols},
		{"sem marcador nenhum", "fixo", cols},
		{"coluna do groupBy de fora", "{olt}", cols},
		{"coluna repetida", "{olt}.{olt}.{slot}", cols},
		{"nome que nao e coluna", "{olt}.{porta}.{slot}", cols},
		{"nome com espaco", "{ olt }.{slot}", cols},
		{"marcador sem nome", "{}.{olt}.{slot}", cols},
		{"marcadores colados", "{olt}{slot}", cols},
		{"literal com barra invertida", `{olt}\{slot}`, cols},
		{"prefixo com barra invertida", `\\{olt}.{slot}`, cols},
		{"literal com caractere de controle", "{olt}\n{slot}", cols},
		{"literal comeca com digito depois de int64", "{slot}0{olt}", cols},
		{"literal comeca com sinal depois de int64", "{slot}-{olt}", cols},
		{"zero a esquerda em string", "{olt:02}.{slot}", cols},
		{"zero a esquerda em boolean", "{olt}.{slot:02}", []Column{s("olt"), b("slot")}},
		{"largura sem o zero", "{olt}.{slot:2}", cols},
		{"largura zero", "{olt}.{slot:00}", cols},
		{"largura acima do teto", "{olt}.{slot:021}", cols},
		{"largura nao numerica", "{olt}.{slot:0x}", cols},
		{"chave sem fechar", "{olt}.{slot", cols},
		{"chave de fechar sozinha", "{olt}}.{slot}", cols},
		{"molde acima do teto", "{olt}." + string(make([]byte, MaxMoldBytes)) + "{slot}", cols},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := Parse(c.mold, c.cols)
			if err == nil {
				t.Fatalf("molde %q foi aceito e renderiza %q", c.mold, f.String())
			}
			t.Logf("recusa: %v", err)
		})
	}
}

// TestRenderizacaoEhInjetiva é a prova de que dois grupos distintos nunca
// rendem o mesmo texto — a invariante que sustenta o point lookup por groupKey.
//
// Não é golden: é exaustivo sobre um alfabeto que inclui de propósito o
// caractere de escape, os separadores dos moldes e a string vazia.
//
// A lista inclui os moldes PERIGOSOS, que hoje o Parse recusa — a invariante
// exercitada é "todo molde que o Parse ACEITA é injetivo", e não "os moldes que
// eu escolhi são injetivos". É essa forma que faz o teste acusar a queda de uma
// recusa: sem R5, `{a}\{b}` passa a compilar e colide para (`x\`,`y`) e
// (`x`,`\y`), que rendem os dois `x\\\y`. Um teste que só listasse molde
// válido passaria com o defeito presente e não provaria nada.
func TestRenderizacaoEhInjetiva(t *testing.T) {
	alfabeto := []string{"", "a", "b", `\`, "|", ".", ":", "-", `a\`, `\a`, "a|", "|a", "a.", ".a", "aa", "ab", "aba", "::", "a:b"}
	moldes := []struct {
		mold string
		cols []Column
	}{
		{"", []Column{s("a"), s("c")}},         // default
		{"", []Column{s("a"), s("c"), s("d")}}, // default de três
		{"{a}.{c}", []Column{s("a"), s("c")}},
		{"{a}::{c}", []Column{s("a"), s("c")}},
		{"{a}aa{c}", []Column{s("a"), s("c")}},
		{"{a}aba{c}", []Column{s("a"), s("c")}},
		{"[{a}]-[{c}]", []Column{s("a"), s("c")}},
		{"{a}.{c}.{d}", []Column{s("a"), s("c"), s("d")}},
		{"{a}.{c:02}", []Column{s("a"), n("c")}},
		{"{c:03}.{a}", []Column{s("a"), n("c")}},
		{"{a}.{c}", []Column{s("a"), b("c")}},
		// Perigosos: hoje recusados. Se algum passar a compilar, tem de
		// continuar injetivo — ou o teste acusa a colisão.
		{`{a}\{c}`, []Column{s("a"), s("c")}},
		{`{a}\\{c}`, []Column{s("a"), s("c")}},
		{"{a}{c}", []Column{s("a"), s("c")}},
		{"{c}0{a}", []Column{s("a"), n("c")}},
		{"{c}-{a}", []Column{s("a"), n("c")}},
	}
	for _, m := range moldes {
		t.Run(fmt.Sprintf("%s sobre %s", nomeDoMolde(m.mold), namesOf(m.cols)), func(t *testing.T) {
			f := Default(m.cols)
			if m.mold != "" {
				var err error
				if f, err = Parse(m.mold, m.cols); err != nil {
					t.Skipf("molde recusado, e a recusa é o que protege a invariante: %v", err)
				}
			}
			visto := make(map[string][]types.Value)
			buf := make([]byte, 0, 128)
			for _, vs := range tuplas(m.cols, alfabeto) {
				var err error
				if buf, err = f.Append(buf[:0], vs); err != nil {
					t.Fatal(err)
				}
				texto := string(buf)
				if antes, colide := visto[texto]; colide {
					t.Fatalf("molde %q: %s e %s rendem os dois %q", f.String(), mostra(antes), mostra(vs), texto)
				}
				visto[texto] = append([]types.Value(nil), vs...)
			}
		})
	}
}

func nomeDoMolde(m string) string {
	if m == "" {
		return "default"
	}
	return m
}

// tuplas devolve o produto cartesiano do alfabeto sobre as colunas. Para
// int64 e boolean o domínio é fixo: o que interessa nesses tipos é o sinal, o
// zero e a fronteira com o literal.
func tuplas(cols []Column, alfabeto []string) [][]types.Value {
	out := [][]types.Value{{}}
	for _, col := range cols {
		var dominio []types.Value
		switch col.Type {
		case types.TypeString:
			for _, a := range alfabeto {
				dominio = append(dominio, str(a))
			}
		case types.TypeInt64:
			for _, v := range []int64{0, 1, 8, -8, 12, 123, -123, 1000} {
				dominio = append(dominio, i64(v))
			}
		case types.TypeBool:
			dominio = append(dominio, boolv(true), boolv(false))
		}
		prox := make([][]types.Value, 0, len(out)*len(dominio))
		for _, prefixo := range out {
			for _, v := range dominio {
				linha := make([]types.Value, len(prefixo), len(prefixo)+1)
				copy(linha, prefixo)
				prox = append(prox, append(linha, v))
			}
		}
		out = prox
	}
	return out
}

func mostra(vs []types.Value) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		switch v.Type {
		case types.TypeString:
			out[i] = fmt.Sprintf("%q", v.Bytes)
		default:
			out[i] = fmt.Sprintf("%d", v.I64)
		}
	}
	return "(" + fmt.Sprint(out) + ")"
}

// TestAppendRespeitaOBufferDoChamador: a renderização roda a cada grupo novo no
// writer e sobre todos os grupos na abertura; alocar ali seria pressão de heap
// proporcional ao inventário.
func TestAppendRespeitaOBufferDoChamador(t *testing.T) {
	f, err := Parse("{olt}.{slot:02}", []Column{s("olt"), n("slot")})
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	vs := []types.Value{str("BR_SPOAM_OLT"), i64(8)}
	got, err := f.Append(dst, vs)
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != cap(dst) || &got[:1][0] != &dst[:1][0] {
		t.Fatal("Append alocou o texto em vez de usar o buffer do chamador")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		dst, _ = f.Append(dst[:0], vs)
	}); allocs != 0 {
		t.Errorf("Append alocou %v vezes por chamada; o esperado é zero", allocs)
	}
}

func TestAppendRecusaAridadeEZTipoSemRenderizacao(t *testing.T) {
	f := Default([]Column{s("a"), n("c")})
	if _, err := f.Append(nil, []types.Value{str("x")}); err == nil {
		t.Fatal("valores a menos que o molde tem de ser recusado")
	}
	if _, err := f.Append(nil, []types.Value{str("x"), {Type: types.TypeTimestamp, I64: 1}}); err == nil {
		t.Fatal("tipo fora de string/int64/boolean não tem renderização declarada")
	}
}
