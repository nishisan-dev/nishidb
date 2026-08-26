package binder

import "testing"

func TestCasaPadroes(t *testing.T) {
	casos := []struct {
		pat, v string
		quer   bool
	}{
		// Sem curinga.
		{"abc", "abc", true},
		{"abc", "abcd", false},
		{"abc", "ab", false},
		{"", "", true},
		{"", "x", false},
		// Prefixo, sufixo, contém.
		{"sp-%", "sp-01", true},
		{"sp-%", "sp-", true},
		{"sp-%", "rj-01", false},
		{"%-hl4", "i-br-hl4", true},
		{"%-hl4", "i-br-hl4-01", false},
		{"%hl4%", "i-br-hl4-01", true},
		{"%hl4%", "i-br-hl5-01", false},
		// `%` sozinho casa tudo, inclusive vazio.
		{"%", "", true},
		{"%", "qualquer", true},
		{"%%", "qualquer", true},
		// Sublinhado.
		{"_", "a", true},
		{"_", "", false},
		{"_", "ab", false},
		{"a_c", "abc", true},
		{"a_c", "ac", false},
		{"a_c", "abbc", false},
		// Combinações que caem no glob geral.
		{"a%c", "abc", true},
		{"a%c", "ac", true},
		{"a%c", "abbbbc", true},
		{"a%c", "abbbbd", false},
		{"%a%b%", "xxaxxbxx", true},
		{"%a%b%", "xxbxxaxx", false},
		{"a%b%c", "a-b-c", true},
		{"a%_c", "abc", true},
		{"a%_c", "ac", false},
		// Backtrack: o primeiro casamento do `%` não serve, o segundo sim.
		{"%ab", "aab", true},
		{"%abc", "abxabc", true},
		{"a%a%a", "aaa", true},
		// UTF-8: `_` casa UM CARACTERE, não um byte. 'ç' tem 2 bytes.
		{"_", "ç", true},
		{"__", "ç", false},
		{"a_c", "açc", true},
		{"caç%", "cação", true},
		{"_ão", "cão", true},   // `_` casa o `c`, e `ão` casa `ão`
		{"__ão", "cão", false}, // exige dois caracteres antes de `ão`
		{"c_o", "cão", true},
		// Emoji tem 4 bytes e continua sendo UM caractere.
		{"_", "😀", true},
		{"a_b", "a😀b", true},
	}
	for _, tc := range casos {
		p := CompilaLike([]byte(tc.pat))
		if got := p.Casa([]byte(tc.v)); got != tc.quer {
			t.Errorf("CompilaLike(%q).Casa(%q) = %v, quer %v (tipo=%d)", tc.pat, tc.v, got, tc.quer, p.Tipo)
		}
	}
}

// A classificação existe para que o caminho por linha seja barato; se ela
// errar, o resultado continua certo (o glob geral casa tudo o que os atalhos
// casam) e o custo sobe em silêncio. Por isso é aferida à parte.
func TestClassificacaoDoPadrao(t *testing.T) {
	casos := []struct {
		pat  string
		quer TipoDePadrao
	}{
		{"abc", PadraoIgual},
		{"abc%", PadraoPrefixo},
		{"%abc", PadraoSufixo},
		{"%abc%", PadraoContem},
		{"%", PadraoTudo},
		{"%%", PadraoTudo},
		{"a%b%c", PadraoGeral},
		{"a_c", PadraoGeral},
		{"_%", PadraoGeral},
	}
	for _, tc := range casos {
		if got := CompilaLike([]byte(tc.pat)).Tipo; got != tc.quer {
			t.Errorf("CompilaLike(%q).Tipo = %d, quer %d", tc.pat, got, tc.quer)
		}
	}
}

// O piso de tamanho é uma rejeição O(1) e precisa errar para o lado
// PERMISSIVO: um piso alto demais produziria falso negativo, que é o defeito
// que este motor não tolera. `_` conta 1 byte porque é o menor tamanho de um
// caractere — e um valor de 2 bytes ainda pode casar `__`... não, casa `_`.
func TestPisoDeTamanhoNuncaProduzFalsoNegativo(t *testing.T) {
	casos := []struct{ pat, v string }{
		{"___", "ção"}, // 3 caracteres, 5 bytes
		{"_", "😀"},     // 1 caractere, 4 bytes
		{"a_c", "açc"}, // 3 caracteres, 4 bytes
		{"%_%", "ç"},   // 1 caractere, 2 bytes
	}
	for _, tc := range casos {
		p := CompilaLike([]byte(tc.pat))
		if len(tc.v) < p.MinBytes {
			t.Errorf("MinBytes de %q = %d rejeitaria %q (%d bytes), que CASA",
				tc.pat, p.MinBytes, tc.v, len(tc.v))
		}
		if !p.Casa([]byte(tc.v)) {
			t.Errorf("CompilaLike(%q).Casa(%q) = false, quer true", tc.pat, tc.v)
		}
	}
}

// Byte inválido de UTF-8 não pode entrar em laço infinito nem em panic: o
// matcher precisa ser TOTAL sobre qualquer entrada, porque o valor vem do
// payload e o payload vem da origem.
func TestValorNaoUtf8NaoTravaOMatcher(t *testing.T) {
	invalido := []byte{'a', 0xff, 0xfe, 'c'}
	for _, pat := range []string{"a_c", "a__c", "%", "a%c", "___", "a_%_c"} {
		p := CompilaLike([]byte(pat))
		// Qual das duas respostas sai não importa — o valor não é texto, e
		// nenhuma delas é "certa". O que importa é a função TERMINAR e ser
		// DETERMINÍSTICA: duas chamadas sobre a mesma entrada não podem
		// divergir, o que pegaria estado retido entre chamadas no matcher.
		primeira := p.Casa(invalido)
		segunda := p.Casa(invalido)
		if primeira != segunda {
			t.Errorf("CompilaLike(%q).Casa divergiu entre duas chamadas sobre a mesma entrada", pat)
		}
	}
}

func BenchmarkCasaPrefixo(b *testing.B) {
	p := CompilaLike([]byte("i-br-sp-%"))
	v := []byte("i-br-sp-cmp-cmp-hl4-01")
	b.ReportAllocs()
	for b.Loop() {
		if !p.Casa(v) {
			b.Fatal("não casou")
		}
	}
}

func BenchmarkCasaGeral(b *testing.B) {
	p := CompilaLike([]byte("i-%-cmp-%-hl_-01"))
	v := []byte("i-br-sp-cmp-cmp-hl4-01")
	b.ReportAllocs()
	for b.Loop() {
		if !p.Casa(v) {
			b.Fatal("não casou")
		}
	}
}
