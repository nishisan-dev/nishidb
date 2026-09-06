package units

import (
	"strings"
	"testing"
	"time"
)

func TestTamanhoAceitaAsUnidadesBinariasDaSpec(t *testing.T) {
	cases := []struct {
		input string
		want  int64
	}{
		{"512MiB", 512 << 20},
		{"1GiB", 1 << 30},
		{"64KiB", 64 << 10},
		{"2TiB", 2 << 40},
		{"4096B", 4096},
		{"4096", 4096},
		{"  1GiB  ", 1 << 30},
		{"1 GiB", 1 << 30}, // espaço entre número e sufixo
	}
	for _, c := range cases {
		got, err := Size(c.input, 7)
		if err != nil {
			t.Errorf("Tamanho(%q) devolveu erro: %v", c.input, err)
			continue
		}
		if got != c.want {
			t.Errorf("Tamanho(%q) = %d, quer %d", c.input, got, c.want)
		}
	}
}

func TestTamanhoAusenteHerdaOPadrao(t *testing.T) {
	for _, input := range []string{"", "   ", "\t"} {
		got, err := Size(input, 512<<20)
		if err != nil {
			t.Fatalf("Tamanho(%q) devolveu erro: %v", input, err)
		}
		if got != 512<<20 {
			t.Errorf("Tamanho(%q) = %d, quer o padrão %d", input, got, int64(512<<20))
		}
	}
}

// As unidades DECIMAIS são recusadas em vez de interpretadas: aceitar as duas
// grafias faria "512MB" e "512MiB" parecerem a mesma coisa e diferirem em 7%
// de disco.
func TestTamanhoRecusaUnidadeDecimal(t *testing.T) {
	for _, input := range []string{"512MB", "1GB", "1kB", "3TB"} {
		if _, err := Size(input, 1); err == nil {
			t.Errorf("Tamanho(%q) foi aceito; unidade decimal tem de ser recusada", input)
		}
	}
}

func TestTamanhoRecusaNaoPositivoELixo(t *testing.T) {
	for _, input := range []string{"0", "0MiB", "-1", "-4GiB", "abc", "MiB", "1,5GiB", "1.5GiB"} {
		if _, err := Size(input, 1); err == nil {
			t.Errorf("Tamanho(%q) foi aceito; deveria ser recusado", input)
		}
	}
}

// A mensagem tem de citar o valor recebido: quem lê o erro precisa saber o que
// escreveu de errado sem reabrir o YAML.
func TestTamanhoNomeiaOValorRecebido(t *testing.T) {
	_, err := Size("512MB", 1)
	if err == nil {
		t.Fatal("quer erro")
	}
	if !strings.Contains(err.Error(), `"512MB"`) {
		t.Errorf("erro = %q, quer que cite o valor recebido", err)
	}
}

func TestDuracaoAceitaEHerdaOPadrao(t *testing.T) {
	got, err := Duration("", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != 30*time.Second {
		t.Errorf("Duracao(\"\") = %s, quer 30s", got)
	}
	got, err = Duration(" 100ms ", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100*time.Millisecond {
		t.Errorf("Duracao(\"100ms\") = %s, quer 100ms", got)
	}
}

func TestDuracaoRecusaNaoPositivaELixo(t *testing.T) {
	for _, input := range []string{"0s", "0", "-5s", "trinta", "30"} {
		if _, err := Duration(input, time.Second); err == nil {
			t.Errorf("Duracao(%q) foi aceita; deveria ser recusada", input)
		}
	}
}

// Zero e negativo não são "sem valor": são teto que nenhuma consulta poderia
// satisfazer, e aceitá-los em silêncio faria a configuração mentir.
func TestInteiroPositivoSeparaAusenteDeZero(t *testing.T) {
	got, err := PositiveInt(nil, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100000 {
		t.Errorf("InteiroPositivo(nil) = %d, quer o padrão 100000", got)
	}

	for _, v := range []int{0, -1, -999} {
		if _, err := PositiveInt(&v, 100000); err == nil {
			t.Errorf("InteiroPositivo(%d) foi aceito; deveria ser recusado", v)
		}
	}

	v := 4242
	got, err = PositiveInt(&v, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if got != 4242 {
		t.Errorf("InteiroPositivo(4242) = %d, quer 4242", got)
	}
}

func TestDuracaoComDias(t *testing.T) {
	ok := map[string]time.Duration{
		"7d":     7 * 24 * time.Hour,
		"1d":     24 * time.Hour,
		"1d12h":  36 * time.Hour,
		"36h":    36 * time.Hour,
		"30s":    30 * time.Second,
		"1d0h1m": 24*time.Hour + time.Minute,
	}
	for in, want := range ok {
		got, err := DurationWithDays(in, 0)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("%q = %v, quer %v", in, got, want)
		}
	}

	// Recusa em vez de adivinhar: reordenar "12h1d" seria inventar intenção.
	bad := []string{"12h1d", "1d2d", "-1d", "0d", "d", "7dias", "7 d", "0s"}
	for _, in := range bad {
		if got, err := DurationWithDays(in, 0); err == nil {
			t.Errorf("%q foi aceito como %v", in, got)
		}
	}

	// Vazio devolve o padrão, como a Duracao.
	if got, err := DurationWithDays("", 5*time.Minute); err != nil || got != 5*time.Minute {
		t.Errorf(`"" = %v, %v; quer o padrão`, got, err)
	}
}

// A fixture normativa do §12 declara 7d: se esta gramática deixar de aceitá-la,
// o YAML que a própria spec documenta para de carregar.
func TestGramaticaAceitaAJanelaDoSpec12(t *testing.T) {
	if _, err := DurationWithDays("7d", 0); err != nil {
		t.Fatalf("tombstoneRetention do §12 recusado: %v", err)
	}
}
