package units

import (
	"strings"
	"testing"
	"time"
)

func TestTamanhoAceitaAsUnidadesBinariasDaSpec(t *testing.T) {
	casos := []struct {
		entrada string
		quer    int64
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
	for _, c := range casos {
		got, err := Tamanho(c.entrada, 7)
		if err != nil {
			t.Errorf("Tamanho(%q) devolveu erro: %v", c.entrada, err)
			continue
		}
		if got != c.quer {
			t.Errorf("Tamanho(%q) = %d, quer %d", c.entrada, got, c.quer)
		}
	}
}

func TestTamanhoAusenteHerdaOPadrao(t *testing.T) {
	for _, entrada := range []string{"", "   ", "\t"} {
		got, err := Tamanho(entrada, 512<<20)
		if err != nil {
			t.Fatalf("Tamanho(%q) devolveu erro: %v", entrada, err)
		}
		if got != 512<<20 {
			t.Errorf("Tamanho(%q) = %d, quer o padrão %d", entrada, got, int64(512<<20))
		}
	}
}

// As unidades DECIMAIS são recusadas em vez de interpretadas: aceitar as duas
// grafias faria "512MB" e "512MiB" parecerem a mesma coisa e diferirem em 7%
// de disco.
func TestTamanhoRecusaUnidadeDecimal(t *testing.T) {
	for _, entrada := range []string{"512MB", "1GB", "1kB", "3TB"} {
		if _, err := Tamanho(entrada, 1); err == nil {
			t.Errorf("Tamanho(%q) foi aceito; unidade decimal tem de ser recusada", entrada)
		}
	}
}

func TestTamanhoRecusaNaoPositivoELixo(t *testing.T) {
	for _, entrada := range []string{"0", "0MiB", "-1", "-4GiB", "abc", "MiB", "1,5GiB", "1.5GiB"} {
		if _, err := Tamanho(entrada, 1); err == nil {
			t.Errorf("Tamanho(%q) foi aceito; deveria ser recusado", entrada)
		}
	}
}

// A mensagem tem de citar o valor recebido: quem lê o erro precisa saber o que
// escreveu de errado sem reabrir o YAML.
func TestTamanhoNomeiaOValorRecebido(t *testing.T) {
	_, err := Tamanho("512MB", 1)
	if err == nil {
		t.Fatal("quer erro")
	}
	if !strings.Contains(err.Error(), `"512MB"`) {
		t.Errorf("erro = %q, quer que cite o valor recebido", err)
	}
}

func TestDuracaoAceitaEHerdaOPadrao(t *testing.T) {
	got, err := Duracao("", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != 30*time.Second {
		t.Errorf("Duracao(\"\") = %s, quer 30s", got)
	}
	got, err = Duracao(" 100ms ", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100*time.Millisecond {
		t.Errorf("Duracao(\"100ms\") = %s, quer 100ms", got)
	}
}

func TestDuracaoRecusaNaoPositivaELixo(t *testing.T) {
	for _, entrada := range []string{"0s", "0", "-5s", "trinta", "30"} {
		if _, err := Duracao(entrada, time.Second); err == nil {
			t.Errorf("Duracao(%q) foi aceita; deveria ser recusada", entrada)
		}
	}
}

// Zero e negativo não são "sem valor": são teto que nenhuma consulta poderia
// satisfazer, e aceitá-los em silêncio faria a configuração mentir.
func TestInteiroPositivoSeparaAusenteDeZero(t *testing.T) {
	got, err := InteiroPositivo(nil, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100000 {
		t.Errorf("InteiroPositivo(nil) = %d, quer o padrão 100000", got)
	}

	for _, v := range []int{0, -1, -999} {
		if _, err := InteiroPositivo(&v, 100000); err == nil {
			t.Errorf("InteiroPositivo(%d) foi aceito; deveria ser recusado", v)
		}
	}

	v := 4242
	got, err = InteiroPositivo(&v, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if got != 4242 {
		t.Errorf("InteiroPositivo(4242) = %d, quer 4242", got)
	}
}
