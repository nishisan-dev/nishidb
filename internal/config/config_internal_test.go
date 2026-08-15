package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// DefaultsDeQuery é a costura entre o nishidb.yaml e o schema: é por ela que a
// chave global chega ao dataset. O teto tem teste fim-a-fim pelo binário
// (o truncamento é observável no exit code); o TIMEOUT é medido aqui, na
// costura, e não pelo relógio.
//
// A razão é deliberada: um teste que dependesse de um deadline de nanossegundos
// vencer antes da primeira checagem do executor seria intermitente por
// construção. A cadeia já está fechada sem isso — daqui até spec.Query.Timeout
// é TestDatasetSemQueryHerdaOsDefaultsGlobais (internal/schema), e de
// spec.Query.Timeout até o cancelamento da consulta é TestTimeoutDoDataset
// (internal/query), que usa atraso injetado em vez de esperar relógio.

func escreveConfig(t *testing.T, corpo string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "nishidb.yaml")
	cabecalho := "version: 1\nstorage:\n  path: " + dir +
		"\ndatasets:\n  directory: " + dir + "\n"
	if err := os.WriteFile(path, []byte(cabecalho+corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsDeQueryCarregaAsChavesGlobais(t *testing.T) {
	path := escreveConfig(t, "query:\n  defaultMaxRows: 4242\n  defaultTimeout: 7s\n")
	c, err := CarregaConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	d := c.DefaultsDeQuery()
	if d.MaxRows != 4242 {
		t.Errorf("MaxRows = %d, want 4242", d.MaxRows)
	}
	if d.Timeout != 7*time.Second {
		t.Errorf("Timeout = %s, want 7s", d.Timeout)
	}
}

func TestDefaultsDeQuerySemBlocoUsaOsPadroes(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	d := c.DefaultsDeQuery()
	if d.MaxRows != maxRowsPadrao || d.Timeout != timeoutPadrao {
		t.Errorf("DefaultsDeQuery() = %+v, want {%d %s}", d, maxRowsPadrao, timeoutPadrao)
	}
}

// Valor não positivo é recusado com a chave nomeada, em vez de virar "ausente".
func TestDefaultMaxRowsNaoPositivoFalhaNoCarregamento(t *testing.T) {
	for _, valor := range []string{"0", "-1", "-100000"} {
		t.Run(valor, func(t *testing.T) {
			_, err := CarregaConfig(escreveConfig(t, "query:\n  defaultMaxRows: "+valor+"\n"))
			if err == nil {
				t.Fatalf("defaultMaxRows: %s foi aceito", valor)
			}
			if !strings.Contains(err.Error(), "query.defaultMaxRows") ||
				!strings.Contains(err.Error(), valor) {
				t.Errorf("erro = %v, want nomear a chave e o valor", err)
			}
		})
	}
}

// O default do bind é LOOPBACK desde a Fase 6, e essa é uma decisão de
// segurança, não de gosto: com POST /v1/query no ar e sem autenticação até a
// Fase 11, um default aberto entregaria o dataset a quem alcançasse a porta.
func TestOBindPadraoEhLoopback(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Bind != "127.0.0.1" {
		t.Fatalf("bind padrão = %q, quer 127.0.0.1", c.Server.Bind)
	}
	if c.Endereco() != "127.0.0.1:9142" {
		t.Fatalf("Endereco = %q", c.Endereco())
	}
	if c.BindExposto() {
		t.Fatal("o bind padrão foi classificado como exposto")
	}
}

func TestBindExpostoReconheceOsEnderecosQueAlcancamARede(t *testing.T) {
	casos := []struct {
		bind    string
		exposto bool
	}{
		{"127.0.0.1", false},
		{"::1", false},
		{"localhost", false},
		{"0.0.0.0", true},
		{"::", true},
		{"10.0.0.5", true},
		// Nome que não resolve para IP conta como exposto: o servidor vai
		// bindar naquilo, e presumir inocência do que não se sabe qual é seria
		// errar para o lado errado.
		{"db01.interno", true},
	}
	for _, c := range casos {
		t.Run(c.bind, func(t *testing.T) {
			cfg, err := CarregaConfig(escreveConfig(t, "server:\n  bind: \""+c.bind+"\"\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.BindExposto(); got != c.exposto {
				t.Fatalf("BindExposto(%q) = %v, quer %v", c.bind, got, c.exposto)
			}
		})
	}
}

// O teto de consultas simultâneas do §80 tem leitor, e a chave segue a mesma
// regra de todo inteiro positivo do arquivo: ausente herda o padrão, zero e
// negativo são recusados com a chave e o valor na mensagem.
func TestMaxConcurrentTemLeitorEValidacao(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxConcurrent() != 16 {
		t.Fatalf("MaxConcurrent padrão = %d, quer 16", c.MaxConcurrent())
	}

	c, err = CarregaConfig(escreveConfig(t, "runtime:\n  query:\n    maxConcurrent: 4\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxConcurrent() != 4 {
		t.Fatalf("MaxConcurrent = %d, quer 4", c.MaxConcurrent())
	}

	_, err = CarregaConfig(escreveConfig(t, "runtime:\n  query:\n    maxConcurrent: 0\n"))
	if err == nil {
		t.Fatal("maxConcurrent: 0 deveria ser recusado")
	}
	if !strings.Contains(err.Error(), "runtime.query.maxConcurrent") {
		t.Fatalf("a mensagem não nomeia a chave: %v", err)
	}
}

// Chave aceita e não implementada não pode ficar em silêncio: quem escreve
// memoryTarget dimensiona a máquina por ela (débito 15).
func TestChavesDeRuntimeSemImplementacaoSaoNomeadas(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t,
		"runtime:\n  memoryTarget: 1GiB\n  managedMemoryTarget: 512MiB\n  query:\n    maxBufferPerQuery: 4MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	quer := []string{"runtime.memoryTarget", "runtime.managedMemoryTarget", "runtime.query.maxBufferPerQuery"}
	got := c.ChavesDeRuntimeIgnoradas()
	if len(got) != len(quer) {
		t.Fatalf("chaves = %v, quer %v", got, quer)
	}
	for i := range quer {
		if got[i] != quer[i] {
			t.Fatalf("chaves = %v, quer %v", got, quer)
		}
	}

	// O bloco §78 sem as chaves não avisa nada — aviso que aparece sempre é
	// ruído, e ruído treina o operador a ignorar.
	c, err = CarregaConfig(escreveConfig(t, "runtime:\n  query:\n    maxConcurrent: 8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ChavesDeRuntimeIgnoradas(); len(got) != 0 {
		t.Fatalf("avisos = %v, quer nenhum", got)
	}
}
