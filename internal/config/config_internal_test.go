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
	return escreveConfigComStorage(t, "", corpo)
}

// escreveConfigComStorage acrescenta chaves DENTRO do bloco storage, que o
// cabeçalho já abre, e é por onde entram as de storage.compaction.
//
// Existe porque repetir `storage:` no corpo não funcionaria: o yaml.v3 recusa
// chave de mapa duplicada ("mapping key already defined"), e o teste falharia
// no parse em vez de falhar pela regra que ele afirma — o pior tipo de verde,
// porque o vermelho viria pelo motivo errado.
func escreveConfigComStorage(t *testing.T, storage, corpo string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "nishidb.yaml")
	cabecalho := "version: 1\nstorage:\n  path: " + dir + "\n" + storage +
		"datasets:\n  directory: " + dir + "\n"
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

// O bloco storage.compaction era aceito e ignorado. Ausente ele tem de valer
// os defaults da spec, e não "desligado": o §53 fixa minimumLiveRatio em 0.50
// e o §78 traz `enabled: true` no exemplo. Um arquivo que não fala de
// compaction é um arquivo que aceita a política da spec — exigir declaração
// explícita para ter compaction inverteria o default seguro e deixaria o disco
// crescer para sempre em toda instalação que herdou um YAML do M1.
func TestCompactionHerdaODefaultDoParagrafo53(t *testing.T) {
	c, err := CarregaConfig(escreveConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	habilitada, ratio := c.Compaction()
	if !habilitada {
		t.Error("compaction ausente veio desligada; ausente vale ligada (§78)")
	}
	if ratio != compactionRatioPadrao {
		t.Errorf("minimumLiveRatio = %v, quer 0.50 (§53)", ratio)
	}
}

// A chave declarada tem de VENCER o default, senão ela continua aceita e
// ignorada — que é exatamente o defeito que esta tarefa fecha, e que nenhum
// teste de default sozinho pegaria: um leitor que devolvesse a constante e
// nunca olhasse o YAML passaria no teste do parágrafo 53.
//
// `enabled: false` é o operador desligando deliberadamente, pelo mesmo
// raciocínio de server.enabled: ponteiro separa "ausente" de "false", e os
// dois têm significados opostos.
func TestCompactionDeclaradaVenceODefault(t *testing.T) {
	c, err := CarregaConfig(escreveConfigComStorage(t,
		"  compaction:\n    enabled: false\n    minimumLiveRatio: 0.25\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	habilitada, ratio := c.Compaction()
	if habilitada {
		t.Error("enabled: false foi ignorado")
	}
	if ratio != 0.25 {
		t.Errorf("minimumLiveRatio = %v, quer 0.25", ratio)
	}
}

// TestMinimumLiveRatioForaDeZeroUmEhRecusado: ratio >= 1 elegeria TODO
// segmento, inclusive os cheios de dado vivo — compaction perpétua que só
// queima I/O. Ratio <= 0 nunca elegeria nenhum, e a chave mentiria. A faixa é
// ABERTA nas duas pontas por isso, e a recusa nomeia a chave para o operador
// não ter de adivinhar qual das dezenas do arquivo está errada.
func TestMinimumLiveRatioForaDeZeroUmEhRecusado(t *testing.T) {
	for _, v := range []string{"0", "1", "1.5", "-0.2"} {
		t.Run(v, func(t *testing.T) {
			_, err := CarregaConfig(escreveConfigComStorage(t,
				"  compaction:\n    minimumLiveRatio: "+v+"\n", ""))
			if err == nil {
				t.Fatalf("minimumLiveRatio: %s foi aceito; queria recusa nomeando a chave", v)
			}
			if !strings.Contains(err.Error(), "storage.compaction.minimumLiveRatio") {
				t.Errorf("erro = %v, quer nomear a chave", err)
			}
		})
	}
}
