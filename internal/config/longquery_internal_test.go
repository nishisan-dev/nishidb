package config

import (
	"strings"
	"testing"
	"time"
)

// O sub-pool de consulta longa tem leitor, e o DEFAULT dele não pode recusar
// arquivo que subia ontem.
//
// A assimetria entre o default e a chave explícita é o conteúdo do teste:
// `runtime.query.maxConcurrent: 1` é configuração legítima e existente, e um
// default fixo de 2 mataria a subida dela na primeira troca de binário — uma
// chave nova recusando um arquivo que ninguém editou. Já quem ESCREVE
// maxLongRunning acima do maxConcurrent pede algo que o semáforo geral recusaria
// primeiro, e aí a recusa é o serviço.
func TestMaxLongRunningTemLeitorEValidacao(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxLongRunning() != 2 {
		t.Fatalf("MaxLongRunning padrão = %d, quer 2", c.MaxLongRunning())
	}

	c, err = Load(writeConfig(t, "runtime:\n  query:\n    maxLongRunning: 5\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxLongRunning() != 5 {
		t.Fatalf("MaxLongRunning = %d, quer 5", c.MaxLongRunning())
	}

	// O caso de compatibilidade: pool de uma vaga, sub-pool não declarado.
	c, err = Load(writeConfig(t, "runtime:\n  query:\n    maxConcurrent: 1\n"))
	if err != nil {
		t.Fatalf("maxConcurrent: 1 sem maxLongRunning tem de subir: %v", err)
	}
	if c.MaxLongRunning() != 1 {
		t.Fatalf("MaxLongRunning = %d com maxConcurrent 1, quer 1: o sub-pool não pode passar do pool",
			c.MaxLongRunning())
	}

	_, err = Load(writeConfig(t, "runtime:\n  query:\n    maxConcurrent: 2\n    maxLongRunning: 3\n"))
	if err == nil {
		t.Fatal("maxLongRunning acima do maxConcurrent deveria ser recusado")
	}
	for _, key := range []string{"runtime.query.maxLongRunning", "runtime.query.maxConcurrent"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("a mensagem não nomeia %s: %v", key, err)
		}
	}

	_, err = Load(writeConfig(t, "runtime:\n  query:\n    maxLongRunning: 0\n"))
	if err == nil {
		t.Fatal("maxLongRunning: 0 deveria ser recusado")
	}
	if !strings.Contains(err.Error(), "runtime.query.maxLongRunning") {
		t.Fatalf("a mensagem não nomeia a chave: %v", err)
	}
}

// O teto global de prazo tem leitor, e AUSENTE é zero — não 30 segundos.
//
// Zero é o sinal que schema.validate lê como "cada dataset herda o próprio
// query.timeout". Um default de 30s aqui daria teto de 30s a datasets que
// declaram `query.timeout: 5m`, e a spec deles passaria a ser RECUSADA na
// abertura: o processo morreria na troca de binário por causa de um arquivo que
// ninguém tocou.
func TestDefaultMaxTimeoutTemLeitorEValidacao(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if d := c.QueryDefaults(); d.MaxTimeout != 0 {
		t.Fatalf("QueryDefaults().MaxTimeout = %s sem a chave, quer 0 (o sinal de \"herda o timeout\")",
			d.MaxTimeout)
	}

	c, err = Load(writeConfig(t, "query:\n  defaultMaxTimeout: 12m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d := c.QueryDefaults(); d.MaxTimeout != 12*time.Minute {
		t.Fatalf("QueryDefaults().MaxTimeout = %s, quer 12m", d.MaxTimeout)
	}
	// Declarar o TETO não pode mexer no prazo padrão: são chaves distintas
	// justamente para que subir uma não suba a outra.
	if d := c.QueryDefaults(); d.Timeout != defaultQueryTimeout {
		t.Fatalf("QueryDefaults().Timeout = %s, quer %s", d.Timeout, defaultQueryTimeout)
	}

	_, err = Load(writeConfig(t, "query:\n  defaultMaxTimeout: nao-e-duracao\n"))
	if err == nil {
		t.Fatal("defaultMaxTimeout inválido deveria ser recusado")
	}
	if !strings.Contains(err.Error(), "query.defaultMaxTimeout") {
		t.Fatalf("a mensagem não nomeia a chave: %v", err)
	}
}

// Teto global abaixo do prazo global é contradição, e a recusa acontece AQUI,
// com os nomes GLOBAIS.
//
// Deixar o par inválido chegar em schema.validate faria a mensagem nomear
// `query.maxTimeout`/`query.timeout` — chaves de OUTRO arquivo, que o operador
// não encontraria no nishidb.yaml que ele acabou de editar.
func TestDefaultMaxTimeoutAbaixoDoDefaultTimeoutEhRecusado(t *testing.T) {
	_, err := Load(writeConfig(t, "query:\n  defaultTimeout: 30s\n  defaultMaxTimeout: 10s\n"))
	if err == nil {
		t.Fatal("quer recusa: o teto global recusaria o prazo que ele mesmo dá por padrão")
	}
	for _, key := range []string{"query.defaultMaxTimeout", "query.defaultTimeout"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("a mensagem não nomeia %s: %v", key, err)
		}
	}

	// Igual é o caso normal e tem de passar: um `<=` no lugar do `<` recusaria
	// a configuração mais natural que existe.
	if _, err := Load(writeConfig(t,
		"query:\n  defaultTimeout: 30s\n  defaultMaxTimeout: 30s\n")); err != nil {
		t.Fatalf("teto igual ao prazo padrão tem de ser aceito: %v", err)
	}
}

// runtime.memoryTarget e runtime.query.maxBufferPerQuery têm LEITOR.
//
// As duas eram parseadas e nunca lidas (metade do débito 15), e a chave não era
// só inócua: quem escreve `memoryTarget: 1GiB` dimensiona a MÁQUINA por ela. O
// teste cobre os três estados que separam "tem leitor" de "está no arquivo":
// o default do §80, o valor declarado, e a recusa do valor inválido pela chave.
func TestMaxBufferPerQueryTemLeitorEValidacao(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxBufferPerQuery() != 4<<20 {
		t.Fatalf("MaxBufferPerQuery padrão = %d, quer 4MiB (§80)", c.MaxBufferPerQuery())
	}
	if c.MemoryTarget() != 512<<20 {
		t.Fatalf("MemoryTarget padrão = %d, quer 512MiB (§80)", c.MemoryTarget())
	}

	c, err = Load(writeConfig(t,
		"runtime:\n  memoryTarget: 2GiB\n  query:\n    maxBufferPerQuery: 16MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxBufferPerQuery() != 16<<20 {
		t.Fatalf("MaxBufferPerQuery = %d, quer 16MiB", c.MaxBufferPerQuery())
	}
	if c.MemoryTarget() != 2<<30 {
		t.Fatalf("MemoryTarget = %d, quer 2GiB", c.MemoryTarget())
	}

	// Valor inválido é recusado NOMEANDO a chave — e a unidade DECIMAL é
	// recusada junto, porque units.Size não aceita as duas grafias: "4MB" e
	// "4MiB" pareceriam a mesma coisa e diferiam em 5%.
	for _, v := range []string{"quatro", "0", "-1", "4MB"} {
		t.Run("maxBufferPerQuery "+v, func(t *testing.T) {
			_, err := Load(writeConfig(t, "runtime:\n  query:\n    maxBufferPerQuery: \""+v+"\"\n"))
			if err == nil {
				t.Fatalf("maxBufferPerQuery %q deveria ser recusado", v)
			}
			if !strings.Contains(err.Error(), "runtime.query.maxBufferPerQuery") {
				t.Fatalf("a mensagem não nomeia a chave: %v", err)
			}
		})
	}
	for _, v := range []string{"quatro", "0", "-1", "512MB"} {
		t.Run("memoryTarget "+v, func(t *testing.T) {
			_, err := Load(writeConfig(t, "runtime:\n  memoryTarget: \""+v+"\"\n"))
			if err == nil {
				t.Fatalf("memoryTarget %q deveria ser recusado", v)
			}
			if !strings.Contains(err.Error(), "runtime.memoryTarget") {
				t.Fatalf("a mensagem não nomeia a chave: %v", err)
			}
		})
	}
}

// Orçamento por consulta acima do orçamento do processo é contradição.
//
// A PRIMEIRA consulta a chegar perto do próprio limite já seria recusada pelo
// global, e o operador leria a recusa como defeito do motor em vez de como a
// contradição que ele escreveu. Recusar na abertura, nomeando as duas chaves, é
// o que transforma isso num erro de configuração legível.
func TestMaxBufferPerQueryAcimaDoMemoryTargetEhRecusado(t *testing.T) {
	_, err := Load(writeConfig(t,
		"runtime:\n  memoryTarget: 8MiB\n  query:\n    maxBufferPerQuery: 16MiB\n"))
	if err == nil {
		t.Fatal("quer recusa: o orçamento de UMA consulta não pode passar o do processo")
	}
	for _, key := range []string{"runtime.query.maxBufferPerQuery", "runtime.memoryTarget"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("a mensagem não nomeia %s: %v", key, err)
		}
	}

	// Igual passa: um processo de uma consulta só é configuração legítima, e um
	// `>=` no lugar do `>` a recusaria.
	if _, err := Load(writeConfig(t,
		"runtime:\n  memoryTarget: 8MiB\n  query:\n    maxBufferPerQuery: 8MiB\n")); err != nil {
		t.Fatalf("orçamento por consulta igual ao do processo tem de ser aceito: %v", err)
	}
}

// A chave do teto do journal de versões segue a política de config do projeto:
// ausente usa o padrão, presente e fora de faixa é RECUSADO na subida — nunca
// arredondado, que faria o operador acreditar ter configurado o que não
// configurou.
func TestSnapshotMaxBytesAusenteUsaOPadraoEAcimaDoMemoryTargetRecusa(t *testing.T) {
	// Com o memoryTarget padrão (512 MiB), um oitavo dá 64 MiB — o teto
	// absoluto. É o caso comum, e o que o operador vê sem declarar nada.
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.SnapshotMaxBytes(); got != defaultSnapshotMaxBytes {
		t.Errorf("SnapshotMaxBytes ausente = %d, quero o padrão %d", got, defaultSnapshotMaxBytes)
	}

	// Num processo pequeno o padrão ENCOLHE junto, e não derruba a subida: uma
	// configuração que sobe hoje tem de continuar subindo depois de a chave
	// existir.
	c, err = Load(writeConfig(t, "runtime:\n  memoryTarget: 8MiB\n"))
	if err != nil {
		t.Fatalf("o padrão do journal derrubou a subida de um processo pequeno: %v", err)
	}
	if got := c.SnapshotMaxBytes(); got != 1<<20 {
		t.Errorf("SnapshotMaxBytes com memoryTarget de 8MiB = %d, quero 1MiB (um oitavo)", got)
	}

	c, err = Load(writeConfig(t, "runtime:\n  query:\n    snapshotMaxBytes: 128MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.SnapshotMaxBytes(); got != 128<<20 {
		t.Errorf("SnapshotMaxBytes = %d, quero 128MiB", got)
	}

	// Acima do memoryTarget, o journal sozinho estouraria o orçamento do
	// processo inteiro — é a mesma recusa que o maxBufferPerQuery já faz, e
	// pela mesma razão: um teto que não cabe no teto de cima não é teto.
	_, err = Load(writeConfig(t, "runtime:\n  memoryTarget: 64MiB\n  query:\n    snapshotMaxBytes: 128MiB\n"))
	if err == nil {
		t.Error("snapshotMaxBytes acima do memoryTarget foi ACEITO: o journal sozinho estouraria o orçamento do processo")
	}
}
