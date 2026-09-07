package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// QueryDefaults é a costura entre o nishidb.yaml e o schema: é por ela que a
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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	return writeConfigWithStorage(t, "", body)
}

// writeConfigWithStorage acrescenta chaves DENTRO do bloco storage, que o
// cabeçalho já abre, e é por onde entram as de storage.compaction.
//
// Existe porque repetir `storage:` no corpo não funcionaria: o yaml.v3 recusa
// chave de mapa duplicada ("mapping key already defined"), e o teste falharia
// no parse em vez de falhar pela regra que ele afirma — o pior tipo de verde,
// porque o vermelho viria pelo motivo errado.
func writeConfigWithStorage(t *testing.T, storage, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "nishidb.yaml")
	header := "version: 1\nstorage:\n  path: " + dir + "\n" + storage +
		"datasets:\n  directory: " + dir + "\n"
	if err := os.WriteFile(path, []byte(header+body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsDeQueryCarregaAsChavesGlobais(t *testing.T) {
	path := writeConfig(t, "query:\n  defaultMaxRows: 4242\n  defaultTimeout: 7s\n")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	d := c.QueryDefaults()
	if d.MaxRows != 4242 {
		t.Errorf("MaxRows = %d, want 4242", d.MaxRows)
	}
	if d.Timeout != 7*time.Second {
		t.Errorf("Timeout = %s, want 7s", d.Timeout)
	}
}

func TestDefaultsDeQuerySemBlocoUsaOsPadroes(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	d := c.QueryDefaults()
	if d.MaxRows != defaultQueryMaxRows || d.Timeout != defaultQueryTimeout {
		t.Errorf("QueryDefaults() = %+v, want {%d %s}", d, defaultQueryMaxRows, defaultQueryTimeout)
	}
}

// Valor não positivo é recusado com a chave nomeada, em vez de virar "ausente".
func TestDefaultMaxRowsNaoPositivoFalhaNoCarregamento(t *testing.T) {
	for _, value := range []string{"0", "-1", "-100000"} {
		t.Run(value, func(t *testing.T) {
			_, err := Load(writeConfig(t, "query:\n  defaultMaxRows: "+value+"\n"))
			if err == nil {
				t.Fatalf("defaultMaxRows: %s foi aceito", value)
			}
			if !strings.Contains(err.Error(), "query.defaultMaxRows") ||
				!strings.Contains(err.Error(), value) {
				t.Errorf("erro = %v, want nomear a chave e o valor", err)
			}
		})
	}
}

// O default do bind é LOOPBACK desde a Fase 6, e essa é uma decisão de
// segurança, não de gosto: com POST /v1/query no ar e sem autenticação até a
// Fase 11, um default aberto entregaria o dataset a quem alcançasse a porta.
func TestOBindPadraoEhLoopback(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Bind != "127.0.0.1" {
		t.Fatalf("bind padrão = %q, quer 127.0.0.1", c.Server.Bind)
	}
	if c.Addr() != "127.0.0.1:9142" {
		t.Fatalf("Endereco = %q", c.Addr())
	}
	if c.BindExposed() {
		t.Fatal("o bind padrão foi classificado como exposto")
	}
}

func TestBindExpostoReconheceOsEnderecosQueAlcancamARede(t *testing.T) {
	cases := []struct {
		bind    string
		exposed bool
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
	for _, c := range cases {
		t.Run(c.bind, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, "server:\n  bind: \""+c.bind+"\"\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.BindExposed(); got != c.exposed {
				t.Fatalf("BindExposed(%q) = %v, quer %v", c.bind, got, c.exposed)
			}
		})
	}
}

// O teto de consultas simultâneas do §80 tem leitor, e a chave segue a mesma
// regra de todo inteiro positivo do arquivo: ausente herda o padrão, zero e
// negativo são recusados com a chave e o valor na mensagem.
func TestMaxConcurrentTemLeitorEValidacao(t *testing.T) {
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxConcurrent() != 16 {
		t.Fatalf("MaxConcurrent padrão = %d, quer 16", c.MaxConcurrent())
	}

	c, err = Load(writeConfig(t, "runtime:\n  query:\n    maxConcurrent: 4\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxConcurrent() != 4 {
		t.Fatalf("MaxConcurrent = %d, quer 4", c.MaxConcurrent())
	}

	_, err = Load(writeConfig(t, "runtime:\n  query:\n    maxConcurrent: 0\n"))
	if err == nil {
		t.Fatal("maxConcurrent: 0 deveria ser recusado")
	}
	if !strings.Contains(err.Error(), "runtime.query.maxConcurrent") {
		t.Fatalf("a mensagem não nomeia a chave: %v", err)
	}
}

// Chave aceita e não implementada não pode ficar em silêncio: quem escreve
// managedMemoryTarget dimensiona a máquina por ela (resto do débito 15).
//
// A lista ENCOLHEU quando memoryTarget e query.maxBufferPerQuery ganharam
// leitor, e encolher é obrigatório — não é ajuste de teste. Um aviso que
// continua dizendo "esta chave não faz nada" sobre uma chave que passou a fazer
// mente na direção OPOSTA, e é a pior das duas mentiras: o operador desconfia de
// um limite que está de fato valendo, e sobe o valor para "compensar" um teto
// que ele acha inexistente.
func TestChavesDeRuntimeSemImplementacaoSaoNomeadas(t *testing.T) {
	c, err := Load(writeConfig(t,
		"runtime:\n  memoryTarget: 1GiB\n  managedMemoryTarget: 512MiB\n  query:\n    maxBufferPerQuery: 4MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"runtime.managedMemoryTarget"}
	got := c.IgnoredRuntimeKeys()
	if len(got) != len(want) {
		t.Fatalf("chaves = %v, quer %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chaves = %v, quer %v", got, want)
		}
	}

	// E as duas que SAÍRAM da lista têm de estar de fato valendo: sem esta
	// asserção, tirá-las do aviso e esquecer de ligá-las passaria — que é o
	// mesmo silêncio de antes, com o aviso a menos.
	if c.MemoryTarget() != 1<<30 {
		t.Errorf("MemoryTarget() = %d, quer 1GiB: a chave saiu do aviso e precisa ter leitor", c.MemoryTarget())
	}
	if c.MaxBufferPerQuery() != 4<<20 {
		t.Errorf("MaxBufferPerQuery() = %d, quer 4MiB", c.MaxBufferPerQuery())
	}

	// O bloco §78 sem as chaves não avisa nada — aviso que aparece sempre é
	// ruído, e ruído treina o operador a ignorar.
	c, err = Load(writeConfig(t, "runtime:\n  query:\n    maxConcurrent: 8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.IgnoredRuntimeKeys(); len(got) != 0 {
		t.Fatalf("avisos = %v, quer nenhum", got)
	}
}

// storage.durability (§47) é aceita e ignorada (débito 50): só existe o
// SyncEveryPut interno, que o CLI nunca liga. Quem escreve `interval: 100ms`
// conclui uma janela de durabilidade que o binário não dá — a chave precisa
// ser NOMEADA na subida, no mesmo molde do débito 15 em IgnoredRuntimeKeys.
func TestChaveDeStorageDurabilitySemImplementacaoENomeada(t *testing.T) {
	c, err := Load(writeConfigWithStorage(t,
		"  durability:\n    mode: sync\n    interval: 100ms\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"storage.durability"}
	got := c.IgnoredStorageKeys()
	if len(got) != len(want) {
		t.Fatalf("chaves = %v, quer %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chaves = %v, quer %v", got, want)
		}
	}

	// O bloco §78 sem durability não avisa nada — aviso que aparece sempre é
	// ruído, e ruído treina o operador a ignorar.
	c, err = Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.IgnoredStorageKeys(); len(got) != 0 {
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
	c, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	enabled, ratio := c.Compaction()
	if !enabled {
		t.Error("compaction ausente veio desligada; ausente vale ligada (§78)")
	}
	// O 0.50 entra LITERAL, e não como defaultCompactionRatio: comparar a
	// constante consigo mesma não prende número nenhum — trocar o padrão para
	// 0.75 passaria por esta suíte inteira sem uma linha vermelha, e o único
	// número que esta tarefa existe para fixar é justamente o que o §53 e o
	// exemplo do §78 escrevem. A mensagem de falha também só é honesta assim:
	// dizer "quer 0.50" enquanto se compara contra o que quer que a constante
	// contenha é mentira em caso de divergência.
	if ratio != 0.50 {
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
	c, err := Load(writeConfigWithStorage(t,
		"  compaction:\n    enabled: false\n    minimumLiveRatio: 0.25\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	enabled, ratio := c.Compaction()
	if enabled {
		t.Error("enabled: false foi ignorado")
	}
	if ratio != 0.25 {
		t.Errorf("minimumLiveRatio = %v, quer 0.25", ratio)
	}
}

// O terceiro estado de `enabled` é o do EXEMPLO do §78 — a chave escrita
// `true` — e é o único que nenhum dos dois testes acima alcança: "ausente" e
// `enabled: false` são os dois satisfeitos por um leitor que devolvesse
// `enabled == nil`, e esse leitor devolveria DESLIGADA exatamente para o YAML
// que a spec publica. O bloco entra aqui como o §78 o escreve, as duas linhas
// juntas, porque é esse arquivo que o operador copia.
func TestCompactionDoExemploDoParagrafo78FicaLigada(t *testing.T) {
	c, err := Load(writeConfigWithStorage(t,
		"  compaction:\n    enabled: true\n    minimumLiveRatio: 0.50\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	enabled, ratio := c.Compaction()
	if !enabled {
		t.Error("enabled: true explícito veio desligada")
	}
	if ratio != 0.50 {
		t.Errorf("minimumLiveRatio = %v, quer 0.50 (§78)", ratio)
	}
}

// TestMinimumLiveRatioForaDeZeroUmEhRecusado: ratio >= 1 elegeria TODO
// segmento, inclusive os cheios de dado vivo — compaction perpétua que só
// queima I/O. Ratio <= 0 nunca elegeria nenhum, e a chave mentiria. A faixa é
// ABERTA nas duas pontas por isso, e a recusa nomeia a chave E o valor para o
// operador não ter de adivinhar qual das dezenas do arquivo está errada nem
// conferir de cabeça o número que acha que escreveu.
func TestMinimumLiveRatioForaDeZeroUmEhRecusado(t *testing.T) {
	cases := []struct {
		yaml         string
		valueSnippet string
	}{
		{"0", "0"},
		{"1", "1"},
		{"1.5", "1.5"},
		{"-0.2", "-0.2"},
		// `.nan` e `.NaN` são literais de float do YAML, e não texto solto: o
		// parse os aceita e entrega um float64 que NÃO está na faixa aberta e
		// mesmo assim escapa de toda guarda escrita por comparação — `<= 0` e
		// `>= 1` são as duas falsas quando um lado é NaN. Ele é o pior dos dois
		// casos que o comentário de livenessRatio descreve: com ratio=NaN o
		// predicado do §53 nunca é verdadeiro, nenhum segmento é eleito, e o
		// operador fica com a chave configurada, o disco crescendo e nada
		// compactado.
		{".nan", "NaN"},
		{".NaN", "NaN"},
		// Os infinitos entram junto porque são a outra ponta da mesma família e
		// porque a recusa deles hoje é acidente da comparação: se a guarda um
		// dia trocar de forma, é aqui que se descobre.
		{"+.inf", "+Inf"},
		{"-.inf", "-Inf"},
	}
	for _, tc := range cases {
		t.Run(tc.yaml, func(t *testing.T) {
			_, err := Load(writeConfigWithStorage(t,
				"  compaction:\n    minimumLiveRatio: "+tc.yaml+"\n", ""))
			if err == nil {
				t.Fatalf("minimumLiveRatio: %s foi aceito; queria recusa nomeando a chave", tc.yaml)
			}
			// A chave E o valor, como já faz o irmão deste arquivo para
			// query.defaultMaxRows: sem o valor o operador confere a chave, vê
			// um número que lhe parece plausível e não descobre que o que o
			// arquivo tem não é o que ele acha que escreveu.
			//
			// O valor é procurado com o "é " na frente de propósito: a própria
			// mensagem cita a faixa ("entre 0 e 1"), então um substring nu de
			// "0" ou de "1" seria satisfeito pelo texto fixo e essas duas
			// linhas da tabela não provariam nada.
			if !strings.Contains(err.Error(), "storage.compaction.minimumLiveRatio") ||
				!strings.Contains(err.Error(), "é "+tc.valueSnippet) {
				t.Errorf("erro = %v, quer nomear a chave e o valor %s", err, tc.valueSnippet)
			}
		})
	}
}

// TestFormaDoRatioEhValidadaMesmoComACompactionDesligada cruza os dois eixos que
// a Task 3 introduz — o estado de `enabled` e a faixa de `minimumLiveRatio` —,
// e nenhum outro teste os cruza: os três de `enabled` usam ratio válido e a
// tabela de recusa omite `enabled`. Sem este cruzamento, uma implementação que
// pule a checagem de faixa quando o subsistema está desligado passa pela suíte
// inteira, e é exatamente o "aceito e ignorado" que a doutrina do débito 49
// proíbe: forma validada no PARSE, comportamento na ATIVAÇÃO.
//
// Chave errada num arquivo é erro do arquivo, e desligar o subsistema não a
// torna certa — quem ligasse `enabled: true` meses depois descobriria o valor
// inválido só ali, no pior momento possível.
func TestFormaDoRatioEhValidadaMesmoComACompactionDesligada(t *testing.T) {
	for _, value := range []string{"5", "0", ".nan"} {
		t.Run(value, func(t *testing.T) {
			_, err := Load(writeConfigWithStorage(t,
				"  compaction:\n    enabled: false\n    minimumLiveRatio: "+value+"\n", ""))
			if err == nil {
				t.Fatalf("minimumLiveRatio: %s foi aceito com enabled: false; a forma é validada no parse, não na ativação (débito 49)", value)
			}
		})
	}
}

// A partir daqui: Fase 11 (§74-78) — server.tls, server.authentication e
// server.loopback ganham forma no parse. As oito recusas abaixo seguem a
// mesma doutrina do bloco de compaction logo acima: FORMA validada no PARSE,
// independente de qualquer `enabled`, exceto quando a própria recusa É sobre
// a implicação de `enabled: true` (TLS exigindo cert/key, authentication
// exigindo users).
//
// A resolução de segredos (ler a env var, ler o arquivo de senha) e a matriz
// de recusa do §77 (auth sem TLS com bind exposto etc.) NÃO são desta tarefa
// — ficam para a ativação, em server.Novo (B2 do plano do ciclo).

// TestSenhaLiteralNoYAMLEhRecusada fecha o §75: "senha literal no YAML não
// deve ser aceita" é uma exigência de RECUSA EXPLÍCITA, e não de silêncio.
// `password` nunca foi campo de UserSpec — por isso a mesma whitelist manual
// que a recusa 7 exercita já a nomeia como chave desconhecida.
//
// O usuário já traz passwordEnv E role válidos: se o teste falhasse por
// QUALQUER outro motivo (fonte de senha ausente, role ruim), a mensagem já
// conteria "password" como substring de "passwordEnv" e a asserção passaria
// pelo motivo ERRADO. Com as outras chaves corretas, a única forma de haver
// erro é a whitelist recusando a chave `password` em si.
func TestSenhaLiteralNoYAMLEhRecusada(t *testing.T) {
	body := "server:\n  authentication:\n    enabled: true\n    users:\n" +
		"      - username: grafana\n        passwordEnv: NISHIDB_GRAFANA_PASSWORD\n        password: hunter2\n        role: READ\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("password literal foi aceita")
	}
	if !strings.Contains(err.Error(), "password: chave desconhecida") {
		t.Errorf("erro = %v, quer nomear a chave password como desconhecida", err)
	}
}

func TestRoleForaDoEnumEhRecusado(t *testing.T) {
	body := "server:\n  authentication:\n    enabled: true\n    users:\n" +
		"      - username: grafana\n        passwordEnv: NISHIDB_GRAFANA_PASSWORD\n        role: SUPERUSER\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("role fora do enum {READ, ADMIN} foi aceita")
	}
	if !strings.Contains(err.Error(), "role") || !strings.Contains(err.Error(), "SUPERUSER") {
		t.Errorf("erro = %v, quer nomear a chave role e o valor recebido", err)
	}
}

// TestUsuarioExigeExatamenteUmaFonteDeSenha cobre as duas pontas: nenhuma
// fonte declarada e as duas ao mesmo tempo são igualmente inválidas — a
// credencial precisa morar em EXATAMENTE um lugar.
func TestUsuarioExigeExatamenteUmaFonteDeSenha(t *testing.T) {
	cases := map[string]string{
		"nenhuma": "      - username: grafana\n        role: READ\n",
		"ambas":   "      - username: grafana\n        passwordEnv: NISHIDB_GRAFANA_PASSWORD\n        passwordFile: /run/secrets/grafana\n        role: READ\n",
	}
	for name, user := range cases {
		t.Run(name, func(t *testing.T) {
			body := "server:\n  authentication:\n    enabled: true\n    users:\n" + user
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("%s fontes de senha foi aceito", name)
			}
			if !strings.Contains(err.Error(), "passwordEnv") || !strings.Contains(err.Error(), "passwordFile") {
				t.Errorf("erro = %v, quer nomear passwordEnv e passwordFile", err)
			}
		})
	}
}

func TestUsernameDuplicadoEntreUsersEhRecusado(t *testing.T) {
	body := "server:\n  authentication:\n    enabled: true\n    users:\n" +
		"      - username: grafana\n        passwordEnv: A\n        role: READ\n" +
		"      - username: grafana\n        passwordEnv: B\n        role: ADMIN\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("username duplicado entre users foi aceito")
	}
	if !strings.Contains(err.Error(), "username") || !strings.Contains(err.Error(), "grafana") {
		t.Errorf("erro = %v, quer nomear username e o valor duplicado", err)
	}
}

func TestTLSLigadoSemCertificadoOuChaveEhRecusado(t *testing.T) {
	cases := map[string]string{
		"sem certificateFile": "server:\n  tls:\n    enabled: true\n    privateKeyFile: /etc/nishidb/tls/server.key\n",
		"sem privateKeyFile":  "server:\n  tls:\n    enabled: true\n    certificateFile: /etc/nishidb/tls/server.crt\n",
		"sem os dois":         "server:\n  tls:\n    enabled: true\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("tls.enabled: true sem cert/key foi aceito (%s)", name)
			}
			if !strings.Contains(err.Error(), "server.tls") {
				t.Errorf("erro = %v, quer nomear server.tls", err)
			}
		})
	}
}

func TestAuthenticationLigadaSemUsersEhRecusada(t *testing.T) {
	cases := map[string]string{
		"ausente": "server:\n  authentication:\n    enabled: true\n",
		"vazia":   "server:\n  authentication:\n    enabled: true\n    users: []\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("authentication.enabled: true sem users foi aceito (%s)", name)
			}
			if !strings.Contains(err.Error(), "server.authentication") {
				t.Errorf("erro = %v, quer nomear server.authentication", err)
			}
		})
	}
}

// TestChaveDesconhecidaNosBlocosDeSegurancaEhRecusada é a recusa que a
// doutrina do arquivo mais teme: "enabld: true" (typo) engolido é o typo
// mais caro do arquivo — bloco de segurança não pode herdar a tolerância do
// KnownFields global, que continua desligado de propósito para o resto do
// documento.
func TestChaveDesconhecidaNosBlocosDeSegurancaEhRecusada(t *testing.T) {
	cases := map[string]string{
		"tls":            "server:\n  tls:\n    enabld: true\n",
		"authentication": "server:\n  authentication:\n    enbaled: true\n",
		"users":          "server:\n  authentication:\n    enabled: true\n    users:\n      - usernme: grafana\n        passwordEnv: A\n        role: READ\n",
		"loopback":       "server:\n  loopback:\n    enabld: true\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("chave desconhecida em server.%s foi aceita", name)
			}
			if !strings.Contains(err.Error(), "chave desconhecida") {
				t.Errorf("erro = %v, quer \"chave desconhecida\"", err)
			}
		})
	}
}

func TestLoopbackNaMesmaPortaDoServerPrincipalEhRecusado(t *testing.T) {
	body := "server:\n  port: 9142\n  loopback:\n    enabled: true\n    port: 9142\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("server.loopback.port igual a server.port foi aceito")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") || !strings.Contains(err.Error(), "server.port") {
		t.Errorf("erro = %v, quer nomear as duas chaves", err)
	}
}

// TestLoopbackMesmaPortaComBindLoopbackExplicitoEhRecusado é a mesma célula
// da recusa 8, com server.bind ESCRITO por extenso em vez de confiar no
// default implícito (127.0.0.1) — a colisão de fato é sobre o bind
// alcançar 127.0.0.1, não sobre a chave estar ausente do arquivo.
func TestLoopbackMesmaPortaComBindLoopbackExplicitoEhRecusado(t *testing.T) {
	body := "server:\n  bind: 127.0.0.1\n  port: 9142\n  loopback:\n    enabled: true\n    port: 9142\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("server.bind: 127.0.0.1 com server.loopback.port igual a server.port foi aceito")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") || !strings.Contains(err.Error(), "server.port") {
		t.Errorf("erro = %v, quer nomear as duas chaves", err)
	}
}

// TestLoopbackMesmaPortaComBindWildcardEhRecusado é a segunda célula do
// refinamento de topologia (RULING da revisão final): 0.0.0.0 escuta em
// TODAS as interfaces locais, 127.0.0.1 inclusive — colide de fato com o
// listener loopback na mesma porta, do mesmo jeito que um bind loopback
// explícito colide.
func TestLoopbackMesmaPortaComBindWildcardEhRecusado(t *testing.T) {
	body := "server:\n  bind: 0.0.0.0\n  port: 9142\n  loopback:\n    enabled: true\n    port: 9142\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("server.bind: 0.0.0.0 com server.loopback.port igual a server.port foi aceito")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") || !strings.Contains(err.Error(), "server.port") {
		t.Errorf("erro = %v, quer nomear as duas chaves", err)
	}
}

// TestLoopbackMesmaPortaComBindEmIPEspecificoEhPermitido é a terceira
// célula, e a que o plano da Fase C depende: expor o servidor num IP
// específico e NÃO-loopback (10.200.20.218, no exemplo do plano aprovado)
// na MESMA porta que o plano loopback usa em 127.0.0.1 não colide de fato —
// para o kernel são dois endereços diferentes, cada um seu próprio socket.
// A recusa 8, antes desta revisão, vetava esta topologia comparando só a
// porta e ignorando o bind — o que impedia o passo 3 do plano aprovado
// (expor 10.200.20.218:9142 mantendo o plano loopback intocado em
// 127.0.0.1:9142, sem mexer em painel/script/cron).
func TestLoopbackMesmaPortaComBindEmIPEspecificoEhPermitido(t *testing.T) {
	body := "server:\n  bind: 10.200.20.218\n  port: 9142\n  loopback:\n    enabled: true\n    port: 9142\n"
	_, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("server.bind em IP específico não-loopback com a mesma porta do loopback foi recusado: %v", err)
	}
}

// TestLoopbackPortForaDaFaixaEhRecusado é o Minor 7 da revisão final:
// server.loopback.port não tinha a MESMA validação de faixa que
// server.port (units.PositiveInt + teto de 65535) — `port: 0` hoje
// vira porta EFÊMERA escolhida pelo kernel em silêncio (net.Listen aceita
// ":0"), o oposto do endereço determinístico que o operador declarou.
func TestLoopbackPortForaDaFaixaEhRecusado(t *testing.T) {
	cases := map[string]int{
		"zero":            0,
		"negativo":        -1,
		"acima_do_teto":   70000,
		"acima_do_teto_2": 65536,
	}
	for name, port := range cases {
		t.Run(name, func(t *testing.T) {
			body := fmt.Sprintf("server:\n  loopback:\n    enabled: true\n    port: %d\n", port)
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("server.loopback.port: %d foi aceito", port)
			}
			if !strings.Contains(err.Error(), "server.loopback.port") {
				t.Errorf("erro = %v, quer nomear server.loopback.port", err)
			}
		})
	}
}

// TestLoopbackPortNaFaixaContinuaAceito blinda o caminho feliz: uma porta
// válida (1..65535) não pode ser pega pela recusa nova do Minor 7.
func TestLoopbackPortNaFaixaContinuaAceito(t *testing.T) {
	body := "server:\n  loopback:\n    enabled: true\n    port: 9143\n"
	c, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("server.loopback.port: 9143 foi recusado: %v", err)
	}
	if c.Server.Loopback.Port == nil || *c.Server.Loopback.Port != 9143 {
		t.Errorf("Loopback.Port = %v, quer 9143", c.Server.Loopback.Port)
	}
}

// TestLoopbackNaMesmaPortaDoDefaultImplicitoEhRecusado é a mesma recusa 8,
// mas com `server.port` OMITIDO: a colisão precisa ser detectada contra o
// default implícito (9142, defaultServerPort) tanto quanto contra um `port:`
// explícito — um operador que confiasse no default e não repetisse a porta
// no YAML não pode escapar da checagem só por isso.
func TestLoopbackNaMesmaPortaDoDefaultImplicitoEhRecusado(t *testing.T) {
	body := "server:\n  loopback:\n    enabled: true\n    port: 9142\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("server.loopback.port igual ao default implícito de server.port (9142) foi aceito")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") || !strings.Contains(err.Error(), "server.port") {
		t.Errorf("erro = %v, quer nomear as duas chaves mesmo com server.port omitido", err)
	}
}

// TestPasswordEnvVazioExplicitoTemOMesmoTratamentoDeAusente blinda o
// comportamento atual contra uma migração futura de PasswordEnv para
// ponteiro (*string): hoje `passwordEnv: ""` decodifica para a MESMA string
// vazia que o campo ausente produziria, e as duas devem continuar caindo na
// mesma recusa ("nenhuma fonte declarada") com a MESMA mensagem — nunca
// serem tratadas como "fonte declarada, porém vazia".
func TestPasswordEnvVazioExplicitoTemOMesmoTratamentoDeAusente(t *testing.T) {
	body := "server:\n  authentication:\n    enabled: true\n    users:\n" +
		"      - username: grafana\n        passwordEnv: \"\"\n        role: READ\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("passwordEnv vazio explícito foi aceito")
	}
	if !strings.Contains(err.Error(), "nenhum declarado") {
		t.Errorf("erro = %v, quer a MESMA mensagem do caso ausente (\"nenhum declarado\")", err)
	}
}

// TestRoleEhCaseSensitive trava a escolha atual: o enum fechado {READ,
// ADMIN} não normaliza case. Não é limitação a corrigir — ver o comentário
// no switch de validateUsers — mas uma variação de case aceita em silêncio
// autorizaria um papel que o middleware da B3 nunca reconheceria como válido
// (ele compara a string exata), então a recusa nomeando o valor recebido
// importa tanto quanto a recusa de um valor fora do enum.
func TestRoleEhCaseSensitive(t *testing.T) {
	cases := []string{"read", "Read", "admin", "Admin"}
	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			body := "server:\n  authentication:\n    enabled: true\n    users:\n" +
				"      - username: grafana\n        passwordEnv: A\n        role: " + value + "\n"
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatalf("role %q (case diferente do enum) foi aceito", value)
			}
			if !strings.Contains(err.Error(), "role") || !strings.Contains(err.Error(), value) {
				t.Errorf("erro = %v, quer nomear role e o valor recebido %q", err, value)
			}
		})
	}
}

// TestServerSemBlocosDeSegurancaContinuaValido é o teste de COMPATIBILIDADE:
// o YAML de hoje — inclusive o do piloto, que não conhece tls/authentication/
// loopback — precisa continuar carregando exatamente como antes desta tarefa.
func TestServerSemBlocosDeSegurancaContinuaValido(t *testing.T) {
	c, err := Load(writeConfig(t, "server:\n  bind: 127.0.0.1\n  port: 9142\n"))
	if err != nil {
		t.Fatalf("YAML sem os blocos novos passou a ser recusado: %v", err)
	}
	if c.TLSEnabled() {
		t.Error("TLSEnabled() = true sem bloco tls no YAML")
	}
	if c.AuthenticationEnabled() {
		t.Error("AuthenticationEnabled() = true sem bloco authentication no YAML")
	}
	if c.LoopbackEnabled() {
		t.Error("LoopbackEnabled() = true sem bloco loopback no YAML")
	}
}

// TestBlocosDeSegurancaValidosSaoCarregados prova a INTERFACE que a B2 vai
// consumir: os três predicados e os campos por trás deles, com um YAML no
// molde do exemplo do §78.
func TestBlocosDeSegurancaValidosSaoCarregados(t *testing.T) {
	body := "server:\n" +
		"  tls:\n" +
		"    enabled: true\n" +
		"    certificateFile: /etc/nishidb/tls/server.crt\n" +
		"    privateKeyFile: /etc/nishidb/tls/server.key\n" +
		"  authentication:\n" +
		"    enabled: true\n" +
		"    users:\n" +
		"      - username: reader\n" +
		"        passwordEnv: NISHIDB_READER_PASSWORD\n" +
		"        role: READ\n" +
		"      - username: admin\n" +
		"        passwordFile: /run/secrets/nishidb-admin\n" +
		"        role: ADMIN\n" +
		"  loopback:\n" +
		"    enabled: true\n" +
		"    port: 9143\n"
	c, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("YAML do exemplo do §78 foi recusado: %v", err)
	}
	if !c.TLSEnabled() {
		t.Error("TLSEnabled() = false com tls.enabled: true")
	}
	if c.Server.TLS.CertificateFile != "/etc/nishidb/tls/server.crt" ||
		c.Server.TLS.PrivateKeyFile != "/etc/nishidb/tls/server.key" {
		t.Errorf("TLS = %+v, cert/key não bateram", c.Server.TLS)
	}
	if !c.AuthenticationEnabled() {
		t.Error("AuthenticationEnabled() = false com authentication.enabled: true")
	}
	if len(c.Server.Authentication.Users) != 2 {
		t.Fatalf("Users = %+v, quer 2 usuários", c.Server.Authentication.Users)
	}
	reader := c.Server.Authentication.Users[0]
	if reader.Username != "reader" || reader.PasswordEnv != "NISHIDB_READER_PASSWORD" || reader.Role != "READ" {
		t.Errorf("users[0] = %+v", reader)
	}
	admin := c.Server.Authentication.Users[1]
	if admin.Username != "admin" || admin.PasswordFile != "/run/secrets/nishidb-admin" || admin.Role != "ADMIN" {
		t.Errorf("users[1] = %+v", admin)
	}
	if !c.LoopbackEnabled() {
		t.Error("LoopbackEnabled() = false com loopback.enabled: true")
	}
	if c.Server.Loopback.Port == nil || *c.Server.Loopback.Port != 9143 {
		t.Errorf("Loopback.Port = %v, quer 9143", c.Server.Loopback.Port)
	}
}

// A partir daqui: ajuste pós-B2 (ruling do controlador). A matriz de recusa
// do §77 precisa distinguir "authentication AUSENTE do YAML" de
// "authentication.enabled: false EXPLÍCITO" — os dois davam FALSE em
// AuthenticationEnabled() e eram indistinguíveis. AuthenticationSpec.Enabled
// virou *bool (só ele; TLSSpec e LoopbackSpec continuam bool, nenhuma célula
// da matriz depende de presença neles) e AuthenticationDeclared() expõe a
// pergunta "a seção apareceu?" separada de "está ligada?".

// TestAuthenticationDeclaradaDistingueOsTresEstados prova os três estados que
// a matriz do §77 precisa: seção ausente, seção presente com `enabled: false`
// explícito, e seção presente com `enabled: true`. Os dois primeiros dão
// AuthenticationEnabled() == false, mas só o segundo dá
// AuthenticationDeclared() == true — é essa diferença que a matriz consulta
// para saber se o operador decidiu alguma coisa sobre auth ou não.
func TestAuthenticationDeclaradaDistingueOsTresEstados(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		declared bool
		enabled  bool
	}{
		{
			name:     "ausente",
			body:     "",
			declared: false,
			enabled:  false,
		},
		{
			name:     "presente com enabled false explícito",
			body:     "server:\n  authentication:\n    enabled: false\n",
			declared: true,
			enabled:  false,
		},
		{
			name: "presente com enabled true",
			body: "server:\n  authentication:\n    enabled: true\n    users:\n" +
				"      - username: grafana\n        passwordEnv: A\n        role: READ\n",
			declared: true,
			enabled:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Load(writeConfig(t, tc.body))
			if err != nil {
				t.Fatalf("carregamento falhou: %v", err)
			}
			if got := c.AuthenticationDeclared(); got != tc.declared {
				t.Errorf("AuthenticationDeclared() = %v, quer %v", got, tc.declared)
			}
			if got := c.AuthenticationEnabled(); got != tc.enabled {
				t.Errorf("AuthenticationEnabled() = %v, quer %v", got, tc.enabled)
			}
		})
	}
}

// TestFormaDosUsersEhValidadaMesmoComAuthenticationDesligadaExplicitamente
// cruza os dois eixos que este ajuste introduz — `enabled: false` EXPLÍCITO e
// a forma de cada user —, no mesmo espírito de
// TestFormaDoRatioEhValidadaMesmoComACompactionDesligada: o ponteiro em
// Enabled muda COMO se pergunta "está ligada?", mas não muda a doutrina de
// que forma é validada no parse, independente de ativação (débito 49). Um
// `role` inválido não vira aceitável só porque authentication está desligada
// por decisão explícita do operador.
func TestFormaDosUsersEhValidadaMesmoComAuthenticationDesligadaExplicitamente(t *testing.T) {
	body := "server:\n  authentication:\n    enabled: false\n    users:\n" +
		"      - username: grafana\n        passwordEnv: A\n        role: SUPERUSER\n"
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("role inválido foi aceito com authentication.enabled: false")
	}
	if !strings.Contains(err.Error(), "role") || !strings.Contains(err.Error(), "SUPERUSER") {
		t.Errorf("erro = %v, quer nomear a chave role e o valor recebido", err)
	}
}
