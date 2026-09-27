package gates_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/config"
)

// scripts/build-deb.sh empacota o binário `nishidb` num .deb FHS
// (/usr/bin, /etc/nishidb, /usr/lib/systemd/system), e este arquivo é o
// oráculo dele — mesma doutrina de lint_test.go e rollback_update_test.go:
// um script que ninguém EXECUTA em teste não é gate, é comentário. Os
// testes abaixo rodam o script de VERDADE contra um binário ELF fabricado
// (o bastante para os 20 primeiros bytes do cabeçalho, que é tudo que o
// script lê) e inspecionam o .deb resultante com o próprio dpkg-deb — nunca
// abrindo o arquivo .deb (formato ar + dois tarballs) à mão.

// debBuildScript é o caminho do script, resolvido a partir de repoRoot —
// já definido em lint_test.go, e compartilhado por todo o pacote.
var debBuildScript = filepath.Join(repoRoot, "scripts", "build-deb.sh")

// locateDpkgDeb resolve o binário antes de qualquer teste deste arquivo
// precisar inspecionar um .deb. Ausência é FALHA, não SKIP — mesmo
// argumento de locateMake (lint_test.go): um gate que pula quando a
// ferramenta que ele inspeciona está ausente devolveria verde justamente no
// caso que existe para pegar (débito 53).
func locateDpkgDeb(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("dpkg-deb")
	if err != nil {
		t.Fatalf("dpkg-deb não está no PATH — instale o pacote dpkg-dev (Debian/Ubuntu: apt install dpkg-dev) para rodar este gate: %v", err)
	}
	return path
}

// elfMachine são os dois bytes (little-endian) do campo e_machine no
// cabeçalho ELF, nos offsets 18-19 — os mesmos que scripts/build-deb.sh lê
// com `od` para recusar o binário da arquitetura errada.
var elfMachine = map[string][2]byte{
	"amd64": {0x3e, 0x00}, // EM_X86_64
	"arm64": {0xb7, 0x00}, // EM_AARCH64
}

// writeFakeELF escreve, em path, um cabeçalho ELF mínimo (64 bytes, os
// primeiros 16 do e_ident mais e_type/e_machine) com o e_machine da
// arquitetura pedida. O script só lê os bytes 18-19; o resto do arquivo é
// preenchido de zero e nunca é interpretado como código de verdade — os
// testes deste arquivo nunca EXECUTAM o binário fabricado, só o empacotam.
func writeFakeELF(t *testing.T, path, arch string) {
	t.Helper()
	machine, ok := elfMachine[arch]
	if !ok {
		t.Fatalf("arquitetura de teste desconhecida: %s", arch)
	}
	buf := make([]byte, 64)
	copy(buf[0:4], []byte{0x7f, 'E', 'L', 'F'})
	buf[4] = 2  // ELFCLASS64
	buf[5] = 1  // ELFDATA2LSB (little-endian)
	buf[6] = 1  // EV_CURRENT
	buf[16] = 2 // e_type = ET_EXEC, irrelevante para o script
	buf[18] = machine[0]
	buf[19] = machine[1]
	if err := os.WriteFile(path, buf, 0o755); err != nil {
		t.Fatalf("escrevendo ELF falso em %s: %v", path, err)
	}
}

// runBuildDeb executa o script de verdade com os argumentos dados e devolve
// stdout, stderr e o código de saída.
func runBuildDeb(t *testing.T, extraEnv []string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(locateBash(t), append([]string{debBuildScript}, args...)...)
	if extraEnv != nil {
		cmd.Env = extraEnv
	}
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("scripts/build-deb.sh não chegou a executar: %v", err)
		}
	}
	return out.String(), errBuf.String(), code
}

// dpkgField devolve o valor de um campo do control do .deb, via
// `dpkg-deb --field`. Com um campo só, dpkg-deb imprime só o VALOR, sem o
// prefixo "Campo: ".
func dpkgField(t *testing.T, deb, field string) string {
	t.Helper()
	out, err := exec.Command(locateDpkgDeb(t), "--field", deb, field).Output()
	if err != nil {
		t.Fatalf("dpkg-deb --field %s %s: %v", deb, field, err)
	}
	return strings.TrimSpace(string(out))
}

// dpkgInfo devolve o `dpkg-deb --info` inteiro — usado para conferir
// presença e modo executável dos scripts de mantenedor, que moram no
// control.tar e por isso não aparecem em `--contents` (que só lista o
// data.tar).
func dpkgInfo(t *testing.T, deb string) string {
	t.Helper()
	out, err := exec.Command(locateDpkgDeb(t), "--info", deb).CombinedOutput()
	if err != nil {
		t.Fatalf("dpkg-deb --info %s: %v\n%s", deb, err, out)
	}
	return string(out)
}

// dpkgContents devolve o `dpkg-deb --contents` inteiro — o data.tar, com
// modo, dono e caminho de cada entrada.
func dpkgContents(t *testing.T, deb string) string {
	t.Helper()
	out, err := exec.Command(locateDpkgDeb(t), "--contents", deb).Output()
	if err != nil {
		t.Fatalf("dpkg-deb --contents %s: %v", deb, err)
	}
	return string(out)
}

// mustDpkgInfoMember lê o conteúdo de UM membro do control.tar
// (`dpkg-deb --info <deb> <membro>` imprime o conteúdo bruto do arquivo).
func mustDpkgInfoMember(t *testing.T, deb, member string) []byte {
	t.Helper()
	out, err := exec.Command(locateDpkgDeb(t), "--info", deb, member).Output()
	if err != nil {
		t.Fatalf("dpkg-deb --info %s %s: %v", deb, member, err)
	}
	return out
}

// TestBuildDebProducesInstallablePackage é o caminho feliz, para as duas
// arquiteturas que a release publica: builda o .deb com um ELF fabricado da
// arquitetura certa e confere TODO o contrato do pacote — nome do arquivo,
// campos do control (inclusive Installed-Size e Depends), conteúdo do
// data.tar, md5sums e presença/modo dos scripts de mantenedor.
func TestBuildDebProducesInstallablePackage(t *testing.T) {
	locateDpkgDeb(t)
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			dir := t.TempDir()
			binaryPath := filepath.Join(dir, "nishidb")
			writeFakeELF(t, binaryPath, arch)
			out := filepath.Join(dir, "dist")
			const version = "1.2.3-9-gabcdef0"

			stdout, stderr, code := runBuildDeb(t, nil, "--binary", binaryPath, "--version", version, "--arch", arch, "--out", out)
			if code != 0 {
				t.Fatalf("build-deb.sh recusou o caminho feliz (exit %d)\nstdout: %s\nstderr: %s", code, stdout, stderr)
			}

			wantDeb := filepath.Join(out, "nishidb_"+version+"_"+arch+".deb")
			gotDeb := strings.TrimSpace(stdout)
			if gotDeb != wantDeb {
				t.Fatalf("stdout final = %q, esperava o caminho do .deb gerado (%q)", gotDeb, wantDeb)
			}
			if _, err := os.Stat(wantDeb); err != nil {
				t.Fatalf("o .deb não existe em %s: %v", wantDeb, err)
			}

			if got := dpkgField(t, wantDeb, "Package"); got != "nishidb" {
				t.Errorf("Package = %q, queria \"nishidb\"", got)
			}
			if got := dpkgField(t, wantDeb, "Version"); got != version {
				t.Errorf("Version = %q, queria %q", got, version)
			}
			if got := dpkgField(t, wantDeb, "Architecture"); got != arch {
				t.Errorf("Architecture = %q, queria %q", got, arch)
			}
			if got := dpkgField(t, wantDeb, "Depends"); got != "passwd" {
				t.Errorf("Depends = %q, queria \"passwd\" (é de lá que vem useradd/userdel/getent dos scripts de mantenedor)", got)
			}
			if got := dpkgField(t, wantDeb, "Installed-Size"); got == "" || got == "0" {
				t.Errorf("Installed-Size = %q, queria um KiB positivo (o staging tem o binário fabricado + a unit + a config)", got)
			}

			contents := dpkgContents(t, wantDeb)
			if !regexp.MustCompile(`(?m)^-rwxr-xr-x root/root\s+\d+.*\./usr/bin/nishidb$`).MatchString(contents) {
				t.Errorf("/usr/bin/nishidb ausente ou sem modo -rwxr-xr-x root/root\ncontents:\n%s", contents)
			}
			if !regexp.MustCompile(`(?m)\./usr/lib/systemd/system/nishidb\.service$`).MatchString(contents) {
				t.Errorf("unit ausente de /usr/lib/systemd/system/\ncontents:\n%s", contents)
			}
			if !regexp.MustCompile(`(?m)\./etc/nishidb/nishidb\.yaml$`).MatchString(contents) {
				t.Errorf("/etc/nishidb/nishidb.yaml ausente\ncontents:\n%s", contents)
			}
			if !regexp.MustCompile(`(?m)\./etc/logrotate\.d/nishidb$`).MatchString(contents) {
				t.Errorf("/etc/logrotate.d/nishidb ausente\ncontents:\n%s", contents)
			}
			if !regexp.MustCompile(`(?m)^drwxr-xr-x root/root\s+0.*\./etc/nishidb/datasets/$`).MatchString(contents) {
				t.Errorf("diretório vazio /etc/nishidb/datasets/ ausente\ncontents:\n%s", contents)
			}
			for _, line := range strings.Split(strings.TrimSpace(contents), "\n") {
				if !strings.Contains(line, "root/root") {
					t.Errorf("entrada com dono diferente de root/root: %q", line)
				}
			}

			conffiles := strings.TrimSpace(string(mustDpkgInfoMember(t, wantDeb, "conffiles")))
			wantConffiles := "/etc/nishidb/nishidb.yaml\n/etc/logrotate.d/nishidb"
			if conffiles != wantConffiles {
				t.Errorf("conffiles = %q, queria %q", conffiles, wantConffiles)
			}

			// md5sums cobre os arquivos de DADO, exceto os CONFFILES
			// (etc/nishidb/nishidb.yaml e etc/logrotate.d/nishidb):
			// integridade de conffile é rastreada pelo dpkg via
			// DEBIAN/conffiles, não por md5sums — listá-lo aqui faria
			// `dpkg -V` acusar "alterado" em todo host que já editou a
			// própria config, um falso positivo permanente (é o caso
			// NORMAL de um conffile, não uma exceção).
			md5sums := string(mustDpkgInfoMember(t, wantDeb, "md5sums"))
			for _, want := range []string{"usr/bin/nishidb", "usr/lib/systemd/system/nishidb.service"} {
				if strings.Contains(md5sums, "./"+want) {
					t.Errorf("md5sums cita %q com prefixo \"./\" — a Policy espera caminho relativo puro\nmd5sums:\n%s", want, md5sums)
				}
				if !regexp.MustCompile(`(?m)^[0-9a-f]{32}  ` + regexp.QuoteMeta(want) + `$`).MatchString(md5sums) {
					t.Errorf("md5sums não cobre %q (ou está num formato inesperado)\nmd5sums:\n%s", want, md5sums)
				}
			}
			for _, conffile := range []string{"etc/nishidb/nishidb.yaml", "etc/logrotate.d/nishidb"} {
				if strings.Contains(md5sums, conffile) {
					t.Errorf("md5sums cita o conffile %s — ele tem de ficar de FORA (a integridade dele é rastreada por DEBIAN/conffiles, não por md5sums)\nmd5sums:\n%s", conffile, md5sums)
				}
			}

			info := dpkgInfo(t, wantDeb)
			for _, script := range []string{"postinst", "prerm", "postrm"} {
				if !regexp.MustCompile(`(?m)\*\s+` + script + `\s`).MatchString(info) {
					t.Errorf("%s ausente do control ou sem bit de execução (sem o marcador \"*\" no --info)\ninfo:\n%s", script, info)
				}
			}
		})
	}
}

// TestBuildDebRefusesBinaryOfAnotherArchitecture fixa a proteção central do
// script: o workflow builda amd64 e arm64 no MESMO caminho `./nishidb`,
// trocando o conteúdo entre as duas chamadas, e sem esta checagem um `--arch`
// pedido depois do build errado empacotaria silenciosamente o binário da
// arquitetura ERRADA.
func TestBuildDebRefusesBinaryOfAnotherArchitecture(t *testing.T) {
	locateDpkgDeb(t)
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "nishidb")
	writeFakeELF(t, binaryPath, "amd64")

	stdout, stderr, code := runBuildDeb(t, nil, "--binary", binaryPath, "--version", "1.0.0", "--arch", "arm64", "--out", filepath.Join(dir, "dist"))
	if code == 0 {
		t.Fatalf("build-deb.sh aceitou um binário amd64 empacotado como arm64\nstdout: %s", stdout)
	}
	if !strings.Contains(stderr, "arm64") {
		t.Errorf("stderr não menciona a arquitetura recusada: %q", stderr)
	}
}

// TestBuildDebRefusesEmptyBinary fixa a recusa de um binário vazio — sintoma
// de um build que falhou silenciosamente antes do empacotamento.
func TestBuildDebRefusesEmptyBinary(t *testing.T) {
	locateDpkgDeb(t)
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "nishidb")
	if err := os.WriteFile(binaryPath, nil, 0o755); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runBuildDeb(t, nil, "--binary", binaryPath, "--version", "1.0.0", "--arch", "amd64", "--out", filepath.Join(dir, "dist"))
	if code == 0 {
		t.Fatalf("build-deb.sh aceitou um binário vazio\nstdout: %s", stdout)
	}
	if !strings.Contains(stderr, "is empty") {
		t.Errorf("stderr não menciona que o binário está vazio: %q", stderr)
	}
}

// TestBuildDebRefusesInvalidVersion fixa a validação do formato Debian —
// `v0.9.2` é a versão de TAG do git (com o "v"), não a de PACOTE, e as duas
// não podem ser confundidas silenciosamente.
//
// A asserção mira a mensagem PRÓPRIA do script ("expected Debian format"),
// não uma substring genérica como "v0.9.2": o dpkg-deb TAMBÉM recusaria essa
// versão (não começa com dígito) se a validação do script fosse afrouxada, e
// o texto da recusa dele citaria a mesma versão — um teste que procurasse só
// "v0.9.2" no stderr passaria com a checagem do script DESLIGADA, vacuamente
// coberto pela checagem do dpkg-deb (ver mutação registrada no relatório).
func TestBuildDebRefusesInvalidVersion(t *testing.T) {
	locateDpkgDeb(t)
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "nishidb")
	writeFakeELF(t, binaryPath, "amd64")

	stdout, stderr, code := runBuildDeb(t, nil, "--binary", binaryPath, "--version", "v0.9.2", "--arch", "amd64", "--out", filepath.Join(dir, "dist"))
	if code == 0 {
		t.Fatalf("build-deb.sh aceitou a versão 'v0.9.2' (prefixo de tag, não de pacote)\nstdout: %s", stdout)
	}
	if !strings.Contains(stderr, "expected Debian format") {
		t.Errorf("stderr não traz a mensagem PRÓPRIA do script (\"expected Debian format\") — pode estar sendo recusada só pelo dpkg-deb, o que deixaria a validação do script vacuamente coberta: %q", stderr)
	}
}

// TestBuildDebRefusesUnknownArchitecture fixa que só amd64/arm64 são
// aceitas — a matriz do release.yml não builda nenhuma outra.
//
// A asserção mira a mensagem da WHITELIST ("not supported"), não uma
// substring genérica como "mips": a checagem de e_machine (mais abaixo no
// script) também recusaria "mips" por não ter entrada na tabela de arch
// esperada, citando "mips" na mesma frase — um teste que procurasse só
// "mips" no stderr passaria com a whitelist DESLIGADA, vacuamente coberto
// pela checagem de e_machine (ver mutação registrada no relatório).
func TestBuildDebRefusesUnknownArchitecture(t *testing.T) {
	locateDpkgDeb(t)
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "nishidb")
	writeFakeELF(t, binaryPath, "amd64")

	stdout, stderr, code := runBuildDeb(t, nil, "--binary", binaryPath, "--version", "1.0.0", "--arch", "mips", "--out", filepath.Join(dir, "dist"))
	if code == 0 {
		t.Fatalf("build-deb.sh aceitou a arquitetura 'mips'\nstdout: %s", stdout)
	}
	if !strings.Contains(stderr, "not supported") {
		t.Errorf("stderr não traz a mensagem PRÓPRIA da whitelist (\"not supported\") — pode estar sendo recusada só pela checagem de e_machine, o que deixaria a whitelist vacuamente coberta: %q", stderr)
	}
}

// TestBuildDebRefusesWhenDpkgDebMissing é o mesmo teste de
// TestAlvoLintRecusaQuandoOGolangciLintNaoEstaNoPATH (lint_test.go), agora
// para o empacotador: com PATH vazio, o script tem de FALHAR alto nomeando
// o pacote a instalar — nunca tentar montar o staging sem a ferramenta que
// vai lê-lo.
func TestBuildDebRefusesWhenDpkgDebMissing(t *testing.T) {
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "nishidb")
	writeFakeELF(t, binaryPath, "amd64")

	env := envWithPATH(t.TempDir())
	stdout, stderr, code := runBuildDeb(t, env, "--binary", binaryPath, "--version", "1.0.0", "--arch", "amd64", "--out", filepath.Join(dir, "dist"))
	if code == 0 {
		t.Fatalf("build-deb.sh saiu 0 com dpkg-deb AUSENTE do PATH\nstdout: %s\nstderr: %s", stdout, stderr)
	}
	for _, snippet := range []string{"dpkg-deb", "dpkg-dev"} {
		if !strings.Contains(stderr, snippet) {
			t.Errorf("stderr não menciona %q — a recusa precisa nomear o que falta e o que instalar\nstderr: %q", snippet, stderr)
		}
	}
}

// TestPackagedConfigLoadsWithTheRealLoader carrega packaging/deb/nishidb.yaml
// pela função REAL de internal/config (config.Load), e não por um parser de
// teste — a doutrina de "ler como dado" vira aqui "ler pelo consumidor real".
// config.Load NUNCA verifica se storage.path/datasets.directory existem em
// disco (só valida a FORMA do arquivo — chaves obrigatórias, faixas,
// combinações), então o arquivo original é carregado sem cópia nem reescrita
// de caminho; a prova de que o servidor sobe de verdade com ele mora na
// verificação manual do brief (item 4), fora deste gate.
func TestPackagedConfigLoadsWithTheRealLoader(t *testing.T) {
	path := filepath.Join(repoRoot, "packaging", "deb", "nishidb.yaml")
	c, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load(%s) recusou a config empacotada: %v", path, err)
	}
	if c.Storage.Path != "/var/lib/nishidb" {
		t.Errorf("storage.path = %q, queria /var/lib/nishidb", c.Storage.Path)
	}
	if c.Datasets.Directory != "/etc/nishidb/datasets" {
		t.Errorf("datasets.directory = %q, queria /etc/nishidb/datasets", c.Datasets.Directory)
	}
	if c.Server.Bind != "127.0.0.1" {
		t.Errorf("server.bind = %q, queria 127.0.0.1", c.Server.Bind)
	}
}

// TestPackagedLogrotateRotatesDailyWithThreeRetentions fixa o contrato do
// arquivo de rotação de log: o serviço NishiDB escreve o log em
// /var/log/nishidb/service.log via append:, exigindo rotação diária, retenção
// de no máximo 3 arquivos rotacionados e copytruncate para não perder o
// descritor aberto pelo systemd.
func TestPackagedLogrotateRotatesDailyWithThreeRetentions(t *testing.T) {
	path := filepath.Join(repoRoot, "packaging", "logrotate", "nishidb")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	content := string(raw)

	for _, want := range []string{
		"/var/log/nishidb/service.log",
		"daily",
		"rotate 3",
		"copytruncate",
		"missingok",
		"notifempty",
		"compress",
		"delaycompress",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("packaging/logrotate/nishidb não contém a diretiva %q\nconteúdo:\n%s", want, content)
		}
	}
}

// parseUnitFile lê um arquivo de unit systemd como DADO — pares chave=valor,
// ignorando cabeçalho de seção ([Unit], [Service]...), linha em branco e
// comentário (# ou ;). É o parser mais simples que resolve o que os testes
// abaixo precisam; não interpreta include (.d/), variável nem quoting —
// nenhuma das duas units deste repositório usa isso.
func parseUnitFile(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "[") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return out
}

// TestPackagedServiceUnitMatchesThePilotOperationalSettings fixa o
// ExecStart/User do layout FHS e que os ajustes operacionais — KillSignal,
// TimeoutStopSec, LimitNOFILE, Restart — são os MESMOS da unit do piloto
// (packaging/systemd/nishidb.service). A comparação é entre as duas
// EXTRAÇÕES, nunca contra um valor hardcoded aqui: hardcodar reabriria,
// neste teste, o defeito que ele existe para fechar — alguém ajusta um valor
// operacional numa unit (depois de um incidente, por exemplo) e esquece a
// outra, e o gate segue verde porque cada lado bate com a constante escrita
// à mão, não com o vizinho.
//
// StateDirectoryMode/LogsDirectoryMode NÃO entram nessa comparação: são
// chaves exclusivas da unit do pacote (a do piloto nem usa StateDirectory/
// LogsDirectory, layout de symlink próprio), então são fixadas direto aqui,
// sem par no piloto para comparar.
func TestPackagedServiceUnitMatchesThePilotOperationalSettings(t *testing.T) {
	packageUnit := parseUnitFile(t, filepath.Join(repoRoot, "packaging", "deb", "nishidb.service"))
	pilotUnit := parseUnitFile(t, filepath.Join(repoRoot, "packaging", "systemd", "nishidb.service"))

	if !strings.HasPrefix(packageUnit["ExecStart"], "/usr/bin/nishidb server") {
		t.Errorf("ExecStart = %q, esperava prefixo \"/usr/bin/nishidb server\"", packageUnit["ExecStart"])
	}
	if !strings.Contains(packageUnit["ExecStart"], "--config /etc/nishidb/nishidb.yaml") {
		t.Errorf("ExecStart não referencia --config /etc/nishidb/nishidb.yaml: %q", packageUnit["ExecStart"])
	}
	if packageUnit["User"] != "nishidb" {
		t.Errorf("User = %q, queria nishidb", packageUnit["User"])
	}
	if packageUnit["StateDirectoryMode"] != "0750" {
		t.Errorf("StateDirectoryMode = %q, queria 0750", packageUnit["StateDirectoryMode"])
	}
	if packageUnit["LogsDirectoryMode"] != "0750" {
		t.Errorf("LogsDirectoryMode = %q, queria 0750", packageUnit["LogsDirectoryMode"])
	}

	for _, key := range []string{"KillSignal", "TimeoutStopSec", "LimitNOFILE", "Restart"} {
		pilotValue, pilotHasKey := pilotUnit[key]
		if !pilotHasKey {
			t.Fatalf("a unit do piloto não declara %s — a comparação perde a referência", key)
		}
		if packageUnit[key] != pilotValue {
			t.Errorf("%s = %q na unit do pacote, %q na unit do piloto — os dois ajustes vieram do mesmo incidente e precisam continuar iguais", key, packageUnit[key], pilotValue)
		}
	}
}

// ---------------------------------------------------------------------
// Sandbox de execução dos scripts de mantenedor (postinst/prerm/postrm)
// ---------------------------------------------------------------------
//
// As garantias de restart-after-upgrade (postinst religar SEM re-habilitar
// autostart), de prerm nunca parar o serviço em upgrade e de postrm nunca
// tocar em estado/config em upgrade são texto normativo em README,
// CLAUDE.md e nos próprios cabeçalhos dos scripts — e texto normativo que
// ninguém executa é o defeito de primeira classe da doutrina de
// verificação do projeto (godoc/comentário afirmando barreira que o código
// não estabelece). Os testes abaixo EXECUTAM os três scripts de verdade,
// com TODO comando externo que eles chamam substituído por um dublê que só
// registra a chamada — nenhum getent/useradd/systemctl/userdel/rm REAL da
// máquina que roda `go test` é tocado.

// stubTools são todos os comandos externos que os três scripts de
// mantenedor chamam. Ausência de qualquer um deles do PATH controlado faria
// o script real abortar (`command -v` não os encontraria, ou a chamada
// direta — getent/useradd/chmod/mkdir/rm, que não têm guarda de
// `command -v` antes — sairia "not found"), então TODOS entram na lista.
var stubTools = []string{
	"getent", "useradd", "chmod", "mkdir",
	"systemctl", "deb-systemd-invoke", "deb-systemd-helper",
	"userdel", "rm",
}

// writeStub escreve, em dir/name, um dublê POSIX sh que registra sua
// própria chamada (nome da ferramenta + argumentos, uma linha por chamada)
// no arquivo apontado por $STUB_LOG e sai 0 — a menos que $STUB_FAIL_CMD
// seja exatamente "<name> <primeiro argumento>", caso em que sai com
// $STUB_FAIL_EXIT (default 1). Autocontido: só usa builtins de shell
// (printf, for, if, exit) — não depende de nenhum binário externo, então
// funciona mesmo com o PATH controlado que os testes montam (só os próprios
// stubTools).
func writeStub(t *testing.T, dir, name string) {
	t.Helper()
	content := "#!/bin/sh\n" +
		"{\n" +
		"  printf '%s' '" + name + "'\n" +
		"  for a in \"$@\"; do printf ' %s' \"$a\"; done\n" +
		"  printf '\\n'\n" +
		"} >> \"$STUB_LOG\"\n" +
		"if [ \"$STUB_FAIL_CMD\" = \"" + name + " $1\" ]; then\n" +
		"  exit \"${STUB_FAIL_EXIT:-1}\"\n" +
		"fi\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o755); err != nil {
		t.Fatalf("escrevendo dublê %s: %v", name, err)
	}
}

// locateSh resolve o interpretador POSIX que os três scripts de mantenedor
// declaram no shebang (#!/bin/sh). Ausência é FALHA, não SKIP — mesmo
// argumento de locateBash (rollback_update_test.go) e locateDpkgDeb, acima.
func locateSh(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("sh")
	if err != nil {
		t.Fatalf("sh não está no PATH e é o interpretador exigido pelos scripts de mantenedor do pacote (#!/bin/sh): %v", err)
	}
	return path
}

// requireSystemdHost falha alto se /run/systemd/system não existir: os três
// scripts de mantenedor testam esse caminho ANTES de chamar systemctl/
// deb-systemd-*, e sem ele os cenários de restart-after-upgrade deste bloco
// nunca chegariam a exercitar a chamada que existem para fixar —
// silenciosamente, sem SKIP explícito (débito 53). O runner ubuntu-latest e
// qualquer host Linux-only deste projeto rodam systemd como PID 1; é a
// mesma pressuposição que os scripts já fazem.
func requireSystemdHost(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/run/systemd/system"); err != nil {
		t.Fatalf("/run/systemd/system ausente — este gate exige um host com systemd como PID 1 (mesma pressuposição dos scripts de mantenedor): %v", err)
	}
}

// runMaintainerScript executa packaging/deb/<script> DE VERDADE, com TODOS
// os stubTools na frente do PATH (nenhuma ferramenta real de sistema é
// alcançável) e devolve stdout, stderr, código de saída e as chamadas
// registradas pelos dublês, na ordem em que ocorreram.
//
// failCmd/failExit injetam falha numa chamada específica
// ("<ferramenta> <primeiro argumento>", ex.: "deb-systemd-invoke
// try-restart") — failCmd vazio significa "nenhuma falha injetada, todo
// dublê sai 0".
func runMaintainerScript(t *testing.T, script string, args []string, failCmd, failExit string) (stdout, stderr string, code int, calls []string) {
	t.Helper()
	return runMaintainerScriptExcluding(t, script, args, nil, failCmd, failExit)
}

// runMaintainerScriptExcluding é runMaintainerScript com a lista `exclude`
// de stubTools DELIBERADAMENTE fora do PATH — usado para exercitar um
// fallback (ex.: "sem deb-systemd-helper instalado, cai para systemctl").
// A ferramenta excluída fica genuinamente AUSENTE do PATH, não apenas sem
// registrar chamada: se o script tentar chamá-la mesmo assim, o `sh` sai
// "not found" e o teste vê isso no código de saída, não um log vazio
// enganoso.
func runMaintainerScriptExcluding(t *testing.T, script string, args []string, exclude []string, failCmd, failExit string) (stdout, stderr string, code int, calls []string) {
	t.Helper()
	requireSystemdHost(t)

	excluded := make(map[string]bool, len(exclude))
	for _, tool := range exclude {
		excluded[tool] = true
	}

	stubDir := t.TempDir()
	for _, tool := range stubTools {
		if excluded[tool] {
			continue
		}
		writeStub(t, stubDir, tool)
	}
	logPath := filepath.Join(t.TempDir(), "calls.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	scriptPath := filepath.Join(repoRoot, "packaging", "deb", script)
	cmd := exec.Command(locateSh(t), append([]string{scriptPath}, args...)...)
	env := []string{"PATH=" + stubDir, "STUB_LOG=" + logPath}
	if failCmd != "" {
		env = append(env, "STUB_FAIL_CMD="+failCmd, "STUB_FAIL_EXIT="+failExit)
	}
	cmd.Env = env

	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("packaging/deb/%s não chegou a executar: %v", script, err)
		}
	}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("lendo log de chamadas: %v", err)
	}
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if line != "" {
			calls = append(calls, line)
		}
	}
	return out.String(), errBuf.String(), code, calls
}

// callsWithFirstArg devolve as chamadas cujo PRIMEIRO ARGUMENTO (depois do
// nome da ferramenta) é exatamente want — "systemctl try-restart" e
// "deb-systemd-invoke try-restart" batem com want="try-restart", por
// exemplo, sem amarrar a asserção a qual dos dois o script preferiu chamar.
func callsWithFirstArg(calls []string, want string) []string {
	var out []string
	for _, c := range calls {
		fields := strings.Fields(c)
		if len(fields) >= 2 && fields[1] == want {
			out = append(out, c)
		}
	}
	return out
}

// callsOfTool devolve as chamadas cuja FERRAMENTA (primeiro campo) é tool.
func callsOfTool(calls []string, tool string) []string {
	var out []string
	for _, c := range calls {
		fields := strings.Fields(c)
		if len(fields) >= 1 && fields[0] == tool {
			out = append(out, c)
		}
	}
	return out
}

// TestPostinstUpgradeRestartsButNeverReEnables fixa o coração do
// restart-after-upgrade: um upgrade in-place (`configure <versão-antiga>`,
// $2 não vazio) religa o serviço para carregar o binário novo, mas NUNCA
// chama enable — reabilitar autostart numa reconfiguração desfaria a
// decisão de um operador que desligou a unit de propósito entre versões, e
// a mensagem de instalação nova não é impressa (ela é específica do
// primeiro `configure`).
func TestPostinstUpgradeRestartsButNeverReEnables(t *testing.T) {
	stdout, _, code, calls := runMaintainerScript(t, "postinst", []string{"configure", "0.9.1"}, "", "")
	if code != 0 {
		t.Fatalf("postinst configure 0.9.1 (upgrade) saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(callsWithFirstArg(calls, "try-restart")) == 0 {
		t.Errorf("upgrade não chamou try-restart\nchamadas: %v", calls)
	}
	if got := callsWithFirstArg(calls, "enable"); len(got) != 0 {
		t.Errorf("upgrade chamou enable — reabilitaria autostart que o operador pode ter desligado de propósito: %v", got)
	}
	if strings.Contains(stdout, "nishidb installed") {
		t.Errorf("upgrade imprimiu a mensagem de instalação NOVA: %q", stdout)
	}
}

// TestPostinstFreshInstallEnablesButNeverRestarts é o espelho: instalação
// NOVA (`configure` sem segundo argumento) habilita o autostart e nunca
// chama try-restart — não há serviço rodando para religar —, e imprime a
// mensagem de instalação.
//
// A asserção mira ESPECIFICAMENTE "deb-systemd-helper enable", não uma
// checagem genérica de "algo chamou enable": o comentário do postinst é
// normativo sobre PREFERIR o helper quando ele existe (o mecanismo debhelper
// padrão, que respeita unit mascarada), e "systemctl enable" também bateria
// com uma checagem frouxa — deixando essa preferência sem prova nenhuma.
func TestPostinstFreshInstallEnablesButNeverRestarts(t *testing.T) {
	stdout, _, code, calls := runMaintainerScript(t, "postinst", []string{"configure"}, "", "")
	if code != 0 {
		t.Fatalf("postinst configure (instalação nova) saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(callsWithFirstArg(callsOfTool(calls, "deb-systemd-helper"), "enable")) == 0 {
		t.Errorf("instalação nova não chamou \"deb-systemd-helper enable\" (mecanismo PREFERIDO, presente no sandbox)\nchamadas: %v", calls)
	}
	if got := callsWithFirstArg(callsOfTool(calls, "systemctl"), "enable"); len(got) != 0 {
		t.Errorf("instalação nova chamou \"systemctl enable\" com deb-systemd-helper disponível — devia preferir o helper: %v", got)
	}
	if got := callsWithFirstArg(calls, "try-restart"); len(got) != 0 {
		t.Errorf("instalação nova chamou try-restart — não há serviço rodando para religar: %v", got)
	}
	if !strings.Contains(stdout, "nishidb installed") {
		t.Errorf("instalação nova não imprimiu a mensagem de instalação: %q", stdout)
	}
}

// TestPostinstFreshInstallFallsBackToSystemctlEnableWithoutHelper fixa o
// FALLBACK: num host sem debhelper instalado (deb-systemd-helper ausente do
// PATH — não só sem registrar chamada, genuinamente ausente), a instalação
// nova tem de cair para `systemctl enable` puro, e não falhar silenciosamente
// sem habilitar nada.
func TestPostinstFreshInstallFallsBackToSystemctlEnableWithoutHelper(t *testing.T) {
	_, _, code, calls := runMaintainerScriptExcluding(t, "postinst", []string{"configure"}, []string{"deb-systemd-helper"}, "", "")
	if code != 0 {
		t.Fatalf("postinst configure (sem deb-systemd-helper) saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(callsOfTool(calls, "deb-systemd-helper")) != 0 {
		t.Fatalf("dublê deb-systemd-helper foi chamado mesmo excluído do PATH — sandbox quebrado: %v", calls)
	}
	if len(callsWithFirstArg(callsOfTool(calls, "systemctl"), "enable")) == 0 {
		t.Errorf("sem deb-systemd-helper, a instalação nova não caiu para \"systemctl enable\"\nchamadas: %v", calls)
	}
}

// TestPostinstCreatesSystemUserWhenMissing fixa os argumentos EXATOS do
// useradd quando getent reporta o usuário nishidb como ausente: --system
// (faixa de UID/GID de sistema, não de usuário comum), --user-group (grupo
// próprio, não um grupo compartilhado), --home-dir /var/lib/nishidb (o
// mesmo diretório que StateDirectory da unit usa) e --shell
// /usr/sbin/nologin (usuário de serviço, nunca login interativo).
func TestPostinstCreatesSystemUserWhenMissing(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "postinst", []string{"configure"}, "getent passwd", "2")
	if code != 0 {
		t.Fatalf("postinst saiu %d com getent reportando o usuário ausente\nchamadas: %v", code, calls)
	}
	useraddCalls := callsOfTool(calls, "useradd")
	if len(useraddCalls) == 0 {
		t.Fatalf("postinst não chamou useradd com o usuário ausente\nchamadas: %v", calls)
	}
	want := "useradd --system --user-group --home-dir /var/lib/nishidb --shell /usr/sbin/nologin nishidb"
	if useraddCalls[0] != want {
		t.Errorf("useradd = %q, queria %q", useraddCalls[0], want)
	}
}

// TestPostinstUpgradeToleratesTryRestartFailure fixa que um try-restart que
// falha (unit mascarada pelo operador, por exemplo) NÃO derruba o
// postinst — a falha é tolerada de propósito (ver cabeçalho do script).
func TestPostinstUpgradeToleratesTryRestartFailure(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "postinst", []string{"configure", "0.9.1"}, "deb-systemd-invoke try-restart", "3")
	if code != 0 {
		t.Fatalf("postinst saiu %d com o try-restart falhando (dublê saiu 3) — a falha devia ser tolerada\nchamadas: %v", code, calls)
	}
	if len(callsWithFirstArg(calls, "try-restart")) == 0 {
		t.Errorf("try-restart não chegou a ser chamado\nchamadas: %v", calls)
	}
}

// TestPostinstCreatesDatasetsDirectoryBeforeChmod fixa DUAS coisas sobre
// /etc/nishidb/datasets na passada de `configure`: (1) mkdir -p é chamado
// ANTES do chmod, e (2) o código de saída do chmod REALMENTE se propaga
// (set -e) — não é uma chamada cujo resultado o script ignoraria. A junção
// das duas é o que prova que a ordem importa de verdade: um chmod que
// falhasse contra o diretório ausente (o que aconteceria SE o mkdir não
// tivesse rodado antes, ou não tivesse rodado) aborta o postinst, não passa
// batido. O teste NÃO prova que mkdir cria o diretório no sistema de
// arquivos real — os dublês não tocam disco nenhum — só a ordem das
// chamadas e a propagação do código de saída.
func TestPostinstCreatesDatasetsDirectoryBeforeChmod(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "postinst", []string{"configure"}, "", "")
	if code != 0 {
		t.Fatalf("postinst configure saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	mkdirIndex, chmodIndex := -1, -1
	for i, c := range calls {
		fields := strings.Fields(c)
		if len(fields) < 2 || fields[len(fields)-1] != "/etc/nishidb/datasets" {
			continue
		}
		switch fields[0] {
		case "mkdir":
			if mkdirIndex == -1 {
				mkdirIndex = i
			}
		case "chmod":
			if chmodIndex == -1 {
				chmodIndex = i
			}
		}
	}
	if mkdirIndex == -1 {
		t.Fatalf("postinst não chamou mkdir em /etc/nishidb/datasets\nchamadas: %v", calls)
	}
	if chmodIndex == -1 {
		t.Fatalf("postinst não chamou chmod em /etc/nishidb/datasets\nchamadas: %v", calls)
	}
	if mkdirIndex >= chmodIndex {
		t.Errorf("mkdir (chamada %d) não veio ANTES do chmod (chamada %d)", mkdirIndex, chmodIndex)
	}

	// Prova de sobrevivência: se o chmod falhasse — o que aconteceria de
	// verdade contra um diretório ausente, na hipótese do mkdir -p não ter
	// rodado antes —, o postinst tem de ABORTAR, não seguir como se nada
	// tivesse acontecido.
	_, _, failCode, failCalls := runMaintainerScript(t, "postinst", []string{"configure"}, "chmod 0755", "1")
	if failCode == 0 {
		t.Errorf("postinst saiu 0 com o chmod de /etc/nishidb/datasets falhando — um diretório ausente devia abortar a configuração\nchamadas: %v", failCalls)
	}
}

// requirePrermActuallyStopsOnRemove roda o cenário `remove` do prerm e
// falha se o serviço não for parado. É a PROVA DE VITALIDADE chamada de
// dentro de outros testes deste bloco (upgrade/deconfigure): sozinhos, eles
// só provam AUSÊNCIA de chamada — e um prerm MORTO (`exit 0` antes de
// alcançar o case) passaria por essa ausência do mesmo jeito que o script
// correto. Só um cenário que EXIGE chamada (remove) descarta essa
// possibilidade.
func requirePrermActuallyStopsOnRemove(t *testing.T) {
	t.Helper()
	_, _, code, calls := runMaintainerScript(t, "prerm", []string{"remove"}, "", "")
	if code != 0 {
		t.Fatalf("prova de vitalidade: prerm remove saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(callsWithFirstArg(calls, "stop")) == 0 {
		t.Fatalf("prova de vitalidade: prerm remove não parou o serviço — o script pode estar morto (case nunca alcançado)\nchamadas: %v", calls)
	}
}

// requirePostrmActuallyActsOnPurge é o equivalente para postrm: roda
// `purge` e falha se NADA for chamado — a prova de vitalidade para os
// testes que, sozinhos, só provam ausência de chamada em upgrade.
func requirePostrmActuallyActsOnPurge(t *testing.T) {
	t.Helper()
	_, _, code, calls := runMaintainerScript(t, "postrm", []string{"purge"}, "", "")
	if code != 0 {
		t.Fatalf("prova de vitalidade: postrm purge saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(calls) == 0 {
		t.Fatalf("prova de vitalidade: postrm purge não chamou nada — o script pode estar morto (case nunca alcançado)")
	}
}

// TestPrermUpgradeNeverStopsTheService fixa que um upgrade in-place
// (`prerm upgrade <versão-nova>`) NÃO para nem desabilita o serviço — ver
// cabeçalho do prerm: parar aqui deixaria o serviço fora do ar pela janela
// inteira do upgrade, antes mesmo do binário novo estar no disco. A prova
// de vitalidade (requirePrermActuallyStopsOnRemove) descarta um prerm morto
// que "passaria" por pura ausência de chamada.
func TestPrermUpgradeNeverStopsTheService(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "prerm", []string{"upgrade", "0.9.1"}, "", "")
	if code != 0 {
		t.Fatalf("prerm upgrade saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if got := callsWithFirstArg(calls, "stop"); len(got) != 0 {
		t.Errorf("prerm upgrade parou o serviço: %v", got)
	}
	requirePrermActuallyStopsOnRemove(t)
}

// TestPrermDeconfigureNeverStopsTheService fixa o item 1 da revisão:
// `deconfigure` é transição PASSAGEIRA (o dpkg reconfigura em seguida com
// `postinst configure <MESMA versão>`, que cai no ramo de upgrade — religa,
// mas nunca chama enable), não saída — parar/desabilitar aqui faria a unit
// perder o autostart em silêncio. Mesma prova de vitalidade do teste
// irmão de upgrade.
func TestPrermDeconfigureNeverStopsTheService(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "prerm", []string{"deconfigure"}, "", "")
	if code != 0 {
		t.Fatalf("prerm deconfigure saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(calls) != 0 {
		t.Errorf("prerm deconfigure chamou algo (esperava zero chamadas): %v", calls)
	}
	requirePrermActuallyStopsOnRemove(t)
}

// TestPrermRemoveStopsTheService é o espelho: remoção de verdade para e
// desabilita o serviço.
func TestPrermRemoveStopsTheService(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "prerm", []string{"remove"}, "", "")
	if code != 0 {
		t.Fatalf("prerm remove saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(callsWithFirstArg(calls, "stop")) == 0 {
		t.Errorf("prerm remove não parou o serviço\nchamadas: %v", calls)
	}
	if len(callsWithFirstArg(calls, "disable")) == 0 {
		t.Errorf("prerm remove não desabilitou o serviço\nchamadas: %v", calls)
	}
}

// TestPostrmPurgeRemovesUserAndDirectories fixa o purge: some com estado,
// log, config E o usuário de sistema.
func TestPostrmPurgeRemovesUserAndDirectories(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "postrm", []string{"purge"}, "", "")
	if code != 0 {
		t.Fatalf("postrm purge saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	rmCalls := callsOfTool(calls, "rm")
	if len(rmCalls) == 0 {
		t.Fatalf("postrm purge não chamou rm\nchamadas: %v", calls)
	}
	joined := strings.Join(rmCalls, "\n")
	for _, want := range []string{"/var/lib/nishidb", "/var/log/nishidb", "/etc/nishidb"} {
		if !strings.Contains(joined, want) {
			t.Errorf("postrm purge não removeu %s\nchamadas rm: %s", want, joined)
		}
	}
	if len(callsOfTool(calls, "userdel")) == 0 {
		t.Errorf("postrm purge não removeu o usuário nishidb\nchamadas: %v", calls)
	}
}

// TestPostrmUpgradeCallsNothing fixa que um upgrade in-place não mexe em
// NADA — nem estado nem config podem ser tocados antes de o postinst da
// versão nova rodar e religar o serviço (ver cabeçalho do postrm). A prova
// de vitalidade (requirePostrmActuallyActsOnPurge) descarta um postrm morto
// que "passaria" por pura ausência de chamada.
func TestPostrmUpgradeCallsNothing(t *testing.T) {
	_, _, code, calls := runMaintainerScript(t, "postrm", []string{"upgrade", "0.9.1"}, "", "")
	if code != 0 {
		t.Fatalf("postrm upgrade saiu %d, queria 0\nchamadas: %v", code, calls)
	}
	if len(calls) != 0 {
		t.Errorf("postrm upgrade chamou algo (esperava zero chamadas): %v", calls)
	}
	requirePostrmActuallyActsOnPurge(t)
}
