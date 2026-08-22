// Package buildinfo é a identidade do binário — a issue #11 nasceu porque o
// piloto em produção não tinha como correlacionar o que estava no ar com um
// commit. String() é a única função de apresentação: CLI, log de subida do
// servidor, GET /v1/stats, o painel Python e o artefato de benchmark chamam
// TODOS ela, para nunca haver duas superfícies narrando a mesma subida com
// textos diferentes.
package buildinfo

import "runtime/debug"

// Versao, Commit e Data são POPULADAS PELO LINKER, via:
//
//	-ldflags "-X .../internal/buildinfo.Versao=... \
//	          -X .../internal/buildinfo.Commit=... \
//	          -X .../internal/buildinfo.Data=..."
//
// que é exatamente o que o alvo `build` do Makefile faz. Só o linker escreve
// em variável de pacote por esse caminho — não há outro jeito de carimbar um
// valor calculado no momento do build dentro do binário. Ficam vazias em
// `go test`, `go run` e em qualquer build que não passe os três ldflags.
var (
	Versao string
	Commit string
	Data   string
)

// semInformacao é a resposta quando NEM os ldflags NEM o carimbo do VCS têm o
// que informar — o caso do binário de teste, que em geral não carrega
// vcs.revision.
const semInformacao = "dev"

// leInfo é indireção sobre debug.ReadBuildInfo. Existe só para o teste poder
// simular a ausência (ou a presença) do carimbo VCS sem depender de como o
// binário de teste em si foi compilado — o que variaria com o ambiente e
// tornaria o teste do fallback não-determinístico.
var leInfo = debug.ReadBuildInfo

// String monta o texto que toda superfície mostra. Com os ldflags presentes,
// é Versao seguido de commit e data entre parênteses. Sem eles, cai no
// carimbo que o próprio toolchain do Go grava de graça em todo binário
// compilado dentro de um repositório git (`go install` direto se beneficia
// disso sem precisar do Makefile) — e só quando nem isso existe é que a
// resposta vira "dev" pura.
func String() string {
	versao, commit, data := Versao, Commit, Data
	if versao == "" && commit == "" && data == "" {
		commit, data = doVCS()
	}
	if versao == "" {
		versao = semInformacao
	}
	if commit == "" && data == "" {
		return versao
	}

	texto := versao + " ("
	switch {
	case commit != "" && data != "":
		texto += "commit " + commit + ", build " + data
	case commit != "":
		texto += "commit " + commit
	default:
		texto += "build " + data
	}
	return texto + ")"
}

// doVCS lê vcs.revision e vcs.time do carimbo que o Go grava automaticamente
// quando o build roda dentro de um repositório git — o mesmo dado que
// `git describe`/`git rev-parse` leriam na hora do build, só que sem
// depender do Makefile ter rodado.
func doVCS() (commit, data string) {
	info, ok := leInfo()
	if !ok {
		return "", ""
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			commit = curto(s.Value)
		case "vcs.time":
			data = s.Value
		}
	}
	return commit, data
}

// curto abrevia o SHA completo do vcs.revision (40 caracteres) para um
// tamanho fixo mais generoso que os 7 caracteres que `git rev-parse --short`
// costuma produzir. Não precisa ser o abreviamento MÍNIMO e único do git —
// só de curto o bastante para o operador colar num grep.
func curto(rev string) string {
	const n = 12
	if len(rev) > n {
		return rev[:n]
	}
	return rev
}
