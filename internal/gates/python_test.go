package gates_test

import (
	"regexp"
	"strings"
	"testing"
)

// Os gates do client Python, no molde exato dos do driver JDBC: os arquivos do
// client são lidos como DADO, e o que se compara são duas EXTRAÇÕES — nunca
// uma extração contra um valor decorado, que envelhece junto com quem o
// escreveu.
//
// Eles vivem em Go, e não em pytest, pela mesma razão prática que os do JDBC:
// `go test ./...` roda em todo commit e no CI, enquanto uma suíte Python só
// reprova quem lembra de rodá-la. Um pyproject alterado sem rodar o Python
// chegaria ao main sem nada acusar.

const caminhoDoPyproject = "clients/python/pyproject.toml"
const caminhoDoInitPython = "clients/python/src/nishidb/__init__.py"
const caminhoDosErrosPython = "clients/python/src/nishidb/erros.py"

// blocoProject recorta o bloco `[project]` do pyproject.
//
// Por recorte de texto, e não por parser: o Go não tem TOML na stdlib, e
// acrescentar uma dependência ao ENGINE para ler o manifesto do client seria
// pagar em dependência exatamente o que o client existe para não custar. O
// recorte é suficiente porque o que se afirma são linhas de forma fixa, e a
// falha do recorte é detectada pelos próprios testes (bloco vazio reprova).
func blocoProject(t *testing.T) string {
	t.Helper()
	texto := leArquivo(t, caminhoDoPyproject)
	ini := strings.Index(texto, "\n[project]\n")
	if ini < 0 {
		t.Fatalf("%s não tem a seção [project]", caminhoDoPyproject)
	}
	resto := texto[ini+len("\n[project]\n"):]
	if fim := strings.Index(resto, "\n["); fim >= 0 {
		resto = resto[:fim]
	}
	if strings.TrimSpace(resto) == "" {
		t.Fatalf("a seção [project] de %s saiu vazia: o recorte quebrou, e um gate "+
			"que não acha nada passa sempre", caminhoDoPyproject)
	}
	return resto
}

// "Zero dependência de runtime" é a doutrina do binário único traduzida para um
// pacote do PyPI, e este é o gate que a sustenta.
//
// O que ela promete é sobre o ambiente de QUEM INSTALA: um client que arraste
// `requests` obriga quem o usa a resolvê-lo, e deixa de ser instalável no host
// que se está diagnosticando — que é justamente onde um client de diagnóstico
// precisa entrar. `[project.optional-dependencies].dev` não conta: um extra
// nunca é instalado por `pip install nishidb`.
func TestOPyprojectDoClientPythonNaoTemDependenciaDeRuntime(t *testing.T) {
	bloco := blocoProject(t)
	re := regexp.MustCompile(`(?m)^dependencies\s*=\s*(.*)$`)
	casou := re.FindStringSubmatch(bloco)
	if casou == nil {
		t.Fatalf("%s não declara `dependencies` em [project]: declare-o VAZIO, "+
			"para que a ausência seja uma afirmação e não um esquecimento", caminhoDoPyproject)
	}
	if got := strings.TrimSpace(casou[1]); got != "[]" {
		t.Errorf("dependencies = %s em %s: o client Python é stdlib-only", got, caminhoDoPyproject)
	}
}

// Duas fontes para o mesmo número divergem sozinhas: o pyproject sobe de versão
// numa release e o `__version__` fica para trás, e a partir daí o pacote
// instalado reporta uma versão que nunca existiu — sem nada acusar, porque as
// duas continuam válidas. É o irmão de TestAVersaoAnunciadaPeloDriverEhADoArtefato.
func TestAVersaoDoClientPythonEhADoPyproject(t *testing.T) {
	bloco := blocoProject(t)
	reProj := regexp.MustCompile(`(?m)^version\s*=\s*"([^"]+)"`)
	casou := reProj.FindStringSubmatch(bloco)
	if casou == nil {
		t.Fatalf("%s não declara version em [project]", caminhoDoPyproject)
	}
	versao := casou[1]

	init := leArquivo(t, caminhoDoInitPython)
	reInit := regexp.MustCompile(`(?m)^__version__\s*=\s*"([^"]+)"`)
	casouInit := reInit.FindStringSubmatch(init)
	if casouInit == nil {
		t.Fatalf("%s não declara __version__", caminhoDoInitPython)
	}
	if casouInit[1] != versao {
		t.Errorf("__version__ = %q mas o pyproject diz %q", casouInit[1], versao)
	}
}

// TODO código do protocolo tem exceção no client Python.
//
// É o irmão exato de TestTodoCodigoDoProtocoloTemSqlStateNoDriver, e existe
// pelo mesmo motivo: o modo de falha é SILENCIOSO. Um código novo em Go sem
// entrada aqui não quebra o client — ele degrada para DatabaseError com o
// código cru, que é a degradação honesta —, mas o usuário perde a classe
// específica que diria o que fazer, e ninguém liga uma coisa à outra.
//
// A lista não é escrita à mão em lugar nenhum: sai do go/ast sobre
// protocolo.go de um lado e do texto de erros.py do outro.
func TestTodoCodigoDoProtocoloTemExcecaoNoClientPython(t *testing.T) {
	codigos := codigosDoProtocolo(t)
	if len(codigos) < 15 {
		t.Fatalf("só %d códigos extraídos de protocolo.go: a extração quebrou, e um gate que "+
			"não acha nada passa sempre", len(codigos))
	}

	python := leArquivo(t, caminhoDosErrosPython)
	for nomeGo, valor := range codigos {
		// A constante de módulo, para que o usuário possa comparar
		// `err.code == nishidb.BAD_PARAMETER` sem digitar a string.
		decl := valor + ` = "` + valor + `"`
		if !strings.Contains(python, decl) {
			t.Errorf("%s (%s) não é constante em erros.py: acrescente `%s`", nomeGo, valor, decl)
			continue
		}
		// A constante existir não basta: ela precisa estar NA TABELA, senão o
		// código continua caindo no DatabaseError genérico.
		if !regexp.MustCompile(`(?m)^\s+` + valor + `:\s`).MatchString(python) {
			t.Errorf("%s (%s) tem constante mas não está na tabela CODIGOS de erros.py",
				nomeGo, valor)
		}
	}
}

// O paramstyle anunciado tem de ser o que o servidor de fato aceita.
//
// A PEP 249 obriga toda ferramenta a ler `paramstyle` para saber como escrever
// os placeholders. Anunciar o errado faz TODA query gerada por ferramenta
// falhar no bind — e o sintoma aparece longe daqui, como "o SQLAlchemy não
// funciona com este banco".
func TestOClientPythonAnunciaOParamstyleDoServidor(t *testing.T) {
	init := leArquivo(t, caminhoDoInitPython)
	if !strings.Contains(init, `paramstyle = "qmark"`) {
		t.Errorf("%s não anuncia paramstyle qmark: o NishiDB usa `?` posicional",
			caminhoDoInitPython)
	}
	// threadsafety 1 é a verdade: por baixo de cada Connection há UMA conexão
	// HTTP, que executa uma requisição por vez. Anunciar 2 ou 3 convidaria a um
	// compartilhamento que corromperia o stream no meio de uma leitura.
	if !strings.Contains(init, "threadsafety = 1") {
		t.Errorf("%s não anuncia threadsafety = 1", caminhoDoInitPython)
	}
}

// O job `python` roda o teste de INTEGRAÇÃO do client, e não só os unitários.
//
// Sem ele, `make test-python-it` só roda para quem lembrar de chamá-lo — e o
// que fica sem cobertura é justamente a metade que os unitários não podem
// provar. Não é hipótese: o bug de reuso de conexão da primeira versão do
// client passou pela suíte inteira e só apareceu contra o binário real, porque
// o servidor de verdade responde /v1/query em chunked e o de mentira respondia
// com Content-Length.
//
// A verificação roda sobre o SCRIPT do job, e não sobre o arquivo bruto: um
// comentário mencionando "test-python-it" não pode satisfazer o teste com o
// passo de fato removido.
func TestWorkflowDeCIRodaOTesteDeIntegracaoDoClientPython(t *testing.T) {
	script := scriptDoJob(t, "python")
	if !strings.Contains(script, "test-python-it") {
		t.Error("o job `python` não roda `make test-python-it` — o client fica sem prova " +
			"contra o servidor real em CI")
	}
}

// O lint e a tipagem do client rodam em CI.
//
// Job próprio, e não um passo do `test`: ele exige instalar ferramenta, e
// `make test-python` tem de continuar rodando com zero `pip install` — é essa
// promessa que o torna executável em qualquer máquina com python3.
func TestWorkflowDeCIRodaOLintDoClientPython(t *testing.T) {
	if !strings.Contains(scriptDoJob(t, "python-lint"), "lint-python") {
		t.Error("o job `python-lint` não roda `make lint-python`")
	}
}

var rePythonVersion = regexp.MustCompile(`python-version:\s*['"]?([^\s'"\n]+)['"]?`)

// A versão de Python do workflow é a EXATA do piso declarado no pyproject.
//
// A comparação é entre as duas EXTRAÇÕES — do ci.yml e do pyproject.toml —,
// nunca contra um "3.10" escrito à mão neste teste. Hardcodar o número
// reabriria aqui o defeito que o teste fecha: alguém sobe o piso do client, o
// workflow continua validando com o interpretador antigo, e o gate segue verde
// porque cada lado é conferido contra a constante e não contra o vizinho.
//
// O CI roda no PISO de propósito: é a versão em que uma sintaxe nova demais
// quebra, e validar só na mais recente deixaria o `requires-python` mentir.
func TestWorkflowDeCIPinaPythonNoPisoDoPyproject(t *testing.T) {
	achados := rePythonVersion.FindAllStringSubmatch(leWorkflowDeCI(t), -1)
	if len(achados) == 0 {
		t.Fatal("o workflow não fixa python-version em nenhum step de actions/setup-python")
	}

	bloco := blocoProject(t)
	rePiso := regexp.MustCompile(`(?m)^requires-python\s*=\s*">=\s*([0-9]+\.[0-9]+)"`)
	casou := rePiso.FindStringSubmatch(bloco)
	if casou == nil {
		t.Fatalf("%s não declara requires-python na forma \">=X.Y\"", caminhoDoPyproject)
	}
	piso := casou[1]

	for _, achado := range achados {
		// Faixa solta é recusada pelo mesmo motivo que a versão de Go é fixa:
		// versão de instrumento é configuração que muda o resultado, e deixar o
		// runner escolher o patch faz o CI validar num interpretador que
		// ninguém mediu.
		if strings.ContainsAny(achado[1], "+*") || strings.Count(achado[1], ".") > 1 {
			t.Errorf("python-version = %q não é o piso major.minor exato", achado[1])
		}
		if achado[1] != piso {
			t.Errorf("python-version = %q no workflow e requires-python >= %q no pyproject: "+
				"o CI validaria o client num Python que não é o piso declarado", achado[1], piso)
		}
	}
}

// A saída de build do client não pode ser versionável por acidente.
//
// Escopado, e nunca `dist/` solto para o client: o repositório já tem um
// `dist/` na raiz por outra razão, e um padrão largo esconderia um diretório
// chamado dist que fosse código.
func TestOsArtefatosDoClientPythonEstaoNoGitignore(t *testing.T) {
	ignore := leArquivo(t, ".gitignore")
	for _, padrao := range []string{
		"clients/python/dist/",
		"clients/python/**/__pycache__/",
		"clients/python/.mypy_cache/",
	} {
		if !strings.Contains(ignore, padrao) {
			t.Errorf(".gitignore não ignora %q", padrao)
		}
	}
}
