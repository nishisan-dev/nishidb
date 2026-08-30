package gates_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Este arquivo fecha duas classes de defeito que o ciclo do índice por
// segmento pagou caro, e que nenhuma ferramenta acusava porque as duas vivem
// em texto sem compilador: citação de spec que não resolve, e amostra de
// métrica sem família.
//
// A doutrina que os sustenta é a mesma dos outros gates deste pacote: ler o
// documento/código como DADO, comparar duas extrações, e falhar alto nomeando
// o que está errado — nunca SKIP silencioso.
//
// Os dois gates já foram, eles próprios, o defeito que dizem fechar. A primeira
// versão do reconhecedor de amostra só enxergava o literal com o tipo escrito
// (`metrics.Amostra{...}`) e era CEGA à forma elidida (`[]metrics.Amostra{{...}}`),
// que é a dominante no repositório: 23 dos 58 literais passavam por baixo, e
// entre eles estavam justamente as quatro do coletor Kafka que aquele mesmo
// commit tinha acabado de consertar. A primeira versão do gate de citação lia
// `.go` como bytes crus, contra a doutrina de ler código como DADO. Ambos estão
// consertados abaixo, e ambos ganharam guarda de vacuidade: gate que não
// discrimina é pior que gate nenhum, porque dá falsa segurança.

// reCitacao casa uma citação de spec: §NN ou §NN.M.
var reCitacao = regexp.MustCompile(`§(\d+)(?:\.(\d+))?`)

// reCabecalho casa um cabeçalho numerado de spec: "# 14.", "## 1.7", "### 3.2".
var reCabecalho = regexp.MustCompile(`^#{1,4}\s+(\d+(?:\.\d+)?)\.?\s`)

// reCabecalhoQualquer casa QUALQUER cabeçalho (numerado ou não), para encerrar
// a seção corrente: um divisor como "## Core" não pode deixar os itens
// seguintes atribuídos à seção anterior.
var reCabecalhoQualquer = regexp.MustCompile(`^#{1,4}\s`)

// reItemNumerado casa um item de lista numerado ("1. iniciar...").
var reItemNumerado = regexp.MustCompile(`^(\d+)\.\s`)

// reItemMarcador casa um item de lista com marcador ("* Consumir...").
var reItemMarcador = regexp.MustCompile(`^\s*[*\-]\s`)

// specsDoMarcador é o conjunto FECHADO de documentos que definem o marcador
// §NN, e a lista é explícita de propósito.
//
// # O NAMESPACE É ACHATADO, E ISSO É UMA DECISÃO REGISTRADA
//
// O repositório usa UM marcador (§NN) para MAIS DE UMA spec: a normativa da
// raiz numera as seções em nível único; `m1-design-storage.md` tem §10.2
// (checkpoint) e §14.NN (riscos); `m1-design-materializer-query.md` tem §2.NN e
// §3.NN; a spec do subsistema 1 tem sete seções próprias. Um arquivo de produção cita TRÊS
// desses documentos — `internal/storage/store.go` cita §92 (raiz), §10.2
// (design de storage) e §2.4 (subsistema) —, então separar os namespaces pela
// ORIGEM do arquivo que cita é impossível sem qualificar o marcador, e
// qualificá-lo custaria reescrever milhares de ocorrências em código e prosa.
//
// A decisão, portanto, é: o namespace é ACHATADO sobre este conjunto, e o
// preço está nomeado — uma citação a §4 resolve mesmo que o autor tenha
// pensado na outra spec. O que este gate NÃO faz é o achatamento SILENCIOSO
// que ele fazia antes: registrar como alvo todo `planning/*.md` fazia 25
// documentos definirem o namespace, e cada plano novo o alargava sozinho — um
// `## 4.` num plano qualquer ressuscitava uma citação morta. Com a lista
// fechada, alargar o namespace é um ato deliberado, visível no diff.
//
// Documento que sai da lista sem sair do disco não é permitido: o gate confere
// que os quatro existem, porque um rename silencioso ENCOLHE o oráculo e
// transforma citação viva em órfã (ou, pior, some com o alvo sem ninguém ver).
var specsDoMarcador = []string{
	"nishidb-spec-proposal.md",
	"planning/m1-design-storage.md",
	"planning/m1-design-materializer-query.md",
	"planning/spec-subsistema-1-indice-por-segmento.md",
}

// TestCitacoesDeSecaoResolvem confere que toda citação §NN/§NN.M em docs,
// README, CLAUDE.md, planning e comentários Go resolve em pelo menos uma seção
// ou item numerado das specs declaradas em specsDoMarcador.
//
// # COMO A FONTE É LIDA
//
// Como DADO, nunca por grep em prosa — a mesma doutrina do resto do pacote.
// Arquivo `.go` é parseado com `parser.ParseComments` e só os COMENTÁRIOS são
// varridos (a técnica de `citacoes_test.go`): um `§` dentro de string literal,
// de exemplo de SQL ou de mensagem de erro não é citação. Arquivo `.md` tem os
// blocos cercados por ``` pulados, pela mesma razão — citação dentro de exemplo
// não é citação.
//
// # O NÍVEL 2 (§NN.M) É UMA FAIXA, NÃO UM CONJUNTO DE POSIÇÕES
//
// §NN.M resolve quando existe um cabeçalho "NN.M" (ex.: "### 1.5 Corrupção"),
// quando M é um número ESCRITO num item numerado da seção NN, ou quando M está
// dentro da CONTAGEM de itens dessa seção — a faixa 1..N, com N sendo quantos
// itens (marcador ou numerado) a seção tem.
//
// A versão anterior montava o alvo do item numerado a partir do número
// ESCRITO e o do marcador a partir da POSIÇÃO, e aceitava só o conjunto exato
// que saía disso. O efeito prático era um alvo com buracos: um item removido de
// uma lista numerada sem renumerar os vizinhos deixa aquele número inválido para
// sempre, e a citação a ele fica órfã mesmo apontando para texto vivo. A faixa
// contígua tira o alvo da mão do editor — o gate fecha "citar seção que não
// existe", não "citar o item errado", que continua sendo revisão.
//
// A alternativa de tirar os marcadores da conta — aceitar só cabeçalho numerado
// e item com número explícito, que é o alvo que o documento realmente escreve —
// foi MEDIDA e recusada: ela deixa 14 citações vivas órfãs, entre elas as oito
// do §105, que é uma lista de marcadores.
//
// # O ERRO É AGRUPADO POR NÚMERO
//
// Um erro por NÚMERO citado, listando todos os caminhos, com o remédio na
// mensagem. Uma renumeração de seção invalida dezenas de citações de uma vez, e
// um erro por ocorrência transforma o conserto em whack-a-mole — gate assim as
// pessoas desligam.
func TestCitacoesDeSecaoResolvem(t *testing.T) {
	alvos := secoesDasSpecs(t)
	fontes := fontesDeCitacao(t)

	// Um erro por número citado; os caminhos entram na mensagem.
	locais := map[string][]string{}
	ordem := []string{}
	total := 0
	for _, f := range fontes {
		for _, c := range citacoesDoArquivo(t, f) {
			total++
			if alvos[c.numero] {
				continue
			}
			local := caminhoRelativo(t, f) + ":" + strconv.Itoa(c.linha)
			if _, visto := locais[c.numero]; !visto {
				ordem = append(ordem, c.numero)
			}
			locais[c.numero] = append(locais[c.numero], local)
		}
	}
	if total == 0 {
		t.Fatalf("nenhuma citação §NN encontrada em %d fontes: ou a leitura por AST/markdown quebrou, "+
			"ou o gate passaria por vacuidade", len(fontes))
	}

	sort.Slice(ordem, func(i, j int) bool { return menorCitacao(ordem[i], ordem[j]) })
	for _, num := range ordem {
		l := locais[num]
		sort.Strings(l)
		t.Errorf("§%s não resolve em nenhuma das %d specs declaradas, e é citado em %d lugar(es): %s\n"+
			"\tremédio: se a seção foi RENUMERADA, atualize as citações listadas; se foi REMOVIDA, a citação "+
			"é que está morta; se ela vive numa spec fora de specsDoMarcador, acrescente o documento à lista",
			num, len(specsDoMarcador), len(l), strings.Join(l, ", "))
	}
}

// menorCitacao ordena "14.2" antes de "14.10" e "9" antes de "14": a mensagem
// de um gate é lida por gente, e ordem lexicográfica de número não é ordem.
func menorCitacao(a, b string) bool {
	as, asub := partesDaCitacao(a)
	bs, bsub := partesDaCitacao(b)
	if as != bs {
		return as < bs
	}
	return asub < bsub
}

func partesDaCitacao(c string) (int, int) {
	sec, sub, _ := strings.Cut(c, ".")
	s, _ := strconv.Atoi(sec)
	m, _ := strconv.Atoi(sub)
	return s, m
}

// citacao é uma ocorrência de §NN/§NN.M com a linha em que apareceu.
type citacao struct {
	numero string
	linha  int
}

// citacoesDoArquivo extrai as citações de um arquivo lendo-o como DADO: AST
// (só comentários) para `.go`, markdown com as cercas puladas para `.md`.
func citacoesDoArquivo(t *testing.T, caminho string) []citacao {
	t.Helper()
	var out []citacao
	acha := func(texto string, linha int) {
		for _, m := range reCitacao.FindAllStringSubmatch(texto, -1) {
			out = append(out, citacao{numero: strings.TrimPrefix(m[0], "§"), linha: linha})
		}
	}

	if strings.HasSuffix(caminho, ".go") {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, caminho, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			// Arquivo que não parseia é problema do compilador, não deste gate.
			return nil
		}
		for _, grupo := range f.Comments {
			for _, c := range grupo.List {
				base := fset.Position(c.Slash).Line
				for i, linha := range strings.Split(c.Text, "\n") {
					acha(linha, base+i)
				}
			}
		}
		return out
	}

	conteudo, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	dentroDaCerca := false
	for i, linha := range strings.Split(string(conteudo), "\n") {
		if strings.HasPrefix(strings.TrimSpace(linha), "```") {
			dentroDaCerca = !dentroDaCerca
			continue
		}
		if dentroDaCerca {
			continue
		}
		acha(linha, i+1)
	}
	return out
}

// fontesDeCitacao lista os arquivos varridos: README, CLAUDE.md, docs
// (recursivo), planning e todo `.go` do repositório.
//
// O CLAUDE.md entra porque ele passou a ser versionado e é normativo — cita
// seções de spec e testes como prova das convenções que impõe, e ficar de fora
// dos gates o tornava a única prosa normativa do repositório sem compilador.
func fontesDeCitacao(t *testing.T) []string {
	t.Helper()
	var fontes []string
	fontes = append(fontes,
		filepath.Join(raizDoRepo, "README.md"),
		filepath.Join(raizDoRepo, "CLAUDE.md"),
	)
	err := filepath.WalkDir(filepath.Join(raizDoRepo, "docs"), func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(c, ".md") {
			fontes = append(fontes, c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	entradas, err := os.ReadDir(filepath.Join(raizDoRepo, "planning"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entradas {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			fontes = append(fontes, filepath.Join(raizDoRepo, "planning", e.Name()))
		}
	}
	err = filepath.WalkDir(raizDoRepo, func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// .claude guarda worktrees, que são cópias da árvore.
			if nome := d.Name(); nome == ".git" || nome == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(c, ".go") {
			fontes = append(fontes, c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fontes
}

// caminhoRelativo devolve o caminho a partir da raiz do repositório, que é o
// que o leitor da mensagem de erro consegue abrir.
func caminhoRelativo(t *testing.T, caminho string) string {
	t.Helper()
	rel, err := filepath.Rel(raizDoRepo, caminho)
	if err != nil {
		return caminho
	}
	return filepath.ToSlash(rel)
}

// secoesDasSpecs indexa toda seção e item numerado das specs declaradas em
// specsDoMarcador, e falha se alguma delas sumiu do disco.
func secoesDasSpecs(t *testing.T) map[string]bool {
	t.Helper()
	alvos := map[string]bool{}
	// itensDaSecao conta os ITENS de cada seção (marcador ou numerado). A faixa
	// aceita no nível 2 é 1..contagem, e não 1..maior número escrito: uma linha
	// que só PARECE item ("2026. foi o ano em que...") acrescentaria um item à
	// contagem, mas não pode alargar a faixa até 2026 e fazer o gate aceitar
	// qualquer coisa naquela seção.
	itensDaSecao := map[string]int{}

	for _, relativo := range specsDoMarcador {
		caminho := filepath.Join(raizDoRepo, filepath.FromSlash(relativo))
		conteudo, err := os.ReadFile(caminho)
		if err != nil {
			t.Fatalf("a spec declarada %s não pôde ser lida (%v): um rename silencioso ENCOLHE o oráculo "+
				"e some com os alvos sem ninguém ver", relativo, err)
		}
		secao := ""
		dentroDaCerca := false
		for _, linha := range strings.Split(string(conteudo), "\n") {
			// Cabeçalho e item dentro de bloco de código são exemplo, não
			// estrutura: contá-los inventaria alvo que ninguém pode citar.
			if strings.HasPrefix(strings.TrimSpace(linha), "```") {
				dentroDaCerca = !dentroDaCerca
				continue
			}
			if dentroDaCerca {
				continue
			}
			if reCabecalhoQualquer.MatchString(linha) {
				if m := reCabecalho.FindStringSubmatch(linha); m != nil {
					secao = m[1]
					alvos[secao] = true
				} else {
					// Divisor sem número ("## Core"): encerra a seção.
					secao = ""
				}
				continue
			}
			if secao == "" {
				continue
			}
			if m := reItemNumerado.FindStringSubmatch(linha); m != nil {
				// O número ESCRITO é alvo exato: §126.15 é o item 15 da lista do
				// §126 mesmo que a lista comece em 10 ou pule números.
				n, _ := strconv.Atoi(m[1])
				alvos[secao+"."+strconv.Itoa(n)] = true
				itensDaSecao[secao]++
				continue
			}
			if reItemMarcador.MatchString(linha) {
				itensDaSecao[secao]++
			}
		}
	}
	// A faixa 1..contagem é aceita inteira: ver o godoc do teste sobre o nível 2
	// ser faixa e não conjunto de posições.
	for secao, itens := range itensDaSecao {
		for i := 1; i <= itens; i++ {
			alvos[secao+"."+strconv.Itoa(i)] = true
		}
	}
	if len(alvos) == 0 {
		t.Fatal("nenhuma seção indexada: o gate passaria por vacuidade")
	}
	return alvos
}

// sufixosDeHistograma são os únicos sufixos que o formato de exposição admite
// entre a família e o nome da linha. Ver internal/metrics/metrics.go: o
// histograma emite `x_bucket`/`x_sum`/`x_count` sob o `# HELP`/`# TYPE` de `x`.
var sufixosDeHistograma = []string{"_bucket", "_sum", "_count"}

// TestAmostrasDeMetricaTemFamiliaENome confere que todo literal
// metrics.Amostra{...} em código de produção declara Familia e Nome por
// CONSTANTE (nunca string literal, nunca vazio) e que o Nome pertence à
// Familia.
//
// O defeito que ele fecha é concreto: as amostras do coletor Kafka nasciam sem
// Familia, e o `# HELP`/`# TYPE` saía com nome vazio — o Prometheus recusa o
// scrape inteiro, não só a linha. Nenhum teste acusava, porque o formato de
// exposição só é exercitado com amostras bem-formadas.
//
// A regra "não pode ser string literal" é a mesma doutrina de
// internal/metrics/nomes.go: nome de métrica digitado à mão em dois lugares
// diverge no dia em que um deles é corrigido, e o sintoma é um painel vazio.
//
// # AS DUAS FORMAS DO LITERAL, E POR QUE ISSO É O CENTRO DO GATE
//
// O reconhecedor precisa enxergar a forma ELIDIDA — os elementos de
// `[]metrics.Amostra{{...}, {...}}` são `CompositeLit` com `Type == nil`, e o
// tipo mora no ARRAY, não no elemento. Ela é a forma dominante do repositório
// (quase 40% dos literais), e a primeira versão deste gate era cega a ela: as
// quatro amostras do Kafka que o commit da doutrina acabara de consertar
// podiam regredir com o gate verde. Daí a guarda de vacuidade exigir as DUAS
// formas: um reconhecedor que só vê uma delas volta a ser carimbo.
//
// # FAMILIA E NOME TÊM DE COMBINAR
//
// Conferir só a presença deixa passar duas constantes válidas mas TROCADAS, o
// que publica uma série sob o cabeçalho de outra família. A regra não é
// `Nome == Familia`: o histograma usa `Nome = Familia + sufixo` de propósito.
// A regra é: Nome resolve para a MESMA expressão de Familia, ou para Familia
// mais um dos sufixos de sufixosDeHistograma.
func TestAmostrasDeMetricaTemFamiliaENome(t *testing.T) {
	raiz := filepath.Join(raizDoRepo, "internal")
	explicitos, elididos := 0, 0
	err := filepath.WalkDir(raiz, func(caminho string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(caminho, ".go") || strings.HasSuffix(caminho, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, caminho, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		pkg := f.Name.Name
		confere := func(cl *ast.CompositeLit, elidido bool) {
			if elidido {
				elididos++
			} else {
				explicitos++
			}
			confereAmostra(t, fset, caminhoRelativo(t, caminho), cl)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil {
				// Literal sem tipo é elidido: quem sabe o tipo dele é o pai, e
				// é o pai que o visita (percorreElididos). Visitá-lo aqui seria
				// julgá-lo sem saber se é uma Amostra.
				return true
			}
			if ehTipoAmostra(cl.Type, pkg) {
				confere(cl, false)
				return true
			}
			percorreElididos(cl, cl.Type, pkg, func(inner *ast.CompositeLit) { confere(inner, true) })
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("literais metrics.Amostra alcançados: %d explícitos + %d elididos = %d",
		explicitos, elididos, explicitos+elididos)
	if explicitos == 0 || elididos == 0 {
		t.Fatalf("o gate alcançou %d literais explícitos e %d elididos: com zero de qualquer das duas formas "+
			"ele vira carimbo. Antes de concluir que a forma sumiu do repositório, confira se não foi o "+
			"RECONHECEDOR que parou de enxergá-la — foi exatamente esse o defeito da primeira versão",
			explicitos, elididos)
	}
}

// confereAmostra aplica as três regras a um literal já reconhecido como
// Amostra: Familia e Nome presentes, nenhum dos dois string literal, e Nome
// pertencendo à Familia.
func confereAmostra(t *testing.T, fset *token.FileSet, caminho string, cl *ast.CompositeLit) {
	t.Helper()
	linha := fset.Position(cl.Pos()).Line
	valores := map[string]ast.Expr{}
	for _, nome := range []string{"Familia", "Nome"} {
		valor, presente := campoDoLiteral(cl, nome)
		if !presente {
			t.Errorf("%s:%d: Amostra sem o campo %s — o # HELP/# TYPE sai vazio",
				caminho, linha, nome)
			continue
		}
		if ehStringLiteral(valor) {
			t.Errorf("%s:%d: Amostra com %s como string literal — use a constante de internal/metrics/nomes.go",
				caminho, linha, nome)
			continue
		}
		valores[nome] = valor
	}
	familia, nome := valores["Familia"], valores["Nome"]
	if familia == nil || nome == nil {
		return
	}
	if nomeSegueAFamilia(fset, familia, nome) {
		return
	}
	t.Errorf("%s:%d: Amostra com Nome (%s) que não pertence à Familia (%s) — duas constantes válidas mas "+
		"TROCADAS publicam a série sob o cabeçalho de outra família; o Nome só pode ser a própria Familia "+
		"ou ela mais %s",
		caminho, linha, expressao(fset, nome), expressao(fset, familia), strings.Join(sufixosDeHistograma, "/"))
}

// nomeSegueAFamilia decide se o Nome pertence à Familia: ou é a MESMA
// expressão, ou é ela somada a um sufixo de histograma.
//
// A comparação é feita sobre a expressão IMPRESSA, e não sobre a constante
// resolvida, porque resolver exigiria carregar o pacote com go/types — caro
// demais para o modo de falha que importa, que é o rótulo trocado entre dois
// literais vizinhos.
func nomeSegueAFamilia(fset *token.FileSet, familia, nome ast.Expr) bool {
	if expressao(fset, familia) == expressao(fset, nome) {
		return true
	}
	soma, ok := nome.(*ast.BinaryExpr)
	if !ok || soma.Op != token.ADD {
		return false
	}
	if expressao(fset, soma.X) != expressao(fset, familia) {
		return false
	}
	lit, ok := soma.Y.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	sufixo, err := strconv.Unquote(lit.Value)
	if err != nil {
		return false
	}
	for _, s := range sufixosDeHistograma {
		if sufixo == s {
			return true
		}
	}
	return false
}

// expressao imprime uma expressão para comparação e para a mensagem de erro.
func expressao(fset *token.FileSet, e ast.Expr) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, e); err != nil {
		return "<ilegível>"
	}
	return b.String()
}

// percorreElididos entrega ao chamador todo literal de Amostra ELIDIDO dentro
// de um literal de slice, array ou mapa.
//
// Os elementos de `[]metrics.Amostra{{...}}` são CompositeLit com `Type == nil`:
// o tipo mora no `Elt` do ArrayType (ou no `Key`/`Value` do MapType). O
// aninhamento é seguido até o fim porque `map[string][]metrics.Amostra{...}`
// elide DUAS vezes.
func percorreElididos(cl *ast.CompositeLit, tipo ast.Expr, pkg string, achou func(*ast.CompositeLit)) {
	var tipoDaChave, tipoDoElemento ast.Expr
	switch t := tipo.(type) {
	case *ast.ArrayType:
		tipoDoElemento = t.Elt
	case *ast.MapType:
		tipoDaChave, tipoDoElemento = t.Key, t.Value
	default:
		return
	}
	trata := func(e, tp ast.Expr) {
		interno, ok := e.(*ast.CompositeLit)
		if !ok || interno.Type != nil || tp == nil {
			// Literal com tipo escrito é visitado pelo ast.Inspect do chamador;
			// julgá-lo aqui também o reportaria duas vezes.
			return
		}
		if ehTipoAmostra(tp, pkg) {
			achou(interno)
			return
		}
		percorreElididos(interno, tp, pkg, achou)
	}
	for _, el := range cl.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			trata(kv.Key, tipoDaChave)
			trata(kv.Value, tipoDoElemento)
			continue
		}
		trata(el, tipoDoElemento)
	}
}

// ehTipoAmostra reconhece a EXPRESSÃO DE TIPO metrics.Amostra, qualificada
// (metrics.Amostra) ou não (Amostra, dentro do pacote metrics), atravessando o
// ponteiro de `[]*metrics.Amostra`.
//
// `&metrics.Amostra{...}` não precisa de caso próprio: o `&` é um UnaryExpr em
// volta do CompositeLit, e o literal interno já traz o tipo escrito.
func ehTipoAmostra(e ast.Expr, pkg string) bool {
	switch t := e.(type) {
	case *ast.StarExpr:
		return ehTipoAmostra(t.X, pkg)
	case *ast.SelectorExpr:
		if t.Sel.Name != "Amostra" {
			return false
		}
		id, ok := t.X.(*ast.Ident)
		return ok && id.Name == "metrics"
	case *ast.Ident:
		return t.Name == "Amostra" && pkg == "metrics"
	}
	return false
}

// campoDoLiteral devolve o valor do campo nomeado num literal chave:valor.
func campoDoLiteral(cl *ast.CompositeLit, nome string) (ast.Expr, bool) {
	for _, el := range cl.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		id, ok := kv.Key.(*ast.Ident)
		if !ok || id.Name != nome {
			continue
		}
		return kv.Value, true
	}
	return nil, false
}

// ehStringLiteral reconhece uma string literal (o que a regra proíbe).
func ehStringLiteral(e ast.Expr) bool {
	bl, ok := e.(*ast.BasicLit)
	return ok && bl.Kind == token.STRING
}
