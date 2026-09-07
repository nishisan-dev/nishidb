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
// (`metrics.Sample{...}`) e era CEGA à forma elidida (`[]metrics.Sample{{...}}`),
// que é a dominante no repositório: 23 dos 58 literais passavam por baixo, e
// entre eles estavam justamente as quatro do coletor Kafka que aquele mesmo
// commit tinha acabado de consertar. A primeira versão do gate de citação lia
// `.go` como bytes crus, contra a doutrina de ler código como DADO. Ambos estão
// consertados abaixo, e ambos ganharam guarda de vacuidade: gate que não
// discrimina é pior que gate nenhum, porque dá falsa segurança.

// reCitation casa uma citação de spec: §NN ou §NN.M.
var reCitation = regexp.MustCompile(`§(\d+)(?:\.(\d+))?`)

// reHeading casa um cabeçalho numerado de spec: "# 14.", "## 1.7", "### 3.2".
var reHeading = regexp.MustCompile(`^#{1,4}\s+(\d+(?:\.\d+)?)\.?\s`)

// reAnyHeading casa QUALQUER cabeçalho (numerado ou não), para encerrar
// a seção corrente: um divisor como "## Core" não pode deixar os itens
// seguintes atribuídos à seção anterior.
var reAnyHeading = regexp.MustCompile(`^#{1,4}\s`)

// reNumberedItem casa um item de lista numerado ("1. iniciar...").
var reNumberedItem = regexp.MustCompile(`^(\d+)\.\s`)

// reBulletItem casa um item de lista com marcador ("* Consumir...").
var reBulletItem = regexp.MustCompile(`^\s*[*\-]\s`)

// markerSpecs é o conjunto FECHADO de documentos que definem o marcador
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
// que os cinco existem, porque um rename silencioso ENCOLHE o oráculo e
// transforma citação viva em órfã (ou, pior, some com o alvo sem ninguém ver).
var markerSpecs = []string{
	"nishidb-spec-proposal.md",
	"planning/m1-design-storage.md",
	"planning/m1-design-materializer-query.md",
	"planning/spec-subsistema-1-indice-por-segmento.md",
	// planning/identificadores-em-ingles.md: citada por scripts/ifacerename,
	// scripts/renameinv, scripts/renametools e scripts/renamecite (§4.1, §4.2,
	// §4.4, §5, §8).
	"planning/identificadores-em-ingles.md",
}

// TestCitacoesDeSecaoResolvem confere que toda citação §NN/§NN.M em docs,
// README, CLAUDE.md, planning e comentários Go resolve em pelo menos uma seção
// ou item numerado das specs declaradas em markerSpecs.
//
// # COMO A FONTE É LIDA
//
// Como DADO, nunca por grep em prosa — a mesma doutrina do resto do pacote.
// Arquivo `.go` é parseado com `parser.ParseComments` e só os COMENTÁRIOS são
// varridos (a técnica de `citations_test.go`): um `§` dentro de string literal,
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
	targets := specSections(t)
	sources := citationSources(t)

	// Um erro por número citado; os caminhos entram na mensagem.
	locations := map[string][]string{}
	order := []string{}
	total := 0
	for _, f := range sources {
		for _, c := range citationsInFile(t, f) {
			total++
			if targets[c.number] {
				continue
			}
			location := relativePath(t, f) + ":" + strconv.Itoa(c.line)
			if _, seen := locations[c.number]; !seen {
				order = append(order, c.number)
			}
			locations[c.number] = append(locations[c.number], location)
		}
	}
	if total == 0 {
		t.Fatalf("nenhuma citação §NN encontrada em %d fontes: ou a leitura por AST/markdown quebrou, "+
			"ou o gate passaria por vacuidade", len(sources))
	}

	sort.Slice(order, func(i, j int) bool { return citationLess(order[i], order[j]) })
	for _, num := range order {
		l := locations[num]
		sort.Strings(l)
		t.Errorf("§%s não resolve em nenhuma das %d specs declaradas, e é citado em %d lugar(es): %s\n"+
			"\tremédio: se a seção foi RENUMERADA, atualize as citações listadas; se foi REMOVIDA, a citação "+
			"é que está morta; se ela vive numa spec fora de markerSpecs, acrescente o documento à lista",
			num, len(markerSpecs), len(l), strings.Join(l, ", "))
	}
}

// citationLess ordena "14.2" antes de "14.10" e "9" antes de "14": a mensagem
// de um gate é lida por gente, e ordem lexicográfica de número não é ordem.
func citationLess(a, b string) bool {
	as, asub := citationParts(a)
	bs, bsub := citationParts(b)
	if as != bs {
		return as < bs
	}
	return asub < bsub
}

func citationParts(c string) (int, int) {
	sec, sub, _ := strings.Cut(c, ".")
	s, _ := strconv.Atoi(sec)
	m, _ := strconv.Atoi(sub)
	return s, m
}

// citation é uma ocorrência de §NN/§NN.M com a linha em que apareceu.
type citation struct {
	number string
	line   int
}

// citationsInFile extrai as citações de um arquivo lendo-o como DADO: AST
// (só comentários) para `.go`, markdown com as cercas puladas para `.md`.
func citationsInFile(t *testing.T, path string) []citation {
	t.Helper()
	var out []citation
	find := func(text string, line int) {
		for _, m := range reCitation.FindAllStringSubmatch(text, -1) {
			out = append(out, citation{number: strings.TrimPrefix(m[0], "§"), line: line})
		}
	}

	if strings.HasSuffix(path, ".go") {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			// Arquivo que não parseia é problema do compilador, não deste gate.
			return nil
		}
		for _, group := range f.Comments {
			for _, c := range group.List {
				base := fset.Position(c.Slash).Line
				for i, line := range strings.Split(c.Text, "\n") {
					find(line, base+i)
				}
			}
		}
		return out
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	inFence := false
	for i, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		find(line, i+1)
	}
	return out
}

// citationSources lista os arquivos varridos: README, CLAUDE.md, docs
// (recursivo), planning e todo `.go` do repositório.
//
// O CLAUDE.md entra porque ele passou a ser versionado e é normativo — cita
// seções de spec e testes como prova das convenções que impõe, e ficar de fora
// dos gates o tornava a única prosa normativa do repositório sem compilador.
func citationSources(t *testing.T) []string {
	t.Helper()
	var sources []string
	sources = append(sources,
		filepath.Join(repoRoot, "README.md"),
		filepath.Join(repoRoot, "CLAUDE.md"),
	)
	err := filepath.WalkDir(filepath.Join(repoRoot, "docs"), func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(c, ".md") {
			sources = append(sources, c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, "planning"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			sources = append(sources, filepath.Join(repoRoot, "planning", e.Name()))
		}
	}
	err = filepath.WalkDir(repoRoot, func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// .claude guarda worktrees, que são cópias da árvore.
			if name := d.Name(); name == ".git" || name == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(c, ".go") {
			sources = append(sources, c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sources
}

// relativePath devolve o caminho a partir da raiz do repositório, que é o
// que o leitor da mensagem de erro consegue abrir.
func relativePath(t *testing.T, path string) string {
	t.Helper()
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(rel)
}

// specSections indexa toda seção e item numerado das specs declaradas em
// markerSpecs, e falha se alguma delas sumiu do disco.
func specSections(t *testing.T) map[string]bool {
	t.Helper()
	targets := map[string]bool{}
	// sectionItemCount conta os ITENS de cada seção (marcador ou numerado). A faixa
	// aceita no nível 2 é 1..contagem, e não 1..maior número escrito: uma linha
	// que só PARECE item ("2026. foi o ano em que...") acrescentaria um item à
	// contagem, mas não pode alargar a faixa até 2026 e fazer o gate aceitar
	// qualquer coisa naquela seção.
	sectionItemCount := map[string]int{}

	for _, relative := range markerSpecs {
		path := filepath.Join(repoRoot, filepath.FromSlash(relative))
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("a spec declarada %s não pôde ser lida (%v): um rename silencioso ENCOLHE o oráculo "+
				"e some com os alvos sem ninguém ver", relative, err)
		}
		section := ""
		inFence := false
		for _, line := range strings.Split(string(content), "\n") {
			// Cabeçalho e item dentro de bloco de código são exemplo, não
			// estrutura: contá-los inventaria alvo que ninguém pode citar.
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				inFence = !inFence
				continue
			}
			if inFence {
				continue
			}
			if reAnyHeading.MatchString(line) {
				if m := reHeading.FindStringSubmatch(line); m != nil {
					section = m[1]
					targets[section] = true
				} else {
					// Divisor sem número ("## Core"): encerra a seção.
					section = ""
				}
				continue
			}
			if section == "" {
				continue
			}
			if m := reNumberedItem.FindStringSubmatch(line); m != nil {
				// O número ESCRITO é alvo exato: §126.15 é o item 15 da lista do
				// §126 mesmo que a lista comece em 10 ou pule números.
				n, _ := strconv.Atoi(m[1])
				targets[section+"."+strconv.Itoa(n)] = true
				sectionItemCount[section]++
				continue
			}
			if reBulletItem.MatchString(line) {
				sectionItemCount[section]++
			}
		}
	}
	// A faixa 1..contagem é aceita inteira: ver o godoc do teste sobre o nível 2
	// ser faixa e não conjunto de posições.
	for section, itemCount := range sectionItemCount {
		for i := 1; i <= itemCount; i++ {
			targets[section+"."+strconv.Itoa(i)] = true
		}
	}
	if len(targets) == 0 {
		t.Fatal("nenhuma seção indexada: o gate passaria por vacuidade")
	}
	return targets
}

// histogramSuffixes são os únicos sufixos que o formato de exposição admite
// entre a família e o nome da linha. Ver internal/metrics/metrics.go: o
// histograma emite `x_bucket`/`x_sum`/`x_count` sob o `# HELP`/`# TYPE` de `x`.
var histogramSuffixes = []string{"_bucket", "_sum", "_count"}

// TestAmostrasDeMetricaTemFamiliaENome confere que todo literal
// metrics.Sample{...} em código de produção declara Family e Name por
// CONSTANTE (nunca string literal, nunca vazio) e que o Name pertence à
// Family.
//
// O defeito que ele fecha é concreto: as amostras do coletor Kafka nasciam sem
// Family, e o `# HELP`/`# TYPE` saía com nome vazio — o Prometheus recusa o
// scrape inteiro, não só a linha. Nenhum teste acusava, porque o formato de
// exposição só é exercitado com amostras bem-formadas.
//
// A regra "não pode ser string literal" é a mesma doutrina de
// internal/metrics/names.go: nome de métrica digitado à mão em dois lugares
// diverge no dia em que um deles é corrigido, e o sintoma é um painel vazio.
//
// # AS DUAS FORMAS DO LITERAL, E POR QUE ISSO É O CENTRO DO GATE
//
// O reconhecedor precisa enxergar a forma ELIDIDA — os elementos de
// `[]metrics.Sample{{...}, {...}}` são `CompositeLit` com `Type == nil`, e o
// tipo mora no ARRAY, não no elemento. Ela é a forma dominante do repositório
// (quase 40% dos literais), e a primeira versão deste gate era cega a ela: as
// quatro amostras do Kafka que o commit da doutrina acabara de consertar
// podiam regredir com o gate verde. Daí a guarda de vacuidade exigir as DUAS
// formas: um reconhecedor que só vê uma delas volta a ser carimbo.
//
// # FAMILY E NAME TÊM DE COMBINAR
//
// Conferir só a presença deixa passar duas constantes válidas mas TROCADAS, o
// que publica uma série sob o cabeçalho de outra família. A regra não é
// `Name == Family`: o histograma usa `Name = Family + sufixo` de propósito.
// A regra é: Name resolve para a MESMA expressão de Family, ou para Family
// mais um dos sufixos de histogramSuffixes.
func TestAmostrasDeMetricaTemFamiliaENome(t *testing.T) {
	root := filepath.Join(repoRoot, "internal")
	explicitCount, elidedCount := 0, 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		pkg := f.Name.Name
		check := func(cl *ast.CompositeLit, elided bool) {
			if elided {
				elidedCount++
			} else {
				explicitCount++
			}
			checkSample(t, fset, relativePath(t, path), cl)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil {
				// Literal sem tipo é elidido: quem sabe o tipo dele é o pai, e
				// é o pai que o visita (walkElided). Visitá-lo aqui seria
				// julgá-lo sem saber se é uma Sample.
				return true
			}
			if isSampleType(cl.Type, pkg) {
				check(cl, false)
				return true
			}
			walkElided(cl, cl.Type, pkg, func(inner *ast.CompositeLit) { check(inner, true) })
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("literais metrics.Sample alcançados: %d explícitos + %d elididos = %d",
		explicitCount, elidedCount, explicitCount+elidedCount)
	if explicitCount == 0 || elidedCount == 0 {
		t.Fatalf("o gate alcançou %d literais explícitos e %d elididos: com zero de qualquer das duas formas "+
			"ele vira carimbo. Antes de concluir que a forma sumiu do repositório, confira se não foi o "+
			"RECONHECEDOR que parou de enxergá-la — foi exatamente esse o defeito da primeira versão",
			explicitCount, elidedCount)
	}
}

// checkSample aplica as três regras a um literal já reconhecido como
// Sample: Family e Name presentes, nenhum dos dois string literal, e Name
// pertencendo à Family.
func checkSample(t *testing.T, fset *token.FileSet, path string, cl *ast.CompositeLit) {
	t.Helper()
	line := fset.Position(cl.Pos()).Line
	values := map[string]ast.Expr{}
	for _, name := range []string{"Family", "Name"} {
		value, present := literalField(cl, name)
		if !present {
			t.Errorf("%s:%d: Sample sem o campo %s — o # HELP/# TYPE sai vazio",
				path, line, name)
			continue
		}
		if isStringLiteral(value) {
			t.Errorf("%s:%d: Sample com %s como string literal — use a constante de internal/metrics/names.go",
				path, line, name)
			continue
		}
		values[name] = value
	}
	family, name := values["Family"], values["Name"]
	if family == nil || name == nil {
		return
	}
	if nameFollowsFamily(fset, family, name) {
		return
	}
	t.Errorf("%s:%d: Sample com Name (%s) que não pertence à Family (%s) — duas constantes válidas mas "+
		"TROCADAS publicam a série sob o cabeçalho de outra família; o Name só pode ser a própria Family "+
		"ou ela mais %s",
		path, line, expression(fset, name), expression(fset, family), strings.Join(histogramSuffixes, "/"))
}

// nameFollowsFamily decide se o Name pertence à Family: ou é a MESMA
// expressão, ou é ela somada a um sufixo de histograma.
//
// A comparação é feita sobre a expressão IMPRESSA, e não sobre a constante
// resolvida, porque resolver exigiria carregar o pacote com go/types — caro
// demais para o modo de falha que importa, que é o rótulo trocado entre dois
// literais vizinhos.
func nameFollowsFamily(fset *token.FileSet, family, name ast.Expr) bool {
	if expression(fset, family) == expression(fset, name) {
		return true
	}
	concat, ok := name.(*ast.BinaryExpr)
	if !ok || concat.Op != token.ADD {
		return false
	}
	if expression(fset, concat.X) != expression(fset, family) {
		return false
	}
	lit, ok := concat.Y.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	suffix, err := strconv.Unquote(lit.Value)
	if err != nil {
		return false
	}
	for _, s := range histogramSuffixes {
		if suffix == s {
			return true
		}
	}
	return false
}

// expression imprime uma expressão para comparação e para a mensagem de erro.
func expression(fset *token.FileSet, e ast.Expr) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, e); err != nil {
		return "<ilegível>"
	}
	return b.String()
}

// walkElided entrega ao chamador todo literal de Sample ELIDIDO dentro
// de um literal de slice, array ou mapa.
//
// Os elementos de `[]metrics.Sample{{...}}` são CompositeLit com `Type == nil`:
// o tipo mora no `Elt` do ArrayType (ou no `Key`/`Value` do MapType). O
// aninhamento é seguido até o fim porque `map[string][]metrics.Sample{...}`
// elide DUAS vezes.
func walkElided(cl *ast.CompositeLit, typ ast.Expr, pkg string, onFound func(*ast.CompositeLit)) {
	var keyType, elemType ast.Expr
	switch t := typ.(type) {
	case *ast.ArrayType:
		elemType = t.Elt
	case *ast.MapType:
		keyType, elemType = t.Key, t.Value
	default:
		return
	}
	handle := func(e, tp ast.Expr) {
		inner, ok := e.(*ast.CompositeLit)
		if !ok || inner.Type != nil || tp == nil {
			// Literal com tipo escrito é visitado pelo ast.Inspect do chamador;
			// julgá-lo aqui também o reportaria duas vezes.
			return
		}
		if isSampleType(tp, pkg) {
			onFound(inner)
			return
		}
		walkElided(inner, tp, pkg, onFound)
	}
	for _, el := range cl.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			handle(kv.Key, keyType)
			handle(kv.Value, elemType)
			continue
		}
		handle(el, elemType)
	}
}

// isSampleType reconhece a EXPRESSÃO DE TIPO metrics.Sample, qualificada
// (metrics.Sample) ou não (Sample, dentro do pacote metrics), atravessando o
// ponteiro de `[]*metrics.Sample`.
//
// `&metrics.Sample{...}` não precisa de caso próprio: o `&` é um UnaryExpr em
// volta do CompositeLit, e o literal interno já traz o tipo escrito.
func isSampleType(e ast.Expr, pkg string) bool {
	switch t := e.(type) {
	case *ast.StarExpr:
		return isSampleType(t.X, pkg)
	case *ast.SelectorExpr:
		if t.Sel.Name != "Sample" {
			return false
		}
		id, ok := t.X.(*ast.Ident)
		return ok && id.Name == "metrics"
	case *ast.Ident:
		return t.Name == "Sample" && pkg == "metrics"
	}
	return false
}

// literalField devolve o valor do campo nomeado num literal chave:valor.
func literalField(cl *ast.CompositeLit, name string) (ast.Expr, bool) {
	for _, el := range cl.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		id, ok := kv.Key.(*ast.Ident)
		if !ok || id.Name != name {
			continue
		}
		return kv.Value, true
	}
	return nil, false
}

// isStringLiteral reconhece uma string literal (o que a regra proíbe).
func isStringLiteral(e ast.Expr) bool {
	bl, ok := e.(*ast.BasicLit)
	return ok && bl.Kind == token.STRING
}
