package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write escreve `content` em `rel` dentro de `dir`, criando os diretórios.
func write(t *testing.T, dir, rel, content string) string {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func read(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ofForTable é o conteúdo que a primeira redação destruía: uma tabela
// De → Para, cuja coluna da esquerda é, por construção, o nome VELHO — que é
// exatamente o que a ferramenta procura.
const ofForTable = "# De → Para\n" +
	"\n" +
	"| velho | novo |\n" +
	"|---|---|\n" +
	"| `consumer.subiu` | `consumer.up` |\n" +
	"\n" +
	"Ver TestFilaRecusaCheia.\n"

// TestExcludedFilesAreUntouchedAndAppliedSubstitutionsAreReported cobre o
// achado crítico: rodada de verdade, `docs/glossario.md` colapsava em
// `X → X` linha a linha, sem uma linha de relatório sequer, porque o
// relatório só listava as PULAS.
//
// Aqui a exclusão padrão (arquivo e diretório), a exclusão por -exclude e o
// relatório de tudo que foi aplicado são conferidos na mesma rodada.
func TestExcludedFilesAreUntouchedAndAppliedSubstitutionsAreReported(t *testing.T) {
	dir := t.TempDir()
	glossario := write(t, dir, "docs/glossario.md", ofForTable)
	planoDoc := write(t, dir, "planning/identificadores-em-ingles.md", ofForTable)
	planoDir := write(t, dir, "planning/identificadores-em-ingles/mapeamento-1.md", ofForTable)
	byFlag := write(t, dir, "notas/rascunho.md", ofForTable)
	// ferramentaFixture reproduz o achado da Tarefa 1.1: os testes do próprio
	// renamecite usam nomes reais de pacote como fixture de tabela De → Para
	// (ver godoc de defaultExcludes), e uma rodada real contra esses mesmos
	// nomes não pode colapsar o fixture em `X → X` como fez antes deste caso.
	ferramentaFixture := write(t, dir, "scripts/renamecite/rules_test.go", ofForTable)
	target := write(t, dir, "docs/outro.md", ofForTable)

	logkeys := write(t, dir, "mapa/logkeys.tsv", "velho\tnovo\nconsumer.subiu\tconsumer.up\n")
	tests := write(t, dir, "mapa/tests.tsv", "pacote\tvelho\tnovo\ninternal/kafka\tTestFilaRecusaCheia\tTestQueueRefusesWhenFull\n")
	report := filepath.Join(dir, "relatorio.txt")

	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", dir,
		"-logkeys", logkeys,
		"-tests", tests,
		"-exclude", "notas/*",
		"-exclude", "mapa",
		"-report", report,
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}

	for name, p := range map[string]string{
		"docs/glossario.md":                                                glossario,
		"planning/identificadores-em-ingles.md":                            planoDoc,
		"planning/identificadores-em-ingles/…":                             planoDir,
		"notas/rascunho.md (excluído pela -exclude)":                       byFlag,
		"scripts/renamecite/rules_test.go (fixture da própria ferramenta)": ferramentaFixture,
	} {
		if got := read(t, p); got != ofForTable {
			t.Errorf("%s foi reescrito:\n%s", name, got)
		}
	}

	want := strings.Replace(ofForTable, "`consumer.subiu`", "`consumer.up`", 1)
	want = strings.Replace(want, "TestFilaRecusaCheia", "TestQueueRefusesWhenFull", 1)
	if got := read(t, target); got != want {
		t.Errorf("docs/outro.md:\n%s\nquer:\n%s", got, want)
	}

	rel := read(t, report)
	for _, row := range []string{
		"aplicado\tdocs/outro.md:5\tconsumer.subiu\tconsumer.up\tcrase\n",
		"aplicado\tdocs/outro.md:7\tTestFilaRecusaCheia\tTestQueueRefusesWhenFull\tteste\n",
	} {
		if !strings.Contains(rel, row) {
			t.Errorf("relatório sem %q:\n%s", row, rel)
		}
	}
	for _, excluido := range []string{"glossario", "identificadores-em-ingles", "rascunho", "renamecite"} {
		if strings.Contains(rel, excluido) {
			t.Errorf("relatório cita arquivo excluído %q:\n%s", excluido, rel)
		}
	}
	if !strings.HasPrefix(out.String(), "1 arquivos reescritos") {
		t.Errorf("resumo: %q", out.String())
	}
}

// TestProtectedTokenIsKeptAndReported cobre a válvula do achado do
// `grupos.dict`: nome de arquivo em disco, coluna de tabela virtual e chave
// de YAML são texto entre crases que casa com identificador do lote sem ser
// citação de identificador. O -protect os tira da regra, e a pula aparece no
// relatório — silêncio aqui é o que criou o achado.
func TestProtectedTokenIsKeptAndReported(t *testing.T) {
	dir := t.TempDir()
	target := write(t, dir, "doc.md", "a coluna `linhas` e a função `Compila`\n")
	idents := write(t, dir, "mapa/idents.tsv",
		"pacote\tespecie\tendereco\tnovo\tmotivo\n"+
			"internal/storage\tfunc\tlinhas\trows\t\n"+
			"internal/contadores\tfunc\tCompila\tCompile\t\n")
	protect := write(t, dir, "mapa/protect.txt", "# nomes que não são citação de identificador\nlinhas\n")
	report := filepath.Join(dir, "relatorio.txt")

	var out, errb bytes.Buffer
	if code := run([]string{"-dir", dir, "-idents", idents, "-protect", protect, "-exclude", "mapa", "-report", report}, &out, &errb); code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, target), "a coluna `linhas` e a função `Compile`\n"; got != want {
		t.Fatalf("saída: %q, quer %q", got, want)
	}
	rel := read(t, report)
	if !strings.Contains(rel, "pulado\tdoc.md:1\tlinhas\tprotegido\n") {
		t.Errorf("relatório sem a pula do protegido:\n%s", rel)
	}
	if !strings.Contains(rel, "aplicado\tdoc.md:1\tCompila\tCompile\tcrase\n") {
		t.Errorf("relatório sem a aplicação:\n%s", rel)
	}
}

// TestBareUnexportedTokenIsStillReplaced é a contraparte do teste acima: a
// decisão registrada é que token nu não exportado CONTINUA sendo substituído,
// e quem guarda é o relatório, não uma exceção embutida.
func TestBareUnexportedTokenIsStillReplaced(t *testing.T) {
	dir := t.TempDir()
	target := write(t, dir, "doc.md", "a coluna `linhas`\n")
	idents := write(t, dir, "mapa/idents.tsv",
		"pacote\tespecie\tendereco\tnovo\tmotivo\ninternal/storage\tfunc\tlinhas\trows\t\n")
	report := filepath.Join(dir, "relatorio.txt")

	var out, errb bytes.Buffer
	if code := run([]string{"-dir", dir, "-idents", idents, "-exclude", "mapa", "-report", report}, &out, &errb); code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, target), "a coluna `rows`\n"; got != want {
		t.Fatalf("saída: %q, quer %q", got, want)
	}
	if rel := read(t, report); !strings.Contains(rel, "aplicado\tdoc.md:1\tlinhas\trows\tcrase\n") {
		t.Errorf("relatório sem a aplicação:\n%s", rel)
	}
}

// TestLoadMappingSynthesizesTheFullNewAddressForMethodsAndFields: a coluna
// `novo` do TSV é sempre a FOLHA (`Push`), e a primeira redação guardava a
// folha como endereço novo inteiro — `Store.Apenda` citado em prosa virava
// `Append`, perdendo o receptor. O fixture de bancada escondia o defeito
// porque escrevia `Queue.Push` na mão.
func TestLoadMappingSynthesizesTheFullNewAddressForMethodsAndFields(t *testing.T) {
	dir := t.TempDir()
	idents := write(t, dir, "idents.tsv",
		"pacote\tespecie\tendereco\tnovo\tmotivo\n"+
			"internal/kafka\ttype\tFila\tQueue\t\n"+
			"internal/kafka\tmethod\tFila.Empurra\tPush\t\n"+
			"internal/kafka\tfield\tFila.tamanho\tsize\t\n"+
			"internal/storage\tmethod\tStore.Apenda\tAppend\t\n")

	m, err := loadMapping([]string{idents}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	src := "`Fila` e `Fila.Empurra` e `Fila.tamanho` e `Store.Apenda`"
	want := "`Queue` e `Queue.Push` e `Queue.size` e `Store.Append`"
	out, _ := RewriteText([]byte(src), ".md", m)
	if string(out) != want {
		t.Fatalf("saída: %q\nquer:  %q", out, want)
	}
}

// TestDottedTokenOnlyFallsBackWhenTheFirstSegmentIsAType: o atalho "só o tipo
// mudou" (`T.membro` com só `T` no mapeamento) disparava para QUALQUER
// espécie, e por isso `grupos.dict` — nome de arquivo em disco — virava
// `groups.dict` só porque existe uma variável chamada `grupos`.
func TestDottedTokenOnlyFallsBackWhenTheFirstSegmentIsAType(t *testing.T) {
	withVar := NewMapping()
	withVar.addIdentRow("internal/storage", "var", "grupos", "groups")
	out, _ := RewriteText([]byte("o arquivo `grupos.dict`"), ".md", withVar)
	if got, want := string(out), "o arquivo `grupos.dict`"; got != want {
		t.Errorf("com `grupos` como var: %q, quer %q", got, want)
	}

	withTipo := NewMapping()
	withTipo.addIdentRow("internal/storage", "type", "grupos", "groups")
	out, _ = RewriteText([]byte("o campo `grupos.dict`"), ".md", withTipo)
	if got, want := string(out), "o campo `groups.dict`"; got != want {
		t.Errorf("com `grupos` como type: %q, quer %q", got, want)
	}
}

// TestPackageMapCompletesTheQualifiedCitation é a razão de o workflow passar
// `-paths` em TODA passada de identificadores, e não só na passada de
// caminhos: o pacote foi renomeado na Fase 1 e os símbolos dele na Fase 2, de
// modo que a citação `orcamento.NovoGlobal` tem as DUAS metades velhas. Sem o
// mapa de pacotes carregado, o qualificador não resolve e a citação inteira
// fica para trás — foi assim que o godoc de NewStreamWriter continuou mandando
// o chamador escrever `orcamento.NovoGlobal(0).Query(0)`.
//
// A segunda metade do teste é a contraparte, e existe para o motivo ficar
// verificável em vez de escrito: com o MESMO documento e o MESMO mapa de
// identificadores, sem `-paths`, nada muda.
func TestPackageMapCompletesTheQualifiedCitation(t *testing.T) {
	const doc = "package x\n\n// Ver orcamento.NovoGlobal(0).\nfunc F() {}\n"
	const idents = "pacote\tespecie\tendereco\tnovo\tmotivo\n" +
		"internal/query/budget\tfunc\tNovoGlobal\tNewGlobal\t\n"
	const paths = "velho\tnovo\ninternal/query/orcamento\tinternal/query/budget\n"

	withMapa := t.TempDir()
	target := write(t, withMapa, "doc.go", doc)
	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", withMapa,
		"-idents", write(t, withMapa, "mapa/idents.tsv", idents),
		"-paths", write(t, withMapa, "mapa/pacotes.tsv", paths),
		"-exclude", "mapa",
		"-report", filepath.Join(withMapa, "relatorio.txt"),
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, target), "package x\n\n// Ver budget.NewGlobal(0).\nfunc F() {}\n"; got != want {
		t.Errorf("com -paths: %q\nquer:       %q", got, want)
	}

	withoutMapa := t.TempDir()
	target = write(t, withoutMapa, "doc.go", doc)
	out.Reset()
	errb.Reset()
	code = run([]string{
		"-dir", withoutMapa,
		"-idents", write(t, withoutMapa, "mapa/idents.tsv", idents),
		"-exclude", "mapa",
		"-report", filepath.Join(withoutMapa, "relatorio.txt"),
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got := read(t, target); got != doc {
		t.Errorf("sem -paths a citação foi tocada pela metade: %q", got)
	}
}

// TestDirectoryNameIsAPackageNotAReceiver: `storage.Modo` é `pkg.Ident` de um
// pacote que o lote nem está tocando, e sem saber que `storage` é pacote a
// regra do receptor lia o `Modo` como folha de `Manifest.Modo` — de OUTRO
// pacote — e reescrevia. Foi o que uma rodada de verdade fez com
// planning/historico-append-only-plano.md, onde `storage.Modo` continua sendo
// o nome do tipo em internal/storage.
//
// O oráculo é a árvore: todo diretório com arquivo Go é um pacote. O
// mapeamento sozinho não daria a resposta — ele só conhece os pacotes do lote.
func TestDirectoryNameIsAPackageNotAReceiver(t *testing.T) {
	dir := t.TempDir()
	// O arquivo NÃO declara `Modo`: quem tem de segurar a troca aqui é a lista
	// de pacotes, e não o oráculo da declaração viva. Com `type Modo uint8` no
	// fixture os dois seguravam, e a mutação da lista de pacotes passava.
	write(t, dir, "internal/storage/store.go", "package storage\n\ntype Options struct{}\n")
	doc := write(t, dir, "docs/plano.md", "as Options ganham um campo do tipo `storage.Modo`\n")
	idents := write(t, dir, "mapa/idents.tsv",
		"pacote\tespecie\tendereco\tnovo\tmotivo\n"+
			"internal/storage/checkpoint\tfield\tManifest.Modo\tMode\t\n")

	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", dir,
		"-idents", idents,
		"-exclude", "mapa",
		"-report", filepath.Join(dir, "relatorio.txt"),
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, doc), "as Options ganham um campo do tipo `storage.Modo`\n"; got != want {
		t.Fatalf("saída: %q, quer %q", got, want)
	}
}

// TestLiveDeclarationBlocksTheLeafRewriteAndIsReported é o oráculo da
// DECLARAÇÃO VIVA (R28): folha que ainda é declaração de alguém no módulo não
// é citação pendurada, e trocá-la pelo nome novo de um homônimo renomeado em
// OUTRO pacote cria a citação pendurada que esta ferramenta existe para não
// deixar para trás.
//
// O par positivo/negativo é a mesma citação, a mesma linha de mapeamento e o
// mesmo documento — muda só se a declaração existe na árvore. Antes deste
// oráculo, a única defesa era despejar o token na lista de protegidos, que
// cega a ferramenta E o gate.
func TestLiveDeclarationBlocksTheLeafRewriteAndIsReported(t *testing.T) {
	const doc = "o campo `Nome` do coletor\n"
	const idents = "pacote\tespecie\tendereco\tnovo\tmotivo\n" +
		"internal/storage/checkpoint\tfield\tContadorInfo.Nome\tName\t\n"

	withDeclaracao := t.TempDir()
	write(t, withDeclaracao, "internal/metrics/metrics.go", "package metrics\n\ntype Amostra struct {\n\tNome string\n}\n")
	target := write(t, withDeclaracao, "docs/doc.md", doc)
	relatorio := filepath.Join(withDeclaracao, "relatorio.txt")
	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", withDeclaracao,
		"-idents", write(t, withDeclaracao, "mapa/idents.tsv", idents),
		"-exclude", "mapa",
		"-report", relatorio,
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got := read(t, target); got != doc {
		t.Errorf("com a declaração viva: %q, quer %q", got, doc)
	}
	if rel := read(t, relatorio); !strings.Contains(rel, "pulado\tdocs/doc.md:1\tNome\tainda declarado internal/metrics.Nome\n") {
		t.Errorf("relatório sem a pula do oráculo:\n%s", rel)
	}

	// A árvore sem a declaração tem um fixture de testdata que declara o MESMO
	// nome: o go tool ignora essas árvores, então o que está lá não é
	// declaração do módulo e não pode bloquear o rename.
	withoutDeclaracao := t.TempDir()
	write(t, withoutDeclaracao, "scripts/ferramenta/testdata/modulo/a.go", "package a\n\ntype Amostra struct {\n\tNome string\n}\n")
	target = write(t, withoutDeclaracao, "docs/doc.md", doc)
	out.Reset()
	errb.Reset()
	code = run([]string{
		"-dir", withoutDeclaracao,
		"-idents", write(t, withoutDeclaracao, "mapa/idents.tsv", idents),
		"-exclude", "mapa",
		"-report", filepath.Join(withoutDeclaracao, "relatorio.txt"),
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, target), "o campo `Name` do coletor\n"; got != want {
		t.Errorf("sem a declaração: %q, quer %q", got, want)
	}
}

// TestQualifiedAddressRewritesDespiteALiveHomonym: o oráculo vale para a folha
// NUA. `Manifest.Contadores` diz de quem está falando, e o tipo do endereço é o
// desempate que a folha não tem — a citação qualificada continua sendo
// reescrita mesmo com `Store.Contadores` vivo em outro pacote.
func TestQualifiedAddressRewritesDespiteALiveHomonym(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "internal/storage/store.go", "package storage\n\ntype Store struct{}\n\nfunc (s *Store) Contadores() int { return 0 }\n")
	target := write(t, dir, "docs/doc.md", "o `Manifest.Contadores` e o `Contadores` solto\n")
	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", dir,
		"-idents", write(t, dir, "mapa/idents.tsv",
			"pacote\tespecie\tendereco\tnovo\tmotivo\n"+
				"internal/storage/checkpoint\tfield\tManifest.Contadores\tCounters\t\n"),
		"-exclude", "mapa",
		"-report", filepath.Join(dir, "relatorio.txt"),
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got, want := read(t, target), "o `Manifest.Counters` e o `Contadores` solto\n"; got != want {
		t.Fatalf("saída: %q\nquer:  %q", got, want)
	}
}

// TestInterfaceGroupsTSVReachesTheCitations é o furo estrutural que a revisão
// do G3 encontrou: método de interface é renomeado pelo `scripts/ifacerename`,
// que lê um TSV de OUTRO formato (`pacote interface metodo novo`), e enquanto
// esta passada só lia os `mapeamento-*.tsv` as citações desses métodos não
// tinham ferramenta nenhuma. `AbreSnapshot`, `FechaSnapshot`, `Indexado` e
// `Estado` sobreviveram assim em godoc, diagrama e documento.
//
// As duas metades do caso são as duas formas em que a prosa cita um método: a
// folha nua (`AbreSnapshot`, alcançada por ter maiúscula interna) e o endereço
// qualificado pelo dono (`Store.FechaSnapshot`), que só resolve com o endereço
// SINTETIZADO `Interface.metodo`.
//
// A contraparte sem `-ifaces` é o que prova que é o flag, e não outra regra,
// que alcança as duas: com o MESMO documento, nada muda.
func TestInterfaceGroupsTSVReachesTheCitations(t *testing.T) {
	const doc = "package x\n\n// A leitura chama AbreSnapshot e devolve por Store.FechaSnapshot.\nfunc F() {}\n"
	const ifaces = "pacote\tinterface\tmetodo\tnovo\n" +
		"internal/query/executor\tStore\tAbreSnapshot\tOpenSnapshot\n" +
		"internal/query/executor\tStore\tFechaSnapshot\tCloseSnapshot\n"

	with := t.TempDir()
	target := write(t, with, "doc.go", doc)
	report := filepath.Join(with, "relatorio.txt")
	var out, errb bytes.Buffer
	code := run([]string{
		"-dir", with,
		"-ifaces", write(t, with, "mapa/ifaces.tsv", ifaces),
		"-exclude", "mapa",
		"-report", report,
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	want := "package x\n\n// A leitura chama OpenSnapshot e devolve por Store.CloseSnapshot.\nfunc F() {}\n"
	if got := read(t, target); got != want {
		t.Fatalf("com -ifaces: %q\nquer:       %q", got, want)
	}
	rel := read(t, report)
	for _, row := range []string{
		"aplicado\tdoc.go:3\tAbreSnapshot\tOpenSnapshot\tnu\n",
		"aplicado\tdoc.go:3\tStore.FechaSnapshot\tStore.CloseSnapshot\tqualificado\n",
	} {
		if !strings.Contains(rel, row) {
			t.Errorf("relatório sem %q:\n%s", row, rel)
		}
	}

	without := t.TempDir()
	target = write(t, without, "doc.go", doc)
	out.Reset()
	errb.Reset()
	code = run([]string{"-dir", without, "-report", filepath.Join(without, "relatorio.txt")}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, errb.String())
	}
	if got := read(t, target); got != doc {
		t.Errorf("sem -ifaces o documento mudou, e a regra alcançada não seria a do flag: %q", got)
	}
}
