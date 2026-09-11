package storage

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
)

// states roda uma passada de pendência e devolve o retrato que ela publicou.
//
// Ele passa pelo SegmentosPendentesOfRebuild de propósito, e não lê os
// atômicos direto: o que estes testes provam é que o retrato sai da mesma conta
// que decide a fila. Um oráculo que escrevesse os atômicos na mão provaria
// apenas que atomic.Store funciona.
func states(t *testing.T, s *Store, j WindowOfIndex) StatesOfSegment {
	t.Helper()
	pendentes(t, s, j)
	return s.EstatisticasOfIndex().States
}

// totalOfSegmentos conta os .dat do dataset, por fora do Store. É o denominador
// da partição, e derivá-lo do próprio código dos estados faria o teste concordar
// com a implementação por construção.
func totalOfSegmentos(t *testing.T, s *Store) int64 {
	t.Helper()
	refs, err := s.listaSegmentos()
	if err != nil {
		t.Fatalf("listaSegmentos: %v", err)
	}
	return int64(len(refs))
}

// diskOfIndicesOfSegment é o `du` do operador sobre os arquivos de índice que
// moram em data/, medido por FORA do Store.
//
// A lista de extensões é escrita AQUI à mão, e essa duplicação é deliberada:
// perguntar ao segindex.IsIndexFile — o predicado que a produção usa —
// faria o oráculo concordar com a implementação por construção, e a pergunta em
// julgamento é exatamente "quais arquivos entram na conta?". É o mesmo argumento
// que faz o oráculo de storage_bytes somar os .dat por fora em vez de perguntar
// ao Store.
func diskOfIndicesOfSegment(t *testing.T, s *Store) int64 {
	t.Helper()
	entradas, err := os.ReadDir(s.dirOfData())
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", s.dirOfData(), err)
	}
	var total int64
	for _, e := range entradas {
		name := e.Name()
		if !strings.HasSuffix(name, ".sidx") && !strings.HasSuffix(name, ".widx") && !strings.HasSuffix(name, ".tmp") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // sumiu entre o readdir e o stat: é disco que já voltou
		}
		total += blocosOf(info)
	}
	return total
}

// TestOsQuatroEstadosParticionamOsSegmentos é o oráculo da correção que separou
// `fora` de `pendente`, e ele existe porque a conflação NÃO produz resposta
// errada nenhuma — só um painel mudo.
//
// Um rótulo só para as duas ausências somaria, num dataset grande, a maioria
// esmagadora dos segmentos (os que a janela deliberadamente não cobre) com a
// fila de trabalho. O número seria enorme, normal e constante, e o operador não
// teria como responder "o laço está avançando?" — que é a única pergunta que uma
// migração faz. Nenhum teste de corretude pega isso: a consulta responde igual
// nos dois desenhos.
//
// A soma dos quatro contra o total de .dat é o que impede o defeito simétrico:
// um segmento que não caia em balde nenhum some do painel sem deixar rastro.
func TestOsQuatroEstadosParticionamOsSegmentos(t *testing.T) {
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)

	estreita := WindowOfIndex{Duration: 5 * time.Minute}
	if aplicaWindow(t, hist, estreita) == 0 {
		t.Fatal("montagem: estreitar a janela não expulsou nenhum índice, e sem expulsão não há estado `fora` a distinguir")
	}

	e := states(t, hist, estreita)
	total := totalOfSegmentos(t, hist)
	if soma := e.Sealed + e.Current + e.Pendente + e.Fora; soma != total {
		t.Errorf("os quatro estados somam %d e o dataset tem %d segmento(s) em disco (%+v): "+
			"segmento que não cai em balde nenhum some do painel sem rastro", soma, total, e)
	}
	if e.Current != 1 {
		t.Errorf("estado corrente = %d, quer 1: o segmento que ainda recebe records é exatamente um, "+
			"e o índice dele é o WIP", e.Current)
	}
	if e.Fora == 0 {
		t.Fatalf("estado fora = 0 depois de a janela expulsar índices (%+v): a expulsão aconteceu e o painel não a vê", e)
	}
	if e.Pendente != 0 {
		t.Errorf("estado pendente = %d com a janela estreita (%+v): o que a janela acabou de expulsar NÃO é trabalho "+
			"por fazer — contá-lo como fila faria o operador ver uma fila que nunca esvazia e concluir que o laço travou",
			e.Pendente, e)
	}
}

// TestOsEstadosMigramDePendenteParaSeladoAoReconstruir é a outra metade do par, e
// sem ela a primeira não prova nada: publicar `pendente` fixo em zero satisfaz o
// teste acima inteiro.
//
// É esta a leitura que a migração do piloto vai fazer minuto a minuto — a fila
// caindo e o selado subindo na mesma medida —, e o "na mesma medida" é o que
// separa um laço que trabalha de um laço que apenas apaga a própria fila.
func TestOsEstadosMigramDePendenteParaSeladoAoReconstruir(t *testing.T) {
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)

	estreita := WindowOfIndex{Duration: 5 * time.Minute}
	larga := WindowOfIndex{Duration: 24 * time.Hour}
	if aplicaWindow(t, hist, estreita) == 0 {
		t.Fatal("montagem: estreitar a janela não expulsou nenhum índice")
	}

	before := states(t, hist, larga)
	if before.Pendente == 0 {
		t.Fatalf("montagem: alargar a janela não pôs ninguém na fila (%+v); sem pendência não há migração a medir", before)
	}
	fila := pendentes(t, hist, larga)
	for _, sec := range fila {
		if err := hist.ConstroiIndexOfSegment(t.Context(), sec, hist.FieldsIndexados()); err != nil {
			t.Fatalf("ConstroiIndexOfSegment(%d): %v", sec, err)
		}
	}

	depois := states(t, hist, larga)
	if depois.Pendente != 0 {
		t.Errorf("pendente = %d depois de reconstruir a fila inteira, quer 0 (%+v)", depois.Pendente, depois)
	}
	if ganho := depois.Sealed - before.Sealed; ganho != before.Pendente {
		t.Errorf("selado subiu %d e a fila tinha %d: o que sai de `pendente` tem de entrar em `selado`, senão o painel "+
			"mostra trabalho sumindo sem virar índice (antes=%+v, depois=%+v)", ganho, before.Pendente, before, depois)
	}
	if soma := depois.Sealed + depois.Current + depois.Pendente + depois.Fora; soma != totalOfSegmentos(t, hist) {
		t.Errorf("a partição furou depois da reconstrução: %+v soma %d", depois, soma)
	}
}

// TestOMedidorDaReconstrucaoNaoEhOContadorDeCorrentes separa o único par de
// números do bloco `index` que a bancada do servidor não consegue distinguir.
//
// Lá, `Current` é estruturalmente 1 — só existe um segmento corrente — e
// `RebuildRunning` também é 1, porque o servidor sobe o laço. Dois números iguais
// tornam invisível trocar um pelo outro, e a guarda de distinção do
// exigeDezNumerosDistintos declara essa exceção apontando para cá.
//
// Aqui os dois se separam porque este Store tem segmento corrente e NÃO tem laço:
// o histIndexed abre o dataset sem chamar StartRebuild. É o estado de
// qualquer dataset aberto pelo CLI, e nele um medidor que respondesse 1 sempre —
// ou que ecoasse a contagem de correntes — estaria dizendo que a reconstrução
// está de pé num processo onde ela nunca subiu.
func TestOMedidorDaReconstrucaoNaoEhOContadorDeCorrentes(t *testing.T) {
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)

	e := states(t, hist, WindowOfIndex{})
	if e.Current != 1 {
		t.Fatalf("montagem: corrente = %d, quer 1 (%+v)", e.Current, e)
	}
	if est := hist.EstatisticasOfIndex(); est.RebuildActive {
		t.Errorf("reconstrucao_ativa = true num Store cujo laço nunca subiu, com corrente = %d: "+
			"o medidor está ecoando outra coisa, e no servidor os dois valem 1 — lá a troca seria invisível",
			e.Current)
	}
	// E a outra ponta do par: com o laço de pé, ele sobe.
	hist.StartRebuild(t.Context(), WindowOfIndex{})
	defer hist.ForRebuild()
	if est := hist.EstatisticasOfIndex(); !est.RebuildActive {
		t.Error("reconstrucao_ativa = false logo depois do StartRebuild: o §92 manda o medidor subir ANTES " +
			"da goroutine, e sem esta metade um medidor preso em false passaria pela primeira")
	}
}

// TestOsSegmentosConsultadosSeparamIndiceDeEnumeracao é o oráculo da §3.4, e ele
// é o ÚNICO observável daquela seção: o resultado da consulta é idêntico pelos
// dois caminhos — essa é a tese —, e só o contador distingue um dataset inteiro
// indexado de um dataset inteiro enumerando.
//
// A bancada tem os dois estados no mesmo Lookup, e é isso que faz a asserção
// discriminar: com todos os segmentos indexados, "não contar enumeração" e
// "contar certo" dão o mesmo número.
func TestOsSegmentosConsultadosSeparamIndiceDeEnumeracao(t *testing.T) {
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)
	field := fieldOfDicionario(t, hist, "status")

	// A janela é PARCIAL de propósito: com 40 records de um em um minuto, vinte
	// minutos deixam os segmentos novos indexados e expulsam os velhos. Uma
	// janela que expulsasse tudo daria um Lookup só de enumeração, e aí "não
	// contar índice" passaria.
	parcial := WindowOfIndex{Duration: 20 * time.Minute}
	if aplicaWindow(t, hist, parcial) == 0 {
		t.Fatal("montagem: sem expulsão todos os segmentos responderiam por índice e o par não se separaria")
	}
	withIndex := int64(len(segmentosWithIndex(t, hist)))
	total := totalOfSegmentos(t, hist)
	if withIndex == 0 || withIndex == total {
		t.Fatalf("montagem: %d de %d segmentos com índice; o teste precisa dos DOIS caminhos no mesmo Lookup",
			withIndex, total)
	}

	before := hist.EstatisticasOfIndex()
	candidatosCrus(t, hist, field, strVal("a"))
	depois := hist.EstatisticasOfIndex()

	byIndice := depois.ByIndice - before.ByIndice
	byEnum := depois.ByEnumeracao - before.ByEnumeracao
	if byIndice+byEnum != total {
		t.Errorf("um Lookup contou %d por índice + %d por enumeração = %d, e o dataset tem %d segmentos: "+
			"cada segmento responde por exatamente um caminho", byIndice, byEnum, byIndice+byEnum, total)
	}
	// O corrente responde pelo WIP, que conta como índice — daí o +1.
	if byIndice != withIndex+1 {
		t.Errorf("modo=indice contou %d, e há %d selados com .sidx mais o corrente pelo WIP: "+
			"o contador não está seguindo a decisão real do fontesOfField", byIndice, withIndex)
	}
	if byEnum != total-withIndex-1 {
		t.Errorf("modo=enumeracao contou %d, e %d segmentos estão sem índice: um dataset degradado que reporta "+
			"zero enumerações é indistinguível de um dataset indexado", byEnum, total-withIndex-1)
	}
}

// TestOCrcDivergenteContaUmDegrauEEnumeraSempre liga o contador ao efeito e fixa
// a SEMÂNTICA dele, que é onde a prosa deste ciclo errou.
//
// Três afirmações, e nenhuma basta sozinha: contar sem degradar seria uma consulta
// derrubada por índice podre; degradar sem contar é o estado anterior a esta
// tarefa, em que o dataset responde certo e devagar para sempre sem ninguém
// saber; e contar UMA VEZ enquanto a enumeração persiste é o que separa o alarme
// que se auto-resolve do sinal que continua verdadeiro.
func TestOCrcDivergenteContaUmDegrauEEnumeraSempre(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
	target := s.curSecID.Load()
	poeRowsUntilRotacionar(t, s, target)
	poeRowsUntilRotacionar(t, s, s.curSecID.Load())
	field := fieldOfDicionario(t, s, "node")

	// A corrupção acontece ANTES de o arquivo ser aberto, que é o path real:
	// um .sidx que já estava podre em disco quando a consulta chegou.
	path := segindex.SealedPath(filepath.Join(dir, "data"), target)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xAA, 0xBB, 0xCC, 0xDD}, 64); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// TRÊS consultas, e não uma: é a segunda e a terceira que fixam a SEMÂNTICA
	// da série, e uma medição de consulta única não distingue "conta por abertura"
	// de "conta por consulta".
	before := s.EstatisticasOfIndex()
	var deltasCrc, deltasEnum []int64
	for range 3 {
		crus := candidatosCrus(t, s, field, strVal("nunca-visto"))
		if len(crus) == 0 {
			t.Fatal("o segmento corrompido não virou candidato: ele tem de DEGRADAR para enumeração, não responder " +
				"zero linhas — falso negativo é a única coisa que a revalidação do §44 não sabe consertar")
		}
		depois := s.EstatisticasOfIndex()
		deltasCrc = append(deltasCrc, depois.CRCFalhas-before.CRCFalhas)
		deltasEnum = append(deltasEnum, depois.ByEnumeracao-before.ByEnumeracao)
		before = depois
	}

	// O contador é um DEGRAU, e a razão é o cache: o garanteSealed publica o .sidx
	// reprovado no s.selados (publicar mantém o sync.Once do crc cumprido, e
	// recusar obrigaria toda consulta a remapear e reconferir o arquivo podre
	// inteiro). Da segunda consulta em diante o sourceIndexada acha o mapeamento no
	// cache e volta antes de reconferir nada, então a série NÃO sobe.
	//
	// Isto está aqui como oráculo, e não como lamento: a prosa das quatro
	// superfícies desta série — inclusive a Ajuda emitida no scrape — já afirmou o
	// contrário ("uma vez por abertura, a falha não é memorizada"), mandando o
	// operador ler uma taxa que na prática dá um degrau e se auto-resolve. O
	// conserto foi alinhar o texto ao código, e este teste é o que impede a
	// afirmação de voltar.
	if quer := []int64{1, 0, 0}; !slices.Equal(deltasCrc, quer) {
		t.Errorf("crc_falhas subiu %v em três consultas ao MESMO segmento podre, queria %v: "+
			"se ela subisse a cada consulta seria taxa, e a Ajuda poderia mandar ler rate(); "+
			"como é degrau, quem responde \"há índice podre agora\" é a enumeração persistente", deltasCrc, quer)
	}
	// E a outra metade, que é o sinal que o operador de fato tem: a enumeração
	// sobe a CADA consulta, porque o segmento continua respondendo sem índice.
	for i, d := range deltasEnum {
		if d == 0 {
			t.Errorf("a consulta %d não contou enumeração nenhuma (%v): o segmento reprovado no crc responde por "+
				"enumeração em TODA consulta, e é essa persistência — não o degrau do contador de crc — que denuncia "+
				"um índice podre que ninguém consertou", i+1, deltasEnum)
		}
	}
}

// TestOsBytesDeIndiceContamExatamenteOsArquivosDeIndice fixa QUAIS ARQUIVOS a
// série soma, que é a única pergunta que este pacote consegue responder sobre
// ela.
//
// O teste já se chamou "...SaoParcelaEstritaDoIndexBytes" e o nome prometia
// demais: a contenção contra o history_index_bytes é trivial dentro de um Store
// só — aquele número é este mais indexes/ e checkpoint/, por construção, e
// nenhuma mutação plausível a quebra. Pior, ela escondia o que de fato importa,
// que é o CRUZAMENTO DE RÓTULOS: no servidor o history_index_bytes sai no rótulo
// do dataset BASE com os números da IRMÃ, e a comparação entre as duas séries é
// entre rótulos diferentes. Quem prova isso é
// TestOsBytesDeIndiceVivemNoRotuloDaIrma, no pacote server, que é onde o rótulo
// existe.
//
// O que sobra aqui, e vale: o conjunto de arquivos contado.
func TestOsBytesDeIndiceContamExatamenteOsArquivosDeIndice(t *testing.T) {
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)

	idx := hist.EstatisticasOfIndex().Bytes
	if idx <= 0 {
		t.Fatalf("nishidb_index_bytes = %d num dataset com .sidx e .widx em disco", idx)
	}
	// O oráculo é um `du` do operador sobre data/, feito POR FORA do Store e sem
	// perguntar ao predicado que está sendo testado. Comparar contra o
	// history_index_bytes não bastava: com o filtro removido, a série passava a
	// somar os .dat inteiros e CONTINUAVA menor que aquele número — a asserção
	// aprovava a mutação. O que discrimina é o conjunto exato de arquivos.
	if quer := diskOfIndicesOfSegment(t, hist); idx != quer {
		t.Errorf("index_bytes = %d e os arquivos de índice em data/ somam %d blocos: a série está contando um "+
			"conjunto de arquivos diferente do que promete (.sidx, .sidx.tmp e .widx)", idx, quer)
	}

	// E ele responde à expulsão, que é a metade que dá sentido ao aceite: um
	// número que só sobe não distingue "o índice parou de crescer" de "a série
	// não olha para o disco".
	if aplicaWindow(t, hist, WindowOfIndex{Duration: 5 * time.Minute}) == 0 {
		t.Fatal("montagem: a janela não expulsou nada")
	}
	if depois := hist.EstatisticasOfIndex().Bytes; depois >= idx {
		t.Errorf("index_bytes = %d depois de a janela expulsar índices, e era %d: a expulsão devolve .sidx, "+
			"e uma série que não cai não mede disco", depois, idx)
	}
}

// TestAsReconstrucoesSaoContadasPorSegmentoPublicado tem DUAS metades, e é a
// segunda que carrega o teste.
//
// A primeira mede o caso feliz: uma passada sobre segmentos cujo índice foi
// expulso publica arquivo para todos, e o contador acompanha. Sozinha ela é
// tautológica — quando toda visita publica, "contar visita" e "contar publicação"
// dão o mesmo número, e foi assim que a versão anterior deste teste aprovou o
// contador que subia para o segmento pulado.
//
// A segunda metade é o segmento SEM CAMPO INDEXADO em record algum. Ele é
// visitado, varrido inteiro, não produz posting nenhuma, não gera arquivo — e
// FICA na fila de propósito (§2.4), porque "ausente" é pendência e o formato do
// .sidx não sabe dizer "considerei este campo e ele não aparece". Contá-lo como
// reconstrução faria a série crescer para sempre num dataset estável, e taxa que
// nunca zera é exatamente o sintoma de moinho que ela existe para denunciar.
//
// São TRÊS contagens diferentes no mesmo laço, e o teste as separa: `varreduras`
// (leituras, o custo), `feitos` (visitados nesta passada, o que a §2.4 afirma) e
// `Reconstrucoes` (publicados, o que a série promete).
func TestAsReconstrucoesSaoContadasPorSegmentoPublicado(t *testing.T) {
	// Metade 1 — quem publica é contado.
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)

	larga := WindowOfIndex{Duration: 24 * time.Hour}
	if aplicaWindow(t, hist, WindowOfIndex{Duration: 5 * time.Minute}) == 0 {
		t.Fatal("montagem: a janela não expulsou nada e não haveria o que reconstruir")
	}
	fila := pendentes(t, hist, larga)
	if len(fila) == 0 {
		t.Fatal("montagem: a fila nasceu vazia")
	}

	before := hist.EstatisticasOfIndex().Reconstrucoes
	feitos, err := hist.reconstroiOsPendentes(t.Context(), fila)
	if err != nil {
		t.Fatalf("reconstroiOsPendentes: %v", err)
	}
	depois := hist.EstatisticasOfIndex().Reconstrucoes
	if feitos != len(fila) {
		t.Fatalf("a passada visitou %d de %d pendentes; o oráculo abaixo compara contra a fila inteira",
			feitos, len(fila))
	}
	if depois-before != int64(feitos) {
		t.Errorf("reconstrucoes subiu %d e a passada publicou %d segmento(s): um contador que não acompanha o "+
			"trabalho concluído não distingue a fila drenando de um laço moendo", depois-before, feitos)
	}

	// Metade 2 — quem NÃO publica não é contado. Sem ela, contar a visita passa.
	mudo, _ := datasetWithPendencias(t, 2, []string{"campo-que-nenhum-record-tem"})
	change := pendentes(t, mudo, WindowOfIndex{})
	if len(change) == 0 {
		t.Fatal("montagem: a fila do dataset sem campo indexado nasceu vazia")
	}

	varrBefore := mudo.varreduras.Load()
	recBefore := mudo.EstatisticasOfIndex().Reconstrucoes
	visitados, err := mudo.reconstroiOsPendentes(t.Context(), change)
	if err != nil {
		t.Fatalf("reconstroiOsPendentes (sem campo): %v", err)
	}
	if visitados != len(change) {
		t.Fatalf("montagem: a passada visitou %d de %d pendentes; sem visita não há o que deixar de contar",
			visitados, len(change))
	}
	if lidas := mudo.varreduras.Load() - varrBefore; lidas != int64(len(change)) {
		t.Fatalf("montagem: a passada varreu %d segmentos para %d pendentes; o trabalho tem de ter acontecido "+
			"de verdade para o oráculo abaixo significar alguma coisa", lidas, len(change))
	}
	if subiu := mudo.EstatisticasOfIndex().Reconstrucoes - recBefore; subiu != 0 {
		t.Errorf("reconstrucoes subiu %d para %d segmento(s) que foram varridos e NÃO publicaram arquivo nenhum: "+
			"eles ficam na fila de propósito, então num dataset estável a série cresceria para sempre — e taxa que "+
			"nunca zera é o próprio sintoma de moinho que ela existe para denunciar", subiu, len(change))
	}
	// E a fila continua lá, que é a pré-condição de tudo isso ser um problema.
	if depois := pendentes(t, mudo, WindowOfIndex{}); len(depois) != len(change) {
		t.Fatalf("a fila mudou de %v para %v: o segmento sem campo indexado tem de CONTINUAR pendente", change, depois)
	}
}

// TestAPassadaQueFalhaPreservaORetratoAnterior protege o medidor contra a
// medição interrompida, e as duas metades cobrem as duas formas de a passada
// morrer: ANTES de varrer (chamador recusado) e NO MEIO da varredura (falha de
// infra num arquivo).
//
// Uma varredura que morreu no meio descreve um dataset pela metade. Publicá-la
// faria a fila parecer ter encolhido quando o que encolheu foi a passada — e
// durante uma migração essa é exatamente a leitura que faz alguém concluir que o
// laço terminou e ir dormir.
//
// A ordem "passada BOA primeiro, falha depois" é o que dá poder ao oráculo, e ela
// foi escrita depois de a versão anterior deste teste APROVAR a mutação que
// publica zeros no caminho de erro: sem um retrato bom instalado antes, "publicou
// zeros" e "não publicou" são o mesmo estado observável, e a asserção passava por
// vacuidade.
func TestAPassadaQueFalhaPreservaORetratoAnterior(t *testing.T) {
	// Metade 1 — recusa ANTES da varredura. O materializado recebe uma passada
	// boa (janela infinita, que é a dele por construção) e só então uma janela
	// finita, que é erro de quem monta os limites.
	mat := openStore(t, Options{IndexedFieldsByName: []string{"node"}})
	if _, err := mat.SegmentosPendentesOfRebuild(WindowOfIndex{}); err != nil {
		t.Fatalf("montagem: a passada boa do materializado falhou: %v", err)
	}
	bomMat := mat.EstatisticasOfIndex().States
	if bomMat == (StatesOfSegment{}) {
		t.Fatalf("montagem: a passada boa não publicou retrato nenhum (%+v); sem ele a asserção abaixo "+
			"não distingue 'publicou zeros' de 'não publicou'", bomMat)
	}
	if _, err := mat.SegmentosPendentesOfRebuild(WindowOfIndex{Duration: time.Hour}); err == nil {
		t.Fatal("a pendência aceitou janela finita sobre um materializado")
	}
	if e := mat.EstatisticasOfIndex().States; e != bomMat {
		t.Errorf("a passada RECUSADA trocou o retrato de %+v para %+v: quem não mediu nada não publica nada",
			bomMat, e)
	}

	// Metade 2 — falha NO MEIO do laço. Um diretório no lugar do .sidx faz o
	// mmap devolver ENODEV, que não é nenhum dos três estados não-anômalos do
	// unreadableIndex: é falha de infra, e ela para a passada com parte dos
	// segmentos já classificados.
	hist, rel := histIndexed(t, 512)
	povoaHistory(t, hist, rel, 40, time.Minute)
	larga := WindowOfIndex{Duration: 24 * time.Hour}
	bom := states(t, hist, larga)
	if bom.Sealed == 0 {
		t.Fatalf("montagem: nenhum segmento selado (%+v)", bom)
	}

	segs := selados(t, hist)
	target := segs[len(segs)-1]
	path := segindex.SealedPath(hist.dirOfData(), target)
	if err := os.Remove(path); err != nil {
		t.Fatalf("montagem: remover %s: %v", path, err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("montagem: plantar diretório em %s: %v", path, err)
	}
	if _, err := hist.SegmentosPendentesOfRebuild(larga); err == nil {
		t.Fatal("a pendência não subiu o erro de infra: um .sidx que não se deixa ler é falha, não 'refaça' (§101)")
	}
	if e := hist.EstatisticasOfIndex().States; e != bom {
		t.Errorf("a passada interrompida publicou %+v por cima de %+v: o retrato de meia varredura mostra a fila "+
			"encolhendo quando o que encolheu foi a medição", e, bom)
	}
}

// TestOGaugeDeEstadosEhCegoAIndicePodre é o LIMITE do subsistema, medido em vez
// de presumido — e é ele que sustenta a prosa da nishidb_index_crc_falhas_total,
// que já mandou o operador olhar a série errada duas vezes neste ciclo.
//
// A intuição diz que um dataset com índices corrompidos deve aparecer diferente
// de um saudável em alguma série. No gauge de estados ele é IDÊNTICO, e a razão é
// estrutural: a fila de pendência é derivada da TABELA DE SEÇÕES, que não confere
// crc — não pode, seriam até 2,25 GB de leitura por arquivo a cada passada
// (débito 103). Para a contagem de estados, o arquivo podre é um índice completo,
// e ele é contado como `selado`.
//
// Esse é justamente o gauge que a migração acompanha minuto a minuto. Ele pode
// mostrar a fila drenando e todos os segmentos `selado` num dataset em que metade
// dos índices não responde.
//
// O QUE A ENUMERAÇÃO FAZ, e por que ela também não serve de alarme: ela sobe, sim
// — o segmento corrompido responde sem índice —, mas o incremento é o MESMO que
// um segmento fora da janela ou na fila produz, no mesmo balde e sem rótulo que
// separe a causa. Ela conta "quantos responderam sem índice", nunca "quantos
// deveriam ter índice e não tinham". Este teste mede as duas metades.
//
// Se um dia o gauge passar a discriminar — apagando o arquivo reprovado, que é o
// remédio do débito 103 —, ele reprova e manda atualizar a prosa junto. É o
// acoplamento que se quer: o limite escrito e o limite medido caem juntos.
func TestOGaugeDeEstadosEhCegoAIndicePodre(t *testing.T) {
	retrato := func(corromper int) EstatisticasOfIndex {
		t.Helper()
		base := openStore(t, Options{})
		dir := t.TempDir()
		s := openHistoryIndexedIn(t, base, dir, []string{"node"}, 8<<10)
		for range 4 {
			poeRowsUntilRotacionar(t, s, s.curSecID.Load())
		}
		field := fieldOfDicionario(t, s, "node")
		for sec := uint32(1); sec <= uint32(corromper); sec++ {
			corruptOUltimoByte(t, segindex.SealedPath(filepath.Join(dir, "data"), sec))
		}
		// A passada de pendência é o que publica os estados; sem ela o retrato
		// nasce zerado e a comparação abaixo seria entre dois nadas.
		pendentes(t, s, WindowOfIndex{})
		candidatosCrus(t, s, field, strVal("n1"))
		return s.EstatisticasOfIndex()
	}

	const podres = 2
	saudavel := retrato(0)
	podre := retrato(podres)

	if podre.CRCFalhas != podres || saudavel.CRCFalhas != 0 {
		t.Fatalf("montagem: crc_falhas = %d no podre e %d no saudável, queria %d e 0; sem a corrupção detectada "+
			"a comparação abaixo não descreve nada", podre.CRCFalhas, saudavel.CRCFalhas, podres)
	}
	if saudavel.States == (StatesOfSegment{}) {
		t.Fatalf("montagem: os estados nasceram zerados (%+v); a igualdade abaixo passaria por vacuidade", saudavel.States)
	}

	// A metade que carrega o limite: o gauge não se move.
	if saudavel.States != podre.States {
		t.Errorf("os estados DIFEREM entre o dataset saudável (%+v) e o corrompido (%+v): se isso passou a ser "+
			"verdade, o gauge deixou de ser cego à corrupção — atualize a Ajuda da série de crc, este godoc e o "+
			"débito 103 junto", saudavel.States, podre.States)
	}

	// A outra metade: a enumeração sobe exatamente como subiria com mais dois
	// segmentos sem índice por qualquer outra causa. O número é o mesmo; a causa
	// não viaja com ele.
	if delta := podre.ByEnumeracao - saudavel.ByEnumeracao; delta != podres {
		t.Errorf("a enumeração subiu %d com %d índices podres, queria %d: o incremento é o de um segmento sem "+
			"índice, e é por isso que ele não distingue corrupção de segmento fora da janela", delta, podres, podres)
	}
}

// corruptOUltimoByte troca o byte final do arquivo, que é onde o crc32c mora.
//
// O FIM, e não o começo: corrompendo o cabeçalho o arquivo vira "ilegível", cai
// no unreadableIndex e o segmento é reagendado — a corrupção some e o teste passa
// a medir outra coisa. É o mesmo cuidado que a bancada do servidor documenta.
func corruptOUltimoByte(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("montagem: stat de %s: %v", path, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("montagem: abrir %s: %v", path, err)
	}
	if _, err := f.WriteAt([]byte{0xAA}, fi.Size()-1); err != nil {
		t.Fatalf("montagem: corromper %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("montagem: fechar %s: %v", path, err)
	}
}
