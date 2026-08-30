package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
)

// estados roda uma passada de pendência e devolve o retrato que ela publicou.
//
// Ele passa pelo SegmentosPendentesDeReconstrucao de propósito, e não lê os
// atômicos direto: o que estes testes provam é que o retrato sai da mesma conta
// que decide a fila. Um oráculo que escrevesse os atômicos na mão provaria
// apenas que atomic.Store funciona.
func estados(t *testing.T, s *Store, j JanelaDeIndice) EstadosDeSegmento {
	t.Helper()
	pendentes(t, s, j)
	return s.EstatisticasDeIndice().Estados
}

// totalDeSegmentos conta os .dat do dataset, por fora do Store. É o denominador
// da partição, e derivá-lo do próprio código dos estados faria o teste concordar
// com a implementação por construção.
func totalDeSegmentos(t *testing.T, s *Store) int64 {
	t.Helper()
	refs, err := s.listaSegmentos()
	if err != nil {
		t.Fatalf("listaSegmentos: %v", err)
	}
	return int64(len(refs))
}

// discoDosIndicesDeSegmento é o `du` do operador sobre os arquivos de índice que
// moram em data/, medido por FORA do Store.
//
// A lista de extensões é escrita AQUI à mão, e essa duplicação é deliberada:
// perguntar ao segindex.EhArquivoDeIndice — o predicado que a produção usa —
// faria o oráculo concordar com a implementação por construção, e a pergunta em
// julgamento é exatamente "quais arquivos entram na conta?". É o mesmo argumento
// que faz o oráculo de storage_bytes somar os .dat por fora em vez de perguntar
// ao Store.
func discoDosIndicesDeSegmento(t *testing.T, s *Store) int64 {
	t.Helper()
	entradas, err := os.ReadDir(s.dirDeDados())
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", s.dirDeDados(), err)
	}
	var total int64
	for _, e := range entradas {
		nome := e.Name()
		if !strings.HasSuffix(nome, ".sidx") && !strings.HasSuffix(nome, ".widx") && !strings.HasSuffix(nome, ".tmp") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // sumiu entre o readdir e o stat: é disco que já voltou
		}
		total += blocosDe(info)
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
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	estreita := JanelaDeIndice{Duracao: 5 * time.Minute}
	if aplicaJanela(t, hist, estreita) == 0 {
		t.Fatal("montagem: estreitar a janela não expulsou nenhum índice, e sem expulsão não há estado `fora` a distinguir")
	}

	e := estados(t, hist, estreita)
	total := totalDeSegmentos(t, hist)
	if soma := e.Selado + e.Corrente + e.Pendente + e.Fora; soma != total {
		t.Errorf("os quatro estados somam %d e o dataset tem %d segmento(s) em disco (%+v): "+
			"segmento que não cai em balde nenhum some do painel sem rastro", soma, total, e)
	}
	if e.Corrente != 1 {
		t.Errorf("estado corrente = %d, quer 1: o segmento que ainda recebe records é exatamente um, "+
			"e o índice dele é o WIP", e.Corrente)
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
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	estreita := JanelaDeIndice{Duracao: 5 * time.Minute}
	larga := JanelaDeIndice{Duracao: 24 * time.Hour}
	if aplicaJanela(t, hist, estreita) == 0 {
		t.Fatal("montagem: estreitar a janela não expulsou nenhum índice")
	}

	antes := estados(t, hist, larga)
	if antes.Pendente == 0 {
		t.Fatalf("montagem: alargar a janela não pôs ninguém na fila (%+v); sem pendência não há migração a medir", antes)
	}
	fila := pendentes(t, hist, larga)
	for _, seg := range fila {
		if err := hist.ConstroiIndiceDoSegmento(t.Context(), seg, hist.CamposIndexados()); err != nil {
			t.Fatalf("ConstroiIndiceDoSegmento(%d): %v", seg, err)
		}
	}

	depois := estados(t, hist, larga)
	if depois.Pendente != 0 {
		t.Errorf("pendente = %d depois de reconstruir a fila inteira, quer 0 (%+v)", depois.Pendente, depois)
	}
	if ganho := depois.Selado - antes.Selado; ganho != antes.Pendente {
		t.Errorf("selado subiu %d e a fila tinha %d: o que sai de `pendente` tem de entrar em `selado`, senão o painel "+
			"mostra trabalho sumindo sem virar índice (antes=%+v, depois=%+v)", ganho, antes.Pendente, antes, depois)
	}
	if soma := depois.Selado + depois.Corrente + depois.Pendente + depois.Fora; soma != totalDeSegmentos(t, hist) {
		t.Errorf("a partição furou depois da reconstrução: %+v soma %d", depois, soma)
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
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)
	campo := campoDoDicionario(t, hist, "status")

	// A janela é PARCIAL de propósito: com 40 records de um em um minuto, vinte
	// minutos deixam os segmentos novos indexados e expulsam os velhos. Uma
	// janela que expulsasse tudo daria um Lookup só de enumeração, e aí "não
	// contar índice" passaria.
	parcial := JanelaDeIndice{Duracao: 20 * time.Minute}
	if aplicaJanela(t, hist, parcial) == 0 {
		t.Fatal("montagem: sem expulsão todos os segmentos responderiam por índice e o par não se separaria")
	}
	comIndice := int64(len(segmentosComIndice(t, hist)))
	total := totalDeSegmentos(t, hist)
	if comIndice == 0 || comIndice == total {
		t.Fatalf("montagem: %d de %d segmentos com índice; o teste precisa dos DOIS caminhos no mesmo Lookup",
			comIndice, total)
	}

	antes := hist.EstatisticasDeIndice()
	candidatosCrus(t, hist, campo, strVal("a"))
	depois := hist.EstatisticasDeIndice()

	porIndice := depois.PorIndice - antes.PorIndice
	porEnum := depois.PorEnumeracao - antes.PorEnumeracao
	if porIndice+porEnum != total {
		t.Errorf("um Lookup contou %d por índice + %d por enumeração = %d, e o dataset tem %d segmentos: "+
			"cada segmento responde por exatamente um caminho", porIndice, porEnum, porIndice+porEnum, total)
	}
	// O corrente responde pelo WIP, que conta como índice — daí o +1.
	if porIndice != comIndice+1 {
		t.Errorf("modo=indice contou %d, e há %d selados com .sidx mais o corrente pelo WIP: "+
			"o contador não está seguindo a decisão real do fontesDoCampo", porIndice, comIndice)
	}
	if porEnum != total-comIndice-1 {
		t.Errorf("modo=enumeracao contou %d, e %d segmentos estão sem índice: um dataset degradado que reporta "+
			"zero enumerações é indistinguível de um dataset indexado", porEnum, total-comIndice-1)
	}
}

// TestOCrcDivergenteContaFalhaEDegradaParaEnumeracao liga o contador ao efeito.
//
// As duas metades juntas, porque cada uma sozinha passa com o defeito da outra:
// contar sem degradar seria uma consulta derrubada por índice podre, e degradar
// sem contar é exatamente o estado anterior a esta tarefa — o dataset responde
// certo e devagar, para sempre, sem ninguém saber por quê.
func TestOCrcDivergenteContaFalhaEDegradaParaEnumeracao(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	s := abreHistoricoIndexadoEm(t, base, dir, []string{"node"}, 8<<10)
	alvo := s.curSegID.Load()
	poeLinhasAteRotacionar(t, s, alvo)
	poeLinhasAteRotacionar(t, s, s.curSegID.Load())
	campo := campoDoDicionario(t, s, "node")

	// A corrupção acontece ANTES de o arquivo ser aberto, que é o caminho real:
	// um .sidx que já estava podre em disco quando a consulta chegou.
	caminho := segindex.CaminhoSelado(filepath.Join(dir, "data"), alvo)
	f, err := os.OpenFile(caminho, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xAA, 0xBB, 0xCC, 0xDD}, 64); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	antes := s.EstatisticasDeIndice()
	crus := candidatosCrus(t, s, campo, strVal("nunca-visto"))
	depois := s.EstatisticasDeIndice()

	if depois.CRCFalhas-antes.CRCFalhas != 1 {
		t.Errorf("crc_falhas subiu %d ao abrir um .sidx corrompido, quer 1: sem esta série um índice podre responde "+
			"por enumeração para sempre e nada denuncia", depois.CRCFalhas-antes.CRCFalhas)
	}
	if len(crus) == 0 {
		t.Fatal("o segmento corrompido não virou candidato: ele tem de DEGRADAR para enumeração, não responder zero linhas — " +
			"falso negativo é a única coisa que a revalidação do §44 não sabe consertar")
	}
	if depois.PorEnumeracao-antes.PorEnumeracao == 0 {
		t.Error("nenhum segmento foi contado como enumerado depois da corrupção: o arquivo reprovado no crc é " +
			"exatamente um segmento sem índice, e o painel precisa vê-lo assim")
	}
}

// TestOsBytesDoIndicePorSegmentoSaoParcelaEstritaDoIndexBytes fixa a relação
// entre as duas séries de disco, que é o que impede a próxima leitura de tratar
// uma como a outra.
//
// nishidb_index_bytes é o que este subsistema governa e o que precisa PARAR DE
// CRESCER com a tabela em regime; nishidb_history_index_bytes soma também
// indexes/ e checkpoint/, que nenhuma janela devolve. Publicar as duas iguais
// faria o aceite do ciclo ser medido sobre um número que o desenho não promete.
func TestOsBytesDoIndicePorSegmentoSaoParcelaEstritaDoIndexBytes(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	idx := hist.EstatisticasDeIndice().Bytes
	if idx <= 0 {
		t.Fatalf("nishidb_index_bytes = %d num dataset com .sidx e .widx em disco", idx)
	}
	// O oráculo é um `du` do operador sobre data/, feito POR FORA do Store e sem
	// perguntar ao predicado que está sendo testado. Comparar contra o
	// history_index_bytes não bastava: com o filtro removido, a série passava a
	// somar os .dat inteiros e CONTINUAVA menor que aquele número — a asserção
	// aprovava a mutação. O que discrimina é o conjunto exato de arquivos.
	if quer := discoDosIndicesDeSegmento(t, hist); idx != quer {
		t.Errorf("index_bytes = %d e os arquivos de índice em data/ somam %d blocos: a série está contando um "+
			"conjunto de arquivos diferente do que promete (.sidx, .sidx.tmp e .widx)", idx, quer)
	}
	ret := hist.EstatisticasDeRetencao().IndexBytes
	if idx >= ret {
		t.Errorf("index_bytes = %d e history_index_bytes = %d: o segundo soma também indexes/ e checkpoint/, "+
			"então o primeiro tem de ser ESTRITAMENTE menor — iguais significa que um dos dois parou de contar "+
			"o que promete", idx, ret)
	}

	// E ele responde à expulsão, que é a metade que dá sentido ao aceite: um
	// número que só sobe não distingue "o índice parou de crescer" de "a série
	// não olha para o disco".
	if aplicaJanela(t, hist, JanelaDeIndice{Duracao: 5 * time.Minute}) == 0 {
		t.Fatal("montagem: a janela não expulsou nada")
	}
	if depois := hist.EstatisticasDeIndice().Bytes; depois >= idx {
		t.Errorf("index_bytes = %d depois de a janela expulsar índices, e era %d: a expulsão devolve .sidx, "+
			"e uma série que não cai não mede disco", depois, idx)
	}
}

// TestAsReconstrucoesSaoContadasPorSegmentoPublicado separa este contador do
// varreduras, que é o vizinho fácil de confundir.
//
// varreduras conta LEITURAS de segmento — inclusive a da abertura e inclusive a
// que terminou sem posting nenhuma e não publicou arquivo. Este conta trabalho
// concluído, e é ele que responde "a fila está drenando?". Somá-los faria a taxa
// parecer alta num laço que só relê e não publica, que é precisamente o moinho
// que a fila-superconjunto pode produzir.
func TestAsReconstrucoesSaoContadasPorSegmentoPublicado(t *testing.T) {
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)

	larga := JanelaDeIndice{Duracao: 24 * time.Hour}
	if aplicaJanela(t, hist, JanelaDeIndice{Duracao: 5 * time.Minute}) == 0 {
		t.Fatal("montagem: a janela não expulsou nada e não haveria o que reconstruir")
	}
	fila := pendentes(t, hist, larga)
	if len(fila) == 0 {
		t.Fatal("montagem: a fila nasceu vazia")
	}

	antes := hist.EstatisticasDeIndice().Reconstrucoes
	feitos, err := hist.reconstroiOsPendentes(t.Context(), fila)
	if err != nil {
		t.Fatalf("reconstroiOsPendentes: %v", err)
	}
	depois := hist.EstatisticasDeIndice().Reconstrucoes
	if feitos != len(fila) {
		t.Fatalf("a passada reconstruiu %d de %d pendentes; o oráculo abaixo compara contra a fila inteira",
			feitos, len(fila))
	}
	if depois-antes != int64(feitos) {
		t.Errorf("reconstrucoes subiu %d e a passada publicou %d segmento(s): um contador que não acompanha o "+
			"trabalho concluído não distingue a fila drenando de um laço moendo", depois-antes, feitos)
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
	mat := openStore(t, Options{CamposIndexadosPorNome: []string{"node"}})
	if _, err := mat.SegmentosPendentesDeReconstrucao(JanelaDeIndice{}); err != nil {
		t.Fatalf("montagem: a passada boa do materializado falhou: %v", err)
	}
	bomMat := mat.EstatisticasDeIndice().Estados
	if bomMat == (EstadosDeSegmento{}) {
		t.Fatalf("montagem: a passada boa não publicou retrato nenhum (%+v); sem ele a asserção abaixo "+
			"não distingue 'publicou zeros' de 'não publicou'", bomMat)
	}
	if _, err := mat.SegmentosPendentesDeReconstrucao(JanelaDeIndice{Duracao: time.Hour}); err == nil {
		t.Fatal("a pendência aceitou janela finita sobre um materializado")
	}
	if e := mat.EstatisticasDeIndice().Estados; e != bomMat {
		t.Errorf("a passada RECUSADA trocou o retrato de %+v para %+v: quem não mediu nada não publica nada",
			bomMat, e)
	}

	// Metade 2 — falha NO MEIO do laço. Um diretório no lugar do .sidx faz o
	// mmap devolver ENODEV, que não é nenhum dos três estados não-anômalos do
	// indiceIlegivel: é falha de infra, e ela para a passada com parte dos
	// segmentos já classificados.
	hist, rel := histIndexado(t, 512)
	povoaHistorico(t, hist, rel, 40, time.Minute)
	larga := JanelaDeIndice{Duracao: 24 * time.Hour}
	bom := estados(t, hist, larga)
	if bom.Selado == 0 {
		t.Fatalf("montagem: nenhum segmento selado (%+v)", bom)
	}

	segs := selados(t, hist)
	alvo := segs[len(segs)-1]
	caminho := segindex.CaminhoSelado(hist.dirDeDados(), alvo)
	if err := os.Remove(caminho); err != nil {
		t.Fatalf("montagem: remover %s: %v", caminho, err)
	}
	if err := os.Mkdir(caminho, 0o755); err != nil {
		t.Fatalf("montagem: plantar diretório em %s: %v", caminho, err)
	}
	if _, err := hist.SegmentosPendentesDeReconstrucao(larga); err == nil {
		t.Fatal("a pendência não subiu o erro de infra: um .sidx que não se deixa ler é falha, não 'refaça' (§101)")
	}
	if e := hist.EstatisticasDeIndice().Estados; e != bom {
		t.Errorf("a passada interrompida publicou %+v por cima de %+v: o retrato de meia varredura mostra a fila "+
			"encolhendo quando o que encolheu foi a medição", e, bom)
	}
}
