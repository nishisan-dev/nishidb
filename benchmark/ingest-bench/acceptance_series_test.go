package main

import (
	"encoding/json"
	"os"
	"testing"
)

// Os artefatos de aceite do M4 são REGISTRO DE EXECUÇÃO — não são regerados, e
// é sobre eles que os números de docs/aceite-m4.md são conferíveis. O que não
// existia era alguém que os CONFERISSE: `avaliaGateDeDisco` só era exercitado
// contra séries sintéticas (TestGateDeDiscoReprovaCrescimentoLinear e vizinhos),
// de modo que a série da rodada REAL entrava no documento sem oráculo nenhum.
// É o débito 6 num nível acima: o instrumento mede, publica, e ninguém confere o
// que ele publicou.
const (
	artefatoDoAceiteM4 = "../baselines/aceite-m4-1m-20260822.json"
	artefatoBase250k   = "../baselines/aceite-m4-base-250k-20260822.json"
)

func leArtefato(t *testing.T, caminho string) Report {
	t.Helper()
	bruto, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("lendo %s: %v", caminho, err)
	}
	var rep Report
	if err := json.Unmarshal(bruto, &rep); err != nil {
		t.Fatalf("decodificando %s: %v", caminho, err)
	}
	return rep
}

// TestOPlateauDoAceiteM4DependeDaAmostraColhidaForaDaJanelaDeCarga fixa o
// defeito que o gate NÃO apanha, e o fixa como fato para que ninguém volte a
// citar a razão 0,339 como prova de plateau.
//
// A rodada travou no débito 67 ao fim da janela de regime e ficou 12 minutos
// parada. A série de disco não tem amostra DENTRO do bloqueio — é verdade —,
// mas tem a amostra do FIM dele: a penúltima está em elapsed 1.209,05 s com
// 8.400.000 eventos, e a última em 1.936,70 s com 8.400.001, ou seja, 727,65 s
// depois, com UM evento de avanço. Nesse vão a compaction seguiu moendo com
// ZERO evento entrando, e devolveu 33,5 MiB.
//
// `avaliaGateDeDisco` lê `serie[len(serie)-1]` como extremo da segunda metade,
// sem perguntar QUANDO aquela amostra foi colhida. O resultado é que o veredito
// do gate DURO da rodada inteira pendura-se num ponto medido fora da janela de
// carga — e o comentário do próprio collector (harness.go, no `sample`) condena
// o padrão com todas as letras ao recusar carregar valor para frente: "a série
// ficaria PLANA no trecho não medido, e plano é o desenho que o gate aprova".
// Aqui é pior que plano: é uma QUEDA injetada no extremo.
//
// A asserção que prova alguma coisa é a SEGUNDA. A primeira é só o registro do
// que o artefato publica; um oráculo que só confirmasse o publicado seria
// satisfeito por `return GatePassou`. Conserto do instrumento é o débito 68.
func TestOPlateauDoAceiteM4DependeDaAmostraColhidaForaDaJanelaDeCarga(t *testing.T) {
	rep := leArtefato(t, artefatoDoAceiteM4)
	serie := serieDeDiscoDoSteady(rep.Samples)
	if len(serie) < 2 {
		t.Fatalf("série de disco do steady com %d amostras: o artefato mudou", len(serie))
	}

	// O vão. Ele é a razão de a última amostra não pertencer à janela de carga:
	// entre ela e a anterior a ingestão avançou UM evento — o que foi
	// reproduzido à mão no tópico para destravar o harness.
	amostras := amostrasDoSteady(rep.Samples)
	ultima, penultima := amostras[len(amostras)-1], amostras[len(amostras)-2]
	vao := ultima.ElapsedSec - penultima.ElapsedSec
	avanco := ultima.Events - penultima.Events
	if vao < 600 || avanco > 1 {
		t.Fatalf("vão de %.2f s com %d eventos de avanço entre as duas últimas amostras: "+
			"o artefato deixou de ser o da rodada travada, e este teste perdeu o objeto", vao, avanco)
	}
	if ultima.DiskBytes >= penultima.DiskBytes {
		t.Fatalf("a última amostra tem %d B contra %d B da anterior: sem a QUEDA no extremo "+
			"o defeito descrito aqui não é este", ultima.DiskBytes, penultima.DiskBytes)
	}

	comAAmostra := avaliaGateDeDisco(serie)
	if comAAmostra.Status != GatePassou {
		t.Errorf("com a amostra pós-bloqueio o gate = %q, e o artefato publica pass: %s",
			comAAmostra.Status, comAAmostra.Detail)
	}

	semAAmostra := avaliaGateDeDisco(serie[:len(serie)-1])
	if semAAmostra.Status != GateReprovou {
		t.Errorf("sem a amostra pós-bloqueio o gate = %q, queria %q — se esta asserção "+
			"passar a falhar, a rodada passou a provar o plateau por si e o texto de "+
			"docs/aceite-m4.md precisa ser reescrito: %s",
			semAAmostra.Status, GateReprovou, semAAmostra.Detail)
	}
	t.Logf("com a amostra:  %s", comAAmostra.Detail)
	t.Logf("sem a amostra:  %s", semAAmostra.Detail)

	// A razão publicada não depende só da última amostra: ela é um ponto de
	// SORTE na vizinhança. Truncando de 1 a 7 amostras do fim o veredito
	// OSCILA — a maioria reprova, e k=3 volta a aprovar com 0,262 —, e é essa
	// oscilação, não a reprovação uniforme, que o documento publica.
	//
	// A asserção é a INSTABILIDADE, e não "todos reprovam", porque a segunda
	// seria falsa: escrevê-la aqui foi o primeiro rascunho deste teste, e o
	// próprio oráculo a derrubou. Um gate cujo veredito muda conforme qual
	// amostra calhou de ser a última não está decidindo sobre a curva; está
	// decidindo sobre o recorte. Quem responde sobre a curva é o
	// TestOPlateauDoAceiteM4TambemReprovaPorMinimosQuadrados.
	vistos := map[GateStatus]int{}
	for k := 0; k <= 7; k++ {
		g := avaliaGateDeDisco(serie[:len(serie)-k])
		vistos[g.Status]++
	}
	if vistos[GatePassou] == 0 || vistos[GateReprovou] == 0 {
		t.Errorf("truncando de 0 a 7 amostras do fim o veredito foi sempre o mesmo "+
			"(%d pass, %d fail): a razão do gate deixou de ser instável na "+
			"vizinhança, e o argumento do documento precisa ser refeito",
			vistos[GatePassou], vistos[GateReprovou])
	}
}

// TestOPlateauDoAceiteM4TambemReprovaPorMinimosQuadrados confirma a leitura
// acima por um estimador que NÃO é o do gate.
//
// O `disk_plateau` compara EXTREMOS de cada metade, e extremo é justamente o
// que uma amostra solta move. Um crítico honesto pergunta se a reprovação não é
// artefato desse estimador; a resposta é medir a inclinação por mínimos
// quadrados, que usa todos os pontos e não dá peso especial a nenhum. Com a
// amostra contaminada a razão dá 0,473; sem ela, 0,478 — as duas acima do teto
// de 0,35. O gate é que oscilava entre 0,339 e 0,452; a curva sempre foi a
// mesma.
//
// O estimador é AFERIDO antes de ser usado (rampa de inclinação conhecida),
// porque helper de teste com aritmética própria e sem oráculo é o débito 63 e
// não vale a pena repetir o erro para provar que o outro existe.
func TestOPlateauDoAceiteM4TambemReprovaPorMinimosQuadrados(t *testing.T) {
	rampa := []int64{100, 300, 500, 700, 900}
	if got := inclinacaoPorMinimosQuadrados(rampa); got != 200 {
		t.Fatalf("o estimador devolveu %.4f para uma rampa de 200 por amostra: "+
			"sem aferição ele não serve para aferir coisa nenhuma", got)
	}

	rep := leArtefato(t, artefatoDoAceiteM4)
	serie := serieDeDiscoDoSteady(rep.Samples)
	for _, caso := range []struct {
		name  string
		serie []int64
	}{
		{"com a amostra pós-bloqueio", serie},
		{"sem a amostra pós-bloqueio", serie[:len(serie)-1]},
	} {
		meio := len(caso.serie) / 2
		primeira := inclinacaoPorMinimosQuadrados(caso.serie[:meio+1])
		segunda := inclinacaoPorMinimosQuadrados(caso.serie[meio:])
		razao := segunda / primeira
		if razao < fracaoDeCrescimentoTolerada {
			t.Errorf("%s: razão por mínimos quadrados = %.3f, abaixo do teto %.2f — "+
				"o estimador robusto passou a APROVAR, e o texto de docs/aceite-m4.md "+
				"precisa ser reescrito", caso.name, razao, fracaoDeCrescimentoTolerada)
		}
		t.Logf("%s: 1ª %.0f B/amostra, 2ª %.0f B/amostra, razão %.3f",
			caso.name, primeira, segunda, razao)
	}
}

// TestODiscoDoAceiteM4CresceEmTodaJanelaDeSessentaSegundos é a afirmação
// positiva que sobra depois de a razão do gate cair.
//
// Reprovar o gate diz que a segunda metade não é plana o bastante; não diz que
// o disco subiu o tempo todo. Esta é a medida que diz: a MÉDIA do disco em cada
// janela de 60 s da fase de carga é maior que a da janela anterior, das dez
// janelas, sem exceção. A compaction desacelera o crescimento — 91 MiB/min na
// primeira transição contra 26 MiB/min na última —, e desacelerar adia o ENOSPC
// do §86; parar é o que o evita, e parar não aconteceu nesta escala.
func TestODiscoDoAceiteM4CresceEmTodaJanelaDeSessentaSegundos(t *testing.T) {
	rep := leArtefato(t, artefatoDoAceiteM4)
	amostras := amostrasDoSteady(rep.Samples)
	sobCarga := amostras[:len(amostras)-1] // a pós-bloqueio não pertence à janela

	inicio := sobCarga[0].ElapsedSec
	var medias []float64
	var soma float64
	var n int
	janelaAtual := 0
	fecha := func() {
		if n > 0 {
			medias = append(medias, soma/float64(n)/(1<<20))
		}
		soma, n = 0, 0
	}
	for _, a := range sobCarga {
		if j := int((a.ElapsedSec - inicio) / 60); j != janelaAtual {
			fecha()
			janelaAtual = j
		}
		soma += float64(a.DiskBytes)
		n++
	}
	fecha()

	if len(medias) < 10 {
		t.Fatalf("%d janelas de 60 s na fase de carga: a série encurtou", len(medias))
	}
	for i := 1; i < len(medias); i++ {
		if medias[i] <= medias[i-1] {
			t.Errorf("janela %d com média de %.1f MiB contra %.1f MiB da anterior: "+
				"o disco parou de crescer em algum ponto da janela, e o documento "+
				"afirma que não parou", i+1, medias[i], medias[i-1])
		}
	}
	t.Logf("médias por janela de 60 s (MiB): %.1f", medias)
}

// inclinacaoPorMinimosQuadrados devolve o crescimento por AMOSTRA da reta que
// melhor ajusta a série — o eixo x é o índice, exatamente a normalização que o
// gate usa, para que as duas leituras sejam comparáveis.
func inclinacaoPorMinimosQuadrados(serie []int64) float64 {
	n := float64(len(serie))
	var somaX, somaY float64
	for i, v := range serie {
		somaX += float64(i)
		somaY += float64(v)
	}
	mediaX, mediaY := somaX/n, somaY/n
	var num, den float64
	for i, v := range serie {
		dx := float64(i) - mediaX
		num += dx * (float64(v) - mediaY)
		den += dx * dx
	}
	return num / den
}

// TestOChurnNaoSeparaAprovacaoDeReprovacaoNosArtefatosDoM4 desmente, contra
// artefato do próprio repositório, a regra causal que o M4 publicou.
//
// O documento e o débito 65 afirmavam que a variável que decide o plateau é o
// CHURN — quantas vezes o key set inteiro foi reescrito —, e o sidecar da
// rodada chegava a escrever a fronteira: "abaixo de ~6× o key set reescrito,
// reprova". A rodada de referência do §110, arquivada no mesmo commit, tem o
// MENOR churn de todas (959.994 eventos sobre 250.000 entidades = 3,84×) e o
// `disk_plateau` dela PASSA. Uma regra que o próprio conjunto de artefatos
// contradiz não é regra; é coincidência promovida a explicação.
func TestOChurnNaoSeparaAprovacaoDeReprovacaoNosArtefatosDoM4(t *testing.T) {
	base := leArtefato(t, artefatoBase250k)
	churn := float64(base.Steady.Events) / float64(base.Config.Entities)
	if churn >= 6 {
		t.Fatalf("churn de %.2f× na rodada de referência: ela deixou de ser o "+
			"contraexemplo da fronteira de ~6×", churn)
	}
	g := avaliaGateDeDisco(serieDeDiscoDoSteady(base.Samples))
	if g.Status != GatePassou {
		t.Errorf("disk_plateau = %q com churn de %.2f×, queria %q — a fronteira por "+
			"churn voltaria a se sustentar e docs/aceite-m4.md precisaria voltar "+
			"a afirmá-la: %s", g.Status, churn, GatePassou, g.Detail)
	}
	t.Logf("churn %.2f×, disk_plateau %s: %s", churn, g.Status, g.Detail)
}

// amostrasDoSteady é o gêmeo de serieDeDiscoDoSteady que preserva a amostra
// inteira, porque o defeito acima só é visível no par (elapsed, events) que a
// série de int64 joga fora.
func amostrasDoSteady(amostras []Sample) []Sample {
	var fora []Sample
	for _, a := range amostras {
		if a.Phase == "steady" {
			fora = append(fora, a)
		}
	}
	return fora
}
