package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompactaUmaVezFuncionaComOLacoDesligado é o caso de uso central da Task
// B5: o operador desligou o laço automático (storage.compaction.enabled:
// false) e quer devolver disco na mão. Sem NUNCA ter chamado IniciaCompaction,
// CompactaUmaVez varre e aposenta os candidatos de uma eleição só, e devolve
// o agregado do trabalho feito.
func TestCompactaUmaVezFuncionaComOLacoDesligado(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	eleitos := s.candidatos(0.5)
	if len(eleitos) == 0 {
		t.Fatal("nenhum candidato com 30 rodadas de update sobre 40 keys: sem eleição não há o que este teste exercite")
	}

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados != len(eleitos) {
		t.Errorf("SegmentosAposentados = %d, quer %d (a mesma eleição que candidatos(0.5) fotografou antes)",
			res.SegmentosAposentados, len(eleitos))
	}
	if res.BytesLidos <= 0 || res.BytesEscritos < 0 {
		t.Errorf("BytesLidos=%d BytesEscritos=%d: uma passada que aposentou %d segmento(s) tem de ter lido algo",
			res.BytesLidos, res.BytesEscritos, res.SegmentosAposentados)
	}
	exigeCorpusNaUltimaRodada(t, s, keys, rodadas-1, "após CompactaUmaVez com o laço desligado")
}

// TestCompactaUmaVezSemCandidatoDevolveZerosSemErro é o "idempotente por
// efeito" do brief: repetir sobre um dataset já compactado (ou um dataset sem
// garbage nenhum) custa uma eleição vazia e não é erro — os três campos saem
// zerados, e é essa a resposta HONESTA de "não havia o que fazer".
func TestCompactaUmaVezSemCandidatoDevolveZerosSemErro(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	if _, err := s.CompactaUmaVez(context.Background(), 0.5); err != nil {
		t.Fatalf("primeira passada: %v", err)
	}

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("segunda passada (nada para compactar): %v", err)
	}
	if res != (ResultadoCompactacao{}) {
		t.Errorf("segunda passada sobre dataset já compactado devolveu %+v, queria todos os campos zerados", res)
	}
}

// TestCompactaUmaVezComPassadaDoLacoEmVooDevolveBusy é o Critical da revisão
// da B5: o 409 tem de disparar com uma passada de VERDADE em voo — não só
// porque o laço automático EXISTE (compCancela fica não-nil da primeira à
// última hora do laço, dormindo ou varrendo; checá-lo tornava a rota morta o
// tempo todo na config default). Aqui o mesmo gate de aposentadoria de
// TestCloseCancelaEAguardaACompaction trava a PRIMEIRA passada do laço (ela é
// imediata, sem cochilo antes) no meio, e só então CompactaUmaVez é chamada
// — determinístico, sem sleep de corrida.
func TestCompactaUmaVezComPassadaDoLacoEmVooDevolveBusy(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var umaVez sync.Once
	s.SetAposentadoriaTraceForTest(func(string) {
		umaVez.Do(func() {
			close(emVoo)
			<-libera
		})
	})

	s.IniciaCompaction(context.Background(), 0.5)
	defer s.ParaCompaction()

	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("o laço não chegou a aposentar nada em 60s: sem uma passada em voo o teste não exercita nada")
	}

	if _, err := s.CompactaUmaVez(context.Background(), 0.5); !errors.Is(err, ErrCompactionBusy) {
		t.Fatalf("CompactaUmaVez com a passada do laço em voo: err = %v, quer ErrCompactionBusy", err)
	}

	liberar()
}

// TestRodaCompactionPulaARodadaQuandoUmaChamadaManualEstaEmVoo é o OUTRO
// sentido da exclusão hermética (revisão da B5): com uma passada MANUAL em
// voo, a tentativa do laço automático de rodar a SUA passada (aqui, a
// primeira, que é imediata) tem de PULAR — nunca subir uma segunda
// varredura concorrente disputando wmu record a record com a manual. O
// oráculo é o mesmo estilo de "ausência dentro de uma janela" de
// TestSegundaPartidaDaCompactionNaoSobeSegundaGoroutine: se o laço tivesse
// intrudido, um segundo passo de aposentadoria chegaria ao seam quase
// imediatamente (a primeira passada de IniciaCompaction não tem cochilo
// antes dela) — a janela de 500ms é folga larga sobre qualquer agendamento
// de goroutine.
func TestRodaCompactionPulaARodadaQuandoUmaChamadaManualEstaEmVoo(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var primeira atomic.Bool
	var intrusos atomic.Int64
	s.SetAposentadoriaTraceForTest(func(string) {
		if primeira.CompareAndSwap(false, true) {
			close(emVoo)
			<-libera
			return
		}
		intrusos.Add(1)
	})

	manual := make(chan error, 1)
	go func() {
		_, err := s.CompactaUmaVez(context.Background(), 0.5)
		manual <- err
	}()

	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("a chamada manual não chegou a aposentar nada em 60s")
	}

	// A passada MANUAL está em voo (parada no gate). O laço automático
	// começa AGORA — a primeira passada dele é imediata — e disputa a MESMA
	// vaga do single-flight fino.
	s.IniciaCompaction(context.Background(), 0.5)
	defer s.ParaCompaction()

	time.Sleep(500 * time.Millisecond)
	if n := intrusos.Load(); n != 0 {
		t.Fatalf("o laço automático rodou %d passo(s) de aposentadoria enquanto a chamada manual estava em voo: "+
			"duas varreduras concorrentes disputariam wmu record a record", n)
	}

	liberar()
	if err := <-manual; err != nil {
		t.Fatalf("chamada manual: %v", err)
	}
}

// TestCompactaUmaVezFuncionaComOLacoLigadoMasOcioso é o outro lado do
// Critical: só existir o laço (storage.compaction.enabled: true, o default)
// não pode bastar para recusar CompactaUmaVez para sempre — a rota tem de
// responder quando não há passada NENHUMA em voo, mesmo com o laço
// configurado. Sem garbage nenhum, a eleição do laço e a manual são sempre
// vazias — nenhuma das duas passa tempo "aposentando" —, então não há como
// travar nenhuma delas no meio para um gate; o oráculo é chamar de novo,
// imediatamente, sem sleep de corrida nenhum: cada tentativa ou vê a vaga
// livre (o caso esperado — a eleição vazia do laço dura microssegundos, não
// segundos) ou recebe ErrCompactionBusy e tenta de novo na hora. O teto
// existe só para falhar alto se a rota estivesse morta de verdade.
func TestCompactaUmaVezFuncionaComOLacoLigadoMasOcioso(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})

	s.IniciaCompaction(context.Background(), 0.5)
	defer s.ParaCompaction()

	const tentativas = 500
	var err error
	for i := 0; i < tentativas; i++ {
		_, err = s.CompactaUmaVez(context.Background(), 0.5)
		if !errors.Is(err, ErrCompactionBusy) {
			break
		}
	}
	if err != nil {
		t.Fatalf("CompactaUmaVez nunca teve sucesso em %d tentativas com o laço ligado e ocioso: %v", tentativas, err)
	}
}

// TestCompactaUmaVezDuasChamadasConcorrentesUmaGanhaUmaRecebeBusy é a outra
// metade do critério: duas chamadas MANUAIS concorrentes, sem laço nenhum de
// pé. Determinístico pelo mesmo gate que TestCloseCancelaEAguardaACompaction e
// TestSegundaPartidaDaCompactionNaoSobeSegundaGoroutine já usam — SetAposentadoriaTraceForTest
// trava a primeira chamada NO MEIO da aposentadoria do primeiro candidato, e é
// só depois de confirmar (por canal fechado, nunca por sleep) que ela está em
// voo que a segunda chamada roda — de forma síncrona, porque CompactaUmaVez
// não enfileira: ou ela vê a vaga livre na hora, ou devolve ErrCompactionBusy
// na hora.
func TestCompactaUmaVezDuasChamadasConcorrentesUmaGanhaUmaRecebeBusy(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var umaVez sync.Once
	s.SetAposentadoriaTraceForTest(func(string) {
		umaVez.Do(func() {
			close(emVoo)
			<-libera
		})
	})

	primeira := make(chan error, 1)
	go func() {
		_, err := s.CompactaUmaVez(context.Background(), 0.5)
		primeira <- err
	}()

	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("a primeira chamada não chegou a aposentar nada em 60s: sem uma passada em voo a segunda não tem com o que colidir")
	}

	if _, err := s.CompactaUmaVez(context.Background(), 0.5); !errors.Is(err, ErrCompactionBusy) {
		t.Fatalf("segunda chamada concorrente: err = %v, quer ErrCompactionBusy", err)
	}

	liberar()
	select {
	case err := <-primeira:
		if err != nil {
			t.Fatalf("primeira chamada: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("a primeira chamada nunca retornou depois de liberada")
	}
}

// TestCompactaUmaVezCanceladoNoMeioNaoCorrompeReabreEContaRows é a garantia
// de segurança do brief: o contexto do CLIENTE HTTP (r.Context()) cancelado
// no meio de uma passada não pode corromper o dataset. O cancelamento chega
// ENTRE dois candidatos — o de dentro (aposentaSegmento) não observa ctx e
// sempre termina o candidato corrente por inteiro, e é só na volta do laço
// que candidatos() é revisitado que o ctx.Err() interrompe o resto. O
// oráculo é reabrir o dataset (recovery de verdade) e contar rows: qualquer
// corrupção do meio do caminho apareceria como Open recusado ou row perdida.
func TestCompactaUmaVezCanceladoNoMeioNaoCorrompeReabreEContaRows(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	eleitos := s.candidatos(0.5)
	if len(eleitos) < 2 {
		t.Fatalf("só %d candidato(s): cancelamento ENTRE candidatos não é observável com menos de dois", len(eleitos))
	}

	ctx, cancela := context.WithCancel(context.Background())
	var umaVez sync.Once
	s.SetAposentadoriaTraceForTest(func(string) {
		umaVez.Do(cancela) // cancela o ctx DA CHAMADA no meio da 1ª aposentadoria
	})

	res, err := s.CompactaUmaVez(ctx, 0.5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, quer context.Canceled: o teste cancelou o contexto no meio da passada", err)
	}
	if res.SegmentosAposentados == 0 || res.SegmentosAposentados >= len(eleitos) {
		t.Fatalf("SegmentosAposentados = %d (de %d eleitos): quer ALGUNS aposentados (o candidato em voo terminou por "+
			"inteiro antes do cancelamento pegar) e NEM TODOS (senão o cancelamento não interrompeu nada)",
			res.SegmentosAposentados, len(eleitos))
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reaberto := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	exigeCorpusNaUltimaRodada(t, reaberto, keys, rodadas-1, "após cancelamento no meio do CompactaUmaVez")
}

// TestCloseEsperaAPassadaManualEmVooAntesDeFechar é o lado (a) do Important
// do round 2 da revisão da B5: uma chamada manual CLAIMADA (já passou por
// tentaClaimarPassada, compEmVoo já sabe disso) tem de segurar o Close até
// terminar — nunca o contrário. É o MESMO teste de
// TestCloseCancelaEAguardaACompaction, mas para o caminho MANUAL em vez do
// laço automático: aquele já provava que o Add/Done do LAÇO protege a
// janela; este prova que o Add/Done (agora atômico com o claim, dentro de
// tentaClaimarPassada) protege a janela também para CompactaUmaVez.
func TestCloseEsperaAPassadaManualEmVooAntesDeFechar(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, SegmentSize: 8 << 10})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var umaVez sync.Once
	s.SetAposentadoriaTraceForTest(func(string) {
		umaVez.Do(func() {
			close(emVoo)
			<-libera
		})
	})

	manual := make(chan error, 1)
	go func() {
		_, err := s.CompactaUmaVez(context.Background(), 0.5)
		manual <- err
	}()

	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("a chamada manual não chegou a aposentar nada em 60s: sem uma passada em voo o teste não exercita nada")
	}

	fechado := make(chan error, 1)
	go func() { fechado <- s.Close() }()

	select {
	case err := <-fechado:
		t.Fatalf("Close retornou (%v) com a chamada MANUAL parada NO MEIO de uma aposentadoria: "+
			"o Add/Done de compEmVoo não protegeu a janela, e o que vem depois dele é rename/unlink de "+
			"arquivo sobre um Store já fechado", err)
	case <-time.After(500 * time.Millisecond):
	}

	liberar()
	select {
	case err := <-fechado:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Close não retornou depois de liberado: a chamada manual não foi aguardada nem cancelada")
	}
	if err := <-manual; err != nil {
		t.Fatalf("chamada manual: %v", err)
	}
}

// TestCompactaUmaVezRecusaPermanentementeDepoisDeParaCompaction é o lado (b)
// do Important do round 2: uma vez que ParaCompaction marca compEncerrando
// (mesmo sem nunca ter havido laço nenhum — ela é idempotente), NENHUMA
// chamada manual nova consegue reivindicar a vaga — a recusa é limpa
// (ErrCompactionBusy), nunca uma tentativa de tocar arquivo já em vias de
// ser fechado. Determinístico sem gate nenhum: compEncerrando é permanente
// (godoc do campo, store.go), então a ordem "ParaCompaction, depois
// CompactaUmaVez" já basta — não há corrida a decidir.
func TestCompactaUmaVezRecusaPermanentementeDepoisDeParaCompaction(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	s.ParaCompaction() // idempotente, nunca houve laço — mas marca compEncerrando

	if _, err := s.CompactaUmaVez(context.Background(), 0.5); !errors.Is(err, ErrCompactionBusy) {
		t.Fatalf("CompactaUmaVez depois de ParaCompaction: err = %v, quer ErrCompactionBusy "+
			"(compEncerrando é permanente — nunca reabre a vaga)", err)
	}
}
