package storage

import (
	"context"
	"errors"
	"sync"
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

// TestCompactaUmaVezComOLacoAutomaticoDePeDevolveBusy é a metade "laço em
// voo + POST" do critério de 409 concorrente do brief, e é determinística sem
// nenhum gate: IniciaCompaction grava compCancela de forma SÍNCRONA, antes de
// devolver o controle ao chamador — a goroutine ainda nem pode ter rodado um
// passo, e mesmo assim CompactaUmaVez já vê o laço como "de pé" e recusa. É a
// leitura conservadora da restrição estrutural: enquanto o laço existe, ele
// pode acordar do cochilo a qualquer instante e colidir com uma varredura
// manual, então NENHUMA janela do laço (nem dormindo) é tratada como livre.
func TestCompactaUmaVezComOLacoAutomaticoDePeDevolveBusy(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	s.IniciaCompaction(context.Background(), 0.5)
	defer s.ParaCompaction()

	if _, err := s.CompactaUmaVez(context.Background(), 0.5); !errors.Is(err, ErrCompactionBusy) {
		t.Fatalf("CompactaUmaVez com o laço automático de pé: err = %v, quer ErrCompactionBusy", err)
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
