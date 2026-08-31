package storage

import (
	"testing"
	"time"
)

// A varredura tem de enxergar o universo do instante em que COMEÇOU.
//
// Sem o relógio congelado, prazoVencido consulta s.agora() a cada row testada
// (store.go), e uma linha viva quando a consulta abriu desaparece do MEIO do
// resultado quando a varredura a alcança. É a primeira metade do débito 94, e
// com o query.maxTimeout da consulta longa a janela deixou de ser estreita:
// ela passou a ser a duração inteira de um relatório.
func TestRowQueVenceNoMeioDaVarreduraContinuaVisivelAteOFimDaQuery(t *testing.T) {
	s, rel := storeComRegras(t, t.TempDir(), 0)
	// Duas linhas com o MESMO prazo. A varredura passa pela primeira antes de
	// o relógio andar e alcança a segunda depois — que é exatamente a forma do
	// defeito: não é a consulta inteira que falha, é o meio dela.
	mustPut(t, s, "A", 1, payloadComPrazo(t, s, "A", rel.t, 3*time.Minute), nil)
	mustPut(t, s, "B", 1, payloadComPrazo(t, s, "B", rel.t, 3*time.Minute), nil)

	snap := s.AbreSnapshot()

	it := s.ScanLive(snap)
	defer func() { _ = it.Close() }()
	if !it.Next() {
		t.Fatal("a varredura não devolveu nem a primeira linha")
	}
	vistas := 1

	// O relógio anda DEPOIS de o snapshot ser tomado: as duas linhas vencem
	// agora, mas nenhuma delas tinha vencido em T0.
	rel.avanca(4 * time.Minute)

	for it.Next() {
		vistas++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if vistas != 2 {
		t.Errorf("a varredura devolveu %d linhas, quero 2: a linha que venceu DEPOIS do snapshot sumiu do meio do resultado", vistas)
	}
}

// O relógio congelado não pode valer só para a VARREDURA: quem decide a
// liveness de cada candidato é o FETCH (§44), e é por ele que passam os quatro
// planos indexados — bitmap, hash, interseção e união. Um teto no iterador com
// um relógio corrente no GetByRowID deixaria o plano indexado com a metade da
// garantia: o conjunto de candidatos seria estável e o resultado não.
func TestGetByRowIDUsaORelogioDoSnapshotENaoODoPresente(t *testing.T) {
	s, rel := storeComRegras(t, t.TempDir(), 0)
	id, _ := mustPut(t, s, "A", 1, payloadComPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	snap := s.AbreSnapshot()

	// A linha vence DEPOIS de o snapshot ser tomado.
	rel.avanca(4 * time.Minute)

	if _, _, ok, err := s.GetByRowID(snap, id, nil); err != nil || !ok {
		t.Errorf("GetByRowID sob o snapshot: ok=%v err=%v; a linha viva em T0 sumiu porque o fetch consultou o relógio do PRESENTE", ok, err)
	}

	// A contraprova: sob um snapshot tomado AGORA, ela está mesmo vencida — sem
	// isto o teste acima passaria com um GetByRowID que ignorasse a expiração.
	if _, _, ok, _ := s.GetByRowID(s.AbreSnapshot(), id, nil); ok {
		t.Error("sob um snapshot do presente a linha continua visível: o teste acima não distingue congelar o relógio de não olhá-lo")
	}
}

// O point lookup é o QUARTO ponto de liveness, e o mais fácil de esquecer: ele
// não passa nem pela varredura nem pelo GetByRowID — o keySource do executor
// resolve a key direto, para não pagar a leitura dupla. Um snapshot que valesse
// nos três primeiros e não neste deixaria `WHERE id = ?` respondendo sobre um
// instante diferente do de `WHERE status = ?` na mesma consulta.
func TestGetByKeyUsaORelogioDoSnapshotENaoODoPresente(t *testing.T) {
	s, rel := storeComRegras(t, t.TempDir(), 0)
	mustPut(t, s, "A", 1, payloadComPrazo(t, s, "A", rel.t, 3*time.Minute), nil)

	snap := s.AbreSnapshot()
	rel.avanca(4 * time.Minute)

	if _, _, ok, err := s.GetByKey(snap, []byte("A"), nil); err != nil || !ok {
		t.Errorf("GetByKey sob o snapshot: ok=%v err=%v; a linha viva em T0 sumiu do point lookup", ok, err)
	}
	if _, _, ok, _ := s.GetByKey(s.AbreSnapshot(), []byte("A"), nil); ok {
		t.Error("sob um snapshot do presente a linha continua visível: o teste acima não distingue congelar o relógio de não olhá-lo")
	}
}
