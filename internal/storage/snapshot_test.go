package storage

import (
	"context"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
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

// ISOLAMENTO (C6): a consulta não vê apenas o mesmo CONJUNTO de linhas do
// começo ao fim — ela vê cada linha na versão que estava publicada em T0.
//
// Sem isso, um relatório de oito minutos mistura estados: as linhas que a
// varredura alcançou antes da atualização saem na versão velha e as de depois
// na nova, e o resultado não corresponde a instante nenhum do banco.
func TestLinhaAtualizadaDuranteAConsultaEhEntregueNaVersaoDeT0(t *testing.T) {
	s := openStore(t, Options{})
	id, _ := mustPut(t, s, "A", 1, statusPayload(t, s, "A", "Start", 1), nil)

	snap := s.AbreSnapshot()
	defer s.FechaSnapshot(snap)

	// A entidade é atualizada com a consulta em voo.
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "Stop", 2), nil)

	rec, _, ok, err := s.GetByRowID(snap, id, nil)
	if err != nil || !ok {
		t.Fatalf("GetByRowID sob o snapshot: ok=%v err=%v", ok, err)
	}
	if got := statusDoPayload(t, s, rec.Payload); got != "Start" {
		t.Errorf("a linha veio com status %q, quero \"Start\": a consulta recebeu a versão do PRESENTE, não a de T0", got)
	}

	// A contraprova: quem abre depois vê o estado novo. Sem ela, um GetByRowID
	// quebrado que devolvesse sempre a versão mais antiga passaria no teste.
	rec2, _, ok, err := s.GetByRowID(s.AbreSnapshot(), id, nil)
	if err != nil || !ok {
		t.Fatalf("GetByRowID no presente: ok=%v err=%v", ok, err)
	}
	if got := statusDoPayload(t, s, rec2.Payload); got != "Stop" {
		t.Errorf("a consulta aberta DEPOIS veio com status %q, quero \"Stop\": o snapshot está congelando o que não devia", got)
	}
}

// statusDoPayload extrai o campo `status` do record — o observável que separa
// a versão de T0 da versão corrente.
func statusDoPayload(t *testing.T, s *Store, payload []byte) string {
	t.Helper()
	id, err := s.FieldDict().LookupOrCreate([]byte("status"))
	if err != nil {
		t.Fatal(err)
	}
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		campo, v, ok := r.Next()
		if !ok {
			return ""
		}
		if campo == id {
			return string(v.Bytes)
		}
	}
}

// Duas atualizações na mesma linha durante a mesma consulta: o leitor tem de
// receber a versão de T0, e não a INTERMEDIÁRIA.
//
// É o caso que um teste de uma atualização só não distingue — com uma versão
// guardada, "a primeira que achar" e "a de menor época maior que a minha" dão
// a mesma resposta. Com duas, só a segunda regra acerta.
func TestDuasAtualizacoesNaMesmaLinhaEntregamAVersaoDeT0ENaoAIntermediaria(t *testing.T) {
	s := openStore(t, Options{})
	id, _ := mustPut(t, s, "A", 1, statusPayload(t, s, "A", "T0", 1), nil)

	snap := s.AbreSnapshot()
	defer s.FechaSnapshot(snap)

	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "intermediaria", 2), nil)
	mustPut(t, s, "A", 3, statusPayload(t, s, "A", "corrente", 3), nil)

	rec, _, ok, err := s.GetByRowID(snap, id, nil)
	if err != nil || !ok {
		t.Fatalf("GetByRowID sob o snapshot: ok=%v err=%v", ok, err)
	}
	if got := statusDoPayload(t, s, rec.Payload); got != "T0" {
		t.Errorf("a linha veio com status %q, quero \"T0\": a busca no journal devolveu a versão errada da cadeia", got)
	}
}

// O journal é memória, e memória que não volta é vazamento. Ele volta quando o
// último leitor de uma época fecha — e é o Close do iterador do executor que
// dispara isso em produção.
func TestSnapshotFechadoDevolveOJournalAoWriter(t *testing.T) {
	s := openStore(t, Options{})
	mustPut(t, s, "A", 1, statusPayload(t, s, "A", "Start", 1), nil)

	snap := s.AbreSnapshot()
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "Stop", 2), nil)
	if got := s.vers.Entradas(); got != 1 {
		t.Fatalf("Entradas() = %d, quero 1: a atualização sob leitor vivo não guardou a versão anterior", got)
	}

	s.FechaSnapshot(snap)
	if got := s.vers.Entradas(); got != 0 {
		t.Errorf("Entradas() = %d, quero 0: o journal não voltou quando o último leitor fechou", got)
	}
}

// Sem leitor nenhum, a ingestão não paga NADA pelo isolamento: nenhuma entrada
// é criada. É o que separa "o dataset suporta consulta longa" de "o dataset
// guarda o passado o tempo todo por via das dúvidas".
func TestSemConsultaAbertaAIngestaoNaoGuardaVersaoNenhuma(t *testing.T) {
	s := openStore(t, Options{})
	mustPut(t, s, "A", 1, statusPayload(t, s, "A", "Start", 1), nil)
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "Stop", 2), nil)

	if got := s.vers.Entradas(); got != 0 {
		t.Errorf("Entradas() = %d, quero 0: o writer guardou versão sem ninguém para lê-la", got)
	}
}

// O PIN: a compaction não pode aposentar um segmento onde mora uma versão que
// uma consulta aberta ainda alcança.
//
// Sem ele o isolamento seria uma promessa que o disco desmente: o journal
// guarda o ENDEREÇO da versão anterior, e um segmento aposentado leva os bytes
// desse endereço junto. A consulta longa passaria de "vê o passado" para "erra
// ao ler", que é pior que o defeito original.
func TestSegmentoComVersaoRetidaNaoEhEleitoPelaCompaction(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	// A eleição SEM leitor nenhum é a linha de base: sem ela, "nenhum
	// candidato" não distingue o pin funcionando de um corpus que nunca
	// produziu candidato.
	semLeitor := s.candidatos(0.5)
	if len(semLeitor) == 0 {
		t.Fatal("nenhum candidato com 30 rodadas sobre 40 keys: sem eleição não há o que este teste exercite")
	}

	// Com a consulta aberta, uma atualização retém a versão anterior — que
	// mora num dos segmentos que a eleição acabou de escolher.
	snap := s.AbreSnapshot()
	defer s.FechaSnapshot(snap)
	corpusDeSegmentosSelados(t, s, keys, 1)

	comLeitor := s.candidatos(0.5)
	for _, seg := range comLeitor {
		if s.vers.Pinado(seg) {
			t.Errorf("o segmento %d guarda versão retida e foi eleito para aposentadoria", seg)
		}
	}
	if len(comLeitor) == len(semLeitor) {
		t.Error("a eleição não mudou com um leitor vivo: nenhum segmento ficou pinado, então o teste acima passou por vacuidade")
	}
}

// O point lookup também entrega a versão de T0. Sem isso, `WHERE id = ?` e
// `WHERE status = ?` responderiam sobre estados diferentes na MESMA consulta —
// e o plano escolhido pelo planner decidiria qual estado o usuário vê.
func TestPointLookupEntregaAVersaoDeT0(t *testing.T) {
	s := openStore(t, Options{})
	mustPut(t, s, "A", 1, statusPayload(t, s, "A", "T0", 1), nil)

	snap := s.AbreSnapshot()
	defer s.FechaSnapshot(snap)
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "corrente", 2), nil)

	rec, _, ok, err := s.GetByKey(snap, []byte("A"), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey sob o snapshot: ok=%v err=%v", ok, err)
	}
	if got := statusDoPayload(t, s, rec.Payload); got != "T0" {
		t.Errorf("o point lookup veio com status %q, quero \"T0\": ele resolve pela primary e ignorou o journal", got)
	}
}

// O COPY-FORWARD NÃO VERSIONA, e este é o teste que segura o achado que
// estrutura o desenho.
//
// A compaction publica location nova com hadPrevious, exatamente como o Put —
// mas ela só MOVE bytes: o conteúdo é idêntico, e o leitor com snapshot deve
// seguir a location nova. Se ela versionasse, cada passada criaria versões
// apontando para o segmento que ela está aposentando, o pin bloquearia a
// própria passada que as gerou, e a compaction pararia de devolver disco
// enquanto houvesse UMA consulta aberta.
func TestCopyForwardNaoCriaVersaoENaoTravaAPropriaPassada(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	// Uma consulta aberta ANTES da passada: é a condição que faria o
	// copy-forward versionar, se ele versionasse.
	snap := s.AbreSnapshot()
	defer s.FechaSnapshot(snap)
	antes := s.vers.Entradas()

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: sem passada de verdade o teste não exercita o copy-forward")
	}
	if got := s.vers.Entradas(); got != antes {
		t.Errorf("Entradas() = %d, quero %d: o copy-forward criou versão e vai pinar o segmento que ele mesmo aposenta", got, antes)
	}
}
