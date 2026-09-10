package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
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

	snap := s.OpenSnapshot()

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

	snap := s.OpenSnapshot()

	// A linha vence DEPOIS de o snapshot ser tomado.
	rel.avanca(4 * time.Minute)

	if _, _, ok, err := s.GetByRowID(snap, id, nil); err != nil || !ok {
		t.Errorf("GetByRowID sob o snapshot: ok=%v err=%v; a linha viva em T0 sumiu porque o fetch consultou o relógio do PRESENTE", ok, err)
	}

	// A contraprova: sob um snapshot tomado AGORA, ela está mesmo vencida — sem
	// isto o teste acima passaria com um GetByRowID que ignorasse a expiração.
	if _, _, ok, _ := s.GetByRowID(s.OpenSnapshot(), id, nil); ok {
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

	snap := s.OpenSnapshot()
	rel.avanca(4 * time.Minute)

	if _, _, ok, err := s.GetByKey(snap, []byte("A"), nil); err != nil || !ok {
		t.Errorf("GetByKey sob o snapshot: ok=%v err=%v; a linha viva em T0 sumiu do point lookup", ok, err)
	}
	if _, _, ok, _ := s.GetByKey(s.OpenSnapshot(), []byte("A"), nil); ok {
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

	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)

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
	rec2, _, ok, err := s.GetByRowID(s.OpenSnapshot(), id, nil)
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

	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)

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

	snap := s.OpenSnapshot()
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "Stop", 2), nil)
	if got := s.vers.Entries(); got != 1 {
		t.Fatalf("Entries() = %d, quero 1: a atualização sob leitor vivo não guardou a versão anterior", got)
	}

	s.CloseSnapshot(snap)
	if got := s.vers.Entries(); got != 0 {
		t.Errorf("Entries() = %d, quero 0: o journal não voltou quando o último leitor fechou", got)
	}
}

// Sem leitor nenhum, a ingestão não paga NADA pelo isolamento: nenhuma entrada
// é criada. É o que separa "o dataset suporta consulta longa" de "o dataset
// guarda o passado o tempo todo por via das dúvidas".
func TestSemConsultaAbertaAIngestaoNaoGuardaVersaoNenhuma(t *testing.T) {
	s := openStore(t, Options{})
	mustPut(t, s, "A", 1, statusPayload(t, s, "A", "Start", 1), nil)
	mustPut(t, s, "A", 2, statusPayload(t, s, "A", "Stop", 2), nil)

	if got := s.vers.Entries(); got != 0 {
		t.Errorf("Entries() = %d, quero 0: o writer guardou versão sem ninguém para lê-la", got)
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
	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)
	corpusDeSegmentosSelados(t, s, keys, 1)

	comLeitor := s.candidatos(0.5)
	for _, seg := range comLeitor {
		if s.vers.Pinned(seg) {
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

	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)
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
	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)
	antes := s.vers.Entries()

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: sem passada de verdade o teste não exercita o copy-forward")
	}
	if got := s.vers.Entries(); got != antes {
		t.Errorf("Entries() = %d, quero %d: o copy-forward criou versão e vai pinar o segmento que ele mesmo aposenta", got, antes)
	}
}

// A COMPACTION NÃO PODE APAGAR O PAYLOAD QUE UMA CONSULTA AINDA VÊ.
//
// A descoberta oportunista da expiração testa o prazo com o relógio ATUAL e,
// se venceu, grava a lápide no destino em vez de copiar o payload. Sob um
// snapshot com o relógio congelado a linha ainda é visível — e o payload dela
// morreria ali, sem nenhuma entrada de journal que o recuperasse: o
// copy-forward não versiona, de propósito.
//
// A saída é a compaction usar o instante do snapshot mais ANTIGO vivo. A linha
// é copiada normalmente, o leitor a alcança pela location nova, e nada precisa
// ser pinado.
func TestCompactionNaoLapidaLinhaVisivelAoSnapshotMaisAntigo(t *testing.T) {
	// SegmentSize pequeno é o que faz a rotação acontecer e a eleição ter o
	// que escolher: com o padrão, o corpus inteiro cabe no segmento corrente e
	// a compaction não elege nada — a D6 nunca elege o corrente.
	s := openStore(t, Options{
		Dir: t.TempDir(), SegmentSize: 8 << 10,
		ActiveTTL: true, FallbackDuration: time.Hour,
		WriteInstantFieldName: "_writtenAt", TTLFieldName: "_expiresIn",
	})
	rel := &relogioDeTeste{t: time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)

	escreve := func(k string, seq int64) {
		t.Helper()
		p := payloadOrdenado(t, s, map[string]types.Value{
			"id":         {Type: types.TypeString, Bytes: []byte(k)},
			"status":     {Type: types.TypeString, Bytes: []byte(k + "-payload-de-tamanho-razoavel-para-encher-segmento")},
			"_writtenAt": {Type: types.TypeTimestamp, I64: rel.t.UnixMilli()},
		})
		rec, _, pres, err := s.State([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		var prev *types.Record
		if pres.Exists() {
			prev = &rec
		}
		if _, _, err := s.Put([]byte(k), seq, 0, p, prev); err != nil {
			t.Fatal(err)
		}
	}

	escreve("abandonada", 1)
	ativas := make([]string, 20)
	for i := range ativas {
		ativas[i] = "ativa-" + string(rune('a'+i))
	}
	rodada := int64(1)
	for r := 0; r < 15; r++ {
		for _, k := range ativas {
			escreve(k, rodada)
		}
		rodada++
	}

	// A consulta abre com TUDO vivo. É este instante que a compaction tem de
	// respeitar.
	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)

	rel.avanca(2 * time.Hour) // a abandonada vence DEPOIS do snapshot
	for r := 0; r < 15; r++ {
		for _, k := range ativas {
			escreve(k, rodada)
		}
		rodada++
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if len(s.candidatos(0.5)) == 0 {
		t.Fatal("nenhum segmento candidato: o cenário não exercita a descoberta oportunista")
	}
	if _, err := s.CompactaUmaVez(context.Background(), 0.5); err != nil {
		t.Fatal(err)
	}

	// A linha continua legível pelo snapshot, COM payload: uma lápide teria
	// levado o campo embora.
	rec, _, ok, err := s.GetByKey(snap, []byte("abandonada"), nil)
	if err != nil || !ok {
		t.Fatalf("a linha sumiu da consulta em voo: ok=%v err=%v", ok, err)
	}
	if got := statusDoPayload(t, s, rec.Payload); got == "" {
		t.Error("a linha voltou sem o campo status: a compaction lapidou o que a consulta ainda vê")
	}
}

// A RETENÇÃO NÃO PODE ABRIR BURACO POR BAIXO de uma consulta em voo.
//
// O snapshot congela o piso, mas congelar o piso sozinho não bastaria: a purga
// não só move a marca-d'água, ela APAGA os segmentos cobertos por ela. Um
// leitor com o piso antigo passaria de "não vê a linha" para "erra ao lê-la",
// que é pior. A marca só avança até onde o snapshot mais antigo já não olha.
func TestRetencaoNaoUltrapassaOPisoDoSnapshotVivo(t *testing.T) {
	_, hist, rel := parComRetencao(t, 1<<20)
	for range 50 {
		apenda(t, hist, "a", rel.t)
		rel.avanca(time.Minute)
	}

	// A consulta abre com as 50 linhas vivas: o piso dela é o rowId zero.
	snap := hist.OpenSnapshot()
	defer hist.CloseSnapshot(snap)

	// A mesma passada que, sem leitor, avançaria a marca em 20.
	res := passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: 30 * time.Minute})
	if res.MarcaAvancou != 0 {
		t.Errorf("a marca avançou %d com uma consulta cujo piso é o rowId 0: a purga passou por baixo dela", res.MarcaAvancou)
	}
	if n := contaVivas(t, hist); n != 50 {
		t.Errorf("%d linhas vivas, quero 50: a purga tirou do dataset linhas que a consulta ainda alcança", n)
	}

	// A contraprova: fechada a consulta, a mesma passada avança normalmente —
	// sem ela, este teste passaria com uma retenção que nunca avança.
	hist.CloseSnapshot(snap)
	res = passada(t, hist, LimitesDeRetencao{ExpireAfterWrite: 30 * time.Minute})
	if res.MarcaAvancou != 20 {
		t.Errorf("a marca avançou %d depois de a consulta fechar, quero 20: a retenção não recuperou o alcance", res.MarcaAvancou)
	}
}

// O TETO DO JOURNAL, e o que acontece quando ele estoura.
//
// O isolamento retém memória proporcional a (taxa de escrita × duração da
// consulta mais longa viva). Sem teto, uma consulta longa esquecida derruba o
// processo — e derrubar o processo é pior que qualquer resposta.
//
// A escolha, quando estoura, é entre parar a INGESTÃO e recusar a CONSULTA. É a
// consulta que perde: a ingestão é o que o dataset existe para fazer, e uma
// consulta recusada com código próprio é um resultado que o cliente sabe
// tratar. Recusar em silêncio, devolvendo a versão corrente, seria o único
// desfecho inaceitável — o cliente receberia um resultado que não corresponde a
// instante nenhum sem nunca saber disso.
func TestSnapshotQueEstouraOTetoEhInvalidadoEAIngestaoContinua(t *testing.T) {
	s := openStore(t, Options{SnapshotMaxBytes: 48}) // duas versões cabem
	id, _ := mustPut(t, s, "A", 1, statusPayload(t, s, "A", "T0", 1), nil)

	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)

	// Cada atualização retém uma versão. A quarta estoura o teto.
	for seq := int64(2); seq <= 5; seq++ {
		if _, _, err := s.Put([]byte("A"), seq, 0, statusPayload(t, s, "A", "v", seq), nil); err != nil {
			t.Fatalf("a ingestão PAROU por causa de uma consulta (seq %d): %v", seq, err)
		}
	}

	if _, _, _, err := s.GetByRowID(snap, id, nil); !errors.Is(err, ErrExpiredSnapshot) {
		t.Errorf("GetByRowID = %v, quero ErrSnapshotExpirado: o snapshot invalidado respondeu como se ainda valesse", err)
	}

	// A contraprova: uma consulta nova continua funcionando. Sem ela, um Store
	// que passasse a recusar TUDO passaria na asserção acima.
	if _, _, ok, err := s.GetByRowID(s.presente(), id, nil); err != nil || !ok {
		t.Errorf("consulta nova: ok=%v err=%v; o teto derrubou o dataset inteiro, não só o snapshot velho", ok, err)
	}
}
