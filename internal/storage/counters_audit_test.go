package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func keyOfServico(i int) string { return fmt.Sprintf("c-%04d", i) }

// relatorioOf devolve o relatório do contador nomeado.
func relatorioOf(t *testing.T, rs []counter.Report, name string) counter.Report {
	t.Helper()
	for _, r := range rs {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("relatório de %q não veio: %+v", name, rs)
	return counter.Report{}
}

// TestAuditoriaSemDivergenciaComIngestaoConcorrente prova que o par (snapshot,
// cópia T0 dos totais) é consistente: com Puts mudando de grupo e de condição,
// e lápides sendo gravadas DURANTE a varredura, a auditoria nunca reporta
// divergência.
func TestAuditoriaSemDivergenciaComIngestaoConcorrente(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true), byOLT("serv", false))
	const n = 300
	for i := 0; i < n; i++ {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	pare := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := int64(2)
		for i := 0; ; i++ {
			select {
			case <-pare:
				return
			default:
			}
			k := keyOfServico(i % n)
			prev, _, ok, err := s.GetByKey(s.presente(), []byte(k), nil)
			if err != nil {
				t.Error(err)
				return
			}
			var pprev *types.Record
			if ok {
				pprev = &prev
			}
			switch i % 5 {
			case 0:
				_, _, err = s.Put([]byte(k), seq, 0, payloadServico(t, s, k, "B", true, rel.agora()), pprev)
			case 1:
				_, _, err = s.Put([]byte(k), seq, 0, payloadServico(t, s, k, "A", false, rel.agora()), pprev)
			case 2:
				if ok {
					_, _, err = s.Put([]byte(k), seq, types.FlagTombstone, payloadServico(t, s, k, "", false, rel.agora()), pprev)
				}
			default:
				_, _, err = s.Put([]byte(k), seq, 0, payloadServico(t, s, k, "A", true, rel.agora()), pprev)
			}
			if err != nil {
				t.Error(err)
				return
			}
			seq++
		}
	}()
	for rodada := 0; rodada < 20; rodada++ {
		rs, err := s.AuditaCounters(context.Background())
		if err != nil {
			close(pare)
			wg.Wait()
			t.Fatalf("rodada %d: %v", rodada, err)
		}
		for _, r := range rs {
			if r.DivergentGroups != 0 || r.NotInterned != 0 {
				close(pare)
				wg.Wait()
				t.Fatalf("rodada %d: %s divergiu com ingestão concorrente: %+v", rodada, r.Name, r)
			}
			if r.RowsScanned == 0 {
				close(pare)
				wg.Wait()
				t.Fatalf("rodada %d: a auditoria não varreu linha nenhuma", rodada)
			}
		}
	}
	close(pare)
	wg.Wait()
}

// TestAuditoriaCopiaOsTotaisNoMesmoInstanteDoSnapshot é a prova
// determinística do que o teste concorrente acima só cobre por sorte: a cópia
// dos totais e o snapshot são do MESMO instante, com o writer parado. O seam
// dispara um Put entre os dois; no código certo o Put espera o wmu e só entra
// depois da cópia, e a auditoria bate. Copiar fora do wmu deixaria o Put
// entrar no meio: o snapshot ainda mostraria a linha em A, e a cópia já a
// contaria em B.
func TestAuditoriaCopiaOsTotaisNoMesmoInstanteDoSnapshot(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	end := make(chan struct{})
	s.SetAuditBeforeOfCopyForTest(func() {
		go func() {
			defer close(end)
			k := keyOfServico(0)
			prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
			mustPut(t, s, k, 2, payloadServico(t, s, k, "B", true, rel.agora()), &prev)
		}()
		// Dá ao Put a chance de entrar. No código certo ele bloqueia no wmu,
		// que o seam segura; no mutante ele completa aqui.
		time.Sleep(50 * time.Millisecond)
	})
	rs, err := s.AuditaCounters(context.Background())
	<-end
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "aut")
	if r.DivergentGroups != 0 {
		t.Fatalf("a cópia dos totais não é do instante do snapshot: %+v", r)
	}
	// Os agregados vêm do MESMO retrato, tirado sob o mesmo wmu. Lidos depois
	// de soltar o writer, o Put que este seam deixa entrar os moveria e a
	// auditoria acusaria divergência onde só houve escrita concorrente.
	if r.DivergentAggregates != 0 {
		t.Fatalf("os agregados não vieram do instante do snapshot: %+v", r)
	}
}

// TestAuditoriaCongelaOsAgregadosNoMesmoInstante é a irmã do teste acima para
// os agregados, e a escrita concorrente é OUTRA de propósito.
//
// Lá a linha muda de grupo, o que move os dois grupos e deixa os agregados
// parados — com aquele Put, ler o agregado tarde daria o mesmo número e o
// teste passaria verde com o defeito presente. Aqui ela muda a CONDIÇÃO no
// mesmo grupo, que é a escrita que move o agregado sem mover as linhas: lido
// depois de soltar o writer, ele viria 9 contra um recalculado de 10, e a
// auditoria acusaria divergência onde só houve escrita concorrente.
func TestAuditoriaCongelaOsAgregadosNoMesmoInstante(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	end := make(chan struct{})
	s.SetAuditBeforeOfCopyForTest(func() {
		go func() {
			defer close(end)
			k := keyOfServico(0)
			prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
			mustPut(t, s, k, 2, payloadServico(t, s, k, "A", false, rel.agora()), &prev)
		}()
		// Dá ao Put a chance de entrar. No código certo ele bloqueia no wmu,
		// que o seam segura; no mutante ele completa aqui.
		time.Sleep(50 * time.Millisecond)
	})
	// A varredura de dez linhas é mais rápida que o Put: sem esperar por ele
	// aqui, o mutante que lê o agregado depois de soltar o writer leria o
	// número ANTIGO e passaria verde. O trace roda com o wmu já solto, então
	// esperar aqui não trava o Put — ele precisa exatamente desse lock.
	var oneVez sync.Once
	s.SetAuditTraceForTest(func(types.RowID) { oneVez.Do(func() { <-end }) })

	rs, err := s.AuditaCounters(context.Background())
	<-end
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "aut")
	if r.AggregateRows != 10 || r.AggregateTotal != 10 {
		t.Fatalf("os agregados não são do instante do snapshot: %+v", r)
	}
	if r.DivergentAggregates != 0 || r.DivergentGroups != 0 {
		t.Fatalf("escrita concorrente não é divergência: %+v", r)
	}
}

// TestAuditoriaContaLinhaVencidaSemLapide fixa a definição: "viva" para o
// contador é "com location e sem lápide". A linha cujo prazo venceu e que o
// coletor ainda não lapidou é invisível à consulta, mas continua contada — e
// a auditoria a conta também, senão reportaria divergência onde não há.
func TestAuditoriaContaLinhaVencidaSemLapide(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	rowID, _ := mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	rel.advance(2 * time.Hour)
	if _, _, ok, _ := s.GetByRowID(s.presente(), rowID, nil); ok {
		t.Fatal("a linha vencida não pode ser visível à leitura")
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rs, err := s.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "aut")
	if r.DivergentGroups != 0 || r.Maintained != 1 || r.Recomputed != 1 || r.RowsScanned != 1 {
		t.Fatalf("a linha vencida sem lápide tem de contar dos dois lados: %+v", r)
	}
}

func TestAuditoriaDetectaDeriva(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	var ultima types.RowID
	for i := 0; i < 10; i++ {
		k := keyOfServico(i)
		ultima, _ = mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	// Deriva injetada por FORA do Store: o contador passa a acreditar que a
	// última linha está em B, enquanto o record diz A.
	c := counterChamado(t, s, "aut")
	if err := c.Transition(ultima, payloadServico(t, s, "x", "B", true, rel.agora()), false); err != nil {
		t.Fatal(err)
	}
	rs, err := s.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "aut")
	if r.DivergentGroups != 2 {
		t.Fatalf("A e B tinham de divergir: %+v", r)
	}
	if r.Maintained != 10 || r.Recomputed != 10 {
		t.Fatalf("as somas batem por coincidência (uma linha trocou de grupo): %+v", r)
	}
	// Os grupos derivaram e o AGREGADO não: a linha continua sendo uma, e
	// continua satisfazendo a condição — só está no grupo errado. É esta
	// asserção que impede a conferência dos agregados de virar uma segunda
	// contagem da mesma coisa que Divergentes já conta.
	if r.DivergentAggregates != 0 {
		t.Fatalf("deriva de grupo não pode acusar o agregado: %+v", r)
	}
	if r.AggregateRows != 10 || r.AggregateTotal != 10 {
		t.Fatalf("agregados = linhas %d total %d, quer 10 e 10", r.AggregateRows, r.AggregateTotal)
	}
	nomes := map[string]counter.Divergence{}
	for _, d := range r.Sample {
		nomes[d.Text] = d
	}
	if a := nomes["A"]; a.Maintained != 9 || a.Recomputed != 10 || a.RefsMaintained != 9 || a.RefsRecomputed != 10 {
		t.Fatalf("A = %+v", a)
	}
	if b := nomes["B"]; b.Maintained != 1 || b.Recomputed != 0 {
		t.Fatalf("B = %+v", b)
	}
}

func TestReconstruirZeraADeriva(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	var ultima types.RowID
	for i := 0; i < 10; i++ {
		k := keyOfServico(i)
		ultima, _ = mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	c := counterChamado(t, s, "aut")
	if err := c.Transition(ultima, payloadServico(t, s, "x", "B", true, rel.agora()), false); err != nil {
		t.Fatal(err)
	}
	recs, err := s.ReconstroiCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Rows != 10 || recs[0].Name != "aut" {
		t.Fatalf("reconstrução = %+v", recs)
	}
	exigeTotal(t, s, "aut", "A", 10, 10)
	exigeTotal(t, s, "aut", "B", 0, 0)
	rs, err := s.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r := relatorioOf(t, rs, "aut"); r.DivergentGroups != 0 {
		t.Fatalf("depois de reconstruir: %+v", r)
	}
	// E o writer continua aceitando escrita.
	mustPut(t, s, "novo", 1, payloadServico(t, s, "novo", "A", true, rel.agora()), nil)
	exigeTotal(t, s, "aut", "A", 11, 11)
}

// TestAuditoriaInconclusivaQuandoOSnapshotEstoura: o journal de versões tem
// teto, e a auditoria segura um snapshot pela varredura inteira. Estourado, a
// resposta é ERRO — nunca um relatório com divergência inventada.
func TestAuditoriaInconclusivaQuandoOSnapshotEstoura(t *testing.T) {
	opts := optionsWithCounters(t.TempDir(), byOLT("aut", true))
	opts.SnapshotMaxBytes = 1
	s := openStore(t, opts)
	rel := &relogioOfTest{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	for i := 0; i < 50; i++ {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	// No meio da varredura, um update registra uma versão no journal — que
	// com o teto de 1 byte invalida o snapshot mais antigo: o da auditoria.
	s.SetAuditTraceForTest(func(row types.RowID) {
		if row != 10 {
			return
		}
		k := keyOfServico(0)
		prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
		mustPut(t, s, k, 2, payloadServico(t, s, k, "B", true, rel.agora()), &prev)
	})
	_, err := s.AuditaCounters(context.Background())
	if !errors.Is(err, ErrExpiredSnapshot) {
		t.Fatalf("a auditoria tinha de sair inconclusiva com ErrSnapshotExpirado, veio %v", err)
	}
}

func TestAuditoriaRespeitaOContexto(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	for i := 0; i < 50; i++ {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.AuditaCounters(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("contexto cancelado tem de interromper a auditoria: %v", err)
	}
	if _, err := s.ReconstroiCounters(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("contexto cancelado tem de interromper a reconstrução: %v", err)
	}
}

func TestAuditoriaSemContadoresEhVazia(t *testing.T) {
	s, _ := storeWithCounters(t, t.TempDir())
	rs, err := s.AuditaCounters(context.Background())
	if err != nil || len(rs) != 0 {
		t.Fatalf("sem contadores: %v, %v", rs, err)
	}
}

// TestAuditoriaAcusaOAgregadoContraORecalculado: o agregado é a SEGUNDA fonte
// para o mesmo fato que os vetores por grupo, e a auditoria é a única coisa
// que os reconcilia.
//
// A deriva injetada aqui é na CONDIÇÃO, e não no grupo: o contador passa a
// acreditar que a última linha não está autenticada, enquanto o record diz que
// está. Isso separa MANTIDO (9) de RECALCULADO (10) — que a deriva de grupo do
// teste acima não separava, porque lá a linha só mudava de lugar. É essa
// separação que prova contra QUEM o agregado é conferido: comparado com o
// mantido ele bateria, e a conferência não valeria nada.
func TestAuditoriaAcusaOAgregadoContraORecalculado(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLT("aut", true))
	var ultima types.RowID
	for i := 0; i < 10; i++ {
		k := keyOfServico(i)
		ultima, _ = mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	c := counterChamado(t, s, "aut")
	if err := c.Transition(ultima, payloadServico(t, s, "x", "A", false, rel.agora()), false); err != nil {
		t.Fatal(err)
	}

	rs, err := s.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "aut")
	if r.Maintained != 9 || r.Recomputed != 10 {
		t.Fatalf("a bancada não separa mantido de recalculado: %+v", r)
	}
	// As LINHAS não mudaram: a linha continua no grupo A, só deixou de contar
	// para a condição. Logo só o total do agregado pode divergir.
	if r.AggregateRows != 10 || r.RowsRecomputed != 10 {
		t.Fatalf("linhas: agregado %d, recalculado %d; quer 10 e 10", r.AggregateRows, r.RowsRecomputed)
	}
	if r.AggregateTotal != 9 {
		t.Fatalf("o agregado tem de carregar a deriva: total %d, quer 9", r.AggregateTotal)
	}
	if r.DivergentAggregates != 1 {
		t.Fatalf("o agregado discorda do recalculado e tem de ser acusado: %+v", r)
	}
	// A divergência do agregado alarma pela MESMA série que a dos grupos: uma
	// conferência que achasse e não registrasse não teria efeito nenhum.
	// Aqui são duas — o grupo A e o agregado.
	if est := c.Stats(); est.AuditDivergences != 2 {
		t.Fatalf("a auditoria registrou %d divergências, quer 2 (o grupo A e o agregado)", est.AuditDivergences)
	}
}
