package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/contador"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func chaveDeServico(i int) string { return fmt.Sprintf("c-%04d", i) }

// relatorioDe devolve o relatório do contador nomeado.
func relatorioDe(t *testing.T, rs []contador.Relatorio, nome string) contador.Relatorio {
	t.Helper()
	for _, r := range rs {
		if r.Nome == nome {
			return r
		}
	}
	t.Fatalf("relatório de %q não veio: %+v", nome, rs)
	return contador.Relatorio{}
}

// TestAuditoriaSemDivergenciaComIngestaoConcorrente prova que o par (snapshot,
// cópia T0 dos totais) é consistente: com Puts mudando de grupo e de condição,
// e lápides sendo gravadas DURANTE a varredura, a auditoria nunca reporta
// divergência.
func TestAuditoriaSemDivergenciaComIngestaoConcorrente(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true), porOLT("serv", false))
	const n = 300
	for i := 0; i < n; i++ {
		k := chaveDeServico(i)
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
			k := chaveDeServico(i % n)
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
		rs, err := s.AuditaContadores(context.Background())
		if err != nil {
			close(pare)
			wg.Wait()
			t.Fatalf("rodada %d: %v", rodada, err)
		}
		for _, r := range rs {
			if r.Divergentes != 0 || r.NaoInternadas != 0 {
				close(pare)
				wg.Wait()
				t.Fatalf("rodada %d: %s divergiu com ingestão concorrente: %+v", rodada, r.Nome, r)
			}
			if r.LinhasVarridas == 0 {
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
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := chaveDeServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	fim := make(chan struct{})
	s.SetAuditoriaAntesDaCopiaForTest(func() {
		go func() {
			defer close(fim)
			k := chaveDeServico(0)
			prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
			mustPut(t, s, k, 2, payloadServico(t, s, k, "B", true, rel.agora()), &prev)
		}()
		// Dá ao Put a chance de entrar. No código certo ele bloqueia no wmu,
		// que o seam segura; no mutante ele completa aqui.
		time.Sleep(50 * time.Millisecond)
	})
	rs, err := s.AuditaContadores(context.Background())
	<-fim
	if err != nil {
		t.Fatal(err)
	}
	if r := relatorioDe(t, rs, "aut"); r.Divergentes != 0 {
		t.Fatalf("a cópia dos totais não é do instante do snapshot: %+v", r)
	}
}

// TestAuditoriaContaLinhaVencidaSemLapide fixa a definição: "viva" para o
// contador é "com location e sem lápide". A linha cujo prazo venceu e que o
// coletor ainda não lapidou é invisível à consulta, mas continua contada — e
// a auditoria a conta também, senão reportaria divergência onde não há.
func TestAuditoriaContaLinhaVencidaSemLapide(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true))
	rowID, _ := mustPut(t, s, "k", 1, payloadServico(t, s, "k", "A", true, rel.agora()), nil)
	rel.avanca(2 * time.Hour)
	if _, _, ok, _ := s.GetByRowID(s.presente(), rowID, nil); ok {
		t.Fatal("a linha vencida não pode ser visível à leitura")
	}
	exigeTotal(t, s, "aut", "A", 1, 1)
	rs, err := s.AuditaContadores(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioDe(t, rs, "aut")
	if r.Divergentes != 0 || r.Mantido != 1 || r.Recalculado != 1 || r.LinhasVarridas != 1 {
		t.Fatalf("a linha vencida sem lápide tem de contar dos dois lados: %+v", r)
	}
}

func TestAuditoriaDetectaDeriva(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true))
	var ultima types.RowID
	for i := 0; i < 10; i++ {
		k := chaveDeServico(i)
		ultima, _ = mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	// Deriva injetada por FORA do Store: o contador passa a acreditar que a
	// última linha está em B, enquanto o record diz A.
	c := contadorChamado(t, s, "aut")
	if err := c.Transiciona(ultima, payloadServico(t, s, "x", "B", true, rel.agora()), false); err != nil {
		t.Fatal(err)
	}
	rs, err := s.AuditaContadores(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioDe(t, rs, "aut")
	if r.Divergentes != 2 {
		t.Fatalf("A e B tinham de divergir: %+v", r)
	}
	if r.Mantido != 10 || r.Recalculado != 10 {
		t.Fatalf("as somas batem por coincidência (uma linha trocou de grupo): %+v", r)
	}
	nomes := map[string]contador.Divergencia{}
	for _, d := range r.Amostra {
		nomes[d.Texto] = d
	}
	if a := nomes["A"]; a.Mantido != 9 || a.Recalculado != 10 || a.RefsMantido != 9 || a.RefsRecalculado != 10 {
		t.Fatalf("A = %+v", a)
	}
	if b := nomes["B"]; b.Mantido != 1 || b.Recalculado != 0 {
		t.Fatalf("B = %+v", b)
	}
}

func TestReconstruirZeraADeriva(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true))
	var ultima types.RowID
	for i := 0; i < 10; i++ {
		k := chaveDeServico(i)
		ultima, _ = mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	c := contadorChamado(t, s, "aut")
	if err := c.Transiciona(ultima, payloadServico(t, s, "x", "B", true, rel.agora()), false); err != nil {
		t.Fatal(err)
	}
	recs, err := s.ReconstroiContadores(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Linhas != 10 || recs[0].Nome != "aut" {
		t.Fatalf("reconstrução = %+v", recs)
	}
	exigeTotal(t, s, "aut", "A", 10, 10)
	exigeTotal(t, s, "aut", "B", 0, 0)
	rs, err := s.AuditaContadores(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r := relatorioDe(t, rs, "aut"); r.Divergentes != 0 {
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
	opts := opcoesComContadores(t.TempDir(), porOLT("aut", true))
	opts.SnapshotMaxBytes = 1
	s := openStore(t, opts)
	rel := &relogioDeTeste{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	for i := 0; i < 50; i++ {
		k := chaveDeServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	// No meio da varredura, um update registra uma versão no journal — que
	// com o teto de 1 byte invalida o snapshot mais antigo: o da auditoria.
	s.SetAuditoriaTraceForTest(func(linha types.RowID) {
		if linha != 10 {
			return
		}
		k := chaveDeServico(0)
		prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
		mustPut(t, s, k, 2, payloadServico(t, s, k, "B", true, rel.agora()), &prev)
	})
	_, err := s.AuditaContadores(context.Background())
	if !errors.Is(err, ErrSnapshotExpirado) {
		t.Fatalf("a auditoria tinha de sair inconclusiva com ErrSnapshotExpirado, veio %v", err)
	}
}

func TestAuditoriaRespeitaOContexto(t *testing.T) {
	s, rel := storeComContadores(t, t.TempDir(), porOLT("aut", true))
	for i := 0; i < 50; i++ {
		k := chaveDeServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", true, rel.agora()), nil)
	}
	ctx, cancela := context.WithCancel(context.Background())
	cancela()
	if _, err := s.AuditaContadores(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("contexto cancelado tem de interromper a auditoria: %v", err)
	}
	if _, err := s.ReconstroiContadores(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("contexto cancelado tem de interromper a reconstrução: %v", err)
	}
}

func TestAuditoriaSemContadoresEhVazia(t *testing.T) {
	s, _ := storeComContadores(t, t.TempDir())
	rs, err := s.AuditaContadores(context.Background())
	if err != nil || len(rs) != 0 {
		t.Fatalf("sem contadores: %v, %v", rs, err)
	}
}
