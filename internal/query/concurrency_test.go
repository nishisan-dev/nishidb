package query_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Writer e leitores ao mesmo tempo, pelo caminho do SQL, sobre o Store real.
//
// O harness de memória já provou isso para as três rotas de leitura de baixo
// nível; aqui a pergunta é outra: uma consulta em andamento pode ver estado
// PARCIAL de um Put concorrente? Se puder, o resultado carrega uma row com
// metade dos campos do evento novo e metade do anterior — e nada acusa.
//
// A verificação é por INVARIANTE de conteúdo, não por contagem: o número de
// linhas muda a cada instante com o writer rodando, mas toda linha devolvida
// tem de ser um estado COERENTE (a sessão sempre tem status e protocolo, e o
// par (status, protocolo) só assume as combinações que o writer escreveu).
func TestConcorrenciaWriterELeitoresPeloSQL(t *testing.T) {
	ig := &integracao{t: t, dir: t.TempDir() + "/conc", oracle: oraculo{}}
	ig.open()

	const (
		entidades = 400
		rodadas   = 6
		leitores  = 4
	)
	// Combinações válidas: o writer só escreve pares coerentes, então uma
	// leitura que devolva um par fora desta lista viu estado parcial.
	pares := [][2]string{{"Start", "PPPoE"}, {"Stop", "PPP"}, {"Alive", "PPPoE"}}

	mat, err := materializer.New(ig.sch, ig.store)
	if err != nil {
		t.Fatal(err)
	}
	// Estado inicial, para que os leitores tenham o que ler desde o começo.
	for i := 0; i < entidades; i++ {
		ev := fmt.Sprintf(`{"id":"K%04d","_SEQ":1,"Acct-Status-Type":%q,"Framed-Protocol":%q}`,
			i, pares[0][0], pares[0][1])
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			t.Fatal(r.Err)
		}
	}

	var (
		wg      sync.WaitGroup
		stopLer atomic.Bool
		lidas   atomic.Int64
	)

	// Writer: um só, como manda o contrato do Store (writer único).
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stopLer.Store(true)
		for r := 1; r <= rodadas; r++ {
			pair := pares[r%len(pares)]
			for i := 0; i < entidades; i++ {
				ev := fmt.Sprintf(`{"id":"K%04d","_SEQ":%d,"Acct-Status-Type":%q,"Framed-Protocol":%q}`,
					i, r+1, pair[0], pair[1])
				if res := mat.Apply([]byte(ev)); res.Err != nil {
					t.Errorf("ingestão concorrente: %v", res.Err)
					return
				}
			}
			if err := ig.store.Checkpoint(); err != nil {
				t.Errorf("checkpoint concorrente: %v", err)
				return
			}
		}
	}()

	coerente := func(status, protocolo string) bool {
		for _, p := range pares {
			if p[0] == status && p[1] == protocolo {
				return true
			}
		}
		return false
	}

	for l := 0; l < leitores; l++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopLer.Load() {
				it, err := ig.engine.Execute(context.Background(), dataset,
					`SELECT id, status, protocol FROM radius_sessions LIMIT 100`, nil)
				if err != nil {
					t.Errorf("Execute concorrente: %v", err)
					return
				}
				for it.Next() {
					var status, protocolo string
					for _, c := range it.Row().Cols() {
						switch c.Name {
						case "status":
							status = string(c.Val.Bytes)
						case "protocol":
							protocolo = string(c.Val.Bytes)
						}
					}
					if !coerente(status, protocolo) {
						t.Errorf("estado PARCIAL visível: status=%q protocolo=%q — "+
							"a consulta viu metade de um Put", status, protocolo)
						it.Close()
						return
					}
					lidas.Add(1)
				}
				if err := it.Err(); err != nil {
					t.Errorf("iteração concorrente: %v", err)
					it.Close()
					return
				}
				it.Close()
			}
		}()
	}

	wg.Wait()
	if lidas.Load() == 0 {
		t.Fatal("nenhuma linha foi lida durante a ingestão: o teste não exercitou concorrência")
	}
	t.Logf("%d linhas lidas por %d leitores durante %d rodadas de ingestão",
		lidas.Load(), leitores, rodadas)

	// Estado final coerente: todas as entidades no último par escrito.
	ultimo := pares[rodadas%len(pares)]
	// Aspas SIMPLES: em SQL as duplas são identificador quoted, e `status =
	// "Start"` viraria comparação entre colunas — que a v1 recusa.
	got := ig.ids(fmt.Sprintf(
		`SELECT id FROM radius_sessions WHERE status = '%s' AND protocol = '%s'`, ultimo[0], ultimo[1]))
	if len(got) != entidades {
		t.Errorf("estado final = %d entidades no par %v, want %d", len(got), ultimo, entidades)
	}
}

// Point lookup concorrente com a ingestão da MESMA key: ou a consulta vê o
// estado anterior, ou o novo — nunca um meio-termo.
func TestPointLookupConcorrenteComEscritaDaMesmaKey(t *testing.T) {
	ig := &integracao{t: t, dir: t.TempDir() + "/conc2", oracle: oraculo{}}
	ig.open()

	mat, err := materializer.New(ig.sch, ig.store)
	if err != nil {
		t.Fatal(err)
	}
	if r := mat.Apply([]byte(`{"id":"K","_SEQ":1,"Acct-Status-Type":"Start","User-Name":"v1"}`)); r.Err != nil {
		t.Fatal(r.Err)
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stop.Store(true)
		for seq := 2; seq <= 300; seq++ {
			ev := fmt.Sprintf(`{"id":"K","_SEQ":%d,"User-Name":"v%d"}`, seq, seq)
			if r := mat.Apply([]byte(ev)); r.Err != nil {
				t.Errorf("ingestão: %v", r.Err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			rows := ig.consulta(`SELECT id, username, status FROM radius_sessions WHERE id = ?`,
				types.Value{Type: types.TypeString, Bytes: []byte("K")})
			if len(rows) != 1 {
				t.Errorf("point lookup devolveu %d linhas durante a escrita", len(rows))
				return
			}
			// O status vem do primeiro evento e nunca é reescrito: se ele
			// sumir ou mudar, a leitura pegou um estado parcial do merge.
			if rows[0]["status"] != "Start" {
				t.Errorf("status = %q durante a escrita, want Start preservado pelo merge",
					rows[0]["status"])
				return
			}
		}
	}()

	wg.Wait()
}
