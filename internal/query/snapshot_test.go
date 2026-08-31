package query_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
)

// eventosDeStop são as três linhas que existem quando a consulta abre, mais a
// quarta que nasce DEPOIS dela.
const (
	stopA = `{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop","User-Name":"ana"}`
	stopB = `{"id":"B","_SEQ":1,"Acct-Status-Type":"Stop","User-Name":"bia"}`
	stopC = `{"id":"C","_SEQ":1,"Acct-Status-Type":"Stop","User-Name":"caio"}`
	stopD = `{"id":"D","_SEQ":1,"Acct-Status-Type":"Stop","User-Name":"davi"}`
)

// O teto do snapshot tem de valer para TODO plano, e é o plano indexado que
// hoje não o tem.
//
// O full scan fixa o limite superior de rowId na abertura do iterador; o
// caminho por índice não fixava nenhum — com o índice POR SEGMENTO, a fonte do
// segmento corrente lê o .widx que a ingestão continua escrevendo, e a posting
// acrescentada durante a varredura entra na varredura em curso. É a segunda
// metade do débito 94: a MESMA consulta devolve conjuntos diferentes conforme
// o planner escolha índice ou varredura.
//
// O teste roda a MESMA situação nos dois planos porque é a DIVERGÊNCIA que é o
// defeito: um teste só no plano indexado passaria a existir sem dizer contra o
// que o resultado dele deveria ser comparado.
func TestRowNascidaDepoisDoOpenNaoApareceEmPlanoNenhum(t *testing.T) {
	casos := []struct {
		nome string
		sql  string
	}{
		// Acct-Status-Type é indexado no fixture: fonte por índice.
		{"plano indexado", `SELECT id FROM radius_sessions WHERE "Acct-Status-Type" = 'Stop'`},
		// User-Name não é indexado: o planner degrada para varredura completa.
		{"varredura completa", `SELECT id FROM radius_sessions WHERE "User-Name" LIKE '%a%'`},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			ig := &integracao{t: t, dir: filepath.Join(t.TempDir(), "radius"), oracle: oraculo{}}
			ig.abre()
			defer ig.fecha()

			mat, err := materializer.New(ig.sch, ig.store)
			if err != nil {
				t.Fatal(err)
			}
			ingere := func(eventos ...string) {
				t.Helper()
				for _, ev := range eventos {
					if r := mat.Apply([]byte(ev)); r.Err != nil {
						t.Fatalf("ingestão de %s: %v (%v)", ev, r.Err, r.Category)
					}
				}
			}
			ingere(stopA, stopB, stopC)

			// O Execute abre o snapshot. Daqui em diante o universo da
			// consulta está fixado, aconteça o que acontecer no dataset.
			it, err := ig.engine.Execute(context.Background(), dataset, c.sql, nil)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			defer func() { _ = it.Close() }()

			// A quarta linha nasce com a consulta EM VOO — e satisfaz o WHERE
			// dos dois planos.
			ingere(stopD)

			var vistas int
			for it.Next() {
				vistas++
			}
			if err := it.Err(); err != nil {
				t.Fatalf("Err: %v", err)
			}
			if vistas != 3 {
				t.Errorf("a consulta devolveu %d linhas, quero 3: a row nascida DEPOIS do snapshot entrou no resultado", vistas)
			}

			// A contraprova, e sem ela o teste acima é vacuidade: uma consulta
			// NOVA tem de enxergar as quatro. Sem esta asserção, "a quarta não
			// apareceu" não distingue o snapshot funcionando de a linha nunca
			// ter sido gravada — que é a forma mais comum de um teste de
			// visibilidade passar por engano.
			depois := ig.consulta(c.sql)
			if len(depois) != 4 {
				t.Errorf("a consulta aberta DEPOIS devolveu %d linhas, quero 4: a quarta linha não chegou ao dataset, então o teste acima não provou nada", len(depois))
			}
		})
	}
}
