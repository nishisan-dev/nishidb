package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// As rotas SQL medidas durante o STEADY. A §109 exige o número de memória com
// ingestão E CONSULTAS ao mesmo tempo, e a rodada anterior mediu com leitores
// de baixo nível — o SQL não existia ainda. Aqui o caminho é o completo:
// parse → bind → plan → executor → NDJSON.
//
// As cinco rotas cobrem as quatro fontes de candidatos do planner mais o
// degradado, que é o caso que a §91 exige cancelável.
var rotasSQL = []struct {
	nome string
	sql  string
	// param preenche o '?' quando a rota é parametrizada.
	usaKey bool
}{
	{nome: "sql_point_lookup", sql: `SELECT id, status FROM radius_sessions WHERE service_id = ?`, usaKey: true},
	{nome: "sql_bitmap_limit_100", sql: `SELECT id, status FROM radius_sessions WHERE status = 'Interim-Update' LIMIT 100`},
	{nome: "sql_intersect_limit_100", sql: `SELECT id FROM radius_sessions WHERE status = 'Interim-Update' AND protocol = 'PPP' LIMIT 100`},
	{nome: "sql_union_limit_100", sql: `SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start' LIMIT 100`},
	// O predicado CASA, para que o LIMIT corte cedo: é o full scan com
	// early-exit da §91. Um predicado que não casasse varreria o dataset
	// inteiro a cada consulta — a 8M rows isso são segundos por query, e o
	// que estaria sendo medido seria a varredura completa, não o caminho que
	// o usuário paga com LIMIT.
	{nome: "sql_full_scan_limit_100", sql: `SELECT id FROM radius_sessions WHERE connect_info >= 0 LIMIT 100`},
	// E o full scan que NÃO casa nada, com LIMIT, medido à parte: é o pior
	// caso do §91 (cancelável, memória constante) e o número honesto de uma
	// varredura completa. Fica fora dos gates de latência por construção.
	{nome: "sql_full_scan_completo", sql: `SELECT id FROM radius_sessions WHERE username = 'nao-existe-jamais' LIMIT 1`},
}

// sqlPool roda consultas SQL concorrentes com a ingestão e mede a latência de
// cada rota.
type sqlPool struct {
	stopCh chan struct{}
	wg     sync.WaitGroup
	lat    []*latencyRec
	eng    *query.Engine
	erros  int64
	mu     sync.Mutex
}

func startSQLReaders(cfg Config, st *storage.Store, c *schema.Compiled, keys [][]byte) *sqlPool {
	p := &sqlPool{stopCh: make(chan struct{}), lat: make([]*latencyRec, len(rotasSQL))}
	for i := range p.lat {
		p.lat[i] = newLatencyRec()
	}
	if cfg.SQLReaders <= 0 || len(keys) == 0 {
		return p
	}
	eng := query.NewEngine()
	if err := eng.Register(c.Name, c, st); err != nil {
		return p
	}
	p.eng = eng

	for i := 0; i < cfg.SQLReaders; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			// Buffer de saída próprio por goroutine: o NDJSON é serializado de
			// verdade, senão a medição não inclui o encoder — que é parte do
			// caminho que o usuário paga.
			saida := &contador{}
			w := executor.NewStreamWriter(saida, 8<<10)
			n := id
			for {
				select {
				case <-p.stopCh:
					return
				default:
				}
				rota := n % len(rotasSQL)
				var params []types.Value
				if rotasSQL[rota].usaKey {
					params = []types.Value{{Type: types.TypeString, Bytes: keys[n%len(keys)]}}
				}
				início := time.Now()
				err := p.roda(w, rotasSQL[rota].sql, params)
				p.lat[rota].add(time.Since(início))
				if err != nil {
					p.mu.Lock()
					p.erros++
					p.mu.Unlock()
				}
				n++
			}
		}(i)
	}
	return p
}

// roda executa uma consulta e SERIALIZA o resultado, para que a medição
// inclua o encoder.
func (p *sqlPool) roda(w *executor.StreamWriter, sql string, params []types.Value) error {
	it, err := p.eng.Execute(context.Background(), "radius_sessions", sql, params)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		if err := w.WriteRow(it.Row()); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return it.Err()
}

func (p *sqlPool) stop() ([]QueryStats, int64) {
	close(p.stopCh)
	p.wg.Wait()
	var out []QueryStats
	for i, rec := range p.lat {
		if s, ok := rec.stats(rotasSQL[i].nome); ok {
			out = append(out, s)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return out, p.erros
}

// contador descarta os bytes e conta: o custo do io.Writer não pode entrar na
// medição da query, mas o volume produzido é dado de relatório.
type contador struct{ bytes int64 }

func (c *contador) Write(p []byte) (int, error) {
	c.bytes += int64(len(p))
	return len(p), nil
}

var _ fmt.Stringer = (*contador)(nil)

func (c *contador) String() string { return fmt.Sprintf("%d bytes", c.bytes) }
