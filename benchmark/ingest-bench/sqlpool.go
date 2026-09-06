package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/budget"
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
	// linhas é o ORÁCULO da rota: quantas linhas ela TEM de devolver.
	//
	// Sem isto, a bancada não conferia resultado nenhum — era o débito 6 — e
	// um bug que fizesse toda query devolver zero linhas passava em TODOS os
	// gates, com p95 MELHOR, porque não devolver linha é mais rápido que
	// devolver. Otimização de performance é exatamente o tipo de mudança que
	// produz esse bug, então a bancada que a julga não pode ser cega a ele.
	//
	// Os números não são estimativa: o point lookup usa uma key sorteada do
	// dataset e devolve exatamente uma; as rotas com LIMIT 100 têm corpus que
	// garante o corte; e a varredura que não casa nada devolve zero.
	linhas int64
}{
	{nome: "sql_point_lookup", sql: `SELECT id, status FROM radius_sessions WHERE service_id = ?`, usaKey: true, linhas: 1},
	{nome: "sql_bitmap_limit_100", sql: `SELECT id, status FROM radius_sessions WHERE status = 'Interim-Update' LIMIT 100`, linhas: 100},
	{nome: "sql_intersect_limit_100", sql: `SELECT id FROM radius_sessions WHERE status = 'Interim-Update' AND protocol = 'PPP' LIMIT 100`, linhas: 100},
	{nome: "sql_union_limit_100", sql: `SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start' LIMIT 100`, linhas: 100},
	// O predicado CASA, para que o LIMIT corte cedo: é o full scan com
	// early-exit da §91. Um predicado que não casasse varreria o dataset
	// inteiro a cada consulta — a 8M rows isso são segundos por query, e o
	// que estaria sendo medido seria a varredura completa, não o caminho que
	// o usuário paga com LIMIT.
	{nome: "sql_full_scan_limit_100", sql: `SELECT id FROM radius_sessions WHERE connect_info >= 0 LIMIT 100`, linhas: 100},
}

// varreduraCompleta é o full scan que NÃO casa nada: percorre o dataset
// inteiro e é o número honesto do pior caso.
//
// Fica FORA do rodízio de propósito. Ele custa ~6 s a 1M rows (e cresce
// linear), então dentro do rodízio cada leitor passaria a maior parte do tempo
// dentro dele e as rotas que os gates medem ficariam com um punhado de
// amostras — o p95 delas viraria ruído. Medido UMA vez, ao fim do STEADY.
const varreduraCompleta = `SELECT id FROM radius_sessions WHERE username = 'nao-existe-jamais' LIMIT 1`

// MedeVarreduraCompleta roda uma varredura completa e devolve a duração.
func (p *sqlPool) MedeVarreduraCompleta() (time.Duration, int64, error) {
	if p.eng == nil {
		return 0, 0, nil
	}
	saida := &contador{}
	// Orçamento do §80 com os defaults do produto: o benchmark tem de pagar o
	// MESMO caminho que o servidor paga, senão ele mede um writer que não
	// existe em produção.
	w := executor.NewStreamWriter(saida, 8<<10, orcamentoDoBench())
	início := time.Now()
	// A varredura completa usa um predicado que não casa nada de propósito, e
	// por isso o oráculo dela é ZERO linhas — conferido aqui, já que ela roda
	// fora do rodízio.
	linhas, err := p.roda(w, varreduraCompleta, nil)
	if err == nil && linhas != 0 {
		err = fmt.Errorf("varredura completa devolveu %d linhas; o predicado não casa nada e o oráculo é 0", linhas)
	}
	return time.Since(início), saida.bytes, err
}

// orcamentoDoBench é o orçamento de memória de consulta (§80) nos defaults do
// produto, um por leitor.
//
// Um por leitor, e não um Global compartilhado, porque o benchmark mede
// LATÊNCIA DE ROTA: com um Global só, a contenção do CAS entre os leitores
// entraria na medição e seria lida como custo da consulta. O que importa aqui é
// que o writer pague o mesmo caminho de código que paga em produção.
func orcamentoDoBench() *budget.PerQuery {
	p := budget.NewGlobal(512 << 20).Query(4 << 20)
	// O retorno é IGNORADO de propósito, e o `_ =` diz isso: um Global de
	// 512 MiB comporta o primeiro Admite de 4 MiB por construção, e o benchmark
	// não tem a quem reportar recusa — ele mede vazão, não admissão.
	_ = p.Admit()
	return p
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
	// violacoes conta, POR ROTA, quantas consultas devolveram um número de
	// linhas diferente do oráculo. Atômico porque os leitores são N goroutines.
	violacoes []atomic.Int64
}

func startSQLReaders(cfg Config, st *storage.Store, c *schema.Compiled, keys [][]byte) *sqlPool {
	p := &sqlPool{
		stopCh:    make(chan struct{}),
		lat:       make([]*latencyRec, len(rotasSQL)),
		violacoes: make([]atomic.Int64, len(rotasSQL)),
	}
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
			w := executor.NewStreamWriter(saida, 8<<10, orcamentoDoBench())
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
				linhas, err := p.roda(w, rotasSQL[rota].sql, params)
				p.lat[rota].add(time.Since(início))
				if err != nil {
					p.mu.Lock()
					p.erros++
					p.mu.Unlock()
				} else if linhas != rotasSQL[rota].linhas {
					// O ORÁCULO. Contado, e não abortado: uma rodada de
					// benchmark que morre no primeiro desvio não diz QUANTAS
					// consultas erraram nem se o erro é sistemático.
					p.violacoes[rota].Add(1)
				}
				n++
			}
		}(i)
	}
	return p
}

// roda executa uma consulta e SERIALIZA o resultado, para que a medição
// inclua o encoder.
func (p *sqlPool) roda(w *executor.StreamWriter, sql string, params []types.Value) (int64, error) {
	it, err := p.eng.Execute(context.Background(), "radius_sessions", sql, params)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var linhas int64
	for it.Next() {
		if err := w.WriteRow(it.Row()); err != nil {
			return linhas, err
		}
		linhas++
	}
	if err := w.Flush(); err != nil {
		return linhas, err
	}
	return linhas, it.Err()
}

func (p *sqlPool) stop() ([]QueryStats, int64) {
	close(p.stopCh)
	p.wg.Wait()
	var out []QueryStats
	for i, rec := range p.lat {
		if s, ok := rec.stats(rotasSQL[i].nome); ok {
			s.RowsExpected = rotasSQL[i].linhas
			s.OracleFails = p.violacoes[i].Load()
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
