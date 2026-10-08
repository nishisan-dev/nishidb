package executor

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// rowSource entrega UM candidato por chamada. A distinção entre "acabou" e
// "candidato morto" é do chamador: o laço de skip vive no iterador, não aqui,
// para que o check de ctx continue disparando quando um milhão de candidatos
// mortos aparecem em sequência.
type rowSource interface {
	// advance devolve o record do próximo candidato. more=false esgota a
	// fonte; live=false marca candidato sem location ou tombstoned. out é o
	// buffer possivelmente crescido e DEVE ser reatribuído pelo chamador em
	// todos os caminhos.
	advance(buf []byte) (rec types.Record, out []byte, more, live bool, err error)
	close() error
}

// candidateSource lê rowIds de um iterador de índice (ou do scan) e busca cada
// record. É onde a liveness é decidida — pelo FETCH, não pela fonte: o índice
// devolve superset por construção.
type candidateSource struct {
	it types.RowIDIterator
	st Store
	// snap é o universo desta consulta, e ele viaja com a FONTE porque é o
	// fetch que decide liveness (§44): o iterador entrega candidatos, e quem
	// diz se cada um estava vivo em T0 é o GetByRowID abaixo.
	snap types.Snapshot
}

func (s *candidateSource) advance(buf []byte) (types.Record, []byte, bool, bool, error) {
	if !s.it.Next() {
		if err := s.it.Err(); err != nil {
			return types.Record{}, buf, false, false, fmt.Errorf("executor: iteração de candidatos: %w", err)
		}
		return types.Record{}, buf, false, false, nil
	}
	id := s.it.RowID()
	rec, out, ok, err := s.st.GetByRowID(s.snap, id, buf)
	if err != nil {
		return types.Record{}, out, false, false, fmt.Errorf("executor: leitura da row %d: %w", id, err)
	}
	return rec, out, true, ok, nil
}

func (s *candidateSource) close() error { return s.it.Close() }

// keySource é o point lookup: um único GetByKey, sem passar por rowID. Evita a
// leitura dupla que existiria se o plano devolvesse um rowID para o fetcher
// reler.
type keySource struct {
	st Store
	// snap entra aqui pela MESMA razão do candidateSource: o point lookup
	// decide liveness por conta própria, sem passar pelo GetByRowID, e um
	// snapshot que não chegasse até ele deixaria `WHERE id = ?` respondendo
	// sobre um instante diferente do resto da mesma consulta.
	snap types.Snapshot
	key  []byte
	done bool
}

func (s *keySource) advance(buf []byte) (types.Record, []byte, bool, bool, error) {
	if s.done {
		return types.Record{}, buf, false, false, nil
	}
	s.done = true
	rec, out, ok, err := s.st.GetByKey(s.snap, s.key, buf)
	if err != nil {
		return types.Record{}, out, false, false, fmt.Errorf("executor: leitura da key %q: %w", s.key, err)
	}
	return rec, out, true, ok, nil
}

func (s *keySource) close() error { return nil }

// sourceOfPlano traduz o plano na fonte de candidatos. Nenhuma leitura de dado
// acontece aqui além de abrir iteradores.
func sourceOfPlano(ctx context.Context, plan planner.Plan, st Store, m *Metrics, snap types.Snapshot) (rowSource, error) {
	switch p := plan.(type) {
	case *planner.HistoryTimeRangePlan:
		return sourceOfHistoryTimeRange(ctx, p, st, m, snap)
	case *planner.FullScanPlan:
		countSource(m, func(m *Metrics) *atomic.Int64 { return &m.SourceFullScan })
		return &candidateSource{it: st.ScanLive(snap), st: st, snap: snap}, nil

	case *planner.PointLookupPlan:
		if len(p.Key.Bytes) == 0 {
			return nil, fmt.Errorf("%w: PointLookupPlan com key sem bytes", ErrInvalidPlan)
		}
		countSource(m, func(m *Metrics) *atomic.Int64 { return &m.PointLookups })
		return &keySource{st: st, snap: snap, key: p.Key.Bytes}, nil

	case *planner.IndexScanPlan:
		it, err := st.Lookup(p.Field.ID, p.Val)
		if err != nil {
			return nil, fmt.Errorf("executor: abrir índice de %q: %w", p.Field.Name, err)
		}
		countSource(m, func(m *Metrics) *atomic.Int64 { return &m.IndexScans })
		return &candidateSource{it: it, st: st, snap: snap}, nil

	case *planner.IndexIntersectPlan:
		its, err := openTermos(p.Terms, st)
		if err != nil {
			return nil, err
		}
		countSource(m, func(m *Metrics) *atomic.Int64 { return &m.IndexScans })
		return &candidateSource{it: &intersectIter{its: its, cur: make([]types.RowID, len(its))}, st: st, snap: snap}, nil

	case *planner.IndexUnionPlan:
		its, err := openTermos(p.Terms, st)
		if err != nil {
			return nil, err
		}
		countSource(m, func(m *Metrics) *atomic.Int64 { return &m.IndexScans })
		return &candidateSource{snap: snap, it: &unionIter{its: its, cur: make([]types.RowID, len(its)),
			vivo: make([]bool, len(its))}, st: st}, nil

	default:
		return nil, fmt.Errorf("%w: tipo de plano %T não suportado", ErrInvalidPlan, plan)
	}
}

// openTermos abre um iterador por termo. Falha no termo i FECHA os i-1 já
// abertos: vazar iterador de índice num caminho de erro é o tipo de coisa que
// só aparece como file descriptor esgotado em produção.
func openTermos(terms []planner.IndexTerm, st Store) ([]types.RowIDIterator, error) {
	its := make([]types.RowIDIterator, 0, len(terms))
	for _, t := range terms {
		it, err := st.Lookup(t.Field.ID, t.Val)
		if err != nil {
			for _, aberto := range its {
				aberto.Close()
			}
			return nil, fmt.Errorf("executor: abrir índice de %q: %w", t.Field.Name, err)
		}
		its = append(its, it)
	}
	return its, nil
}

// countSource incrementa um contador de abertura de fonte. Recebe a FUNÇÃO que
// escolhe o campo, e não o ponteiro do campo: `conta(m, &m.X)` avalia `&m.X`
// no chamador e explode com m nil — e Metrics é opcional por contrato.
func countSource(m *Metrics, escolhe func(*Metrics) *atomic.Int64) {
	if m != nil {
		escolhe(m).Add(1)
	}
}

// intersectIter é a interseção k-way de iteradores ASCENDENTES: avança os que
// estão atrás do máximo corrente até todos coincidirem. Memória O(k), nunca
// O(resultado).
//
// Interseção de supersets é superset da interseção — por isso continua válida
// como fonte de candidatos mesmo com postings órfãos.
type intersectIter struct {
	its  []types.RowIDIterator
	cur  []types.RowID
	id   types.RowID
	done bool
	err  error
}

func (i *intersectIter) Next() bool {
	if i.done {
		return false
	}
	// Na primeira chamada isto posiciona cada iterador no seu primeiro id; nas
	// seguintes, avança todos para além do id já emitido — os dois casos são a
	// mesma operação.
	for k := range i.its {
		if !i.advance(k) {
			return false
		}
	}
	for {
		max := i.cur[0]
		for _, v := range i.cur[1:] {
			if v > max {
				max = v
			}
		}
		coincidem := true
		for k := range i.its {
			for i.cur[k] < max {
				if !i.advance(k) {
					return false
				}
			}
			if i.cur[k] != max {
				// Ultrapassou o máximo: há um novo candidato a máximo e a
				// varredura recomeça.
				coincidem = false
			}
		}
		if coincidem {
			i.id = max
			return true
		}
	}
}

// advance puxa o próximo id do iterador k; esgotamento ou erro terminam a
// interseção inteira (não há mais como coincidir).
func (i *intersectIter) advance(k int) bool {
	if !i.its[k].Next() {
		i.err = i.its[k].Err()
		i.done = true
		return false
	}
	i.cur[k] = i.its[k].RowID()
	return true
}

func (i *intersectIter) RowID() types.RowID { return i.id }
func (i *intersectIter) Err() error         { return i.err }

func (i *intersectIter) Close() error {
	var primeiro error
	for _, it := range i.its {
		if err := it.Close(); err != nil && primeiro == nil {
			primeiro = err
		}
	}
	return primeiro
}

// unionIter é a união k-way COM DEDUP sobre iteradores ascendentes: emite o
// mínimo corrente e consome TODOS os iteradores posicionados nele.
//
// Consumir só o primeiro devolveria o mesmo rowID k vezes, e um OR duplicando
// linha é bug visível; perder um iterador devolveria resultado incompleto, que
// é pior porque é silencioso. Sem heap: com k pequeno a varredura linear é
// mais rápida e não aloca.
type unionIter struct {
	its  []types.RowIDIterator
	cur  []types.RowID
	vivo []bool
	id   types.RowID
	init bool
	err  error
}

func (u *unionIter) Next() bool {
	if !u.init {
		u.init = true
		for k := range u.its {
			u.puxa(k)
		}
	}
	min, achou := types.RowID(0), false
	for k := range u.its {
		if u.vivo[k] && (!achou || u.cur[k] < min) {
			min, achou = u.cur[k], true
		}
	}
	if !achou {
		return false
	}
	u.id = min
	for k := range u.its {
		if u.vivo[k] && u.cur[k] == min {
			u.puxa(k)
		}
	}
	return true
}

func (u *unionIter) puxa(k int) {
	if u.its[k].Next() {
		u.cur[k], u.vivo[k] = u.its[k].RowID(), true
		return
	}
	u.vivo[k] = false
	if err := u.its[k].Err(); err != nil && u.err == nil {
		u.err = err
	}
}

func (u *unionIter) RowID() types.RowID { return u.id }
func (u *unionIter) Err() error         { return u.err }

func (u *unionIter) Close() error {
	var primeiro error
	for _, it := range u.its {
		if err := it.Close(); err != nil && primeiro == nil {
			primeiro = err
		}
	}
	return primeiro
}
