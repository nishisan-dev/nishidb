package counter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// WithoutGroup é a entrada do vetor de uma linha que não está em grupo nenhum:
// nunca vista, tombstonada, sem alguma coluna do groupBy, ou recusada pelo
// teto. É zero porque zero é o que o fallocate deixa no arquivo — "nunca
// gravada" e "sem grupo" são deliberadamente a mesma coisa.
const WithoutGroup uint32 = 0

// conditionBit marca, na entrada do vetor, que a linha satisfazia o `where`
// quando foi gravada. Os 31 bits abaixo são o id do grupo, e é por isso que o
// teto físico de grupos é 2^31-1.
const conditionBit uint32 = 1 << 31

// Evaluator é o `where` compilado: diz se o payload (o estado INTEIRO da
// linha, pós-merge) satisfaz a condição. Quem o implementa é o predicado; este
// pacote só conhece a interface, para que o storage não importe o binder.
type Evaluator interface {
	Evaluate(payload []byte) (bool, error)
}

// Definition é um contador como declarado no YAML e compilado contra o schema.
type Definition struct {
	Name string
	// Columns são os FieldIDs do groupBy, na ordem DECLARADA — é a ordem dos
	// componentes da chave composta e das colunas da tabela virtual.
	Columns []types.FieldID
	// Names e Types acompanham Columns, índice a índice.
	Names []string
	Types []types.FieldType
	// Where é o texto normalizado da condição; "" é "sem condição", e nesse
	// caso NewEvaluator é nil e toda linha com grupo conta.
	Where string
	// NewEvaluator instancia um avaliador com scratch próprio. É fábrica, e
	// não instância, porque o writer e a auditoria avaliam em goroutines
	// diferentes e o scratch é de um dono só.
	NewEvaluator func() Evaluator
	// MaxGroups é o teto de grupos VIVOS.
	MaxGroups uint32
	// RetainEmptyGroups é quanto tempo um grupo com refs == 0 continua
	// visível antes de ser aposentado; zero é "nunca".
	RetainEmptyGroups time.Duration
}

var vectorMagic = [4]byte{'N', 'D', 'B', 'K'}

const (
	dictFileName   = "grupos.dict"
	vectorFileName = "grupos.idx"
	oldSuffix      = ".old"
)

// Counter é o estado de um contador declarado: o vetor rowId → entrada, o
// dicionário de grupos e os totais por grupo.
//
// # Consistência através de crash
//
// Os totais NÃO são persistidos. Na abertura eles são DOBRA do vetor em disco,
// e cada transição aplica `delta = f(nova) − f(antiga)` com antiga lida do
// PRÓPRIO vetor — nunca do payload anterior, que o replay da recovery não tem
// (store_recovery.go). Assim `totals == fold(vector)` vale em qualquer
// instante, e o replay reaplicando uma transição já refletida no vetor vê
// antiga == nova e não mexe em nada.
//
// Os agregados do contador inteiro (rows e total) herdam essa propriedade
// inteira: são a MESMA dobra, acumulada no mesmo laço, e por isso nada de novo
// vai a disco — nem no vetor de 32 bits, nem no dicionário, nem na seção dos
// contadores do manifest, que só carrega os parâmetros que decidem o CONTEÚDO
// do vetor.
//
// O vetor em disco nunca fica à frente do log durável: a transição vai para um
// OVERLAY em heap, e só o ApplyOverlay — que o checkpoint chama depois do
// fdatasync dos segmentos — a leva ao mmap. É a mesma invariante das locations
// (rowindex.Locations), pelo mesmo motivo: uma página descarregada pelo kernel
// antes do crash não pode descrever um record que não existe.
//
// # Concorrência
//
// Transition, Fold, Clear, ApplyOverlay, RecordCoveredTouches e
// RetireEmptyGroups são exclusivos do writer (o Store os chama sob o wmu).
// Limit, Group, ByText, AuditSnapshot e Stats são seguros para leitores
// concorrentes: os totais são atômicos e os slices por grupo só crescem sob
// o lock FOLHA do dicionário, que os leitores tomam para indexar. Nada aqui
// toma lock do Store.
type Counter struct {
	def    Definition
	dir    string
	dict   *Dict
	vector *rowindex.Dense32
	// over é o overlay: transições ainda não aplicadas ao mmap. Writer-only —
	// ninguém lê o vetor além do writer, então não precisa de lock.
	over map[types.RowID]uint32

	// Por grupo, índice = id (0 reservado). Crescem sob dict.mu quando um id
	// nasce; os leitores indexam sob RLock e conferem o limite.
	totals     []int64 // atômicos (atomic.LoadInt64/AddInt64)
	refs       []int64 // idem
	lastTouch  []uint64
	emptySince []int64 // unix ms em que refs chegou a 0; 0 = não está vazio
	texts      [][]byte
	byText     map[string]uint32

	// rows e total são os agregados do contador INTEIRO: rows é a soma
	// dos refs de todos os grupos, total a soma dos totais.
	//
	// Mantidos AQUI, e nunca somados na leitura, porque o delta já é conhecido
	// dentro do Transition (sempre ±1) e somar os grupos custaria O(grupos)
	// por linha de _meta.counters — num contador por PON são milhares.
	//
	// Grupo aposentado não entra na conta e não precisa sair dela: a
	// aposentadoria exige refs == 0 (retirement.go), e totals[g] <= refs[g]
	// por construção — cada linha contribui 1 para refs e 0 ou 1 para totals,
	// decidido pelo MESMO bit da MESMA entrada. Logo a soma sobre todos os ids
	// é igual à soma sobre os vivos, em qualquer instante.
	//
	// São atomic.Int64 (método, nunca indexação) para que `c.total` não se
	// confunda com `c.totals[g]` na leitura de quem mexer neste arquivo. O
	// preço de mantê-los é uma SEGUNDA fonte para o mesmo fato, e o que a
	// reconcilia é a auditoria, que os confere contra o recalculado.
	rows  atomic.Int64
	total atomic.Int64

	transitions      uint64
	coveredTouches   uint64
	refused          atomic.Uint64
	orphans          atomic.Uint64
	auditDivergences atomic.Int64
	auditLast        atomic.Int64
	warnedOfCeiling  bool
	onRefuse         func()

	deriv *Deriver
	// textBuf e textScratch são o scratch da renderização de groupKey de um
	// grupo NOVO; writer-only.
	textBuf     []byte
	textScratch []types.Value
	now         func() time.Time
}

// Open abre (ou cria) o dicionário e o vetor do contador em dir. recreate=true
// renomeia os arquivos existentes para *.old e nasce vazio — é a reconstrução
// por divergência de parâmetros, decidida pelo Store a partir do manifest
// ANTES da recovery; RemoveOld remove os renomeados quando a reconstrução
// terminar.
func Open(dir string, def Definition, minRows uint64, recreate bool) (*Counter, error) {
	if err := validateDefinition(def); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("contador %s: criar %s: %w", def.Name, dir, err)
	}
	if recreate {
		for _, name := range []string{dictFileName, vectorFileName} {
			from := filepath.Join(dir, name)
			if err := os.Rename(from, from+oldSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("contador %s: pôr %s de lado: %w", def.Name, from, err)
			}
		}
	}
	dict, err := OpenDict(filepath.Join(dir, dictFileName), def.MaxGroups)
	if err != nil {
		return nil, fmt.Errorf("contador %s: %w", def.Name, err)
	}
	vector, err := rowindex.OpenDense32(filepath.Join(dir, vectorFileName), vectorMagic, minRows)
	if err != nil {
		_ = dict.Close()
		return nil, fmt.Errorf("contador %s: %w", def.Name, err)
	}
	c := &Counter{
		def:    def,
		dir:    dir,
		dict:   dict,
		vector: vector,
		over:   make(map[types.RowID]uint32),
		byText: make(map[string]uint32),
		now:    time.Now,
	}
	c.deriv = c.NewDeriver()
	// Os slices por grupo e os textos nascem do dicionário que já está em
	// disco: os ids são estáveis, e a tabela virtual precisa do texto de todo
	// grupo desde a primeira consulta.
	if err := c.catchUpWithDict(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func validateDefinition(def Definition) error {
	switch {
	case def.Name == "":
		return errors.New("contador: definição sem nome")
	case len(def.Columns) == 0:
		return fmt.Errorf("contador %s: groupBy vazio", def.Name)
	case len(def.Types) != len(def.Columns) || len(def.Names) != len(def.Columns):
		return fmt.Errorf("contador %s: Columns, Names e Types têm de ter o mesmo tamanho", def.Name)
	case def.MaxGroups == 0 || def.MaxGroups > conditionBit-1:
		return fmt.Errorf("contador %s: maxGroups %d fora de [1, %d]", def.Name, def.MaxGroups, conditionBit-1)
	case def.Where != "" && def.NewEvaluator == nil:
		return fmt.Errorf("contador %s: where declarado sem avaliador", def.Name)
	}
	return nil
}

// catchUpWithDict cresce os slices por grupo até o limite do dicionário
// e renderiza o texto dos ids que ainda não têm. Roda na abertura e a cada
// grupo novo.
func (c *Counter) catchUpWithDict() error {
	c.dict.mu.Lock()
	defer c.dict.mu.Unlock()
	limit := len(c.dict.keys) + 1
	for len(c.totals) < limit {
		id := uint32(len(c.totals))
		c.totals = append(c.totals, 0)
		c.refs = append(c.refs, 0)
		c.lastTouch = append(c.lastTouch, 0)
		c.emptySince = append(c.emptySince, 0)
		c.texts = append(c.texts, nil)
		if id == WithoutGroup {
			continue
		}
		var err error
		c.textBuf, c.textScratch, err = KeyText(c.textBuf[:0], c.dict.keys[id-1], c.textScratch)
		if err != nil {
			return fmt.Errorf("contador %s: grupo %d: %w", c.def.Name, id, err)
		}
		text := make([]byte, len(c.textBuf))
		copy(text, c.textBuf)
		c.texts[id] = text
		if c.dict.live[id-1] {
			c.byText[string(text)] = id
		}
	}
	return nil
}

// Def devolve a definição com que o contador foi aberto.
func (c *Counter) Def() Definition { return c.def }

// Born diz se o vetor foi criado por ESTA abertura.
func (c *Counter) Born() bool { return c.vector.Born() }

// Covers diz se o vetor tem entrada para toda linha abaixo de limit.
func (c *Counter) Covers(limit types.RowID) bool { return c.vector.Covers(limit) }

// OnRefuse registra o callback chamado UMA vez, na primeira recusa pelo teto
// desta abertura — é o log do Store, que não quer uma linha por evento.
func (c *Counter) OnRefuse(fn func()) { c.onRefuse = fn }

// Deriver calcula (grupo, condição) de um payload. Tem scratch próprio e é
// de um dono só: o writer tem o seu, a auditoria instancia outro.
type Deriver struct {
	c          *Counter
	eval       Evaluator
	fr         encoding.FieldReader
	vals       []types.Value // na ordem DECLARADA do groupBy
	order      []int         // posição declarada da i-ésima coluna em ordem crescente de id
	ids        []types.FieldID
	foundCount int
	keyBuf     []byte
}

// NewDeriver instancia um derivador com avaliador e scratch próprios.
func (c *Counter) NewDeriver() *Deriver {
	d := &Deriver{c: c, vals: make([]types.Value, len(c.def.Columns))}
	if c.def.NewEvaluator != nil {
		d.eval = c.def.NewEvaluator()
	}
	// O payload é ordenado por FieldID; o groupBy não. A leitura casa os dois
	// por merge linear sobre a ordem crescente e devolve cada valor à posição
	// declarada.
	type pair struct {
		id  types.FieldID
		pos int
	}
	pairs := make([]pair, len(c.def.Columns))
	for i, id := range c.def.Columns {
		pairs[i] = pair{id, i}
	}
	slices.SortFunc(pairs, func(a, b pair) int { return int(a.id) - int(b.id) })
	d.ids = make([]types.FieldID, len(pairs))
	d.order = make([]int, len(pairs))
	for i, p := range pairs {
		d.ids[i], d.order[i] = p.id, p.pos
	}
	return d
}

// Derive lê o payload UMA vez e devolve a chave composta do grupo e a condição.
// hasGroup=false quando alguma coluna do groupBy está ausente (NULL é ausência)
// ou com tipo diferente do declarado — a linha não é contada. A chave é válida
// até a próxima chamada. Não aloca.
func (d *Deriver) Derive(payload []byte) (key []byte, cond, hasGroup bool, err error) {
	d.foundCount = 0
	d.fr.Reset(payload)
	i := 0
	for i < len(d.ids) {
		id, v, ok := d.fr.Next()
		if !ok {
			if err := d.fr.Err(); err != nil {
				return nil, false, false, err
			}
			break
		}
		for i < len(d.ids) && d.ids[i] < id {
			i++
		}
		if i < len(d.ids) && d.ids[i] == id {
			pos := d.order[i]
			if v.Type != d.c.def.Types[pos] {
				return nil, false, false, nil
			}
			d.vals[pos] = v
			d.foundCount++
			i++
		}
	}
	if d.foundCount != len(d.ids) {
		return nil, false, false, nil
	}
	d.keyBuf, err = valuekey.AppendComposite(d.keyBuf[:0], d.vals)
	if err != nil {
		return nil, false, false, err
	}
	cond = true
	if d.eval != nil {
		if cond, err = d.eval.Evaluate(payload); err != nil {
			return nil, false, false, err
		}
	}
	return d.keyBuf, cond, true, nil
}

// entry devolve a entrada corrente da linha: overlay, senão mmap; além do
// arquivo é WithoutGroup.
func (c *Counter) entry(rowID types.RowID) uint32 {
	if v, ok := c.over[rowID]; ok {
		return v
	}
	v, _ := c.vector.Get(rowID)
	return v
}

// Transition é o ÚNICO ponto de escrita do contador: aplica ao estado a
// transição da linha do que ela era (a entrada do vetor) para o que passou a
// ser (derivado do payload). tombstone=true força "sem grupo" — a lápide não
// carrega o groupBy, e é a entrada antiga que lembra de onde a linha saiu.
//
// Roda nos DOIS modos do applyRecord — write e replay —, sob o wmu. Zero
// alocação com grupo existente; grupo novo aloca (mapa, cópia da chave, texto)
// e é limitado pelo teto.
//
// O incremento do grupo novo vem ANTES do decremento do antigo: um leitor
// somando os grupos no meio da transição vê a linha em dois lugares, nunca
// em nenhum — o contador nunca subestima.
func (c *Counter) Transition(rowID types.RowID, payload []byte, tombstone bool) error {
	newEntry := WithoutGroup
	if !tombstone {
		key, cond, hasGroup, err := c.deriv.Derive(payload)
		if err != nil {
			return fmt.Errorf("contador %s: derivar a linha %d: %w", c.def.Name, rowID, err)
		}
		if hasGroup {
			id, err := c.dict.Intern(key)
			switch {
			case errors.Is(err, ErrGroupCeilingReached):
				c.refused.Add(1)
				if !c.warnedOfCeiling {
					c.warnedOfCeiling = true
					if c.onRefuse != nil {
						c.onRefuse()
					}
				}
			case err != nil:
				return fmt.Errorf("contador %s: internar o grupo da linha %d: %w", c.def.Name, rowID, err)
			default:
				if int(id) >= len(c.totals) {
					if err := c.catchUpWithDict(); err != nil {
						return err
					}
				}
				newEntry = id
				if cond {
					newEntry |= conditionBit
				}
			}
		}
	}
	oldEntry := c.entry(rowID)
	if oldEntry == newEntry {
		return nil
	}
	c.transitions++
	// O agregado anda COLADO ao grupo, nas duas pontas: cada Add do agregado
	// fica ao lado do AddInt64 do vetor que ele soma, e DENTRO das mesmas
	// guardas. O do decremento, em especial, mora dentro do
	// `int(g) < len(c.totals)`: quando essa guarda é falsa não existe slot e
	// nenhum grupo desce, então um agregado que descesse ali derivaria da soma
	// dos grupos em silêncio.
	if g := newEntry &^ conditionBit; g != WithoutGroup {
		atomic.AddInt64(&c.refs[g], 1)
		c.rows.Add(1)
		if newEntry&conditionBit != 0 {
			atomic.AddInt64(&c.totals[g], 1)
			c.total.Add(1)
		}
		c.lastTouch[g] = c.transitions
		c.emptySince[g] = 0
	}
	if g := oldEntry &^ conditionBit; g != WithoutGroup && int(g) < len(c.totals) {
		if oldEntry&conditionBit != 0 {
			atomic.AddInt64(&c.totals[g], -1)
			c.total.Add(-1)
		}
		c.rows.Add(-1)
		if atomic.AddInt64(&c.refs[g], -1) == 0 {
			c.emptySince[g] = c.now().UnixMilli()
		}
		c.lastTouch[g] = c.transitions
	}
	c.over[rowID] = newEntry
	return nil
}

// Fold recomputa totais e refs a partir do vetor em DISCO, pelo tamanho do
// arquivo — antes da recovery, nextRowID ainda é zero, e é a recovery que
// vai reaplicar a ponta por cima. Chamada UMA vez na abertura; o overlay tem
// de estar vazio.
//
// Entrada apontando para um grupo aposentado ou para um id que o dicionário
// não tem é ÓRFÃ: contá-la creditaria um grupo que já saiu. Ela é reescrita
// para WithoutGroup (o Open não tem leitores) e contada em Orphans, que vira
// métrica — órfã não é esperada fora de uma janela de crash que a
// aposentadoria coberta por checkpoint fecha, e um número diferente de zero é
// sinal para investigar.
func (c *Counter) Fold() error {
	if len(c.over) != 0 {
		return fmt.Errorf("contador %s: Fold com overlay pendente", c.def.Name)
	}
	c.dict.mu.RLock()
	live := slices.Clone(c.dict.live)
	c.dict.mu.RUnlock()
	for i := range c.totals {
		atomic.StoreInt64(&c.totals[i], 0)
		atomic.StoreInt64(&c.refs[i], 0)
	}
	// Zerar ANTES do laço é o que torna a Fold idempotente; sem isto uma
	// segunda chamada acumularia sobre a primeira.
	c.rows.Store(0)
	c.total.Store(0)
	// Os agregados acumulam em LOCAIS e são gravados uma vez ao fim: dois
	// atômicos por linha entrariam no orçamento de 36-49 ns/linha da dobra de
	// 1M. Um leitor no meio veria zero, e não um número parcial — mas a Fold
	// roda no Open, sem leitores.
	var rows, total int64
	n := c.vector.Entries()
	for i := uint64(0); i < n; i++ {
		v, _ := c.vector.Get(types.RowID(i))
		g := v &^ conditionBit
		if g == WithoutGroup {
			continue
		}
		if int(g) > len(live) || !live[g-1] {
			c.orphans.Add(1)
			if err := c.vector.Set(types.RowID(i), WithoutGroup); err != nil {
				return err
			}
			continue
		}
		// Depois do continue da órfã, de propósito: a entrada que grupo
		// nenhum recebe também não pode ser creditada ao agregado.
		atomic.AddInt64(&c.refs[g], 1)
		rows++
		if v&conditionBit != 0 {
			atomic.AddInt64(&c.totals[g], 1)
			total++
		}
	}
	c.rows.Store(rows)
	c.total.Store(total)
	now := c.now().UnixMilli()
	for g := 1; g < len(c.refs); g++ {
		if live[g-1] && atomic.LoadInt64(&c.refs[g]) == 0 {
			// O reinício só ESTENDE a visibilidade de um grupo vazio: o
			// instante em que ele esvaziou não é persistido, e o pior caso é
			// ele viver mais um prazo inteiro.
			c.emptySince[g] = now
		}
	}
	return nil
}

// Clear apaga o vetor, o overlay e os totais; o dicionário fica, porque os ids
// são estáveis. É o começo de uma reconstrução: o Store transiciona de novo
// cada linha viva.
func (c *Counter) Clear() error {
	if err := c.vector.Clear(); err != nil {
		return err
	}
	clear(c.over)
	for i := range c.totals {
		atomic.StoreInt64(&c.totals[i], 0)
		atomic.StoreInt64(&c.refs[i], 0)
		c.emptySince[i] = 0
	}
	// Os agregados zeram junto: a reconstrução transiciona de novo cada linha
	// viva, e um agregado que sobrevivesse ao Clear sairia com o dobro do valor
	// ao lado de grupos corretos.
	c.rows.Store(0)
	c.total.Store(0)
	return nil
}

// ApplyOverlay leva o overlay ao mmap e o esvazia. SÓ no checkpoint, DEPOIS
// do fdatasync dos segmentos: é isso que garante que nenhuma página do vetor
// descreve transição de record não durável. Erro no meio deixa o restante no
// overlay para o próximo checkpoint.
func (c *Counter) ApplyOverlay() error {
	for r, v := range c.over {
		if err := c.vector.Set(r, v); err != nil {
			return err
		}
		delete(c.over, r)
	}
	return nil
}

// OverlayLen devolve quantas transições ainda não foram ao mmap.
func (c *Counter) OverlayLen() int { return len(c.over) }

// Transitions é o contador monotônico de transições efetivas (reprocessar
// não conta). Com RecordCoveredTouches, é o que diz se a última mexida de um
// grupo já está durável.
func (c *Counter) Transitions() uint64 { return c.transitions }

// RecordCoveredTouches registra que todas as transições até n estão
// duráveis — em record E em página. O Store a chama no checkpoint, depois do
// Msync do vetor, sob o wmu. Nunca regride.
func (c *Counter) RecordCoveredTouches(n uint64) {
	if n > c.coveredTouches {
		c.coveredTouches = n
	}
}

// CoveredTouches devolve o último valor registrado por RecordCoveredTouches.
func (c *Counter) CoveredTouches() uint64 { return c.coveredTouches }

// AuditSnapshot é o retrato T0 de um contador para a auditoria: os vetores por
// grupo, o limite de ids e os DOIS agregados, todos do MESMO instante.
//
// Viajam numa estrutura, e não em valores de retorno soltos, porque é a
// coincidência de instante que a auditoria compara: ler o agregado depois de
// soltar o writer inventaria divergência onde só houve escrita concorrente.
type AuditSnapshot struct {
	Totals []int64
	Refs   []int64
	// Limit é o maior id emitido mais um, no instante do retrato.
	Limit uint32
	// Rows e Total são Stats.Rows e .Total no mesmo instante — a
	// segunda fonte para o mesmo fato, e a auditoria é a única coisa que a
	// confere.
	Rows  int64
	Total int64
}

// AuditSnapshot devolve o retrato T0 da auditoria, tirado sob o wmu.
func (c *Counter) AuditSnapshot() AuditSnapshot {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	snap := AuditSnapshot{
		Totals: make([]int64, len(c.totals)),
		Refs:   make([]int64, len(c.refs)),
		Limit:  uint32(len(c.dict.keys)) + 1,
		Rows:   c.rows.Load(),
		Total:  c.total.Load(),
	}
	for i := range c.totals {
		snap.Totals[i] = atomic.LoadInt64(&c.totals[i])
		snap.Refs[i] = atomic.LoadInt64(&c.refs[i])
	}
	return snap
}

// Limit é o maior id de grupo emitido mais um.
func (c *Counter) Limit() uint32 { return c.dict.Limit() }

// Group devolve a chave composta, o texto (groupKey), o total e os refs do
// grupo id, e se ele está vivo. Seguro para leitores concorrentes; chave e
// texto são estáveis. Um id que o dicionário já emitiu mas cujos totais ainda
// não cresceram sai como não vivo — janela de microssegundos entre a internação
// e o crescimento, e o leitor a vê como "ainda não existe".
func (c *Counter) Group(id uint32) (key, text []byte, total, refs int64, live bool) {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	if id < 1 || int(id) > len(c.dict.keys) || int(id) >= len(c.totals) {
		return nil, nil, 0, 0, false
	}
	return c.dict.keys[id-1], c.texts[id],
		atomic.LoadInt64(&c.totals[id]), atomic.LoadInt64(&c.refs[id]), c.dict.live[id-1]
}

// ByText resolve o groupKey de um grupo VIVO para o id. Não aloca.
func (c *Counter) ByText(text []byte) (uint32, bool) {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	id, ok := c.byText[string(text)]
	return id, ok
}

// Stats é o retrato do contador para métricas e /v1/stats — a MESMA
// estrutura nos dois.
type Stats struct {
	Name    string
	Groups  uint32 // vivos
	Retired uint32
	Ceiling uint32
	Refused uint64 // grupos novos recusados pelo teto nesta abertura
	Orphans uint64 // entradas órfãs vistas na dobra desta abertura
	Bytes   int64  // vetor + dicionário
	// Rows é quantas linhas vivas apontam para algum grupo — a soma dos refs.
	// Menor que o rows do dataset é diagnóstico direto: são as linhas SEM
	// alguma coluna do groupBy, que o contador não conta por desenho.
	Rows int64
	// Total é quantas linhas satisfazem a condição no dataset inteiro — a soma
	// dos totais dos grupos. Sem `where`, é igual a Rows.
	Total int64
	// AuditDivergences e AuditLastUnix são o resultado da última
	// auditoria conclusiva desta abertura: zero e zero até a primeira.
	AuditDivergences int64
	AuditLastUnix    int64
}

// Stats devolve o retrato corrente. Seguro para leitores concorrentes.
func (c *Counter) Stats() Stats {
	return Stats{
		Name:             c.def.Name,
		Groups:           c.dict.LiveGroups(),
		Retired:          c.dict.RetiredGroups(),
		Ceiling:          c.def.MaxGroups,
		Refused:          c.refused.Load(),
		Orphans:          c.orphans.Load(),
		Bytes:            c.vector.SizeBytes() + c.dict.SizeBytes(),
		Rows:             c.rows.Load(),
		Total:            c.total.Load(),
		AuditDivergences: c.auditDivergences.Load(),
		AuditLastUnix:    c.auditLast.Load(),
	}
}

// RecordAudit guarda o resultado de uma auditoria CONCLUSIVA para as
// métricas: quantas DIVERGÊNCIAS ela achou e quando. Não são só grupos — o
// número soma os grupos divergentes, as linhas cujo grupo não está internado e
// os agregados que discordam do recalculado. Uma auditoria inconclusiva
// (snapshot estourado, contexto cancelado) não passa por aqui — ela não
// mediu nada.
func (c *Counter) RecordAudit(divergences int, when time.Time) {
	c.auditDivergences.Store(int64(divergences))
	c.auditLast.Store(when.Unix())
}

// Msync força as páginas sujas do vetor para disco.
func (c *Counter) Msync() error { return c.vector.Msync() }

// RemoveOld remove os arquivos postos de lado por uma abertura com recreate.
func (c *Counter) RemoveOld() error {
	var errs []error
	for _, name := range []string{dictFileName, vectorFileName} {
		if err := os.Remove(filepath.Join(c.dir, name+oldSuffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close libera o vetor e o dicionário.
func (c *Counter) Close() error {
	return errors.Join(c.vector.Close(), c.dict.Close())
}
