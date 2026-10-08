package executor

import (
	"context"
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/query/spill"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// RowIterator é o contrato de streaming: nada do resultado é materializado.
type RowIterator interface {
	// Columns descreve a PROJEÇÃO e é válida desde o Open, antes do primeiro
	// Next — inclusive quando o resultado é vazio.
	//
	// Existe porque o NDJSON não carrega tipos, e o ResultSetMetaData do §71
	// precisa deles ANTES da primeira row: derivá-los do dado é impossível
	// para a coluna que é NULL em todas as linhas, e é tarde demais para quem
	// já começou a receber o stream.
	Columns() []ColMeta
	// Next avança para a próxima row. false = fim, LIMIT, teto, cancelamento
	// ou erro — Err() distingue.
	Next() bool
	// Row é válida SOMENTE até o próximo Next; depois do fim devolve Row zero.
	Row() Row
	// Err é nil quando a iteração terminou por esgotamento ou por LIMIT
	// explícito.
	Err() error
	// Close é idempotente e fecha a fonte upstream.
	Close() error
}

// Options configura a execução.
type Options struct {
	Store   Store
	Metrics *Metrics // opcional
	// BatchSize é a CADÊNCIA do check de contexto em planos indexados — não um
	// buffer de rows. Não existe buffer de rows (§66); quem introduzir um
	// violou o requisito de memória. 0 usa 256; valores não potência de dois
	// são arredondados para cima, porque a cadência é medida com máscara.
	BatchSize int
	// SortMemory é o teto de memória do operador de ORDER BY, em bytes. 0 usa
	// sortMemoryPadrao.
	//
	// É teto de MEMÓRIA e não de linhas: o custo de uma linha ordenada depende
	// do tamanho dos valores, e um teto em linhas trataria uma coluna de 8
	// bytes e um documento de 4 KiB como a mesma coisa.
	//
	// AINDA NÃO TEM CHAVE DE CONFIGURAÇÃO, e é deliberado: o lugar dele é o
	// orçamento de memória por query do §80
	// (`runtime.query.maxBufferPerQuery`), e inventar uma segunda chave
	// paralela agora deixaria duas fontes de verdade para "quanta memória uma
	// query pode usar". Até lá vale o padrão, e o operador só o vê pela
	// mensagem de recusa.
	SortMemory int64
	// SpillBytes é o teto de DISCO do transbordo de UMA query. 0 = sem teto.
	SpillBytes int64
	// TopNMax é o maior N (= OFFSET + limite efetivo) que ainda escolhe o
	// caminho rápido de top-N do ORDER BY. 0 usa ceilingTopNPadrao; NEGATIVO
	// desliga o caminho rápido e força o ORDER BY completo.
	//
	// É teto de LINHAS retidas, e não de bytes, porque é decidido no Open — não
	// há uma linha para medir ainda. O teto de bytes é o SortMemory, e continua
	// valendo dentro do modo rápido.
	//
	// Pelo mesmo motivo do SortMemory, AINDA NÃO TEM CHAVE DE CONFIGURAÇÃO: o
	// lugar dela é o orçamento por query do §80. O valor negativo existe para
	// quem precisa comparar os dois caminhos sobre a MESMA query — é o que
	// torna TestTopNDaOMesmoResultadoQueOSortCompleto um diferencial de verdade
	// e não uma comparação do caminho consigo mesmo.
	TopNMax int64
}

// DiretorioOfSpill é a capability opcional do Store que diz onde o transbordo
// pode escrever.
//
// Store que não a implementa — ou que devolve vazio — faz o ORDER BY RECUSAR ao
// estourar o orçamento de memória, em vez de degradar para disco. É o desfecho
// certo: sem lugar declarado para escrever, escrever em os.TempDir seria o
// motor inventando onde pôr dado do usuário.
type DiretorioOfSpill interface {
	DiretorioOfSpill() string
}

// sortMemoryPadrao é o teto do ORDER BY quando o dataset não o declara.
//
// 64 MiB é folgado para o uso normal (100 mil linhas de poucas colunas cabem
// com sobra) e pequeno o bastante para que uma query descuidada seja RECUSADA
// em vez de derrubar o processo — que é o desfecho que o §128 exige quando o
// buffer deixa de ser proporcional ao batch.
const sortMemoryPadrao = 64 << 20

const batchPadrao = 256

// Open valida o plano, compila predicado e projeção e devolve o iterador.
//
// NENHUMA leitura de dado acontece aqui: a primeira row só é lida no primeiro
// Next — inclusive para LIMIT 0, que não deve tocar o Store.
func Open(ctx context.Context, bq *binder.BoundQuery, plan planner.Plan, opts Options) (RowIterator, error) {
	switch {
	case bq == nil:
		return nil, fmt.Errorf("%w: query ligada nula", ErrInvalidPlan)
	case plan == nil:
		return nil, fmt.Errorf("%w: plano nulo", ErrInvalidPlan)
	case opts.Store == nil:
		return nil, fmt.Errorf("%w: store nulo", ErrInvalidPlan)
	case len(bq.Columns) == 0:
		return nil, fmt.Errorf("%w: projeção vazia", ErrInvalidPlan)
	case bq.Timeout <= 0:
		// Prazo ausente é BUG do binder, não caso a tratar: o binder o copia do
		// schema compilado, e schema.validate já recusa `query.timeout` não
		// positivo. Aqui existia um `defaultQueryTimeout = 30s` que cobria este
		// buraco — uma SEGUNDA fonte da verdade para o prazo, invisível, que
		// venceria em silêncio o teto configurado no dia em que o caminho de
		// cima quebrasse. Errar alto é o que impede isso.
		return nil, fmt.Errorf("%w: query ligada sem prazo (bq.Timeout = %s)", ErrInvalidPlan, bq.Timeout)
	}

	it := &iterador{
		st:      opts.Store,
		metrics: opts.Metrics,
		slots:   novosSlots(bq.Slots.IDs),
		eff:     bq.EffectiveLimit,
		pular:   bq.Offset,
		// Truncamento por teto IMPLÍCITO: só quando a parada não veio de um
		// LIMIT que o usuário escreveu e que é menor que o teto.
		ceilingImplicito: !bq.HasLimit || bq.Limit > bq.MaxRows,
	}
	if len(bq.OrderBy) > 0 {
		for i := range bq.OrderBy {
			k := &bq.OrderBy[i]
			if k.Field == nil || k.Kind == binder.CompareUnset {
				return nil, fmt.Errorf("%w: chave de ordenação sem campo ou sem kind", ErrInvalidPlan)
			}
			if k.Slot < 0 || k.Slot >= len(it.slots) {
				return nil, fmt.Errorf("%w: chave de ordenação %q com slot %d fora de [0,%d)",
					ErrInvalidPlan, k.Field.Name, k.Slot, len(it.slots))
			}
		}
		ceiling := opts.SortMemory
		if ceiling <= 0 {
			ceiling = sortMemoryPadrao
		}
		var area *spill.Area
		if d, ok := opts.Store.(DiretorioOfSpill); ok {
			if dir := d.DiretorioOfSpill(); dir != "" {
				a, err := spill.NewArea(dir, opts.SpillBytes)
				if err != nil {
					return nil, err
				}
				area = a
			}
		}
		it.ord = newOrdenador(bq.OrderBy, ceiling, area, bq.Slots.IDs)
		// O modo é decidido AQUI, e não na primeira linha, porque ele depende
		// só da forma da query — LIMIT explícito e OFFSET —, e trocar de modo
		// no meio da varredura obrigaria a reprocessar o que já passou.
		if n, ok := sizeOfTopN(bq, opts.TopNMax); ok {
			it.ord.activeTopN(n)
			if opts.Metrics != nil {
				// Uma vez por QUERY, direto: não é caminho quente, e o
				// acumulador local do iterador existe para o que roda por
				// linha.
				opts.Metrics.TopN.Add(1)
			}
		}
	}

	cols := make([]ColView, len(bq.Columns))
	slotDaColumn := make([]int32, len(bq.Columns))
	for i, c := range bq.Columns {
		if c.Field == nil {
			return nil, fmt.Errorf("%w: coluna %d com campo nulo", ErrInvalidPlan, i)
		}
		if c.Slot < 0 || c.Slot >= len(it.slots) {
			return nil, fmt.Errorf("%w: coluna %q com slot %d fora de [0,%d)",
				ErrInvalidPlan, c.Name, c.Slot, len(it.slots))
		}
		// Type é constante por query: o tipo DECLARADO (ou o fixado, para campo
		// auto). Não é o tipo físico — esse vive em Val.Type, por row.
		cols[i] = ColView{
			Name: c.Name,
			Type: c.Field.EffectiveType(),
			// Chave e sequence são as duas colunas que o materializer garante
			// não-nulas em toda row viva, recusando o evento que as traga
			// ausentes ou nulas.
			Nullable: !c.Field.IsKey && !c.Field.IsSequence,
		}
		slotDaColumn[i] = int32(c.Slot)
	}
	it.cols, it.slotDaColumn = cols, slotDaColumn

	pred, err := compile(bq.Where, len(it.slots))
	if err != nil {
		// O erro do avaliador é reembrulhado em ErrInvalidPlan porque a
		// classificação é do EXECUTOR: quem consome (o servidor) decide o
		// código HTTP por esse sentinela, e um programa mal formado continua
		// sendo bug de plan-time, venha de onde vier.
		return nil, fmt.Errorf("%w: %w", ErrInvalidPlan, err)
	}
	it.pred = pred

	// Máscara da cadência de ctx: 0 no full scan (checa a cada candidato),
	// BatchSize-1 nos planos indexados.
	it.mask = int64(potenciaOfDois(opts.BatchSize) - 1)
	cadencePlan := plan
	if ranged, ok := plan.(*planner.HistoryTimeRangePlan); ok {
		cadencePlan = ranged.Source
	}
	if _, full := cadencePlan.(*planner.FullScanPlan); full {
		it.mask = 0
	}

	// bq.Timeout é o prazo DEFAULT do dataset, e só vale quando o chamador não
	// trouxe prazo próprio.
	//
	// Antes ele era aplicado SEMPRE, e a consequência era silenciosa: um
	// servidor que já tivesse resolvido o prazo efetivo do request contra
	// `query.maxTimeout` — cinco minutos, digamos — veria o WithTimeout de
	// baixo cortá-lo de volta para os 30s do `query.timeout`, porque o menor
	// deadline vence. A consulta longa terminaria em TIMEOUT com o teto
	// configurado dizendo que ela podia. Quem tem deadline já decidiu a
	// política; repeti-la aqui só pode encurtá-la.
	if _, hasPrazo := ctx.Deadline(); hasPrazo {
		it.ctx, it.cancel = context.WithCancel(ctx)
	} else {
		it.ctx, it.cancel = context.WithTimeout(ctx, bq.Timeout)
	}

	// O snapshot é fixado ANTES da fonte, e uma vez só para a consulta
	// inteira: é ele que dá ao plano indexado e ao full scan o MESMO universo,
	// e é a ordem que importa — aberta a fonte, o teto já teria escapado.
	snap := opts.Store.OpenSnapshot()
	it.snap = snap

	// A fonte é aberta POR ÚLTIMO: qualquer erro acima não pode deixar
	// iteradores de índice abertos e sem dono.
	src, err := sourceOfPlanoContext(it.ctx, plan, opts.Store, opts.Metrics, snap)
	if err != nil {
		it.cancel()
		// O Open que falha DEPOIS de abrir o snapshot é o único caminho em
		// que ninguém chamará o Close: o iterador não chega ao caller. Sem
		// esta linha, toda consulta recusada na abertura da fonte deixaria um
		// leitor fantasma na contagem.
		it.snapFechado = true
		opts.Store.CloseSnapshot(snap)
		return nil, err
	}
	it.src = src
	return it, nil
}

// potenciaOfDois arredonda para cima até a próxima potência de dois (mínimo 1),
// porque a cadência é medida com máscara de bits.
func potenciaOfDois(n int) int64 {
	if n <= 0 {
		n = batchPadrao
	}
	p := int64(1)
	for p < int64(n) {
		p <<= 1
	}
	return p
}

type iterador struct {
	st      Store
	src     rowSource
	metrics *Metrics

	// snap é o universo desta consulta, guardado para ser devolvido no Close.
	snap        types.Snapshot
	snapFechado bool

	ctx    context.Context
	cancel context.CancelFunc

	slots        []slot
	cols         []ColView
	slotDaColumn []int32
	pred         *programa
	fr           encoding.FieldReader

	rowBuf []byte

	eff              int64 // teto efetivo de linhas (min(LIMIT, maxRows))
	ceilingImplicito bool
	emitidas         int64
	pular            int64 // linhas do OFFSET ainda a descartar
	// ord é nil quando não há ORDER BY, e a query segue estritamente
	// streaming. Não-nil, o executor troca de modo: drena e depois emite.
	ord    *ordenador
	drenou bool
	n      int64 // candidatos consumidos, para a cadência de ctx
	mask   int64

	cnt      counters
	done     bool
	fechou   bool
	err      error
	errClose error
}

func (it *iterador) Next() bool {
	if it.done {
		return false
	}
	if it.emitidas >= it.eff {
		// Inclui LIMIT 0: termina antes de tocar o Store.
		it.termina(nil)
		return false
	}
	if it.ord != nil {
		return it.proximaOrdenada()
	}
	for {
		if it.mask == 0 || it.n&it.mask == 0 {
			if err := it.ctx.Err(); err != nil {
				it.countCancel()
				it.termina(fmt.Errorf("executor: query interrompida após %d candidatos: %w", it.n, err))
				return false
			}
		}

		rec, buf, more, live, err := it.src.advance(it.rowBuf)
		it.rowBuf = buf // obrigatório em TODOS os caminhos, inclusive erro
		if err != nil {
			it.termina(err)
			return false
		}
		if !more {
			it.termina(nil)
			return false
		}
		it.n++
		it.cnt.candidates++
		if !live {
			// Row sem location ou tombstoned: o índice a devolveu porque as
			// postings não são limpas. Conta como candidato, não como row.
			it.cnt.deadSkipped++
			continue
		}

		if err := coleta(it.slots, &it.fr, rec.Payload); err != nil {
			it.termina(fmt.Errorf("executor: payload da row %d: %w", rec.RowID, err))
			return false
		}
		if !it.pred.Evaluate(it.slots, &it.cnt.typeMismatch) {
			it.cnt.rejected++
			continue
		}

		if it.pular > 0 {
			// OFFSET: a linha PASSOU no predicado e é descartada mesmo assim.
			// O contador de skip vive aqui, e não no ramo do predicado acima,
			// porque pular não é rejeitar — o WHERE já disse que ela serve.
			it.pular--
			continue
		}
		it.projeta()
		it.emitidas++
		it.cnt.emitted++
		if it.emitidas >= it.eff {
			// Early-exit: fecha a fonte AGORA, não no Next seguinte. Uma query
			// com LIMIT 3 sobre um milhão de candidatos não pode segurar o
			// iterador do índice até o consumidor decidir chamar Next de novo.
			it.encerra()
			it.markTruncado()
		}
		return true
	}
}

// proximaOrdenada é o Next no modo ORDER BY.
//
// Na primeira chamada DRENA a fonte inteira; nas seguintes só emite. É o que
// torna o operador bloqueante, e a razão é aritmética: a menor linha pode ser a
// última lida, então não há como emitir nada antes de ver todas.
//
// O OFFSET é aplicado DEPOIS de ordenar — pular antes ordenaria o conjunto
// errado. O LIMIT continua sendo teto de linhas ENTREGUES, e agora corta um
// PREFIXO LEGÍTIMO da resposta ordenada, em vez de "as N primeiras por rowId".
func (it *iterador) proximaOrdenada() bool {
	if !it.drenou {
		if !it.drena() {
			return false
		}
		it.drenou = true
		if err := it.ord.ordena(); err != nil {
			it.termina(err)
			return false
		}
		// A fonte já não serve para nada: fecha AGORA, e não no Close, para não
		// segurar o iterador do índice durante a emissão inteira.
		it.encerra()
		for it.pular > 0 {
			if _, ok, err := it.ord.proxima(it.slots); err != nil {
				it.termina(err)
				return false
			} else if !ok {
				break
			}
			it.pular--
		}
	}
	_, ok, err := it.ord.proxima(it.slots)
	if err != nil {
		it.termina(err)
		return false
	}
	if !ok {
		it.termina(nil)
		return false
	}
	it.projeta()
	it.emitidas++
	it.cnt.emitted++
	if it.emitidas >= it.eff {
		it.markTruncado()
	}
	return true
}

// drena consome a fonte inteira alimentando o ordenador. Devolve false quando
// terminou por erro (o iterador já está encerrado).
func (it *iterador) drena() bool {
	for {
		if it.mask == 0 || it.n&it.mask == 0 {
			if err := it.ctx.Err(); err != nil {
				it.countCancel()
				// A mensagem NOMEIA A FASE: com ORDER BY, "interrompida após N
				// candidatos" sem mais nada faria o operador procurar lentidão
				// na entrega, quando o tempo foi todo na varredura.
				it.termina(fmt.Errorf("executor: query interrompida ao ordenar, após %d candidatos: %w", it.n, err))
				return false
			}
		}
		rec, buf, more, live, err := it.src.advance(it.rowBuf)
		it.rowBuf = buf
		if err != nil {
			it.termina(err)
			return false
		}
		if !more {
			return true
		}
		it.n++
		it.cnt.candidates++
		if !live {
			it.cnt.deadSkipped++
			continue
		}
		if err := coleta(it.slots, &it.fr, rec.Payload); err != nil {
			it.termina(fmt.Errorf("executor: payload da row %d: %w", rec.RowID, err))
			return false
		}
		if !it.pred.Evaluate(it.slots, &it.cnt.typeMismatch) {
			it.cnt.rejected++
			continue
		}
		if err := it.ord.acrescenta(it.slots, rec.RowID); err != nil {
			it.termina(err)
			return false
		}
	}
}

// projeta copia os valores dos slots para as colunas de saída.
func (it *iterador) projeta() {
	for j, s := range it.slotDaColumn {
		sl := &it.slots[s]
		if sl.Present {
			it.cols[j].Null, it.cols[j].Val = false, sl.Val
			continue
		}
		// Zerar o Val é obrigatório: o coleta reseta só o `present`, nunca o
		// `val` — é o que dá o zero alocação —, então o slot de um campo
		// ausente carrega literalmente o valor da row ANTERIOR, e este
		// zeramento é a única coisa entre os dois.
		//
		// Hoje todo consumidor checa Null antes de olhar Val, o que mascara a
		// falha; ela apareceria no primeiro que fizesse o contrário, e o
		// candidato é o driver JDBC (getInt() devolve o valor, wasNull()
		// devolve o flag, nessa ordem).
		it.cols[j].Null, it.cols[j].Val = true, types.Value{}
	}
}

// markTruncado registra o erro de teto quando a parada veio do limite
// IMPLÍCITO (query.maxRows). Resultado truncado apresentado como completo é
// resposta errada, e o erro depois das rows é o menor mal.
//
// Precisão deliberada: o erro significa "parou no teto", e aparece também
// quando o resultado tinha EXATAMENTE maxRows linhas. Distinguir os dois casos
// exigiria continuar avaliando o predicado até achar a próxima linha viva —
// potencialmente o dataset inteiro, que é exatamente o custo que o teto existe
// para evitar. Um alarme falso é aceitável; um resultado truncado silencioso
// não é.
func (it *iterador) markTruncado() {
	if !it.ceilingImplicito {
		return
	}
	it.err = fmt.Errorf("%w: %d", ErrMaxRowsExceeded, it.eff)
	if it.metrics != nil {
		it.metrics.Truncated.Add(1)
	}
}

func (it *iterador) countCancel() {
	if it.metrics != nil {
		it.metrics.Canceled.Add(1)
	}
}

// termina é o ÚNICO estado terminal: fecha upstream na hora, publica o erro e
// libera o contexto. Depois dele, Next é false e Row é zero sem tocar o Store.
func (it *iterador) termina(err error) {
	if it.done {
		return
	}
	it.done = true
	if err != nil && it.err == nil {
		it.err = err
	}
	it.encerra()
	it.releaseSpill()
}

// releaseSpill remove os arquivos de transbordo desta query.
//
// SEPARADO do encerra de propósito, e a separação é o defeito que ela impede: o
// encerra é chamado logo DEPOIS de ordenar, para soltar o iterador do índice
// enquanto a emissão ainda vai acontecer — e nesse instante os runs em disco
// ainda estão sendo LIDOS pelo merge. Fechá-los ali apagaria os arquivos sob os
// pés do próprio operador.
//
// Roda no termina (fim normal, erro e timeout) e no Close (o defer de quem
// consome), que juntos cobrem todos os caminhos de saída.
func (it *iterador) releaseSpill() {
	if it.ord == nil || it.ord.area == nil {
		return
	}
	if err := it.ord.area.Close(); err != nil {
		it.errClose = err
		if it.err == nil {
			it.err = err
		}
	}
	it.ord.area = nil
}

// encerra fecha a fonte e cancela o contexto derivado. Idempotente: o
// early-exit do LIMIT chama antes do estado terminal.
func (it *iterador) encerra() {
	if it.fechou {
		return
	}
	it.fechou = true
	if it.src != nil {
		if err := it.src.close(); err != nil {
			it.errClose = err
			if it.err == nil {
				it.err = err
			}
		}
	}

	// cancel SEMPRE, inclusive na parada por LIMIT: senão o timer do
	// WithTimeout fica vivo até o deadline em toda query com LIMIT.
	it.cancel()
	// A degradação do top-N é decidida lá dentro do ordenador, durante a
	// drenagem; o flush é o único ponto por onde toda query passa uma vez.
	if it.ord != nil && it.ord.degradou {
		it.cnt.topNDegradado = 1
	}
	it.cnt.flush(it.metrics)
}

// Columns devolve uma CÓPIA do metadado da projeção. A cópia é deliberada: as
// ColView internas são reescritas a cada Next, e entregar o slice interno faria
// o chamador guardar um metadado que muda sob os pés dele. É uma alocação por
// QUERY, fora do caminho quente.
func (it *iterador) Columns() []ColMeta {
	out := make([]ColMeta, len(it.cols))
	for i, c := range it.cols {
		out[i] = ColMeta{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
	}
	return out
}

func (it *iterador) Row() Row {
	if it.done {
		return Row{}
	}
	return Row{cols: it.cols}
}

func (it *iterador) Err() error { return it.err }

// Close devolve apenas o erro de FECHAMENTO upstream — não o erro da query.
// Um Close que devolvesse ErrMaxRowsExceeded faria `defer it.Close()` parecer
// falha de recurso.
func (it *iterador) Close() error {
	it.done = true
	it.encerra()
	it.releaseSpill()
	// O snapshot é devolvido DEPOIS do encerra: enquanto a fonte upstream
	// pode estar lendo, as versões que ela alcança têm de continuar retidas.
	// O guard de idempotência é do contrato do Close (ele é chamado por
	// defer e de novo pelo caller), e sem ele um Close a mais tiraria da
	// contagem um leitor que nunca existiu.
	if !it.snapFechado {
		it.snapFechado = true
		it.st.CloseSnapshot(it.snap)
	}
	return it.errClose
}
