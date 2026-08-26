package executor

import (
	"fmt"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/spill"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrOrcamentoDeOrdenacao é o teto de memória do ORDER BY estourado.
//
// A query é RECUSADA, nunca truncada. Truncar a ENTRADA do sort daria "as N
// primeiras por rowId, depois ordenadas", que não é prefixo de resposta
// nenhuma — é resultado errado com cara de certo, e este motor não tolera isso.
var ErrOrcamentoDeOrdenacao = fmt.Errorf("orçamento de ordenação excedido")

// ordenador acumula as linhas que passaram no predicado e as devolve na ordem
// pedida.
//
// É um operador BLOQUEANTE, e isso muda o contrato do executor: com ORDER BY a
// fonte é drenada por INTEIRO antes da primeira linha sair, porque a linha
// certa pode ser a última lida. O LIMIT deixa de ser teto de TRABALHO e passa a
// ser teto de MEMÓRIA.
//
// ## Lifetime, que é a parte perigosa
//
// Os valores que chegam apontam para o buffer do iterador, válido só até o Next
// seguinte — então tudo é COPIADO. A cópia vai para um arena que cresce por
// append, e append REALOCA: guardar ponteiros para dentro dele produziria
// leituras de memória liberada assim que o arena dobrasse. Por isso as chaves
// guardam OFFSETS (ini, fim) e a janela é resolvida no momento da comparação.
// É a mesma disciplina que o binder usa para os literais.
//
// ## Três modos, UM operador
//
// O caminho descrito acima é o COMPLETO, e é o default. Há mais dois, e nenhum
// deles é um operador separado:
//
//   - TRANSBORDO, quando o orçamento de memória estoura e há lugar em disco: o
//     lote corrente vira um run ordenado, e a emissão é um merge k-way.
//   - TOP-N, escolhido no Open quando `OFFSET + limite` cabe num teto de
//     linhas: um heap limitado que retém só as N melhores. Ver topn.go.
//
// Os três compartilham comparaChave, e isso é requisito e não economia: duas
// regras de ordenação divergiriam no primeiro empate, e o resultado passaria a
// depender de qual modo o Open escolheu.
type ordenador struct {
	chaves []binder.BoundOrderKey
	arena  []byte
	tuplas []tupla
	// vals é achatado: nKeys entradas por tupla, contíguas. Um slice por tupla
	// alocaria uma vez por LINHA, que é exatamente o que não pode acontecer.
	vals  []chaveVal
	nKeys int

	orcamento   int64
	slotsModelo []types.FieldID
	fr          encoding.FieldReader
	pos         int // próxima tupla a emitir

	// area é nil quando não há lugar em disco para transbordar — aí o
	// orçamento estourado RECUSA a query. Não-nil, ele degrada: o lote
	// corrente vira um run ordenado em disco e a acumulação recomeça.
	area *spill.Area
	runs []*spill.Arquivo
	// cabecas é o estado do merge k-way, montado no ordena() e nil enquanto a
	// ordenação couber em memória.
	cabecas  []*cabeca
	pendente int

	// topN é o tamanho do heap limitado quando o caminho rápido está ligado, e
	// ZERO quando o operador usa o caminho completo — inclusive depois de uma
	// degradação. É o único discriminador do modo; ver topn.go.
	topN int
	heap []entradaTopN
	// valsTN é achatado como o vals do caminho completo: nKeys entradas por
	// entrada do heap, contíguas, endereçadas pelo chaveIdx que viaja na
	// entrada.
	valsTN []chaveVal
	// bytesTN é a soma das capacidades dos buffers do heap, mantida por
	// aritmética para que o orçamento não precise varrer N entradas por linha.
	bytesTN int64
	// degradou registra que a query COMEÇOU no top-N e caiu no completo. É
	// diagnóstico: sem ele, a degradação é invisível de fora e nenhum teste
	// consegue provar que ela aconteceu.
	degradou bool
	// slotsDaDegradacao só existe durante a degradação, para reler as entradas
	// retidas sem tocar nos slots do iterador — que naquele instante ainda
	// carregam a linha que acabou de chegar.
	slotsDaDegradacao []slot
}

// cabeca é a posição corrente de UM run durante o merge.
//
// Cada uma tem seus PRÓPRIOS slots e seu próprio leitor, e é isso que permite
// comparar k runs ao mesmo tempo: os payloads de todas as cabeças estão vivos
// simultaneamente, cada um no buffer do seu leitor.
type cabeca struct {
	leitor *spill.Leitor
	slots  []slot
	fr     encoding.FieldReader
	rowID  types.RowID
	valida bool
}

type tupla struct {
	rowID  types.RowID
	payIni int32
	payFim int32
	// chaveIdx é o índice do bloco desta tupla dentro de `vals`, e viaja DENTRO
	// da tupla porque o sort as REORDENA: derivar o bloco da posição no slice
	// funcionaria só antes de ordenar, e depois passaria a ler as chaves de
	// outra linha — resultado errado, em silêncio.
	chaveIdx int32
}

// chaveVal é um valor de chave já copiado. `presente` significa "a coluna está
// na row E no tipo esperado" — as duas condições colapsam de propósito, porque
// valor de tipo divergente não é ordenável no domínio declarado e vai para o
// mesmo grupo dos ausentes, como já acontece no predicado.
type chaveVal struct {
	presente bool
	i64      int64
	ini      int32
	fim      int32
	// direto é o valor já resolvido, e vale nos dois modos em que os bytes NÃO
	// vivem no arena: o merge de runs, onde eles estão no buffer do leitor da
	// cabeça, e o top-N, onde estão no buffer da própria entrada. No caminho
	// completo em memória ele é nil e a janela (ini, fim) manda — ver o godoc
	// do ordenador sobre por que o arena não admite ponteiros.
	//
	// No top-N o `direto` é RE-RESOLVIDO a cada substituição da entrada, porque
	// a reescrita pode realocar o buffer; (ini, fim) continuam sendo a forma
	// durável, e o ponteiro a forma derivada.
	direto []byte
}

func novoOrdenador(chaves []binder.BoundOrderKey, orcamento int64, area *spill.Area, ids []types.FieldID) *ordenador {
	return &ordenador{
		chaves: chaves, nKeys: len(chaves), orcamento: orcamento,
		area: area, slotsModelo: ids, pendente: -1}
}

// acrescenta copia a row para o arena. Devolve erro se o orçamento estourar.
func (o *ordenador) acrescenta(slots []slot, rowID types.RowID) error {
	if o.topN > 0 {
		return o.acrescentaTopN(slots, rowID)
	}
	ini := int32(len(o.arena))
	var err error
	for i := range slots {
		if !slots[i].Present {
			continue
		}
		// AppendField exige FieldID crescente e sem duplicata: os slots vêm do
		// binder já nessa ordem, e é a mesma invariante que o payload real
		// honra. Por isso a releitura pode ser o `coleta` de sempre, sem uma
		// linha de desserialização nova.
		if o.arena, err = encoding.AppendField(o.arena, slots[i].ID, slots[i].Val); err != nil {
			return fmt.Errorf("executor: serializando a row %d para ordenar: %w", rowID, err)
		}
	}
	o.tuplas = append(o.tuplas, tupla{
		rowID: rowID, payIni: ini, payFim: int32(len(o.arena)),
		chaveIdx: int32(len(o.vals))})

	for _, k := range o.chaves {
		s := &slots[k.Slot]
		cv := chaveVal{presente: s.Present && s.Val.Type == k.Want}
		if cv.presente {
			if k.Kind == binder.CompareBytes {
				cv.ini = int32(len(o.arena))
				o.arena = append(o.arena, s.Val.Bytes...)
				cv.fim = int32(len(o.arena))
			} else {
				cv.i64 = s.Val.I64
			}
		}
		o.vals = append(o.vals, cv)
	}

	if o.usado() > o.orcamento {
		if o.area == nil {
			// Sem lugar em disco: RECUSA. Truncar a entrada do sort daria "as
			// N primeiras por rowId, depois ordenadas", que não é prefixo de
			// resposta nenhuma.
			return fmt.Errorf("%w: %d bytes acima do teto de %d; "+
				"reduza o resultado com WHERE, ou peça menos colunas",
				ErrOrcamentoDeOrdenacao, o.usado(), o.orcamento)
		}
		return o.transborda()
	}
	return nil
}

// transborda grava o lote corrente como um run ORDENADO em disco e zera a
// memória.
//
// Ordenar ANTES de gravar é o que torna o merge final possível em memória
// O(k): um run desordenado obrigaria a reler o arquivo inteiro para achar o
// mínimo, a cada linha emitida.
func (o *ordenador) transborda() error {
	o.ordenaEmMemoria()
	arq, err := o.area.Novo()
	if err != nil {
		return err
	}
	o.runs = append(o.runs, arq)
	for i := range o.tuplas {
		t := &o.tuplas[i]
		if err := arq.Escreve(t.rowID, o.arena[t.payIni:t.payFim]); err != nil {
			return err
		}
	}
	if err := arq.Sela(); err != nil {
		return err
	}
	// Zera SEM devolver a capacidade: os buffers voltam a ser preenchidos
	// imediatamente, e realocá-los a cada run seria alocação proporcional ao
	// número de transbordos.
	o.arena = o.arena[:0]
	o.tuplas = o.tuplas[:0]
	o.vals = o.vals[:0]
	return nil
}

// usado é a memória do operador. Conta o arena e as duas tabelas — sem elas o
// teto mediria só metade do que o operador segura.
//
// No modo top-N o arena e as tabelas estão vazios e quem responde é o heap: os
// buffers das entradas MAIS o custo fixo delas. Deixar o heap de fora faria o
// teto de bytes valer só para o caminho completo, e a linha de 4 MiB de JSON
// passaria a derrubar o processo exatamente pela porta que o modo novo abriu.
func (o *ordenador) usado() int64 {
	const bytesPorTupla = 24 // rowID(8) + três int32 + padding
	const bytesPorChave = 24 // bool+pad(8) + i64(8) + dois int32
	u := int64(len(o.arena)) +
		int64(len(o.tuplas))*bytesPorTupla +
		int64(len(o.vals))*bytesPorChave
	if o.topN > 0 {
		u += o.bytesTN +
			int64(len(o.heap))*bytesPorEntradaTopN +
			int64(len(o.valsTN))*bytesPorChave
	}
	return u
}

// ordena fecha a acumulação. Depois dela o arena não cresce mais, e por isso as
// janelas de bytes podem ser resolvidas com segurança.
//
// Sem transbordo, ordena o lote único em memória — o caminho de sempre. Com
// transbordo, grava o último lote como run e monta o merge k-way.
func (o *ordenador) ordena() error {
	if o.topN > 0 {
		o.ordenaTopN()
		return nil
	}
	if len(o.runs) == 0 {
		o.ordenaEmMemoria()
		return nil
	}
	if len(o.tuplas) > 0 {
		if err := o.transborda(); err != nil {
			return err
		}
	}
	return o.montaMerge()
}

func (o *ordenador) ordenaEmMemoria() {
	slices.SortFunc(o.tuplas, func(a, b tupla) int {
		return o.compara(&a, &b)
	})
}

// montaMerge abre um leitor por run e posiciona cada cabeça no primeiro
// registro.
func (o *ordenador) montaMerge() error {
	o.cabecas = make([]*cabeca, 0, len(o.runs))
	for _, r := range o.runs {
		l, err := r.Le()
		if err != nil {
			return err
		}
		c := &cabeca{leitor: l, slots: novosSlots(o.slotsModelo)}
		if err := c.avanca(); err != nil {
			return err
		}
		o.cabecas = append(o.cabecas, c)
	}
	return nil
}

// avanca lê o registro seguinte do run e decodifica os slots.
func (c *cabeca) avanca() error {
	id, payload, ok, err := c.leitor.Proximo()
	if err != nil {
		return err
	}
	if !ok {
		c.valida = false
		return nil
	}
	if err := coleta(c.slots, &c.fr, payload); err != nil {
		return fmt.Errorf("executor: relendo a row %d do spill: %w", id, err)
	}
	c.rowID, c.valida = id, true
	return nil
}

// proximaDoMerge escolhe a menor cabeça e a copia para os slots do chamador.
//
// A varredura é LINEAR sobre as k cabeças, sem heap, pelo mesmo argumento que o
// unionIter do candidate.go já registra: com k pequeno — e k aqui é o número de
// transbordos — a varredura linear é mais rápida e não aloca.
//
// O AVANÇO DA CABEÇA VENCEDORA É ADIADO para a chamada seguinte, e essa é a
// parte que não pode ser "simplificada": os slots do chamador apontam para o
// buffer do leitor daquela cabeça, e avançá-la aqui invalidaria os bytes ANTES
// de o consumidor os ler. Adiando, o payload vale até o próximo Next — que é
// exatamente o contrato que o executor já publica.
func (o *ordenador) proximaDoMerge(slots []slot) (types.RowID, bool, error) {
	if o.pendente >= 0 {
		if err := o.cabecas[o.pendente].avanca(); err != nil {
			return 0, false, err
		}
		o.pendente = -1
	}
	vencedora := -1
	for i, c := range o.cabecas {
		if !c.valida {
			continue
		}
		if vencedora < 0 || o.comparaCabecas(c, o.cabecas[vencedora]) < 0 {
			vencedora = i
		}
	}
	if vencedora < 0 {
		return 0, false, nil
	}
	c := o.cabecas[vencedora]
	copy(slots, c.slots)
	o.pendente = vencedora
	return c.rowID, true, nil
}

// comparaCabecas aplica as mesmas chaves, na mesma ordem, com o mesmo
// desempate por rowID do caminho em memória. Duas regras de ordenação no mesmo
// operador divergiriam no primeiro empate, e o resultado dependeria de ter
// transbordado ou não.
func (o *ordenador) comparaCabecas(a, b *cabeca) int {
	for i := range o.chaves {
		k := &o.chaves[i]
		va := doSlot(&a.slots[k.Slot], k)
		vb := doSlot(&b.slots[k.Slot], k)
		if c := o.comparaChave(k, &va, &vb); c != 0 {
			return c
		}
	}
	return int(a.rowID) - int(b.rowID)
}

// doSlot converte um slot lido para a forma que o comparador entende. As
// janelas de bytes apontam para o buffer do leitor da cabeça, que está vivo —
// por isso ini/fim não servem aqui, e o valor viaja direto.
func doSlot(s *slot, k *binder.BoundOrderKey) chaveVal {
	cv := chaveVal{presente: s.Present && s.Val.Type == k.Want}
	if cv.presente {
		cv.i64 = s.Val.I64
		cv.direto = s.Val.Bytes
	}
	return cv
}

// compara aplica as chaves em ordem, com DESEMPATE FINAL POR rowID.
//
// O desempate não é estética: sem ordem TOTAL, `LIMIT 10 OFFSET 10` sobre uma
// coluna com empates devolve páginas que se sobrepõem e omitem linhas — a
// segunda execução do sort pode arrumar os empatados de outro jeito. Com o
// rowID como última chave, a ordem é determinística e a paginação fecha.
func (o *ordenador) compara(a, b *tupla) int {
	ia, ib := int(a.chaveIdx), int(b.chaveIdx)
	for i := range o.chaves {
		k := &o.chaves[i]
		va, vb := &o.vals[ia+i], &o.vals[ib+i]
		if c := o.comparaChave(k, va, vb); c != 0 {
			return c
		}
	}
	return int(a.rowID) - int(b.rowID)
}

func (o *ordenador) comparaChave(k *binder.BoundOrderKey, a, b *chaveVal) int {
	// AUSENTE VAI POR ÚLTIMO NAS DUAS DIREÇÕES, e é o único default em que
	// `ORDER BY x DESC LIMIT 10` significa "os 10 maiores" e não "10 linhas sem
	// valor". Diverge de Postgres e Oracle (que invertem no DESC) e de MySQL e
	// SQLite (que tratam NULL como o menor) — deliberadamente, e com
	// NULLS FIRST disponível para quem quiser a convenção alheia.
	if !a.presente || !b.presente {
		switch {
		case !a.presente && !b.presente:
			return 0
		case k.NullsFirst:
			if !a.presente {
				return -1
			}
			return 1
		default:
			if !a.presente {
				return 1
			}
			return -1
		}
	}
	c := compara(k.Kind, o.valor(k, a), o.valor(k, b))
	if k.Desc {
		return -c
	}
	return c
}

// valor materializa o types.Value para a comparação. Não aloca: para bytes é
// uma janela no arena, e para os demais tipos é um campo escalar.
func (o *ordenador) valor(k *binder.BoundOrderKey, c *chaveVal) types.Value {
	if k.Kind == binder.CompareBytes {
		if c.direto != nil {
			return types.Value{Bytes: c.direto}
		}
		return types.Value{Bytes: o.arena[c.ini:c.fim]}
	}
	return types.Value{I64: c.i64}
}

// proxima devolve a próxima row já ordenada, recarregando os slots.
//
// A releitura usa o MESMO `coleta` do caminho normal, sobre a mini-payload que
// o acrescenta serializou. Não há segundo formato nem segundo desserializador —
// e é isso que garante que uma linha ordenada e a mesma linha sem ORDER BY
// produzam exatamente os mesmos valores.
func (o *ordenador) proxima(slots []slot) (types.RowID, bool, error) {
	if o.topN > 0 {
		return o.proximaDoTopN(slots)
	}
	if o.cabecas != nil {
		return o.proximaDoMerge(slots)
	}
	if o.pos >= len(o.tuplas) {
		return 0, false, nil
	}
	t := o.tuplas[o.pos]
	o.pos++
	if err := coleta(slots, &o.fr, o.arena[t.payIni:t.payFim]); err != nil {
		return 0, false, fmt.Errorf("executor: relendo a row %d ordenada: %w", t.rowID, err)
	}
	return t.rowID, true, nil
}
