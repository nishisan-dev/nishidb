package executor

import (
	"fmt"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
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
type ordenador struct {
	chaves []binder.BoundOrderKey
	arena  []byte
	tuplas []tupla
	// vals é achatado: nKeys entradas por tupla, contíguas. Um slice por tupla
	// alocaria uma vez por LINHA, que é exatamente o que não pode acontecer.
	vals  []chaveVal
	nKeys int

	orcamento int64
	fr        encoding.FieldReader
	pos       int // próxima tupla a emitir
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
}

func novoOrdenador(chaves []binder.BoundOrderKey, orcamento int64) *ordenador {
	return &ordenador{chaves: chaves, nKeys: len(chaves), orcamento: orcamento}
}

// acrescenta copia a row para o arena. Devolve erro se o orçamento estourar.
func (o *ordenador) acrescenta(slots []slot, rowID types.RowID) error {
	ini := int32(len(o.arena))
	var err error
	for i := range slots {
		if !slots[i].present {
			continue
		}
		// AppendField exige FieldID crescente e sem duplicata: os slots vêm do
		// binder já nessa ordem, e é a mesma invariante que o payload real
		// honra. Por isso a releitura pode ser o `coleta` de sempre, sem uma
		// linha de desserialização nova.
		if o.arena, err = encoding.AppendField(o.arena, slots[i].id, slots[i].val); err != nil {
			return fmt.Errorf("executor: serializando a row %d para ordenar: %w", rowID, err)
		}
	}
	o.tuplas = append(o.tuplas, tupla{
		rowID: rowID, payIni: ini, payFim: int32(len(o.arena)),
		chaveIdx: int32(len(o.vals))})

	for _, k := range o.chaves {
		s := &slots[k.Slot]
		cv := chaveVal{presente: s.present && s.val.Type == k.Want}
		if cv.presente {
			if k.Kind == binder.CompareBytes {
				cv.ini = int32(len(o.arena))
				o.arena = append(o.arena, s.val.Bytes...)
				cv.fim = int32(len(o.arena))
			} else {
				cv.i64 = s.val.I64
			}
		}
		o.vals = append(o.vals, cv)
	}

	if o.usado() > o.orcamento {
		return fmt.Errorf("%w: %d bytes acima do teto de %d; "+
			"reduza o resultado com WHERE, ou peça menos colunas",
			ErrOrcamentoDeOrdenacao, o.usado(), o.orcamento)
	}
	return nil
}

// usado é a memória do operador. Conta o arena e as duas tabelas — sem elas o
// teto mediria só metade do que o operador segura.
func (o *ordenador) usado() int64 {
	const bytesPorTupla = 24 // rowID(8) + três int32 + padding
	const bytesPorChave = 24 // bool+pad(8) + i64(8) + dois int32
	return int64(len(o.arena)) +
		int64(len(o.tuplas))*bytesPorTupla +
		int64(len(o.vals))*bytesPorChave
}

// ordena fecha a acumulação. Depois dela o arena não cresce mais, e por isso as
// janelas de bytes podem ser resolvidas com segurança.
func (o *ordenador) ordena() {
	slices.SortFunc(o.tuplas, func(a, b tupla) int {
		return o.compara(&a, &b)
	})
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
