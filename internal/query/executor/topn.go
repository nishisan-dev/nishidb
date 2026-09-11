package executor

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ceilingTopNPadrao é o maior N (= OFFSET + limite efetivo) que ainda escolhe o
// heap limitado.
//
// O número é MEDIDO — BenchmarkOrdena e BenchmarkOrdenaCorpusCurto, com
// -count=6 e benchstat, em benchmark/baselines/topn-20260826.txt.
//
// A primeira leitura da medição CONTRARIA a intuição de que existe um N em que
// o heap passa a perder. Sobre 100 mil linhas varridas ele ganha em TODA a
// faixa medida, inclusive no pior formato de dado (aquele em que toda linha é
// aceita e despeja outra):
//
//	                          aleatório      pior caso
//	N=10                       -91% tempo     -69% tempo
//	N=1.000                    -83%           -43%
//	N=4.096                    -77%           -25%
//	N=16.384                   -50%           -14%
//
// Onde a vantagem some é no corpus CURTO, e é ele que fixa o teto. Sobre 20 mil
// linhas varridas:
//
//	N=1.024   (5% do varrido)   -80% tempo     -45% tempo
//	N=4.096  (20% do varrido)   -59%           -35%
//	N=16.384 (82% do varrido)   sem diferença estatística nos dois formatos
//	                            (p=0,065; no aleatório a MEDIANA já é pior)
//
// A conta explica: o heap custa O(varrido × log N) mais uma reescrita por linha
// aceita, e o sort custa O(varrido × log varrido) mais uma cópia por linha.
// Enquanto N é uma fração pequena do varrido o heap ganha; quando N se aproxima
// do varrido os dois expoentes se encontram e sobra só o sift.
//
// E o varrido é DESCONHECIDO no Open, que é quando o modo é escolhido. Por isso
// o teto não pode ser "N contra o tamanho do dataset": ele é um valor fixo, e
// tem de ser o maior N que ainda ganha mesmo quando o dataset é pequeno. 4.096
// é esse valor — a 20% do varrido ele ainda economiza 35% de tempo no pior
// formato, e 16.384 já não economiza nada.
//
// A segunda dimensão concorda. Em bytes retidos, 4.096 linhas custaram 801 KiB
// contra os 91,6 MiB que o caminho completo materializa em 100 mil linhas;
// 16.384 custaram 3,1 MiB e, no corpus curto, 3,1 MiB contra 16,7 MiB — cinco
// vezes menos em vez de cem. O modo existe para dar TETO ao que fica em
// memória, e um teto que segura um quinto do dataset é quase nenhum.
//
// Acima daqui vale o caminho completo, e não por ser mais rápido (nem sempre
// é): porque é ele que sabe TRANSBORDAR. Passados alguns milhares de linhas
// retidas, a pergunta deixa de ser "quanto custa ordenar" e passa a ser "e se
// não couber" — e a resposta a essa é o spill, não um heap maior.
//
// Não confundir com o teto de MEMÓRIA (Options.SortMemory): este conta LINHAS
// retidas, e é o que decide o modo no Open, antes de existir uma linha para
// medir. O teto de bytes continua valendo dentro do modo — ver degradaTopN.
const ceilingTopNPadrao = 4096

// bufInicialTopN é a capacidade com que o buffer de uma entrada nasce.
//
// Nasce NÃO-NULO de propósito: as janelas de bytes das chaves são re-resolvidas
// como sub-slices desse buffer, e um buffer nil produziria um `direto` nil, que
// o comparador interpreta como "o valor está no arena" — o arena que o modo
// top-N nem usa. 64 bytes é o suficiente para a mini-payload de uma linha
// estreita sem uma realocação de aquecimento.
const bufInicialTopN = 64

// bytesByEntryTopN é o custo fixo de uma entrada do heap, para o orçamento:
// rowID(8) + o slice do buffer(24) + dois int32.
const bytesByEntryTopN = 40

// entryTopN é UMA linha retida pelo heap, com o SEU PRÓPRIO buffer.
//
// # Por que um buffer por entrada, e não o arena compartilhado
//
// O caminho completo copia tudo para um arena único que só CRESCE, e isso
// funciona porque nada é removido: a memória é proporcional ao que entrou, e o
// que entrou é tudo. No top-N a entrada despejada é substituída INDIVIDUALMENTE,
// e o espaço que ela ocupava fica no meio do arena — sem um alocador de blocos
// livres, escrever a linha nova no fim faria o arena crescer com o número de
// linhas VARRIDAS, não com N. O modo deixaria de ser limitado, que é a única
// razão de ele existir.
//
// Com um buffer por entrada o reuso é trivial (`buf[:0]` e reescreve) e o
// consumo em regime é exatamente N buffers, cada um do tamanho da maior linha
// que já passou por ele. Depois do aquecimento — heap cheio, buffers do tamanho
// certo — não há alocação nenhuma por linha varrida.
//
// # Por que as janelas continuam sendo OFFSETS
//
// A reescrita usa `append`, e um `append` que não cabe REALOCA o buffer da
// entrada. (ini, fim) são a forma DURÁVEL da janela — sobrevivem à realocação —
// e o ponteiro é a forma derivada, resolvida uma vez por reescrita, depois do
// último append, quando o buffer já é final. Ver writeEntryTopN.
type entryTopN struct {
	rowID types.RowID
	// buf carrega a mini-payload da linha em [0, payEnd) e, logo depois dela,
	// os bytes das chaves de texto. Um buffer só, e não dois, porque os dois
	// nascem e morrem juntos.
	buf    []byte
	payEnd int32
	// keyIDx é o índice do bloco desta entrada dentro de valsTN, e viaja
	// DENTRO da entrada porque o heap a TROCA de lugar: derivar o bloco da
	// posição no slice leria as chaves de outra linha depois da primeira troca.
	// É a mesma disciplina do keyIDx da tupla.
	keyIDx int32
}

// activeTopN liga o caminho rápido com heap de n entradas.
//
// Os dois slices nascem com a capacidade FINAL: n é conhecido no Open, é no
// máximo o teto, e crescer por append durante a varredura tiraria do modo a
// única coisa que ele promete — consumo previsível. O custo de pedir
// `LIMIT 4096` é reservar 4096 entradas; o custo de pedir `LIMIT 10` é reservar
// dez.
func (o *ordenador) activeTopN(n int) {
	o.topN = n
	o.heap = make([]entryTopN, 0, n)
	o.valsTN = make([]keyVal, 0, n*o.nKeys)
}

// acrescentaTopN é o acrescenta do modo rápido.
//
// Enquanto o heap não está cheio, toda linha entra. Cheio, a RAIZ é a PIOR das
// retidas segundo a ordem pedida, e a linha nova só entra se for melhor que
// ela — caso em que o buffer da despejada é REAPROVEITADO para a nova. A linha
// que não passa nesse teste é descartada sem tocar em memória nenhuma: é esse
// descarte que faz o custo do modo ser O(linhas varridas) em comparações e
// O(N) em bytes.
func (o *ordenador) acrescentaTopN(slots []slot, rowID types.RowID) error {
	if len(o.heap) < o.topN {
		i := int32(len(o.valsTN))
		// Sem append: a capacidade já foi reservada no activeTopN, e reslicing
		// mantém o bloco de chaves desta entrada estável pelo resto da query.
		o.valsTN = o.valsTN[:int(i)+o.nKeys]
		o.heap = append(o.heap, entryTopN{buf: make([]byte, 0, bufInicialTopN), keyIDx: i})
		o.bytesTN += bufInicialTopN
		if err := o.writeEntryTopN(&o.heap[len(o.heap)-1], slots, rowID); err != nil {
			return err
		}
		o.risesTopN(len(o.heap) - 1)
		return o.confereBudgetTopN()
	}

	// Comparar a linha nova com a raiz SEM materializá-la: os valores ainda
	// estão nos slots do iterador, e o ofSlot — o mesmo do merge de runs — os
	// apresenta ao comparador sem cópia. Materializar antes de saber se ela
	// serve gastaria uma reescrita de buffer por linha varrida.
	if o.comparaSlotsWithEntry(slots, rowID, &o.heap[0]) >= 0 {
		return nil
	}
	if err := o.writeEntryTopN(&o.heap[0], slots, rowID); err != nil {
		return err
	}
	o.desceTopN(len(o.heap), 0)
	return o.confereBudgetTopN()
}

// writeEntryTopN (re)preenche uma entrada REAPROVEITANDO o buffer que ela
// já tinha.
func (o *ordenador) writeEntryTopN(e *entryTopN, slots []slot, rowID types.RowID) error {
	before := int64(cap(e.buf))
	buf := e.buf[:0]
	var err error
	for i := range slots {
		if !slots[i].Present {
			continue
		}
		// Mesma serialização do caminho completo, e por isso a releitura pode
		// ser o `coleta` de sempre: um segundo formato aqui abriria a porta
		// para uma linha ordenada pelo top-N e a mesma linha ordenada pelo sort
		// completo produzirem valores diferentes.
		if buf, err = encoding.AppendField(buf, slots[i].ID, slots[i].Val); err != nil {
			e.buf = buf
			o.bytesTN += int64(cap(buf)) - before
			return fmt.Errorf("executor: serializando a row %d para ordenar: %w", rowID, err)
		}
	}
	e.payEnd = int32(len(buf))

	vals := o.valsTN[e.keyIDx : int(e.keyIDx)+o.nKeys]
	for i := range o.chaves {
		k := &o.chaves[i]
		s := &slots[k.Slot]
		cv := keyVal{presente: s.Present && s.Val.Type == k.Want}
		if cv.presente {
			if k.Kind == binder.CompareBytes {
				cv.ini = int32(len(buf))
				buf = append(buf, s.Val.Bytes...)
				cv.end = int32(len(buf))
			} else {
				cv.i64 = s.Val.I64
			}
		}
		vals[i] = cv
	}
	e.buf, e.rowID = buf, rowID
	o.bytesTN += int64(cap(buf)) - before

	// SEGUNDA PASSADA, depois do último append, para que todo `direto` seja
	// uma janela no buffer FINAL da entrada.
	//
	// Resolver os ponteiros dentro do laço acima leria os bytes certos — o
	// append COPIA o conteúdo ao realocar, então o array antigo continua com o
	// que já tinha sido escrito nele. O que ele deixaria para trás é a
	// invariante: a entrada passaria a apontar para DOIS arrays, um deles morto
	// e vivo só porque uma janela de chave o segura. Duas consequências, as
	// duas ruins e nenhuma visível em teste de resultado — o bytesTN, que é o
	// que faz o teto de memória valer aqui, contaria só o buffer corrente e
	// subestimaria o retido; e a próxima linha que re-derivasse a janela a
	// partir de (ini, fim) sobre `e.buf` — o caminho da degradação está a um
	// passo disso — leria um array diferente do que o comparador lê. Uma
	// passada de nKeys por linha aceita compra as duas.
	for i := range vals {
		if vals[i].presente && o.chaves[i].Kind == binder.CompareBytes {
			vals[i].direct = e.buf[vals[i].ini:vals[i].end]
			continue
		}
		vals[i].direct = nil
	}
	return nil
}

// comparaSlotsWithEntry compara a linha AINDA NOS SLOTS com uma entrada
// retida.
//
// Usa a MESMA comparaKey do caminho completo e do merge de runs, com o mesmo
// desempate final por rowID. Uma segunda regra de ordenação aqui divergiria da
// primeira no primeiro empate, e o resultado passaria a depender de qual
// caminho o Open escolheu — que é exatamente o defeito que o modo rápido não
// pode introduzir.
func (o *ordenador) comparaSlotsWithEntry(slots []slot, rowID types.RowID, e *entryTopN) int {
	base := int(e.keyIDx)
	for i := range o.chaves {
		k := &o.chaves[i]
		va := ofSlot(&slots[k.Slot], k)
		if c := o.comparaKey(k, &va, &o.valsTN[base+i]); c != 0 {
			return c
		}
	}
	return int(rowID) - int(e.rowID)
}

// comparaEntradasTopN é a mesma comparação entre duas entradas retidas.
func (o *ordenador) comparaEntradasTopN(a, b *entryTopN) int {
	ia, ib := int(a.keyIDx), int(b.keyIDx)
	for i := range o.chaves {
		k := &o.chaves[i]
		if c := o.comparaKey(k, &o.valsTN[ia+i], &o.valsTN[ib+i]); c != 0 {
			return c
		}
	}
	return int(a.rowID) - int(b.rowID)
}

// risesTopN e desceTopN mantêm a invariante do heap: o PAI é sempre pior ou
// igual ao filho, segundo a ordem PEDIDA.
//
// É um heap de máximos sobre uma ordem que já embute ASC/DESC, ausentes e o
// desempate — com `ORDER BY x ASC LIMIT 10` a raiz é o MAIOR dos dez menores.
// Guardar a pior no topo é o que torna o teste de admissão uma comparação só.
func (o *ordenador) risesTopN(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if o.comparaEntradasTopN(&o.heap[p], &o.heap[i]) >= 0 {
			return
		}
		o.heap[p], o.heap[i] = o.heap[i], o.heap[p]
		i = p
	}
}

// desceTopN opera sobre o prefixo heap[:n] — o n explícito é o que permite ao
// ordenaTopN encolher o heap enquanto o sufixo já ordenado fica de fora.
func (o *ordenador) desceTopN(n, i int) {
	for {
		pior := i
		if l := 2*i + 1; l < n && o.comparaEntradasTopN(&o.heap[l], &o.heap[pior]) > 0 {
			pior = l
		}
		if r := 2*i + 2; r < n && o.comparaEntradasTopN(&o.heap[r], &o.heap[pior]) > 0 {
			pior = r
		}
		if pior == i {
			return
		}
		o.heap[i], o.heap[pior] = o.heap[pior], o.heap[i]
		i = pior
	}
}

// ordenaTopN fecha a acumulação e deixa o heap na ORDEM DE EMISSÃO.
//
// Drenar o heap dá as linhas da PIOR para a melhor, então a sequência precisa
// ser invertida. A inversão é feita IN PLACE, e é o que este laço é: cada
// iteração troca a raiz (a pior das que sobraram) com a última posição viva e
// encolhe o heap — as piores vão se acumulando no fim, e o prefixo termina
// ordenado da melhor para a pior. Drenar para um segundo slice e inverter daria
// exatamente a mesma sequência ao custo de uma segunda cópia de N linhas.
func (o *ordenador) ordenaTopN() {
	for n := len(o.heap); n > 1; n-- {
		o.heap[0], o.heap[n-1] = o.heap[n-1], o.heap[0]
		o.desceTopN(n-1, 0)
	}
}

// proximaOfTopN emite a próxima entrada já ordenada.
//
// A releitura é o MESMO `coleta` do caminho normal sobre a MESMA mini-payload —
// nenhum desserializador novo entrou com este modo.
func (o *ordenador) proximaOfTopN(slots []slot) (types.RowID, bool, error) {
	if o.pos >= len(o.heap) {
		return 0, false, nil
	}
	e := &o.heap[o.pos]
	o.pos++
	if err := coleta(slots, &o.fr, e.buf[:e.payEnd]); err != nil {
		return 0, false, fmt.Errorf("executor: relendo a row %d ordenada: %w", e.rowID, err)
	}
	return e.rowID, true, nil
}

// confereBudgetTopN faz o teto de MEMÓRIA valer também no modo rápido.
//
// O teto por LINHAS decidido no Open não é teto de bytes: N linhas de 4 KiB de
// JSON são uma ordem de grandeza diferente de N linhas de dois inteiros, e é o
// mesmo argumento que fez SortMemory ser contado em bytes.
func (o *ordenador) confereBudgetTopN() error {
	if o.usado() <= o.budget {
		return nil
	}
	return o.degradaTopN()
}

// degradaTopN abandona o caminho rápido e devolve as linhas retidas ao caminho
// completo.
//
// RECUSAR aqui seria a saída barata e seria uma REGRESSÃO: hoje
// `ORDER BY x LIMIT 10` sobre linhas grandes responde, transbordando para
// disco, e passar a recusá-la porque o operador escolheu um modo mais esperto
// seria o modo rápido piorando o produto. Nem adiantaria transbordar o heap: as
// N linhas retidas SÃO a resposta e precisam estar vivas para serem comparadas
// com a próxima que chegar.
//
// A degradação é CORRETA e não aproximada: depois de varrer um prefixo, o heap
// contém exatamente as N melhores desse prefixo — e nenhuma linha fora das N
// melhores de um prefixo pode estar nas N melhores do total. Jogar fora o que
// ele já descartou não muda a resposta; o caminho completo recebe as retidas
// mais todo o resto e ordena tudo, com o seu próprio transbordo.
func (o *ordenador) degradaTopN() error {
	heap := o.heap
	// Desligar o modo ANTES do laço não é ordem estética: o acrescenta abaixo é
	// o do caminho completo, e com topN ainda ligado ele voltaria para cá em
	// recursão infinita.
	o.topN, o.heap, o.valsTN, o.bytesTN = 0, nil, nil, 0
	o.degradou = true
	if o.slotsOfDegradacao == nil {
		o.slotsOfDegradacao = novosSlots(o.slotsModelo)
	}
	for i := range heap {
		e := &heap[i]
		if err := coleta(o.slotsOfDegradacao, &o.fr, e.buf[:e.payEnd]); err != nil {
			return fmt.Errorf("executor: relendo a row %d retida pelo top-N: %w", e.rowID, err)
		}
		if err := o.acrescenta(o.slotsOfDegradacao, e.rowID); err != nil {
			return err
		}
	}
	return nil
}

// sizeOfTopN decide, no Open, se a query cabe no caminho rápido.
//
// A regra é UMA: `OFFSET + limite efetivo` tem de caber no teto de linhas. O
// OFFSET entra na conta porque as linhas PULADAS também precisam ser as certas
// — reter só `limite` e pular depois pularia sobre o conjunto errado.
//
// Exigir LIMIT ESCRITO seria uma segunda condição, e ela foi tentada e
// removida: sem LIMIT o limite efetivo é o query.maxRows do dataset, e é ele —
// não a ausência da palavra — que diz quantas linhas o cliente pode receber. No
// padrão de 100 mil o teto já recusa sozinho, e o caso em que a condição
// mudaria alguma coisa é o dataset com maxRows PEQUENO, onde ela desligaria o
// caminho rápido justamente onde ele é seguro e útil. Duas condições, uma delas
// sem efeito observável no default, é uma condição que nenhum teste consegue
// cobrir — e regra sem oráculo apodrece.
//
// O resultado é o mesmo nos dois modos inclusive na TRUNCAGEM: parar em maxRows
// é teto de linhas ENTREGUES, e as maxRows melhores retidas pelo heap são as
// mesmas maxRows primeiras que o caminho completo emitiria.
func sizeOfTopN(bq *binder.BoundQuery, ceiling int64) (int, bool) {
	if ceiling < 0 {
		return 0, false
	}
	if ceiling == 0 {
		ceiling = ceilingTopNPadrao
	}
	n := bq.Offset + bq.EffectiveLimit
	if n <= 0 || n > ceiling {
		return 0, false
	}
	return int(n), true
}
