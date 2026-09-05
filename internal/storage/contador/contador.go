package contador

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

// SemGrupo é a entrada do vetor de uma linha que não está em grupo nenhum:
// nunca vista, tombstonada, sem alguma coluna do groupBy, ou recusada pelo
// teto. É zero porque zero é o que o fallocate deixa no arquivo — "nunca
// gravada" e "sem grupo" são deliberadamente a mesma coisa.
const SemGrupo uint32 = 0

// bitCondicao marca, na entrada do vetor, que a linha satisfazia o `where`
// quando foi gravada. Os 31 bits abaixo são o id do grupo, e é por isso que o
// teto físico de grupos é 2^31-1.
const bitCondicao uint32 = 1 << 31

// Avaliador é o `where` compilado: diz se o payload (o estado INTEIRO da
// linha, pós-merge) satisfaz a condição. Quem o implementa é o predicado; este
// pacote só conhece a interface, para que o storage não importe o binder.
type Avaliador interface {
	Avalia(payload []byte) (bool, error)
}

// Definicao é um contador como declarado no YAML e compilado contra o schema.
type Definicao struct {
	Nome string
	// Colunas são os FieldIDs do groupBy, na ordem DECLARADA — é a ordem dos
	// componentes da chave composta e das colunas da tabela virtual.
	Colunas []types.FieldID
	// Nomes e Tipos acompanham Colunas, índice a índice.
	Nomes []string
	Tipos []types.FieldType
	// Where é o texto normalizado da condição; "" é "sem condição", e nesse
	// caso NovoAvaliador é nil e toda linha com grupo conta.
	Where string
	// NovoAvaliador instancia um avaliador com scratch próprio. É fábrica, e
	// não instância, porque o writer e a auditoria avaliam em goroutines
	// diferentes e o scratch é de um dono só.
	NovoAvaliador func() Avaliador
	// MaxGroups é o teto de grupos VIVOS.
	MaxGroups uint32
	// RetainEmptyGroups é quanto tempo um grupo com refs == 0 continua
	// visível antes de ser aposentado; zero é "nunca".
	RetainEmptyGroups time.Duration
}

var vetorMagic = [4]byte{'N', 'D', 'B', 'K'}

const (
	nomeDoDicionario = "grupos.dict"
	arquivoDoVetor   = "grupos.idx"
	sufixoAntigo     = ".old"
)

// Contador é o estado de um contador declarado: o vetor rowId → entrada, o
// dicionário de grupos e os totais por grupo.
//
// # Consistência através de crash
//
// Os totais NÃO são persistidos. Na abertura eles são DOBRA do vetor em disco,
// e cada transição aplica `delta = f(nova) − f(antiga)` com antiga lida do
// PRÓPRIO vetor — nunca do payload anterior, que o replay da recovery não tem
// (store_recovery.go). Assim `totais == fold(vetor)` vale em qualquer instante,
// e o replay reaplicando uma transição já refletida no vetor vê antiga == nova
// e não mexe em nada.
//
// O vetor em disco nunca fica à frente do log durável: a transição vai para um
// OVERLAY em heap, e só o ApplyOverlay — que o checkpoint chama depois do
// fdatasync dos segmentos — a leva ao mmap. É a mesma invariante das locations
// (rowindex.Locations), pelo mesmo motivo: uma página descarregada pelo kernel
// antes do crash não pode descrever um record que não existe.
//
// # Concorrência
//
// Transiciona, Dobra, Zera, ApplyOverlay, CobreMexidas e AposentaVazios são
// exclusivos do writer (o Store os chama sob o wmu). Limite, Grupo, PorTexto,
// Congela e Estatisticas são seguros para leitores concorrentes: os totais são
// atômicos e os slices por grupo só crescem sob o lock FOLHA do dicionário,
// que os leitores tomam para indexar. Nada aqui toma lock do Store.
type Contador struct {
	def   Definicao
	dir   string
	dict  *Dicionario
	vetor *rowindex.Denso32
	// over é o overlay: transições ainda não aplicadas ao mmap. Writer-only —
	// ninguém lê o vetor além do writer, então não precisa de lock.
	over map[types.RowID]uint32

	// Por grupo, índice = id (0 reservado). Crescem sob dict.mu quando um id
	// nasce; os leitores indexam sob RLock e conferem o limite.
	totais       []int64 // atômicos (atomic.LoadInt64/AddInt64)
	refs         []int64 // idem
	ultimaMexida []uint64
	vazioDesde   []int64 // unix ms em que refs chegou a 0; 0 = não está vazio
	textos       [][]byte
	porTexto     map[string]uint32

	transicoes      uint64
	mexidasCobertas uint64
	recusados       atomic.Uint64
	orfas           atomic.Uint64
	avisouTeto      bool
	aoRecusar       func()

	deriv *Derivador
	// textoBuf e textoScratch são o scratch da renderização de groupKey de um
	// grupo NOVO; writer-only.
	textoBuf     []byte
	textoScratch []types.Value
	agora        func() time.Time
}

// Abre abre (ou cria) o dicionário e o vetor do contador em dir. recria=true
// renomeia os arquivos existentes para *.old e nasce vazio — é a reconstrução
// por divergência de parâmetros, decidida pelo Store a partir do manifest
// ANTES da recovery; ApagaAntigos remove os renomeados quando a reconstrução
// terminar.
func Abre(dir string, def Definicao, minRows uint64, recria bool) (*Contador, error) {
	if err := validaDefinicao(def); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("contador %s: criar %s: %w", def.Nome, dir, err)
	}
	if recria {
		for _, nome := range []string{nomeDoDicionario, arquivoDoVetor} {
			de := filepath.Join(dir, nome)
			if err := os.Rename(de, de+sufixoAntigo); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("contador %s: pôr %s de lado: %w", def.Nome, de, err)
			}
		}
	}
	dict, err := AbreDicionario(filepath.Join(dir, nomeDoDicionario), def.MaxGroups)
	if err != nil {
		return nil, fmt.Errorf("contador %s: %w", def.Nome, err)
	}
	vetor, err := rowindex.OpenDenso32(filepath.Join(dir, arquivoDoVetor), vetorMagic, minRows)
	if err != nil {
		_ = dict.Close()
		return nil, fmt.Errorf("contador %s: %w", def.Nome, err)
	}
	c := &Contador{
		def:      def,
		dir:      dir,
		dict:     dict,
		vetor:    vetor,
		over:     make(map[types.RowID]uint32),
		porTexto: make(map[string]uint32),
		agora:    time.Now,
	}
	c.deriv = c.NovoDerivador()
	// Os slices por grupo e os textos nascem do dicionário que já está em
	// disco: os ids são estáveis, e a tabela virtual precisa do texto de todo
	// grupo desde a primeira consulta.
	if err := c.acompanhaODicionario(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func validaDefinicao(def Definicao) error {
	switch {
	case def.Nome == "":
		return errors.New("contador: definição sem nome")
	case len(def.Colunas) == 0:
		return fmt.Errorf("contador %s: groupBy vazio", def.Nome)
	case len(def.Tipos) != len(def.Colunas) || len(def.Nomes) != len(def.Colunas):
		return fmt.Errorf("contador %s: Colunas, Nomes e Tipos têm de ter o mesmo tamanho", def.Nome)
	case def.MaxGroups == 0 || def.MaxGroups > bitCondicao-1:
		return fmt.Errorf("contador %s: maxGroups %d fora de [1, %d]", def.Nome, def.MaxGroups, bitCondicao-1)
	case def.Where != "" && def.NovoAvaliador == nil:
		return fmt.Errorf("contador %s: where declarado sem avaliador", def.Nome)
	}
	return nil
}

// acompanhaODicionario cresce os slices por grupo até o limite do dicionário
// e renderiza o texto dos ids que ainda não têm. Roda na abertura e a cada
// grupo novo.
func (c *Contador) acompanhaODicionario() error {
	c.dict.mu.Lock()
	defer c.dict.mu.Unlock()
	limite := len(c.dict.chaves) + 1
	for len(c.totais) < limite {
		id := uint32(len(c.totais))
		c.totais = append(c.totais, 0)
		c.refs = append(c.refs, 0)
		c.ultimaMexida = append(c.ultimaMexida, 0)
		c.vazioDesde = append(c.vazioDesde, 0)
		c.textos = append(c.textos, nil)
		if id == SemGrupo {
			continue
		}
		var err error
		c.textoBuf, c.textoScratch, err = TextoDaChave(c.textoBuf[:0], c.dict.chaves[id-1], c.textoScratch)
		if err != nil {
			return fmt.Errorf("contador %s: grupo %d: %w", c.def.Nome, id, err)
		}
		texto := make([]byte, len(c.textoBuf))
		copy(texto, c.textoBuf)
		c.textos[id] = texto
		if c.dict.vivo[id-1] {
			c.porTexto[string(texto)] = id
		}
	}
	return nil
}

// Def devolve a definição com que o contador foi aberto.
func (c *Contador) Def() Definicao { return c.def }

// Nasceu diz se o vetor foi criado por ESTA abertura.
func (c *Contador) Nasceu() bool { return c.vetor.Nasceu() }

// Cobre diz se o vetor tem entrada para toda linha abaixo de limite.
func (c *Contador) Cobre(limite types.RowID) bool { return c.vetor.Cobre(limite) }

// AoRecusar registra o callback chamado UMA vez, na primeira recusa pelo teto
// desta abertura — é o log do Store, que não quer uma linha por evento.
func (c *Contador) AoRecusar(fn func()) { c.aoRecusar = fn }

// Derivador calcula (grupo, condição) de um payload. Tem scratch próprio e é
// de um dono só: o writer tem o seu, a auditoria instancia outro.
type Derivador struct {
	c        *Contador
	av       Avaliador
	fr       encoding.FieldReader
	vals     []types.Value // na ordem DECLARADA do groupBy
	ordem    []int         // posição declarada da i-ésima coluna em ordem crescente de id
	ids      []types.FieldID
	achados  int
	chaveBuf []byte
}

// NovoDerivador instancia um derivador com avaliador e scratch próprios.
func (c *Contador) NovoDerivador() *Derivador {
	d := &Derivador{c: c, vals: make([]types.Value, len(c.def.Colunas))}
	if c.def.NovoAvaliador != nil {
		d.av = c.def.NovoAvaliador()
	}
	// O payload é ordenado por FieldID; o groupBy não. A leitura casa os dois
	// por merge linear sobre a ordem crescente e devolve cada valor à posição
	// declarada.
	type par struct {
		id  types.FieldID
		pos int
	}
	pares := make([]par, len(c.def.Colunas))
	for i, id := range c.def.Colunas {
		pares[i] = par{id, i}
	}
	slices.SortFunc(pares, func(a, b par) int { return int(a.id) - int(b.id) })
	d.ids = make([]types.FieldID, len(pares))
	d.ordem = make([]int, len(pares))
	for i, p := range pares {
		d.ids[i], d.ordem[i] = p.id, p.pos
	}
	return d
}

// Deriva lê o payload UMA vez e devolve a chave composta do grupo e a condição.
// temGrupo=false quando alguma coluna do groupBy está ausente (NULL é ausência)
// ou com tipo diferente do declarado — a linha não é contada. A chave é válida
// até a próxima chamada. Não aloca.
func (d *Derivador) Deriva(payload []byte) (chave []byte, cond, temGrupo bool, err error) {
	d.achados = 0
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
			pos := d.ordem[i]
			if v.Type != d.c.def.Tipos[pos] {
				return nil, false, false, nil
			}
			d.vals[pos] = v
			d.achados++
			i++
		}
	}
	if d.achados != len(d.ids) {
		return nil, false, false, nil
	}
	d.chaveBuf, err = valuekey.AppendComposta(d.chaveBuf[:0], d.vals)
	if err != nil {
		return nil, false, false, err
	}
	cond = true
	if d.av != nil {
		if cond, err = d.av.Avalia(payload); err != nil {
			return nil, false, false, err
		}
	}
	return d.chaveBuf, cond, true, nil
}

// entrada devolve a entrada corrente da linha: overlay, senão mmap; além do
// arquivo é SemGrupo.
func (c *Contador) entrada(rowID types.RowID) uint32 {
	if v, ok := c.over[rowID]; ok {
		return v
	}
	v, _ := c.vetor.Get(rowID)
	return v
}

// Transiciona é o ÚNICO ponto de escrita do contador: aplica ao estado a
// transição da linha do que ela era (a entrada do vetor) para o que passou a
// ser (derivado do payload). lapide=true força "sem grupo" — a lápide não
// carrega o groupBy, e é a entrada antiga que lembra de onde a linha saiu.
//
// Roda nos DOIS modos do applyRecord — write e replay —, sob o wmu. Zero
// alocação com grupo existente; grupo novo aloca (mapa, cópia da chave, texto)
// e é limitado pelo teto.
//
// O incremento do grupo novo vem ANTES do decremento do antigo: um leitor
// somando os grupos no meio da transição vê a linha em dois lugares, nunca
// em nenhum — o contador nunca subestima.
func (c *Contador) Transiciona(rowID types.RowID, payload []byte, lapide bool) error {
	nova := SemGrupo
	if !lapide {
		chave, cond, tem, err := c.deriv.Deriva(payload)
		if err != nil {
			return fmt.Errorf("contador %s: derivar a linha %d: %w", c.def.Nome, rowID, err)
		}
		if tem {
			id, err := c.dict.Interna(chave)
			switch {
			case errors.Is(err, ErrTetoDeGrupos):
				c.recusados.Add(1)
				if !c.avisouTeto {
					c.avisouTeto = true
					if c.aoRecusar != nil {
						c.aoRecusar()
					}
				}
			case err != nil:
				return fmt.Errorf("contador %s: internar o grupo da linha %d: %w", c.def.Nome, rowID, err)
			default:
				if int(id) >= len(c.totais) {
					if err := c.acompanhaODicionario(); err != nil {
						return err
					}
				}
				nova = id
				if cond {
					nova |= bitCondicao
				}
			}
		}
	}
	antiga := c.entrada(rowID)
	if antiga == nova {
		return nil
	}
	c.transicoes++
	if g := nova &^ bitCondicao; g != SemGrupo {
		atomic.AddInt64(&c.refs[g], 1)
		if nova&bitCondicao != 0 {
			atomic.AddInt64(&c.totais[g], 1)
		}
		c.ultimaMexida[g] = c.transicoes
		c.vazioDesde[g] = 0
	}
	if g := antiga &^ bitCondicao; g != SemGrupo && int(g) < len(c.totais) {
		if antiga&bitCondicao != 0 {
			atomic.AddInt64(&c.totais[g], -1)
		}
		if atomic.AddInt64(&c.refs[g], -1) == 0 {
			c.vazioDesde[g] = c.agora().UnixMilli()
		}
		c.ultimaMexida[g] = c.transicoes
	}
	c.over[rowID] = nova
	return nil
}

// Dobra recomputa totais e refs a partir do vetor em DISCO, pelo tamanho do
// arquivo — antes da recovery, nextRowID ainda é zero, e é a recovery que
// vai reaplicar a ponta por cima. Chamada UMA vez na abertura; o overlay tem
// de estar vazio.
//
// Entrada apontando para um grupo aposentado ou para um id que o dicionário
// não tem é ÓRFÃ: contá-la creditaria um grupo que já saiu. Ela é reescrita
// para SemGrupo (o Open não tem leitores) e contada em Orfas, que vira
// métrica — órfã não é esperada fora de uma janela de crash que a
// aposentadoria coberta por checkpoint fecha, e um número diferente de zero é
// sinal para investigar.
func (c *Contador) Dobra() error {
	if len(c.over) != 0 {
		return fmt.Errorf("contador %s: Dobra com overlay pendente", c.def.Nome)
	}
	c.dict.mu.RLock()
	vivo := slices.Clone(c.dict.vivo)
	c.dict.mu.RUnlock()
	for i := range c.totais {
		atomic.StoreInt64(&c.totais[i], 0)
		atomic.StoreInt64(&c.refs[i], 0)
	}
	n := c.vetor.Entradas()
	for i := uint64(0); i < n; i++ {
		v, _ := c.vetor.Get(types.RowID(i))
		g := v &^ bitCondicao
		if g == SemGrupo {
			continue
		}
		if int(g) > len(vivo) || !vivo[g-1] {
			c.orfas.Add(1)
			if err := c.vetor.Set(types.RowID(i), SemGrupo); err != nil {
				return err
			}
			continue
		}
		atomic.AddInt64(&c.refs[g], 1)
		if v&bitCondicao != 0 {
			atomic.AddInt64(&c.totais[g], 1)
		}
	}
	agora := c.agora().UnixMilli()
	for g := 1; g < len(c.refs); g++ {
		if vivo[g-1] && atomic.LoadInt64(&c.refs[g]) == 0 {
			// O reinício só ESTENDE a visibilidade de um grupo vazio: o
			// instante em que ele esvaziou não é persistido, e o pior caso é
			// ele viver mais um prazo inteiro.
			c.vazioDesde[g] = agora
		}
	}
	return nil
}

// Zera apaga o vetor, o overlay e os totais; o dicionário fica, porque os ids
// são estáveis. É o começo de uma reconstrução: o Store transiciona de novo
// cada linha viva.
func (c *Contador) Zera() error {
	if err := c.vetor.Zera(); err != nil {
		return err
	}
	clear(c.over)
	for i := range c.totais {
		atomic.StoreInt64(&c.totais[i], 0)
		atomic.StoreInt64(&c.refs[i], 0)
		c.vazioDesde[i] = 0
	}
	return nil
}

// ApplyOverlay leva o overlay ao mmap e o esvazia. SÓ no checkpoint, DEPOIS
// do fdatasync dos segmentos: é isso que garante que nenhuma página do vetor
// descreve transição de record não durável. Erro no meio deixa o restante no
// overlay para o próximo checkpoint.
func (c *Contador) ApplyOverlay() error {
	for r, v := range c.over {
		if err := c.vetor.Set(r, v); err != nil {
			return err
		}
		delete(c.over, r)
	}
	return nil
}

// OverlayLen devolve quantas transições ainda não foram ao mmap.
func (c *Contador) OverlayLen() int { return len(c.over) }

// Transicoes é o contador monotônico de transições efetivas (reprocessar não
// conta). Com CobreMexidas, é o que diz se a última mexida de um grupo já
// está durável.
func (c *Contador) Transicoes() uint64 { return c.transicoes }

// CobreMexidas registra que todas as transições até n estão duráveis — em
// record E em página. O Store a chama no checkpoint, depois do Msync do vetor,
// sob o wmu. Nunca regride.
func (c *Contador) CobreMexidas(n uint64) {
	if n > c.mexidasCobertas {
		c.mexidasCobertas = n
	}
}

// MexidasCobertas devolve o último valor registrado por CobreMexidas.
func (c *Contador) MexidasCobertas() uint64 { return c.mexidasCobertas }

// Congela devolve cópias dos totais e dos refs, indexadas por id, e o limite
// de ids — o retrato T0 da auditoria, tirado sob o wmu.
func (c *Contador) Congela() (totais, refs []int64, limite uint32) {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	totais = make([]int64, len(c.totais))
	refs = make([]int64, len(c.refs))
	for i := range c.totais {
		totais[i] = atomic.LoadInt64(&c.totais[i])
		refs[i] = atomic.LoadInt64(&c.refs[i])
	}
	return totais, refs, uint32(len(c.dict.chaves)) + 1
}

// Limite é o maior id de grupo emitido mais um.
func (c *Contador) Limite() uint32 { return c.dict.Limite() }

// Grupo devolve a chave composta, o texto (groupKey), o total e os refs do
// grupo id, e se ele está vivo. Seguro para leitores concorrentes; chave e
// texto são estáveis. Um id que o dicionário já emitiu mas cujos totais ainda
// não cresceram sai como não vivo — janela de microssegundos entre a internação
// e o crescimento, e o leitor a vê como "ainda não existe".
func (c *Contador) Grupo(id uint32) (chave, texto []byte, total, refs int64, vivo bool) {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	if id < 1 || int(id) > len(c.dict.chaves) || int(id) >= len(c.totais) {
		return nil, nil, 0, 0, false
	}
	return c.dict.chaves[id-1], c.textos[id],
		atomic.LoadInt64(&c.totais[id]), atomic.LoadInt64(&c.refs[id]), c.dict.vivo[id-1]
}

// PorTexto resolve o groupKey de um grupo VIVO para o id. Não aloca.
func (c *Contador) PorTexto(texto []byte) (uint32, bool) {
	c.dict.mu.RLock()
	defer c.dict.mu.RUnlock()
	id, ok := c.porTexto[string(texto)]
	return id, ok
}

// Estatisticas é o retrato do contador para métricas e /v1/stats.
type Estatisticas struct {
	Nome        string
	Grupos      uint32 // vivos
	Aposentados uint32
	Teto        uint32
	Recusados   uint64 // grupos novos recusados pelo teto nesta abertura
	Orfas       uint64 // entradas órfãs vistas na dobra desta abertura
	Bytes       int64  // vetor + dicionário
}

// Estatisticas devolve o retrato corrente. Seguro para leitores concorrentes.
func (c *Contador) Estatisticas() Estatisticas {
	return Estatisticas{
		Nome:        c.def.Nome,
		Grupos:      c.dict.Vivos(),
		Aposentados: c.dict.Aposentados(),
		Teto:        c.def.MaxGroups,
		Recusados:   c.recusados.Load(),
		Orfas:       c.orfas.Load(),
		Bytes:       c.vetor.SizeBytes() + c.dict.SizeBytes(),
	}
}

// Msync força as páginas sujas do vetor para disco.
func (c *Contador) Msync() error { return c.vetor.Msync() }

// ApagaAntigos remove os arquivos postos de lado por uma abertura com recria.
func (c *Contador) ApagaAntigos() error {
	var errs []error
	for _, nome := range []string{nomeDoDicionario, arquivoDoVetor} {
		if err := os.Remove(filepath.Join(c.dir, nome+sufixoAntigo)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close libera o vetor e o dicionário.
func (c *Contador) Close() error {
	return errors.Join(c.vetor.Close(), c.dict.Close())
}
