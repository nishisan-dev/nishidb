package segindex

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Fonte é o conjunto de rowIds que UM segmento oferece para um predicado.
//
// Mora aqui porque o Selado é o primeiro a produzi-la, e as outras três
// fontes da §3.2 da spec (WIP, enumeração do histórico, enumeração do
// materializado) precisam satisfazer a MESMA interface para que a união da
// Tarefa 5 não saiba de qual estado cada segmento veio.
//
// Contrato: MinRowID e MaxRowID LIMITAM os rowIds que a fonte vai emitir, e
// uma fonte que não vai emitir nada reporta Min > Max. Os dois valem ANTES de
// consumir qualquer coisa — é o que permite à união descartar as vazias e
// decidir entre concatenação e merge sem tocar em nenhuma. Uma implementação
// que só soubesse os limites depois de iterar quebraria a decisão, não só o
// desempenho: a concatenação só é legítima sobre faixas comprovadamente
// disjuntas.
type Fonte interface {
	types.RowIDIterator
	MinRowID() types.RowID
	MaxRowID() types.RowID
}

// secaoSelada é uma entrada da tabela de seções já validada e convertida para
// offsets de byte absolutos dentro do mapeamento.
//
// Os quatro limites são resolvidos na ABERTURA, e não a cada Lookup, porque é
// a validação deles que impede a busca binária de caminhar para fora do
// diretório. postFim em particular não sai do arquivo: o número de postings
// de uma seção não é gravado em lugar nenhum, e é derivado do fato de as
// seções serem contíguas — a seção i termina onde o diretório da i+1 começa, e
// a última termina no crc.
type secaoSelada struct {
	campo types.FieldID
	// slots é o número de valores distintos, ou seja, de entradas de 16 bytes
	// do diretório.
	slots int
	// postings é o número de deltas do array da seção.
	postings int
	dirOff   int
	postOff  int
}

// Selado é o leitor da forma imutável do índice de um segmento.
//
// Ele não tem lock nem estado mutável no caminho de consulta, e isso é
// propriedade do FORMATO, não uma aposta: o arquivo nunca cresce e nunca é
// reescrito, então a slice do mapeamento é cacheada uma vez na abertura e
// todas as leituras saem dela com binary.LittleEndian direto. É o que tira o
// RWMutex do mmap.Region da rota crítica (débito 102: ~68% do custo de uma
// sondagem no piloto) — ele existe para proteger contra o remap do Grow, e um
// arquivo selado nunca cresce.
//
// Seguro para uso concorrente por N leitores.
//
// # O que a contagem de vidas cobre, e o que ela NÃO cobre
//
// Ela cobre a Fonte DEPOIS que o Lookup retornou: o Fecha espera a última sair
// antes do munmap, e por isso uma varredura em voo nunca lê memória desmapeada.
//
// Ela NÃO torna o Lookup seguro contra um Fecha CONCORRENTE. Com vidas == 0 o
// Fecha desmonta na hora e zera `dados`; um Lookup que entrasse depois disso
// tomaria o pânico do exigeAberto, e não um ok=false. Quem chama o Lookup tem de
// garantir, por conta própria, que ele não corre com o Fecha — no Store isso é o
// selMu, que a aposentadoria toma para fechar. Esta é a fronteira que um segundo
// chamador (o laço de reconstrução da §2.4) precisa respeitar.
type Selado struct {
	reg     *mmap.Region
	caminho string
	// dados é o mapeamento inteiro, incluindo header e crc. Cacheado na
	// abertura de propósito: chamar reg.Bytes() por consulta pagaria o RLock
	// que este desenho existe para eliminar.
	dados     []byte
	secoes    []secaoSelada
	segmentID uint32
	baseRowID types.RowID
	maxRowID  types.RowID
	largura   int

	// A verificação do crc é PREGUIÇOSA, na primeira consulta, e não na
	// abertura: um segmento com uma coluna quase-única chega a 2,25 GB, e
	// conferi-lo na subida custaria segundos por um arquivo que a consulta
	// talvez nem toque (spec §1.5). O Once também garante que ela aconteça uma
	// única vez por processo.
	umaVez          sync.Once
	erroIntegridade error

	// vidas conta as Fontes que ainda podem ler o mapeamento, e existe para que
	// o munmap nunca aconteça sob os pés de uma varredura.
	//
	// NÃO É PROTEÇÃO CONTRA DESCUIDO: é o único desfecho seguro de uma corrida
	// que o produto TEM. A aposentadoria de segmento (retenção e compaction)
	// roda em goroutine de fundo e fecha o índice do segmento que acabou de
	// sair; uma consulta longa iniciada antes dela continua com a Fonte na mão.
	// O Close do dataset está coberto pelo quiesce do Store, mas a
	// aposentadoria não passa por ele — e o desfecho de ler memória desmapeada
	// não é resultado degradado, é SIGSEGV: sinal incapturável, sem nome de
	// arquivo e longe da linha que errou.
	//
	// O mutex NÃO entra na rota crítica: Lookup o toma uma vez por (segmento,
	// consulta) e a varredura não o toca. É o oposto do RWMutex do mmap.Region
	// que esta forma existe para eliminar (débito 102), que era por LEITURA.
	vidasMu      sync.Mutex
	vidas        int
	fechando     bool
	desmontado   bool
	erroDesmonte error
}

// AbreSelado mapeia o índice selado do segmento e valida o header e a tabela
// de seções — NÃO o crc.
//
// A validação de limites acontece aqui mesmo havendo crc, e as duas não são
// redundantes porque não rodam no mesmo momento: o crc só é conferido na
// primeira consulta, então entre o Open e ela existe uma janela em que a
// tabela de seções já foi lida. Sem os limites conferidos, um dirOff ou um
// postOff corrompido nessa janela levaria a busca binária a comparar bytes
// arbitrários do mapeamento — ou, no melhor caso, a estourar o slice com um
// pânico que não nomeia o arquivo. Com eles, o arquivo é recusado com
// ErrFormato e o segmento passa a responder por enumeração.
func AbreSelado(caminho string) (*Selado, error) {
	reg, err := mmap.AbreSomenteLeitura(caminho)
	if err != nil {
		return nil, err
	}
	s, err := montaSelado(reg, caminho)
	if err != nil {
		_ = reg.Close()
		return nil, err
	}
	return s, nil
}

// montaSelado valida o mapeamento e devolve o leitor. Separado de AbreSelado
// para que TODO caminho de recusa desmonte a região num lugar só.
func montaSelado(reg *mmap.Region, caminho string) (*Selado, error) {
	dados := reg.Bytes()
	if len(dados) < HeaderSelado+TamCRC {
		return nil, fmt.Errorf("%w: %s tem %d bytes, menos que o header (%d) mais o crc (%d)",
			ErrFormato, caminho, len(dados), HeaderSelado, TamCRC)
	}
	if string(dados[0:4]) != MagicSelado {
		return nil, fmt.Errorf("%w: %s não começa com %q", ErrFormato, caminho, MagicSelado)
	}
	if v := dados[4]; v != VersaoSelado {
		return nil, fmt.Errorf("%w: %s é da versão %d, este binário lê a %d",
			ErrFormato, caminho, v, VersaoSelado)
	}
	largura := int(dados[32])
	if largura != 4 && largura != 8 {
		return nil, fmt.Errorf("%w: %s declara postingWidth %d (só 4 ou 8)", ErrFormato, caminho, largura)
	}
	segID := binary.LittleEndian.Uint64(dados[8:16])
	if segID > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %s declara segmentID %d, acima do teto de u32", ErrFormato, caminho, segID)
	}
	base := types.RowID(binary.LittleEndian.Uint64(dados[16:24]))
	maxRow := types.RowID(binary.LittleEndian.Uint64(dados[24:32]))
	if base > maxRow {
		return nil, fmt.Errorf("%w: %s declara baseRowID %d acima de maxRowID %d",
			ErrFormato, caminho, base, maxRow)
	}

	// fimCorpo é o último byte que pertence ao corpo: o crc mora depois dele, e
	// nenhum offset da tabela de seções pode alcançá-lo.
	fimCorpo := len(dados) - TamCRC
	secCount := int(binary.LittleEndian.Uint16(dados[6:8]))
	if secCount == 0 {
		// O escritor recusa emitir índice sem nenhuma seção — "sem índice" é
		// arquivo AUSENTE, e é esse estado que a reconstrução sabe reconhecer.
		// Um selado com zero seções faria a fila considerar o segmento pronto
		// e ele nunca ganharia índice de verdade.
		return nil, fmt.Errorf("%w: %s não declara nenhuma seção", ErrFormato, caminho)
	}
	fimTabela := HeaderSelado + secCount*TamSecao
	if fimTabela > fimCorpo {
		return nil, fmt.Errorf("%w: %s declara %d seções, que não cabem em %d bytes de corpo",
			ErrFormato, caminho, secCount, fimCorpo-HeaderSelado)
	}

	secoes, err := leTabelaDeSecoes(dados, caminho, secCount, fimTabela, fimCorpo, largura)
	if err != nil {
		return nil, err
	}
	return &Selado{
		reg:       reg,
		caminho:   caminho,
		dados:     dados,
		secoes:    secoes,
		segmentID: uint32(segID),
		baseRowID: base,
		maxRowID:  maxRow,
		largura:   largura,
	}, nil
}

// leTabelaDeSecoes decodifica as secCount entradas para o heap e confere todos
// os limites contra o tamanho do arquivo.
//
// A tabela vai para o heap — e não é lida do mmap a cada consulta — porque são
// dezenas de entradas, não milhares: o teto do fielddict é 65.535 campos, mas
// um dataset real indexa unidades deles. Em troca, a busca binária por fieldID
// não toca página nenhuma.
func leTabelaDeSecoes(dados []byte, caminho string, secCount, fimTabela, fimCorpo, largura int) ([]secaoSelada, error) {
	secoes := make([]secaoSelada, secCount)
	for i := range secoes {
		off := HeaderSelado + i*TamSecao
		campo := types.FieldID(binary.LittleEndian.Uint16(dados[off : off+2]))
		distintos := binary.LittleEndian.Uint32(dados[off+4 : off+8])
		dirOff := binary.LittleEndian.Uint64(dados[off+8 : off+16])
		postOff := binary.LittleEndian.Uint64(dados[off+16 : off+24])

		// A tabela é ORDENADA por fieldID, e a busca binária de secao() depende
		// disso. Conferir aqui é O(secCount) uma vez; não conferir trocaria uma
		// recusa nomeada por um Lookup que não acha um campo que está no
		// arquivo — falso negativo, o único desfecho que este produto não
		// tolera.
		if i > 0 && campo <= secoes[i-1].campo {
			return nil, fmt.Errorf("%w: %s tem a tabela de seções fora de ordem (campo %d após %d)",
				ErrFormato, caminho, campo, secoes[i-1].campo)
		}

		dirFim := dirOff + uint64(distintos)*TamSlotDir
		if dirOff < uint64(fimTabela) || dirFim > uint64(fimCorpo) || dirFim < dirOff {
			return nil, fmt.Errorf("%w: %s: diretório do campo %d em [%d,%d) fora do corpo [%d,%d)",
				ErrFormato, caminho, campo, dirOff, dirFim, fimTabela, fimCorpo)
		}
		if postOff < dirFim || postOff > uint64(fimCorpo) {
			return nil, fmt.Errorf("%w: %s: postings do campo %d começam em %d, fora de [%d,%d)",
				ErrFormato, caminho, campo, postOff, dirFim, fimCorpo)
		}
		secoes[i] = secaoSelada{
			campo:   campo,
			slots:   int(distintos),
			dirOff:  int(dirOff),
			postOff: int(postOff),
		}
	}

	// O número de postings de cada seção é DERIVADO da contiguidade: a seção i
	// termina onde o diretório da i+1 começa, e a última termina no crc. Ele
	// não é gravado no arquivo de propósito — seria a mesma informação duas
	// vezes e uma chance de divergirem, pela mesma razão de não existir campo
	// minRowID (formato.go).
	for i := range secoes {
		fim := fimCorpo
		if i+1 < len(secoes) {
			fim = secoes[i+1].dirOff
		}
		bytesDePostings := fim - secoes[i].postOff
		if bytesDePostings < 0 || bytesDePostings%largura != 0 {
			return nil, fmt.Errorf("%w: %s: o campo %d tem %d bytes de postings, que não é múltiplo da largura %d",
				ErrFormato, caminho, secoes[i].campo, bytesDePostings, largura)
		}
		secoes[i].postings = bytesDePostings / largura
	}
	return secoes, nil
}

// SegmentID devolve o segmento a que este índice pertence.
func (s *Selado) SegmentID() uint32 { return s.segmentID }

// MinRowID devolve o menor rowId coberto pelo segmento — que é o baseRowID dos
// deltas, e não um campo separado do header.
func (s *Selado) MinRowID() types.RowID { return s.baseRowID }

// MaxRowID devolve o maior rowId coberto pelo segmento.
func (s *Selado) MaxRowID() types.RowID { return s.maxRowID }

// Campos devolve os fieldIDs com seção neste arquivo, em ordem crescente.
//
// Devolve uma cópia: a tabela de seções é a estrutura de que a busca binária
// depende, e emprestá-la deixaria um chamador distraído reordená-la.
//
// Como o TemCampo, responde sobre a EXISTÊNCIA das seções e não confere o crc
// — a ressalva está escrita por extenso no godoc do TemCampo, e vale igual
// aqui.
func (s *Selado) Campos() []types.FieldID {
	campos := make([]types.FieldID, len(s.secoes))
	for i := range s.secoes {
		campos[i] = s.secoes[i].campo
	}
	return campos
}

// TemCampo reporta se este segmento tem seção para o campo.
//
// Responder false NÃO é "o campo não existe no schema": é "este segmento não
// foi indexado por ele", o que acontece entre acrescentar o campo ao schema e a
// reconstrução alcançar este segmento. O chamador resolve por enumeração.
//
// # Não use isto para decidir indexar-ou-enumerar
//
// TemCampo e Campos respondem sobre a EXISTÊNCIA da seção; só o Lookup responde
// sobre a capacidade de SERVI-LA. Os dois primeiros leem a tabela de seções, já
// validada e em heap, e de propósito não conferem o crc: planejar uma consulta
// não pode custar a leitura de 2,25 GB de índice, e o custo teria de ser pago
// por segmento, em todos os 500.
//
// A consequência é que sobre um arquivo CORROMPIDO os dois discordam do Lookup:
// TemCampo devolve true e o Lookup devolve ok=false. Quem usasse TemCampo como
// predicado de plano leria o segmento corrompido como "indexado, e a resposta é
// zero linhas" em vez de "enumere" — e perderia linhas que existem. Falso
// positivo o executor absorve (ele SEMPRE reavalia o WHERE completo, §44);
// falso negativo, nunca.
//
// Quem decide entre índice e enumeração usa o `ok` do Lookup, que é o único a
// integrar as três razões de não responder: campo sem seção, seção que o
// binário não consegue ler e arquivo corrompido.
func (s *Selado) TemCampo(campo types.FieldID) bool {
	_, ok := s.secao(campo)
	return ok
}

// secao acha a seção do campo por busca binária na tabela em heap.
func (s *Selado) secao(campo types.FieldID) (*secaoSelada, bool) {
	lo, hi := 0, len(s.secoes)
	for lo < hi {
		meio := int(uint(lo+hi) >> 1)
		switch c := s.secoes[meio].campo; {
		case c < campo:
			lo = meio + 1
		case c > campo:
			hi = meio
		default:
			return &s.secoes[meio], true
		}
	}
	return nil, false
}

// Lookup devolve a fonte das postings do hash no campo.
//
// ok=false significa que este segmento não responde por este campo — ou o
// campo não é indexado, ou a seção ainda não foi construída porque ele foi
// acrescentado ao schema depois. NÃO significa "valor ausente": valor ausente
// devolve ok=true com uma fonte vazia. A distinção decide se o chamador
// enumera o segmento (§3.2 da spec) ou aceita zero linhas dele.
//
// Crc divergente também devolve ok=false, e pela mesma porta: índice é dado
// derivado, o chamador degrada para enumeração e a resposta continua correta.
// Quem precisa saber que foi corrupção — para apagar o arquivo, logar ERROR e
// contar a métrica — pergunta a ErroDeIntegridade.
//
// NÃO é seguro chamar isto concorrentemente com o Fecha: a exclusão entre os
// dois é do chamador (ver o godoc do Selado). E convém que a conferência do crc
// já tenha acontecido antes — ela é preguiçosa e a primeira chamada varre o
// arquivo inteiro, o que num chamador que segure lock para invocar o Lookup põe
// centenas de MB de varredura debaixo desse lock.
func (s *Selado) Lookup(campo types.FieldID, h uint64) (Fonte, bool) {
	s.exigeAberto("Lookup")
	if err := s.confereCRC(); err != nil {
		return nil, false
	}
	sec, ok := s.secao(campo)
	if !ok {
		return nil, false
	}
	postIdx, count, achou := s.buscaNoDiretorio(sec, h)
	if !achou {
		return fonteVazia{}, true
	}
	// postIdx e count vêm do diretório, que o crc já cobriu — mas a conferência
	// não custa nada e o que ela evita não é resposta errada, é um pânico de
	// slice fora dos limites no meio de uma consulta. Um escritor com bug
	// produz um arquivo com crc VÁLIDO sobre bytes inválidos, e é esse caso que
	// sobra aqui. Degradar para "não respondo por este campo" mantém a resposta
	// correta.
	// A soma não estoura: os dois são u32 alargados para int de 64 bits.
	fim := int(postIdx) + int(count)
	if count == 0 || fim > sec.postings {
		return nil, false
	}
	// A vida é adquirida AQUI, e não no Fonte.Close do chamador, porque é este
	// o único ponto em que se sabe que a Fonte devolvida vai ler o mapeamento.
	// A fonteVazia não lê nada e por isso não conta: fazê-la contar seria adiar
	// um munmap por uma leitura que nunca acontece.
	//
	// Um Selado já em desmonte responde ok=false, e o chamador ENUMERA o
	// segmento — a mesma degradação de um índice ausente, que é sempre correta.
	if !s.adquire() {
		return nil, false
	}
	ini := sec.postOff + int(postIdx)*s.largura
	return novoIterSelado(s, s.dados, s.baseRowID, s.largura, ini, ini+int(count)*s.largura), true
}

// adquire registra mais um leitor do mapeamento, ou recusa se ele já está em
// desmonte.
func (s *Selado) adquire() bool {
	s.vidasMu.Lock()
	defer s.vidasMu.Unlock()
	if s.fechando {
		return false
	}
	s.vidas++
	return true
}

// libera tira um leitor e desmonta o mapeamento se ele era o último de um
// Selado já fechado.
func (s *Selado) libera() {
	s.vidasMu.Lock()
	defer s.vidasMu.Unlock()
	s.vidas--
	if s.vidas == 0 && s.fechando {
		s.desmonta()
	}
}

// desmonta faz o munmap de verdade. Chamado com o vidasMu na mão.
func (s *Selado) desmonta() {
	if s.desmontado {
		return
	}
	s.desmontado = true
	s.dados = nil
	s.erroDesmonte = s.reg.Close()
}

// buscaNoDiretorio acha o valor no diretório ORDENADO por hash da seção.
//
// Busca binária, e não sondagem: o arquivo é imutável, então não há load
// factor a respeitar — o diretório tem exatamente `distintos` entradas, e não
// `distintos/0,7` arredondado para cima. É a diferença entre log2(D) leituras
// e a cadeia de probe que fez o Set do motor antigo custar 1,5 ms.
func (s *Selado) buscaNoDiretorio(sec *secaoSelada, h uint64) (postIdx, count uint32, achou bool) {
	lo, hi := 0, sec.slots
	for lo < hi {
		meio := int(uint(lo+hi) >> 1)
		off := sec.dirOff + meio*TamSlotDir
		switch hm := binary.LittleEndian.Uint64(s.dados[off : off+8]); {
		case hm < h:
			lo = meio + 1
		case hm > h:
			hi = meio
		default:
			return binary.LittleEndian.Uint32(s.dados[off+8 : off+12]),
				binary.LittleEndian.Uint32(s.dados[off+12 : off+16]),
				true
		}
	}
	return 0, 0, false
}

// ErroDeIntegridade força a verificação do crc (se ainda não aconteceu) e
// devolve ErrCRC quando o arquivo está corrompido, ou nil quando está íntegro.
//
// Existe porque o ok=false do Lookup é ambíguo entre "campo ausente" e
// "arquivo corrompido", e só o segundo dispara reconstrução, ERROR e métrica.
// Sem ele, um dataset com um .sidx podre responderia certo por enumeração para
// sempre, sem nunca se curar — e sem ninguém saber.
func (s *Selado) ErroDeIntegridade() error {
	s.exigeAberto("ErroDeIntegridade")
	return s.confereCRC()
}

// exigeAberto entra em pânico se o mapeamento já foi desmontado.
//
// É a rede para o dia em que um caminho novo escapar do quiesce do dono. Sem
// ela, ler o mapeamento desmontado seria SIGSEGV — sinal incapturável, sem
// nome de arquivo e longe da linha que errou; o pânico nomeia o arquivo e o
// método, e o servidor ainda o converte em erro na fronteira da requisição.
func (s *Selado) exigeAberto(op string) {
	if s.dados == nil {
		panic("segindex: " + op + " após Fecha em " + s.caminho)
	}
}

// confereCRC roda a verificação uma única vez e memoriza o resultado.
func (s *Selado) confereCRC() error {
	s.umaVez.Do(func() {
		corpo := s.dados[:len(s.dados)-TamCRC]
		gravado := binary.LittleEndian.Uint32(s.dados[len(s.dados)-TamCRC:])
		if calculado := crc32.Checksum(corpo, castagnoli); calculado != gravado {
			s.erroIntegridade = fmt.Errorf("%w: %s (calculado %08x, gravado %08x)",
				ErrCRC, s.caminho, calculado, gravado)
		}
	})
	return s.erroIntegridade
}

// Fecha impede leitores NOVOS e desmonta o mapeamento assim que o último dos
// atuais sair — o que pode ser aqui mesmo, se não houver nenhum.
//
// A espera é o ponto, e ela substitui uma exigência que o dono não tinha como
// cumprir: as Fontes leem o mapeamento direto, sem cópia, e sobrevivem à
// chamada que as criou. O Store desmonta índice em DOIS momentos, e só um deles
// passa pelo quiesce — o Close do dataset. A aposentadoria de segmento não
// passa, porque roda na goroutine de retenção enquanto uma consulta longa
// varre. Ali, exigir que "nenhuma Fonte viva sobrou" seria exigir do dono uma
// garantia que ele não tem; sem a espera, o desfecho é SIGSEGV.
//
// Depois desta chamada, um Lookup que AINDA ALCANCE este Selado responde
// ok=false, e o chamador degrada para enumeração — correto por construção, e é
// o mesmo que ele já faz com índice ausente. "Ainda alcance" é a ressalva
// inteira: isso vale no ramo em que havia leitor e o mapeamento continua de pé.
// Sem leitor nenhum o desmonte é imediato e o Lookup seguinte PANICA no
// exigeAberto — por isso a exclusão entre Lookup e Fecha é do chamador, e não
// desta contagem (ver o godoc do Selado).
//
// O ERRO DO munmap ADIADO NÃO SOBE por aqui: quando há leitor, o desmonte
// acontece depois, no Close do último deles, e já não há chamada a que
// devolvê-lo. Fecha devolve nil nesse caso. É perda aceitável — falha de munmap
// não tem resposta operacional além do log —, e não vale inventar um canal de
// erro assíncrono para ela.
func (s *Selado) Fecha() error {
	s.vidasMu.Lock()
	defer s.vidasMu.Unlock()
	s.fechando = true
	if s.vidas > 0 {
		return nil
	}
	s.desmonta()
	return s.erroDesmonte
}

// fonteVazia é a resposta de "respondo por este campo, e a resposta é zero
// linhas". É um tipo próprio, e não um iterSelado com faixa vazia, para que a
// distinção contra ok=false fique explícita no código do Lookup.
type fonteVazia struct{}

func (fonteVazia) Next() bool            { return false }
func (fonteVazia) RowID() types.RowID    { return 0 }
func (fonteVazia) Err() error            { return nil }
func (fonteVazia) Close() error          { return nil }
func (fonteVazia) MaxRowID() types.RowID { return 0 }

// MinRowID devolve o maior rowId representável, de modo que Min > Max sempre.
// É o contrato da Fonte para "não vou emitir nada", e é também o elemento
// neutro de um min-fold — quem dobrar as fontes para achar o limite inferior
// da união acerta sem tratar a vazia como caso especial.
func (fonteVazia) MinRowID() types.RowID { return math.MaxUint64 }

// iterSelado percorre uma faixa contígua do array de postings.
//
// Os quatro campos de que a VARREDURA precisa são resolvidos na construção, e
// resolvê-los ali poupa uma indireção por linha. Não tem lock nem atômico — o
// arquivo é imutável, e é essa a metade do ganho desta forma (débito 102).
//
// O ponteiro para o Selado existe só para o Close devolver a vida adquirida no
// Lookup, e por isso não custa nada por linha: Next e RowID não o tocam.
type iterSelado struct {
	sel     *Selado
	dados   []byte
	base    types.RowID
	row     types.RowID
	min     types.RowID
	max     types.RowID
	ini     int
	off     int
	fim     int
	largura int
}

// novoIterSelado monta o iterador sobre [ini, fim) e resolve os limites da
// faixa lendo o primeiro e o último delta.
//
// Os limites saem daqui, e não da iteração, porque o contrato da Fonte exige
// que eles valham ANTES de consumir: são duas leituras, e a faixa já está
// ascendente por construção do escritor. A chamada devolve Fonte — o boxing da
// interface é uma alocação por SEGMENTO, e é por isso que o gate mede a
// varredura e não o Lookup.
func novoIterSelado(sel *Selado, dados []byte, base types.RowID, largura, ini, fim int) *iterSelado {
	it := &iterSelado{
		sel:     sel,
		dados:   dados,
		base:    base,
		ini:     ini,
		off:     ini,
		fim:     fim,
		largura: largura,
	}
	it.min = base + types.RowID(leDelta(dados, ini, largura))
	it.max = base + types.RowID(leDelta(dados, fim-largura, largura))
	return it
}

// leDelta lê um delta de largura bytes em off.
func leDelta(dados []byte, off, largura int) uint64 {
	if largura == 4 {
		return uint64(binary.LittleEndian.Uint32(dados[off : off+4]))
	}
	return binary.LittleEndian.Uint64(dados[off : off+8])
}

// Next avança para a próxima posting. Lê o delta do mapeamento com
// binary.LittleEndian, sem lock, sem atômico e sem cópia.
func (it *iterSelado) Next() bool {
	if it.off >= it.fim {
		return false
	}
	it.row = it.base + types.RowID(leDelta(it.dados, it.off, it.largura))
	it.off += it.largura
	return true
}

// RowID devolve o rowId da posting corrente.
func (it *iterSelado) RowID() types.RowID { return it.row }

// Err nunca falha: o formato foi validado na abertura e o crc na primeira
// consulta, então não sobra decisão para tomar durante a varredura.
func (it *iterSelado) Err() error { return nil }

// Close devolve a vida adquirida no Lookup, e é ele que autoriza o munmap de um
// Selado já fechado. Idempotente: fechar duas vezes não derruba a contagem
// duas vezes, e a união chega a fechar a mesma fonte por mais de um caminho.
//
// Iterador que NUNCA é fechado adia o munmap até o fim do processo. É a troca
// deliberada: memória virtual retida é barata e visível; ler memória
// desmapeada mata o processo.
func (it *iterSelado) Close() error {
	if it.sel == nil {
		return nil
	}
	sel := it.sel
	it.sel = nil
	sel.libera()
	return nil
}

// MinRowID devolve o menor rowId que esta fonte vai emitir.
func (it *iterSelado) MinRowID() types.RowID { return it.min }

// MaxRowID devolve o maior rowId que esta fonte vai emitir.
func (it *iterSelado) MaxRowID() types.RowID { return it.max }

// Rebobina volta o cursor ao início da faixa.
//
// Existe para o gate de alocação, que precisa repetir a MESMA varredura cem
// vezes sem reabrir o arquivo — reabrir mediria a abertura junto. Não faz
// parte da interface Fonte de propósito: nenhum consumidor de produção
// reitera uma fonte, e prometer isso na interface obrigaria as outras três
// fontes da §3.2 a implementá-lo sem ter para quê.
func (it *iterSelado) Rebobina() { it.off = it.ini }
