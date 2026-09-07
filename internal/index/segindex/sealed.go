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

// Source é o conjunto de rowIds que UM segmento oferece para um predicado.
//
// Mora aqui porque o Sealed é o primeiro a produzi-la, e as outras três
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
type Source interface {
	types.RowIDIterator
	MinRowID() types.RowID
	MaxRowID() types.RowID
}

// sealedSection é uma entrada da tabela de seções já validada e convertida para
// offsets de byte absolutos dentro do mapeamento.
//
// Os quatro limites são resolvidos na ABERTURA, e não a cada Lookup, porque é
// a validação deles que impede a busca binária de caminhar para fora do
// diretório. postFim em particular não sai do arquivo: o número de postings
// de uma seção não é gravado em lugar nenhum, e é derivado do fato de as
// seções serem contíguas — a seção i termina onde o diretório da i+1 começa, e
// a última termina no crc.
type sealedSection struct {
	field types.FieldID
	// slots é o número de valores distintos, ou seja, de entradas de 16 bytes
	// do diretório.
	slots int
	// postings é o número de deltas do array da seção.
	postings int
	dirOff   int
	postOff  int
}

// Sealed é o leitor da forma imutável do índice de um segmento.
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
// Ela cobre a Source DEPOIS que o Lookup retornou: o Close espera a última sair
// antes do munmap, e por isso uma varredura em voo nunca lê memória desmapeada.
//
// Ela NÃO torna o Lookup seguro contra um Close CONCORRENTE. Com refs == 0 o
// Close desmonta na hora e zera `data`; um Lookup que entrasse depois disso
// tomaria o pânico do requireOpen, e não um ok=false. Quem chama o Lookup tem de
// garantir, por conta própria, que ele não corre com o Close — no Store isso é o
// selMu, que a aposentadoria toma para fechar. Esta é a fronteira que um segundo
// chamador (o laço de reconstrução da §2.4) precisa respeitar.
type Sealed struct {
	reg  *mmap.Region
	path string
	// data é o mapeamento inteiro, incluindo header e crc. Cacheado na
	// abertura de propósito: chamar reg.Bytes() por consulta pagaria o RLock
	// que este desenho existe para eliminar.
	data      []byte
	sections  []sealedSection
	segmentID uint32
	baseRowID types.RowID
	maxRowID  types.RowID
	width     int

	// A verificação do crc é PREGUIÇOSA, na primeira consulta, e não na
	// abertura: um segmento com uma coluna quase-única chega a 2,25 GB, e
	// conferi-lo na subida custaria segundos por um arquivo que a consulta
	// talvez nem toque (spec §1.5). O Once também garante que ela aconteça uma
	// única vez por processo.
	once         sync.Once
	integrityErr error

	// refs conta as Sources que ainda podem ler o mapeamento, e existe para que
	// o munmap nunca aconteça sob os pés de uma varredura.
	//
	// NÃO É PROTEÇÃO CONTRA DESCUIDO: é o único desfecho seguro de uma corrida
	// que o produto TEM. A aposentadoria de segmento (retenção e compaction)
	// roda em goroutine de fundo e fecha o índice do segmento que acabou de
	// sair; uma consulta longa iniciada antes dela continua com a Source na mão.
	// O Close do dataset está coberto pelo quiesce do Store, mas a
	// aposentadoria não passa por ele — e o desfecho de ler memória desmapeada
	// não é resultado degradado, é SIGSEGV: sinal incapturável, sem nome de
	// arquivo e longe da linha que errou.
	//
	// O mutex NÃO entra na rota crítica: Lookup o toma uma vez por (segmento,
	// consulta) e a varredura não o toca. É o oposto do RWMutex do mmap.Region
	// que esta forma existe para eliminar (débito 102), que era por LEITURA.
	refsMu   sync.Mutex
	refs     int
	closing  bool
	unmapped bool
	unmapErr error
}

// OpenSealed mapeia o índice selado do segmento e valida o header e a tabela
// de seções — NÃO o crc.
//
// A validação de limites acontece aqui mesmo havendo crc, e as duas não são
// redundantes porque não rodam no mesmo momento: o crc só é conferido na
// primeira consulta, então entre o Open e ela existe uma janela em que a
// tabela de seções já foi lida. Sem os limites conferidos, um dirOff ou um
// postOff corrompido nessa janela levaria a busca binária a comparar bytes
// arbitrários do mapeamento — ou, no melhor caso, a estourar o slice com um
// pânico que não nomeia o arquivo. Com eles, o arquivo é recusado com
// ErrFormat e o segmento passa a responder por enumeração.
func OpenSealed(path string) (*Sealed, error) {
	reg, err := mmap.OpenReadOnly(path)
	if err != nil {
		return nil, err
	}
	s, err := newSealed(reg, path)
	if err != nil {
		_ = reg.Close()
		return nil, err
	}
	return s, nil
}

// newSealed valida o mapeamento e devolve o leitor. Separado de OpenSealed
// para que TODO caminho de recusa desmonte a região num lugar só.
func newSealed(reg *mmap.Region, path string) (*Sealed, error) {
	data := reg.Bytes()
	if len(data) < SealedHeaderSize+CRCSize {
		return nil, fmt.Errorf("%w: %s tem %d bytes, menos que o header (%d) mais o crc (%d)",
			ErrFormat, path, len(data), SealedHeaderSize, CRCSize)
	}
	if string(data[0:4]) != SealedMagic {
		return nil, fmt.Errorf("%w: %s não começa com %q", ErrFormat, path, SealedMagic)
	}
	if v := data[4]; v != SealedVersion {
		return nil, fmt.Errorf("%w: %s é da versão %d, este binário lê a %d",
			ErrFormat, path, v, SealedVersion)
	}
	width := int(data[32])
	if width != 4 && width != 8 {
		return nil, fmt.Errorf("%w: %s declara postingWidth %d (só 4 ou 8)", ErrFormat, path, width)
	}
	segID := binary.LittleEndian.Uint64(data[8:16])
	if segID > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %s declara segmentID %d, acima do teto de u32", ErrFormat, path, segID)
	}
	base := types.RowID(binary.LittleEndian.Uint64(data[16:24]))
	maxRow := types.RowID(binary.LittleEndian.Uint64(data[24:32]))
	if base > maxRow {
		return nil, fmt.Errorf("%w: %s declara baseRowID %d acima de maxRowID %d",
			ErrFormat, path, base, maxRow)
	}

	// bodyEnd é o último byte que pertence ao corpo: o crc mora depois dele, e
	// nenhum offset da tabela de seções pode alcançá-lo.
	bodyEnd := len(data) - CRCSize
	secCount := int(binary.LittleEndian.Uint16(data[6:8]))
	if secCount == 0 {
		// O escritor recusa emitir índice sem nenhuma seção — "sem índice" é
		// arquivo AUSENTE, e é esse estado que a reconstrução sabe reconhecer.
		// Um selado com zero seções faria a fila considerar o segmento pronto
		// e ele nunca ganharia índice de verdade.
		return nil, fmt.Errorf("%w: %s não declara nenhuma seção", ErrFormat, path)
	}
	tableEnd := SealedHeaderSize + secCount*SectionEntrySize
	if tableEnd > bodyEnd {
		return nil, fmt.Errorf("%w: %s declara %d seções, que não cabem em %d bytes de corpo",
			ErrFormat, path, secCount, bodyEnd-SealedHeaderSize)
	}

	sections, err := readSectionTable(data, path, secCount, tableEnd, bodyEnd, width)
	if err != nil {
		return nil, err
	}
	return &Sealed{
		reg:       reg,
		path:      path,
		data:      data,
		sections:  sections,
		segmentID: uint32(segID),
		baseRowID: base,
		maxRowID:  maxRow,
		width:     width,
	}, nil
}

// readSectionTable decodifica as secCount entradas para o heap e confere todos
// os limites contra o tamanho do arquivo.
//
// A tabela vai para o heap — e não é lida do mmap a cada consulta — porque são
// dezenas de entradas, não milhares: o teto do fielddict é 65.535 campos, mas
// um dataset real indexa unidades deles. Em troca, a busca binária por fieldID
// não toca página nenhuma.
func readSectionTable(data []byte, path string, secCount, tableEnd, bodyEnd, width int) ([]sealedSection, error) {
	sections := make([]sealedSection, secCount)
	for i := range sections {
		off := SealedHeaderSize + i*SectionEntrySize
		field := types.FieldID(binary.LittleEndian.Uint16(data[off : off+2]))
		distinctValues := binary.LittleEndian.Uint32(data[off+4 : off+8])
		dirOff := binary.LittleEndian.Uint64(data[off+8 : off+16])
		postOff := binary.LittleEndian.Uint64(data[off+16 : off+24])

		// A tabela é ORDENADA por fieldID, e a busca binária de section() depende
		// disso. Conferir aqui é O(secCount) uma vez; não conferir trocaria uma
		// recusa nomeada por um Lookup que não acha um campo que está no
		// arquivo — falso negativo, o único desfecho que este produto não
		// tolera.
		if i > 0 && field <= sections[i-1].field {
			return nil, fmt.Errorf("%w: %s tem a tabela de seções fora de ordem (campo %d após %d)",
				ErrFormat, path, field, sections[i-1].field)
		}

		dirEnd := dirOff + uint64(distinctValues)*DirSlotSize
		if dirOff < uint64(tableEnd) || dirEnd > uint64(bodyEnd) || dirEnd < dirOff {
			return nil, fmt.Errorf("%w: %s: diretório do campo %d em [%d,%d) fora do corpo [%d,%d)",
				ErrFormat, path, field, dirOff, dirEnd, tableEnd, bodyEnd)
		}
		if postOff < dirEnd || postOff > uint64(bodyEnd) {
			return nil, fmt.Errorf("%w: %s: postings do campo %d começam em %d, fora de [%d,%d)",
				ErrFormat, path, field, postOff, dirEnd, bodyEnd)
		}
		sections[i] = sealedSection{
			field:   field,
			slots:   int(distinctValues),
			dirOff:  int(dirOff),
			postOff: int(postOff),
		}
	}

	// O número de postings de cada seção é DERIVADO da contiguidade: a seção i
	// termina onde o diretório da i+1 começa, e a última termina no crc. Ele
	// não é gravado no arquivo de propósito — seria a mesma informação duas
	// vezes e uma chance de divergirem, pela mesma razão de não existir campo
	// minRowID (format.go).
	for i := range sections {
		end := bodyEnd
		if i+1 < len(sections) {
			end = sections[i+1].dirOff
		}
		postingBytes := end - sections[i].postOff
		if postingBytes < 0 || postingBytes%width != 0 {
			return nil, fmt.Errorf("%w: %s: o campo %d tem %d bytes de postings, que não é múltiplo da largura %d",
				ErrFormat, path, sections[i].field, postingBytes, width)
		}
		sections[i].postings = postingBytes / width
	}
	return sections, nil
}

// SegmentID devolve o segmento a que este índice pertence.
func (s *Sealed) SegmentID() uint32 { return s.segmentID }

// MinRowID devolve o menor rowId coberto pelo segmento — que é o baseRowID dos
// deltas, e não um campo separado do header.
func (s *Sealed) MinRowID() types.RowID { return s.baseRowID }

// MaxRowID devolve o maior rowId coberto pelo segmento.
func (s *Sealed) MaxRowID() types.RowID { return s.maxRowID }

// Fields devolve os fieldIDs com seção neste arquivo, em ordem crescente.
//
// Devolve uma cópia: a tabela de seções é a estrutura de que a busca binária
// depende, e emprestá-la deixaria um chamador distraído reordená-la.
//
// Como o HasField, responde sobre a EXISTÊNCIA das seções e não confere o crc
// — a ressalva está escrita por extenso no godoc do HasField, e vale igual
// aqui.
func (s *Sealed) Fields() []types.FieldID {
	fields := make([]types.FieldID, len(s.sections))
	for i := range s.sections {
		fields[i] = s.sections[i].field
	}
	return fields
}

// SealedFields abre o arquivo só para ler a tabela de seções e devolve os
// campos que ele cobre, em ordem crescente, SEM deixar mapeamento de pé.
//
// Ela existe para o teste de pendência da reconstrução (§2.6), que precisa
// responder "este segmento tem a seção deste campo?" para CADA segmento do
// dataset a cada passada — e a pergunta anterior, "existe o arquivo?", deixou de
// bastar no dia em que um campo pôde ser acrescentado ao schema depois de o
// segmento ter sido selado.
//
// O formato fica DENTRO deste pacote, e é por isso que ela não é um pread de 64
// bytes em internal/storage: o header e a tabela de seções são lidos pelo mesmo
// newSealed que a consulta usa, com as mesmas validações de limite. Uma
// segunda decodificação da tabela passaria a divergir na primeira vez que o
// layout mudasse — e divergiria em SILÊNCIO, porque as duas continuariam
// produzindo listas de fieldIDs plausíveis.
//
// NÃO confere o crc, pela mesma razão que Fields e HasField não conferem: um
// segmento com coluna quase-única chega a 2,25 GB, e pagar essa varredura por
// arquivo a cada passada da fila custaria mais que a reconstrução que ela
// agenda. Arquivo corrompido continua sendo assunto do gatilho 3 da §2.4 — quem
// o detecta na consulta apaga o arquivo, e o segmento volta a ser pendente por
// AUSÊNCIA, que é a pergunta barata.
func SealedFields(path string) ([]types.FieldID, error) {
	s, err := OpenSealed(path)
	if err != nil {
		return nil, err
	}
	fields := s.Fields()
	if err := s.Close(); err != nil {
		return nil, err
	}
	return fields, nil
}

// HasField reporta se este segmento tem seção para o campo.
//
// Responder false NÃO é "o campo não existe no schema": é "este segmento não
// foi indexado por ele", o que acontece entre acrescentar o campo ao schema e a
// reconstrução alcançar este segmento. O chamador resolve por enumeração.
//
// # Não use isto para decidir indexar-ou-enumerar
//
// HasField e Fields respondem sobre a EXISTÊNCIA da seção; só o Lookup responde
// sobre a capacidade de SERVI-LA. Os dois primeiros leem a tabela de seções, já
// validada e em heap, e de propósito não conferem o crc: planejar uma consulta
// não pode custar a leitura de 2,25 GB de índice, e o custo teria de ser pago
// por segmento, em todos os 500.
//
// A consequência é que sobre um arquivo CORROMPIDO os dois discordam do Lookup:
// HasField devolve true e o Lookup devolve ok=false. Quem usasse HasField como
// predicado de plano leria o segmento corrompido como "indexado, e a resposta é
// zero linhas" em vez de "enumere" — e perderia linhas que existem. Falso
// positivo o executor absorve (ele SEMPRE reavalia o WHERE completo, §44);
// falso negativo, nunca.
//
// Quem decide entre índice e enumeração usa o `ok` do Lookup, que é o único a
// integrar as três razões de não responder: campo sem seção, seção que o
// binário não consegue ler e arquivo corrompido.
func (s *Sealed) HasField(field types.FieldID) bool {
	_, ok := s.section(field)
	return ok
}

// section acha a seção do campo por busca binária na tabela em heap.
func (s *Sealed) section(field types.FieldID) (*sealedSection, bool) {
	lo, hi := 0, len(s.sections)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch c := s.sections[mid].field; {
		case c < field:
			lo = mid + 1
		case c > field:
			hi = mid
		default:
			return &s.sections[mid], true
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
// contar a métrica — pergunta a IntegrityErr.
//
// NÃO é seguro chamar isto concorrentemente com o Close: a exclusão entre os
// dois é do chamador (ver o godoc do Sealed). E convém que a conferência do crc
// já tenha acontecido antes — ela é preguiçosa e a primeira chamada varre o
// arquivo inteiro, o que num chamador que segure lock para invocar o Lookup põe
// centenas de MB de varredura debaixo desse lock.
func (s *Sealed) Lookup(field types.FieldID, h uint64) (Source, bool) {
	s.requireOpen("Lookup")
	if err := s.checkCRC(); err != nil {
		return nil, false
	}
	sec, ok := s.section(field)
	if !ok {
		return nil, false
	}
	postIdx, count, found := s.findInDirectory(sec, h)
	if !found {
		return emptySource{}, true
	}
	// postIdx e count vêm do diretório, que o crc já cobriu — mas a conferência
	// não custa nada e o que ela evita não é resposta errada, é um pânico de
	// slice fora dos limites no meio de uma consulta. Um escritor com bug
	// produz um arquivo com crc VÁLIDO sobre bytes inválidos, e é esse caso que
	// sobra aqui. Degradar para "não respondo por este campo" mantém a resposta
	// correta.
	// A soma não estoura: os dois são u32 alargados para int de 64 bits.
	end := int(postIdx) + int(count)
	if count == 0 || end > sec.postings {
		return nil, false
	}
	// A vida é adquirida AQUI, e não no Source.Close do chamador, porque é este
	// o único ponto em que se sabe que a Source devolvida vai ler o mapeamento.
	// A emptySource não lê nada e por isso não conta: fazê-la contar seria adiar
	// um munmap por uma leitura que nunca acontece.
	//
	// Um Sealed que já começou a fechar responde ok=false, e o chamador ENUMERA
	// o segmento — a mesma degradação de um índice ausente, e ela é correta.
	//
	// ISSO NÃO TORNA O Lookup SEGURO CONTRA UM Close CONCORRENTE, e a ressalva é
	// a mesma dos godocs do Sealed e do Close. O ramo abaixo só é alcançado
	// quando o fecho começou com LEITOR VIVO, e por isso o mapeamento ainda está
	// de pé. Com zero leitores o Close desmonta dentro de si mesmo, e um Lookup
	// concorrente teria entrado em pânico no requireOpen lá em cima — nunca
	// chega a esta linha para receber a degradação que ela promete. Quem exclui
	// um do outro é o chamador; no Store, o selMu.
	if !s.acquire() {
		return nil, false
	}
	start := sec.postOff + int(postIdx)*s.width
	return newSealedIter(s, s.data, s.baseRowID, s.width, start, start+int(count)*s.width), true
}

// acquire registra mais um leitor do mapeamento, ou recusa se ele já está em
// desmonte.
func (s *Sealed) acquire() bool {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	if s.closing {
		return false
	}
	s.refs++
	return true
}

// release tira um leitor e desmonta o mapeamento se ele era o último de um
// Sealed já fechado.
func (s *Sealed) release() {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	s.refs--
	if s.refs == 0 && s.closing {
		s.unmap()
	}
}

// unmap faz o munmap de verdade. Chamado com o refsMu na mão.
func (s *Sealed) unmap() {
	if s.unmapped {
		return
	}
	s.unmapped = true
	s.data = nil
	s.unmapErr = s.reg.Close()
}

// findInDirectory acha o valor no diretório ORDENADO por hash da seção.
//
// Busca binária, e não sondagem: o arquivo é imutável, então não há load
// factor a respeitar — o diretório tem exatamente `distinctValues` entradas,
// e não `distinctValues/0,7` arredondado para cima. É a diferença entre
// log2(D) leituras e a cadeia de probe que fez o Set do motor antigo custar
// 1,5 ms.
func (s *Sealed) findInDirectory(sec *sealedSection, h uint64) (postIdx, count uint32, found bool) {
	lo, hi := 0, sec.slots
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		off := sec.dirOff + mid*DirSlotSize
		switch midHash := binary.LittleEndian.Uint64(s.data[off : off+8]); {
		case midHash < h:
			lo = mid + 1
		case midHash > h:
			hi = mid
		default:
			return binary.LittleEndian.Uint32(s.data[off+8 : off+12]),
				binary.LittleEndian.Uint32(s.data[off+12 : off+16]),
				true
		}
	}
	return 0, 0, false
}

// IntegrityErr força a verificação do crc (se ainda não aconteceu) e
// devolve ErrCRC quando o arquivo está corrompido, ou nil quando está íntegro.
//
// Existe porque o ok=false do Lookup é ambíguo entre "campo ausente" e
// "arquivo corrompido", e só o segundo dispara reconstrução, ERROR e métrica.
// Sem ele, um dataset com um .sidx podre responderia certo por enumeração para
// sempre, sem nunca se curar — e sem ninguém saber.
func (s *Sealed) IntegrityErr() error {
	s.requireOpen("IntegrityErr")
	return s.checkCRC()
}

// requireOpen entra em pânico se o mapeamento já foi desmontado.
//
// É a rede para o dia em que um caminho novo escapar do quiesce do dono. Sem
// ela, ler o mapeamento desmontado seria SIGSEGV — sinal incapturável, sem
// nome de arquivo e longe da linha que errou; o pânico nomeia o arquivo e o
// método, e o servidor ainda o converte em erro na fronteira da requisição.
func (s *Sealed) requireOpen(op string) {
	if s.data == nil {
		panic("segindex: " + op + " após Close em " + s.path)
	}
}

// checkCRC roda a verificação uma única vez e memoriza o resultado.
func (s *Sealed) checkCRC() error {
	s.once.Do(func() {
		body := s.data[:len(s.data)-CRCSize]
		stored := binary.LittleEndian.Uint32(s.data[len(s.data)-CRCSize:])
		if computed := crc32.Checksum(body, castagnoli); computed != stored {
			s.integrityErr = fmt.Errorf("%w: %s (calculado %08x, gravado %08x)",
				ErrCRC, s.path, computed, stored)
		}
	})
	return s.integrityErr
}

// Close impede leitores NOVOS e desmonta o mapeamento assim que o último dos
// atuais sair — o que pode ser aqui mesmo, se não houver nenhum.
//
// A espera é o ponto, e ela substitui uma exigência que o dono não tinha como
// cumprir: as Sources leem o mapeamento direto, sem cópia, e sobrevivem à
// chamada que as criou. O Store desmonta índice em DOIS momentos, e só um deles
// passa pelo quiesce — o Close do dataset. A aposentadoria de segmento não
// passa, porque roda na goroutine de retenção enquanto uma consulta longa
// varre. Ali, exigir que "nenhuma Source viva sobrou" seria exigir do dono uma
// garantia que ele não tem; sem a espera, o desfecho é SIGSEGV.
//
// Depois desta chamada, um Lookup que AINDA ALCANCE este Sealed responde
// ok=false, e o chamador degrada para enumeração — correto por construção, e é
// o mesmo que ele já faz com índice ausente. "Ainda alcance" é a ressalva
// inteira: isso vale no ramo em que havia leitor e o mapeamento continua de pé.
// Sem leitor nenhum o desmonte é imediato e o Lookup seguinte PANICA no
// requireOpen — por isso a exclusão entre Lookup e Close é do chamador, e não
// desta contagem (ver o godoc do Sealed).
//
// O ERRO DO munmap ADIADO NÃO SOBE por aqui: quando há leitor, o desmonte
// acontece depois, no Close do último deles, e já não há chamada a que
// devolvê-lo. Close devolve nil nesse caso. É perda aceitável — falha de munmap
// não tem resposta operacional além do log —, e não vale inventar um canal de
// erro assíncrono para ela.
func (s *Sealed) Close() error {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()
	s.closing = true
	if s.refs > 0 {
		return nil
	}
	s.unmap()
	return s.unmapErr
}

// emptySource é a resposta de "respondo por este campo, e a resposta é zero
// linhas". É um tipo próprio, e não um sealedIter com faixa vazia, para que a
// distinção contra ok=false fique explícita no código do Lookup.
type emptySource struct{}

func (emptySource) Next() bool            { return false }
func (emptySource) RowID() types.RowID    { return 0 }
func (emptySource) Err() error            { return nil }
func (emptySource) Close() error          { return nil }
func (emptySource) MaxRowID() types.RowID { return 0 }

// MinRowID devolve o maior rowId representável, de modo que Min > Max sempre.
// É o contrato da Source para "não vou emitir nada", e é também o elemento
// neutro de um min-fold — quem dobrar as fontes para achar o limite inferior
// da união acerta sem tratar a vazia como caso especial.
func (emptySource) MinRowID() types.RowID { return math.MaxUint64 }

// sealedIter percorre uma faixa contígua do array de postings.
//
// Os quatro campos de que a VARREDURA precisa são resolvidos na construção, e
// resolvê-los ali poupa uma indireção por linha. Não tem lock nem atômico — o
// arquivo é imutável, e é essa a metade do ganho desta forma (débito 102).
//
// O ponteiro para o Sealed existe só para o Close devolver a vida adquirida no
// Lookup, e por isso não custa nada por linha: Next e RowID não o tocam.
type sealedIter struct {
	sel   *Sealed
	data  []byte
	base  types.RowID
	row   types.RowID
	min   types.RowID
	max   types.RowID
	start int
	off   int
	end   int
	width int
}

// newSealedIter monta o iterador sobre [start, end) e resolve os limites da
// faixa lendo o primeiro e o último delta.
//
// Os limites saem daqui, e não da iteração, porque o contrato da Source exige
// que eles valham ANTES de consumir: são duas leituras, e a faixa já está
// ascendente por construção do escritor. A chamada devolve Source — o boxing da
// interface é uma alocação por SEGMENTO, e é por isso que o gate mede a
// varredura e não o Lookup.
func newSealedIter(sel *Sealed, data []byte, base types.RowID, width, start, end int) *sealedIter {
	it := &sealedIter{
		sel:   sel,
		data:  data,
		base:  base,
		start: start,
		off:   start,
		end:   end,
		width: width,
	}
	it.min = base + types.RowID(readDelta(data, start, width))
	it.max = base + types.RowID(readDelta(data, end-width, width))
	return it
}

// readDelta lê um delta de width bytes em off.
func readDelta(data []byte, off, width int) uint64 {
	if width == 4 {
		return uint64(binary.LittleEndian.Uint32(data[off : off+4]))
	}
	return binary.LittleEndian.Uint64(data[off : off+8])
}

// Next avança para a próxima posting. Lê o delta do mapeamento com
// binary.LittleEndian, sem lock, sem atômico e sem cópia.
func (it *sealedIter) Next() bool {
	if it.off >= it.end {
		return false
	}
	it.row = it.base + types.RowID(readDelta(it.data, it.off, it.width))
	it.off += it.width
	return true
}

// RowID devolve o rowId da posting corrente.
func (it *sealedIter) RowID() types.RowID { return it.row }

// Err nunca falha: o formato foi validado na abertura e o crc na primeira
// consulta, então não sobra decisão para tomar durante a varredura.
func (it *sealedIter) Err() error { return nil }

// Close devolve a vida adquirida no Lookup, e é ele que autoriza o munmap de um
// Sealed já fechado. Idempotente: fechar duas vezes não derruba a contagem
// duas vezes, e a união chega a fechar a mesma fonte por mais de um caminho.
//
// Iterador que NUNCA é fechado adia o munmap até o fim do processo. É a troca
// deliberada: memória virtual retida é barata e visível; ler memória
// desmapeada mata o processo.
func (it *sealedIter) Close() error {
	if it.sel == nil {
		return nil
	}
	sel := it.sel
	it.sel = nil
	sel.release()
	return nil
}

// MinRowID devolve o menor rowId que esta fonte vai emitir.
func (it *sealedIter) MinRowID() types.RowID { return it.min }

// MaxRowID devolve o maior rowId que esta fonte vai emitir.
func (it *sealedIter) MaxRowID() types.RowID { return it.max }

// Rewind volta o cursor ao início da faixa.
//
// Existe para o gate de alocação, que precisa repetir a MESMA varredura cem
// vezes sem reabrir o arquivo — reabrir mediria a abertura junto. Não faz
// parte da interface Source de propósito: nenhum consumidor de produção
// reitera uma fonte, e prometer isso na interface obrigaria as outras três
// fontes da §3.2 a implementá-lo sem ter para quê.
func (it *sealedIter) Rewind() { it.off = it.start }
