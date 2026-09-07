package segindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Layout da forma WIP, little-endian (data/segment-%08d-%05d.widx):
//
//	0   4  magic "NDBW"
//	4   1  version = 1
//	5   1  reservado
//	6   2  fieldID u16
//	8   4  dirSlots u32 (potência de 2)
//	12  4  postCount u32
//	16  48 reservado
//	64  .. diretório: dirSlots x 16 bytes
//	          hash u64 | estado u64, com estado = (contagem << 32) | ultimo
//	.. .. postings: postCount x 16 bytes
//	          rowID u64 | anterior u64
//
// ultimo e anterior são índices 1-BASED no array de postings; 0 é "não há".
// hash == 0 marca slot vazio, e isso é seguro porque primary.HashKey nunca
// devolve 0 nem 1 (ela remapeia os dois valores reservados).
//
// O rowId é ABSOLUTO e a posting tem 16 bytes. Não há base nem largura
// variável aqui, e a razão é o dataset materializado: um update traz para o
// segmento corrente uma linha de rowId arbitrário, possivelmente MENOR que a
// do primeiro record dele, então não existe base conhecida no momento em que
// o arquivo é criado. Os 16 bytes também mantêm tudo alinhado em 8, que é o
// que o mmap.Region exige das cargas atômicas. O desperdício é de UM segmento
// (3,6 MB para 224 mil linhas) e a selagem re-baseia para 4 bytes.
const (
	// WIPMagic identifica o arquivo da forma WIP.
	WIPMagic = "NDBW"
	// WIPVersion é a versão do formato WIP.
	WIPVersion = 1
	// WIPHeaderSize é o tamanho do header em bytes.
	WIPHeaderSize = 64
	// WIPSlotSize é o tamanho de um slot de diretório: hash u64 + estado u64.
	WIPSlotSize = 16
	// WIPPostingSize é o tamanho de uma posting: rowID u64 + anterior u64.
	WIPPostingSize = 16
)

// minWIPSlots é o piso do diretório. Um diretório de poucos slots rehasheia
// nas primeiras dezenas de valores, e cada rehash move o array de postings —
// 16 slots são 256 bytes e evitam a metade inicial dessas mudanças sem custo
// mensurável em disco.
const minWIPSlots = 16

// Ocupação máxima do diretório antes do rehash. É a mesma folga que o índice
// primário usa: acima de 70% a sondagem linear começa a formar clusters, e a
// promessa deste pacote é sondagem AMORTIZADA constante — o gate de custo
// mede exatamente isso.
const (
	loadFactorNum = 7
	loadFactorDen = 10
)

// Offsets dos campos do header.
const (
	offVersionWIP  = 4
	offFieldWIP    = 6
	offDirSlotsWIP = 8
	offPostCount   = 12
)

// WIP é a forma MUTÁVEL do índice de um segmento: um arquivo por campo, vivo
// apenas enquanto aquele segmento é o corrente.
//
// A estrutura é um diretório open-addressing sobre os VALORES DISTINTOS do
// segmento, mais um array append-only de postings encadeadas para trás. Inserir
// é uma sondagem amortizada mais duas escritas, e é aí que o O(n²) do motor
// anterior morre: a carga do diretório é o número de valores distintos, não o
// de postings, então a N-ésima posting de um valor custa o mesmo que a
// primeira.
//
// # Concorrência
//
// Set é exclusivo do writer único, como o resto do caminho de escrita do
// storage. Lookup e ForEach aceitam N leitores concorrentes ao Set, e a
// segurança vem de duas coisas distintas que não se substituem.
//
// Contra o SET, a linearização: contagem e cabeça viajam na mesma palavra de 64
// bits, e TUDO de que o leitor precisa para caminhar a cadeia fica visível antes
// dela — a posting, o elo `anterior` e o postCount que limita os elos. A cabeça
// é o commit de visibilidade, e é o último a ser publicado. Um leitor que a
// enxerga enxerga as três outras coisas, e dimensiona a fatia com a contagem
// que corresponde àquela cabeça.
//
// Contra o CRESCIMENTO, o mu. O rehash muda dirSlots e postBase — campos Go
// simples — e desloca o array de postings com um memmove que não é atômico em
// lugar nenhum; nenhuma palavra publicada salva um leitor disso. O lock existe
// pela MESMA razão que existia no motor global que este pacote substituiu —
// proteger a região mapeada e a contagem de slots contra o swap da
// reconstrução —, e o lugar dele é aqui: o Union da união é combinador puro
// sobre Sources já abertas e não tem referência ao writer, e a fonte que lê o
// WIP numa consulta só poderia se serializar pelo wmu — o que serializaria
// consulta contra ingestão, que é exatamente o que este ciclo existe para
// evitar, além de violar a ordem de locks vida → wmu → aposMu.
//
// O custo DESTE lock é um par RLock/RUnlock por Set e por Lookup, e ele é
// ruído. O que ele NÃO elimina — e o texto anterior afirmava que sim — é o par
// interno do mmap.Region, que cada acesso ao mapeamento (w.countedRead,
// AtomicStore64) continua pagando: a alternativa (a) do débito 102 NÃO está
// aplicada aqui. O que matou o custo medido no piloto foi outra coisa: o
// NÚMERO de acessos por operação caiu por desenho — a sondagem é sobre valores
// DISTINTOS (1-2 acessos) e a caminhada é só sobre as postings do valor —,
// então o par por acesso, que antes era pago centenas de milhares de vezes por
// Set, é pago um punhado de vezes. O débito 102 segue aberto para o custo por
// acesso.
//
// O que este lock NÃO promete é atomicidade entre operações: dois Set
// concorrentes continuam proibidos (o writer é único), e um Lookup que corra
// com um Set pode ver a inserção ou não — as duas respostas são corretas para
// um índice cujo contrato é superconjunto de candidatos (§44).
type WIP struct {
	// mu protege dirSlots, postBase, postCap e o CONTEÚDO deslocado pelo
	// crescimento. Escrita só no crescimento; leitura no Set e no Lookup.
	mu   sync.RWMutex
	reg  *mmap.Region
	path string
	// postBase é o offset da primeira posting: WIPHeaderSize +
	// dirSlots*WIPSlotSize. Ele MUDA quando o diretório dobra, e é por isso que
	// o rehash tem de deslocar o array de postings — o que ele nunca faz é
	// renumerá-las, porque o que a cadeia guarda é o índice, não o endereço.
	postBase int64
	field    types.FieldID
	dirSlots uint32
	// postCount é atômico porque o Lookup o usa como LIMITE dos elos da cadeia
	// (um elo além dele é arquivo incoerente) e o Set o incrementa sob lock
	// compartilhado. Sem o atômico a conferência do leitor seria a corrida que
	// ela existe para evitar.
	postCount atomic.Uint32
	// postCap é derivado do tamanho do mapeamento, e não gravado no header: uma
	// segunda fonte de verdade para a capacidade daria, ao divergir, um Set
	// escrevendo fora do arquivo. Mesma razão de o formato selado não ter
	// minRowID (format.go).
	postCap uint32
	// distinctValues é recontado na abertura varrendo o diretório, pela mesma
	// razão: ele é DERIVÁVEL dos slots ocupados, e gravá-lo seria a chance de o
	// arquivo dizer uma coisa e o diretório outra.
	distinctValues uint32
	// accesses conta os ACESSOS AO MAPEAMENTO do último Set — não só as sondas
	// de diretório. Existe para o gate de custo (export_test.go) e é escrito só
	// pelo writer: o caminho de leitura não pode tocá-lo, senão o contador
	// vira corrida de dados.
	accesses int
	// closed marca que o mapeamento já foi desmontado. É o mesmo papel do
	// `data == nil` de Sealed, num campo atômico porque aqui há leitores
	// concorrentes de verdade.
	closed atomic.Bool
}

// WIPPath devolve o caminho do índice WIP do par (segmento, campo).
//
// É um arquivo POR CAMPO, e não por segmento como o selado: o WIP existe para
// um segmento por vez, então ele pode se dar ao luxo de descritores que o
// selado — 500 segmentos vezes N campos contra um ulimit -n de 1024 — não
// poderia (spec §1.2).
func WIPPath(dir string, seg uint32, field types.FieldID) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d-%05d.widx", seg, field))
}

// parseWIPName separa segmento e campo de um nome do espaço
// `segment-%08d-%05d.widx`.
//
// ok=false significa "este nome não é um .widx", e não é anomalia: o diretório
// de dados também tem .dat, .sidx e temporários. Já um nome QUE É um .widx e não
// decompõe — fieldID ilegível, segmento ilegível, padding divergente — é
// RECUSADO com ErrFormat, nunca pulado: esse espaço de nomes é deste pacote, e
// um arquivo ali que não saiu daqui significa que outra coisa escreveu no
// diretório do dataset. Pular seria perder um campo inteiro da selagem em
// silêncio, ou deixar em disco um resíduo que a limpeza da abertura não alcança.
//
// A comparação final contra o nome canônico é o mesmo cuidado do
// segment.ListSegments, e ela é load-bearing aqui por um motivo que lá não
// existe: quem enumera .widx REMOVE o que encontra, e a remoção reconstrói o
// caminho por WIPPath. Aceitar `segment-1-3.widx` como o segmento 1 campo 3
// faria a limpeza mirar um nome canônico que não existe e devolver "removido"
// sobre um arquivo intacto.
func parseWIPName(name string) (uint32, types.FieldID, bool, error) {
	const prefix = "segment-"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".widx") {
		return 0, 0, false, nil
	}
	middle := name[len(prefix) : len(name)-len(".widx")]
	sep := strings.IndexByte(middle, '-')
	if sep < 0 {
		return 0, 0, false, fmt.Errorf("%w: %s não separa segmento e campo em %q", ErrFormat, name, middle)
	}
	seg, err := strconv.ParseUint(middle[:sep], 10, 32)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%w: %s não tem um segmento legível em %q", ErrFormat, name, middle[:sep])
	}
	field, err := strconv.ParseUint(middle[sep+1:], 10, 16)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%w: %s não tem um fieldID legível em %q", ErrFormat, name, middle[sep+1:])
	}
	if canonical := filepath.Base(WIPPath("", uint32(seg), types.FieldID(field))); name != canonical {
		return 0, 0, false, fmt.Errorf("%w: %s não é o nome canônico do par (%d, %d), que seria %s",
			ErrFormat, name, seg, field, canonical)
	}
	return uint32(seg), types.FieldID(field), true, nil
}

// ListWIP devolve, em ordem crescente, os campos que têm .widx no segmento.
//
// O FILESYSTEM é a verdade sobre quais campos o segmento corrente indexa: nada
// no manifest lista isso, seguindo a doutrina que primeiroRowIDDoSeguinte já
// escreve — "um metadado a menos é uma janela de crash a menos" (spec §5.3).
func ListWIP(dir string, seg uint32) ([]types.FieldID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segindex: listar os .widx de %s: %w", dir, err)
	}
	var fields []types.FieldID
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fileSeg, field, ok, err := parseWIPName(e.Name())
		if err != nil {
			return nil, err
		}
		if !ok || fileSeg != seg {
			continue
		}
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields, nil
}

// SegmentsWithWIP devolve, em ordem crescente e sem repetição, os segmentos com
// ao menos um .widx em dir.
//
// Ela é a contrapartida de ListWIP para quem precisa da pergunta ao contrário —
// "de quais segmentos há WIP em disco?" —, e quem precisa é a abertura: a §2.5
// manda descartar o .widx de TODO segmento, e os dois casos que não são o
// corrente só existem como resíduo de crash. O de um segmento já aposentado é o
// que obriga esta varredura a ser do DIRETÓRIO e não da lista de segmentos: ele
// não tem mais .dat, então nenhuma enumeração que parta dos dados o alcançaria,
// e ele ficaria em disco para sempre — contado pelo IndexBytes como disco que
// nada explica, que é o modo de falha que aquela contagem existe para eliminar.
//
// A CONSEQUÊNCIA DE RECUSAR AQUI É MAIOR DO QUE NO ListWIP, e quem chamar
// precisa saber: como a abertura varre o diretório INTEIRO por esta função, um
// .widx de nome ilegível de QUALQUER segmento deixa de custar "a selagem perde um
// campo" e passa a custar "o dataset não abre". A doutrina continua sendo
// recusar em vez de pular — um arquivo neste espaço de nomes que não saiu daqui
// significa que outra coisa escreveu no diretório do dataset, e seguir em frente
// seria decidir sobre um diretório que já não se entende. Mas o chamador tem de
// classificar o ErrFormat numa sentinela de dataset ilegível: sem isso ele cai
// no exit code genérico de erro de uso, que manda o operador olhar a linha de
// comando em vez do dataset.
func SegmentsWithWIP(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segindex: listar os .widx de %s: %w", dir, err)
	}
	var segs []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seg, _, ok, err := parseWIPName(e.Name())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		segs = append(segs, seg)
	}
	slices.Sort(segs)
	return slices.Compact(segs), nil
}

// OpenWIP abre — criando se preciso — o índice WIP do par (segmento, campo).
//
// slotsMin dimensiona o diretório de um arquivo NOVO e é arredondado para cima
// até a próxima potência de dois. slotsMin == 0 significa "abre o que já está
// lá, sem redimensionar, e falha se não houver": é a forma que a selagem usa
// para varrer um WIP já gravado, e criar um arquivo vazio ali faria o segmento
// passar por indexado sem posting nenhuma.
//
// Sobre um arquivo EXISTENTE o slotsMin é ignorado: o dirSlots gravado é o que
// a máscara do diretório usa, e adotá-lo é a única leitura correta dos slots
// que já estão ocupados.
func OpenWIP(dir string, seg uint32, field types.FieldID, slotsMin uint32) (*WIP, error) {
	path := WIPPath(dir, seg, field)
	info, err := os.Stat(path)
	switch {
	case err == nil && info.Size() > 0:
		return openExistingWIP(path, field)
	case err == nil || errors.Is(err, os.ErrNotExist):
		// Arquivo de zero bytes cai aqui junto com o ausente, e é o tratamento
		// certo: ele só existe como criação interrompida antes do fallocate, e
		// não tem header para adotar. O erro carrega os.ErrNotExist para que o
		// chamador não precise distinguir os dois.
		if slotsMin == 0 {
			return nil, fmt.Errorf("segindex: %s não tem conteúdo e slotsMin 0 pede abertura sem redimensionar: %w",
				path, os.ErrNotExist)
		}
		return createWIP(path, field, slotsMin)
	default:
		return nil, fmt.Errorf("segindex: stat de %s: %w", path, err)
	}
}

// createWIP cria o arquivo já dimensionado e grava o header.
//
// A capacidade inicial de postings acompanha o número de slots do diretório
// porque as duas medem a mesma aposta do chamador: um campo com muitos valores
// distintos tem, no mínimo, uma posting por valor. Errar para baixo custa um
// Grow amortizado; errar para cima custa 16 bytes por slot de UM segmento.
func createWIP(path string, field types.FieldID, slotsMin uint32) (*WIP, error) {
	slots := powerOfTwo(max(slotsMin, minWIPSlots))
	base := int64(WIPHeaderSize) + int64(slots)*WIPSlotSize
	reg, err := mmap.Open(path, base+int64(slots)*WIPPostingSize)
	if err != nil {
		return nil, err
	}
	w := &WIP{reg: reg, path: path, field: field, dirSlots: slots, postBase: base}
	w.updateCapacity()
	w.writeHeader()
	return w, nil
}

// openExistingWIP adota o header gravado e reconta os distintos.
func openExistingWIP(path string, field types.FieldID) (*WIP, error) {
	reg, err := mmap.Open(path, WIPHeaderSize)
	if err != nil {
		return nil, err
	}
	w, err := newWIP(reg, path, field)
	if err != nil {
		_ = reg.Close()
		return nil, err
	}
	return w, nil
}

// newWIP valida o header e devolve o WIP. Separado de openExistingWIP para
// que TODO caminho de recusa desmonte a região num lugar só, como em
// newSealed.
func newWIP(reg *mmap.Region, path string, field types.FieldID) (*WIP, error) {
	var (
		magic     [4]byte
		version   byte
		stored    types.FieldID
		dirSlots  uint32
		postCount uint32
	)
	reg.WithBytes(0, WIPHeaderSize, func(b []byte) {
		copy(magic[:], b[0:4])
		version = b[offVersionWIP]
		stored = types.FieldID(binary.LittleEndian.Uint16(b[offFieldWIP : offFieldWIP+2]))
		dirSlots = binary.LittleEndian.Uint32(b[offDirSlotsWIP : offDirSlotsWIP+4])
		postCount = binary.LittleEndian.Uint32(b[offPostCount : offPostCount+4])
	})
	if string(magic[:]) != WIPMagic {
		return nil, fmt.Errorf("%w: %s não começa com %q", ErrFormat, path, WIPMagic)
	}
	if version != WIPVersion {
		return nil, fmt.Errorf("%w: %s é da versão %d, este binário lê a %d",
			ErrFormat, path, version, WIPVersion)
	}
	// O fieldID está no NOME e no header, e a conferência é o que impede um
	// arquivo renomeado de responder pelo campo errado — um Lookup sobre o
	// campo errado devolve linhas erradas sem sintoma nenhum.
	if stored != field {
		return nil, fmt.Errorf("%w: %s declara o campo %d, mas foi aberto como %d",
			ErrFormat, path, stored, field)
	}
	if dirSlots == 0 || dirSlots&(dirSlots-1) != 0 {
		return nil, fmt.Errorf("%w: %s declara dirSlots %d, que não é potência de dois",
			ErrFormat, path, dirSlots)
	}
	base := int64(WIPHeaderSize) + int64(dirSlots)*WIPSlotSize
	if minSize := base + int64(postCount)*WIPPostingSize; reg.Size() < minSize {
		return nil, fmt.Errorf("%w: %s tem %d bytes e declara %d slots com %d postings, que pedem %d",
			ErrFormat, path, reg.Size(), dirSlots, postCount, minSize)
	}
	w := &WIP{
		reg:      reg,
		path:     path,
		field:    field,
		dirSlots: dirSlots,
		postBase: base,
	}
	w.postCount.Store(postCount)
	w.updateCapacity()
	if err := w.checkDirectory(); err != nil {
		return nil, err
	}
	return w, nil
}

// powerOfTwo arredonda n para cima até a próxima potência de dois — exceto
// acima de 2^31, onde devolve 2^31 (o clamp arredonda para BAIXO: não existe
// potência de dois maior num uint32, e um diretório desse tamanho já teria
// 32 GiB só de slots).
func powerOfTwo(n uint32) uint32 {
	if n <= 1 {
		return 1
	}
	if n > 1<<31 {
		return 1 << 31
	}
	return 1 << bits.Len32(n-1)
}

// Field devolve o campo indexado por este arquivo.
func (w *WIP) Field() types.FieldID { return w.field }

// Postings devolve quantas postings foram gravadas, DUPLICATAS INCLUÍDAS. É o
// tamanho do array append-only, e não o número de pares distintos: a mesma
// linha pode receber dois records com o mesmo valor dentro de um segmento, e é
// o Lookup (e o Writer, na selagem) quem compacta.
func (w *WIP) Postings() uint32 { return w.postCount.Load() }

// DistinctValues devolve quantos valores distintos ocupam o diretório. É a
// carga que dimensiona a sondagem — e, portanto, o número que explica o custo
// do Set.
//
// Acessor do lado do WRITER, ao contrário de Postings: o contador de distintos
// é um uint32 comum, escrito pelo Set sob lock compartilhado, então lê-lo de
// outro goroutine é corrida. Ele não é atômico porque nenhum leitor precisa
// dele — o Lookup usa postCount como limite dos elos, e não este.
func (w *WIP) DistinctValues() uint32 { return w.distinctValues }

// slotOff devolve o offset do slot de diretório.
func (w *WIP) slotOff(slot uint32) int64 {
	return int64(WIPHeaderSize) + int64(slot)*WIPSlotSize
}

// postOff devolve o offset da posting de índice 0-based idx.
func (w *WIP) postOff(idx uint32) int64 {
	return w.postBase + int64(idx)*WIPPostingSize
}

// Set liga a posting (h, row). O(1): uma sondagem amortizada de diretório —
// cuja carga é o número de valores DISTINTOS do segmento, não o de postings —
// mais duas escritas.
//
// Exclusivo do writer único.
func (w *WIP) Set(h uint64, row types.RowID) error {
	// O sentinela de slot vazio é o hash 0, e a segurança disso vem de
	// primary.HashKey nunca devolver 0 nem 1 (remapReserved soma 2 aos dois
	// valores reservados). Nada no pacote IMPÕE isso, e é essa a lacuna que o
	// pânico fecha: um hash 0 ocuparia um slot que todo leitor continua lendo
	// como vazio, incrementaria os distintos e faria a posting sumir em
	// SILÊNCIO. Convenção documentada vira invariante gateada.
	if h == 0 {
		panic("segindex: Set com hash 0 em " + w.path +
			" — 0 é o sentinela de slot vazio, e primary.HashKey nunca o devolve")
	}
	// O uso-após-fim é conferido ANTES de qualquer trabalho — inclusive do
	// crescimento, que com o array cheio tocaria o mapeamento desmontado e
	// devolveria um erro de mmap no lugar do pânico nomeado que o godoc
	// promete. Fora do lock de propósito, e é seguro: o flag é atômico, e Set e
	// Close são serializados pelo mesmo dono (o writer único, sob o wmu) — não
	// existe Close concorrente com um Set em voo.
	w.requireOpen("Set")
	if err := w.maybeGrow(); err != nil {
		return err
	}
	// O RLock entra DEPOIS do crescimento, e não pode entrar antes: o
	// crescimento toma o mesmo mu em modo exclusivo, e o RWMutex do Go não é
	// reentrante. postCount e accesses são escritos aqui sob lock COMPARTILHADO,
	// e isso é correto porque o writer é único — o que o mu serializa é o
	// layout (dirSlots, postBase e os bytes que o rehash desloca), não o
	// contador de quem já é dono exclusivo da escrita.
	w.mu.RLock()
	defer w.mu.RUnlock()

	w.accesses = 0             // o contador do gate de custo cobre o Set INTEIRO
	slot := w.probeOrCreate(h) // conta os acessos em w.accesses
	idx := w.postCount.Load()  // 0-based; publicado como idx+1
	off := w.postOff(idx)
	state := w.countedRead(w.slotOff(slot) + 8)

	// A posting é gravada ANTES de o estado ser publicado. Um leitor que
	// enxerga a cabeça nova necessariamente enxerga o rowId e o elo dela.
	w.reg.AtomicStore64(off, uint64(row))
	w.reg.AtomicStore64(off+8, uint64(uint32(state))) // anterior = ultimo atual

	// postCount sobe ANTES de a cabeça ser publicada, e a ordem é obrigatória.
	// O Lookup usa postCount como LIMITE dos elos da cadeia — um elo além do
	// array é arquivo incoerente —, então um leitor que enxergasse a cabeça
	// nova com o total velho concluiria que o elo cai fora e descartaria a
	// cadeia INTEIRA: o Lookup devolveria vazio para um valor que tem
	// centenas de postings. Não é hipótese: foi o que
	// TestOWIPAtendeLeitorConcorrenteDuranteOCrescimento reprovou na primeira
	// versão desta guarda, com "encolheu de 3309 para 0".
	//
	// A regra que sai daí é a mesma de sempre, com um item a mais: TUDO que o
	// leitor precisa para caminhar a cadeia — a posting, o elo dela e agora o
	// total — fica visível ANTES da cabeça. A cabeça continua sendo o commit
	// de visibilidade.
	w.postCount.Store(idx + 1)

	// Contagem e cabeça viajam na MESMA palavra: publicá-las em duas escritas
	// deixaria um leitor ver a cabeça nova com a contagem velha, e ele
	// dimensionaria a fatia com um elemento a menos que a caminhada produz.
	newState := (uint64(uint32(state>>32)+1) << 32) | uint64(idx+1)
	w.reg.AtomicStore64(w.slotOff(slot)+8, newState)
	return nil
}

// countedRead carrega uma palavra do mapeamento CONTANDO o acesso.
//
// É o instrumento do gate de custo, e a diferença entre contar acessos e contar
// sondas de diretório é o que dá valor ao gate. Um Set que voltasse a varrer a
// cadeia de postings para deduplicar — a forma exata do motor antigo traduzida
// para esta estrutura — não tem como fazer isso sem ler o mapeamento, então o
// acréscimo aparece aqui obrigatoriamente. Um contador que só soubesse de slots
// veria 1 sonda e aprovaria a regressão.
//
// Só o CAMINHO DE ESCRITA pode usar este método: w.accesses é um int comum, e
// um leitor concorrente que o incrementasse criaria a corrida de dados que o mu
// existe para não ter. O Lookup e o ForEach leem o mapeamento direto, de
// propósito — e TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador confere que a
// recíproca também vale, para que o contador seja completo por construção.
func (w *WIP) countedRead(off int64) uint64 {
	w.accesses++
	return w.reg.AtomicLoad64(off)
}

// Lookup devolve os rowIds do hash em ordem ESTRITAMENTE crescente.
//
// UMA alocação, de tamanho exato, vinda da contagem publicada junto da cabeça.
// A caminhada é de trás para frente e o preenchimento também, o que já entrega
// ordem crescente no histórico — lá o rowId é monotônico. No materializado não
// é, e por isso o IsSorted: ele é O(n) sem alocação e passa sempre no
// histórico, então o caso quente não paga pelo caso geral.
//
// A compactação no fim não é enfeite: a mesma linha pode receber dois records
// com o mesmo valor dentro de um segmento, e o contrato do iterador é
// ESTRITAMENTE crescente — um id repetido faria o unionIter do executor emitir
// a mesma linha duas vezes num OR.
func (w *WIP) Lookup(h uint64) []types.RowID {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.requireOpen("Lookup")

	slot, ok := w.find(h)
	if !ok {
		return nil
	}
	state := w.reg.AtomicLoad64(w.slotOff(slot) + 8)
	n, cur := uint32(state>>32), uint32(state)
	if n == 0 {
		return nil
	}
	// total é carregado DEPOIS do estado, e a ordem importa tanto quanto a do
	// Set: o Set grava postCount antes de publicar a cabeça, então quem lê a
	// cabeça primeiro e o total depois tem a garantia de que o total já cobre
	// os elos daquela cabeça. Invertido, o limite seria velho e a cadeia inteira
	// seria descartada.
	total := w.postCount.Load()
	out := make([]types.RowID, n)
	i := int(n) - 1
	for ; i >= 0 && cur != 0 && cur <= total; i-- {
		off := w.postOff(cur - 1)
		out[i] = types.RowID(w.reg.AtomicLoad64(off))
		cur = uint32(w.reg.AtomicLoad64(off + 8))
	}
	// A fatia é cortada no ponto em que a caminhada parou, e não devolvida
	// inteira: uma cadeia mais curta que a contagem — ou com um elo fora do
	// array — deixaria ZEROS na frente, e um rowId 0 que não existe é pior que
	// um resultado menor. O corte degrada em vez de panicar, que é o mesmo que
	// o irmão selado faz com postIdx/count fora da faixa; a resposta de produto
	// para o arquivo incoerente é a §2.5, que o descarta no Open.
	out = out[i+1:]
	if !slices.IsSorted(out) {
		slices.Sort(out)
	}
	return slices.Compact(out)
}

// ForEach varre TODAS as postings do WIP, uma vez cada.
//
// É o que a selagem consome para alimentar o Writer, e ela NÃO entrega nada
// ordenado de propósito: o Writer já ordena por (field, hash, row) e
// deduplica na construção do arquivo selado. Ordenar aqui seria a MESMA
// ordenação em dois lugares, e duas implementações do mesmo critério divergem
// na primeira vez que o critério muda.
//
// A varredura é por CADEIA, e não pelo array de postings, porque a posting não
// carrega o hash: só o slot de diretório sabe a que valor ela pertence.
//
// fn roda com o lock de leitura tomado e NÃO pode reentrar no WIP: um Set de
// dentro dela travaria contra o próprio RLock desta varredura assim que
// precisasse crescer. Quem a consome é a selagem, e ela só alimenta o Writer.
func (w *WIP) ForEach(fn func(h uint64, row types.RowID)) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.requireOpen("ForEach")

	for slot := uint32(0); slot < w.dirSlots; slot++ {
		off := w.slotOff(slot)
		h := w.reg.AtomicLoad64(off)
		if h == 0 {
			continue
		}
		state := w.reg.AtomicLoad64(off + 8)
		// O elo é confrontado com o total de postings pela mesma razão do
		// Lookup: ele vem do ARQUIVO, e um elo além do array viraria pânico de
		// bounds no meio da selagem. E o total é carregado por slot, DEPOIS do
		// estado daquele slot, pela mesma razão de ordem — um total lido uma
		// vez no topo do laço ficaria velho em relação às cabeças publicadas
		// durante a varredura, e a cadeia seria descartada inteira.
		total := w.postCount.Load()
		for n, cur := uint32(state>>32), uint32(state); n > 0 && cur != 0 && cur <= total; n-- {
			p := w.postOff(cur - 1)
			fn(h, types.RowID(w.reg.AtomicLoad64(p)))
			cur = uint32(w.reg.AtomicLoad64(p + 8))
		}
	}
	return nil
}

// find acha o slot do hash, ou reporta que ele não está no diretório.
//
// Para no primeiro slot VAZIO, e isso é correto porque este índice é
// append-only dentro do segmento: não há Clear, não há lápide, e portanto
// nenhuma cadeia de sondagem pode ter sido cortada no meio (spec §5.2).
func (w *WIP) find(h uint64) (uint32, bool) {
	mask := w.dirSlots - 1
	for i := uint32(0); i < w.dirSlots; i++ {
		slot := (uint32(h) + i) & mask
		switch w.reg.AtomicLoad64(w.slotOff(slot)) {
		case 0:
			return 0, false
		case h:
			return slot, true
		}
	}
	return 0, false
}

// probeOrCreate devolve o slot do hash, ocupando um vazio se ele ainda não
// estiver no diretório. O que sobe em w.accesses são os ACESSOS AO MAPEAMENTO
// (via w.countedRead), não os slots examinados — é a unidade que o gate de
// custo mede.
//
// O hash é publicado ANTES de qualquer estado, e o slot recém-ocupado tem
// estado zero — um leitor que enxergue o hash novo lê contagem 0 e devolve
// nada, que é a resposta certa até o Set publicar a primeira posting.
func (w *WIP) probeOrCreate(h uint64) uint32 {
	mask := w.dirSlots - 1
	for i := uint32(0); i < w.dirSlots; i++ {
		slot := (uint32(h) + i) & mask
		off := w.slotOff(slot)
		switch w.countedRead(off) {
		case 0:
			w.reg.AtomicStore64(off, h)
			w.distinctValues++
			return slot
		case h:
			return slot
		}
	}
	// Inalcançável: maybeGrow mantém a ocupação em 70%, então sempre há
	// slot vazio. Chegar aqui é bug de load factor, e um erro devolvido seria
	// tratado como falha de I/O pelo chamador — que é o balde errado (§101).
	panic(fmt.Sprintf("segindex: diretório de %s cheio com %d slots e %d distintos (bug de load factor)",
		w.path, w.dirSlots, w.distinctValues))
}

// maybeGrow garante que cabem mais um valor distinto e mais uma posting.
//
// O rehash vem PRIMEIRO porque ele desloca o array de postings: crescer o array
// antes seria dimensioná-lo contra uma base que o rehash muda em seguida.
func (w *WIP) maybeGrow() error {
	// O caminho comum — que é o de TODO Set em regime — nem toca o lock
	// exclusivo. Ler os quatro campos aqui sem lock nenhum é seguro porque
	// quem os escreve é o writer, e o writer é este goroutine: os leitores
	// concorrentes só leem.
	if uint64(w.distinctValues)+1 <= uint64(w.dirSlots)*loadFactorNum/loadFactorDen && w.postCount.Load() < w.postCap {
		return nil
	}
	// Daqui para baixo o layout muda debaixo de quem estiver lendo: dirSlots e
	// postBase mudam de valor e as postings mudam de endereço. O lock exclusivo
	// cobre o Grow, o rehash e o memmove como UMA seção crítica — dividi-la
	// deixaria um leitor entrar entre o memmove e a publicação do postBase novo.
	w.mu.Lock()
	defer w.mu.Unlock()
	if uint64(w.distinctValues)+1 > uint64(w.dirSlots)*loadFactorNum/loadFactorDen {
		if err := w.rehash(); err != nil {
			return err
		}
	}
	if w.postCount.Load() == w.postCap {
		if err := w.growPostings(); err != nil {
			return err
		}
	}
	return nil
}

// growPostings dobra a capacidade do array de postings.
func (w *WIP) growPostings() error {
	newCap := uint64(w.postCap) * 2
	if newCap == 0 {
		newCap = minWIPSlots
	}
	if err := w.ensure(w.postBase + int64(newCap)*WIPPostingSize); err != nil {
		return err
	}
	w.updateCapacity()
	return nil
}

// rehash dobra o diretório e reindexa os valores nele.
//
// As postings NÃO são renumeradas — o que a cadeia guarda é o índice 1-based
// delas, e ele não muda. O que muda é o endereço do array, porque o diretório
// mora antes dele no arquivo: o rehash desloca o bloco inteiro com um memmove e
// segue em frente. Renumerar seria invalidar todo elo `anterior` já gravado.
//
// O diretório antigo é copiado para um buffer TEMPORÁRIO antes de reindexar, e
// o temporário não é evitável: o diretório novo ocupa exatamente o espaço do
// antigo mais o mesmo de novo, então reindexar no lugar leria slots que a
// própria reindexação já teria sobrescrito. Reindexar "esvaziando conforme lê"
// também não serve — as entradas ainda não lidas ocupariam slots durante as
// sondagens e depois sumiriam, cortando cadeias e produzindo FALSO NEGATIVO. O
// custo é amortizado: o diretório dobra, e o buffer é proporcional a ele.
func (w *WIP) rehash() error {
	newSlots := w.dirSlots * 2
	if newSlots < w.dirSlots {
		return fmt.Errorf("segindex: diretório de %s no teto de %d slots", w.path, w.dirSlots)
	}
	newBase := int64(WIPHeaderSize) + int64(newSlots)*WIPSlotSize
	if err := w.ensure(newBase + int64(w.postCap)*WIPPostingSize); err != nil {
		return err
	}

	old := make([]uint64, 0, int(w.dirSlots)*2)
	for i := uint32(0); i < w.dirSlots; i++ {
		off := w.slotOff(i)
		old = append(old, w.reg.AtomicLoad64(off), w.reg.AtomicLoad64(off+8))
	}

	// Desloca as postings para cima. copy é memmove, então a sobreposição
	// entre origem e destino é tratada; a view cobre as duas de uma vez porque
	// WithBytes não pode ser reentrado (o RWMutex do mmap não é reentrante).
	if n := int(w.postCount.Load()) * WIPPostingSize; n > 0 {
		shift := int(newBase - w.postBase)
		w.reg.WithBytes(int(w.postBase), shift+n, func(b []byte) {
			copy(b[shift:], b[:n])
		})
	}
	// Zera o diretório novo — que cobre o antigo e o espaço que as postings
	// acabaram de deixar. Só depois disso os slots voltam a ser reindexados.
	w.reg.WithBytes(WIPHeaderSize, int(newSlots)*WIPSlotSize, func(b []byte) { clear(b) })

	w.dirSlots = newSlots
	w.postBase = newBase
	mask := newSlots - 1
	for i := 0; i < len(old); i += 2 {
		h, state := old[i], old[i+1]
		if h == 0 {
			continue
		}
		reinserted := false
		for p := uint32(0); p < newSlots; p++ {
			off := w.slotOff((uint32(h) + p) & mask)
			if w.reg.AtomicLoad64(off) == 0 {
				// Estado primeiro, hash por último: o hash é o marcador de
				// ocupação, e publicá-lo antes exporia um slot que se diz
				// habitado com contagem zero. É a mesma ordem do Set, e era a
				// mesma do motor global que este pacote substituiu.
				w.reg.AtomicStore64(off+8, state)
				w.reg.AtomicStore64(off, h)
				reinserted = true
				break
			}
		}
		// O laço é LIMITADO pela mesma razão que o de probeOrCreate: a tabela
		// nova tem o dobro dos slots da antiga e no máximo os mesmos ocupantes,
		// então não achar lugar é impossível. Impossível sem limite vira loop
		// infinito, que é a falha mais cara de diagnosticar; com limite, vira
		// um pânico que nomeia o arquivo e o hash.
		if !reinserted {
			panic(fmt.Sprintf("segindex: rehash de %s não achou slot para o hash %d em %d slots (bug de load factor)",
				w.path, h, newSlots))
		}
	}
	w.updateCapacity()
	w.writeHeader()
	return nil
}

// ensure estende o mapeamento até tamanho, se ele ainda não chegou lá.
func (w *WIP) ensure(size int64) error {
	if w.reg.Size() >= size {
		return nil
	}
	return w.reg.Grow(size)
}

// updateCapacity recalcula postCap a partir do tamanho REAL do mapeamento.
// A capacidade é sempre derivada, nunca lembrada: um Grow que arredondasse para
// cima deixaria capacidade paga e não usada.
func (w *WIP) updateCapacity() {
	w.postCap = uint32((w.reg.Size() - w.postBase) / WIPPostingSize)
}

// checkDirectory varre o diretório uma vez, contando os valores distintos e
// conferindo cada cabeça de cadeia contra o número de postings declarado.
//
// A conferência é o que impede o pior desfecho de um .widx incoerente. O header
// só ganha o postCount definitivo no Close (writeHeader não roda por Set), então
// um arquivo que sobreviva a um crash tem a contagem ATRASADA e cadeias
// apontando além dela. Adotado assim, o próximo Set começaria a escrever em
// idx = postCount e sobrescreveria postings vivas — resultado silenciosamente
// errado, que é o desfecho que este produto não tolera.
//
// Em produção o estado não chega até aqui: o Open apaga o .widx órfão e
// reconstrói o do segmento corrente lendo o próprio segmento (spec §2.5). Isto
// é a rede do nível de baixo, para o dia em que um caminho novo escapar do
// contrato de outra tarefa — e custa a MESMA passada O(slots) que a recontagem
// dos distintos já fazia.
func (w *WIP) checkDirectory() error {
	total := w.postCount.Load()
	w.distinctValues = 0
	for i := uint32(0); i < w.dirSlots; i++ {
		off := w.slotOff(i)
		if w.reg.AtomicLoad64(off) == 0 {
			continue
		}
		w.distinctValues++
		state := w.reg.AtomicLoad64(off + 8)
		count, last := uint32(state>>32), uint32(state)
		if (count == 0) != (last == 0) || count > total || last > total {
			return fmt.Errorf("%w: %s: o slot %d declara contagem %d e cabeça %d contra %d postings",
				ErrFormat, w.path, i, count, last, total)
		}
	}
	return nil
}

// writeHeader escreve os campos de dimensão no header.
//
// Ele é chamado na criação, no rehash e no Close — não a cada Set. Entre um e
// outro, o postCount do ARQUIVO fica atrás do postCount em memória, e isso não
// custa nada porque não existe leitor para quem a diferença importe: um .widx
// que sobreviva a um crash é APAGADO no Open, nunca adotado (spec §2.5). Quem
// lê o arquivo é a selagem, depois do Close.
func (w *WIP) writeHeader() {
	w.reg.WithBytes(0, WIPHeaderSize, func(b []byte) {
		copy(b[0:4], WIPMagic)
		b[offVersionWIP] = WIPVersion
		binary.LittleEndian.PutUint16(b[offFieldWIP:offFieldWIP+2], uint16(w.field))
		binary.LittleEndian.PutUint32(b[offDirSlotsWIP:offDirSlotsWIP+4], w.dirSlots)
		binary.LittleEndian.PutUint32(b[offPostCount:offPostCount+4], w.postCount.Load())
	})
}

// Close grava o header definitivo e desmonta o mapeamento.
//
// NÃO há msync, e a ausência é a decisão: o .widx não participa do checkpoint
// nem da ordem de durabilidade do §48 (spec §5.3). Ele é índice do segmento
// CORRENTE, dado derivado, e a recuperação mais barata que está correta é
// derivá-lo de novo a partir do próprio segmento.
func (w *WIP) Close() error {
	// O lock exclusivo não substitui o quiesce do dono — desmontar o
	// mapeamento debaixo de um Lookup em voo seria SIGSEGV, e nenhum lock
	// deste tipo alcança uma fatia que já escapou para dentro do mmap. Ele
	// está aqui pelo mesmo motivo do resto: fechar é a última mudança de
	// layout, e ela não pode cruzar com uma leitura dos campos.
	w.mu.Lock()
	defer w.mu.Unlock()
	w.requireOpen("Close")
	w.writeHeader()
	w.closed.Store(true)
	return w.reg.Close()
}

// requireOpen entra em pânico se o mapeamento já foi desmontado.
//
// É a mesma rede que Sealed.requireOpen arma, e ela existe porque a falha
// natural é MUDA: Region.Close zera data e size, então um writeHeader depois do
// fechamento não daria erro nenhum — daria um pânico de bounds vindo de dentro
// do mmap, sem nome de método e sem nome de arquivo. Aqui o pânico diz os dois,
// e o servidor ainda o converte em erro na fronteira da requisição.
func (w *WIP) requireOpen(op string) {
	if w.closed.Load() {
		panic("segindex: " + op + " após o fechamento de " + w.path)
	}
}

// Remove remove o arquivo, desmontando o mapeamento antes se ele ainda estiver
// de pé.
//
// É a operação TERMINAL do WIP, e é a única que aceita ser chamada depois do
// Close. A assimetria é deliberada e tem um consumidor concreto: a selagem
// fecha o .widx e o apaga em seguida, e um Remove que recusasse região já
// fechada devolveria a ela o erro de "descritor já fechado" — que ela leria
// como falha de uma selagem que deu certo. Close depois de Remove, ao
// contrário, é uso após o fim e entra em pânico nomeado como qualquer outro.
//
// Arquivo já ausente não é erro: a selagem e a limpeza de órfãos do Open podem
// correr sobre um resto de uma execução anterior, e o estado desejado — não
// existir — é o mesmo. Chamar Remove duas vezes também não é.
func (w *WIP) Remove() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var errs []error
	// Não grava o header: o arquivo está indo embora, e sujar páginas que o
	// filesystem teria de escrever por nada é trabalho puro.
	if w.closed.CompareAndSwap(false, true) {
		errs = append(errs, w.reg.Close())
	}
	if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("segindex: remover %s: %w", w.path, err))
	}
	return errors.Join(errs...)
}
