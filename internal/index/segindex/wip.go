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
	// MagicWIP identifica o arquivo da forma WIP.
	MagicWIP = "NDBW"
	// VersaoWIP é a versão do formato WIP.
	VersaoWIP = 1
	// HeaderWIP é o tamanho do header em bytes.
	HeaderWIP = 64
	// TamSlotWIP é o tamanho de um slot de diretório: hash u64 + estado u64.
	TamSlotWIP = 16
	// TamPosting é o tamanho de uma posting: rowID u64 + anterior u64.
	TamPosting = 16
)

// slotsMinimoWIP é o piso do diretório. Um diretório de poucos slots rehasheia
// nas primeiras dezenas de valores, e cada rehash move o array de postings —
// 16 slots são 256 bytes e evitam a metade inicial dessas mudanças sem custo
// mensurável em disco.
const slotsMinimoWIP = 16

// Ocupação máxima do diretório antes do rehash. É a mesma folga que o índice
// primário usa: acima de 70% a sondagem linear começa a formar clusters, e a
// promessa deste pacote é sondagem AMORTIZADA constante — o gate de custo
// mede exatamente isso.
const (
	cargaNum = 7
	cargaDen = 10
)

// Offsets dos campos do header.
const (
	offVersaoWIP   = 4
	offCampoWIP    = 6
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
// # Concorrência, e o que ela ainda NÃO cobre
//
// Set é exclusivo do writer único, como o resto do caminho de escrita do
// storage. Um leitor concorrente ao Set é seguro por construção: contagem e
// cabeça viajam na mesma palavra de 64 bits, e a posting é gravada antes de
// essa palavra ser publicada.
//
// Um leitor concorrente ao CRESCIMENTO não é. O rehash muda dirSlots e
// postBase — campos Go simples — e desloca o array de postings com um memmove
// que não é atômico em lugar nenhum. Quem consultar o segmento corrente terá de
// se ordenar com o writer por fora deste tipo; a forma disso é da união das
// fontes, não desta estrutura, e está deliberadamente não resolvida aqui.
type WIP struct {
	reg     *mmap.Region
	caminho string
	// postBase é o offset da primeira posting: HeaderWIP + dirSlots*TamSlotWIP.
	// Ele MUDA quando o diretório dobra, e é por isso que o rehash tem de
	// deslocar o array de postings — o que ele nunca faz é renumerá-las, porque
	// o que a cadeia guarda é o índice, não o endereço.
	postBase  int64
	campo     types.FieldID
	dirSlots  uint32
	postCount uint32
	// postCap é derivado do tamanho do mapeamento, e não gravado no header: uma
	// segunda fonte de verdade para a capacidade daria, ao divergir, um Set
	// escrevendo fora do arquivo. Mesma razão de o formato selado não ter
	// minRowID (formato.go).
	postCap uint32
	// distintos é recontado na abertura varrendo o diretório, pela mesma razão:
	// ele é DERIVÁVEL dos slots ocupados, e gravá-lo seria a chance de o
	// arquivo dizer uma coisa e o diretório outra.
	distintos uint32
	// sondas conta os slots examinados pelo último Set. Existe para o gate de
	// custo (export_test.go) e é escrito só pelo writer, como o resto.
	sondas int
}

// CaminhoWIP devolve o caminho do índice WIP do par (segmento, campo).
//
// É um arquivo POR CAMPO, e não por segmento como o selado: o WIP existe para
// um segmento por vez, então ele pode se dar ao luxo de descritores que o
// selado — 500 segmentos vezes N campos contra um ulimit -n de 1024 — não
// poderia (spec §1.2).
func CaminhoWIP(dir string, seg uint32, campo types.FieldID) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d-%05d.widx", seg, campo))
}

// ListaWIP devolve, em ordem crescente, os campos que têm .widx no segmento.
//
// O FILESYSTEM é a verdade sobre quais campos o segmento corrente indexa: nada
// no manifest lista isso, seguindo a doutrina que primeiroRowIDDoSeguinte já
// escreve — "um metadado a menos é uma janela de crash a menos" (spec §5.3).
//
// Um nome dentro do padrão com fieldID ilegível é RECUSADO em vez de pulado:
// esse espaço de nomes é deste pacote, e um arquivo ali que não saiu daqui
// significa que outra coisa escreveu no diretório do dataset. Pular seria
// perder um campo inteiro da selagem em silêncio.
func ListaWIP(dir string, seg uint32) ([]types.FieldID, error) {
	prefixo := fmt.Sprintf("segment-%08d-", seg)
	entradas, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segindex: listar os .widx de %s: %w", dir, err)
	}
	var campos []types.FieldID
	for _, e := range entradas {
		nome := e.Name()
		if e.IsDir() || !strings.HasPrefix(nome, prefixo) || !strings.HasSuffix(nome, ".widx") {
			continue
		}
		miolo := nome[len(prefixo) : len(nome)-len(".widx")]
		id, err := strconv.ParseUint(miolo, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("%w: %s não tem um fieldID legível em %q", ErrFormato, nome, miolo)
		}
		campos = append(campos, types.FieldID(id))
	}
	slices.Sort(campos)
	return campos, nil
}

// AbreWIP abre — criando se preciso — o índice WIP do par (segmento, campo).
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
func AbreWIP(dir string, seg uint32, campo types.FieldID, slotsMin uint32) (*WIP, error) {
	caminho := CaminhoWIP(dir, seg, campo)
	info, err := os.Stat(caminho)
	switch {
	case err == nil && info.Size() > 0:
		return abreWIPExistente(caminho, campo)
	case err == nil || errors.Is(err, os.ErrNotExist):
		// Arquivo de zero bytes cai aqui junto com o ausente, e é o tratamento
		// certo: ele só existe como criação interrompida antes do fallocate, e
		// não tem header para adotar. O erro carrega os.ErrNotExist para que o
		// chamador não precise distinguir os dois.
		if slotsMin == 0 {
			return nil, fmt.Errorf("segindex: %s não tem conteúdo e slotsMin 0 pede abertura sem redimensionar: %w",
				caminho, os.ErrNotExist)
		}
		return criaWIP(caminho, campo, slotsMin)
	default:
		return nil, fmt.Errorf("segindex: stat de %s: %w", caminho, err)
	}
}

// criaWIP cria o arquivo já dimensionado e grava o header.
//
// A capacidade inicial de postings acompanha o número de slots do diretório
// porque as duas medem a mesma aposta do chamador: um campo com muitos valores
// distintos tem, no mínimo, uma posting por valor. Errar para baixo custa um
// Grow amortizado; errar para cima custa 16 bytes por slot de UM segmento.
func criaWIP(caminho string, campo types.FieldID, slotsMin uint32) (*WIP, error) {
	slots := potenciaDeDois(max(slotsMin, slotsMinimoWIP))
	base := int64(HeaderWIP) + int64(slots)*TamSlotWIP
	reg, err := mmap.Open(caminho, base+int64(slots)*TamPosting)
	if err != nil {
		return nil, err
	}
	w := &WIP{reg: reg, caminho: caminho, campo: campo, dirSlots: slots, postBase: base}
	w.atualizaCapacidade()
	w.gravaHeader()
	return w, nil
}

// abreWIPExistente adota o header gravado e reconta os distintos.
func abreWIPExistente(caminho string, campo types.FieldID) (*WIP, error) {
	reg, err := mmap.Open(caminho, HeaderWIP)
	if err != nil {
		return nil, err
	}
	w, err := montaWIP(reg, caminho, campo)
	if err != nil {
		_ = reg.Close()
		return nil, err
	}
	return w, nil
}

// montaWIP valida o header e devolve o WIP. Separado de abreWIPExistente para
// que TODO caminho de recusa desmonte a região num lugar só, como em
// montaSelado.
func montaWIP(reg *mmap.Region, caminho string, campo types.FieldID) (*WIP, error) {
	var (
		magic     [4]byte
		versao    byte
		gravado   types.FieldID
		dirSlots  uint32
		postCount uint32
	)
	reg.WithBytes(0, HeaderWIP, func(b []byte) {
		copy(magic[:], b[0:4])
		versao = b[offVersaoWIP]
		gravado = types.FieldID(binary.LittleEndian.Uint16(b[offCampoWIP : offCampoWIP+2]))
		dirSlots = binary.LittleEndian.Uint32(b[offDirSlotsWIP : offDirSlotsWIP+4])
		postCount = binary.LittleEndian.Uint32(b[offPostCount : offPostCount+4])
	})
	if string(magic[:]) != MagicWIP {
		return nil, fmt.Errorf("%w: %s não começa com %q", ErrFormato, caminho, MagicWIP)
	}
	if versao != VersaoWIP {
		return nil, fmt.Errorf("%w: %s é da versão %d, este binário lê a %d",
			ErrFormato, caminho, versao, VersaoWIP)
	}
	// O fieldID está no NOME e no header, e a conferência é o que impede um
	// arquivo renomeado de responder pelo campo errado — um Lookup sobre o
	// campo errado devolve linhas erradas sem sintoma nenhum.
	if gravado != campo {
		return nil, fmt.Errorf("%w: %s declara o campo %d, mas foi aberto como %d",
			ErrFormato, caminho, gravado, campo)
	}
	if dirSlots == 0 || dirSlots&(dirSlots-1) != 0 {
		return nil, fmt.Errorf("%w: %s declara dirSlots %d, que não é potência de dois",
			ErrFormato, caminho, dirSlots)
	}
	base := int64(HeaderWIP) + int64(dirSlots)*TamSlotWIP
	if minimo := base + int64(postCount)*TamPosting; reg.Size() < minimo {
		return nil, fmt.Errorf("%w: %s tem %d bytes e declara %d slots com %d postings, que pedem %d",
			ErrFormato, caminho, reg.Size(), dirSlots, postCount, minimo)
	}
	w := &WIP{
		reg:       reg,
		caminho:   caminho,
		campo:     campo,
		dirSlots:  dirSlots,
		postCount: postCount,
		postBase:  base,
	}
	w.atualizaCapacidade()
	w.recontaDistintos()
	return w, nil
}

// potenciaDeDois arredonda n para cima até a próxima potência de dois.
func potenciaDeDois(n uint32) uint32 {
	if n <= 1 {
		return 1
	}
	if n > 1<<31 {
		return 1 << 31
	}
	return 1 << bits.Len32(n-1)
}

// Campo devolve o campo indexado por este arquivo.
func (w *WIP) Campo() types.FieldID { return w.campo }

// Postings devolve quantas postings foram gravadas, DUPLICATAS INCLUÍDAS. É o
// tamanho do array append-only, e não o número de pares distintos: a mesma
// linha pode receber dois records com o mesmo valor dentro de um segmento, e é
// o Lookup (e o Escritor, na selagem) quem compacta.
func (w *WIP) Postings() uint32 { return w.postCount }

// Distintos devolve quantos valores distintos ocupam o diretório. É a carga
// que dimensiona a sondagem — e, portanto, o número que explica o custo do Set.
func (w *WIP) Distintos() uint32 { return w.distintos }

// slotOff devolve o offset do slot de diretório.
func (w *WIP) slotOff(slot uint32) int64 {
	return int64(HeaderWIP) + int64(slot)*TamSlotWIP
}

// postOff devolve o offset da posting de índice 0-based idx.
func (w *WIP) postOff(idx uint32) int64 {
	return w.postBase + int64(idx)*TamPosting
}

// Set liga a posting (h, row). O(1): uma sondagem amortizada de diretório —
// cuja carga é o número de valores DISTINTOS do segmento, não o de postings —
// mais duas escritas.
//
// Exclusivo do writer único.
func (w *WIP) Set(h uint64, row types.RowID) error {
	if err := w.talvezCresce(); err != nil {
		return err
	}
	slot := w.sondaOuCria(h) // conta as sondas em w.sondas
	idx := w.postCount       // 0-based; publicado como idx+1
	off := w.postOff(idx)
	estado := w.reg.AtomicLoad64(w.slotOff(slot) + 8)

	// A posting é gravada ANTES de o estado ser publicado. Um leitor que
	// enxerga a cabeça nova necessariamente enxerga o rowId e o elo dela.
	w.reg.AtomicStore64(off, uint64(row))
	w.reg.AtomicStore64(off+8, uint64(uint32(estado))) // anterior = ultimo atual

	// Contagem e cabeça viajam na MESMA palavra: publicá-las em duas escritas
	// deixaria um leitor ver a cabeça nova com a contagem velha, e ele
	// dimensionaria a fatia com um elemento a menos que a caminhada produz.
	novo := (uint64(uint32(estado>>32)+1) << 32) | uint64(idx+1)
	w.reg.AtomicStore64(w.slotOff(slot)+8, novo)

	w.postCount++
	return nil
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
	slot, ok := w.busca(h)
	if !ok {
		return nil
	}
	estado := w.reg.AtomicLoad64(w.slotOff(slot) + 8)
	n, cur := uint32(estado>>32), uint32(estado)
	if n == 0 {
		return nil
	}
	out := make([]types.RowID, n)
	for i := int(n) - 1; i >= 0 && cur != 0; i-- {
		off := w.postOff(cur - 1)
		out[i] = types.RowID(w.reg.AtomicLoad64(off))
		cur = uint32(w.reg.AtomicLoad64(off + 8))
	}
	if !slices.IsSorted(out) {
		slices.Sort(out)
	}
	return slices.Compact(out)
}

// ParaCada varre TODAS as postings do WIP, uma vez cada.
//
// É o que a selagem consome para alimentar o Escritor, e ela NÃO entrega nada
// ordenado de propósito: o Escritor já ordena por (campo, hash, row) e
// deduplica na construção do arquivo selado. Ordenar aqui seria a MESMA
// ordenação em dois lugares, e duas implementações do mesmo critério divergem
// na primeira vez que o critério muda.
//
// A varredura é por CADEIA, e não pelo array de postings, porque a posting não
// carrega o hash: só o slot de diretório sabe a que valor ela pertence.
func (w *WIP) ParaCada(fn func(h uint64, row types.RowID)) error {
	for slot := uint32(0); slot < w.dirSlots; slot++ {
		off := w.slotOff(slot)
		h := w.reg.AtomicLoad64(off)
		if h == 0 {
			continue
		}
		estado := w.reg.AtomicLoad64(off + 8)
		for n, cur := uint32(estado>>32), uint32(estado); n > 0 && cur != 0; n-- {
			p := w.postOff(cur - 1)
			fn(h, types.RowID(w.reg.AtomicLoad64(p)))
			cur = uint32(w.reg.AtomicLoad64(p + 8))
		}
	}
	return nil
}

// busca acha o slot do hash, ou reporta que ele não está no diretório.
//
// Para no primeiro slot VAZIO, e isso é correto porque este índice é
// append-only dentro do segmento: não há Clear, não há lápide, e portanto
// nenhuma cadeia de sondagem pode ter sido cortada no meio (spec §5.2).
func (w *WIP) busca(h uint64) (uint32, bool) {
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

// sondaOuCria devolve o slot do hash, ocupando um vazio se ele ainda não
// estiver no diretório. Conta os slots examinados em w.sondas.
//
// O hash é publicado ANTES de qualquer estado, e o slot recém-ocupado tem
// estado zero — um leitor que enxergue o hash novo lê contagem 0 e devolve
// nada, que é a resposta certa até o Set publicar a primeira posting.
func (w *WIP) sondaOuCria(h uint64) uint32 {
	mask := w.dirSlots - 1
	w.sondas = 0
	for i := uint32(0); i < w.dirSlots; i++ {
		slot := (uint32(h) + i) & mask
		w.sondas++
		off := w.slotOff(slot)
		switch w.reg.AtomicLoad64(off) {
		case 0:
			w.reg.AtomicStore64(off, h)
			w.distintos++
			return slot
		case h:
			return slot
		}
	}
	// Inalcançável: talvezCresce mantém a ocupação em 70%, então sempre há
	// slot vazio. Chegar aqui é bug de load factor, e um erro devolvido seria
	// tratado como falha de I/O pelo chamador — que é o balde errado (§101).
	panic(fmt.Sprintf("segindex: diretório de %s cheio com %d slots e %d distintos (bug de load factor)",
		w.caminho, w.dirSlots, w.distintos))
}

// talvezCresce garante que cabem mais um valor distinto e mais uma posting.
//
// O rehash vem PRIMEIRO porque ele desloca o array de postings: crescer o array
// antes seria dimensioná-lo contra uma base que o rehash muda em seguida.
func (w *WIP) talvezCresce() error {
	if uint64(w.distintos)+1 > uint64(w.dirSlots)*cargaNum/cargaDen {
		if err := w.rehash(); err != nil {
			return err
		}
	}
	if w.postCount == w.postCap {
		if err := w.crescePostings(); err != nil {
			return err
		}
	}
	return nil
}

// crescePostings dobra a capacidade do array de postings.
func (w *WIP) crescePostings() error {
	novo := uint64(w.postCap) * 2
	if novo == 0 {
		novo = slotsMinimoWIP
	}
	if err := w.garante(w.postBase + int64(novo)*TamPosting); err != nil {
		return err
	}
	w.atualizaCapacidade()
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
	novoSlots := w.dirSlots * 2
	if novoSlots < w.dirSlots {
		return fmt.Errorf("segindex: diretório de %s no teto de %d slots", w.caminho, w.dirSlots)
	}
	novaBase := int64(HeaderWIP) + int64(novoSlots)*TamSlotWIP
	if err := w.garante(novaBase + int64(w.postCap)*TamPosting); err != nil {
		return err
	}

	antigo := make([]uint64, 0, int(w.dirSlots)*2)
	for i := uint32(0); i < w.dirSlots; i++ {
		off := w.slotOff(i)
		antigo = append(antigo, w.reg.AtomicLoad64(off), w.reg.AtomicLoad64(off+8))
	}

	// Desloca as postings para cima. copy é memmove, então a sobreposição
	// entre origem e destino é tratada; a view cobre as duas de uma vez porque
	// WithBytes não pode ser reentrado (o RWMutex do mmap não é reentrante).
	if n := int(w.postCount) * TamPosting; n > 0 {
		desloc := int(novaBase - w.postBase)
		w.reg.WithBytes(int(w.postBase), desloc+n, func(b []byte) {
			copy(b[desloc:], b[:n])
		})
	}
	// Zera o diretório novo — que cobre o antigo e o espaço que as postings
	// acabaram de deixar. Só depois disso os slots voltam a ser reindexados.
	w.reg.WithBytes(HeaderWIP, int(novoSlots)*TamSlotWIP, func(b []byte) { clear(b) })

	w.dirSlots = novoSlots
	w.postBase = novaBase
	mask := novoSlots - 1
	for i := 0; i < len(antigo); i += 2 {
		h, estado := antigo[i], antigo[i+1]
		if h == 0 {
			continue
		}
		for p := uint32(0); ; p++ {
			off := w.slotOff((uint32(h) + p) & mask)
			if w.reg.AtomicLoad64(off) == 0 {
				// Estado primeiro, hash por último: o hash é o marcador de
				// ocupação, e publicá-lo antes exporia um slot que se diz
				// habitado com contagem zero. É a mesma ordem do Set e a mesma
				// que internal/index/hash/index.go documenta.
				w.reg.AtomicStore64(off+8, estado)
				w.reg.AtomicStore64(off, h)
				break
			}
		}
	}
	w.atualizaCapacidade()
	w.gravaHeader()
	return nil
}

// garante estende o mapeamento até tamanho, se ele ainda não chegou lá.
func (w *WIP) garante(tamanho int64) error {
	if w.reg.Size() >= tamanho {
		return nil
	}
	return w.reg.Grow(tamanho)
}

// atualizaCapacidade recalcula postCap a partir do tamanho REAL do mapeamento.
// A capacidade é sempre derivada, nunca lembrada: um Grow que arredondasse para
// cima deixaria capacidade paga e não usada.
func (w *WIP) atualizaCapacidade() {
	w.postCap = uint32((w.reg.Size() - w.postBase) / TamPosting)
}

// recontaDistintos varre o diretório contando slots ocupados. O(dirSlots), uma
// vez por abertura.
func (w *WIP) recontaDistintos() {
	w.distintos = 0
	for i := uint32(0); i < w.dirSlots; i++ {
		if w.reg.AtomicLoad64(w.slotOff(i)) != 0 {
			w.distintos++
		}
	}
}

// gravaHeader escreve os campos de dimensão no header.
//
// Ele é chamado na criação, no rehash e no Fecha — não a cada Set. Entre um e
// outro, o postCount do ARQUIVO fica atrás do postCount em memória, e isso não
// custa nada porque não existe leitor para quem a diferença importe: um .widx
// que sobreviva a um crash é APAGADO no Open, nunca adotado (spec §2.5). Quem
// lê o arquivo é a selagem, depois do Fecha.
func (w *WIP) gravaHeader() {
	w.reg.WithBytes(0, HeaderWIP, func(b []byte) {
		copy(b[0:4], MagicWIP)
		b[offVersaoWIP] = VersaoWIP
		binary.LittleEndian.PutUint16(b[offCampoWIP:offCampoWIP+2], uint16(w.campo))
		binary.LittleEndian.PutUint32(b[offDirSlotsWIP:offDirSlotsWIP+4], w.dirSlots)
		binary.LittleEndian.PutUint32(b[offPostCount:offPostCount+4], w.postCount)
	})
}

// Fecha grava o header definitivo e desmonta o mapeamento.
//
// NÃO há msync, e a ausência é a decisão: o .widx não participa do checkpoint
// nem da ordem de durabilidade do §48 (spec §5.3). Ele é índice do segmento
// CORRENTE, dado derivado, e a recuperação mais barata que está correta é
// derivá-lo de novo a partir do próprio segmento.
func (w *WIP) Fecha() error {
	w.gravaHeader()
	return w.reg.Close()
}

// Apaga desmonta o mapeamento e remove o arquivo.
//
// É o que a selagem chama depois de emitir o .sidx, e o que o Open chama sobre
// um .widx órfão. Não grava o header: o arquivo está indo embora, e escrever
// nele antes seria sujar páginas que o filesystem teria de gravar por nada.
//
// Arquivo já ausente não é erro: as duas chamadas acima podem correr sobre um
// resto de uma execução anterior, e o estado desejado — não existir — é o
// mesmo.
func (w *WIP) Apaga() error {
	errs := []error{w.reg.Close()}
	if err := os.Remove(w.caminho); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("segindex: remover %s: %w", w.caminho, err))
	}
	return errors.Join(errs...)
}
