// Package hash implementa o índice hash FÍSICO de coluna secundária (spec
// §63 regra R2): uma tabela open-addressing disk-backed (mmap) que mapeia o
// valor canônico de uma coluna para o conjunto de rowIds que o carregam.
//
// Ele é o motor que faltava. O `index: hash` já era aceito no YAML, o planner
// já tinha a regra R2 e o executor já sabia consumir um LookupHash — mas
// nenhum *storage.Store implementava a capability, então toda igualdade sobre
// coluna declarada com hash degradava para full scan em silêncio. No piloto
// isso é concreto: `WHERE node = 'x'` varria 17,8M linhas e a query morria com
// "context deadline exceeded" depois de 1.175.015 candidatos.
//
// # Como ele difere do primary
//
// O primary (internal/index/primary) é o molde deste arquivo, e as diferenças
// não são estilo:
//
//   - o primary mapeia key→UMA row (bijeção), este mapeia valor→MUITAS rows.
//     Todas as postings de um valor caem na MESMA cadeia de probe, porque
//     partem do mesmo hash; o Lookup varre a cadeia inteira até o primeiro
//     slot vazio;
//   - o primary NUNCA remove. Este remove: um update troca o valor da coluna,
//     e a posting antiga precisa sair, ou a query devolveria a row sob um
//     valor que ela não tem mais. A remoção é por LÁPIDE (slotDeleted), nunca
//     por slot vazio — esvaziar o slot cortaria a cadeia de probe de todos os
//     valores que passam por ele, e o sintoma seria falso NEGATIVO permanente
//     nos vizinhos. As lápides são purgadas na reconstrução;
//   - o primary VALIDA a colisão lendo o record (ValidateFunc). Este não valida
//     nada: devolve SUPERSET e deixa a revalidação do §44 filtrar, que é o
//     mesmo contrato que o executor já cumpre para o bitmap. Sem ValidateFunc
//     não há callback reentrante e não há o risco de deadlock com o rehash
//     enfileirado que o Store documenta para o primary.
//
// # Consistência
//
// As postings podem estar momentaneamente defasadas para leitores — o
// executor sempre revalida o predicate contra o estado corrente (§44), e o
// replay do tail reconverge o índice após crash. Falso positivo é tolerado;
// falso NEGATIVO não, e é por isso que a fase de Set roda antes do commit da
// location e a de Clear depois.
package hash

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Layout do arquivo %05d.hidx (little-endian):
//
//	0   4  magic "NDBH"
//	4   1  version = 1
//	5   1  hashKind = 1 (fnv1a64+fmix64, o MESMO do primary)
//	6   2  fieldID u16 LE
//	8   8  slotCount (u64, potência de 2)
//	16  8  seed (u64, 0 nesta versão)
//	24  40 reservado
//	64  .. slots de 16 bytes: [hash u64][rowId u64]
//
// Estados de slot pelo campo hash: 0=EMPTY, 1=DELETED; hashes reais são
// sempre >= 2 (é o que o remapReserved do primary garante).
//
// NÃO existe campo de ocupação no header, e a ausência é deliberada: um
// contador em página mmap não é confiável depois de um crash (o kernel pode
// ter escrito os slots e não o header, ou o contrário), e publicá-lo
// convidaria a próxima revisão a confiar nele. A ocupação é RECONTADA por
// varredura no Open — é o mesmo argumento de primary/index.go, e ali ele já
// está escrito.
const (
	headerSize = 64
	slotSize   = 16

	idxVersion  = 1
	hashKindFnv = 1

	slotEmpty   = 0
	slotDeleted = 1

	// loadFactorNum/Den: reconstrói ao exceder 70% de OCUPAÇÃO EFETIVA, que
	// conta lápides junto com postings vivas. Lápide não guarda dado, mas
	// alonga o probe exatamente como um slot cheio: ignorá-la faria uma coluna
	// muito atualizada degradar para varredura linear da tabela com a tabela
	// meio vazia.
	loadFactorNum = 7
	loadFactorDen = 10

	minSlotCount = 1024
)

var idxMagic = [4]byte{'N', 'D', 'B', 'H'}

// Index é o índice hash de UM campo.
//
// Concorrência: Set e Clear são exclusivos do writer único; Lookup é seguro
// para leitores concorrentes com o writer.
type Index struct {
	mu        sync.RWMutex // protege region/slotCount contra o swap da reconstrução
	region    *mmap.Region
	path      string
	fieldID   types.FieldID
	slotCount uint64
	// usados conta postings vivas; lapides conta slots removidos. Os dois são
	// RECONTADOS no Open e mantidos em memória — nunca lidos do arquivo.
	usados  atomic.Uint64
	lapides atomic.Uint64
	hashFn  func([]byte) uint64

	// scratch é o buffer da chave canônica no caminho do writer (Set/Clear).
	// O Lookup aloca o próprio: ele roda concorrente com o writer.
	scratch []byte
}

func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len64(v-1)
}

func fileSizeFor(slots uint64) int64 { return headerSize + int64(slots)*slotSize }

func slotOff(i uint64) int64 { return headerSize + int64(i)*slotSize }

// Path devolve o caminho do arquivo de índice do campo dentro de dir.
func Path(dir string, field types.FieldID) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.hidx", field))
}

func writeHeader(r *mmap.Region, field types.FieldID, slots uint64) {
	r.WithBytes(0, headerSize, func(h []byte) {
		copy(h[0:4], idxMagic[:])
		h[4] = idxVersion
		h[5] = hashKindFnv
		binary.LittleEndian.PutUint16(h[6:8], uint16(field))
		binary.LittleEndian.PutUint64(h[8:16], slots)
		binary.LittleEndian.PutUint64(h[16:24], 0) // seed
	})
}

// readHeader valida o header e devolve o slotCount.
//
// A conferência do fieldID é o que impede o pior acidente operacional deste
// formato: dois arquivos trocados de lugar produziriam um índice que responde
// com as rows do campo errado, sem erro em camada nenhuma — a revalidação do
// §44 filtraria tudo e a query devolveria vazio.
func readHeader(r *mmap.Region, field types.FieldID, path string) (uint64, error) {
	var (
		slots uint64
		err   error
	)
	r.WithBytes(0, headerSize, func(h []byte) {
		switch {
		case [4]byte(h[0:4]) != idxMagic:
			err = fmt.Errorf("hash: magic inválida %q em %s", h[0:4], path)
		case h[4] != idxVersion:
			err = fmt.Errorf("hash: version desconhecida %d em %s", h[4], path)
		case h[5] != hashKindFnv:
			err = fmt.Errorf("hash: hashKind desconhecido %d em %s (rebuild necessário)", h[5], path)
		case types.FieldID(binary.LittleEndian.Uint16(h[6:8])) != field:
			err = fmt.Errorf("hash: %s pertence ao campo %d, esperado %d",
				path, binary.LittleEndian.Uint16(h[6:8]), field)
		default:
			slots = binary.LittleEndian.Uint64(h[8:16])
		}
	})
	if err != nil {
		return 0, err
	}
	if slots < minSlotCount || slots&(slots-1) != 0 {
		return 0, fmt.Errorf("hash: slotCount inválido %d em %s", slots, path)
	}
	// r.Size() fora do callback: WithBytes segura o RLock e o RWMutex não é
	// reentrante.
	if r.Size() < fileSizeFor(slots) {
		return 0, fmt.Errorf("hash: %s tem %d bytes, menos que o necessário para %d slots",
			path, r.Size(), slots)
	}
	return slots, nil
}

// Open cria (ou reabre) o índice do campo com pelo menos minSlots slots
// (arredondado para potência de 2). Uma reconstrução interrompida por crash
// deixa um ".new" órfão, que é descartado aqui.
func Open(path string, field types.FieldID, minSlots uint64) (*Index, error) {
	if field == 0 {
		return nil, fmt.Errorf("hash: fieldID 0 é reservado no dicionário")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("hash: criar %s: %w", filepath.Dir(path), err)
	}
	if err := os.Remove(path + ".new"); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("hash: remover reconstrução órfã: %w", err)
	}
	st, err := os.Stat(path)
	novo := os.IsNotExist(err)
	if err != nil && !novo {
		return nil, fmt.Errorf("hash: stat %s: %w", path, err)
	}
	if !novo && st.Size() == 0 {
		novo = true
	}

	ix := &Index{path: path, fieldID: field, hashFn: primary.HashKey}
	slots := nextPow2(max(minSlots, minSlotCount))
	if novo {
		region, err := mmap.Open(path, fileSizeFor(slots))
		if err != nil {
			return nil, err
		}
		writeHeader(region, field, slots)
		ix.region, ix.slotCount = region, slots
		return ix, nil
	}

	region, err := mmap.Open(path, st.Size())
	if err != nil {
		return nil, err
	}
	slots, err = readHeader(region, field, path)
	if err != nil {
		// O erro do header é o que sobe; o Close aqui só evita vazar o
		// descritor de um índice que não vai ser usado.
		_ = region.Close()
		return nil, err
	}
	ix.region, ix.slotCount = region, slots
	ix.reconta()
	return ix, nil
}

// reconta varre a tabela e refaz os contadores de ocupação. É a fonte de
// verdade deles: ver a nota do layout sobre por que o header não os guarda.
func (ix *Index) reconta() {
	var usados, lapides uint64
	for i := uint64(0); i < ix.slotCount; i++ {
		switch ix.region.AtomicLoad64(slotOff(i)) {
		case slotEmpty:
		case slotDeleted:
			lapides++
		default:
			usados++
		}
	}
	ix.usados.Store(usados)
	ix.lapides.Store(lapides)
}

// SlotCount devolve o número de slots da tabela.
func (ix *Index) SlotCount() uint64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.slotCount
}

// Usados devolve o número de postings vivas.
func (ix *Index) Usados() uint64 { return ix.usados.Load() }

// Lapides devolve o número de slots removidos e ainda não purgados.
func (ix *Index) Lapides() uint64 { return ix.lapides.Load() }

// FieldID devolve o campo indexado.
func (ix *Index) FieldID() types.FieldID { return ix.fieldID }

// HashKind devolve o identificador do algoritmo de hash (gravado no header e
// no manifest).
func (ix *Index) HashKind() uint8 { return hashKindFnv }

// chave serializa v e devolve o slot de partida. Usa o buffer do writer.
func (ix *Index) chave(v types.Value) (uint64, error) {
	key, err := valuekey.Append(ix.scratch[:0], v)
	if err != nil {
		return 0, err
	}
	ix.scratch = key
	return ix.hashFn(key), nil
}

// Set liga a posting (v, rowID). É INSERT-IF-ABSENT e portanto idempotente:
// o replay do tail reaplica os mesmos records depois de um crash, e uma
// segunda inserção do mesmo par deixaria a mesma row duas vezes na cadeia —
// que o unionIter do executor emitiria como duas linhas num OR.
//
// Exclusivo do writer.
func (ix *Index) Set(v types.Value, rowID types.RowID) error {
	h, err := ix.chave(v)
	if err != nil {
		return err
	}
	if err := ix.talvezReconstrua(); err != nil {
		return err
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	mask := ix.slotCount - 1
	// Uma lápide encontrada no caminho é o melhor lugar para inserir, mas só
	// DEPOIS de varrer a cadeia inteira: o par pode estar adiante dela, e
	// inserir cedo criaria a duplicata que o godoc acima proíbe.
	lapide, temLapide := uint64(0), false
	for probe := uint64(0); probe < ix.slotCount; probe++ {
		idx := (h + probe) & mask
		off := slotOff(idx)
		switch sh := ix.region.AtomicLoad64(off); sh {
		case slotEmpty:
			destino := off
			if temLapide {
				destino = slotOff(lapide)
				// Go não tem Sub atômico para Uint64; ^0 é o complemento de
				// dois de -1.
				ix.lapides.Add(^uint64(0))
			}
			// Ordem de publicação: rowId primeiro, hash (marcador de ocupação)
			// por último com release — o leitor que enxerga o hash
			// necessariamente enxerga o rowId.
			ix.region.AtomicStore64(destino+8, uint64(rowID))
			ix.region.AtomicStore64(destino, h)
			ix.usados.Add(1)
			return nil
		case slotDeleted:
			if !temLapide {
				lapide, temLapide = idx, true
			}
		case h:
			if types.RowID(ix.region.AtomicLoad64(off+8)) == rowID {
				return nil // já está lá
			}
		}
	}
	return fmt.Errorf("hash: tabela do campo %d cheia com %d slots (bug de load factor)", ix.fieldID, ix.slotCount)
}

// Clear desliga a posting do valor ANTIGO de uma row.
//
// `atual` é o valor que a row passou a ter no MESMO apply, e existe por uma
// razão de correção que não é hipotética o bastante para ser ignorada. A
// tabela endereça o slot por um hash de 64 bits, e dois valores DISTINTOS
// podem cair no mesmo slot. Quando isso acontece justamente entre o valor
// velho e o novo de um update, a sequência do §10.1 se vira contra si mesma: a
// fase de Set encontra a posting antiga no lugar exato onde a nova iria,
// conclui (corretamente) que o par já existe e não faz nada; a fase de Clear
// então remove essa posting — e a row fica SEM posting nenhuma. Falso negativo
// permanente, que a revalidação do §44 não recupera, porque ela só sabe
// descartar candidato a mais.
//
// A guarda é comparar os dois slots e PULAR a remoção quando coincidem: o que
// o Clear removeria é exatamente o que o Set acabou de afirmar. O custo do
// resíduo é um falso positivo (a posting do valor velho continua respondendo
// pelo novo), que é o lado tolerado do contrato.
//
// atual com Type TypeNull significa que o campo SAIU da row — não há nada a
// preservar e a remoção segue.
//
// Valor nunca visto, rowId ausente da cadeia: no-op. Exclusivo do writer.
func (ix *Index) Clear(velho, atual types.Value, rowID types.RowID) error {
	if atual.Type != types.TypeNull {
		// O hash do `atual` primeiro, para que os dois usem o MESMO scratch
		// sem que um invalide a chave do outro.
		hAtual, err := ix.chave(atual)
		if err != nil {
			return err
		}
		hVelho, err := ix.chave(velho)
		if err != nil {
			return err
		}
		if hAtual == hVelho {
			return nil
		}
		return ix.removeSlot(hVelho, rowID)
	}
	hVelho, err := ix.chave(velho)
	if err != nil {
		return err
	}
	return ix.removeSlot(hVelho, rowID)
}

// removeSlot troca o hash do slot do par (h, rowID) pela lápide.
//
// Lápide e não EMPTY: um slot vazio TERMINA a cadeia de probe, e todos os
// valores cuja cadeia passa por ele deixariam de ser encontrados — falso
// negativo permanente e silencioso nos vizinhos, que é o defeito mais caro
// possível nesta engine.
func (ix *Index) removeSlot(h uint64, rowID types.RowID) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	mask := ix.slotCount - 1
	for probe := uint64(0); probe < ix.slotCount; probe++ {
		off := slotOff((h + probe) & mask)
		switch sh := ix.region.AtomicLoad64(off); sh {
		case slotEmpty:
			return nil // a cadeia acabou sem o par: nada a limpar
		case slotDeleted:
			continue
		case h:
			if types.RowID(ix.region.AtomicLoad64(off+8)) == rowID {
				ix.region.AtomicStore64(off, slotDeleted)
				ix.usados.Add(^uint64(0))
				ix.lapides.Add(1)
				return nil
			}
		}
	}
	return nil
}

// Lookup devolve um iterador dos rowIds do valor, em ordem CRESCENTE.
//
// A ordem é INVARIANTE, não estética. O intersectIter e o unionIter do
// executor (internal/query/executor/candidate.go) assumem iteradores
// ascendentes, e o planner mistura termos hash e bitmap no MESMO
// IndexUnionPlan: devolver desordenado produziria interseção INCOMPLETA em
// silêncio.
//
// O resultado é um SUPERSET de candidatos por construção — colisão de hash não
// é validada aqui, e postings órfãs são estado tolerado. Quem filtra é a
// revalidação do §44.
//
// As postings de um valor são MATERIALIZADAS num slice para poderem ser
// ordenadas, e isso é a única alocação O(resultado) do caminho de query. O
// limite é o número de rows que compartilham UM valor, que num campo
// declarado com hash é pequeno por construção: hash é o índice de ALTA
// cardinalidade, e uma coluna cujos valores se repetem em milhões de rows é
// justamente o caso em que a spec manda declarar bitmap. Ordenar exige ver
// todos os elementos; iterar a cadeia sem materializá-la devolveria a ordem
// arbitrária do probe.
func (ix *Index) Lookup(v types.Value) (*Iterator, error) {
	// Buffer próprio: o Lookup roda concorrente com o writer, que é dono do
	// scratch.
	key, err := valuekey.Append(nil, v)
	if err != nil {
		return nil, err
	}
	rows := ix.colhe(ix.hashFn(key))
	slices.Sort(rows)
	// Compact depois do Sort: o contrato dos iteradores de índice é
	// ESTRITAMENTE crescente, e um id repetido faria o unionIter emitir a
	// mesma linha duas vezes num OR. O Set já é insert-if-absent, então isto
	// é a fronteira reafirmando o que a inserção promete — uma passada linear
	// sobre um slice que já está em memória.
	return &Iterator{rows: slices.Compact(rows), pos: -1}, nil
}

// colhe varre a cadeia de probe do hash e devolve os rowIds encontrados.
func (ix *Index) colhe(h uint64) []types.RowID {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var rows []types.RowID
	mask := ix.slotCount - 1
	for probe := uint64(0); probe < ix.slotCount; probe++ {
		off := slotOff((h + probe) & mask)
		switch sh := ix.region.AtomicLoad64(off); sh {
		case slotEmpty:
			return rows
		case slotDeleted:
			continue
		case h:
			rows = append(rows, types.RowID(ix.region.AtomicLoad64(off+8)))
		}
	}
	return rows
}

// talvezReconstrua dispara a reconstrução quando a ocupação efetiva
// (postings + lápides) passa do load factor. Chamado FORA do RLock: a
// reconstrução toma o Lock, e o RWMutex não é reentrante.
func (ix *Index) talvezReconstrua() error {
	if (ix.usados.Load()+ix.lapides.Load()+1)*loadFactorDen <= ix.SlotCount()*loadFactorNum {
		return nil
	}
	return ix.reconstroi()
}

// reconstroi reescreve a tabela num arquivo novo, purgando as lápides, e troca
// atomicamente (rename). Chamado apenas pelo writer.
//
// O tamanho novo é decidido pelas postings VIVAS, e não pelo tamanho atual.
// Dobrar por reflexo seria um defeito caro no perfil real deste produto: cada
// update faz um Set e um Clear, então uma coluna muito atualizada acumula
// lápides sem ganhar postings, e uma tabela que dobrasse a cada limiar
// cresceria sem limite com o número de linhas parado. Quando as lápides são a
// única causa, `novo` fica igual a `oldSlots` e a reconstrução é só uma
// faxina.
//
// A tabela NUNCA encolhe: uma coluna que perdeu postings pode recuperá-las, e
// alternar entre dois tamanhos a cada limiar transformaria a reconstrução —
// que reescreve o arquivo inteiro — num custo recorrente.
func (ix *Index) reconstroi() error {
	oldSlots := ix.SlotCount()
	vivos := ix.usados.Load()
	novo := oldSlots
	for (vivos+1)*loadFactorDen > novo*loadFactorNum {
		novo *= 2
	}

	novoPath := ix.path + ".new"
	if err := os.Remove(novoPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("hash: limpar reconstrução anterior: %w", err)
	}
	novaRegiao, err := mmap.Open(novoPath, fileSizeFor(novo))
	if err != nil {
		return err
	}
	writeHeader(novaRegiao, ix.fieldID, novo)

	// Re-insere pelo hash ARMAZENADO — sem reserializar valor nenhum, que é o
	// que torna a reconstrução independente do dicionário de campos. Lápides
	// não viajam: é aqui que elas desaparecem.
	novaMask := novo - 1
	var movidos uint64
	for i := uint64(0); i < oldSlots; i++ {
		sh := ix.region.AtomicLoad64(slotOff(i))
		if sh < 2 {
			continue
		}
		rid := ix.region.AtomicLoad64(slotOff(i) + 8)
		for probe := uint64(0); ; probe++ {
			off := slotOff((sh + probe) & novaMask)
			if novaRegiao.AtomicLoad64(off) == slotEmpty {
				novaRegiao.AtomicStore64(off+8, rid)
				novaRegiao.AtomicStore64(off, sh)
				break
			}
		}
		movidos++
	}
	if err := novaRegiao.Msync(); err != nil {
		_ = novaRegiao.Close()
		return err
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	if err := os.Rename(novoPath, ix.path); err != nil {
		_ = novaRegiao.Close()
		return fmt.Errorf("hash: rename da reconstrução: %w", err)
	}
	// Pós-rename o swap é OBRIGATÓRIO: o arquivo em ix.path JÁ é a tabela
	// nova, e continuar na antiga (inode desvinculado) perderia postings em
	// silêncio. Por isso o fsync do diretório roda DEPOIS do swap e o erro sobe
	// ao final — no Store, erro de durabilidade é sticky.
	antiga := ix.region
	ix.region = novaRegiao
	ix.slotCount = novo
	ix.usados.Store(movidos)
	ix.lapides.Store(0)
	if err := antiga.Close(); err != nil {
		return fmt.Errorf("hash: fechar tabela antiga: %w", err)
	}
	return syncDirFn(filepath.Dir(ix.path))
}

// syncDirFn é indirecionada para permitir injeção de falha nos testes do
// caminho de erro da reconstrução.
var syncDirFn = syncDir

// syncDir força a durabilidade da entrada de diretório após um rename.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	// O erro do Close entra no resultado em vez de ir para um defer mudo: num
	// diretório recém-sincronizado ele é o mesmo writeback que o Sync tenta
	// forçar, e engoli-lo aqui deixaria a reconstrução se declarar durável
	// sobre uma entrada que não chegou ao disco.
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("hash: fsync do diretório %s: %w", dir, err)
	}
	return nil
}

// Msync força as páginas sujas do índice para disco.
func (ix *Index) Msync() error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.region.Msync()
}

// Close desfaz o mapeamento e fecha o arquivo.
func (ix *Index) Close() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.region.Close()
}

// Iterator percorre os rowIds de um valor em ordem estritamente crescente.
//
// Ele NÃO toca o mmap depois de criado: o Lookup já materializou as postings.
// É a diferença com o iterador do bitmap, que lê a região a cada avanço — e é
// por isso que este não precisa reentrar no Store para se proteger de um Close
// concorrente.
type Iterator struct {
	rows []types.RowID
	pos  int
}

// Next avança para o próximo rowId; false no fim.
func (it *Iterator) Next() bool {
	it.pos++
	return it.pos < len(it.rows)
}

// RowID devolve o rowId corrente.
func (it *Iterator) RowID() types.RowID {
	if it.pos < 0 || it.pos >= len(it.rows) {
		return 0
	}
	return it.rows[it.pos]
}

// Err devolve o erro do iterador (sempre nil; mantido pelo contrato).
func (it *Iterator) Err() error { return nil }

// Close libera o iterador.
func (it *Iterator) Close() error { return nil }
