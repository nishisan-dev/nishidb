// Package storage é a fachada do subsistema de storage do NishiDB: orquestra
// segments, índices e dicionários atrás de uma API única (spec §15A do
// design). Checkpoint/manifest são a S11; recovery é a S12.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/index/bitmap"
	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidArgument e ErrWriterFailed são os MESMOS valores declarados em
// internal/types, reexportados para a API deste pacote: errors.Is casa pelos
// dois caminhos, e o materializer (mais o dublê de teste dele) os classifica
// sem importar esta implementação.
var (
	ErrInvalidArgument = types.ErrInvalidArgument
	ErrWriterFailed    = types.ErrWriterFailed
)

// ErrDatasetCorrupt indica dataset que a recovery não consegue recuperar
// sozinha: manifest ausente com records, manifest ilegível, segmento do
// manifest sumido, buraco na cadeia de segmentos ou ponto S incoerente com
// os arquivos. O rebuild total é fase futura — até lá o Open recusa em vez
// de adivinhar, porque toda alternativa (truncar, pular, recriar) descarta
// dados duráveis em silêncio.
var ErrDatasetCorrupt = errors.New("storage: dataset corrompido")

// semRecords informa se nenhum segmento do diretório tem record algum (só
// header): é o que distingue um dataset recém-criado de um dataset com
// dados cujo manifest se perdeu.
func semRecords(refs []segment.SegmentRef) bool {
	for _, ref := range refs {
		if ref.SizeBytes > segment.HeaderSize {
			return false
		}
	}
	return true
}

// Options configura a abertura de um Store.
type Options struct {
	Dir           string
	SegmentSize   int64  // default 512 MiB; teto validado pelo segment
	EstimatedRows uint64 // pre-sizing da primary (piso 1<<20 slots)
	// BitmapFields são os campos com índice bitmap, por ID.
	BitmapFields []types.FieldID
	// BitmapFieldNames são os mesmos campos por NOME, resolvidos pelo
	// dicionário do próprio dataset durante o Open (criando a entry se
	// necessário). Existe porque os FieldIDs nascem de schema.Compile, que
	// precisa do dicionário — e o dicionário vive dentro do Store: exigir
	// IDs aqui obrigaria a abrir o dataset sem índices, compilar, fechar e
	// reabrir. Os dois campos se somam (união).
	BitmapFieldNames []string
	// AllowBitmapDrop autoriza abrir o dataset SEM um campo que o manifest
	// declara como indexado. Fora disso é erro: o índice ficaria em disco
	// parando de ser atualizado e, se o campo voltasse depois, reapareceria
	// INCOMPLETO — falso negativo silencioso em query.
	AllowBitmapDrop bool
	SyncEveryPut    bool // durability=sync (testes/config)
}

// Stats é o snapshot mínimo de estado do Store.
type Stats struct {
	RowCount          uint64
	SegmentsTotal     uint32
	CurrentSegmentID  uint32
	PrimaryLoadFactor float64
	OverlayLen        int
}

const defaultSegmentSize = 512 << 20

// Store é a fachada do storage.
//
// Concorrência: Put e Sync são exclusivos do writer único (não são
// thread-safe entre si); GetByKey/GetByRowID/ScanLive/LookupBitmap/Sequence/
// Stats são seguros para leitores concorrentes com o writer.
type Store struct {
	opts Options

	dict    *fielddict.Dict
	segW    *segment.Writer
	segR    *segment.Reader
	prim    *primary.Index
	locs    *rowindex.Locations
	seqs    *rowindex.Sequences
	bitmaps map[types.FieldID]*bitmap.Index

	nextRowID atomic.Uint64
	rowCount  atomic.Uint64
	epoch     uint64 // último epoch de manifest gravado (writer-only)

	// curSegID espelha o segmento corrente para leitura concorrente em
	// Stats (segment.Writer não é thread-safe).
	curSegID atomic.Uint32
	// clearFailures conta erros tolerados na fase de Clear dos bitmaps
	// (deixam falso positivo, filtrado pelo §44); exposto via métricas na S13.
	clearFailures atomic.Uint64
	// stickyErr: primeiro erro pós-Append ou de durabilidade. Sem rollback
	// parcial, o writer fica FALHO até Close + reabertura com recovery.
	// Protegido por wmu.
	stickyErr error

	// wmu serializa as operações do writer (Put/Sync/Checkpoint): o
	// contrato "writer único" deixa de ser convenção e vira mecanismo —
	// um Checkpoint de coordenador concorrente com Put não pode mais
	// corromper segTally/epoch nem ler offset além do fsyncado.
	wmu sync.Mutex

	// cpTrace é o seam de teste da ordem de durabilidade do checkpoint
	// (nil em produção).
	cpTrace func(step string)
	// clearErrHook/setErrHook são seams de teste das fases de Clear e Set
	// dos bitmaps (nil em produção; setErrHook viabiliza o phantom commit
	// com payload replayável).
	clearErrHook func(types.FieldID) error
	setErrHook   func(types.FieldID) error

	// segTally acumula os totais por segmento que o writer conhece (para as
	// stats do manifest §9; em memória — a S12 os reconstrói no replay).
	// Writer-only.
	segTally map[uint32]*segStats

	// lock garante acesso exclusivo ao diretório do dataset.
	lock *datasetLock

	// Buffers exclusivos do writer (Put).
	buildBuf []byte // record em construção
	prevBuf  []byte // fallback de leitura do estado anterior
	valBuf   []byte // validação de colisão no Insert
}

// Open abre o dataset com recovery completo (design §10.3):
//
//   - dataset novo (sem manifest, sem segmentos): cria a estrutura e grava
//     o manifest inicial — a partir da criação, "manifest ausente COM
//     dados" indica genuinamente corrupção/adulteração, não uma janela
//     normal de crash;
//   - manifest ausente COM segmentos, ou manifest corrompido: erro fatal
//     claro (rebuild total é fase futura);
//   - manifest válido: recovery — seeds (§14.15), adoção de segmentos
//     pós-checkpoint (inclusive órfão de rotação), tail scan com replay e
//     truncamento do lixo, writer posicionado no fim válido.
//
// Em ambos os fluxos o Open termina com um checkpoint imediato (estado
// limpo; epoch nunca regride) e o modo falho (sticky) zerado — reabrir é o
// remédio da taxonomia §14.14.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("%w: Dir vazio", ErrInvalidArgument)
	}
	if opts.SegmentSize == 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	metaDir := filepath.Join(opts.Dir, "metadata")
	dataDir := filepath.Join(opts.Dir, "data")
	idxDir := filepath.Join(opts.Dir, "indexes")
	cpDir := filepath.Join(opts.Dir, "checkpoint")
	for _, d := range []string{metaDir, dataDir, idxDir, cpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("storage: criar %s: %w", d, err)
		}
	}
	// Política de fsync de diretório (design §14): as ENTRADAS dos
	// diretórios recém-criados precisam ser duráveis — um power loss após o
	// primeiro checkpoint não pode deixar Dir/ sem checkpoint/ (viraria
	// "dados sem manifest" = erro fatal na recovery).
	for _, d := range []string{opts.Dir, metaDir, dataDir, idxDir, cpDir} {
		if err := fsyncDir(d); err != nil {
			return nil, err
		}
	}
	s := &Store{
		opts:     opts,
		bitmaps:  make(map[types.FieldID]*bitmap.Index),
		segTally: make(map[uint32]*segStats),
	}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()

	// Lock antes de tocar em qualquer arquivo do dataset. O Close no defer
	// acima o libera nos caminhos de erro — um Open que falha não pode
	// deixar o dataset travado até o processo morrer.
	var err error
	if s.lock, err = acquireDatasetLock(opts.Dir); err != nil {
		return nil, err
	}
	if s.dict, err = fielddict.Open(filepath.Join(metaDir, "fields.dict")); err != nil {
		// O erro do dicionário já traz arquivo, versão e remediação; aqui entra
		// o dataset, que é o identificador que o operador tem na mão.
		return nil, fmt.Errorf("storage: abrir o dataset %s: %w", opts.Dir, err)
	}
	if s.segR, err = segment.OpenReader(dataDir); err != nil {
		return nil, err
	}
	// Piso de 1<<20 slots do design §5 aplica-se aqui, não no pacote primary.
	// Teto de sanidade evita wraparound do *10/7 com config absurda.
	if opts.EstimatedRows > 1<<40 {
		return nil, fmt.Errorf("%w: EstimatedRows %d acima do teto de sanidade (2^40)", ErrInvalidArgument, opts.EstimatedRows)
	}
	minSlots := max(uint64(1<<20), opts.EstimatedRows*10/7)
	if s.prim, err = primary.Open(filepath.Join(idxDir, "primary.idx"), minSlots); err != nil {
		return nil, err
	}
	minRows := max(uint64(512), opts.EstimatedRows)
	if s.locs, err = rowindex.OpenLocations(filepath.Join(idxDir, "locations.idx"), minRows); err != nil {
		return nil, err
	}
	if s.seqs, err = rowindex.OpenSequences(filepath.Join(idxDir, "sequences.idx"), minRows); err != nil {
		return nil, err
	}
	m, mErr := checkpoint.Load(s.manifestPath())
	refs, err := segment.ListSegments(dataDir)
	if err != nil {
		return nil, err
	}

	campos, err := s.resolveBitmapFields(opts)
	if err != nil {
		return nil, err
	}
	if m != nil {
		if err := validaBitmapsContraManifest(m, campos, opts.AllowBitmapDrop); err != nil {
			return nil, err
		}
	}
	for _, field := range campos {
		dir := filepath.Join(idxDir, "bitmap", fmt.Sprintf("%05d", field))
		ix, err := bitmap.Open(dir, field)
		if err != nil {
			return nil, err
		}
		s.bitmaps[field] = ix
	}
	switch {
	case errors.Is(mErr, checkpoint.ErrNoManifest) && semRecords(refs):
		// Dataset novo. Entram aqui tanto o diretório virgem quanto o
		// dataset que crashou DENTRO do primeiro Open: o segmento 1 nasce
		// antes do manifest inicial, então "segmento sem record e sem
		// manifest" é uma janela normal de crash — condená-la faria um
		// dataset que nunca teve um único record nascer inacessível.
		// Um segmento vazio já criado é ADOTADO (não recriado): o openNew usa
		// O_EXCL e falharia contra o arquivo existente.
		var segID uint32
		var off int64
		if len(refs) > 0 {
			segID, off = refs[len(refs)-1].ID, int64(segment.HeaderSize)
		}
		if s.segW, err = segment.OpenWriter(dataDir, opts.SegmentSize, segID, off); err != nil {
			return nil, fmt.Errorf("storage: abrir segmentos (SegmentSize=%d): %w", opts.SegmentSize, err)
		}
		cur, _ := s.segW.Current()
		s.curSegID.Store(cur)
	case errors.Is(mErr, checkpoint.ErrNoManifest):
		return nil, fmt.Errorf(
			"%w: %s contém records mas o manifest está ausente (o manifest existe desde a criação do dataset; rebuild total é fase futura)",
			ErrDatasetCorrupt, dataDir)
	case errors.Is(mErr, checkpoint.ErrCorruptManifest):
		return nil, fmt.Errorf("%w: manifest ilegível: %w", ErrDatasetCorrupt, mErr)
	case mErr != nil:
		// EIO/EACCES/ENOMEM ao LER o manifest são transitórios: afirmar
		// "dataset corrompido" aqui induz a resposta operacional errada
		// (restaurar backup, reconstruir), que é destrutiva. Só corrupção
		// confirmada justifica a afirmação.
		return nil, fmt.Errorf("storage: ler o manifest de %s: %w", opts.Dir, mErr)
	default:
		if err := s.recoverFrom(m, refs, dataDir); err != nil {
			if errors.Is(err, ErrDatasetCorrupt) {
				return nil, err // já classificado e com contexto próprio
			}
			// Erro do replay que não é corrupção: tipicamente o dataset tem
			// um record incompatível com a configuração desta abertura — por
			// exemplo um valor acima do teto do índice num campo que só
			// agora foi declarado indexado. Sem contexto, o erro sai cru,
			// sem nomear sequer o dataset. O remédio está no §14.22.
			return nil, fmt.Errorf(
				"storage: recovery de %s falhou ao aplicar o tail: %w (se a causa for um índice, reabra sem declarar o campo — o dado não se perde)",
				opts.Dir, err)
		}
	}
	// Checkpoint imediato: manifest desde a criação e estado limpo pós-
	// recovery (novo S = tail; epoch nunca regride).
	if err := s.Checkpoint(); err != nil {
		return nil, err
	}
	ok = true
	return s, nil
}

// resolveBitmapFields devolve a união, ordenada e sem duplicatas, dos
// campos declarados por ID e por nome. Os nomes são resolvidos pelo
// dicionário do dataset, criando a entry se ainda não existir (a regra
// fsync-before-first-use vale, é o próprio LookupOrCreate que a aplica).
func (s *Store) resolveBitmapFields(opts Options) ([]types.FieldID, error) {
	set := make(map[types.FieldID]struct{}, len(opts.BitmapFields)+len(opts.BitmapFieldNames))
	for _, field := range opts.BitmapFields {
		if field == 0 {
			return nil, fmt.Errorf("%w: fieldID 0 em BitmapFields", ErrInvalidArgument)
		}
		set[field] = struct{}{}
	}
	for _, nome := range opts.BitmapFieldNames {
		if nome == "" {
			return nil, fmt.Errorf("%w: nome vazio em BitmapFieldNames", ErrInvalidArgument)
		}
		id, err := s.dict.LookupOrCreate([]byte(nome))
		if err != nil {
			return nil, fmt.Errorf("storage: resolver o campo de bitmap %q: %w", nome, err)
		}
		set[id] = struct{}{}
	}
	out := make([]types.FieldID, 0, len(set))
	for field := range set {
		out = append(out, field)
	}
	slices.Sort(out)
	return out, nil
}

// validaBitmapsContraManifest rejeita as duas divergências que produzem
// índice incompleto sem sintoma. Query indexada é SUPERSET revalidado
// (§44): falso positivo o executor filtra, falso negativo ninguém recupera.
func validaBitmapsContraManifest(m *checkpoint.Manifest, campos []types.FieldID, permiteDrop bool) error {
	declarado := make(map[types.FieldID]bool, len(campos))
	for _, field := range campos {
		declarado[field] = true
	}
	noManifest := make(map[types.FieldID]bool, len(m.Bitmaps))
	for _, bi := range m.Bitmaps {
		noManifest[bi.FieldID] = true
	}

	// (1) Índice que o dataset tem e que esta abertura não declara: ele
	// pararia de receber sets/clears e reapareceria incompleto se o campo
	// voltasse.
	if !permiteDrop {
		var abandonados []types.FieldID
		for _, bi := range m.Bitmaps {
			if !declarado[bi.FieldID] {
				abandonados = append(abandonados, bi.FieldID)
			}
		}
		if len(abandonados) > 0 {
			return fmt.Errorf(
				"%w: o manifest declara índice bitmap para o(s) campo(s) %v, ausente(s) desta abertura; "+
					"abrir assim faria o índice parar de ser atualizado e reaparecer INCOMPLETO depois. "+
					"Declare o(s) campo(s) em BitmapFields/BitmapFieldNames ou use AllowBitmapDrop",
				ErrDatasetCorrupt, abandonados)
		}
	}

	// (2) Índice declarado agora que o dataset não tem, com rows já
	// gravadas: ele nasceria cobrindo só o que vier daqui pra frente, e as
	// rows antigas ficariam invisíveis à query indexada. Reconstruir o
	// índice (backfill) não existe no M1.
	if m.RowCount > 0 {
		var semBackfill []types.FieldID
		for _, field := range campos {
			if !noManifest[field] {
				semBackfill = append(semBackfill, field)
			}
		}
		if len(semBackfill) > 0 {
			return fmt.Errorf(
				"%w: campo(s) %v declarado(s) com índice bitmap num dataset que já tem %d rows; "+
					"o índice cobriria apenas os records futuros e a query indexada omitiria as rows antigas. "+
					"Reconstrução de índice não existe no M1 — crie o dataset já com o campo indexado",
				ErrDatasetCorrupt, semBackfill, m.RowCount)
		}
	}
	return nil
}

// fsyncDir durabiliza as entradas de um diretório.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: fsync do diretório %s: %w", dir, err)
	}
	return nil
}

// FieldDict expõe o dicionário de campos do dataset.
func (s *Store) FieldDict() *fielddict.Dict { return s.dict }

// HasBitmap informa se o campo tem índice bitmap.
//
// É o capability check do planner: sem ele o plano escolhe o caminho
// indexado e a query falha em RUNTIME, em vez de degradar para full scan
// como o design promete. O conjunto é fixado no Open, então a resposta é
// estável durante toda a vida do Store.
func (s *Store) HasBitmap(field types.FieldID) bool {
	_, ok := s.bitmaps[field]
	return ok
}

// BitmapFields devolve, em ordem crescente, os campos com índice bitmap.
func (s *Store) BitmapFields() []types.FieldID {
	out := make([]types.FieldID, 0, len(s.bitmaps))
	for field := range s.bitmaps {
		out = append(out, field)
	}
	slices.Sort(out)
	return out
}

// lookup resolve a key pela primary, lendo o record corrente em buf.
// exists = key presente; live = sem flag de tombstone na location.
//
// O callback de validação roda sob o RLock do índice primary: ele NÃO pode
// reentrar no índice (deadlock com um rehash enfileirado) — mantém-se
// mínimo: locations.Get + pread + comparação de key (spec §36).
func (s *Store) lookup(key, buf []byte) (rec types.Record, out []byte, exists, live bool, err error) {
	out = buf
	validate := func(rid types.RowID) (bool, error) {
		packed, ok := s.locs.Get(rid)
		if !ok {
			// Slot da primary sem location (janela de crash): não é a row.
			return false, nil
		}
		seg, off, locFlags := rowindex.Unpack(packed)
		var r types.Record
		var rerr error
		r, out, rerr = s.segR.ReadRecord(seg, off, out)
		if rerr != nil {
			return false, rerr
		}
		if !bytes.Equal(r.Key, key) {
			return false, nil // colisão de hash: continua o probe
		}
		rec = r
		live = locFlags&types.FlagTombstone == 0
		return true, nil
	}
	_, exists, err = s.prim.Find(key, validate)
	if err != nil {
		return types.Record{}, out, false, false, err
	}
	return rec, out, exists, live, nil
}

// GetByKey resolve a key pela primary (validação de colisão inclusa) e
// devolve o record corrente sobre buf (possivelmente crescido).
// ok=false para key ausente ou tombstoned.
func (s *Store) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	rec, out, exists, live, err := s.lookup(key, buf)
	if err != nil || !exists || !live {
		return types.Record{}, out, false, err
	}
	return rec, out, true, nil
}

// GetByRowID devolve o record corrente da row. ok=false para row sem
// location ou tombstoned (checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	packed, ok := s.locs.Get(id)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	seg, off, locFlags := rowindex.Unpack(packed)
	if locFlags&types.FlagTombstone != 0 {
		return types.Record{}, buf, false, nil
	}
	rec, out, err := s.segR.ReadRecord(seg, off, buf)
	if err != nil {
		return types.Record{}, out, false, err
	}
	if rec.RowID != id {
		return types.Record{}, out, false, fmt.Errorf("%w: location da row %d aponta para record da row %d",
			encoding.ErrCorrupt, id, rec.RowID)
	}
	return rec, out, true, nil
}

// Put aplica um estado materializado na ordem do design §10.1 e publica a
// location atomicamente (commit point, spec §45/§46). Exclusivo do writer.
//
// Contrato do prev: quando fornecido, prev DEVE ser o record corrente DESTA
// key (obtido de GetByKey) — o Put confia em prev.RowID e prev.Payload sem
// refazer o Find. prev de outra key é rejeitado com ErrInvalidArgument
// (a violação silenciosa redirecionaria o estado para a row errada e
// apagaria a key legítima do dataset).
//
// Erros: antes do Append nada foi gravado e o Store segue utilizável.
// Depois do Append não há rollback parcial possível — o writer entra em
// modo FALHO (sticky): Put/Sync/Checkpoint devolvem o mesmo erro até
// Close + reabertura com recovery (S12). Falha do Sync com SyncEveryPut
// devolve (rowID, created, err): é erro de DURABILIDADE, não de
// visibilidade — o estado está commitado e visível.
func (s *Store) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return 0, false, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	if len(key) == 0 {
		return 0, false, fmt.Errorf("%w: key vazia", ErrInvalidArgument)
	}
	if prev != nil && !bytes.Equal(prev.Key, key) {
		return 0, false, fmt.Errorf("%w: prev pertence à key %q, não a %q", ErrInvalidArgument, prev.Key, key)
	}
	// Validação do formato e do dicionário ANTES de qualquer custo
	// irreversível. Reservar um rowId é irreversível por construção (ids
	// nunca são reutilizados — bijeção rowId↔key, §14 risco 9), então
	// validar depois queimaria um id por evento inválido: com fonte ruidosa,
	// 1M de eventos rejeitados inflariam locations.idx e sequences.idx em
	// 8 MB cada e encheriam o intervalo do ScanLive de buracos.
	if _, err := encoding.ValidateRecord(key, payload); err != nil {
		return 0, false, err
	}
	if err := s.validatePayloadAgainstDataset(payload); err != nil {
		return 0, false, err
	}
	var (
		rowID       types.RowID
		created     bool
		prevPayload []byte
	)
	if prev != nil {
		rowID = prev.RowID
		prevPayload = prev.Payload
	} else {
		found, out, exists, _, err := s.lookup(key, s.prevBuf[:0])
		s.prevBuf = out
		if err != nil {
			return 0, false, err
		}
		if exists {
			rowID = found.RowID
			prevPayload = found.Payload
		} else {
			// A partir daqui o id está QUEIMADO mesmo se o Put falhar: o
			// recovery reconcilia por max, mas nunca o reutiliza (reuso
			// quebraria a bijeção rowId↔key no log — ABA).
			rowID = types.RowID(s.nextRowID.Add(1) - 1)
			created = true
		}
	}

	// 1. encode. O BuildRecord revalida por conta própria e deriva o
	// fieldCount por construção (§I4: nunca aceito do caller) — a segunda
	// varredura do payload é deliberada, o custo é medido na S13.
	rec, err := encoding.BuildRecord(s.buildBuf[:0], rowID, seq, flags, key, payload)
	s.buildBuf = rec
	if err != nil {
		return 0, false, err
	}
	// 2. append (rotação interna ao segment). TODO erro do Append é sticky:
	// a rotação embute fdatasync do segmento selado e fsync de diretório
	// (erro de durabilidade ali não pode escapar do modo falho — fsyncgate),
	// e um WriteAt parcial também não tem rollback confiável.
	seg, off, err := s.segW.Append(rec)
	if err != nil {
		return 0, false, s.failWriter(err)
	}
	s.curSegID.Store(seg)
	// 3-8. aplicação do estado aos índices, até o commit da location.
	if err := s.applyRecord(recordApply{
		rowID:       rowID,
		key:         key,
		seq:         seq,
		flags:       flags,
		payload:     payload,
		prevPayload: prevPayload,
		hadPrevious: !created,
		countsAsNew: created,
		seg:         seg,
		off:         off,
		recLen:      len(rec),
	}); err != nil {
		return 0, false, s.failWriter(err)
	}
	if s.opts.SyncEveryPut {
		if err := s.syncLocked(); err != nil {
			return rowID, created, err // syncLocked já marcou o modo falho
		}
	}
	return rowID, created, nil
}

// applyMode distingue os dois callers de applyRecord.
type applyMode uint8

const (
	// applyWrite é o write path: a location vai para o overlay (visível a
	// leitores concorrentes) e o caller marca o writer como falho no erro.
	applyWrite applyMode = iota
	// applyReplay é o replay do tail na recovery: sem leitores, a location
	// vai direto ao mmap e a primary é reconciliada por insert-if-absent.
	applyReplay
)

// recordApply descreve um record JÁ durável no segmento cujo estado precisa
// ser aplicado aos índices.
type recordApply struct {
	rowID       types.RowID
	key         []byte
	seq         int64
	flags       uint8
	payload     []byte
	prevPayload []byte // estado anterior desta row (nil quando não havia)
	// hadPrevious: havia location para esta row — governa o decremento de
	// liveness do segmento do record anterior.
	hadPrevious bool
	// countsAsNew: esta row conta como CRIADA em rowCount. No write path é
	// "key ausente da primary"; no replay é "rowId nascido depois do ponto S"
	// — os dois não coincidem, porque um checkpoint interrompido pode ter
	// deixado a location de uma row nova no mmap sem que o manifest a
	// contabilizasse.
	countsAsNew bool
	seg         uint32
	off         uint32
	recLen      int
	mode        applyMode
}

// applyRecord executa os passos 3-8 do §10.1 (offset accounting, primary,
// bitmaps, sequence, commit da location, clears) para um record já gravado
// no segmento. É o ÚNICO code path de aplicação de estado: o Put o chama
// para o record recém-appendado e o replay da recovery para cada record do
// tail (§10.3 passo 5) — o que garante que restart não diverge da escrita.
func (s *Store) applyRecord(a recordApply) error {
	// 3. offset accounting: packing + tally do segmento (stats do manifest).
	packed := rowindex.Pack(a.seg, a.off, a.flags&types.FlagTombstone)
	tally := s.segTally[a.seg]
	if tally == nil {
		tally = &segStats{}
		s.segTally[a.seg] = tally
	}
	tally.totalRecords++
	tally.liveRecords++
	tally.liveBytes += uint64(a.recLen)
	// Update torna o record ANTERIOR garbage no instante do publish:
	// decrementa a liveness do segmento antigo (lida ANTES do publish
	// sobrescrever a location). Segmentos pré-restart têm o tally semeado
	// do manifest pela recovery.
	if a.hadPrevious {
		if oldPacked, had := s.locs.Get(a.rowID); had {
			oldSeg, _, _ := rowindex.Unpack(oldPacked)
			if t := s.segTally[oldSeg]; t != nil {
				oldLen := uint64(encoding.RecordHeaderSize + len(a.key) + len(a.prevPayload))
				if t.liveRecords > 0 {
					t.liveRecords--
				}
				if t.liveBytes >= oldLen {
					t.liveBytes -= oldLen
				} else {
					t.liveBytes = 0
				}
			}
		}
	}

	// 4. primary insert-if-absent. No write path só a key nova precisa
	// (o lookup já resolveu as demais); no replay roda SEMPRE, porque um
	// checkpoint interrompido pode ter deixado a primary atrás das
	// locations — e o Insert é idempotente.
	if a.countsAsNew || a.mode == applyReplay {
		validate := s.insertValidator(a.key)
		if a.mode == applyReplay {
			validate = s.replayValidator(a.key, a.rowID)
		}
		existing, inserted, err := s.prim.Insert(a.key, a.rowID, validate)
		if err != nil {
			return err
		}
		if !inserted {
			if a.mode == applyWrite {
				return fmt.Errorf("storage: key %q já presente na primary com prev==nil e lookup ausente", a.key)
			}
			if existing != a.rowID {
				return fmt.Errorf("%w: key %q mapeada para a row %d na primary, mas o record do tail declara a row %d",
					encoding.ErrCorrupt, a.key, existing, a.rowID)
			}
		}
	}
	// 5. fase de SET dos bitmaps ANTES do commit: a janela entre índice e
	// location é sempre um SUPERSET de candidatos (falso positivo,
	// filtrado pela revalidação §44) — nunca falso negativo.
	if len(s.bitmaps) > 0 {
		if err := s.applyBitmapSets(a.rowID, a.prevPayload, a.payload); err != nil {
			return err
		}
	}
	// 6. sequence.
	if err := s.seqs.Set(a.rowID, a.seq); err != nil {
		return err
	}
	// 7. COMMIT POINT: publica a location.
	if a.mode == applyReplay {
		// Recovery: grava direto no mmap, sem overlay. A invariante §6.1
		// (página de índice nunca referencia record não durável) continua
		// satisfeita porque os records replayed já estão no disco, e não há
		// leitores ativos antes do Open retornar.
		if err := s.locs.SetDirect(a.rowID, packed); err != nil {
			return err
		}
	} else {
		s.locs.Publish(a.rowID, packed)
	}
	if a.countsAsNew {
		s.rowCount.Add(1)
	}
	// 8. fase de CLEAR dos bitmaps DEPOIS do commit: erro aqui deixa só
	// falso positivo (tolerável; contabilizado p/ métricas da S13).
	if len(s.bitmaps) > 0 {
		s.applyBitmapClears(a.rowID, a.prevPayload, a.payload)
	}
	return nil
}

// failWriter registra o primeiro erro pós-Append e o devolve embrulhado em
// ErrWriterFailed. Sem rollback parcial, o único caminho íntegro é reabrir
// com recovery (replay do tail).
//
// O embrulho vale já na chamada que MATA o writer, e não só nas seguintes:
// é justamente essa chamada que deixa o phantom commit do §14.12, e um
// consumidor sob log-and-skip que recebesse o erro cru concluiria duas
// coisas falsas — que o writer segue vivo e que nada foi gravado.
func (s *Store) failWriter(err error) error {
	if s.stickyErr == nil {
		s.stickyErr = err
	}
	return fmt.Errorf("%w: %w", ErrWriterFailed, err)
}

// validatePayloadAgainstDataset confere o payload contra o estado deste
// dataset ANTES do append, numa única varredura:
//
//   - todo fieldID existe no dicionário (§15A item 3) — um id desconhecido
//     gravado ficaria durável sob CRC e só explodiria no encoder do query;
//   - todo campo COM ÍNDICE tem valor indexável. Este segundo é o que
//     impede um dano fatal: se a violação só aparecesse na fase de Set, o
//     record já estaria durável, o replay do tail tentaria reindexá-lo a
//     cada abertura e falharia sempre — um único evento externo tornaria o
//     dataset INABRÍVEL para sempre, com a configuração default.
func (s *Store) validatePayloadAgainstDataset(payload []byte) error {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return r.Err()
		}
		if _, known := s.dict.Name(id); !known {
			return fmt.Errorf("%w: fieldID %d não existe no dicionário", ErrInvalidArgument, id)
		}
		if _, indexado := s.bitmaps[id]; indexado {
			if err := bitmap.ValidateValue(v); err != nil {
				nome, _ := s.dict.Name(id)
				return fmt.Errorf("%w: campo %d (%q) tem índice bitmap: %w", ErrInvalidArgument, id, nome, err)
			}
		}
	}
}

// insertValidator devolve o ValidateFunc de colisão do Insert. Roda sob o
// RLock da primary — não reentra no índice (ver lookup). Usa um buffer
// próprio para não invalidar o prevPayload em uso pelo Put.
func (s *Store) insertValidator(key []byte) primary.ValidateFunc {
	return func(rid types.RowID) (bool, error) {
		packed, ok := s.locs.Get(rid)
		if !ok {
			return false, nil
		}
		seg, off, _ := rowindex.Unpack(packed)
		var r types.Record
		var err error
		r, s.valBuf, err = s.segR.ReadRecord(seg, off, s.valBuf)
		if err != nil {
			return false, err
		}
		return bytes.Equal(r.Key, key), nil
	}
}

// replayValidator reconhece a própria row antes de cair na validação por
// leitura. Durante o replay a location da row pode ainda não existir (um
// checkpoint interrompido deixa a primary à frente das locations): o
// insertValidator responderia "não é a row", o Insert seguiria o probe e
// gravaria a mesma key num segundo slot.
func (s *Store) replayValidator(key []byte, rowID types.RowID) primary.ValidateFunc {
	byRead := s.insertValidator(key)
	return func(rid types.RowID) (bool, error) {
		if rid == rowID {
			return true, nil
		}
		return byRead(rid)
	}
}

// valueEqual compara duas Values de mesmo campo.
func valueEqual(a, b types.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		return a.I64 == b.I64
	default:
		return bytes.Equal(a.Bytes, b.Bytes)
	}
}

// applyBitmapSets liga as postings novas (campo que entra ou muda) — roda
// ANTES do publish. Campo com o mesmo valor não toca o bitmap.
func (s *Store) applyBitmapSets(rowID types.RowID, prevPayload, newPayload []byte) error {
	var pr, nr encoding.FieldReader
	pr.Reset(prevPayload)
	nr.Reset(newPayload)
	pid, pv, pok := pr.Next()
	nid, nv, nok := nr.Next()
	for pok || nok {
		switch {
		case pok && (!nok || pid < nid):
			// Campo que sai: o Clear correspondente roda pós-commit.
			pid, pv, pok = pr.Next()
		case nok && (!pok || nid < pid):
			if ix := s.bitmaps[nid]; ix != nil {
				if err := s.setBit(ix, nid, nv, rowID); err != nil {
					return err
				}
			}
			nid, nv, nok = nr.Next()
		default: // pid == nid
			if ix := s.bitmaps[pid]; ix != nil && !valueEqual(pv, nv) {
				if err := s.setBit(ix, pid, nv, rowID); err != nil {
					return err
				}
			}
			pid, pv, pok = pr.Next()
			nid, nv, nok = nr.Next()
		}
	}
	if err := pr.Err(); err != nil {
		return err
	}
	return nr.Err()
}

// setBit executa o Set com o seam de injeção de erro dos testes (phantom
// commit precisa de falha pós-Append com payload replayável).
func (s *Store) setBit(ix *bitmap.Index, field types.FieldID, v types.Value, rowID types.RowID) error {
	if s.setErrHook != nil {
		if err := s.setErrHook(field); err != nil {
			return err
		}
	}
	return ix.Set(v, rowID)
}

// applyBitmapClears desliga as postings antigas (campo que sai ou muda) —
// roda DEPOIS do publish. Erro aqui deixa apenas falso positivo (a
// revalidação §44 filtra); é contabilizado em clearFailures.
func (s *Store) applyBitmapClears(rowID types.RowID, prevPayload, newPayload []byte) {
	var pr, nr encoding.FieldReader
	pr.Reset(prevPayload)
	nr.Reset(newPayload)
	pid, pv, pok := pr.Next()
	nid, nv, nok := nr.Next()
	for pok || nok {
		switch {
		case pok && (!nok || pid < nid):
			if ix := s.bitmaps[pid]; ix != nil {
				if err := s.clearBit(ix, pid, pv, rowID); err != nil {
					s.clearFailures.Add(1)
				}
			}
			pid, pv, pok = pr.Next()
		case nok && (!pok || nid < pid):
			nid, nv, nok = nr.Next()
		default: // pid == nid
			if ix := s.bitmaps[pid]; ix != nil && !valueEqual(pv, nv) {
				if err := s.clearBit(ix, pid, pv, rowID); err != nil {
					s.clearFailures.Add(1)
				}
			}
			pid, pv, pok = pr.Next()
			nid, nv, nok = nr.Next()
		}
	}
	if pr.Err() != nil || nr.Err() != nil {
		s.clearFailures.Add(1)
	}
}

// clearBit executa o Clear com o seam de injeção de erro dos testes do
// caminho tolerado (nil em produção).
func (s *Store) clearBit(ix *bitmap.Index, field types.FieldID, v types.Value, rowID types.RowID) error {
	if s.clearErrHook != nil {
		if err := s.clearErrHook(field); err != nil {
			return err
		}
	}
	return ix.Clear(v, rowID)
}

// liveIterator percorre lazy as rows vivas até o limite fixado na criação.
type liveIterator struct {
	s     *Store
	next  types.RowID
	limit types.RowID
	cur   types.RowID
}

func (it *liveIterator) Next() bool {
	for it.next < it.limit {
		id := it.next
		it.next++
		packed, ok := it.s.locs.Get(id)
		if !ok {
			continue
		}
		if _, _, locFlags := rowindex.Unpack(packed); locFlags&types.FlagTombstone != 0 {
			continue
		}
		it.cur = id
		return true
	}
	return false
}

func (it *liveIterator) RowID() types.RowID { return it.cur }
func (it *liveIterator) Err() error         { return nil }
func (it *liveIterator) Close() error       { return nil }

// emptyIterator é um types.RowIDIterator sem elementos.
type emptyIterator struct{}

func (emptyIterator) Next() bool         { return false }
func (emptyIterator) RowID() types.RowID { return 0 }
func (emptyIterator) Err() error         { return nil }
func (emptyIterator) Close() error       { return nil }

// ScanLive devolve um iterador lazy das rows vivas (rowIds crescentes,
// location válida — overlay incluso — e sem tombstone). Rows criadas após a
// chamada não aparecem (snapshot do limite superior).
func (s *Store) ScanLive() types.RowIDIterator {
	return &liveIterator{s: s, limit: types.RowID(s.nextRowID.Load())}
}

// LookupBitmap consulta o índice bitmap do campo; valor nunca visto devolve
// iterador vazio; campo não indexado é erro. O resultado é um SUPERSET de
// candidatos: rows tombstoned/sem location podem aparecer — valide liveness
// e predicate via GetByRowID (spec §44).
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	ix, ok := s.bitmaps[field]
	if !ok {
		return nil, fmt.Errorf("%w: campo %d não possui índice bitmap", ErrInvalidArgument, field)
	}
	it, found, err := ix.Lookup(v)
	if err != nil {
		return nil, err
	}
	if !found {
		return emptyIterator{}, nil
	}
	return it, nil
}

// Sequence devolve a última sequence aplicada à row (ok=false para row sem
// location publicada — a sequence só tem significado com estado visível).
// Pode estar momentaneamente À FRENTE do record visível: ela é gravada no
// passo 6 do Put, antes do publish (passo 7).
func (s *Store) Sequence(id types.RowID) (int64, bool) {
	if _, ok := s.locs.Get(id); !ok {
		return 0, false
	}
	return s.seqs.Get(id), true
}

// Sync força fdatasync do segmento corrente (loop de durability batch).
// Serializado com Put/Checkpoint pelo mutex do writer. Falha marca o writer
// como FALHO: durabilidade comprometida exige reabertura com recovery.
func (s *Store) Sync() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.syncLocked()
}

// syncLocked exige wmu já adquirido.
func (s *Store) syncLocked() error {
	if s.stickyErr != nil {
		return fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	if err := s.segW.Sync(); err != nil {
		return s.failWriter(err)
	}
	return nil
}

// Stats devolve o snapshot de estado do Store. Seguro para uso concorrente
// com o writer (o segmento corrente vem de um espelho atômico, não do
// segment.Writer). RowCount conta rows CRIADAS — não decrementa em
// tombstone; a contagem de vivas é semântica da S14.
func (s *Store) Stats() Stats {
	segID := s.curSegID.Load()
	var lf float64
	if slots := s.prim.SlotCount(); slots > 0 {
		lf = float64(s.prim.Used()) / float64(slots)
	}
	return Stats{
		RowCount:          s.rowCount.Load(),
		SegmentsTotal:     segID, // IDs monotônicos desde 1, sem remoção no M1
		CurrentSegmentID:  segID,
		PrimaryLoadFactor: lf,
		OverlayLen:        s.locs.OverlayLen(),
	}
}

// Close fecha o dataset SEM checkpoint automático: é responsabilidade do
// caller chamar Checkpoint() antes, se quiser durabilizar o delta. Close
// não drena o overlay nem faz msync/fsync de nada — o que não foi
// checkpointado volta apenas pelo tail scan da recovery (S12), a partir do
// último manifest.
func (s *Store) Close() error {
	var errs []error
	for _, ix := range s.bitmaps {
		if ix != nil {
			errs = append(errs, ix.Close())
		}
	}
	if s.seqs != nil {
		errs = append(errs, s.seqs.Close())
	}
	if s.locs != nil {
		errs = append(errs, s.locs.Close())
	}
	if s.prim != nil {
		errs = append(errs, s.prim.Close())
	}
	if s.segR != nil {
		errs = append(errs, s.segR.Close())
	}
	if s.segW != nil {
		errs = append(errs, s.segW.Close())
	}
	if s.dict != nil {
		errs = append(errs, s.dict.Close())
	}
	// O lock sai por ÚLTIMO: enquanto qualquer arquivo do dataset estiver
	// aberto aqui, outro processo não pode assumi-lo.
	errs = append(errs, s.lock.release())
	return errors.Join(errs...)
}
