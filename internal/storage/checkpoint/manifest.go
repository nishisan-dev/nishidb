// Package checkpoint implementa o manifest do NishiDB (design §9): o
// snapshot consistente que amarra segmentos, índices e dicionários, gravado
// atomicamente ao final de cada checkpoint (§10.2).
package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// ErrInManifest indica dataset sem manifest (cold start).
var ErrInManifest = errors.New("checkpoint: manifest ausente")

// ErrCorruptManifest indica manifest ilegível (CRC, magic, truncado).
var ErrCorruptManifest = errors.New("checkpoint: manifest corrompido")

// Layout do manifest.bin (little-endian):
//
//	0   4  magic "NDBM"
//	4   1  version = 1 (sem cauda), 2 (com cauda TLV) ou 3 (índice por segmento)
//	5   3  reserved
//	8   8  epoch (u64)
//	16  8  createdAtUnixMs (i64)
//	24  8  nextRowID (u64)
//	32  8  rowCount (u64)
//	40  4  currentSegment (u32)
//	44  8  durableOffset (u64)
//	52  4  segmentCount (u32)
//	    por segmento: id u32 | sizeBytes i64 | totalRecords u64 |
//	                  liveRecords u64 | liveBytes u64  (36 bytes)
//	    primarySlotCount u64 | primaryUsed u64 | hashKind u8 (RESERVADO — ver abaixo)
//	    fieldDictValidBytes i64
//	    bitmapCount u16 (RESERVADO) | por bitmap: fieldID u16 | valuesDictValidBytes i64
//	    kafkaCount u32 (=0, reservado p/ M3)
//	    [cauda TLV, a partir da v2 — ver abaixo]
//	    crc32c u32 (sobre tudo acima)
//
// DOIS CAMPOS RESERVADOS, e o porquê fica escrito aqui porque campo reservado
// que ninguém sabe explicar é como nasce a próxima leitura errada:
//
//   - `hashKind` descrevia o algoritmo de hash do índice primary. Ele é gravado
//     como ZERO e ignorado na leitura desde a v3. A informação não se perdeu:
//     o header do próprio primary.idx a carrega e RECUSA um kind desconhecido
//     (internal/index/primary), então a cópia daqui era advisory e o único uso
//     dela era um log de divergência entre duas cópias do mesmo fato.
//   - `bitmapCount` descrevia o conjunto de índices bitmap GLOBAIS, que
//     deixaram de existir. Ele é gravado como ZERO desde a v3. Na LEITURA a
//     contagem ainda é consumida e as entradas ainda são PULADAS, e isso não é
//     zelo: é o que faz um manifest v1 ou v2 escrito por um binário anterior
//     continuar parseando, porque o que vem depois dele no corpo (o kafkaCount)
//     está a `bitmapCount * bitmapInfoSize` bytes de distância.
//
// Os dois campos continuam ocupando os mesmos bytes de propósito: tirá-los
// obrigaria a reescrever o parser do corpo e a condenar todo manifest já
// gravado, em troca de onze bytes por checkpoint.
//
// A CAUDA TLV é o ponto de extensão do formato, e existe para que uma seção
// nova não obrigue a reescrever o corpo nem a condenar todo manifest já gravado. Cada registro é
// `tag u16 | len u32 | len bytes`, e a tag carrega a política de
// compatibilidade no bit mais alto:
//
//	tag <  0x8000  IGNORÁVEL: um binário que não a conhece pula os bytes e
//	               segue. Serve para informação puramente descritiva.
//	tag >= 0x8000  CRÍTICA: um binário que não a conhece RECUSA o manifest.
//	               Serve para o que muda o significado do dataset — abrir o
//	               dataset ignorando uma delas produz índice incompleto,
//	               tabela lida com a engine errada ou linha que nunca vence.
//
// O byte de versão acompanha, e a partir da v3 ele é o que decide o rollback: o
// manifest deste binário é SEMPRE v3, e um binário anterior o recusa com
// "version desconhecida". É a resposta certa, e ela precisa vir da VERSÃO e não
// de uma tag: um binário v2 que abrisse este dataset manteria os índices globais
// que ele conhece, ignoraria os de segmento — que ele não sabe que existem — e
// os de segmento reapareceriam INCOMPLETOS na volta.
const (
	// manifestV1 é o corpo sem cauda; manifestV2 é o corpo IDÊNTICO mais a
	// cauda TLV. Os dois ainda são LIDOS — é o que faz um dataset escrito por um
	// binário anterior abrir sem ser recriado, que é a promessa de migração
	// deste ciclo — mas nenhum dos dois é escrito.
	manifestV1 = 1
	manifestV2 = 2
	// manifestV3 é o corpo com hashKind e bitmapCount zerados mais a cauda com
	// a seção do índice por segmento. É a versão que este binário grava, sempre,
	// e é ela que bloqueia o binário anterior.
	manifestV3 = 3

	// tagCritical é o bit que separa as duas políticas acima.
	tagCritical = 0x8000
	// tagHashes ERA a seção do conjunto de índices hash globais. Ela não é mais
	// emitida, e continua nomeada aqui porque a LEITURA precisa reconhecê-la
	// para descartá-la: sem este nome ela cairia no ramo de seção crítica
	// desconhecida e o dataset seria recusado com "escrito por um binário mais
	// novo" — diagnóstico exatamente invertido, sobre um dataset escrito por um
	// binário mais VELHO, e a recusa mandaria o operador procurar o problema no
	// lugar errado.
	tagHashes = 0x8001
	// tagSchedule é a seção da agenda de vencimentos. Ela é crítica pelo mesmo
	// motivo das outras duas, com um modo de falha próprio: a agenda é
	// DERIVADA da duração de fundo, do fanout e do número de níveis, e um
	// binário que a ignorasse continuaria adotando uma árvore construída a
	// partir de parâmetros que ele já não honra. O sintoma não seria índice
	// incompleto nem resposta errada — seria linha que NUNCA vence, com o CRC
	// batendo, sem erro, sem log e sem métrica. É a única das três seções em
	// que a falha é puramente silenciosa.
	tagSchedule = 0x8003

	// tagHistory é a seção do MODO do dataset e da marca-d'água de
	// retenção. Ela é crítica pelo pior modo de falha que este formato tem:
	// um binário que a ignorasse abriria um log append-only de eventos como
	// se fosse estado corrente materializado, e leria uma tabela em que cada
	// entidade aparece dezenas de vezes como se cada linha fosse uma entidade.
	// Não é índice incompleto — é a resposta errada, com o CRC batendo.
	tagHistory = 0x8002

	// tagPerSegmentIndex declara o FORMATO do índice que mora dentro de cada
	// segmento. Ela é crítica pela mesma razão das outras.
	//
	// Ela NÃO é o que bloqueia o binário anterior — a versão já faz isso, e
	// depender de uma tag para bloquear seria depender de o outro lado ler algo
	// que ele não conhece. Ela existe para que a PRÓXIMA mudança de formato do
	// índice tenha onde ser declarada sem outro bump de versão do manifest, que
	// é exatamente o que a cauda TLV foi criada para evitar.
	tagPerSegmentIndex = 0x8004
	// currentIndexFormat é o valor gravado hoje. Um binário que ler um formato
	// que não conhece recusa o dataset, e a recusa é a resposta certa: ele não
	// saberia manter — nem ler — o índice que o dataset tem.
	currentIndexFormat = 1

	// tagCounters é a seção dos contadores mantidos na escrita: por
	// contador, o nome, as colunas do groupBy e o texto do where — os
	// parâmetros que DECIDEM o conteúdo do vetor de grupos. É IGNORÁVEL, e a
	// política é deliberada: a seção existe para o binário novo decidir entre
	// adotar e reconstruir, e "seção ausente ⇒ reconstruir" já é a resposta
	// certa. Torná-la crítica só bloquearia o rollback (nishidb-update.sh)
	// sem proteger nada — o binário anterior pula a seção, grava o manifest
	// sem ela, e o binário novo reconstrói ao voltar. É a primeira tag
	// ignorável do formato, e por isso a menor: sai antes de todas as críticas
	// na cauda em ordem crescente.
	tagCounters = 0x0001

	// tlvHeaderSize é `tag u16 | len u32`.
	tlvHeaderSize = 6
	// hashInfoSize é `fieldID u16 | slotCount u64`, o tamanho de uma entrada da
	// seção 0x8001. Só a LEITURA ainda o usa, para conferir o comprimento da
	// seção legada antes de descartá-la.
	hashInfoSize = 10

	// historyInfoSize é `mode u8 | rowIDBase u64`.
	historyInfoSize = 9

	// scheduleInfoSize é `fallbackDurationMs u64 | blockShift u8 | levels u8`.
	scheduleInfoSize = 10

	// indexInfoSize é `indexFormat u8`.
	indexInfoSize = 1

	// bitmapInfoSize é `fieldID u16 | valuesDictValidBytes i64`, a entrada do
	// array de bitmaps do CORPO. Nada a escreve mais — bitmapCount é reservado e
	// vale zero desde a v3 —, e ela existe porque a leitura de um manifest v1 ou
	// v2 ainda precisa PULAR essas entradas para chegar ao kafkaCount. Constante
	// nomeada, e não um 10 solto no salto: o valor deriva do layout do corpo
	// declarado no topo deste arquivo, e um literal deixaria a leitura desalinhar
	// em silêncio no dia em que a entrada mudasse de tamanho.
	bitmapInfoSize = 10
)

var (
	manifestMagic = [4]byte{'N', 'D', 'B', 'M'}
	castagnoli    = crc32.MakeTable(crc32.Castagnoli)
)

// SegmentInfo descreve um segmento no manifest, com as estatísticas de
// liveness do design §9 rastreadas pelo writer (updates decrementam o
// segmento do record anterior).
//
// Unidades: SizeBytes é o tamanho do ARQUIVO (inclui o header de 64 bytes
// do segmento); TotalRecords conta todos os records já gravados no segmento,
// vivos ou não.
//
// LiveRecords e LiveBytes NÃO são a contagem exata dos records vivos: são um
// LIMITE SUPERIOR dela. Podem ficar ACIMA do real, nunca abaixo. Quem os
// consumir — a compaction do M4 é o consumidor previsto, no minimumLiveRatio
// do §14.16 — precisa lê-los como "no máximo isto está vivo": a assimetria é
// deliberada e é de segurança, porque superestimar liveness apenas ADIA uma
// compaction, enquanto subestimá-la faria a compaction descartar dado vivo.
//
// De onde vem o excesso: do replay do tail no recovery. Quando a row já
// existia no ponto do último checkpoint e o tail traz um update cuja location
// durável já aponta para o record NOVO, o replay não tem como saber qual era
// o estado anterior e não decrementa o record que o update tornou garbage
// (o porquê está em replayRecord, internal/storage/store_recovery.go). Esse
// record órfão fica contado como vivo de forma PERMANENTE: o decremento do
// write path identifica o garbage pela location da row, que já nomeia o
// record do tail, então cada update seguinte desconta o anterior a ELE e
// nunca o órfão. Só uma varredura do segmento — a compaction — reconstrói o
// tally.
//
// O resíduo está medido e travado em TestResiduoDeLivenessDoReplayNaoConverge
// (internal/storage/store_liveness_residue_test.go), que também prova que ele
// não converge com checkpoints posteriores. Débito 38 em docs/debitos.md.
type SegmentInfo struct {
	ID           uint32
	SizeBytes    int64
	TotalRecords uint64
	LiveRecords  uint64
	LiveBytes    uint64
}

// ScheduleInfo descreve a agenda de vencimentos gravada no dataset.
//
// Os três campos são os que DECIDEM o conteúdo da árvore, e é por isso que
// eles moram no manifest em vez de serem inferidos dos arquivos: divergir em
// qualquer um deles obriga a reconstruir.
//
// FallbackDurationMs é o mais importante dos três, e o menos óbvio. A linha sem
// prazo próprio herda a duração de fundo, então o vencimento agendado dela é
// função dela — mudar a duração no YAML move, em silêncio, o vencimento de
// toda linha com prazo desconhecido. Hoje esse efeito é invisível; com a
// agenda ele vira um rebuild explícito, logado e com custo medido.
//
// BlockShift e Levels são a geometria: uma árvore lida com o fanout errado
// endereça o bloco errado em cada nível.
type ScheduleInfo struct {
	FallbackDurationMs uint64
	BlockShift         uint8
	Levels             uint8
}

// CounterInfo descreve um contador mantido na escrita gravado no dataset.
//
// Os três campos são os que decidem o CONTEÚDO do vetor de grupos: outras
// colunas ou outro where dão outro contador, e a abertura que encontrar
// divergência reconstrói em vez de adotar. O teto (maxGroups) NÃO viaja:
// mudá-lo não muda o que já foi contado — só é recusado quando menor que os
// grupos em uso.
type CounterInfo struct {
	Name string
	// Columns são os FieldIDs do groupBy, na ordem declarada.
	Columns []uint16
	// Where é o texto normalizado da condição; vazio é "sem condição".
	Where string
}

// Manifest é o estado amarrado por um checkpoint.
type Manifest struct {
	Epoch               uint64
	CreatedAtUnixMs     int64
	NextRowID           uint64
	RowCount            uint64
	CurrentSegment      uint32
	DurableOffset       uint64
	Segments            []SegmentInfo
	PrimarySlotCount    uint64
	PrimaryUsed         uint64
	FieldDictValidBytes int64

	// Schedule é a seção 0x8003, e é nil no dataset sem expiração declarada.
	Schedule *ScheduleInfo

	// Counters é a seção 0x0001; vazia (ou nil) no dataset sem contador
	// declarado, e nesse caso a seção não é emitida.
	Counters []CounterInfo

	// StoreMode é a engine do dataset: 0 materializado, 1 histórico. O tipo é
	// uint8 cru e não um enum do storage porque este pacote descreve um
	// FORMATO — importar o vocabulário de quem o consome inverteria a
	// dependência para ganhar um nome.
	StoreMode uint8
	// RowIDBase é a marca-d'água de retenção do dataset histórico: rowId
	// abaixo dela foi purgado e não é mais visível. Zero no dataset
	// materializado, e é isso que o mantém na v1.
	RowIDBase uint64
}

// hasHistory diz se a seção 0x8002 precisa ser emitida.
func (m *Manifest) hasHistory() bool { return m.StoreMode != 0 || m.RowIDBase != 0 }

// TLVTailSize devolve quantos bytes a cauda ocupa neste manifest. Ela
// nunca é vazia: a seção do índice por segmento é emitida SEMPRE.
//
// É exportado porque é metade da fórmula de reserva do encode, e a outra
// metade vive no teste que trava a reserva. Deixar a conta duplicada foi
// exatamente o defeito que deixou a capacidade em 12 quando o layout já
// gravava 36 por segmento.
func TLVTailSize(m *Manifest) int {
	n := tlvHeaderSize + indexInfoSize
	if m.hasHistory() {
		n += tlvHeaderSize + historyInfoSize
	}
	if m.Schedule != nil {
		n += tlvHeaderSize + scheduleInfoSize
	}
	if len(m.Counters) > 0 {
		n += tlvHeaderSize + countersSectionSize(m.Counters)
	}
	return n
}

// countersSectionSize é o payload da seção 0x0001:
// `n u16 | por contador: lenNome u16 | nome | nCols u16 | nCols×u16 | lenWhere u32 | where`.
func countersSectionSize(cs []CounterInfo) int {
	n := 2
	for _, c := range cs {
		n += 2 + len(c.Name) + 2 + 2*len(c.Columns) + 4 + len(c.Where)
	}
	return n
}

// version decide o byte 4, e a resposta é SEMPRE v3.
//
// Era condicional enquanto valia a pena preservar o rollback: um dataset que
// não declarasse índice hash nem histórico saía como v1, byte a byte igual ao
// que o binário anterior gravava, e podia ser reaberto por ele. Isso acabou aqui
// — e de propósito. Um binário anterior que abrisse ESTE dataset manteria os
// índices globais que ele conhece, ignoraria os de segmento e os deixaria
// incompletos na volta; recusá-lo pela versão é o que torna o dano impossível em
// vez de improvável. É a única propriedade deste formato que se paga com uma
// migração de mão única, e ela está registrada na §5.4 da spec do subsistema.
func (m *Manifest) version() uint8 { return manifestV3 }

func (m *Manifest) encode() []byte {
	// 36 por segmento, que é o que o laço abaixo grava desde os stats de
	// liveness da S14. A capacidade estava em 12 e ficou para trás: não
	// corrompia nada, mas fazia o append realocar em todo manifest com
	// segmento — desperdício silencioso no caminho do checkpoint.
	b := make([]byte, 0, 96+len(m.Segments)*36+TLVTailSize(m))
	b = append(b, manifestMagic[:]...)
	b = append(b, m.version(), 0, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, m.Epoch)
	b = binary.LittleEndian.AppendUint64(b, uint64(m.CreatedAtUnixMs))
	b = binary.LittleEndian.AppendUint64(b, m.NextRowID)
	b = binary.LittleEndian.AppendUint64(b, m.RowCount)
	b = binary.LittleEndian.AppendUint32(b, m.CurrentSegment)
	b = binary.LittleEndian.AppendUint64(b, m.DurableOffset)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Segments)))
	for _, s := range m.Segments {
		b = binary.LittleEndian.AppendUint32(b, s.ID)
		b = binary.LittleEndian.AppendUint64(b, uint64(s.SizeBytes))
		b = binary.LittleEndian.AppendUint64(b, s.TotalRecords)
		b = binary.LittleEndian.AppendUint64(b, s.LiveRecords)
		b = binary.LittleEndian.AppendUint64(b, s.LiveBytes)
	}
	b = binary.LittleEndian.AppendUint64(b, m.PrimarySlotCount)
	b = binary.LittleEndian.AppendUint64(b, m.PrimaryUsed)
	b = append(b, 0) // hashKind: reservado (ver o layout)
	b = binary.LittleEndian.AppendUint64(b, uint64(m.FieldDictValidBytes))
	b = binary.LittleEndian.AppendUint16(b, 0) // bitmapCount: reservado (ver o layout)
	b = binary.LittleEndian.AppendUint32(b, 0) // kafkaCount (M3)
	b = m.appendTLVTail(b)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// appendTLVTail grava os registros TLV.
func (m *Manifest) appendTLVTail(b []byte) []byte {
	// Ordem CRESCENTE de tag, e não a ordem em que as seções foram inventadas:
	// o manifest é comparado byte a byte pelos testes de formato, e uma ordem
	// que dependesse do histórico do produto mudaria o arquivo sem que o
	// conteúdo mudasse.
	if len(m.Counters) > 0 {
		b = binary.LittleEndian.AppendUint16(b, tagCounters)
		b = binary.LittleEndian.AppendUint32(b, uint32(countersSectionSize(m.Counters)))
		b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Counters)))
		for _, c := range m.Counters {
			b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Name)))
			b = append(b, c.Name...)
			b = binary.LittleEndian.AppendUint16(b, uint16(len(c.Columns)))
			for _, col := range c.Columns {
				b = binary.LittleEndian.AppendUint16(b, col)
			}
			b = binary.LittleEndian.AppendUint32(b, uint32(len(c.Where)))
			b = append(b, c.Where...)
		}
	}
	if m.hasHistory() {
		b = binary.LittleEndian.AppendUint16(b, tagHistory)
		b = binary.LittleEndian.AppendUint32(b, historyInfoSize)
		b = append(b, m.StoreMode)
		b = binary.LittleEndian.AppendUint64(b, m.RowIDBase)
	}
	if m.Schedule != nil {
		b = binary.LittleEndian.AppendUint16(b, tagSchedule)
		b = binary.LittleEndian.AppendUint32(b, scheduleInfoSize)
		b = binary.LittleEndian.AppendUint64(b, m.Schedule.FallbackDurationMs)
		b = append(b, m.Schedule.BlockShift, m.Schedule.Levels)
	}
	// A seção do índice por segmento é INCONDICIONAL, inclusive no dataset que
	// não declara campo indexado nenhum. Ela descreve o formato que este binário
	// escreveria, e não o que o dataset por acaso tem hoje: emiti-la só quando há
	// índice faria um dataset ganhar a declaração — e a versão que ela acompanha
	// — no dia em que alguém acrescentasse um campo, mudando o formato do
	// arquivo por uma alteração de configuração.
	b = binary.LittleEndian.AppendUint16(b, tagPerSegmentIndex)
	b = binary.LittleEndian.AppendUint32(b, indexInfoSize)
	return append(b, currentIndexFormat)
}

// reader é um cursor de decodificação com detecção de truncamento.
type reader struct {
	b   []byte
	pos int
	bad bool
}

func (r *reader) take(n int) []byte {
	if r.bad || r.pos+n > len(r.b) {
		r.bad = true
		return make([]byte, n)
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v
}

func (r *reader) u16() uint16 { return binary.LittleEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) u8() uint8   { return r.take(1)[0] }

func decode(raw []byte) (*Manifest, error) {
	if len(raw) < 8 || [4]byte(raw[0:4]) != manifestMagic {
		return nil, fmt.Errorf("%w: magic inválida", ErrCorruptManifest)
	}
	// As três versões são lidas, e só a v3 é escrita. Ler as duas antigas é o
	// que faz um dataset gravado por um binário anterior abrir sem ser
	// recriado — a promessa de migração deste ciclo —, e o primeiro checkpoint
	// depois disso já o reescreve em v3.
	if raw[4] != manifestV1 && raw[4] != manifestV2 && raw[4] != manifestV3 {
		return nil, fmt.Errorf("%w: version desconhecida %d", ErrCorruptManifest, raw[4])
	}
	if len(raw) < 12 {
		return nil, fmt.Errorf("%w: truncado", ErrCorruptManifest)
	}
	body, tail := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return nil, fmt.Errorf("%w: CRC32C não confere", ErrCorruptManifest)
	}

	r := &reader{b: body, pos: 8}
	m := &Manifest{}
	m.Epoch = r.u64()
	m.CreatedAtUnixMs = int64(r.u64())
	m.NextRowID = r.u64()
	m.RowCount = r.u64()
	m.CurrentSegment = r.u32()
	m.DurableOffset = r.u64()
	secCount := r.u32()
	// Sanidade contra contagens absurdas antes de alocar (36 B/segmento).
	if int(secCount) > len(body)/36 {
		return nil, fmt.Errorf("%w: segmentCount %d impossível", ErrCorruptManifest, secCount)
	}
	m.Segments = make([]SegmentInfo, 0, secCount)
	for i := uint32(0); i < secCount; i++ {
		m.Segments = append(m.Segments, SegmentInfo{
			ID:           r.u32(),
			SizeBytes:    int64(r.u64()),
			TotalRecords: r.u64(),
			LiveRecords:  r.u64(),
			LiveBytes:    r.u64(),
		})
	}
	m.PrimarySlotCount = r.u64()
	m.PrimaryUsed = r.u64()
	r.u8() // hashKind: reservado, lido e descartado (ver o layout)
	m.FieldDictValidBytes = int64(r.u64())
	// bitmapCount é reservado e vale zero em todo manifest v3. As entradas ainda
	// são PULADAS porque um manifest v1 ou v2 legado as tem, e o kafkaCount que
	// vem em seguida está exatamente `bmCount * bitmapInfoSize` bytes adiante: sem este
	// avanço o corpo legado sairia desalinhado e o dataset seria recusado como
	// corrompido sem ter nada de errado.
	if bmCount := r.u16(); bmCount > 0 {
		r.take(int(bmCount) * bitmapInfoSize)
	}
	if kafka := r.u32(); kafka != 0 {
		return nil, fmt.Errorf("%w: seção kafka inesperada (%d)", ErrCorruptManifest, kafka)
	}
	if err := decodeTLVTail(r, m); err != nil {
		return nil, err
	}
	if r.bad || r.pos != len(body) {
		return nil, fmt.Errorf("%w: tamanho inconsistente (pos=%d body=%d)", ErrCorruptManifest, r.pos, len(body))
	}
	return m, nil
}

// decodeTLVTail percorre os registros TLV até o fim do corpo. Um manifest v1
// simplesmente não entra no laço: o corpo acaba no kafkaCount.
//
// A checagem de comprimento é feita ANTES de consumir o payload de propósito.
// O `take` do reader devolve zeros e marca `bad` quando estoura, o que
// bastaria para reprovar no fim — mas com um len adulterado para perto de
// 4 GiB o laço só descobriria isso depois de tentar avançar o cursor por
// aquele tanto, e um manifest corrompido não pode custar mais que a leitura
// dos bytes que ele de fato tem.
func decodeTLVTail(r *reader, m *Manifest) error {
	for r.pos < len(r.b) && !r.bad {
		tag := r.u16()
		n := r.u32()
		if r.bad || uint64(r.pos)+uint64(n) > uint64(len(r.b)) {
			return fmt.Errorf("%w: registro TLV de tag %#04x declara %d bytes, além do fim do manifest", ErrCorruptManifest, tag, n)
		}
		payload := r.take(int(n))
		switch {
		case tag == tagHashes:
			if err := checkLegacyHashes(payload); err != nil {
				return err
			}
		case tag == tagPerSegmentIndex:
			if err := decodePerSegmentIndex(payload); err != nil {
				return err
			}
		case tag == tagHistory:
			if err := decodeHistory(payload, m); err != nil {
				return err
			}
		case tag == tagSchedule:
			if err := decodeSchedule(payload, m); err != nil {
				return err
			}
		case tag == tagCounters:
			if err := decodeCounters(payload, m); err != nil {
				return err
			}
		case tag&tagCritical != 0:
			// Ignorar uma seção crítica desconhecida é o modo de falha que
			// esta política existe para impedir: o dataset teria um índice
			// que esta abertura não mantém, e ele reapareceria INCOMPLETO.
			return fmt.Errorf("%w: seção crítica %#04x desconhecida por esta versão; o dataset foi escrito por um binário mais novo",
				ErrCorruptManifest, tag)
		}
		// Tag ignorável desconhecida: os bytes já foram pulados acima.
	}
	return nil
}

// decodeCounters lê a seção 0x0001.
//
// Tamanho EXATO, contado entrada por entrada: uma seção que declara mais
// contadores do que os bytes contêm é manifest corrompido, e não "menos
// contadores" — ler pela metade adotaria um contador com as colunas de outro,
// e o vetor de grupos passaria a ser interpretado com parâmetros que não são
// os dele.
func decodeCounters(payload []byte, m *Manifest) error {
	cr := &reader{b: payload}
	n := int(cr.u16())
	if cr.bad {
		return fmt.Errorf("%w: seção dos contadores truncada", ErrCorruptManifest)
	}
	cs := make([]CounterInfo, 0, n)
	for i := 0; i < n && !cr.bad; i++ {
		var c CounterInfo
		c.Name = string(cr.take(int(cr.u16())))
		nCols := int(cr.u16())
		c.Columns = make([]uint16, 0, nCols)
		for j := 0; j < nCols && !cr.bad; j++ {
			c.Columns = append(c.Columns, cr.u16())
		}
		c.Where = string(cr.take(int(cr.u32())))
		cs = append(cs, c)
	}
	if cr.bad || cr.pos != len(payload) {
		return fmt.Errorf("%w: seção dos contadores declara %d entradas em %d bytes", ErrCorruptManifest, n, len(payload))
	}
	m.Counters = cs
	return nil
}

// checkLegacyHashes lê a seção 0x8001 de um manifest escrito por um binário
// anterior, confere o comprimento e DESCARTA o conteúdo.
//
// Descartar é o certo: a seção nomeava os campos com índice hash GLOBAL, e esse
// motor não existe mais — os arquivos dele são apagados na abertura, e o índice
// por segmento cobre os mesmos campos por outra estrutura. Não descartar seria
// pior de duas formas: ou o dataset seria recusado como "escrito por um binário
// mais novo", que é o inverso da verdade, ou o Store adotaria um conjunto de
// campos que nenhuma parte dele sabe manter.
//
// O comprimento continua conferido mesmo sem uso do conteúdo, e não é zelo
// vazio: uma seção incoerente é sinal de manifest corrompido, e um manifest
// corrompido não pode passar por causa de uma seção que por acaso ficou sem
// leitor.
func checkLegacyHashes(payload []byte) error {
	hr := &reader{b: payload}
	n := int(hr.u16())
	if hr.bad || 2+n*hashInfoSize != len(payload) {
		return fmt.Errorf("%w: seção de hashes declara %d entradas em %d bytes", ErrCorruptManifest, n, len(payload))
	}
	return nil
}

// decodePerSegmentIndex lê a seção 0x8004 e RECUSA um formato desconhecido.
//
// Recusar é a resposta certa, e é a razão de a seção existir: um binário que não
// conhece o formato do índice não sabe lê-lo nem mantê-lo, e abrir assim
// deixaria o índice parar de ser atualizado — falso negativo silencioso, que a
// revalidação do §44 não conserta. É este ramo que a PRÓXIMA mudança de formato
// vai usar, em vez de mais um bump de versão do manifest.
func decodePerSegmentIndex(payload []byte) error {
	if len(payload) != indexInfoSize {
		return fmt.Errorf("%w: seção do índice por segmento tem %d bytes, esperado %d",
			ErrCorruptManifest, len(payload), indexInfoSize)
	}
	if format := payload[0]; format != currentIndexFormat {
		return fmt.Errorf("%w: o índice por segmento deste dataset está no formato %d, e esta versão só sabe o %d; o dataset foi escrito por um binário mais novo",
			ErrCorruptManifest, format, currentIndexFormat)
	}
	return nil
}

// decodeHistory lê a seção 0x8002.
func decodeHistory(payload []byte, m *Manifest) error {
	if len(payload) != historyInfoSize {
		return fmt.Errorf("%w: seção de histórico tem %d bytes, esperado %d",
			ErrCorruptManifest, len(payload), historyInfoSize)
	}
	hr := &reader{b: payload}
	m.StoreMode = hr.take(1)[0]
	m.RowIDBase = hr.u64()
	if hr.bad {
		return fmt.Errorf("%w: seção de histórico truncada", ErrCorruptManifest)
	}
	return nil
}

// decodeSchedule lê a seção 0x8003.
//
// Tamanho exato, e não "pelo menos": uma seção curta lida pela metade daria
// blockShift zero, que endereça o dataset inteiro num bloco só, e levels zero,
// que é uma árvore sem folhas. Nenhum dos dois falha na leitura — os dois
// falham depois, agendando errado.
func decodeSchedule(payload []byte, m *Manifest) error {
	if len(payload) != scheduleInfoSize {
		return fmt.Errorf("%w: seção da agenda tem %d bytes, esperado %d",
			ErrCorruptManifest, len(payload), scheduleInfoSize)
	}
	sr := &reader{b: payload}
	info := ScheduleInfo{FallbackDurationMs: sr.u64()}
	info.BlockShift = sr.u8()
	info.Levels = sr.u8()
	if sr.bad {
		return fmt.Errorf("%w: seção da agenda truncada", ErrCorruptManifest)
	}
	m.Schedule = &info
	return nil
}

// writeTrace é o seam de teste da ordem de durabilidade do Write (nil em
// produção).
var writeTrace func(step string)

// step executa a operação e, em sucesso, emite o evento — amarrar o trace à
// execução garante que remover a operação também remove o evento (poder de
// detecção contra a mutação "remove o Sync, mantém o emit").
func step(name string, op func() error) error {
	if err := op(); err != nil {
		return err
	}
	if writeTrace != nil {
		writeTrace(name)
	}
	return nil
}

// Write grava o manifest atomicamente.
func Write(path string, m *Manifest) error {
	return writeAtomic(path, m.encode())
}

// writeAtomic grava tmp → fsync → rename → fsync do diretório (a política
// de rename durável do design §9/M18).
//
// É compartilhado pelo manifest.bin e pelo kafka.offsets de propósito: este é
// o único caminho de durabilidade PROVADO do projeto (a ordem está travada
// por TestWriteOrdemDeDurabilidade), e um segundo arquivo com uma segunda
// cópia da sequência seria uma cópia que diverge na primeira correção.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: criar %s: %w", tmp, err)
	}
	if err := step("write", func() error {
		_, werr := f.Write(data)
		return werr
	}); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: escrever %s: %w", tmp, err)
	}
	if err := step("fsync", f.Sync); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := step("rename", func() error {
		return os.Rename(tmp, path)
	}); err != nil {
		return fmt.Errorf("checkpoint: rename de %s: %w", filepath.Base(path), err)
	}
	return step("dirsync", func() error {
		return syncDir(filepath.Dir(path))
	})
}

// syncDir força a durabilidade da entrada de diretório após um rename.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("checkpoint: fsync do diretório %s: %w", dir, err)
	}
	return nil
}

// Load lê e valida o manifest. Ausência devolve ErrInManifest (cold start).
func Load(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrInManifest, path)
	}
	if err != nil {
		return nil, err
	}
	return decode(raw)
}
