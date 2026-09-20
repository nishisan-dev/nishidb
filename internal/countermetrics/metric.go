package countermetrics

import (
	"math"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

const (
	// chunkShift dá o tamanho do bloco de entradas (1024): grande o bastante
	// para que o diretório de blocos fique curto num contador de milhares de
	// grupos, pequeno o bastante para que um contador de dez grupos não pague
	// mais que um bloco por métrica.
	chunkShift = 10
	chunkSize  = 1 << chunkShift
	chunkMask  = chunkSize - 1

	// fnvOffset64 e fnvPrime64 são os parâmetros do FNV-1a de 64 bits. O hash
	// da chave vai para a identidade do arquivo e para a marca de dono de cada
	// balde: é FORMATO DE DISCO, e trocar a função faz toda restauração
	// seguinte ver chave divergente e descartar a janela.
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// publishYield é o seam de teste do meio da escrita sob seqlock (nil em
// produção): chamado com seq ímpar, deixa o teste concorrente ceder o
// processador ali e forçar leitores a encontrarem a escrita em andamento,
// mesmo num núcleo só. O custo é uma comparação com nil por publicação.
var publishYield func()

// readPositionHook é o seam de teste entre a carga da posição do mapa e a
// leitura da entrada em Metric.read (nil em produção): é ali que a posição
// pode ser reatribuída a outro grupo, e o teste põe a reatribuição exatamente
// nessa brecha. O custo é uma comparação com nil por leitura.
var readPositionHook func(id, position uint32)

// keyHash é o FNV-1a de 64 bits da chave composta do grupo, com o zero trocado
// por 1: zero é a identidade vazia (entrada sem dono), e uma chave cujo hash
// desse zero nunca seria reconhecida como dona dos seus baldes.
//
// Implementado aqui, e não por hash/fnv: New64a devolve uma interface que
// escapa para o heap, e a passada chama isto uma vez por grupo.
func keyHash(key []byte) uint64 {
	h := fnvOffset64
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	if h == 0 {
		return 1
	}
	return h
}

// fold é a dobra de amostras: contagem, soma, mínimo e máximo.
type fold struct {
	count   uint32
	sum     int64
	minimum int64
	maximum int64
}

// add dobra uma amostra no balde aberto, SATURANDO a contagem em
// math.MaxUint16, que é o que cabe no balde em disco.
//
// Pelo laço a saturação é inalcançável: com o piso minSampleInterval o maior
// balde recebe no máximo 43.200 amostras (há teste que refaz a conta). Ela
// existe para que a garantia não dependa de quem chama SampleOnce: acima do
// teto, contagem e soma param juntas — o avg continua sendo a média exata das
// amostras contadas, em vez de uma soma de N+k amostras dividida por N —, e
// min e max seguem vendo toda amostra, porque não dependem da contagem.
func (f *fold) add(v int64) {
	if f.count == 0 {
		*f = fold{count: 1, sum: v, minimum: v, maximum: v}
		return
	}
	if f.count < math.MaxUint16 {
		f.count++
		f.sum += v
	}
	f.minimum = min(f.minimum, v)
	f.maximum = max(f.maximum, v)
}

// merge dobra um balde fechado (Count > 0) no cache. A contagem cabe em u32:
// no máximo 59 baldes de 65.535 amostras.
func (f *fold) merge(b metricwindow.Bucket) {
	if f.count == 0 {
		*f = fold{count: uint32(b.Count), sum: b.Sum, minimum: b.Min, maximum: b.Max}
		return
	}
	f.count += uint32(b.Count)
	f.sum += b.Sum
	f.minimum = min(f.minimum, b.Min)
	f.maximum = max(f.maximum, b.Max)
}

// writerState é a parte da entrada que só o amostrador lê e escreve; por isso
// não tem atômico nenhum.
type writerState struct {
	// keyHash é o dono da entrada nesta métrica, e é também o dono dos DADOS
	// do balde aberto: adopt zera o balde aberto sempre que keyHash muda, então
	// OwnerTag(keyHash) é a marca certa para gravá-lo. Zero é "sem dono".
	keyHash uint64
	// validFromEpoch é a primeira época que pertence ao dono corrente.
	validFromEpoch uint32
	// openEpoch é a época do balde aberto.
	openEpoch uint32
	// cacheEpoch é a época em que closed foi calculado: a janela dos fechados
	// é relativa a ela, e muda quando a época corrente muda.
	cacheEpoch uint32
	// openDirty diz que o balde aberto recebeu amostra desde a última vez que
	// foi gravado. Só balde sujo vai ao disco — no fechamento e no Stop —, o
	// que torna o Stop repetido de fato idempotente e evita regravar o balde
	// que a restauração acabou de ler do próprio disco.
	openDirty bool
	open      fold
	closed    fold
}

// published é o agregado publicado de uma entrada, sob seqlock: seq ímpar é
// escrita em andamento, e o leitor refaz a leitura se viu seq ímpar ou se seq
// mudou entre o começo e o fim. Os valores são atômicos só para que o detector
// de corrida não acuse o padrão; a consistência entre eles é do seq.
type published struct {
	seq     atomic.Uint64
	samples atomic.Uint64
	avgBits atomic.Uint64
	minimum atomic.Int64
	maximum atomic.Int64
	// owner é o id do grupo a quem o publicado pertence, dentro do seqlock: a
	// posição que o leitor carregou do mapa pode ter sido reatribuída a outro
	// grupo antes da leitura, e o leitor só aceita o valor cujo dono é o id que
	// pediu. Zero é "sem dono".
	owner atomic.Uint32
}

// entry é o estado de um grupo numa métrica, na posição dele.
type entry struct {
	w   writerState
	pub published
}

// chunk é um bloco de entradas. Blocos NUNCA se movem: o crescimento aloca
// blocos novos e troca só o diretório, então um leitor que carregou o
// diretório antigo continua lendo entradas vivas.
type chunk [chunkSize]entry

// entryBytes é o tamanho de uma entrada, lido do tipo para não divergir dele.
var entryBytes = int64(reflect.TypeFor[entry]().Size())

// Metric é o estado de uma métrica de janela de um contador: um arquivo
// metricwindow e, por POSIÇÃO de grupo vivo, uma entrada em heap. O mapa id →
// posição é do contador (ver counterState), compartilhado com as outras
// métricas dele.
//
// Aggregate e Usage são seguros de qualquer goroutine e não alocam; o resto é
// do amostrador.
type Metric struct {
	measureIndex  int
	measureWindow time.Duration
	counter       string
	name          string
	window        time.Duration
	widthMs       int64
	// state é o contador dono da métrica: o mapa id → posição e as posições
	// ocupadas.
	state *counterState
	// file é nil em modo leitura quando o arquivo não pôde ser lido.
	file *metricwindow.File

	// chunks é o diretório de blocos de entradas, trocado inteiro (cópia no
	// crescimento) só pelo amostrador e carregado pelos leitores.
	chunks atomic.Pointer[[]*chunk]

	newGroups    atomic.Int64
	resets       atomic.Int64
	columnWrites atomic.Int64
	heapBytes    atomic.Int64
	diskBytes    atomic.Int64
	sampledAt    atomic.Int64
}

// Aggregate devolve o agregado publicado do grupo id: a quantidade de amostras
// na janela, a média, o mínimo e o máximo. samples == 0 é "sem valor" (NULL);
// com samples > 0, avg é sempre finito (soma finita dividida por contagem
// positiva). Grupo sem posição, ainda não visto ou aposentado devolve samples 0.
//
// Lê sob seqlock, sem lock e sem alocação, nunca devolve uma linha rasgada
// (avg de uma passada com max de outra) e nunca devolve o valor de outro grupo:
// a posição carregada do mapa só vale se o dono publicado for o id pedido.
// *Metric nil devolve samples 0.
func (m *Metric) Aggregate(id uint32) (samples uint64, avg float64, minimum, maximum int64) {
	samples, avg, minimum, maximum, _ = m.read(id)
	return samples, avg, minimum, maximum
}

// read é Aggregate contando quantas vezes a leitura foi refeita — o número
// que prova, no teste concorrente, que leitor e escritor de fato se
// intercalaram.
func (m *Metric) read(id uint32) (samples uint64, avg float64, minimum, maximum int64, retries int) {
	if m == nil || id == 0 {
		return 0, 0, 0, 0, 0
	}
	position := m.state.positions.load(id)
	if position == 0 {
		return 0, 0, 0, 0, 0
	}
	if readPositionHook != nil {
		readPositionHook(id, position)
	}
	e := m.lookup(position)
	if e == nil {
		return 0, 0, 0, 0, 0
	}
	p := &e.pub
	for {
		s1 := p.seq.Load()
		if s1&1 == 0 {
			owner := p.owner.Load()
			samples = p.samples.Load()
			bits := p.avgBits.Load()
			minimum = p.minimum.Load()
			maximum = p.maximum.Load()
			if p.seq.Load() == s1 {
				if owner != id {
					return 0, 0, 0, 0, retries
				}
				return samples, math.Float64frombits(bits), minimum, maximum, retries
			}
		} else {
			// Escrita em andamento: ceder o processador em vez de girar, porque
			// o escritor pode ter sido preemptado entre os dois incrementos.
			runtime.Gosched()
		}
		retries++
	}
}

// lookup devolve a entrada da posição, ou nil se o diretório ainda não a
// cobre.
func (m *Metric) lookup(position uint32) *entry {
	dir := m.chunks.Load()
	if dir == nil {
		return nil
	}
	ci := position >> chunkShift
	if uint64(ci) >= uint64(len(*dir)) {
		return nil
	}
	return &(*dir)[ci][position&chunkMask]
}

// entry devolve a entrada da posição do lado do escritor, que já garantiu os
// blocos.
func (m *Metric) entry(position uint32) *entry {
	return &(*m.chunks.Load())[position>>chunkShift][position&chunkMask]
}

// Usage devolve o uso da métrica: grupos acompanhados (posições ocupadas do
// contador), bytes em heap, bytes em disco e o instante da última passada
// completa (zero é "nunca amostrou").
//
// heapBytes é a conta dos blocos de entradas alocados: blocos × 1024 entradas
// × o tamanho de uma entrada, até a maior posição usada. Não entra o mapa id →
// posição, que é do contador e sai em MetricStats.PositionMapBytes, nem o
// diretório de blocos (8 bytes por bloco) e os campos fixos de Metric. *Metric
// nil devolve zeros.
func (m *Metric) Usage() (groups, heapBytes, diskBytes, sampledAtUnixMs int64) {
	if m == nil {
		return 0, 0, 0, 0
	}
	return m.state.occupied.Load(), m.heapBytes.Load(), m.diskBytes.Load(), m.sampledAt.Load()
}

// ensureEntries garante blocos para as posições [0, limit). Aloca só quando
// cresce.
func (m *Metric) ensureEntries(limit uint32) {
	cur := m.chunks.Load()
	need := int((uint64(limit) + chunkSize - 1) >> chunkShift)
	if cur != nil && len(*cur) >= need {
		return
	}
	grown := make([]*chunk, need)
	if cur != nil {
		copy(grown, *cur)
	}
	for i := range grown {
		if grown[i] == nil {
			grown[i] = new(chunk)
		}
	}
	m.chunks.Store(&grown)
	m.heapBytes.Store(int64(need) * chunkSize * entryBytes)
}

// epochOf é a época do balde que contém now.
func (m *Metric) epochOf(now time.Time) uint32 {
	ms := now.UnixMilli()
	if ms <= 0 {
		return 0
	}
	return uint32(ms / m.widthMs)
}

// observe dobra a amostra de um grupo vivo na entrada da posição dele. A única
// escrita de balde no mmap é o fechamento do balde aberto (writeOpen) — é o
// que o layout colunar do arquivo promete: uma passada dentro da mesma época
// não suja página de balde nenhuma.
func (m *Metric) observe(position, id uint32, h uint64, total int64, epoch uint32) {
	e := m.entry(position)
	w := &e.w
	if h != w.keyHash {
		m.adopt(position, e, h, epoch)
	}
	if w.openEpoch != epoch {
		if w.openDirty {
			m.writeOpen(position, w)
		}
		w.open = fold{}
		w.openEpoch = epoch
	}
	if w.cacheEpoch != epoch {
		// A janela andou — com ou sem balde aberto a fechar: baldes que
		// saíram dela deixam o cache, e o balde que acabou de fechar entra.
		m.refreshClosed(position, w, epoch)
	}
	w.open.add(total)
	w.openDirty = true
	e.publish(id)
}

// adopt dá à entrada um dono novo: grupo novo numa posição nunca usada, grupo
// que reaproveita a posição de um aposentado, ou contador reconstruído que
// deu a um id outra chave.
//
// A ordem é a que metricwindow.File.SetIdentity exige. O balde aberto e o
// cache do dono antigo são DESCARTADOS — nunca gravados —, e a identidade nova
// vai ao disco antes de qualquer balde do dono novo. Gravar o balde aberto
// antigo depois da identidade nova, com a marca nova, é o único erro que as
// defesas do formato não pegam: ele passaria, na restauração, como do dono
// novo.
//
// O cache fica vazio e calculado para a época corrente sem ler o disco:
// validFromEpoch = epoch exclui todo balde fechado, inclusive os do dono
// antigo cuja marca de 16 bits colida com a do novo.
func (m *Metric) adopt(position uint32, e *entry, h uint64, epoch uint32) {
	m.newGroups.Add(1)
	e.w = writerState{keyHash: h, validFromEpoch: epoch, openEpoch: epoch, cacheEpoch: epoch}
	m.file.SetIdentity(position, metricwindow.Identity{KeyHash: h, ValidFromEpoch: uint64(epoch)})
}

// retire esvazia a entrada de uma posição cujo grupo deixou de estar vivo: o
// publicado vira "sem valor" e sem dono, o estado do escritor zera e a
// identidade em disco fica vazia. O balde aberto é descartado — o grupo saiu do
// inventário, e a janela dele não tem mais leitor.
func (m *Metric) retire(position uint32) {
	e := m.entry(position)
	e.store(0, 0, 0, 0, 0)
	e.w = writerState{}
	m.file.SetIdentity(position, metricwindow.Identity{})
}

// refreshClosed recalcula o cache dos baldes fechados da posição, relativo à
// época corrente, lendo os slots do arquivo.
func (m *Metric) refreshClosed(position uint32, w *writerState, epoch uint32) {
	tag := metricwindow.OwnerTag(w.keyHash)
	var c fold
	for slot := uint32(0); slot < schema.MetricWindowBuckets; slot++ {
		if b := m.file.Bucket(slot, position); closedInWindow(b, tag, w.validFromEpoch, epoch) {
			c.merge(b)
		}
	}
	w.closed = c
	w.cacheEpoch = epoch
}

// closedInWindow diz se um balde lido do disco entra no cache dos fechados da
// época corrente: tem amostra, a marca é do dono corrente, a época é do dono
// corrente (>= validFromEpoch), já fechou (< época corrente) e ainda está na
// janela — que são as schema.MetricWindowBuckets épocas terminando na
// corrente, então o fechado mais antigo é o de época corrente - 59.
//
// Marca e validFromEpoch são defesas independentes: a marca tem 16 bits, e
// quando duas chaves colidem nela, é validFromEpoch que separa os baldes do
// dono antigo de uma posição reaproveitada.
func closedInWindow(b metricwindow.Bucket, tag uint16, validFromEpoch, epoch uint32) bool {
	return b.Count > 0 &&
		b.Owner == tag &&
		b.Epoch >= validFromEpoch &&
		b.Epoch < epoch &&
		epoch-b.Epoch < schema.MetricWindowBuckets
}

// writeOpen grava o balde aberto da posição no slot da sua época, com a marca
// do dono dos dados (ver writerState.keyHash), e o marca como limpo. A
// contagem cabe em u16 pela saturação de fold.add.
func (m *Metric) writeOpen(position uint32, w *writerState) {
	m.file.WriteBucket(w.openEpoch%schema.MetricWindowBuckets, position, metricwindow.Bucket{
		Epoch: w.openEpoch,
		Owner: metricwindow.OwnerTag(w.keyHash),
		Count: uint16(w.open.count),
		Sum:   w.open.sum,
		Min:   w.open.minimum,
		Max:   w.open.maximum,
	})
	w.openDirty = false
	m.columnWrites.Add(1)
}

// publish combina o cache dos fechados com o balde aberto e publica em nome do
// grupo owner.
func (e *entry) publish(owner uint32) {
	w := &e.w
	n := uint64(w.closed.count) + uint64(w.open.count)
	if n == 0 {
		e.store(0, 0, 0, 0, owner)
		return
	}
	lo, hi := w.open.minimum, w.open.maximum
	switch {
	case w.open.count == 0:
		lo, hi = w.closed.minimum, w.closed.maximum
	case w.closed.count > 0:
		lo = min(lo, w.closed.minimum)
		hi = max(hi, w.closed.maximum)
	}
	// As somas viram float ANTES de somar, para que a soma das duas partes não
	// estoure int64; cada parte, por sua vez, é total de linhas vezes amostras.
	avg := (float64(w.closed.sum) + float64(w.open.sum)) / float64(n)
	e.store(n, math.Float64bits(avg), lo, hi, owner)
}

// store é a escrita sob seqlock: seq ímpar, dono e valores, seq par.
func (e *entry) store(samples, avgBits uint64, lo, hi int64, owner uint32) {
	p := &e.pub
	p.seq.Add(1)
	if publishYield != nil {
		publishYield()
	}
	p.owner.Store(owner)
	p.samples.Store(samples)
	p.avgBits.Store(avgBits)
	p.minimum.Store(lo)
	p.maximum.Store(hi)
	p.seq.Add(1)
}

// restoreEntry publica, a partir do disco e relativo a now, o agregado da
// entrada da posição que o grupo vivo id (chave de hash h) reclamou na
// restauração do contador, e devolve 1 se ela saiu com amostra. Não escreve
// nada: é o MESMO código nos dois modos.
//
// Identidade desta métrica diferente de h (arquivo novo, recriado ou de
// histórico divergente) não publica: a primeira passada adota a entrada. O
// balde do slot da época corrente vira o balde aberto se for desta época e do
// dono corrente — reinício dentro do mesmo balde, e a contagem continua. Ele
// volta LIMPO: já está em disco, e só vai de novo se receber amostra.
func (m *Metric) restoreEntry(position, id uint32, h uint64, now time.Time) int64 {
	if m.file == nil || position >= m.file.Capacity() {
		return 0
	}
	ident := m.file.Identity(position)
	if ident.KeyHash != h || ident.ValidFromEpoch > math.MaxUint32 {
		return 0
	}
	epoch := m.epochOf(now)
	e := m.entry(position)
	w := &e.w
	*w = writerState{keyHash: h, validFromEpoch: uint32(ident.ValidFromEpoch), openEpoch: epoch}
	b := m.file.Bucket(epoch%schema.MetricWindowBuckets, position)
	if b.Epoch == epoch && b.Count > 0 && b.Owner == metricwindow.OwnerTag(h) && b.Epoch >= w.validFromEpoch {
		w.open = fold{count: uint32(b.Count), sum: b.Sum, minimum: b.Min, maximum: b.Max}
	}
	m.refreshClosed(position, w, epoch)
	e.publish(id)
	if e.pub.samples.Load() > 0 {
		return 1
	}
	return 0
}

// saveOpenBuckets grava todo balde aberto sujo no seu slot e faz msync.
// Arquivo fechado por um crescimento que falhou não é tocado: o msync devolve
// o erro que diz isso.
func (m *Metric) saveOpenBuckets() error {
	if m.file == nil {
		return nil
	}
	capacity := m.file.Capacity()
	if capacity == 0 {
		return m.file.Msync()
	}
	if dir := m.chunks.Load(); dir != nil {
		for ci, c := range *dir {
			for i := range c {
				e := &c[i]
				if e.w.keyHash == 0 || !e.w.openDirty {
					continue
				}
				position := uint32(ci)<<chunkShift | uint32(i)
				if position >= capacity {
					continue
				}
				m.writeOpen(position, &e.w)
			}
		}
	}
	return m.file.Msync()
}
