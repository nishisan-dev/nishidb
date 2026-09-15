// Package metricwindow é o dono do formato em disco da janela da métrica
// (avg/min/max do total de cada grupo de um contador numa janela deslizante,
// como 5m ou 24h — não confundir com a janela do rowindex): um arquivo por
// janela, com uma coluna de identidade por índice de entrada e uma coluna por slot de
// balde.
//
// O pacote é SÓ o formato. Não tem relógio, nem goroutine, nem conhecimento de
// contador: quem lê o total de cada grupo a cada poucos segundos, dobra a
// amostra no balde aberto em heap e decide quando o balde fecha é o amostrador,
// em outro pacote. Daqui ele usa duas coisas: gravar o balde que acabou de
// fechar e, na subida, ler de volta os baldes que o processo anterior deixou.
//
// # Por que colunar
//
// Cada janela tem uma quantidade fixa de baldes (60 no amostrador), e a
// passada que fecha um balde grava uma entrada por grupo. Se o layout fosse
// por grupo — identidade e os 60 baldes de um id lado a lado —, cada passada
// escreveria uma entrada em CADA bloco de grupo, sujaria todas as páginas do
// arquivo, e o writeback do kernel regravaria o arquivo inteiro a cada ~30 s,
// para mudar 1/60 do conteúdo. Colunar por slot de balde faz o fechamento do
// balde k sujar só a coluna k: o custo de I/O de uma passada é proporcional ao
// que ela muda, não ao tamanho da janela.
//
// A capacidade é potência de 2 com piso 1024 também por isso: capacity*16 e
// capacity*32 são múltiplos de 4096, então TODA coluna começa numa fronteira de
// página e nenhuma página é compartilhada por duas colunas — sem isso, o
// fechamento do balde k sujaria a última página da coluna k-1. Pelo mesmo
// alinhamento, uma entrada de 32 bytes nunca atravessa duas páginas.
//
// # Layout (little-endian)
//
//	[0, 4096)                        cabeçalho: magic "NDBT" | version u32 = 1 |
//	                                 bucketCount u32 | reserved u32 |
//	                                 bucketWidthMs i64 | source u32 | capacity u32 |
//	                                 crc32c u32 (Castagnoli, sobre os 32 bytes
//	                                 anteriores); resto da página zerado
//	[4096, 4096 + capacity*16)       coluna de identidade, por índice de entrada:
//	                                 keyHash u64 | validFromEpoch u64
//	slot s, a partir de
//	4096 + capacity*16 + s*capacity*32  coluna do balde s, por índice de entrada:
//	                                 palavra 0 | sum i64 | min i64 | max i64
//
//	palavra 0 de um balde:           epoch u32 (bits 63..32) |
//	                                 owner u16 (bits 31..16) |
//	                                 count u16 (bits 15..0)
//
// O arquivo tem de ter EXATAMENTE o tamanho que o cabeçalho declara; maior ou
// menor é cabeçalho ilegível.
//
// # Índice da entrada
//
// Todo parâmetro chamado id nesta API — e todo "id" deste godoc — é o ÍNDICE
// DA ENTRADA: a posição que o chamador atribuiu, e não necessariamente o id do
// grupo no contador. O amostrador atribui posições densas a grupos vivos e as
// reaproveita quando um grupo se aposenta, e é esse reaproveitamento que as
// defesas de troca de dono abaixo (identidade, marca de dono, validFromEpoch)
// protegem.
//
// O índice 0 nunca é usado pelo amostrador (as posições começam em 1), mas
// ocupa lugar nas colunas para que o endereço de uma entrada seja id*16 e
// id*32, sem subtração. O formato não o proíbe: a convenção é do chamador.
//
// A magic é "NDBT", e não "NDBW": "NDBW" já é a magic do WIP do índice por
// segmento (segindex.WIPMagic), e duas famílias de arquivo com a mesma magic
// deixam de se distinguir pelo único byte que existe para distingui-las.
//
// # Época e slot
//
// A época do balde é o número do balde desde a época Unix, unixMs /
// bucketWidthMs, e é o CHAMADOR que a calcula e passa: este pacote não lê
// relógio. O slot também é escolha do chamador, e a convenção é epoch %
// bucketCount — com ela o slot de um balde é reaproveitado exatamente
// bucketCount baldes depois, quando o balde antigo já saiu da janela. A época
// cabe em u32 até 2106 para larguras de balde a partir de 1 s.
//
// # Contagem de 16 bits
//
// A contagem de amostras de um balde cabe em u16 pelo teto da forma, não por
// sorte: o maior balde é schema.MaxMetricWindow / schema.MetricWindowBuckets
// = 12 h, e com o piso de amostragem de 1 s isso dá no máximo 43.200 amostras
// por balde, abaixo de 65.535. Um WriteBucket com contagem que não caiba é
// impossível pelo tipo. Que a dobra não estoure ANTES de chegar aqui é
// obrigação do amostrador, noutro pacote: saturar a contagem e provar a
// saturação com teste próprio. Este pacote não tem como conferir isso.
//
// # Escrita parcial num crash
//
// Não há crc no conteúdo. A defesa tem duas escalas, e cada uma usa o que
// está ao seu alcance.
//
// Dentro de UMA entrada, a ordem das palavras: a entrada nunca atravessa
// página, e o writeback copia a página num instante qualquer, então o que um
// crash deixa em disco é um prefixo da sequência de escritas do método
// (WriteBucket e SetIdentity documentam as suas). Uma entrada com época velha é
// inofensiva: quem lê descarta todo balde fora da janela [época corrente -
// bucketCount + 1, época corrente].
//
// ENTRE páginas, a ordem de escrita não protege nada: a identidade de um id e
// os baldes dele moram em colunas diferentes, e numa queda qualquer
// combinação de páginas velhas e novas pode chegar ao disco. É o caso do id
// reaproveitado por outro grupo, e o formato se defende dele sozinho, com duas
// marcas:
//
//   - a marca de dono (OwnerTag), 16 bits do hash da chave do dono dos DADOS,
//     que o chamador passa em b.Owner e WriteBucket grava como recebida. Se a
//     página de baldes do dono novo chegou ao disco e a de identidade não, o
//     balde traz a marca do dono novo ao lado da identidade do antigo, e quem lê
//     descarta o balde cuja marca não é OwnerTag(identity.KeyHash);
//   - validFromEpoch, na identidade. Se a página de identidade do dono novo
//     chegou e a de baldes não, os baldes velhos têm época anterior a
//     validFromEpoch e são descartados — a marca também diverge, então este
//     caso tem as DUAS defesas, e uma colisão da marca não passa.
//
// O limite, escrito para não ser prometido demais: no primeiro caso só a marca
// defende (os baldes do dono novo têm época POSTERIOR ao validFromEpoch velho),
// e duas chaves com a mesma marca — uma chance em 65.536 por id reaproveitado,
// e só quando a queda separa exatamente essas duas páginas — deixam passar os
// baldes do dono novo como do antigo. É telemetria de alarme; o custo de fechar
// esse resto seria crc por entrada ou identidade repetida em todo balde.
//
// # Fora da ordem de checkpoint
//
// O arquivo NÃO entra na ordem de durabilidade do checkpoint (§48: persist →
// checkpoint durável → commit do offset). É telemetria que sobrevive a
// reinício, não estado materializado: nada do dataset é derivado dele, a
// recovery não o lê, e perdê-lo inteiro custa uma janela de histórico de
// alarme, nunca uma linha. Escrever um balde antes ou depois do checkpoint só
// muda QUANDO um alarme sobre a janela dispara, nunca a integridade do dado
// materializado. Amarrá-lo ao checkpoint compraria uma garantia que ninguém
// consome ao preço de mais um msync sob o lock do writer.
//
// # Concorrência
//
// *File NÃO é thread-safe, de propósito: o arquivo tem UM dono, o amostrador.
// Não há leitor concorrente no mmap — quem consulta a janela pelo SQL ou pelo
// alarme lê valores que o amostrador publica em heap, em outro pacote. Um lock
// aqui protegeria um acesso que o desenho não tem.
package metricwindow

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
)

const (
	headerSize = 4096
	// headerFieldsSize são os bytes do cabeçalho que o crc cobre; o crc vem
	// logo depois, e o resto da página é zero.
	headerFieldsSize = 32
	identitySize     = 16
	bucketSize       = 32
	formatVersion    = 1

	// capacityFloor é o piso da capacidade, e o que garante (com a potência de
	// 2) que toda coluna comece numa fronteira de página.
	capacityFloor = 1024
	// capacityLimit é a maior potência de 2 que o campo capacity u32 guarda.
	capacityLimit = 1 << 31

	// copyChunk é o bloco da cópia de colunas no crescimento: WithBytes segura
	// o RLock da região durante o callback, e um bloco limitado mantém cada
	// seção crítica curta mesmo numa coluna de centenas de MiB.
	copyChunk = 1 << 20

	tmpSuffix = ".tmp"
)

var (
	fileMagic  = [4]byte{'N', 'D', 'B', 'T'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// openRegion é o seam de teste da reabertura depois do rename (mmap.Open em
// produção). Só a abertura e o crescimento passam por ele; o caminho quente
// não o toca.
var openRegion = mmap.Open

// writeTrace é o seam de teste da ORDEM das palavras escritas (nil em
// produção), no precedente do writeTrace do checkpoint. O custo no caminho
// quente é carregar uma variável e compará-la com nil, sem alocação — o gate
// de alocação mede as operações com o seam no lugar.
var writeTrace func(off int64, v uint64)

// ErrCapacityExceeded marca um índice de entrada no teto (maxCapacity) ou acima
// dele. Não é corrupção nem falha de infra: o chamador decide o que fazer com
// a entrada que não cabe, e conta.
var ErrCapacityExceeded = errors.New("metricwindow: group id exceeds the maximum capacity")

// ErrCorruptHeader marca um cabeçalho ilegível: magic, versão, crc, campo fora
// de faixa ou arquivo de tamanho diferente do que o cabeçalho declara. Open
// trata como reset; OpenReadOnly devolve o erro, porque não pode reescrever o
// arquivo.
var ErrCorruptHeader = errors.New("metricwindow: corrupt header")

// ErrClosed marca o uso de um File fechado — por Close, ou por um crescimento
// que falhou na reabertura depois do rename. EnsureCapacity e Msync o
// devolvem; as quatro operações quentes, que não devolvem erro, entram em
// pânico com mensagem (ver Identity).
var ErrClosed = errors.New("metricwindow: file is closed")

// Header são os parâmetros que definem a forma do arquivo. Mudar qualquer um
// deles muda o significado de toda entrada, e por isso Open recria o arquivo
// quando o cabeçalho em disco diverge.
type Header struct {
	// BucketCount é a quantidade de baldes (e de colunas de balde) da janela.
	BucketCount uint32
	// BucketWidthMs é a largura de um balde em milissegundos: a janela dividida
	// pela quantidade de baldes.
	BucketWidthMs int64
	// Source identifica de onde vêm as amostras (o contador, no chamador).
	// Uma fonte diferente com a mesma forma não pode herdar os baldes de outra.
	Source uint32
}

// Identity diz a quem pertencem os baldes de uma entrada.
type Identity struct {
	// KeyHash é o hash da chave do grupo dono da entrada.
	KeyHash uint64
	// ValidFromEpoch é a primeira época de balde que pertence a esse dono;
	// baldes de época anterior são de um dono antigo da mesma entrada.
	ValidFromEpoch uint64
}

// Bucket é a dobra das amostras de um grupo em um balde.
type Bucket struct {
	Epoch uint32
	// Owner é a marca de dono do balde: OwnerTag do hash da chave do grupo a
	// quem os DADOS pertencem. WriteBucket a grava como recebida e Bucket a
	// devolve como está em disco, então regravar um balde relido preserva a
	// marca original. Quem lê descarta o balde com
	// Owner != OwnerTag(identity.KeyHash).
	Owner uint16
	// Count é a quantidade de amostras dobradas (ver "Contagem de 16 bits" no
	// godoc do pacote).
	Count uint16
	Sum   int64
	Min   int64
	Max   int64
}

// OwnerTag dobra o hash de 64 bits da chave de um grupo na marca de dono de 16
// bits gravada em cada balde: o XOR das quatro metades de 16 bits. É o ÚNICO
// lugar da dobra — quem lê compara bucket.Owner == OwnerTag(identity.KeyHash)
// e descarta a divergência. Dobrar em vez de truncar faz os 64 bits pesarem na
// marca, então duas chaves que só diferem na metade alta não colidem de graça.
func OwnerTag(keyHash uint64) uint16 {
	return uint16(keyHash ^ keyHash>>16 ^ keyHash>>32 ^ keyHash>>48)
}

// File é um arquivo da janela da métrica mapeado em memória. NÃO é
// thread-safe (ver o godoc do pacote).
type File struct {
	// region é nil no File fechado — por Close, ou por um crescimento que
	// falhou na reabertura (ver EnsureCapacity). capacity é zero junto.
	region      *mmap.Region
	path        string
	header      Header
	capacity    uint32
	maxCapacity uint32
	readOnly    bool
}

// Open abre o arquivo da janela da métrica em path, criando-o se ausente.
//
// Se o cabeçalho existente diverge de h (qualquer dos três campos) ou é
// ilegível (ErrCorruptHeader), o arquivo é recriado do zero e reset=true volta
// para o chamador logar: baldes de outra largura, de outra quantidade ou de
// outra fonte não têm leitura correta, e o custo de descartá-los é uma janela
// de histórico — telemetria, não dado materializado.
//
// minCapacity é arredondada para potência de 2 com piso 1024; um arquivo
// existente com capacidade menor cresce até ela, e um com capacidade maior a
// mantém (nunca encolhe). maxCapacity é o teto dos índices de entrada aceitos
// por EnsureCapacity; no amostrador é o limite do formato, e o tamanho real
// vem das posições que ele atribui, limitadas pelo máximo de grupos vivos
// simultâneos.
//
// A criação e o reset passam pelo mesmo caminho do crescimento — arquivo tmp,
// msync, rename, reabertura, fsync do diretório —, então o arquivo em path,
// quando existe, sempre tem um cabeçalho válido. Uma sobra de tmp de uma
// reescrita interrompida é removida aqui. Qualquer erro, inclusive o do fsync
// do diretório, devolve File nil.
func Open(path string, h Header, minCapacity, maxCapacity uint32) (f *File, reset bool, err error) {
	if err := validateHeader(h); err != nil {
		return nil, false, err
	}
	if maxCapacity == 0 || minCapacity > maxCapacity {
		return nil, false, fmt.Errorf("metricwindow: invalid capacity range: minimum %d, maximum %d", minCapacity, maxCapacity)
	}
	wanted, err := roundCapacity(minCapacity)
	if err != nil {
		return nil, false, err
	}
	if err := removeLeftoverTmp(path); err != nil {
		return nil, false, err
	}

	capacity := wanted
	renamed := false
	onDisk, diskCapacity, err := readHeader(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := build(path, h, capacity, nil, 0); err != nil {
			return nil, false, err
		}
		renamed = true
	case errors.Is(err, ErrCorruptHeader) || (err == nil && onDisk != h):
		// A comparação é do struct inteiro, de propósito: um campo novo no
		// Header entra na comparação sem ninguém precisar lembrar dele.
		reset = true
		if err := build(path, h, capacity, nil, 0); err != nil {
			return nil, false, err
		}
		renamed = true
	case err != nil:
		return nil, false, err
	default:
		// Adota a capacidade em disco; se ela for menor que a pedida, o
		// crescimento abaixo leva o arquivo até a pedida.
		capacity = diskCapacity
	}

	r, err := openRegion(path, fileSize(h.BucketCount, capacity))
	if err != nil {
		return nil, false, err
	}
	f = &File{region: r, path: path, header: h, capacity: capacity, maxCapacity: maxCapacity}
	if renamed {
		if err := atomicfile.SyncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, false, fmt.Errorf("metricwindow: %w", err)
		}
	}
	if wanted > capacity {
		if err := f.grow(wanted); err != nil {
			_ = f.Close()
			return nil, false, err
		}
	}
	return f, reset, nil
}

// OpenReadOnly abre um arquivo EXISTENTE só para leitura, para a CLI local.
// Cabeçalho ilegível — inclusive arquivo vazio, truncado ou maior que o
// declarado — devolve ErrCorruptHeader: sem poder reescrever o arquivo, não há
// reset possível.
//
// SetIdentity e WriteBucket entram em pânico na região somente-leitura (é o
// contrato de mmap.Region), e EnsureCapacity e Msync devolvem mmap.ErrReadOnly.
func OpenReadOnly(path string) (*File, error) {
	r, err := mmap.OpenReadOnly(path)
	if errors.Is(err, mmap.ErrEmptyFile) {
		return nil, fmt.Errorf("%w: %s: %w", ErrCorruptHeader, path, err)
	}
	if err != nil {
		return nil, err
	}
	h, capacity, err := decodeHeader(r.Bytes(), r.Size())
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &File{region: r, path: path, header: h, capacity: capacity, maxCapacity: capacity, readOnly: true}, nil
}

// Header devolve os parâmetros do arquivo.
func (f *File) Header() Header { return f.header }

// Capacity devolve quantas entradas o arquivo endereça hoje: índices válidos são
// [0, Capacity()). Zero no File fechado.
func (f *File) Capacity() uint32 { return f.capacity }

// Identity devolve a identidade do id.
//
// As quatro operações quentes (Identity, SetIdentity, Bucket, WriteBucket)
// entram em pânico com mensagem em dois casos que só um bug do chamador
// alcança: argumento fora da forma (id fora da capacidade, slot fora da
// quantidade de baldes) e File fechado (ErrClosed nos métodos que devolvem
// erro). O precedente é rowindex.Pack, que entra em pânico com argumento que o
// chamador nunca deveria produzir; devolver erro aqui poria uma checagem em
// toda escrita do amostrador por um estado que o desenho não tem. Não dá para
// deixar a checagem de limites para mmap.Region: ela só recusa offset além do
// ARQUIVO, e um id acima da capacidade dentro de uma coluna de balde cairia,
// calado, na coluna seguinte.
func (f *File) Identity(id uint32) Identity {
	off := f.identityOff(id)
	return Identity{
		KeyHash:        f.region.AtomicLoad64(off),
		ValidFromEpoch: f.region.AtomicLoad64(off + 8),
	}
}

// SetIdentity grava a identidade do id. Só o escritor chama.
//
// Quem troca o dono de um id segue esta ordem: a identidade nova vem ANTES do
// primeiro balde do dono novo, e o balde aberto do dono antigo é descartado ou
// gravado com a marca DELE — que o leitor, vendo a identidade nova, descarta.
// Gravar o balde do dono antigo com a marca do novo é o único erro que as duas
// defesas não pegam (época >= validFromEpoch e marca igual à da identidade), e
// é por isso que WriteBucket honra a marca recebida em vez de derivá-la.
//
// Ordem dentro da entrada: validFromEpoch vai ANTES de keyHash. Se a escrita
// for cortada entre as duas, o disco fica com a época nova e o hash antigo — o
// leitor vê um hash que não é o do grupo corrente e descarta os baldes do id;
// a ordem inversa deixaria o hash novo com a época antiga, e os baldes do dono
// anterior passariam como do novo. Entre páginas, quem defende é a marca de
// dono (ver o godoc do pacote).
func (f *File) SetIdentity(id uint32, v Identity) {
	off := f.identityOff(id)
	f.store(off+8, v.ValidFromEpoch)
	f.store(off, v.KeyHash)
}

// Bucket devolve o balde do id no slot. Pânico nos casos de Identity.
func (f *File) Bucket(slot, id uint32) Bucket {
	off := f.bucketOff(slot, id)
	head := f.region.AtomicLoad64(off)
	return Bucket{
		Epoch: uint32(head >> 32),
		Owner: uint16(head >> 16),
		Count: uint16(head),
		Sum:   int64(f.region.AtomicLoad64(off + 8)),
		Min:   int64(f.region.AtomicLoad64(off + 16)),
		Max:   int64(f.region.AtomicLoad64(off + 24)),
	}
}

// WriteBucket grava o balde do id no slot, com a marca b.Owner COMO RECEBIDA.
// Só o escritor chama.
//
// Quem chama passa em b.Owner o OwnerTag do hash do dono dos DADOS do balde, e
// o leitor descarta o balde cuja marca não é OwnerTag(identity.KeyHash).
// Honrar a marca, em vez de derivá-la da identidade corrente, é o que torna
// visível o erro de atribuição: com a marca derivada, o balde aberto do dono
// antigo gravado depois de SetIdentity do novo sairia com a marca nova e época
// >= validFromEpoch, e seria lido como do novo em silêncio; com a marca
// honrada, ele sai com a marca do antigo e o leitor o descarta. Regravar um
// balde relido por Bucket preserva a marca original. A regra de ordem para
// trocar o dono de um id está em SetIdentity.
//
// Ordem dentro da entrada: a primeira escrita ZERA a palavra 0 — época 0 é o
// balde de 1970, fora de qualquer janela viva —, depois vêm sum, min e max, e a
// palavra 0 com época, marca e contagem vai por último. O writeback copia a
// página num instante qualquer, então o que um crash deixa em disco é um
// PREFIXO dessa sequência: a entrada antiga inteira, uma entrada de época 0
// (descartada), ou a nova inteira. Sem o zero inicial, a regravação de um balde
// da MESMA época poderia deixar a contagem antiga ao lado da soma nova, dentro
// da janela.
//
// # Por que AtomicLoad64/AtomicStore64, e não Bytes()
//
// O dono é um só, então a atomicidade não é o motivo. O motivo é que a palavra
// atômica é a ÚNICA superfície de mmap.Region que serve às duas variedades com
// um código só: Bytes() existe apenas na região somente-leitura (na crescível
// ela entra em pânico, porque um Grow remapearia a slice), e WithBytes passa
// por callback. O custo é um RLock sem disputa por palavra — o mesmo que o
// rowindex paga dentro do applyRecord —, e o gate de alocação deste pacote
// prova que as quatro operações não alocam.
func (f *File) WriteBucket(slot, id uint32, b Bucket) {
	off := f.bucketOff(slot, id)
	f.store(off, 0)
	f.store(off+8, uint64(b.Sum))
	f.store(off+16, uint64(b.Min))
	f.store(off+24, uint64(b.Max))
	f.store(off, uint64(b.Epoch)<<32|uint64(b.Owner)<<16|uint64(b.Count))
}

// EnsureCapacity garante id < Capacity(). Só o escritor chama.
//
// File fechado devolve ErrClosed. Id no teto ou acima dele (id >= maxCapacity)
// devolve ErrCapacityExceeded, mesmo que a capacidade arredondada já o cobrisse:
// o teto é uma resposta estável, não um acidente do arredondamento.
//
// O crescimento é por REESCRITA, dobrando a capacidade até cobrir o id: um
// arquivo tmp com a capacidade nova recebe a coluna de identidade e cada coluna
// de balde no endereço novo, passa por msync e é renomeado sobre o antigo; o
// arquivo novo é reaberto e SÓ ENTÃO o diretório recebe fsync. Region.Grow não
// serve aqui: o início de toda coluna de balde depende da capacidade, então
// crescer no lugar exigiria mover as colunas dentro do próprio arquivo, e um
// crash no meio misturaria colunas sem que nada — não há crc no conteúdo —
// pudesse detectar. Com a reescrita, o arquivo em path é sempre o antigo
// inteiro ou o novo inteiro. O preço é espaço para os dois arquivos durante a
// cópia.
//
// Depois do rename o File NUNCA continua no arquivo antigo: a região antiga
// mapeia um inode já sem nome, e escrever nela seria perder a escrita. São três
// desfechos:
//
//   - erro ANTES do rename (ou no próprio rename): o File fica intacto, na
//     capacidade antiga, e o tmp é removido;
//   - erro na reabertura, DEPOIS do rename: o File fica fechado (ErrClosed a
//     seguir) e o arquivo em disco é o novo, válido — o dono tem de chamar Open
//     de novo;
//   - erro no fsync do diretório, depois da reabertura: o File JÁ está no
//     arquivo novo, na capacidade nova, e continua usável; o erro diz que o
//     rename pode não ser durável. Um reinício do processo encontra o arquivo
//     novo com tudo o que foi escrito; uma queda da MÁQUINA pode trazer de volta
//     o arquivo antigo, que Open adota normalmente — perde-se o que foi escrito
//     depois do crescimento, que é telemetria.
func (f *File) EnsureCapacity(id uint32) error {
	if f.region == nil {
		return fmt.Errorf("%w: EnsureCapacity on %s", ErrClosed, f.path)
	}
	if f.readOnly {
		return fmt.Errorf("%w: EnsureCapacity on %s", mmap.ErrReadOnly, f.path)
	}
	if id >= f.maxCapacity {
		return fmt.Errorf("%w: id %d, maximum capacity %d (%s)", ErrCapacityExceeded, id, f.maxCapacity, f.path)
	}
	if id < f.capacity {
		return nil
	}
	// O piso é defesa: com o File aberto a capacidade nunca é zero, mas um laço
	// de dobra que partisse de zero não terminaria.
	next := max(uint64(f.capacity), capacityFloor)
	for next <= uint64(id) {
		next *= 2
	}
	if next > capacityLimit {
		return fmt.Errorf("%w: id %d needs capacity %d, format limit %d (%s)", ErrCapacityExceeded, id, next, uint64(capacityLimit), f.path)
	}
	return f.grow(uint32(next))
}

// Msync força as páginas sujas para disco. File fechado devolve ErrClosed.
func (f *File) Msync() error {
	if f.region == nil {
		return fmt.Errorf("%w: Msync on %s", ErrClosed, f.path)
	}
	return f.region.Msync()
}

// SizeBytes devolve o tamanho do arquivo mapeado, zero no File fechado.
func (f *File) SizeBytes() int64 {
	if f.region == nil {
		return 0
	}
	return f.region.Size()
}

// Close libera o mapeamento. É idempotente: a segunda chamada, ou a chamada
// sobre um File que um crescimento já fechou, devolve nil.
func (f *File) Close() error {
	if f.region == nil {
		return nil
	}
	err := f.region.Close()
	f.region = nil
	f.capacity = 0
	return err
}

// store grava uma palavra, passando pelo seam de ordem de escrita.
func (f *File) store(off int64, v uint64) {
	if writeTrace != nil {
		writeTrace(off, v)
	}
	f.region.AtomicStore64(off, v)
}

// grow reescreve o arquivo com a capacidade nova (ver os três desfechos em
// EnsureCapacity).
func (f *File) grow(newCapacity uint32) error {
	if err := build(f.path, f.header, newCapacity, f.region, f.capacity); err != nil {
		return err
	}
	// Daqui em diante o arquivo em path é o novo, e a região antiga sai de cena
	// seja qual for o desfecho: primeiro a reabertura, depois o fsync do
	// diretório, para que uma falha no fsync não deixe o File escrevendo no
	// inode antigo.
	old := f.region
	r, err := openRegion(f.path, fileSize(f.header.BucketCount, newCapacity))
	if err != nil {
		f.region = nil
		f.capacity = 0
		return errors.Join(fmt.Errorf("metricwindow: reopen %s after growth: %w", f.path, err), old.Close())
	}
	f.region = r
	f.capacity = newCapacity
	var errs []error
	if err := old.Close(); err != nil {
		errs = append(errs, fmt.Errorf("metricwindow: close previous mapping of %s: %w", f.path, err))
	}
	if err := atomicfile.SyncDir(filepath.Dir(f.path)); err != nil {
		errs = append(errs, fmt.Errorf("metricwindow: growth of %s may not be durable: %w", f.path, err))
	}
	return errors.Join(errs...)
}

// identityOff é o offset da identidade do id, com as checagens que Identity
// documenta.
func (f *File) identityOff(id uint32) int64 {
	if f.region == nil {
		panic(fmt.Sprintf("metricwindow: use of closed file %s", f.path))
	}
	if id >= f.capacity {
		panic(fmt.Sprintf("metricwindow: id %d outside capacity %d (%s)", id, f.capacity, f.path))
	}
	return headerSize + int64(id)*identitySize
}

// bucketOff é o offset do balde do id no slot, com as mesmas checagens.
func (f *File) bucketOff(slot, id uint32) int64 {
	if f.region == nil {
		panic(fmt.Sprintf("metricwindow: use of closed file %s", f.path))
	}
	if slot >= f.header.BucketCount {
		panic(fmt.Sprintf("metricwindow: slot %d outside bucket count %d (%s)", slot, f.header.BucketCount, f.path))
	}
	if id >= f.capacity {
		panic(fmt.Sprintf("metricwindow: id %d outside capacity %d (%s)", id, f.capacity, f.path))
	}
	return columnOff(f.capacity, slot) + int64(id)*bucketSize
}

// columnOff é o início da coluna do slot numa capacidade.
func columnOff(capacity, slot uint32) int64 {
	c := int64(capacity)
	return headerSize + c*identitySize + int64(slot)*c*bucketSize
}

// fileSize é o tamanho do arquivo com a forma e a capacidade dadas.
func fileSize(bucketCount, capacity uint32) int64 {
	return columnOff(capacity, bucketCount)
}

// validateHeader recusa a forma que não produz arquivo com leitura possível.
func validateHeader(h Header) error {
	if h.BucketCount == 0 {
		return errors.New("metricwindow: bucket count must be positive")
	}
	if h.BucketWidthMs <= 0 {
		return fmt.Errorf("metricwindow: bucket width must be positive, got %d ms", h.BucketWidthMs)
	}
	return nil
}

// roundCapacity arredonda para potência de 2 com piso capacityFloor.
func roundCapacity(minCapacity uint32) (uint32, error) {
	c := uint64(capacityFloor)
	for c < uint64(minCapacity) {
		c *= 2
	}
	if c > capacityLimit {
		return 0, fmt.Errorf("%w: minimum capacity %d exceeds format limit %d", ErrCapacityExceeded, minCapacity, uint64(capacityLimit))
	}
	return uint32(c), nil
}

// validCapacity diz se a capacidade é uma que este pacote escreveria.
func validCapacity(c uint32) bool {
	return c >= capacityFloor && c <= capacityLimit && c&(c-1) == 0
}

// encodeHeader grava o cabeçalho na página inteira b, zerando o resto.
func encodeHeader(b []byte, h Header, capacity uint32) {
	clear(b)
	copy(b[0:4], fileMagic[:])
	binary.LittleEndian.PutUint32(b[4:8], formatVersion)
	binary.LittleEndian.PutUint32(b[8:12], h.BucketCount)
	binary.LittleEndian.PutUint32(b[12:16], 0) // reserved
	binary.LittleEndian.PutUint64(b[16:24], uint64(h.BucketWidthMs))
	binary.LittleEndian.PutUint32(b[24:28], h.Source)
	binary.LittleEndian.PutUint32(b[28:32], capacity)
	binary.LittleEndian.PutUint32(b[32:36], crc32.Checksum(b[:headerFieldsSize], castagnoli))
}

// decodeHeader lê o cabeçalho de b (o começo do arquivo) e confere que o
// arquivo tem EXATAMENTE size bytes para a forma declarada. mmap.Open não
// arredonda tamanho (mapeia max(tamanho do arquivo, mínimo pedido)), então o
// tamanho que este pacote escreve é o tamanho em disco, byte a byte. Maior que
// o declarado não é inofensivo: é o sinal de um arquivo que não saiu do build
// — um tmp sujo reaproveitado, por exemplo —, e o crescimento seguinte
// mapearia essa cauda. Todo motivo de recusa embrulha ErrCorruptHeader.
func decodeHeader(b []byte, size int64) (Header, uint32, error) {
	if len(b) < headerFieldsSize+4 || size < headerSize {
		return Header{}, 0, fmt.Errorf("%w: file has %d bytes, header needs %d", ErrCorruptHeader, size, headerSize)
	}
	if [4]byte(b[0:4]) != fileMagic {
		return Header{}, 0, fmt.Errorf("%w: magic %q, expected %q", ErrCorruptHeader, b[0:4], fileMagic[:])
	}
	if got, want := binary.LittleEndian.Uint32(b[32:36]), crc32.Checksum(b[:headerFieldsSize], castagnoli); got != want {
		return Header{}, 0, fmt.Errorf("%w: crc %#08x, computed %#08x", ErrCorruptHeader, got, want)
	}
	if v := binary.LittleEndian.Uint32(b[4:8]); v != formatVersion {
		return Header{}, 0, fmt.Errorf("%w: version %d, expected %d", ErrCorruptHeader, v, formatVersion)
	}
	h := Header{
		BucketCount:   binary.LittleEndian.Uint32(b[8:12]),
		BucketWidthMs: int64(binary.LittleEndian.Uint64(b[16:24])),
		Source:        binary.LittleEndian.Uint32(b[24:28]),
	}
	if err := validateHeader(h); err != nil {
		return Header{}, 0, fmt.Errorf("%w: %w", ErrCorruptHeader, err)
	}
	capacity := binary.LittleEndian.Uint32(b[28:32])
	if !validCapacity(capacity) {
		return Header{}, 0, fmt.Errorf("%w: capacity %d is not a power of two in [%d, %d]", ErrCorruptHeader, capacity, capacityFloor, uint64(capacityLimit))
	}
	if want := fileSize(h.BucketCount, capacity); size != want {
		return Header{}, 0, fmt.Errorf("%w: file has %d bytes, header declares %d", ErrCorruptHeader, size, want)
	}
	return h, capacity, nil
}

// readHeader lê o cabeçalho de path pelo descritor, sem mapear: a abertura
// decide entre adotar e recriar ANTES de mexer no arquivo. Arquivo ausente
// devolve o erro do os (errors.Is(err, os.ErrNotExist)).
func readHeader(path string) (Header, uint32, error) {
	fd, err := os.Open(path)
	if err != nil {
		return Header{}, 0, err
	}
	defer func() { _ = fd.Close() }()
	st, err := fd.Stat()
	if err != nil {
		return Header{}, 0, fmt.Errorf("metricwindow: stat %s: %w", path, err)
	}
	buf := make([]byte, headerFieldsSize+4)
	if _, err := io.ReadFull(fd, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, 0, fmt.Errorf("%w: %s: file has %d bytes, header needs %d", ErrCorruptHeader, path, st.Size(), headerSize)
		}
		return Header{}, 0, fmt.Errorf("metricwindow: read header of %s: %w", path, err)
	}
	h, capacity, err := decodeHeader(buf, st.Size())
	if err != nil {
		return Header{}, 0, fmt.Errorf("%s: %w", path, err)
	}
	return h, capacity, nil
}

// removeLeftoverTmp remove o tmp de uma reescrita interrompida. É a ÚNICA
// garantia de que build parte de um tmp vazio: mmap.Open reaproveita arquivo
// existente com o tamanho e o conteúdo que ele tiver, e um tmp grande e sujo
// viraria lixo nos ids novos do arquivo reescrito.
func removeLeftoverTmp(path string) error {
	tmp := path + tmpSuffix
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("metricwindow: remove leftover %s: %w", tmp, err)
	}
	return nil
}

// build escreve em path, por tmp → msync → rename, um arquivo com o cabeçalho
// h e a capacidade dada. Com src não nil, copia a coluna de identidade e cada
// coluna de balde de src (na capacidade srcCapacity) para o endereço novo; sem
// src, o conteúdo é zero (criação e reset).
//
// O fsync do diretório NÃO é daqui: ele vem depois de o chamador reabrir o
// arquivo novo, para que uma falha nele nunca deixe o dono no arquivo antigo.
// Erro devolvido por build é sempre anterior ao rename ou do próprio rename, e
// deixa path como estava.
func build(path string, h Header, capacity uint32, src *mmap.Region, srcCapacity uint32) error {
	if err := removeLeftoverTmp(path); err != nil {
		return err
	}
	tmp := path + tmpSuffix
	dst, err := openRegion(tmp, fileSize(h.BucketCount, capacity))
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metricwindow: create %s: %w", tmp, err)
	}
	dst.WithBytes(0, headerSize, func(b []byte) { encodeHeader(b, h, capacity) })
	if src != nil {
		copyRange(dst, src, headerSize, headerSize, int64(srcCapacity)*identitySize)
		for slot := range h.BucketCount {
			copyRange(dst, src, columnOff(capacity, slot), columnOff(srcCapacity, slot), int64(srcCapacity)*bucketSize)
		}
	}
	if err := dst.Msync(); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("metricwindow: %w", err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metricwindow: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metricwindow: rename %s: %w", filepath.Base(tmp), err)
	}
	return nil
}

// copyRange copia n bytes de src[srcOff:] para dst[dstOff:], em blocos de
// copyChunk. As duas regiões são objetos distintos, então o WithBytes aninhado
// não reentra no RWMutex de nenhuma delas.
func copyRange(dst, src *mmap.Region, dstOff, srcOff, n int64) {
	for done := int64(0); done < n; done += copyChunk {
		k := int(min(copyChunk, n-done))
		src.WithBytes(int(srcOff+done), k, func(from []byte) {
			dst.WithBytes(int(dstOff+done), k, func(to []byte) { copy(to, from) })
		})
	}
}
