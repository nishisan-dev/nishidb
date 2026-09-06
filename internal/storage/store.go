// Package storage é a fachada do subsistema de storage do NishiDB: orquestra
// segments, índices e dicionários atrás de uma API única (spec §15A do
// design). Checkpoint/manifest são a S11; recovery é a S12.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/query/spill"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/storage/versions"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidArgument e ErrWriterFailed são os MESMOS valores declarados em
// internal/types, reexportados para a API deste pacote: errors.Is casa pelos
// dois caminhos, e o materializer (mais o dublê de teste dele) os classifica
// sem importar esta implementação.
var (
	ErrInvalidArgument = types.ErrInvalidArgument

	// ErrDatasetClosed indica operação sobre um Store já fechado. É ERRO, e
	// não panic, porque o chamador (ingestão, query) tem política para erro e
	// não tem para panic.
	ErrDatasetClosed = errors.New("storage: dataset fechado")

	ErrWriterFailed = types.ErrWriterFailed

	// ErrSnapshotExpirado indica consulta cujo universo o dataset não consegue
	// mais reconstruir: o journal de versões estourou o teto de memória e as
	// versões que ela veria foram descartadas.
	//
	// É ERRO, e não uma degradação silenciosa para a versão corrente, porque
	// as duas respostas são indistinguíveis para o cliente e só uma delas é
	// verdadeira. Um resultado que mistura estados sem avisar é o único
	// desfecho que o isolamento não pode ter — quem pediu isolamento precisa
	// saber quando não o recebeu.
	ErrSnapshotExpirado = errors.New("storage: snapshot expirado: o journal de versões estourou o teto de memória")
)

// ErrDatasetCorrupt indica dataset que a recovery não consegue recuperar
// sozinha: manifest ausente com records, manifest ilegível, segmento do
// manifest sumido, buraco na cadeia de segmentos ou ponto S incoerente com
// os arquivos. O rebuild total é fase futura — até lá o Open recusa em vez
// de adivinhar, porque toda alternativa (truncar, pular, recriar) descarta
// dados duráveis em silêncio.
var ErrDatasetCorrupt = errors.New("storage: dataset corrompido")

// ErrCompactionBusy é o que CompactaUmaVez devolve quando já existe uma
// passada em voo — do laço automático (IniciaCompaction de pé) ou de outra
// chamada manual concorrente (Task B5, POST /v1/compact). Não é retentável
// pelo MESMO chamador em loop apertado: é a restrição estrutural do godoc de
// IniciaCompaction (duas varreduras concorrentes são PROIBIDAS) se
// manifestando para quem pede uma passada avulsa em vez de ligar o laço.
var ErrCompactionBusy = errors.New("storage: compaction já em voo")

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

// Modo é a engine do dataset. Ele é DURÁVEL: viaja numa seção crítica do
// manifest, e abrir um dataset com o modo errado é recusado em vez de
// tentado. O modo de falha que isso impede não é índice incompleto — é ler um
// log append-only de eventos como se fosse estado corrente, com cada entidade
// aparecendo dezenas de vezes e o CRC batendo em todas.
type Modo uint8

const (
	// ModoMaterializado é o dataset do produto: uma linha por entidade,
	// deduplicada pela primary e atualizada por merge. É o zero value, e é
	// por isso que todo dataset já em disco continua abrindo sem migração.
	ModoMaterializado Modo = 0
	// ModoHistorico é a tabela irmã append-only: uma linha por evento, sem
	// key, sem dedup e sem update. Ver o godoc de Apenda.
	ModoHistorico Modo = 1
)

func (m Modo) String() string {
	if m == ModoHistorico {
		return "historico"
	}
	return "materializado"
}

// Options configura a abertura de um Store.
type Options struct {
	Dir           string
	SegmentSize   int64  // default 512 MiB; teto validado pelo segment
	EstimatedRows uint64 // pre-sizing da primary (piso 1<<20 slots)
	// CamposIndexados são os campos com índice por segmento, por ID.
	//
	// É UM par, e não mais os quatro campos de dois motores (BitmapFields,
	// BitmapFieldNames, HashFields, HashFieldNames): com um motor só, os dois
	// grupos alimentavam o MESMO índice, e o Store abria a união deles. Um par
	// de campos que só sabe declarar o mesmo conjunto duas vezes é convite ao
	// dia em que alguém preenche um e esquece o outro.
	CamposIndexados []types.FieldID
	// CamposIndexadosPorNome são os mesmos campos por NOME, resolvidos pelo
	// dicionário do próprio dataset durante o Open (criando a entry se
	// necessário). Existe porque os FieldIDs nascem de schema.Compile, que
	// precisa do dicionário — e o dicionário vive dentro do Store: exigir
	// IDs aqui obrigaria a abrir o dataset sem índices, compilar, fechar e
	// reabrir. Os dois campos se somam (união).
	CamposIndexadosPorNome []string

	// Modo é a engine do dataset (ver o tipo). O default — o zero value — é o
	// dataset materializado, e é isso que faz todo dataset já em disco
	// continuar abrindo sem migração nenhuma.
	Modo Modo

	// DictCompartilhado faz este Store USAR o dicionário de campos de outro,
	// em vez de abrir o seu.
	//
	// Existe para a tabela histórica, e a razão é a identidade das colunas: o
	// payload gravado nela é o mesmo que o patch do dataset base montou,
	// endereçado por FieldID. Dois dicionários independentes atribuiriam ids
	// pelo mesmo algoritmo determinístico e coincidiriam — até o primeiro
	// campo autodescoberto, o primeiro teto de campos alcançado num só dos
	// dois, ou a primeira recriação de um deles. A partir daí a mesma coluna
	// teria ids diferentes nas duas tabelas e o payload decodificaria errado,
	// sem erro e sem CRC quebrado.
	//
	// Quem passa o dicionário CONTINUA dono dele: este Store não o fecha. A
	// consequência operacional fica escrita — o dataset que compartilha não
	// abre sozinho, e quem o abrir precisa abrir o dono primeiro.
	DictCompartilhado *fielddict.Dict

	// ColunasDerivadas são os nomes declarados em `extract`. Chegam como
	// STRING para não fazer o storage depender do schema: o que ele precisa
	// saber é só se o nome já está no dicionário.
	ColunasDerivadas []string
	SyncEveryPut     bool // durability=sync (testes/config)
	// TTLAtivo liga a expiração: o dataset declarou ao menos uma regra. Sem
	// ela nem os índices são abertos — o dataset não paga dois mmaps para não
	// fazer nada.
	TTLAtivo bool
	// DuracaoDeFundo é a duração da regra SEM `when`, ou zero quando não há.
	//
	// Ela vale para a linha cuja entrada em prazos.idx é DESCONHECIDA — a que
	// foi materializada antes de as regras existirem. É o que permite adotar
	// as regras num dataset já populado sem que ele fique imortal da noite
	// para o dia, e é o que faz a migração da forma escalar não mudar
	// comportamento nenhum.
	DuracaoDeFundo time.Duration
	// SnapshotMaxBytes é o teto de heap do journal de versões — a memória que
	// o isolamento de leitura pode reter enquanto consultas estão abertas.
	//
	// Zero usa o padrão. Estourado o teto, é a CONSULTA mais antiga que é
	// invalidada, nunca a ingestão que é barrada: o dataset existe para
	// ingerir, e uma consulta recusada com código próprio é um resultado que o
	// cliente sabe tratar.
	SnapshotMaxBytes int64
	// CampoDeEscrita é o NOME do campo de sistema que carrega o instante da
	// escrita, resolvido pelo dicionário do próprio dataset durante o Open.
	// Obrigatório quando TTLAtivo: é dele que o applyRecord tira o valor,
	// tanto no Put quanto no replay do tail, e é por isso que o índice de
	// expiração é reconstruível depois de um crash.
	//
	// Nome e não FieldID pela mesma razão do CamposIndexadosPorNome: os ids nascem
	// de schema.Compile, que precisa do dicionário — e o dicionário vive
	// dentro do Store. Exigir o id aqui obrigaria a abrir o dataset sem
	// expiração, compilar, fechar e reabrir.
	CampoDeEscrita string
	// CampoDePrazo é o NOME do campo de sistema que carrega a DURAÇÃO de
	// ociosidade da entidade, em millis. Obrigatório quando TTLAtivo, e pela
	// mesma razão do CampoDeEscrita: os dois são lidos do payload.
	CampoDePrazo string
	// CompilaContadores devolve as definições dos contadores mantidos na
	// escrita (o bloco `counters:` do YAML) compiladas contra o dicionário.
	//
	// É callback, e não lista, por uma dependência circular que nenhuma ordem
	// de chamadas desfaz: as definições precisam do schema compilado, o schema
	// precisa do dicionário que o Open cria, e o replay da recovery — que
	// roda DENTRO do Open — precisa das definições, porque o applyRecord
	// transiciona os contadores. O Open o chama com o dicionário já com os
	// campos de sistema e indexados registrados, ANTES do ramo de recovery; é
	// dentro dele que o chamador compila o schema (e o guarda para o motor de
	// consulta).
	//
	// Nil é "sem contadores" — e um dataset cujo manifest declara contadores
	// é RECUSADO nesse caso: abrir sem as definições deixaria o vetor parado
	// enquanto o Put anda.
	CompilaContadores func(dict *fielddict.Dict) ([]counter.Definicao, error)
	// Log é o logger estruturado do §95. Quando nil, o Store escreve em
	// stderr no formato texto — que é o comportamento que os log.Printf
	// tinham antes, e perder a divergência de recovery só porque o chamador
	// não injetou logger seria trocar observabilidade por cerimônia.
	Log *logging.Logger
	// Dataset é o nome lógico, usado como label de métrica e campo de log.
	// Vazio herda o nome do diretório.
	Dataset string
	// Metrics, quando presente, recebe as séries do §92 que só o storage
	// pode medir (duração e último sucesso do checkpoint). Nil desliga a
	// instrumentação sem desligar nada mais.
	Metrics *metrics.Registry
	// traceRecovery é o seam de teste do FIM do recoverFrom. Nil em
	// produção, e INEXPORTADO de propósito: ninguém de fora do pacote
	// consegue instalá-lo, então ele não acrescenta superfície ao contrato
	// do Store — a única coisa que muda para quem chama Open é nada.
	//
	// Mora aqui, e não num setter como o cpTrace, porque a recovery roda
	// DENTRO do Open: quando o Store existe para o chamador, o Checkpoint
	// incondicional já recarregou os espelhos em valor absoluto e apagou
	// justamente o estado que se quer medir.
	traceRecovery func(*Store)
}

// Stats é o snapshot mínimo de estado do Store.
type Stats struct {
	RowCount uint64
	// SegmentsTotal conta segmentos de fato: o writer os acrescenta ao
	// rotacionar e o checkpoint os reconcilia com o ListSegments. Já foi
	// DERIVADO do ID corrente ("IDs monotônicos desde 1, sem remoção"),
	// premissa que a compaction do §53-55 quebra: o ID nunca regride, mas o
	// arquivo é aposentado, e derivar passaria a reportar como existente um
	// segmento já removido — justo no painel que serve para acompanhar a
	// remoção.
	SegmentsTotal     uint32
	CurrentSegmentID  uint32
	PrimaryLoadFactor float64
	OverlayLen        int
	// StorageBytes soma o tamanho dos arquivos de segmento. Não herda a
	// imprecisão do tally de liveness (débitos 38 e 45) porque não depende
	// dele: é tamanho de arquivo, medido pelo ListSegments. É a série que
	// antecede o ENOSPC — o §86 dá 2 a 3 dias de margem a 500 ev/s, curto
	// demais para descobrir pelo alerta do filesystem.
	//
	// Ressalva de FRESCOR, e ela é GRANDE: o tamanho é o do último
	// ListSegments, que só roda no checkpoint, então o valor fica atrás de
	// TUDO que foi escrito desde então — não só do crescimento do segmento
	// corrente, mas de segmentos inteiros já selados no intervalo. Medido com
	// SegmentSize de 8 KiB e 400 Puts sem checkpoint intermediário: 64 bytes
	// reportados contra 28.256 em quatro arquivos, subestimativa de ~440x.
	//
	// A direção é o que se garante, e ela vale sempre: limite INFERIOR, nunca
	// superior. Errar para baixo numa série de disco é o lado seguro — errar
	// para cima faria o operador provisionar disco que não estava sendo
	// usado. Mas a MAGNITUDE do atraso é a do intervalo entre checkpoints, e
	// ela deixou de ser ilimitada: o débito 56 FECHOU, e o consumidor Kafka
	// passou a checkpointar por tempo além de por contagem, de modo que uma
	// ingestão ociosa não congela mais a série.
	//
	// Relistar o diretório dentro do Stats() tornaria o número exato ao custo
	// de um readdir por raspagem de métrica. Não foi o caminho escolhido
	// porque o atraso agora é o intervalo de checkpoint (§78, 30s no padrão),
	// ordens de grandeza menor que a margem até o disco encher.
	StorageBytes int64
	// LiveBytesUpperBound é a soma do tally por segmento. NÃO é contagem
	// exata: é limite SUPERIOR, pelo mesmo contrato de
	// checkpoint.SegmentInfo. O nome carrega a ressalva de propósito —
	// chamá-lo de LiveBytes convidaria a próxima revisão a tratá-lo como
	// exato e a dimensionar compaction por ele.
	LiveBytesUpperBound uint64
	// CompactionRunning é o 0/1 de nishidb_compaction_running (§92), e o que
	// ele afirma é que o LAÇO do §53-55 está de pé para este dataset — não
	// que há uma varredura em voo neste microssegundo. A escolha é medida: a
	// passada dura dezenas de milissegundos contra um cochilo de segundos, de
	// modo que um medidor de "varredura em voo" responderia 0 em quase toda
	// raspagem e o painel não distinguiria a compaction viva da desligada.
	//
	// Ele desce quando o laço termina, e o caso que importa não é o Close: é
	// a falha sticky do writer (§14.14), em que a goroutine para sozinha e o
	// disco volta a crescer com o processo de pé. O compaction.parou registra
	// o instante no log; esta série é o que um alerta consegue observar.
	CompactionRunning bool
	// CompactionBytesRead e CompactionBytesWritten são os dois CONTADORES do
	// §92, monotônicos por processo: somam o que as varreduras leram e o que
	// o copy-forward escreveu desde a abertura do dataset. A distância entre
	// eles é o disco devolvido, que é a pergunta do §86.
	//
	// São contadores e não medidores por causa da leitura que o operador faz:
	// o painel pede a TAXA de bytes movidos, e taxa não se calcula sobre um
	// número que zera a cada passada. Reabrir o dataset os zera, como zera
	// qualquer contador de processo — é a semântica que o Prometheus espera e
	// trata com `rate()`.
	// ExpiryRunning é o 0/1 do coletor do idle timeout; os três contadores
	// abaixo são monotônicos e respondem, juntos, "ele está trabalhando?".
	// Carencias importa isolada porque é a única coisa que acontece nos
	// primeiros minutos depois de ligar a expiração num dataset já
	// materializado — período em que Expiradas fica em zero por estar certo.
	ExpiryRunning          bool
	Expiradas              uint64
	Carencias              uint64
	ExpiryExaminadas       uint64
	CompactionBytesRead    int64
	CompactionBytesWritten int64
}

const defaultSegmentSize = 512 << 20

// Store é a fachada do storage.
//
// Concorrência: Put e Sync são exclusivos do writer único (não são
// thread-safe entre si); GetByKey/GetByRowID/ScanLive/Lookup/Sequence/
// Stats são seguros para leitores concorrentes com o writer.
type Store struct {
	opts Options

	dict *fielddict.Dict
	// dictProprio diz se este Store abriu o dicionário e portanto o fecha. É
	// false no dataset que recebeu DictCompartilhado: fechar o dicionário de
	// outro derrubaria a ingestão do dono no meio de um LookupOrCreate.
	dictProprio bool
	segW        *segment.Writer
	segR        *segment.Reader
	prim        *primary.Index
	locs        *rowindex.Locations
	seqs        *rowindex.Sequences
	// escritas é nil quando o dataset não declara expiração: sem ela não há
	// o que indexar, e abrir o arquivo custaria mmap por nada.
	escritas *rowindex.Escritas
	prazos   *rowindex.Prazos
	// agenda é o índice de QUANDO cada linha vence, e acompanha o escritas:
	// nil junto com ele. Ela não é fonte de verdade de nada — prazoVencido
	// continua lendo escritas e prazos —, é o que permite ao coletor
	// perguntar pelo próximo vencimento em vez de varrer o dataset atrás
	// dele.
	agenda         *rowindex.Agenda
	duracaoDeFundo time.Duration
	campoDeEscrita types.FieldID
	campoDePrazo   types.FieldID
	// expiraBuf e tombBuf são do Expira, owned pelo wmu como o compactBuf:
	// reter os dois evita realocar o record lido e o payload da lápide a cada
	// expiração, e a varredura expira em rajada.
	expiraBuf  []byte
	tombBuf    []byte
	expiradas  atomic.Uint64
	carencias  atomic.Uint64
	examinadas atomic.Uint64
	// agendaReconstruida registra que esta abertura teve de construir a
	// agenda em vez de adotar a que estava em disco. É o que permite ao teste
	// afirmar a NÃO reconstrução — sem isso, "adotou" e "reconstruiu e
	// chegou ao mesmo resultado" seriam indistinguíveis.
	agendaReconstruida bool

	// contadores são os contadores mantidos na escrita, na ordem declarada;
	// vazio no dataset sem `counters:`. Transicionados no passo 7c do
	// applyRecord, sob o wmu; lidos pela tabela virtual e pela auditoria sem
	// lock do Store.
	contadores []*counter.Contador
	// contadoresARecriar são os que a abertura decidiu reconstruir; a
	// varredura roda depois do ramo de recovery e a lista é esvaziada.
	contadoresARecriar []pendenciaDeContador
	// contadoresConstruidos conta as reconstruções desta abertura — o
	// observável que separa "adotou" de "reconstruiu e chegou ao mesmo
	// resultado", como agendaReconstruida.
	contadoresConstruidos int
	// audTrace é o seam de teste da auditoria dos contadores: chamado depois
	// de cada linha varrida. Nil em produção.
	audTrace func(linha types.RowID)
	// audAntesDaCopia é o seam entre abrir o snapshot e copiar os totais —
	// dentro do wmu, no código certo. Existe porque a janela que ele expõe é
	// de microssegundos e nenhuma ingestão concorrente a acerta por sorte: é
	// por ele que o teste prova que a cópia e o snapshot são do MESMO instante.
	audAntesDaCopia func()
	// Ciclo de vida do laço de aposentadoria de grupos vazios, no molde do
	// coletor de expiração: apGrupMu guarda o par cancela/emVoo, apGrupAtiva é
	// o medidor observável.
	apGrupMu      sync.Mutex
	apGrupCancela context.CancelFunc
	apGrupEmVoo   sync.WaitGroup
	apGrupAtiva   atomic.Bool

	// Ciclo de vida do coletor, no molde do da compaction: expMu guarda o
	// par cancela/emVoo, expAtiva é o medidor observável e expEmVoo é o que
	// o Close aguarda antes do quiesce.
	expMu      sync.Mutex
	expCancela context.CancelFunc
	expEmVoo   sync.WaitGroup
	expAtiva   atomic.Bool
	// agora é o relógio da expiração. Campo, e não time.Now direto, porque um
	// teste de prazo precisa avançar o tempo sem dormir.
	agora func() time.Time

	// wips são os índices de segmento MUTÁVEIS — um por campo indexado, todos
	// do segmento corrente. Mapa e não slice porque o applyRecord endereça por
	// FieldID, uma vez por campo do payload.
	//
	// A LEITURA DO MAPA PELO CAMINHO DE ESCRITA (aplicaIndiceSets) dispensa o
	// selMu, e a razão é que ele já tem o wmu na mão: as duas únicas escritas
	// deste campo — abreWIPsDoCorrente e fechaWIPs — só acontecem sob o wmu, no
	// aposAppend, ou no Open, antes de existir concorrência. Contra a CONSULTA,
	// que não toma o wmu por desenho, o que separa leitura de escrita é o selMu.
	wips map[types.FieldID]*segindex.WIP
	// wipsSeg é o segmento DONO dos WIPs publicados em wips, e existe porque a
	// consulta decide "este segmento é o corrente?" com um snapshot do curSegID
	// que a rotação pode ter tornado obsoleto. Sem a identidade, uma consulta
	// que atravessasse a rotação inteira (Swap → fechaWIPs → Sela →
	// abreWIPsDoCorrente) chegava ao segmento ANTIGO acreditando que ele era o
	// corrente e recebia ok=true com os WIPs do segmento NOVO — o .sidx
	// recém-selado nunca era consultado e nenhuma fonte de enumeração cobria o
	// segmento: FALSO NEGATIVO, a única coisa que a revalidação do §44 não
	// conserta. Com a identidade, o descompasso cai para o caminho selado, que
	// responde pelo .sidx ou manda enumerar.
	//
	// Zero significa "não há WIP publicado" (o segmento 0 não existe depois do
	// Open — ver aposAppend). Escrita junto com o mapa, sob o mesmo selMu.
	wipsSeg uint32
	// selados são os índices imutáveis, abertos SOB DEMANDA e mantidos
	// mapeados. Não há LRU: o mapeamento somente-leitura não retém descritor
	// (open → mmap → close), e 500 VMAs num teto padrão de 65.530 não é
	// pressão.
	selados map[uint32]*segindex.Selado
	// selGeracao conta as EVIÇÕES do mapa de selados, e existe por causa de uma
	// janela que só passou a ser alcançável quando o laço da §2.4 subiu: o
	// garanteSelado abre o arquivo FORA de lock nenhum (o crc32c de um índice
	// grande não pode rodar sob o selMu, que é o lock da rotação), então entre
	// abrir e publicar cabe uma reescrita inteira da reconstrução. Sem a geração,
	// o mapeamento VELHO era publicado depois da evição e o segmento respondia
	// pelo índice anterior — sem a seção do campo novo, portanto ENUMERANDO — até
	// a próxima abertura do dataset, ainda prendendo o inode substituído.
	//
	// Ela é um contador GLOBAL e não por segmento, e a diferença é só de custo:
	// uma evição de qualquer segmento faz um mapeamento em voo de outro ser
	// descartado, o que custa reabrir o arquivo na consulta seguinte. Errar para
	// esse lado é o barato; a alternativa (um contador por segmento) seria um
	// segundo mapa a podar contra a lista de segmentos vivos.
	//
	// Protegida pelo selMu, como os dois mapas.
	selGeracao uint64
	// selMu protege os DOIS mapas de índice por segmento — selados e wips —, e
	// é FOLHA na ordem de locks: nada tomado sob ele toma wmu ou aposMu. Ele
	// existe porque os mapas são escritos pela aposentadoria e pela rotação e
	// lidos pela consulta, que são goroutines diferentes.
	//
	// UM lock para os dois, e não dois: a consulta monta uma fonte por segmento
	// numa passada só, e um segundo mutex apenas acrescentaria uma ordem a
	// respeitar sem separar contenção nenhuma — os dois mapas são escritos pelo
	// mesmo evento, a rotação de segmento.
	selMu sync.RWMutex
	// camposIndexados é o conjunto ordenado, resolvido no Open e IMUTÁVEL na
	// vida do Store. É ele que responde Indexado e CamposIndexados, e a
	// imutabilidade é o que permite responder aos dois sem lock.
	camposIndexados []types.FieldID
	// chaveIndiceBuf é o buffer da chave canônica do índice por segmento,
	// reusado a cada campo de cada evento. É do Store, e writer-only, pelo
	// mesmo motivo dos outros buffers do caminho quente: uma alocação por campo
	// por evento é o que separa o gate de zero alocação de uma regressão.
	chaveIndiceBuf []byte

	nextRowID atomic.Uint64
	rowCount  atomic.Uint64
	// rowIDBase é a marca-d'água de retenção do dataset histórico: rowId
	// ABAIXO dela já não é visível, e os bytes dele ou já saíram do disco (o
	// segmento foi aposentado) ou vão sair na próxima passada.
	//
	// Uma marca-d'água, e não a limpeza entrada por entrada do locations.idx,
	// porque a purga é de SEGMENTO: zerar milhões de entradas sob o wmu a cada
	// passada disputaria o lock com a ingestão por um trabalho proporcional ao
	// que foi apagado, quando o que mudou é um número só. Ela é durável no
	// manifest — sem isso, o restart devolveria à consulta as linhas que a
	// retenção já tinha tirado dela.
	//
	// Zero no dataset materializado, onde nada a move: lá a visibilidade é
	// decidida pela location e pelo prazo da linha.
	rowIDBase atomic.Uint64

	// epocaPub é o contador de PUBLICAÇÕES do writer, e é o relógio lógico do
	// isolamento: um snapshot nasce carregando o valor corrente, e enxerga
	// tudo publicado até ele e nada do que veio depois. Monotônico e escrito
	// só sob o wmu — é o mesmo lock que serializa o publish, então a época e a
	// location que ela nomeia nunca saem de ordem.
	epocaPub atomic.Uint64
	// snaps conta as consultas abertas por época. É ele que responde "ainda há
	// leitor atrás desta versão?", que é a pergunta de que dependem o journal,
	// a poda e a expiração oportunista da compaction.
	snaps *registroDeSnapshots
	// vers guarda as locations anteriores enquanto algum leitor as alcança.
	vers *versions.Cadeias
	// snapsExpirados conta as consultas invalidadas pelo teto do journal. É
	// contador, e não medidor, porque a pergunta é "isto ACONTECEU?" — um
	// medidor que voltasse a zero esconderia a recusa entre dois scrapes.
	snapsExpirados atomic.Uint64

	// Estado da goroutine de retenção do dataset histórico, no mesmo molde do
	// da compaction: retMu guarda o cancelamento, retPassadaMu serializa as
	// passadas (duas concorrentes disputariam a mesma aposentadoria e a
	// segunda encontraria o segmento já renomeado), retAtiva é o medidor que
	// o Stats publica e retBuf é o destino reusado dos preads da busca
	// binária.
	//
	// O retPassadaMu serializa MAIS que as passadas: ele é também a barreira
	// entre a expulsão da janela e a pendência de reconstrução, e é sob ele que
	// o tamanhoExpulso vive. Ver o godoc de janelaDeSegmentos.
	retMu        sync.Mutex
	retPassadaMu sync.Mutex
	// tamanhoExpulso lembra, POR SEGMENTO, quantos bytes o .sidx dele ocupava
	// no instante em que a janela o expulsou.
	//
	// Existe porque o estimador da janela precisa ser MONÓTONO e "o maior .sidx
	// em disco" não é: a própria expulsão apaga arquivos, então apagar o maior
	// BAIXA a estimativa e readmite quem acabou de sair — o moinho, com "custo
	// zero" trocado por "custo subestimado". Para o segmento recém-expulso este
	// mapa não é estimativa nenhuma: é o tamanho que ele de fato teve.
	//
	// Em MEMÓRIA, e nunca em disco: um metadado a menos é uma janela de crash a
	// menos, que é a mesma escolha do primeiroRowIDDoSeguinte e da §2.5. O preço
	// é que um restart volta ao estimador conservador e paga UMA reconstrução
	// desperdiçada por segmento atípico antes de estabilizar de novo — limitado,
	// e barato ao lado de mais um arquivo durável a recuperar.
	//
	// Protegido pelo retPassadaMu. Podado na expulsão, contra a lista de
	// segmentos vivos: sem isso ele cresceria para sempre num dataset que
	// aposenta segmentos o dia inteiro.
	tamanhoExpulso map[uint32]int64
	retCancela     context.CancelFunc
	retEmVoo       sync.WaitGroup
	retAtiva       atomic.Bool
	retBuf         []byte

	// Ciclo de vida do laço de RECONSTRUÇÃO do índice por segmento (§2.4), no
	// molde do da compaction e do da retenção: recMu guarda o par
	// cancela/emVoo, recAtiva é o medidor observável e recEmVoo é o que o Close
	// aguarda antes do quiesce.
	//
	// Ele é goroutine PRÓPRIA, e não mais um trecho da passada de retenção como a
	// expulsão da janela, porque vale para os DOIS modos de dataset: o backfill de
	// um campo acrescentado ao schema (§2.6) acontece no materializado, onde não
	// há passada de retenção nenhuma.
	recMu      sync.Mutex
	recCancela context.CancelFunc
	recEmVoo   sync.WaitGroup
	recAtiva   atomic.Bool
	// recPassadaMu serializa as passadas de reconstrução, como o retPassadaMu faz
	// com as de retenção: duas concorrentes varreriam o mesmo segmento e
	// publicariam o mesmo arquivo duas vezes.
	//
	// Ele é o lock MAIS EXTERNO das duas cadeias que a passada percorre —
	// recPassadaMu → retPassadaMu (dentro da pendência) e recPassadaMu → vida
	// (dentro de cada reconstrução) —, e nada tomado sob os outros dois volta a
	// tomá-lo. É também ele que dá o happens-before do recPontoFixo, tocado só
	// dentro da passada.
	recPassadaMu sync.Mutex
	// Os contadores atômicos das séries do índice por segmento (§6.6) — sem
	// número no nome de propósito: contá-los aqui divergiria do bloco na
	// primeira série nova, e quem precisa da lista tem os campos logo abaixo.
	//
	// São atômicos, e não uma coleta que vá ler o disco no scrape, por uma razão
	// de LOCK e não de custo: os números de estado de segmento só existem sob o
	// retPassadaMu, que fica ACIMA da cadeia vida → wmu → aposMu. Uma raspagem
	// que fosse buscá-los pediria esse lock de fora da cadeia, e a inversão só
	// apareceria sob carga — que é exatamente quando o /metrics precisa
	// responder. Quem já segura o lock publica; o scrape lê átomo.
	//
	// segSelados/segCorrentes/segPendentes/segFora são os quatro estados, e eles
	// PARTICIONAM os segmentos do dataset: corrente é o único cujo índice é o
	// WIP; entre os selados, `fora` está deliberadamente sem índice (a janela o
	// expulsou), `pendente` está na fila e `selado` tem o .sidx que descreve o
	// conjunto indexado inteiro. A distinção entre os dois últimos é a razão de
	// a série existir — com um rótulo só, o gauge não responde "o laço está
	// avançando?", que é a única pergunta que uma migração faz.
	//
	// Escritos pelo SegmentosPendentesDeReconstrucao, que é o único lugar onde
	// os quatro são decididos pela mesma conta; ver o godoc de lá.
	segSelados   atomic.Int64
	segCorrentes atomic.Int64
	segPendentes atomic.Int64
	segFora      atomic.Int64
	// recFeitas conta os segmentos cujo índice a reconstrução PUBLICOU, e é
	// diferente do varreduras: aquele conta leituras de segmento (inclusive a da
	// abertura, e inclusive a que terminou sem posting nenhuma), este conta
	// trabalho concluído. A distância entre os dois é o que a fila-superconjunto
	// custa.
	recFeitas atomic.Int64
	// crcFalhas conta os .sidx que o crc32c reprovou, e ela é um DEGRAU — não uma
	// taxa.
	//
	// Sobe uma vez por ABERTURA do arquivo, e a abertura acontece uma vez por
	// segmento por processo: o garanteSelado PUBLICA no cache o mapeamento
	// reprovado (publicar mantém o sync.Once do crc cumprido; recusar obrigaria
	// toda consulta a remapear e revarrer o arquivo podre inteiro). Da segunda
	// consulta em diante o fonteIndexada acha o mapeamento no s.selados e volta
	// antes de reconferir nada. Só há incremento novo se o mapeamento for evictado
	// — reconstrução, aposentadoria — ou se o dataset for reaberto.
	//
	// A CONSEQUÊNCIA PARA QUEM OPERA, e ela é o oposto do que esta série já
	// afirmou de si mesma: um alerta por `rate()` dá um pico e se auto-resolve em
	// minutos, enquanto o segmento continua enumerando indefinidamente.
	//
	// E não há série a que redirecionar, o que é o limite honesto a registrar. O
	// gauge de estados é CEGO: o arquivo reprovado conta como `selado`, porque a
	// pendência lê a tabela de seções sem conferir crc (débito 103). O
	// segPorEnumeracao sobe, mas no MESMO balde de um segmento fora da janela, sem
	// rótulo que separe a causa. O sinal é o WARN indice.selado_corrompido; esta
	// série diz "quantos foram pegos desde a subida".
	crcFalhas atomic.Int64
	// segPorIndice e segPorEnumeracao contam os segmentos que cada Lookup
	// resolveu por cada caminho (§3.4). São a ÚNICA coisa que o chamador enxerga
	// da degradação: planner, executor e driver veem um iterador ascendente e
	// nada mais.
	segPorIndice     atomic.Int64
	segPorEnumeracao atomic.Int64
	// recPontoFixo é o que impede o MOINHO da fila-superconjunto.
	//
	// A fila é derivada do disco e é um superconjunto do trabalho real: um
	// segmento cujos records não trazem nenhum campo declarado continua PENDENTE
	// depois de reconstruído, porque o Escritor vazio não cria arquivo e
	// "ausente" é pendência. Sem memória, o laço o varreria de novo a cada
	// passada, para sempre — até 512 MiB de leitura por volta, e é isso que
	// separa "o laço não converge" de "o laço não tem o que fazer".
	//
	// O que se guarda é o conjunto de seções que o arquivo tinha DEPOIS da
	// reconstrução, e não um "já tentei": os records de um segmento selado são
	// imutáveis e a cobertura é determinística, então repetir a varredura daria
	// exatamente esse mesmo conjunto. Guardá-lo é o que torna a memória
	// AUTO-INVALIDÁVEL — se a janela expulsar o .sidx, o conjunto em disco deixa
	// de casar e o segmento é reconstruído, que é justamente o gatilho "janela
	// alargada" da §2.4. Um simples booleano transformaria aquele gatilho em
	// falso negativo permanente.
	//
	// Em MEMÓRIA e podado a cada passada contra a fila: o que sai da fila (porque
	// ficou completo, foi aposentado ou saiu da janela) sai daqui junto.
	recPontoFixo map[uint32][]types.FieldID
	// varreduras conta as leituras COMPLETAS de segmento feitas para construir
	// índice — a da abertura (o WIP do corrente) e as da reconstrução.
	//
	// É o observável do custo que decide esta parte do produto: a varredura é o
	// preço inteiro da operação, e "o laço convergiu" só se distingue de "o laço
	// está moendo" contando quantas ele fez. É também o que prova que os campos
	// pendentes de um segmento são resolvidos numa leitura só.
	varreduras atomic.Int64
	// retPorTempo e retPorTamanho contam os segmentos aposentados por cada
	// política desde a subida. Duas séries e não uma: qual das duas está
	// decidindo é a pergunta que separa "o prazo está sendo honrado" de "o
	// teto está engolindo o prazo".
	retPorTempo   atomic.Int64
	retPorTamanho atomic.Int64
	epoch         uint64 // último epoch de manifest gravado (writer-only)

	// curSegID espelha o segmento corrente para leitura concorrente em
	// Stats (segment.Writer não é thread-safe).
	curSegID atomic.Uint32
	// segCount/liveBytesAll/diskBytesAll espelham os AGREGADOS de segTally e
	// segSize, no mesmo padrão do curSegID acima e pelo mesmo motivo: o que
	// atravessa a fronteira writer→leitor não pode ser a estrutura, tem de
	// ser o número. Mapa Go não se lê sem sincronizar (é fatal error do
	// runtime, não corrida benigna), e sincronizar aqui significaria tomar o
	// wmu no Stats — que o Checkpoint segura do início ao fim, com histograma
	// que vai até 30s (o intervalo padrão do §78). Um Stats atrás desse lock
	// apagaria o /metrics e o /v1/stats exatamente sob a pressão que eles
	// existem para mostrar, que num milestone dedicado a tornar a curva de
	// disco visível (§92) é a regressão mais cara possível.
	//
	// Escrita WRITER-ONLY, sob wmu, nos mesmos pontos em que os mapas se
	// movem: applyRecord (nascimento de segmento e delta de liveness), a
	// semeadura da recovery (que precisa da MESMA base do manifest com que
	// semeia o tally, sob pena de a primeira baixa do replay furar o
	// agregado por baixo) e a reconciliação do checkpoint, que recarrega os
	// três com os valores absolutos.
	//
	// Custo no caminho quente, contado com precisão porque o número já saiu
	// errado uma vez: DUAS somas atômicas no Put que é update — a do record
	// que nasce e a da baixa do record que ele torna garbage — e UMA no Put
	// que só cria. O Put que estreia um segmento paga mais uma, no segCount.
	// Zero alocação nos três casos, então os gates de AllocsPerRun seguem
	// valendo.
	//
	// Os três são atômicos INDEPENDENTES, então um leitor pode pegar um
	// deles de antes e outro de depois do mesmo checkpoint. É aceito de
	// propósito: os campos que eles alimentam já são declaradamente limites
	// (inferior no disco, superior no vivo), e nenhuma decisão operacional
	// depende de os três descreverem o mesmo nanossegundo. Um snapshot
	// coerente exigiria publicar uma struct por ponteiro atômico, ou seja uma
	// alocação por Put, para servir uma raspagem de métrica.
	segCount     atomic.Uint32
	liveBytesAll atomic.Uint64
	diskBytesAll atomic.Int64
	// espelhosDivergentes conta as vezes em que o checkpoint ENCONTROU os
	// três fora dos mapas. Não é contador de falha do checkpoint: a recarga
	// conserta o valor e a ingestão segue. É contador de MENTIRA PUBLICADA —
	// enquanto durou, o /v1/stats e a nishidb_garbage_bytes descreveram um
	// dataset que não existe. Ver conferaEspelhos.
	espelhosDivergentes atomic.Uint64
	// stickyErr: primeiro erro pós-Append ou de durabilidade. Sem rollback
	// parcial, o writer fica FALHO até Close + reabertura com recovery.
	// Protegido por wmu.
	stickyErr error

	// vida protege o CICLO DE VIDA do Store: toda operação pública a toma em
	// leitura e desiste se `fechado`; o Close a toma em escrita, o que faz
	// dele um QUIESCE — ele só prossegue quando não há mais operação em voo, e
	// nenhuma nova entra depois.
	//
	// Sem isso, um Close concorrente com Put ou com leitura produzia PANIC (o
	// mmap fica desmapeado sob os pés da operação), e panic não é capturável
	// pela política de log-and-skip da ingestão — derruba o processo. O CLI
	// abre e fecha o dataset a cada comando, então o que era corrida rara
	// virou caminho normal.
	//
	// É RWMutex e não Mutex porque as leituras são concorrentes entre si por
	// contrato; o writer continua serializado pelo wmu, que é outro nível.
	vida    sync.RWMutex
	fechado bool

	// aposMu guarda a JANELA ENTRE RESOLVER UMA LOCATION E LER OS BYTES QUE
	// ELA NOMEIA. É o terceiro lock do Store, e é preciso dizer o que ele NÃO
	// é, porque os três se parecem: `vida` guarda o ciclo de vida (o quiesce
	// do Close), `wmu` serializa o writer, e este guarda o par (Get, Read)
	// contra a aposentadoria de um segmento acontecendo no meio dele.
	//
	// Antes dele, o que impedia a query de morrer era um ACIDENTE: o cache de
	// fds do segment.Reader não tem despejo, então o descritor que a própria
	// compaction abriu para varrer o segmento continuava aberto depois do
	// unlink, e o pread seguinte devolvia os bytes certos sobre um inode sem
	// nome. O preço desse acidente era o inode preso — os blocos nunca
	// voltavam ao filesystem enquanto o processo vivesse. Fechar o descritor
	// devolve os blocos E remove o acidente; esta barreira é o que põe algo
	// no lugar dele.
	//
	// INVARIANTE DE ORDEM, e ela é a parte perigosa: aposMu é sempre o lock
	// MAIS INTERNO. Put, Expira e os dois validadores da primary tomam wmu (ou
	// o RLock da primary) e só depois entram aqui. Nada tomado sob aposMu pode
	// tomar wmu — em particular, o podaSegmentoAposentado toma wmu e por isso
	// roda FORA da seção crítica da aposentadoria. Invertida, a ordem é
	// wmu→aposMu de um lado e aposMu→wmu do outro, que é deadlock.
	//
	// A seção crítica da aposentadoria é deliberadamente CURTA — fechar o
	// descritor e apagar o arquivo — e não cobre os dois Checkpoint, que
	// tomam wmu e podem levar dezenas de segundos. O rename também fica fora:
	// um descritor já aberto sobrevive a ele, porque o rename move o nome e
	// não o inode.
	aposMu sync.RWMutex

	// compCancela e compEmVoo são o ciclo de vida da goroutine de compaction
	// (§53-55, decisão D7). Ela NÃO se junta ao entra()/sai() pela duração da
	// passada: o Close toma vida.Lock(), que é quiesce, e uma varredura de
	// segmento leva minutos — o Close ficaria preso por todo esse tempo. O
	// desenho é o oposto: o Close CANCELA e AGUARDA a goroutine antes de
	// pedir o vida.Lock(), e cada trecho curto dela entra e sai do lock de
	// vida por conta própria.
	//
	// compMu protege o par contra Inicia e Para concorrentes, e é segurado
	// durante o Wait de propósito: sem isso um Inicia podia somar ao
	// WaitGroup que o Para está esperando, que é uso indevido do tipo.
	//
	// compMu NÃO decide mais (revisão da B5, achado Critical) se HÁ uma
	// passada em voo — só se o LAÇO existe. compCancela fica não-nil da
	// primeira à última hora do laço, dormindo ou varrendo, então checá-lo
	// para decidir 409 tornava a rota morta o tempo todo na config default
	// (storage.compaction.enabled ausente ⇒ true). Quem decide isso agora é
	// compPassadaMu, abaixo — um mutex PRÓPRIO, de propósito: compMu fica
	// preso do início ao fim de ParaCompaction (Lock → compCancela() →
	// compEmVoo.Wait(), que pode durar minutos com uma passada em
	// andamento), e nem o laço nem CompactaUmaVez podem arriscar bloquear
	// nele para reivindicar uma passada — um Lock bloqueante ali, na hora
	// errada, prenderia o próprio goroutine que ParaCompaction está
	// esperando via compEmVoo (deadlock).
	compMu      sync.Mutex
	compCancela context.CancelFunc
	compEmVoo   sync.WaitGroup

	// compPassadaMu protege o trio (compPassadaEmVoo, compManualEmVoo,
	// compEncerrando) — o single-flight FINO que decide, a cada instante,
	// se HÁ uma passada de verdade em andamento (ou se está proibido
	// começar uma nova). As três flags:
	//
	//   - compPassadaEmVoo: true SÓ durante a chamada real a
	//     passadaDeCompaction dentro de rodaCompaction (o laço automático).
	//   - compManualEmVoo: true durante a chamada real a passadaDeCompaction
	//     dentro de CompactaUmaVez (Task B5, POST /v1/compact).
	//   - compEncerrando: setada por ParaCompaction ANTES de compEmVoo.Wait()
	//     (revisão da B5, round 2 — Important). Enquanto true, NENHUM claim
	//     novo é aceito, nem do laço nem manual — é o veto que fecha a
	//     janela entre "compEmVoo pode estar em zero" e "uma reivindicação
	//     nova está prestes a somar 1 nele": sem ele, ParaCompaction podia
	//     decidir que o WaitGroup estava vazio e devolver o controle a
	//     Close() ENQUANTO uma chamada manual, já autorizada a prosseguir,
	//     ainda não tinha chamado Add — e Close() fecharia segR/segW debaixo
	//     dela. Nunca é resetada para false: a única chamadora real de
	//     ParaCompaction é Close(), que também não volta atrás, e reabrir a
	//     vaga depois do Wait() reabriria exatamente esta janela um passo
	//     adiante (ver o godoc de CompactaUmaVez, compaction.go).
	//
	// tentaClaimarPassada/liberaPassada (compaction.go) são os dois únicos
	// pontos que tocam este trio, e um Lock BLOQUEANTE neles é seguro — ao
	// contrário de compMu (nota acima), compPassadaMu NUNCA é mantido além
	// do tempo de checar as flags e (para a chamada manual) parear o
	// Add/Done de compEmVoo: nenhuma das duas funções o segura DURANTE a
	// passada em si (que é o trecho que pode levar minutos), então não
	// existe goroutine que fique presa nele por tempo que importe, e
	// portanto nenhum risco de ParaCompaction (via compEmVoo.Wait())
	// esperar por um goroutine bloqueado aqui.
	//
	// A exclusão é HERMÉTICA NOS DOIS SENTIDOS: com compManualEmVoo true, a
	// tentativa do laço (rodaCompaction) de reivindicar a rodada FALHA e ele
	// PULA — nunca sobe uma segunda varredura; com compPassadaEmVoo true,
	// CompactaUmaVez devolve ErrCompactionBusy na hora.
	//
	// compEmVoo.Add(1)/Done() da chamada MANUAL vivem DENTRO de
	// tentaClaimarPassada/liberaPassada — atômicos com o claim, na MESMA
	// seção crítica — e não como dois passos separados em CompactaUmaVez:
	// ver o godoc de tentaClaimarPassada para o porquê. É esse pareamento,
	// mais o veto de compEncerrando, que faz ParaCompaction (chamado pelo
	// Close) esperar de verdade por uma passada manual antes de fechar
	// segR/segW sob os pés dela — a análise completa está no godoc de
	// CompactaUmaVez (compaction.go).
	compPassadaMu    sync.Mutex
	compPassadaEmVoo bool
	compManualEmVoo  bool
	compEncerrando   bool

	// compAtiva, compBytesLidos e compBytesEscritos são os alimentadores das
	// três séries de compaction do §92, que até o M4 eram nome sem número
	// (names.go). São atômicos e não campos sob o wmu pelo motivo dos
	// espelhos logo acima: quem os lê é o /metrics, no instante do scrape, e
	// um scrape atrás do lock do writer apaga o painel exatamente sob a
	// pressão que ele existe para mostrar.
	//
	// Não há contador duplicado aqui: estes são a ÚNICA soma dos bytes que a
	// compaction move, e o servidor os lê pelo Stats() em vez de manter cópia
	// própria — a doutrina do registry (metrics.go) é que quem publica lê a
	// estrutura existente.
	compAtiva         atomic.Bool
	compBytesLidos    atomic.Int64
	compBytesEscritos atomic.Int64

	// wmu serializa as operações do writer (Put/Sync/Checkpoint): o
	// contrato "writer único" deixa de ser convenção e vira mecanismo —
	// um Checkpoint de coordenador concorrente com Put não pode mais
	// corromper segTally/epoch nem ler offset além do fsyncado.
	wmu sync.Mutex

	// cpTrace é o seam de teste da ordem de durabilidade do checkpoint
	// (nil em produção).
	cpTrace func(step string)
	// apTrace é o seam de teste dos passos da aposentadoria de segmento
	// (§53-55, decisão D5). Nil em produção, mesmo regime do cpTrace acima.
	//
	// Existe porque as duas janelas de crash da aposentadoria são o intervalo
	// ENTRE dois passos, e nenhuma delas é alcançável de fora: quem quer matar
	// o processo entre o rename e o checkpoint precisa de um ponto de parada
	// lá dentro, e reproduzir o estado à mão só afirmaria sobre a ideia que o
	// teste faz da aposentadoria, não sobre o que ela deixa em disco.
	apTrace func(passo string)
	// apDirSync é o seam de teste dos fsyncs de DIRETÓRIO da aposentadoria e
	// do REPARO dela (nil em produção, mesmo regime dos dois acima), e ele é
	// notificado de DENTRO do helper que executa o fsync — nunca de uma linha
	// ao lado.
	//
	// São TRÊS chamadas, não duas: as do rename e do unlink em
	// aposentaSegmento, e a do fim do reparaAposentados. A terceira entrou
	// depois, e o motivo está registrado porque ele é a lição: enquanto o
	// reparo foi função de PACOTE, o seam não o alcançava, e apagar aquele
	// fsync deixava `go test ./...` inteiro verde no único caminho da fase
	// que destrói arquivo. Seam que cobre parte da família não é seam da
	// família.
	//
	// A distinção é a razão de o campo existir. Um rótulo emitido ao lado da
	// chamada mede a si mesmo: apagar o fsync e deixar o rótulo continuaria
	// verde, que é exatamente o defeito que a revisão desta tarefa encontrou
	// no primeiro Checkpoint. Notificado de dentro, o aviso só chega se a
	// chamada de verdade tiver acontecido e devolvido nil, e o teste ainda
	// ancora CADA aviso no estado do diretório naquele instante — é assim
	// que "houve fsync depois do rename e antes do manifest" vira asserção
	// sobre o disco, e não sobre a ordem em que alguém escreveu rótulos.
	apDirSync func(dir string)
	// setErrHook é o seam de teste da fase de SET do índice (nil em produção).
	//
	// Ele é o único jeito de produzir o phantom commit do §14.12 com payload
	// REPLAYÁVEL: um erro depois do Append e antes da publicação da location,
	// que é onde não existe rollback parcial. Todo outro caminho até esse ponto
	// ou é recusado antes do Append (o teto do valor indexável) ou grava um
	// record que o replay não conseguiria reindexar — e um dataset inabrível
	// não serve de montagem para nada.
	//
	// Ele mora no motor por segmento desde que os dois motores globais saíram.
	// A migração do seam é o que preserva os sete cenários que dependem dele
	// (writer sticky, phantom commit em insert e em update, recusa da compaction
	// com o writer morto): nenhum deles fala de bitmap ou de hash — todos falam
	// de "erro pós-Append" —, e deixá-los sem seam trocaria sete provas por
	// nenhuma.
	setErrHook func(types.FieldID) error

	// entreRemoverEEsquecerHook é o seam de teste do removeSidxDoDisco (nil em
	// produção): roda entre o unlink do .sidx e o esquecimento do cache. É o
	// único jeito determinístico de pôr uma consulta DENTRO dessa janela — que
	// é onde a ordem inversa (esquecer primeiro) deixava a consulta publicar o
	// mapeamento de um arquivo já removido, sem ninguém para evictá-lo.
	entreRemoverEEsquecerHook func(seg uint32)

	// segTally acumula os totais por segmento que o writer conhece (para as
	// stats do manifest §9; em memória — a S12 os reconstrói no replay).
	// Writer-only.
	segTally map[uint32]*segStats

	// segSize guarda o tamanho de cada arquivo de segmento visto pelo
	// último ListSegments do checkpoint. Writer-only, mesmo regime do
	// segTally.
	//
	// É mapa à parte, e não um campo do segStats, porque as duas coisas têm
	// FONTES diferentes e precisam continuar tendo: o tally é o que o writer
	// contabilizou (limite superior, débito 38), enquanto isto é o que o
	// diretório respondeu. Fundi-los faria a imprecisão de um contaminar o
	// outro, e é justamente a independência que deixa StorageBytes não
	// herdar a do tally. São também mapas com CICLOS DE VIDA distintos: o
	// tally acompanha cada Put, e isto só se move no checkpoint — que é
	// justamente onde o CONJUNTO de chaves dos dois é reconciliado, porque só
	// ali o Store sabe quais arquivos o diretório ainda tem.
	segSize map[uint32]int64

	// lock garante acesso exclusivo ao diretório do dataset.
	lock *datasetLock

	// Séries do §92 sobre o checkpoint. São nil quando o chamador não passou
	// registry (CLI, testes), e todo uso é guardado — a instrumentação não
	// pode ser condição de funcionamento do storage.
	cpDuracao *metrics.SerieHistograma
	cpUltimo  *metrics.SerieMedidor

	// srcOffsets é a posição no tópico Kafka amarrada a este checkpoint
	// (§48, §50). WRITER-ONLY, sob wmu — mesmo regime de epoch e segTally.
	//
	// Mora no Store, e não num parâmetro do Checkpoint, por causa do Open:
	// ele grava um checkpoint imediato, então qualquer estado que não viva
	// aqui seria apagado na primeira reabertura. Capturar o offset fora do
	// wmu abriria uma janela em que o manifest declara durável um ponto e o
	// arquivo de offsets declara outro.
	//
	// nil = dataset que nunca consumiu de Kafka. Nesse caso o arquivo nem é
	// criado: um dataset alimentado por `nishidb ingest` não tem posição.
	srcOffsets *checkpoint.SourceOffsets
	// srcOffsetsErr guarda a falha de LEITURA do kafka.offsets no Open. Não
	// impede a abertura de propósito: os dados continuam íntegros e o §51
	// manda as consultas subirem antes da ingestão. Quem for retomar o
	// consumo é que precisa tratar isto.
	srcOffsetsErr error

	// Buffers exclusivos do writer (Put).
	buildBuf []byte // record em construção
	prevBuf  []byte // fallback de leitura do estado anterior
	valBuf   []byte // validação de colisão no Insert
	// compactBuf é o record em construção do copy-forward (§53-55). É o
	// "buffer reutilizado" que o §54 exige: sem ele cada record copiado
	// alocaria, e a compaction varre segmentos inteiros. A reutilização está
	// aferida em bytes pelo TestCopyForwardReusaOBufferDoWriterEmVezDeAlocarPorRecord
	// — requisito de performance que a suíte não mede é requisito que não
	// existe, e este campo poderia ser removido sem que nada acusasse.
	//
	// É buffer À PARTE do buildBuf, e não reuso dele, porque os dois nascem em
	// caminhos que só não colidem enquanto nenhum deles segurar o slice além
	// da seção crítica do writer. Hoje nenhum segura, mas reusar o buildBuf
	// transformaria isso numa premissa não escrita ENTRE a ingestão e a
	// compaction — e a compaction chama daqui de dentro de uma varredura, onde
	// a distância entre quem constrói o record e quem o consome só cresce.
	compactBuf []byte
}

// entra registra uma operação em voo e recusa se o Store já fechou. O
// chamador SEMPRE libera com sai(), inclusive nos caminhos de erro.
//
// Devolve erro em vez de panic: a ingestão tem política para erro (§101) e não
// tem para panic.
func (s *Store) entra() error {
	s.vida.RLock()
	if s.fechado {
		s.vida.RUnlock()
		return ErrDatasetClosed
	}
	return nil
}

func (s *Store) sai() { s.vida.RUnlock() }

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
	if opts.Dataset == "" {
		opts.Dataset = filepath.Base(filepath.Clean(opts.Dir))
	}
	if opts.Log == nil {
		opts.Log = logging.New(os.Stderr, logging.FormatText, slog.LevelInfo)
	}
	opts.Log = opts.Log.Component("storage").Dataset(opts.Dataset)
	spillDir := filepath.Join(opts.Dir, spill.NomeDoDiretorio)
	metaDir := filepath.Join(opts.Dir, "metadata")
	dataDir := filepath.Join(opts.Dir, "data")
	idxDir := filepath.Join(opts.Dir, "indexes")
	cpDir := filepath.Join(opts.Dir, "checkpoint")
	for _, d := range []string{metaDir, dataDir, idxDir, cpDir, spillDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("storage: criar %s: %w", d, err)
		}
	}
	// Política de fsync de diretório (design §14): as ENTRADAS dos
	// diretórios recém-criados precisam ser duráveis — um power loss após o
	// primeiro checkpoint não pode deixar Dir/ sem checkpoint/ (viraria
	// "dados sem manifest" = erro fatal na recovery).
	// spillDir fica FORA desta lista de propósito: o transbordo não é durável
	// (não há fsync no conteúdo dele), então gastar um fsync na entrada de
	// diretório seria custo sem consumidor. Um crash que perca o diretório
	// inteiro é reparado pelo MkdirAll da subida seguinte.
	for _, d := range []string{opts.Dir, metaDir, dataDir, idxDir, cpDir} {
		if err := fsyncDir(d); err != nil {
			return nil, err
		}
	}
	s := &Store{
		opts:     opts,
		segTally: make(map[uint32]*segStats),
		segSize:  make(map[uint32]int64),
		agora:    time.Now,
		snaps:    novoRegistroDeSnapshots(),
		vers:     versions.Novas(),
	}
	if opts.Metrics != nil {
		s.cpDuracao = opts.Metrics.Histograma(metrics.MetCheckpointDurationSeconds,
			"Duração do checkpoint em segundos, da chamada ao manifest durável (inclui a espera pelo mutex do writer, que é stall real).",
			limitesDeCheckpoint, metrics.LabelDataset).Com(opts.Dataset)
		s.cpUltimo = opts.Metrics.Medidor(metrics.MetCheckpointLastSuccessTimestmp,
			"Unix timestamp do último checkpoint bem-sucedido. Fica em 0 até o primeiro.",
			metrics.LabelDataset).Com(opts.Dataset)
	}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()

	if err := validaOpcoesDoModo(opts); err != nil {
		return nil, err
	}

	// Lock antes de tocar em qualquer arquivo do dataset. O Close no defer
	// acima o libera nos caminhos de erro — um Open que falha não pode
	// deixar o dataset travado até o processo morrer.
	var err error
	if s.lock, err = acquireDatasetLock(opts.Dir); err != nil {
		return nil, err
	}
	// Resíduo de transbordo de um processo MORTO sai aqui, e é seguro por
	// construção: o flock acima já garantiu que nenhum outro processo tem este
	// dataset, então todo arquivo em spill/ é de alguém que não existe mais.
	// Não é "melhor esforço" — não poder limpar significa não poder confiar no
	// que será escrito ali em seguida.
	if err := spill.Limpa(opts.Dir); err != nil {
		return nil, fmt.Errorf("storage: abrir o dataset %s: %w", opts.Dir, err)
	}
	if opts.DictCompartilhado != nil {
		s.dict = opts.DictCompartilhado
	} else if s.dict, err = fielddict.Open(filepath.Join(metaDir, "fields.dict")); err != nil {
		// O erro do dicionário já traz arquivo, versão e remediação; aqui entra
		// o dataset, que é o identificador que o operador tem na mão.
		return nil, fmt.Errorf("storage: abrir o dataset %s: %w", opts.Dir, err)
	} else {
		s.dictProprio = true
	}
	if s.segR, err = segment.OpenReader(dataDir); err != nil {
		return nil, err
	}
	// Piso de 1<<20 slots do design §5 aplica-se aqui, não no pacote primary.
	// Teto de sanidade evita wraparound do *10/7 com config absurda.
	if opts.EstimatedRows > 1<<40 {
		return nil, fmt.Errorf("%w: EstimatedRows %d acima do teto de sanidade (2^40)", ErrInvalidArgument, opts.EstimatedRows)
	}
	minRows := max(uint64(512), opts.EstimatedRows)
	// A primary É o mecanismo de dedup por identifier, e a sequence só serve
	// ao stale check — os dois descrevem estado corrente. No histórico não há
	// nem um nem outro, e abri-los seria pagar dois mmaps (um deles com rehash
	// a 70% de ocupação, sobre dezenas de milhões de linhas) para não fazer
	// nada. É a mesma economia que o TTLAtivo já faz com expiry.idx.
	if opts.Modo != ModoHistorico {
		minSlots := max(uint64(1<<20), opts.EstimatedRows*10/7)
		if s.prim, err = primary.Open(filepath.Join(idxDir, "primary.idx"), minSlots); err != nil {
			return nil, err
		}
		if s.seqs, err = rowindex.OpenSequences(filepath.Join(idxDir, "sequences.idx"), minRows); err != nil {
			return nil, err
		}
	}
	if s.locs, err = rowindex.OpenLocations(filepath.Join(idxDir, "locations.idx"), minRows); err != nil {
		return nil, err
	}
	if opts.TTLAtivo {
		if opts.CampoDeEscrita == "" || opts.CampoDePrazo == "" {
			// Sem os campos o applyRecord não teria de onde tirar o instante
			// nem a duração, e toda row ficaria com prazo desconhecido — a
			// expiração nunca aconteceria, em silêncio.
			return nil, fmt.Errorf("%w: TTLAtivo exige CampoDeEscrita e CampoDePrazo", ErrInvalidArgument)
		}
		if s.escritas, err = rowindex.OpenEscritas(filepath.Join(idxDir, "expiry.idx"), minRows); err != nil {
			return nil, err
		}
		if s.prazos, err = rowindex.OpenPrazos(filepath.Join(idxDir, "prazos.idx"), minRows); err != nil {
			return nil, err
		}
		if s.agenda, err = rowindex.OpenAgenda(idxDir, minRows); err != nil {
			return nil, err
		}
		s.duracaoDeFundo = opts.DuracaoDeFundo
	}
	m, mErr := checkpoint.Load(s.manifestPath())
	refs, err := segment.ListSegments(dataDir)
	if err != nil {
		return nil, err
	}

	// A recusa de coluna derivada nova sobre dataset com linhas mora AQUI, e
	// não antes do Open nem depois: o dicionário já está aberto (é o que
	// permite saber se a coluna é nova) e nada foi gravado nele ainda —
	// _writtenAt e os campos indexados são registrados logo abaixo, com fsync,
	// e o dicionário é append-only. Uma subida abortada depois disso deixaria
	// o nome registrado para sempre.
	if err := validaDerivadasContraDicionario(m, s.dict, opts.ColunasDerivadas); err != nil {
		return nil, err
	}

	// O modo e a marca-d'água vêm do manifest ANTES de qualquer ramo de
	// recovery: os dois governam o que é visível, e uma abertura que decidisse
	// isso depois de já ter reconstruído índices leria linhas que a retenção
	// tinha tirado da consulta.
	if err := s.adotaModoDoManifest(m); err != nil {
		return nil, err
	}

	// O histórico resolve o campo de escrita SEM abrir expiry.idx: quem lê o
	// _writtenAt lá é a busca binária da marca-d'água, direto do payload. O
	// nome já está no dicionário compartilhado — o dataset base o registrou
	// antes, porque declarar `history.expireAfterWrite` faz o Compile dele
	// exigir o campo.
	if s.escritas == nil && opts.Modo == ModoHistorico && opts.CampoDeEscrita != "" {
		if s.campoDeEscrita, err = s.dict.LookupOrCreate([]byte(opts.CampoDeEscrita)); err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDeEscrita, err)
		}
	}
	if s.escritas != nil {
		// LookupOrCreate e não Lookup, e nesta ORDEM. O id sai daqui e tem de
		// ser o MESMO que o schema.Compile resolve depois, e a ordem das
		// chamadas decide o mapeamento nome→id de um dataset NOVO, gravado
		// para sempre: _writtenAt, _expiresIn, e só então os campos indexados,
		// resolvidos logo abaixo (ver o comentário ali para o que mudou DENTRO
		// desse último grupo desde a D5). Nunca reordene esta sequência.
		if s.campoDeEscrita, err = s.dict.LookupOrCreate([]byte(opts.CampoDeEscrita)); err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDeEscrita, err)
		}
		if s.campoDePrazo, err = s.dict.LookupOrCreate([]byte(opts.CampoDePrazo)); err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDePrazo, err)
		}
	}

	// A resolução do conjunto indexado vem antes de abrir qualquer índice: os
	// nomes viram FieldID pelo dicionário, e o LookupOrCreate queima ids na
	// ordem em que é chamado.
	//
	// ATÉ A D5 (planning/spec-subsistema-1-indice-por-segmento.md) havia DOIS
	// conjuntos e duas chamadas aqui, e a ordem entre elas decidia mais do que
	// "canônica dentro de cada lista": BitmapFieldNames e HashFieldNames vinham
	// de grafias DIFERENTES do YAML (`index: bitmap` e `index: hash`), então
	// "bitmap primeiro, hash depois" também decidia QUAL GRUPO pegava os ids
	// mais baixos — e por isso a ordem ENTRE os dois grupos era chamada de tão
	// contratual quanto a ordem DENTRO de cada um.
	//
	// Não existe mais "grupo bitmap" nem "grupo hash": existe UM grupo, os
	// campos indexados, numa corrida alfabética só.
	//
	// O que CONTINUA contrato: "mesmo spec → mesmo mapeamento" — para uma
	// config dada, o conjunto indexado e a ordem alfabética dentro dele são
	// determinísticos, e um dataset novo sempre nasce com o mesmo mapa
	// nome→id. O que DEIXOU de ser contrato foi a relação ENTRE os dois
	// grupos (quem vem primeiro): não há mais dois grupos a ordenar entre si.
	//
	// Nada disto muda um dataset JÁ ABERTO: o fielddict é append-only em
	// disco, e um FieldID já gravado não é revisitado — a mudança só decide o
	// mapeamento de um dataset NOVO, porque foi a CONFIG (a spec, não o
	// formato) que mudou.
	//
	// NÃO HÁ VALIDAÇÃO DO CONJUNTO CONTRA O MANIFEST, e a ausência é a decisão.
	// Eram duas, e recusavam (1) abrir sem um campo que o dataset declara
	// indexado e (2) declarar um índice num dataset que já tem linhas. As duas
	// eram sobre a mesma coisa: um índice GLOBAL que cobriria só um pedaço do
	// dataset e responderia MENOS linhas do que existem — falso negativo, que a
	// revalidação do §44 não sabe consertar porque ela só descarta candidato a
	// mais. Com o índice por segmento, segmento sem a seção do campo não
	// responde menos: ele ENUMERA as linhas dele como candidatas (spec §3.2), e
	// o resultado é o mesmo, só mais caro. Uma guarda que recusa a subida para
	// evitar um dano que não pode mais acontecer é obstáculo, não proteção — e o
	// manifest deixou de carregar o conjunto contra o qual ela decidia.
	//
	// A OUTRA METADE DO DÉBITO 22 ANDOU E NÃO CHEGOU: acrescentar um campo
	// indexado a um dataset povoado não só é aceito como AGENDA a reconstrução
	// dos segmentos antigos, porque a pendência passou a ser decidida por
	// (segmento, campo) — um .sidx sem a seção do campo declarado entra na fila
	// (SegmentosPendentesDeReconstrucao). Remover um campo entra pela mesma
	// porta, no sentido inverso: a seção que sobrou é apagada pela reescrita.
	// O que ainda NÃO existe é quem CONSUMA a fila: a goroutine da §2.4. Sem ela
	// o agendamento não vira trabalho, e os segmentos antigos enumeram
	// indefinidamente — é por isso que o débito 22 continua aberto.
	campos, err := s.resolveCamposIndexados(opts.CamposIndexados, opts.CamposIndexadosPorNome)
	if err != nil {
		return nil, err
	}

	// Os contadores abrem AQUI — depois de todo LookupOrCreate contratual e
	// antes do ramo de recovery —, dobrados a partir do vetor em disco. O
	// replay do tail passa pelo applyRecord, que os transiciona: eles têm de
	// existir, com os totais do ponto S, quando a primeira linha do tail for
	// reaplicada. Um manifest ilegível cai nos ramos abaixo; aqui ele só é
	// consultado quando existe.
	var manifestParaContadores *checkpoint.Manifest
	if mErr == nil {
		manifestParaContadores = m
	}
	if err := s.abreContadores(opts, manifestParaContadores, idxDir, minRows); err != nil {
		return nil, err
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
	// A reconstrução dos contadores vem DEPOIS do ramo de recovery, porque
	// varre as locations e elas ainda estavam sendo publicadas pelo replay; e
	// antes do índice por segmento e da agenda, porque não depende de nenhum
	// dos dois.
	if err := s.reconstroiContadoresPendentes(); err != nil {
		return nil, err
	}
	if len(s.contadores) > 0 {
		s.opts.Log.Info("contadores.abertos",
			"contadores mantidos na escrita abertos com o dataset",
			"contadores", descreveContadores(s.contadores))
	}
	// O índice por segmento cobre TODO campo indexado, sem distinção de motor:
	// com um motor só, `hash` e `bitmap` deixam de nomear coisas diferentes
	// (spec D5) — os dois viram "valor → conjunto ordenado de rowIds dentro do
	// segmento", e densidade passa a ser decisão de codificação.
	//
	// Ele abre DEPOIS do ramo de recovery, e não junto dos outros dois, porque
	// os WIPs pertencem ao SEGMENTO CORRENTE e é o ramo quem o decide: antes
	// dele, curSegID ainda é zero num dataset existente. Os campos, esses, vêm
	// do resolveCamposIndexados lá em cima, cuja ordem de LookupOrCreate é
	// contratual e não pode ser tocada.
	if err := s.abreIndicesDeSegmento(campos); err != nil {
		return nil, err
	}

	// A agenda vem DEPOIS do ramo de recovery: ela deriva das locations, e o
	// replay do tail ainda as estava publicando. Construí-la antes daria uma
	// árvore que ignora tudo que o tail trouxe.
	if err := s.adotaOuConstroiAgenda(m); err != nil {
		return nil, err
	}

	// Offsets ANTES do checkpoint imediato: o Checkpoint reescreve o
	// kafka.offsets a partir do estado em memória, então carregar depois
	// apagaria a posição na primeira reabertura do dataset.
	s.carregaOffsets()

	// Checkpoint imediato: manifest desde a criação e estado limpo pós-
	// recovery (novo S = tail; epoch nunca regride).
	if err := s.Checkpoint(); err != nil {
		return nil, err
	}
	// A limpeza dos índices globais legados vem DEPOIS deste checkpoint, e a
	// ordem é a única coisa que a torna segura: é ele que grava o manifest v3,
	// e é o v3 que faz um binário anterior recusar o dataset. Antes dele o
	// dataset ainda pode ser aberto pelo binário que criou aqueles arquivos, e
	// apagá-los ali deixaria justamente o índice INCOMPLETO que este ciclo
	// inteiro existe para tornar impossível.
	limparIndicesGlobaisLegados(s.opts.Log, idxDir)
	ok = true
	return s, nil
}

// carregaOffsets semeia a posição Kafka a partir do disco.
//
// Nenhum modo de falha aqui impede a abertura. É deliberado e vem do §51, que
// põe "start queries" ANTES de "seek Kafka offsets": um kafka.offsets
// ilegível não torna os dados inacessíveis, torna a POSIÇÃO desconhecida — e
// recusar a abertura por isso derrubaria também as consultas, que não têm
// nada a ver com o problema. Quem for retomar o consumo consulta o erro.
func (s *Store) carregaOffsets() {
	o, err := checkpoint.LoadOffsets(s.offsetsPath())
	switch {
	case err == nil:
		s.srcOffsets = &o
	case errors.Is(err, checkpoint.ErrNoOffsets):
		// Dataset que nunca consumiu de Kafka. Não é anomalia.
	default:
		s.srcOffsetsErr = err
		s.opts.Log.Warn("offsets.ilegivel",
			"kafka.offsets não pôde ser lido; as consultas sobem, a ingestão não retoma sem decisão explícita",
			"erro", err.Error())
	}
}

// resolveCamposIndexados devolve a união, ordenada e sem duplicatas, dos
// campos declarados por ID e por nome. Os nomes são resolvidos pelo
// dicionário do dataset, criando a entry se ainda não existir (a regra
// fsync-before-first-use vale, é o próprio LookupOrCreate que a aplica).
//
// A garantia que ela existe para dar — ordem canônica antes de queimar id no
// dicionário — não é sobre motor de índice, é sobre a criação do dataset. Era
// por isso que ela já era UMA função quando havia dois motores: um segundo laço
// para o segundo motor seria a primeira cópia a divergir.
func (s *Store) resolveCamposIndexados(ids []types.FieldID, nomesDeclarados []string) ([]types.FieldID, error) {
	set := make(map[types.FieldID]struct{}, len(ids)+len(nomesDeclarados))
	for _, field := range ids {
		if field == 0 {
			return nil, fmt.Errorf("%w: fieldID 0 nos campos indexados", ErrInvalidArgument)
		}
		set[field] = struct{}{}
	}
	// Ordem CANÔNICA antes de resolver: o LookupOrCreate queima os primeiros
	// IDs do dicionário na ordem em que é chamado, então a ordem do slice
	// decidiria o mapeamento nome→ID de um dataset novo — e ele é gravado para
	// sempre. Ordenar aqui, e não em cada chamador, é o que torna a garantia
	// independente de quem abre (o CLI monta a lista iterando um map, e o
	// resultado seria sorteado a cada criação de dataset).
	//
	// Cópia: o slice é do chamador, e reordená-lo no lugar seria efeito
	// colateral de uma função que promete só abrir o dataset.
	nomes := slices.Clone(nomesDeclarados)
	slices.Sort(nomes)
	for _, nome := range nomes {
		if nome == "" {
			return nil, fmt.Errorf("%w: nome vazio nos campos indexados", ErrInvalidArgument)
		}
		id, err := s.dict.LookupOrCreate([]byte(nome))
		if err != nil {
			return nil, fmt.Errorf("storage: resolver o campo indexado %q: %w", nome, err)
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

// limparIndicesGlobaisLegados apaga os arquivos dos dois motores de índice que
// este binário não tem mais: indexes/bitmap/ e indexes/hash/.
//
// Eles não têm leitor nenhum a partir daqui, e por isso não podem ficar. O
// IndexBytes soma o diretório indexes/ inteiro (retention.go), então deixá-los
// seria publicar disco ocupado que nada no produto explica — e num dataset
// grande a soma dos dois é da ordem do próprio dado. É o mesmo modo de falha
// que o .widx órfão de segmento aposentado já obrigou a fechar: métrica de
// disco que aponta para arquivo que ninguém sabe nomear vira, na próxima
// investigação, uma hipótese errada sobre onde o espaço foi.
//
// Só o dataset LEGADO tem esses diretórios; num dataset criado por este binário
// o Stat falha e nada acontece. E o momento importa: ela roda DEPOIS do
// checkpoint incondicional do Open, que é quem grava o manifest v3 — a partir
// dali um binário anterior recusa o dataset com "version desconhecida", e não
// existe mais volta que fosse usar estes arquivos. Chamada antes daquele
// checkpoint, ela deixaria uma janela em que o binário antigo ainda abre o
// dataset e reconstrói os índices dele VAZIOS, que é o falso negativo silencioso
// que este ciclo inteiro existe para tornar impossível.
//
// Falha NÃO impede a abertura, e a assimetria é deliberada: o que está em jogo é
// disco desperdiçado, não corretude. Recusar o Open por um RemoveAll que não deu
// certo trocaria um dataset que funciona por um dataset fora do ar. Fica o WARN
// nomeado, e a próxima abertura tenta de novo.
func limparIndicesGlobaisLegados(log *logging.Logger, idxDir string) {
	for _, motor := range []string{"bitmap", "hash"} {
		dir := filepath.Join(idxDir, motor)
		if _, err := os.Stat(dir); err != nil {
			continue
		}
		// Medido ANTES do RemoveAll e pela MESMA função do IndexBytes: é assim
		// que o número do log é o mesmo que o operador vê descer na
		// nishidb_index_bytes, em vez de uma segunda contagem que diverge.
		ocupado := blocosDoDiretorio(dir)
		if err := os.RemoveAll(dir); err != nil {
			log.Warn("indice.legado_nao_removido",
				"o índice global de um binário anterior continuou em disco; ele não tem leitor e é contado pelo index_bytes",
				"diretorio", dir, "bytes", ocupado, "erro", err.Error())
			continue
		}
		log.Info("indice.legado_removido",
			"o índice global de um binário anterior foi apagado; este binário responde pelo índice por segmento",
			"diretorio", dir, "bytes", ocupado)
	}
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

// Indexado informa se o campo tem índice.
//
// É o capability check do planner: sem ele o plano escolhe o caminho
// indexado e a query falha em RUNTIME, em vez de degradar para full scan
// como o design promete. O conjunto é fixado no Open, então a resposta é
// estável durante toda a vida do Store.
//
// É UM método, e não mais o par HasBitmap/HasHash: os dois respondiam pela
// MESMA fonte — o conjunto indexado do dataset — desde que passou a existir um
// motor só, e duas perguntas com uma resposta só é convite a alguém consultar
// uma delas e concluir a outra.
//
// A enumeração do conjunto é o CamposIndexados (indices.go), que já era a
// terceira grafia da mesma coisa quando BitmapFields e HashFields existiam.
func (s *Store) Indexado(field types.FieldID) bool {
	return slices.Contains(s.camposIndexados, field)
}

// lookup resolve a key pela primary, lendo o record corrente em buf.
// exists = key presente; live = sem flag de tombstone na location.
//
// O callback de validação roda sob o RLock do índice primary: ele NÃO pode
// reentrar no índice (deadlock com um rehash enfileirado) — mantém-se
// mínimo: locations.Get + pread + comparação de key (spec §36).
func (s *Store) lookup(key, buf []byte, snap types.Snapshot) (rec types.Record, out []byte, exists, live bool, err error) {
	out = buf
	if s.prim == nil {
		// Sem primary não há como resolver uma key, e devolver "não existe"
		// seria pior que o erro: o executor trataria como linha ausente e a
		// consulta responderia vazio sobre uma tabela cheia. O planner já não
		// elege este caminho (o Compiled do histórico não tem Key), então
		// chegar aqui é defeito de programa, não de dado.
		return rec, out, false, false, fmt.Errorf("%w: %s não tem índice primary: a tabela histórica não resolve por key",
			ErrInvalidArgument, s.opts.Dataset)
	}
	validate := func(rid types.RowID) (bool, error) {
		// Par (Get, Read) sob a barreira de aposentadoria: ver o godoc de
		// aposMu. O RLock entra AQUI e não em volta do prim.Find inteiro
		// porque o probe pode visitar vários slots, e segurar a barreira por
		// todos eles alargaria a seção sem necessidade.
		s.aposMu.RLock()
		defer s.aposMu.RUnlock()
		// A location da ÉPOCA do leitor, pela mesma razão do GetByRowID: sem
		// ela o point lookup responderia sobre o presente enquanto o resto da
		// consulta responde sobre T0. A key não muda entre versões, então a
		// validação de colisão logo abaixo continua valendo sobre a antiga.
		var packed uint64
		var ok bool
		if !s.vers.Vazio() {
			packed, ok = s.vers.Em(rid, snap.Epoch)
		}
		if !ok {
			if packed, ok = s.locs.Get(rid); !ok {
				// Slot da primary sem location (janela de crash): não é a row.
				return false, nil
			}
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
		// A liveness tem DUAS causas de morte: a lápide já publicada e o
		// prazo vencido que o coletor ainda não recolheu. Contá-las juntas
		// aqui é o que faz a visibilidade não depender do agendador.
		live = locFlags&types.FlagTombstone == 0 && !s.venceuNaVersao(r, snap.Instant)
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
func (s *Store) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	if err := s.entra(); err != nil {
		return types.Record{}, buf, false, err
	}
	defer s.sai()
	if !s.snaps.valido(snap.Epoch) {
		return types.Record{}, buf, false, ErrSnapshotExpirado
	}
	rec, out, exists, live, err := s.lookup(key, buf, snap)
	if err != nil || !exists || !live {
		return types.Record{}, out, false, err
	}
	return rec, out, true, nil
}

// Estado resolve a key e devolve os TRÊS estados possíveis (§58). É o que o
// materializer usa: ele precisa distinguir "key nova" de "entidade que
// existiu", porque só no segundo caso o stale check tem uma sequence anterior
// contra a qual comparar — e sem essa comparação um evento antigo ressuscita
// a row.
//
// O GetByKey continua existindo com bool para o caminho de LEITURA, onde a
// pergunta é outra: "esta row aparece na query?". Duas perguntas diferentes,
// duas assinaturas.
func (s *Store) Estado(key, buf []byte) (types.Record, []byte, types.Presence, error) {
	if err := s.entra(); err != nil {
		return types.Record{}, buf, types.PresenceAbsent, err
	}
	defer s.sai()
	rec, out, exists, live, err := s.lookup(key, buf, s.presente())
	switch {
	case err != nil:
		return types.Record{}, out, types.PresenceAbsent, err
	case !exists:
		return types.Record{}, out, types.PresenceAbsent, nil
	case !live:
		// O record VOLTA, e não zerado: RowID e Sequence são o que o stale
		// check e a ressurreição precisam.
		return rec, out, types.PresenceExpired, nil
	}
	return rec, out, types.PresenceLive, nil
}

// GetByRowID devolve o record corrente da row. ok=false para row sem
// location ou tombstoned (checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if err := s.entra(); err != nil {
		return types.Record{}, buf, false, err
	}
	defer s.sai()
	if !s.snaps.valido(snap.Epoch) {
		return types.Record{}, buf, false, ErrSnapshotExpirado
	}
	// Par (Get, Read) sob a barreira de aposentadoria: ver o godoc de aposMu.
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	// A location da ÉPOCA do leitor, que só difere da corrente quando a linha
	// foi atualizada com esta consulta em voo. O journal é consultado ANTES do
	// locs.Get e o lock dele é solto na saída da chamada: nada é segurado
	// enquanto o aposMu está na mão, e o segmento apontado não pode ser
	// aposentado porque o pin pertence ao snapshot vivo, não a esta leitura.
	var packed uint64
	var ok bool
	// O Vazio() na frente evita a chamada de Em no caso dominante — journal
	// sem nada retido —, e é por linha varrida que isso conta.
	if !s.vers.Vazio() {
		packed, ok = s.vers.Em(id, snap.Epoch)
	}
	if !ok {
		if packed, ok = s.locs.Get(id); !ok {
			return types.Record{}, buf, false, nil
		}
	}
	seg, off, locFlags := rowindex.Unpack(packed)
	if locFlags&types.FlagTombstone != 0 || s.purgada(id) {
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
	// A expiração é avaliada sobre o PAYLOAD que o leitor está recebendo, e
	// não sobre os índices de escrita e prazo — que descrevem a versão
	// CORRENTE. Numa linha reescrita durante a consulta os dois discordam, e é
	// o payload que manda: ele é a fonte da verdade de que os índices são
	// derivados (ver o godoc de rowindex.Escritas), então derivar daqui é a
	// MESMA conta que o applyRecord faz, sobre a versão certa.
	if s.escritas != nil && s.venceuNaVersao(rec, snap.Instant) {
		return types.Record{}, out, false, nil
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
	if err := s.entra(); err != nil {
		return 0, false, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return 0, false, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	if s.EhHistorico() {
		// Put resolve a key pela primary e decide entre criar e atualizar. A
		// tabela histórica não faz nem uma coisa nem outra: ver Apenda.
		return 0, false, fmt.Errorf("%w: %s é uma tabela histórica; use Apenda", ErrInvalidArgument, s.opts.Dataset)
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
		found, out, exists, _, err := s.lookup(key, s.prevBuf[:0], s.presente())
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
	if err := s.aposAppend(seg); err != nil {
		return 0, false, s.failWriter(err)
	}
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
		versiona:    true,
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
	// versiona diz se esta publicação muda o estado VISÍVEL da linha, e por
	// isso precisa guardar a versão anterior para os leitores em voo.
	//
	// A distinção não é decorativa e não pode sair do `mode`: o copy-forward da
	// compaction também publica location nova com hadPrevious, e também é
	// applyWrite — mas ele só MOVE bytes, o conteúdo é idêntico, e o leitor com
	// snapshot deve seguir a location nova. Se ele versionasse, cada passada
	// criaria versões apontando para o segmento que ela está aposentando, e o
	// pin bloquearia a própria compaction que o gerou.
	versiona bool
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
		// Nascimento de entrada no tally é nascimento de segmento aos olhos
		// do writer — é por aqui que uma rotação aparece no Stats no instante
		// em que acontece, sem esperar o checkpoint relistar o diretório
		// (é o que o TestRotacaoDeSegmentoNoPut exige).
		s.segCount.Add(1)
	}
	tally.totalRecords++
	tally.liveRecords++
	tally.liveBytes += uint64(a.recLen)
	s.liveBytesAll.Add(uint64(a.recLen))
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
				// A BAIXA é calculada antes de mexer no tally, e o espelho
				// desconta exatamente ela — nunca o oldLen cru. O oldLen é
				// estimativa (débitos 38 e 45) e o clamp em zero abaixo é o
				// que impede o tally de furar por baixo; descontar o valor
				// não-clampado do agregado o faria divergir da soma dos
				// tallies num erro que só cresce, e o agregado é justamente
				// o que o Stats publica no lugar da soma.
				baixa := min(oldLen, t.liveBytes)
				t.liveBytes -= baixa
				// Go não tem Sub atômico para Uint64; ^(x-1) é o complemento
				// de dois de -x, que é o idioma da própria stdlib (e devolve
				// 0 para x==0, então a baixa nula não vira underflow).
				s.liveBytesAll.Add(^(baixa - 1))
			}
		}
	}

	// 4. primary insert-if-absent. No write path só a key nova precisa
	// (o lookup já resolveu as demais); no replay roda SEMPRE, porque um
	// checkpoint interrompido pode ter deixado a primary atrás das
	// locations — e o Insert é idempotente.
	if s.prim != nil && (a.countsAsNew || a.mode == applyReplay) {
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
	// 5. fase de SET do índice secundário ANTES do commit: a janela entre
	// índice e location é sempre um SUPERSET de candidatos (falso positivo,
	// filtrado pela revalidação §44) — nunca falso negativo.
	if len(s.wips) > 0 {
		if err := s.aplicaIndiceSets(a.rowID, a.payload); err != nil {
			return err
		}
	}
	// 6. sequence. Ausente no histórico: ela só serve ao stale check, que só
	// existe porque há estado a sobrescrever.
	if s.seqs != nil {
		if err := s.seqs.Set(a.rowID, a.seq); err != nil {
			return err
		}
	}
	// 6b. instante da escrita, LIDO DO PAYLOAD.
	//
	// Ler do payload, e não do relógio daqui, é o que torna o índice de
	// expiração reconstruível: applyRecord é o único caminho de aplicação de
	// estado e roda tanto no Put quanto no replay do tail, então depois de um
	// crash o replay repõe exatamente os mesmos instantes. Se o valor viesse
	// do relógio, o replay empurraria todos os prazos para frente pelo tempo
	// de queda — e um processo reiniciado em laço nunca expiraria nada.
	if s.escritas != nil {
		if ms, ok := instanteDeEscrita(a.payload, s.campoDeEscrita); ok {
			if err := s.escritas.Set(a.rowID, ms); err != nil {
				return err
			}
		}
	}
	// 6c. duração de ociosidade da linha, LIDA DO PAYLOAD pela mesma razão do
	// instante: o replay do tail repõe as MESMAS durações depois de um crash.
	if s.prazos != nil {
		if ms, ok := campoDoPayload(a.payload, s.campoDePrazo, types.TypeInt64); ok {
			if err := s.prazos.Set(a.rowID, ms); err != nil {
				return err
			}
		}
	}
	// 7. COMMIT POINT: publica a location.
	//
	// A época avança AQUI, junto do publish e sob o mesmo wmu, porque é a
	// publicação que ela numera. Avançá-la antes deixaria uma janela em que um
	// snapshot nasceria numa época cuja location ainda não está visível — e o
	// leitor procuraria no journal uma versão que ninguém guardou.
	//
	// A versão anterior é guardada ANTES do publish, enquanto locs.Get ainda
	// devolve a location velha, e só quando há leitor atrás dela: sem consulta
	// aberta o journal não recebe uma entrada sequer, e a ingestão não paga
	// nada além da comparação de um atômico.
	if a.versiona && a.mode == applyWrite {
		if s.snaps.menorEpocaViva() != versions.SemLeitor {
			if anterior, tinha := s.locs.Get(a.rowID); tinha {
				s.vers.Registra(a.rowID, anterior, s.epocaPub.Load()+1)
				s.invalidaOMaisAntigoSeEstourou()
			}
		}
	}
	if a.mode == applyWrite {
		s.epocaPub.Add(1)
	}
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
	// 7b. agenda de vencimentos, DEPOIS do commit da location.
	//
	// Depois, e não antes, porque o vencimento é função da location: linha sem
	// location publicada e linha tombstonada não vencem, e ler a location
	// antes do Publish agendaria a partir do estado ANTERIOR — a lápide que
	// acabou de ser gravada continuaria agendada para o vencimento que ela
	// veio cancelar, e o coletor a reentregaria em toda descida.
	//
	// A agenda é derivada e não participa da durabilidade do §6.1: ela guarda
	// um instante, não uma referência a record. Uma janela entre o Publish e
	// esta linha custaria, no pior caso, uma passada de atraso na expiração —
	// e ela não existe, porque as duas rodam sob o mesmo wmu.
	if s.agenda != nil {
		if err := s.agenda.Atualiza(a.rowID, s.vencimentoDe(a.rowID)); err != nil {
			return err
		}
	}
	// 7c. contadores mantidos na escrita, DEPOIS do commit, pelo mesmo
	// argumento da agenda: derivados, fora da durabilidade do §6.1, sob o
	// mesmo wmu. O delta sai do PRÓPRIO vetor do contador (entrada antiga →
	// nova), nunca de prevPayload — no replay ele não existe
	// (store_recovery.go), e é exatamente o replay que este passo tem de
	// atravessar igual ao Put. A lápide entra como "sem grupo": a entrada
	// antiga é quem lembra de onde a linha saiu.
	//
	// Erro aqui deixa o record durável e publicado e o contador uma
	// transição atrás — a próxima abertura dobra o vetor e reaplica a ponta,
	// como um erro na agenda. Não é sticky.
	lapide := a.flags&types.FlagTombstone != 0
	for _, c := range s.contadores {
		if err := c.Transiciona(a.rowID, a.payload, lapide); err != nil {
			return err
		}
	}
	// NÃO HÁ passo 8. A fase de CLEAR saiu junto com os motores globais, e a
	// ausência é o desenho (spec §5.2): a posting do valor antigo mora no índice
	// do segmento ANTIGO, vira falso positivo — que a revalidação do §44
	// descarta — e morre quando a compaction ou a retenção aposenta aquele
	// segmento. Com o Clear somem a lápide, a categoria inteira de "o replay
	// esqueceu de limpar" e o falso negativo que a lápide existia para evitar.
	return nil
}

// instanteDeEscrita extrai o campo de sistema do payload. Ausente devolve
// ok=false, que o índice registra como DESCONHECIDO — nunca como vencido.
func instanteDeEscrita(payload []byte, campo types.FieldID) (int64, bool) {
	return campoDoPayload(payload, campo, types.TypeTimestamp)
}

// campoDoPayload lê o I64 de um campo de sistema, exigindo o tipo esperado.
//
// É uma função só para os dois campos da expiração — o instante (timestamp) e
// a duração (int64) — porque a varredura é idêntica e duplicá-la faria as duas
// divergirem no dia em que o formato do payload mudar. O tipo entra por
// parâmetro porque errá-lo é o modo de falha silencioso: um int64 lido como
// timestamp devolveria um prazo que não é um prazo.
func campoDoPayload(payload []byte, campo types.FieldID, esperado types.FieldType) (int64, bool) {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return 0, false
		}
		if id == campo {
			return v.I64, v.Type == esperado
		}
		if id > campo {
			return 0, false // payload é ordenado por FieldID: passou, não tem
		}
	}
}

// venceuNaVersao decide a expiração da VERSÃO que o leitor está recebendo.
//
// Existe porque prazoVencido consulta os índices, e os índices descrevem a
// versão CORRENTE: numa linha reescrita durante a consulta eles já carregam o
// instante novo, e a conta sairia sobre um estado que este leitor não vê.
//
// O payload manda QUANDO ELE DECLARA o instante — aí ele é a fonte da verdade
// de que o índice é derivado, e a conta é a mesma do applyRecord sobre a
// versão certa. Quando ele NÃO declara, a resposta volta para o índice, e essa
// metade não é um detalhe: o godoc de rowindex.Escritas diz que a fonte da
// verdade é o campo do payload, e isso é verdade só para a linha que o tem. A
// linha LEGADA — materializada antes de as regras existirem — não tem campo
// nenhum, e o instante dela é o da CARÊNCIA, semeado pelo coletor na primeira
// passada, que não existe em record algum. Derivar só do payload a tornaria
// imortal, e é isso que TestPassadaDaCarenciaAQuemNaoTemInstante recusa.
//
// O caso que sobra sem isolamento é estreito e está nomeado aqui de propósito:
// uma linha SEM o campo de escrita, reescrita durante a consulta, é avaliada
// pelo índice corrente. Ela não tem versão própria do instante para ser
// avaliada por — não existe dado que a responda —, e a carência é a única
// resposta que o dataset tem.
func (s *Store) venceuNaVersao(rec types.Record, em time.Time) bool {
	if s.escritas == nil {
		return false
	}
	ms, declarado := instanteDeEscrita(rec.Payload, s.campoDeEscrita)
	if !declarado {
		return s.prazoVencido(rec.RowID, em)
	}
	d := s.duracaoDoPayload(rec.Payload)
	if d <= 0 {
		return false
	}
	return em.UnixMilli()-ms >= d.Milliseconds()
}

// duracaoDoPayload devolve a duração de ociosidade que ESTE payload declara,
// caindo na duração de fundo quando ele não declara nenhuma — a mesma
// precedência de duracaoEfetiva, sobre o payload em vez do índice.
func (s *Store) duracaoDoPayload(payload []byte) time.Duration {
	if ms, ok := campoDoPayload(payload, s.campoDePrazo, types.TypeInt64); ok {
		if ms <= 0 {
			return 0 // imortal declarado pela regra `never`
		}
		return time.Duration(ms) * time.Millisecond
	}
	return s.duracaoDeFundo
}

// prazoVencido é o predicado de leitura da expiração, e ele existe para que a
// visibilidade NÃO dependa do agendador: uma row deixa de aparecer assim que
// o prazo passa, mesmo com o coletor parado, atrasado ou nunca ligado. O
// agendador vira mecanismo de ESPAÇO, não de correção — e `nishidb query` e
// `nishidb server` respondem a mesma coisa sobre o mesmo dataset.
//
// Prazo DESCONHECIDO (entrada zerada) não vence: é a row anterior à ativação
// da expiração, ou a que perdeu o índice, e apagá-la seria perder dado vivo
// por falta de informação. Ela ganha carência na primeira varredura.
//
// O instante entra por PARÂMETRO, e não por s.agora(), porque quem pergunta
// decide o relógio. O leitor passa o Instant do seu Snapshot — congelado na
// abertura da consulta —, e é isso que impede uma linha viva em T0 de sumir do
// meio do resultado. O writer passa s.agora(): a expiração oportunista é dele,
// e ela decide sobre o presente.
func (s *Store) prazoVencido(rowID types.RowID, em time.Time) bool {
	if s.escritas == nil {
		return false
	}
	d := s.duracaoEfetiva(rowID)
	if d <= 0 {
		return false
	}
	ms, conhecido := s.escritas.Get(rowID)
	if !conhecido {
		return false
	}
	return em.UnixMilli()-ms >= d.Milliseconds()
}

// vencimentoDe calcula QUANDO esta linha vence, no formato que a agenda
// guarda. É função pura do estado indexado — location, escritas, prazos e a
// duração de fundo —, e é essa pureza que permite reconstruir a agenda inteira
// a partir dos arquivos que já existem, sem ler um record sequer.
//
// A relação com prazoVencido é deliberadamente de DERIVAÇÃO, não de
// substituição: prazoVencido continua sendo o predicado que decide
// visibilidade, lendo os mesmos dois índices. A agenda só decide quando o
// coletor olha. Fazer prazoVencido ler o vencimento materializado
// transformaria um bug de agenda em um bug de query — a linha sumiria da
// consulta por causa de uma entrada de árvore errada —, e é por isso que ele
// não lê.
//
// Os três casos que não são um instante:
//
//   - sem location, ou tombstonada: rowindex.Nunca. A linha não vence porque
//     não há o que expirar, e a lápide preserva o instante de escrita de
//     propósito, então sem este ramo ela ficaria vencida para sempre.
//   - instante DESCONHECIDO: rowindex.Carencia. Ela não venceu, mas precisa da
//     atenção do coletor para receber o prazo cheio. É o único caso em que a
//     agenda pede uma visita sem que prazoVencido concorde, e é assim que a
//     entidade materializada antes de a expiração ser ligada deixa de ficar
//     imortal por acidente de calendário.
//   - duração não positiva: rowindex.Nunca. É a linha que uma regra `never`
//     declarou imortal, ou o dataset sem duração de fundo.
func (s *Store) vencimentoDe(rowID types.RowID) uint64 {
	if s.escritas == nil {
		return rowindex.Nunca
	}
	packed, publicada := s.locs.Get(rowID)
	if !publicada {
		return rowindex.Nunca
	}
	if _, _, flags := rowindex.Unpack(packed); flags&types.FlagTombstone != 0 {
		return rowindex.Nunca
	}
	ms, conhecido := s.escritas.Get(rowID)
	if !conhecido {
		return rowindex.Carencia
	}
	d := s.duracaoEfetiva(rowID)
	if d <= 0 {
		return rowindex.Nunca
	}
	venc := ms + d.Milliseconds()
	if venc < ms {
		// Estouro: instante somado a uma duração absurda. Uma linha que
		// vencesse "antes de ter sido escrita" seria expirada na primeira
		// descida, apagando dado vivo por causa de uma aritmética. Fora da
		// agenda é o lado seguro — prazoVencido continua respondendo sobre
		// ela.
		return rowindex.Nunca
	}
	if venc < int64(rowindex.PrimeiroVencimentoReal) {
		// Um instante tão antigo que colidiria com os sentinelas. Elevar é
		// seguro: o efeito é vencer em 1970 em vez de 1969.
		return rowindex.PrimeiroVencimentoReal
	}
	return uint64(venc)
}

// infoDaAgenda descreve, para o manifest, os parâmetros de que a agenda em
// disco deriva. Devolve nil no dataset sem expiração — e é isso que mantém o
// manifest dele na v1.
func (s *Store) infoDaAgenda() *checkpoint.ScheduleInfo {
	if s.agenda == nil {
		return nil
	}
	return &checkpoint.ScheduleInfo{
		FallbackDurationMs: uint64(s.duracaoDeFundo.Milliseconds()),
		BlockShift:         rowindex.AgendaBlockShift,
		Levels:             rowindex.AgendaNiveis,
	}
}

// duracaoEfetiva devolve por quanto tempo esta linha pode ficar sem escrita.
//
// Entrada DESCONHECIDA cai na duração de FUNDO, e é isso que faz a adoção das
// regras num dataset já materializado não mudar comportamento: as entidades
// que não têm o campo de prazo no documento continuam expirando pelo prazo que
// a regra de fundo declara, e passam a obedecer as condicionais na primeira
// escrita seguinte.
//
// Zero devolvido COM conhecido=true é a entidade imortal, e ela NÃO cai no
// fundo — é para essa distinção que o rowindex.PrazoImortal existe.
func (s *Store) duracaoEfetiva(rowID types.RowID) time.Duration {
	if s.prazos == nil {
		return 0
	}
	if ms, conhecido := s.prazos.Get(rowID); conhecido {
		return time.Duration(ms) * time.Millisecond
	}
	return s.duracaoDeFundo
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
		// A chave canônica do índice é a MESMA em todo caminho — ingestão,
		// reconstrução e consulta —, então validá-la aqui é o que impede o dano
		// fatal descrito acima. O conjunto consultado é o camposIndexados, e não
		// o mapa de WIPs: durante a janela de rotação em que os WIPs estão
		// fechados o mapa fica vazio, e validar por ele deixaria passar
		// justamente o record que a reconstrução seguinte não conseguiria
		// indexar.
		if slices.Contains(s.camposIndexados, id) {
			if err := valuekey.Validate(v); err != nil {
				nome, _ := s.dict.Name(id)
				return fmt.Errorf("%w: campo %d (%q) é indexado: %w", ErrInvalidArgument, id, nome, err)
			}
		}
	}
}

// insertValidator devolve o ValidateFunc de colisão do Insert. Roda sob o
// RLock da primary — não reentra no índice (ver lookup). Usa um buffer
// próprio para não invalidar o prevPayload em uso pelo Put.
func (s *Store) insertValidator(key []byte) primary.ValidateFunc {
	return func(rid types.RowID) (bool, error) {
		// Par (Get, Read) sob a barreira de aposentadoria: ver o godoc de
		// aposMu. Este validador roda com o wmu do Put na mão, e a ordem
		// wmu→aposMu é a invariante que o godoc fixa.
		s.aposMu.RLock()
		defer s.aposMu.RUnlock()
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

// liveIterator percorre lazy as rows vivas até o limite fixado na criação.
type liveIterator struct {
	s *Store
	// snap é o universo da varredura, e ele é COPIADO para dentro do iterador
	// porque o iterador sobrevive à chamada que o criou: o teto, o piso e o
	// relógio têm de ser os do instante da abertura em todo avanço, inclusive
	// no último.
	snap  types.Snapshot
	next  types.RowID
	limit types.RowID
	cur   types.RowID
	err   error
}

// Next reentra no Store a cada avanço, e não uma vez só na criação: o
// iterador SOBREVIVE à chamada que o criou, e um Close no meio da varredura
// desmapearia o índice sob os seus pés. Segurar o lock de vida pela vida do
// iterador seria pior — um consumidor lento bloquearia o Close para sempre.
func (it *liveIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.s == nil {
		return false
	}
	if err := it.s.entra(); err != nil {
		it.err = err
		return false
	}
	defer it.s.sai()
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
		if it.s.prazoVencido(id, it.snap.Instant) || it.s.purgada(id) {
			continue
		}
		it.cur = id
		return true
	}
	return false
}

func (it *liveIterator) RowID() types.RowID { return it.cur }
func (it *liveIterator) Err() error         { return it.err }
func (it *liveIterator) Close() error       { return nil }

// hashIterator embrulha o iterador do índice por segmento para que um Close do
// dataset no meio da varredura devolva ErrDatasetClosed em Err(), sem panic.
//
// A razão é a MESMA do liveIterator: o iterador SOBREVIVE à chamada que o criou
// e as fontes seladas e a do locations leem o mmap a CADA avanço, então sem a
// reentrada o Close desmaparia a região sob os pés da varredura e o bounds check
// (que passa a comparar contra size=0) viraria pânico. Some disso a
// uniformidade de contrato: o executor mistura as fontes no mesmo
// intersectIter/unionIter, e uma que seguisse produzindo linhas de um dataset
// fechado enquanto a outra para seria a pior forma de descobrir a divergência.
//
// Só o Next reentra. RowID lê um campo já materializado, e o Close das fontes
// não toca região nenhuma — o dono dos mapeamentos é o Store.
//
// O embrulho do índice bitmap saiu daqui junto com o motor que ele servia: com
// um Lookup só respondendo pelo índice por segmento, dois embrulhos idênticos
// seriam duas cópias de uma regra só.
//
// O campo é a INTERFACE, e não o iterador de um motor: a união de fontes da
// §3.3 devolve concatenação, merge k-way ou vazio conforme os dados, e amarrar
// um tipo concreto aqui obrigaria o embrulho a mudar junto com a estratégia.
type hashIterator struct {
	s   *Store
	it  types.RowIDIterator
	err error
}

func (it *hashIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if err := it.s.entra(); err != nil {
		it.err = err
		return false
	}
	defer it.s.sai()
	return it.it.Next()
}

func (it *hashIterator) RowID() types.RowID { return it.it.RowID() }

func (it *hashIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Err()
}

func (it *hashIterator) Close() error { return it.it.Close() }

// ScanLive devolve um iterador lazy das rows vivas (rowIds crescentes,
// location válida — overlay incluso — e sem tombstone). Rows criadas após a
// chamada não aparecem (snapshot do limite superior).
func (s *Store) ScanLive(snap types.Snapshot) types.RowIDIterator {
	if err := s.entra(); err != nil {
		return &liveIterator{err: err}
	}
	defer s.sai()
	// A varredura começa na marca-d'água, e não em zero: no histórico o
	// prefixo purgado pode ser de dezenas de milhões de rowIds, e percorrê-los
	// para descartar cada um seria pagar pela retenção em toda consulta.
	return &liveIterator{s: s, snap: snap, next: snap.BaseRowID, limit: snap.LimitRowID}
}

// DiretorioDeSpill é onde o motor de consulta pode transbordar o que não cabe
// na memória.
//
// Dentro do dataset, e não em os.TempDir, por três razões: fica sob o mesmo
// flock exclusivo, então nenhum outro processo pode confundir os arquivos;
// herda a política de filesystem do dataset (fallocate real, e não tmpfs); e o
// operador que dimensionou o disco pelo §85 não precisa dimensionar um segundo
// lugar.
func (s *Store) DiretorioDeSpill() string {
	return filepath.Join(s.opts.Dir, spill.NomeDoDiretorio)
}

// Lookup consulta o índice do campo; valor nunca visto devolve iterador vazio;
// campo sem índice POR SEGMENTO é ErrInvalidArgument. O resultado é um SUPERSET
// de candidatos — rows tombstoned, sem location, rows que apenas COLIDEM no
// hash do valor e, nos segmentos sem índice, as linhas que o segmento inteiro
// enumera. Valide liveness e predicate via GetByRowID (§44).
//
// É UM método, e não mais o par LookupBitmap/LookupHash: os dois já delegavam
// para o mesmo lookupIndexado desde que passou a existir um motor só (spec D5),
// e a §3.1 é o ponto em que as duas assinaturas colapsam. Enquanto o planner
// ainda distinguia os motores, manter os dois nomes era o que permitia à suíte
// exercitar o motor novo sem uma linha de teste mudada; unificado o planner, o
// segundo nome não descreve mais nada que o primeiro não descreva.
//
// Os rowIds saem em ordem estritamente CRESCENTE. Isso é contrato, não
// detalhe: o intersectIter e o unionIter do executor assumem iteradores
// ascendentes, e o planner mistura os termos no mesmo IndexUnionPlan.
//
// O iterador devolvido é reentrante — um Close do dataset no meio da varredura
// a interrompe com ErrDatasetClosed em Err(), sem panic. Com o motor por
// segmento a proteção deixou de ser só uniformidade de contrato e voltou a ser
// literal: as fontes seladas e a do locations leem mmap a CADA avanço, então
// sem a reentrada o Close desmaparia a região sob os pés da varredura.
func (s *Store) Lookup(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	return s.lookupIndexado(field, v)
}

// Sequence devolve a última sequence aplicada à row (ok=false para row sem
// location publicada — a sequence só tem significado com estado visível).
// Pode estar momentaneamente À FRENTE do record visível: ela é gravada no
// passo 6 do Put, antes do publish (passo 7).
func (s *Store) Sequence(id types.RowID) (int64, types.Presence) {
	if err := s.entra(); err != nil {
		return 0, types.PresenceAbsent
	}
	defer s.sai()
	packed, ok := s.locs.Get(id)
	if !ok {
		return 0, types.PresenceAbsent
	}
	// A sequence de uma row expirada VOLTA, e isso é o débito 25. Escondê-la
	// atrás de um ok=false repetiria na outra porta o defeito do GetByKey:
	// é exatamente ela que o §58 manda o tombstone preservar como
	// lastSequence, e é ela que impede um evento antigo de reabrir a
	// entidade. Quem pergunta a sequence de uma row está decidindo sobre
	// ordem, e "não sei" é a pior resposta possível.
	if _, _, flags := rowindex.Unpack(packed); flags&types.FlagTombstone != 0 || s.prazoVencido(id, s.agora()) {
		return s.sequenceDe(id), types.PresenceExpired
	}
	return s.sequenceDe(id), types.PresenceLive
}

// Sync força fdatasync do segmento corrente (loop de durability batch).
// Serializado com Put/Checkpoint pelo mutex do writer. Falha marca o writer
// como FALHO: durabilidade comprometida exige reabertura com recovery.
func (s *Store) Sync() error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
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
// com o writer, e o "seguro" aqui é forte: Stats NÃO toma o wmu, então nunca
// espera o writer. RowCount conta rows CRIADAS — não decrementa em
// tombstone; a contagem de vivas é semântica da S14.
//
// Tudo o que sai daqui vem de leitura atômica, inclusive os campos de disco.
// A alternativa — varrer segTally/segSize sob o wmu, que são writer-only e
// não se leem sem sincronizar — foi TENTADA e desfeita: o Checkpoint segura
// o wmu do início ao fim, com histograma que vai até 30s por ser esse o
// intervalo padrão do §78, e todo scrape de /metrics e toda chamada de
// /v1/stats passariam a parar atrás dele. O painel cegaria exatamente sob a
// pressão que ele existe para observar. A varredura mudou de lugar, não de
// forma: agora roda na reconciliação do checkpoint, que já está sob o lock, e
// deixa o resultado nos espelhos.
//
// Os espelhos são três atômicos independentes: um leitor pode pegar um deles
// de antes e outro de depois do mesmo checkpoint. A ressalva está na
// declaração dos campos, junto do motivo de ela ser aceitável.
func (s *Store) Stats() Stats {
	if err := s.entra(); err != nil {
		return Stats{}
	}
	defer s.sai()
	segID := s.curSegID.Load()
	var lf float64
	if s.prim != nil {
		if slots := s.prim.SlotCount(); slots > 0 {
			lf = float64(s.prim.Used()) / float64(slots)
		}
	}
	// SegmentsTotal vem do espelho do tally, e não do segSize, porque o tally
	// é o mapa que anda nos DOIS relógios: ganha entrada no applyRecord, então
	// uma rotação aparece na contagem no instante em que acontece, e é
	// reconciliado com o ListSegments a cada checkpoint (semeadura do segmento
	// que o writer nunca tocou, poda do que o diretório não lista mais).
	// Espelhar o segSize perderia o primeiro relógio e quebraria o
	// TestRotacaoDeSegmentoNoPut, que exige a rotação visível sem checkpoint;
	// espelhar o tally SEM a reconciliação perde o segundo, que é o que faz um
	// segmento aposentado pela compaction (§53-55) sumir da contagem.
	return Stats{
		RowCount:               s.rowCount.Load(),
		SegmentsTotal:          s.segCount.Load(),
		CurrentSegmentID:       segID,
		PrimaryLoadFactor:      lf,
		OverlayLen:             s.locs.OverlayLen(),
		StorageBytes:           s.diskBytesAll.Load(),
		LiveBytesUpperBound:    s.liveBytesAll.Load(),
		CompactionRunning:      s.compAtiva.Load(),
		ExpiryRunning:          s.expAtiva.Load(),
		Expiradas:              s.expiradas.Load(),
		Carencias:              s.carencias.Load(),
		ExpiryExaminadas:       s.examinadas.Load(),
		CompactionBytesRead:    s.compBytesLidos.Load(),
		CompactionBytesWritten: s.compBytesEscritos.Load(),
	}
}

// Close fecha o dataset SEM checkpoint automático: é responsabilidade do
// caller chamar Checkpoint() antes, se quiser durabilizar o delta. Close
// não drena o overlay nem faz msync/fsync de nada — o que não foi
// checkpointado volta apenas pelo tail scan da recovery (S12), a partir do
// último manifest.
// Close faz QUIESCE antes de liberar qualquer recurso: toma o lock de vida em
// escrita, o que só acontece quando nenhuma operação está em voo, e marca o
// Store como fechado para que nenhuma nova entre.
//
// Sem isso, fechar com um Put ou uma leitura em andamento desmapeava o índice
// sob os pés da operação e produzia PANIC — que a política de log-and-skip da
// ingestão não captura, então derrubava o processo. E o flock do dataset era
// devolvido no mesmo instante, com escrita em voo: outro processo podia
// assumir o dataset enquanto o primeiro ainda escrevia nele.
//
// É idempotente: o segundo Close devolve nil sem tocar em nada.
func (s *Store) Close() error {
	// A compaction é cancelada e AGUARDADA antes do quiesce (§53-55, decisão
	// D7). O vida.Lock() abaixo só é concedido quando nenhuma operação está em
	// voo, e a varredura do §54 é a operação mais longa que existe no produto:
	// sem o join aqui, ou o Close espera a passada inteira, ou — pior — vence
	// a corrida e desmapeia o índice sob os pés de quem está no meio de
	// renomear e apagar arquivo.
	s.ParaCompaction()
	// O coletor sai pela MESMA razão da compaction: ele toma o wmu por trecho
	// e lê o mmap do índice de escritas, então um Close que vencesse a corrida
	// desmapearia a região sob os pés da passada.
	s.ParaExpiracao()
	// E a retenção pela terceira vez a mesma razão: a passada dela renomeia e
	// apaga arquivo, e lê o segR por pread na busca binária.
	s.ParaRetencao()
	// A reconstrução é a quarta, e nela a razão é a mais direta das quatro: a
	// varredura de um segmento inteiro é uma leitura pelo s.segR, que o
	// fechaIndices/segR.Close logo abaixo fecha inteiro. Sem o join aqui, ela
	// colheria os.ErrClosed no meio de até 512 MiB de leitura — e o laço
	// anunciaria como falha o que é o encerramento normal do processo.
	s.ParaReconstrucao()
	// E a aposentadoria de grupos, a quinta: a passada toma o wmu e escreve no
	// dicionário do contador, que o Close fecha logo abaixo.
	s.ParaAposentadoriaDeGrupos()

	s.vida.Lock()
	defer s.vida.Unlock()
	if s.fechado {
		return nil
	}
	s.fechado = true

	// O writer também é aguardado: uma operação de escrita que já passou pelo
	// entra() e está dentro do wmu termina antes daqui, porque o Lock acima só
	// é concedido quando o RLock dela é liberado.
	var errs []error
	errs = append(errs, s.fechaIndices())
	if s.agenda != nil {
		errs = append(errs, s.agenda.Close())
	}
	// Sem msync no Close, como nos outros derivados: o que não foi
	// checkpointado volta pelo replay, e o overlay dos contadores é
	// exatamente o que a dobra + replay repõem.
	for _, c := range s.contadores {
		errs = append(errs, c.Close())
	}
	if s.prazos != nil {
		errs = append(errs, s.prazos.Close())
	}
	if s.escritas != nil {
		errs = append(errs, s.escritas.Close())
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
	// Só fecha o dicionário que ele mesmo abriu: o compartilhado pertence ao
	// dataset base, que segue ingerindo.
	if s.dict != nil && s.dictProprio {
		errs = append(errs, s.dict.Close())
	}
	// O lock sai por ÚLTIMO: enquanto qualquer arquivo do dataset estiver
	// aberto aqui, outro processo não pode assumi-lo.
	errs = append(errs, s.lock.release())
	return errors.Join(errs...)
}

// RowCount é a contagem de rows materializadas.
//
// Existe separado do Stats porque é o sinal que a ingestão Kafka usa para
// separar "dataset novo" de "dataset com dados e sem posição conhecida" — e
// esse consumidor não deveria depender do tipo Stats inteiro só para ler um
// contador.
func (s *Store) RowCount() uint64 { return s.rowCount.Load() }

// validaDerivadasContraDicionario recusa uma coluna extraída NOVA num dataset
// que já tem linhas.
//
// A extração roda na ingestão: as linhas gravadas antes de a coluna existir
// não a têm, e um filtro sobre ela as omitiria — sem erro, sem log e sem
// métrica. Era o mesmo defeito que a validação do conjunto indexado contra o
// manifest recusava, e ela saiu — mas por uma razão que NÃO vale aqui: o índice
// de um segmento sem a seção do campo enumera as linhas dele e a consulta
// responde certo, enquanto a coluna extraída simplesmente NÃO EXISTE nos records
// já gravados. Não há o que enumerar, e por isso esta recusa fica.
//
// O critério é a AUSÊNCIA no dicionário, e não a mera presença de linhas: o
// dataset criado já com extract tem a coluna registrada desde a primeira
// subida, e recusá-lo faria a feature ser inutilizável depois do primeiro
// evento.
func validaDerivadasContraDicionario(m *checkpoint.Manifest, dict *fielddict.Dict, derivadas []string) error {
	if len(derivadas) == 0 || m == nil || m.RowCount == 0 {
		return nil
	}
	var novas []string
	for _, nome := range derivadas {
		if _, existe := dict.Lookup([]byte(nome)); !existe {
			novas = append(novas, nome)
		}
	}
	if len(novas) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: coluna(s) %v declarada(s) em extract num dataset que já tem %d linhas; "+
			"a extração roda na ingestão, então as linhas antigas ficariam sem a coluna e o filtro sobre ela "+
			"as omitiria em silêncio. Declare extract na CRIAÇÃO do dataset, ou recrie este",
		ErrDatasetCorrupt, novas, m.RowCount)
}

// validaOpcoesDoModo recusa as combinações que produziriam um dataset
// histórico malformado. As duas recusas são de CONFIGURAÇÃO e saem antes do
// flock: um Open que vai falhar não pode travar o dataset nem tocar arquivo.
func validaOpcoesDoModo(opts Options) error {
	if opts.Modo != ModoHistorico {
		return nil
	}
	if opts.DictCompartilhado == nil {
		// O histórico não tem dicionário próprio por desenho: o payload que
		// ele guarda é o que o patch do base montou, endereçado por FieldID.
		// Sem o dicionário do base, os ids seriam outros e todo record
		// decodificaria colunas trocadas — sem erro e com o CRC batendo.
		return fmt.Errorf("%w: ModoHistorico exige DictCompartilhado: a tabela irmã não tem dicionário próprio", ErrInvalidArgument)
	}
	if opts.TTLAtivo {
		// A expiração por inatividade renova o prazo a cada escrita, e no
		// histórico nenhuma linha é reescrita: a regra nunca teria a segunda
		// escrita que ela existe para observar. A retenção daqui é outra, e é
		// por segmento.
		return fmt.Errorf("%w: ModoHistorico não tem expiração por inatividade; a retenção é por tempo de escrita e por tamanho", ErrInvalidArgument)
	}
	return nil
}

// EhHistorico reporta se este Store é a tabela irmã append-only.
func (s *Store) EhHistorico() bool { return s.opts.Modo == ModoHistorico }

// sequenceDe lê o índice de sequences, ou zero quando ele não existe.
//
// Zero é o valor certo e não um placeholder: a tabela histórica não declara
// sequence, e o stale check do materializer só compara quando o schema tem
// uma. Quem chama Sequence sobre o histórico está perguntando por algo que a
// tabela não guarda, e a resposta honesta é o zero que o formato já usa para
// "sem sequence".
func (s *Store) sequenceDe(id types.RowID) int64 {
	if s.seqs == nil {
		return 0
	}
	return s.seqs.Get(id)
}

// adotaModoDoManifest confere o modo declarado nas Options contra o que está
// gravado, e semeia a marca-d'água de retenção.
//
// A divergência é ErrDatasetCorrupt, e não um aviso, porque as duas direções
// são catastróficas em silêncio. Abrir um histórico como materializado leria
// um log de eventos como se fosse estado corrente: cada entidade apareceria
// uma vez por evento, e o operador teria uma tabela plausível e errada. Abrir
// um materializado como histórico é pior ainda — a primary não seria aberta,
// e o primeiro Apenda gravaria uma segunda linha para uma key que já existe,
// duplicando a entidade de forma durável.
//
// Manifest ausente é dataset NOVO: ele nasce com o modo que as Options pedem,
// e o primeiro Checkpoint o grava.
func (s *Store) adotaModoDoManifest(m *checkpoint.Manifest) error {
	if m == nil {
		return nil
	}
	gravado := Modo(m.Mode)
	if gravado != s.opts.Modo {
		return fmt.Errorf("%w: o dataset %s está gravado como %s e a configuração pede %s; "+
			"o modo é imutável — recrie o dataset ou corrija o YAML",
			ErrDatasetCorrupt, s.opts.Dir, gravado, s.opts.Modo)
	}
	s.rowIDBase.Store(m.RowIDBase)
	return nil
}

// RowIDBase é a marca-d'água de retenção corrente. Zero fora do histórico.
func (s *Store) RowIDBase() types.RowID { return types.RowID(s.rowIDBase.Load()) }

// purgada reporta que o rowId ficou abaixo da marca-d'água e não é mais
// visível.
//
// É o quinto ponto de liveness do Store, ao lado dos quatro que o prazo já
// tinha, e o mais barato de todos: uma comparação contra um atômico, sem
// tocar índice nenhum.
func (s *Store) purgada(id types.RowID) bool {
	return uint64(id) < s.rowIDBase.Load()
}

// Apenda grava uma linha nova na tabela histórica e devolve o rowId dela.
// Exclusivo do writer.
//
// É método próprio, e não um Put com key vazia, pela mesma razão que Expira
// também é: o Put resolve a key pela primary e DECIDE entre criar e atualizar.
// Aqui não há decisão nenhuma a tomar — toda chamada é uma linha nova. Passar
// por ele obrigaria a inventar uma key sintética por evento, que a primary
// indexaria para nunca ser consultada: um hash de 16 bytes por linha, com
// rehash a 70% de ocupação, sobre a tabela que mais cresce do produto.
//
// O que ele NÃO faz, e cada ausência é o desenho e não uma economia: não
// procura estado anterior (não há), não decrementa liveness de segmento (sem
// update não há garbage), não insere na primary (não existe) e não passa pela
// fase de CLEAR dos índices secundários (nada sai de um índice numa tabela em
// que a linha nunca muda).
//
// Erros: antes do Append nada foi gravado. Depois dele não há rollback
// parcial, e o writer entra em modo FALHO sticky como em qualquer outra
// escrita. Quem chama trata a falha como degradação do histórico, e não da
// ingestão: o produto é o dataset base.
func (s *Store) Apenda(payload []byte) (types.RowID, error) {
	if err := s.entra(); err != nil {
		return 0, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if !s.EhHistorico() {
		return 0, fmt.Errorf("%w: %s é um dataset materializado; use Put", ErrInvalidArgument, s.opts.Dataset)
	}
	if s.stickyErr != nil {
		return 0, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	// Validação ANTES de queimar um rowId, pela razão do Put: ids nunca são
	// reutilizados, e validar depois deixaria buracos no intervalo do
	// ScanLive proporcionais ao ruído da fonte.
	if _, err := encoding.ValidateRecord(nil, payload); err != nil {
		return 0, err
	}
	if err := s.validatePayloadAgainstDataset(payload); err != nil {
		return 0, err
	}
	rowID := types.RowID(s.nextRowID.Add(1) - 1)

	rec, err := encoding.BuildRecord(s.buildBuf[:0], rowID, 0, 0, nil, payload)
	s.buildBuf = rec
	if err != nil {
		return 0, err
	}
	seg, off, err := s.segW.Append(rec)
	if err != nil {
		return 0, s.failWriter(err)
	}
	if err := s.aposAppend(seg); err != nil {
		return 0, s.failWriter(err)
	}
	if err := s.applyRecord(recordApply{
		rowID:       rowID,
		payload:     payload,
		hadPrevious: false,
		countsAsNew: true,
		seg:         seg,
		off:         off,
		recLen:      len(rec),
	}); err != nil {
		return 0, s.failWriter(err)
	}
	if s.opts.SyncEveryPut {
		if err := s.syncLocked(); err != nil {
			return rowID, err
		}
	}
	return rowID, nil
}
