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

	"github.com/nishisan-dev/nishidb/internal/index/bitmap"
	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
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

	// ErrDatasetClosed indica operação sobre um Store já fechado. É ERRO, e
	// não panic, porque o chamador (ingestão, query) tem política para erro e
	// não tem para panic.
	ErrDatasetClosed = errors.New("storage: dataset fechado")

	ErrWriterFailed = types.ErrWriterFailed
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
	// ExpireAfterWrite liga o idle timeout: a row deixa de ser visível quando
	// passa este tempo sem escrita. Zero desliga tudo — nem o índice é aberto.
	ExpireAfterWrite time.Duration
	// CampoDeEscrita é o NOME do campo de sistema que carrega o instante da
	// escrita, resolvido pelo dicionário do próprio dataset durante o Open.
	// Obrigatório quando ExpireAfterWrite > 0: é dele que o applyRecord tira
	// o valor, tanto no Put quanto no replay do tail, e é por isso que o
	// índice de expiração é reconstruível depois de um crash.
	//
	// Nome e não FieldID pela mesma razão do BitmapFieldNames: os ids nascem
	// de schema.Compile, que precisa do dicionário — e o dicionário vive
	// dentro do Store. Exigir o id aqui obrigaria a abrir o dataset sem
	// expiração, compilar, fechar e reabrir.
	CampoDeEscrita string
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
// thread-safe entre si); GetByKey/GetByRowID/ScanLive/LookupBitmap/Sequence/
// Stats são seguros para leitores concorrentes com o writer.
type Store struct {
	opts Options

	dict *fielddict.Dict
	segW *segment.Writer
	segR *segment.Reader
	prim *primary.Index
	locs *rowindex.Locations
	seqs *rowindex.Sequences
	// escritas é nil quando o dataset não declara expiração: sem ela não há
	// o que indexar, e abrir o arquivo custaria mmap por nada.
	escritas         *rowindex.Escritas
	expireAfterWrite time.Duration
	campoDeEscrita   types.FieldID
	// expiraBuf e tombBuf são do Expira, owned pelo wmu como o compactBuf:
	// reter os dois evita realocar o record lido e o payload da lápide a cada
	// expiração, e a varredura expira em rajada.
	expiraBuf  []byte
	tombBuf    []byte
	expiradas  atomic.Uint64
	carencias  atomic.Uint64
	examinadas atomic.Uint64
	// cursorExpiracao é onde a próxima passada do coletor recomeça. Só o
	// laço do coletor o toca, e sempre sob o entra()/sai().
	cursorExpiracao types.RowID

	// Ciclo de vida do coletor, no molde do da compaction: expMu guarda o
	// par cancela/emVoo, expAtiva é o medidor observável e expEmVoo é o que
	// o Close aguarda antes do quiesce.
	expMu      sync.Mutex
	expCancela context.CancelFunc
	expEmVoo   sync.WaitGroup
	expAtiva   atomic.Bool
	// agora é o relógio da expiração. Campo, e não time.Now direto, porque um
	// teste de prazo precisa avançar o tempo sem dormir.
	agora   func() time.Time
	bitmaps map[types.FieldID]*bitmap.Index

	nextRowID atomic.Uint64
	rowCount  atomic.Uint64
	epoch     uint64 // último epoch de manifest gravado (writer-only)

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
	// clearFailures conta erros tolerados na fase de Clear dos bitmaps
	// (deixam falso positivo, filtrado pelo §44); exposto via métricas na S13.
	clearFailures atomic.Uint64
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
	// (nomes.go). São atômicos e não campos sob o wmu pelo motivo dos
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
	// clearErrHook/setErrHook são seams de teste das fases de Clear e Set
	// dos bitmaps (nil em produção; setErrHook viabiliza o phantom commit
	// com payload replayável).
	clearErrHook func(types.FieldID) error
	setErrHook   func(types.FieldID) error

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
		opts.Log = logging.New(os.Stderr, logging.FormatoTexto, slog.LevelInfo)
	}
	opts.Log = opts.Log.Component("storage").Dataset(opts.Dataset)
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
		segSize:  make(map[uint32]int64),
		agora:    time.Now,
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
	if opts.ExpireAfterWrite > 0 {
		if opts.CampoDeEscrita == "" {
			// Sem o campo o applyRecord não teria de onde tirar o instante, e
			// toda row ficaria com prazo desconhecido — a expiração nunca
			// aconteceria, em silêncio.
			return nil, fmt.Errorf("%w: ExpireAfterWrite exige CampoDeEscrita", ErrInvalidArgument)
		}
		if s.escritas, err = rowindex.OpenEscritas(filepath.Join(idxDir, "expiry.idx"), minRows); err != nil {
			return nil, err
		}
		s.expireAfterWrite = opts.ExpireAfterWrite
	}
	m, mErr := checkpoint.Load(s.manifestPath())
	refs, err := segment.ListSegments(dataDir)
	if err != nil {
		return nil, err
	}

	if s.escritas != nil {
		// LookupOrCreate e não Lookup: num dataset novo o campo ainda não
		// existe, e criá-lo aqui é o que garante que o id seja o MESMO que o
		// schema.Compile vai resolver depois — os dois passam pelo mesmo
		// dicionário durável.
		id, err := s.dict.LookupOrCreate([]byte(opts.CampoDeEscrita))
		if err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDeEscrita, err)
		}
		s.campoDeEscrita = id
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
	// Offsets ANTES do checkpoint imediato: o Checkpoint reescreve o
	// kafka.offsets a partir do estado em memória, então carregar depois
	// apagaria a posição na primeira reabertura do dataset.
	s.carregaOffsets()

	// Checkpoint imediato: manifest desde a criação e estado limpo pós-
	// recovery (novo S = tail; epoch nunca regride).
	if err := s.Checkpoint(); err != nil {
		return nil, err
	}
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
	// Ordem CANÔNICA antes de resolver: o LookupOrCreate queima os primeiros
	// IDs do dicionário na ordem em que é chamado, então a ordem do slice
	// decidiria o mapeamento nome→ID de um dataset novo — e ele é gravado para
	// sempre. Ordenar aqui, e não em cada chamador, é o que torna a garantia
	// independente de quem abre (o CLI monta a lista iterando um map, e o
	// resultado seria sorteado a cada criação de dataset).
	//
	// Cópia: o slice é do chamador, e reordená-lo no lugar seria efeito
	// colateral de uma função que promete só abrir o dataset.
	nomes := slices.Clone(opts.BitmapFieldNames)
	slices.Sort(nomes)
	for _, nome := range nomes {
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
		// A liveness tem DUAS causas de morte: a lápide já publicada e o
		// prazo vencido que o coletor ainda não recolheu. Contá-las juntas
		// aqui é o que faz a visibilidade não depender do agendador.
		live = locFlags&types.FlagTombstone == 0 && !s.prazoVencido(rid)
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
	if err := s.entra(); err != nil {
		return types.Record{}, buf, false, err
	}
	defer s.sai()
	rec, out, exists, live, err := s.lookup(key, buf)
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
func (s *Store) Estado(key, buf []byte) (types.Record, []byte, types.Presenca, error) {
	if err := s.entra(); err != nil {
		return types.Record{}, buf, types.PresencaAusente, err
	}
	defer s.sai()
	rec, out, exists, live, err := s.lookup(key, buf)
	switch {
	case err != nil:
		return types.Record{}, out, types.PresencaAusente, err
	case !exists:
		return types.Record{}, out, types.PresencaAusente, nil
	case !live:
		// O record VOLTA, e não zerado: RowID e Sequence são o que o stale
		// check e a ressurreição precisam.
		return rec, out, types.PresencaExpirada, nil
	}
	return rec, out, types.PresencaViva, nil
}

// GetByRowID devolve o record corrente da row. ok=false para row sem
// location ou tombstoned (checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if err := s.entra(); err != nil {
		return types.Record{}, buf, false, err
	}
	defer s.sai()
	packed, ok := s.locs.Get(id)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	seg, off, locFlags := rowindex.Unpack(packed)
	if locFlags&types.FlagTombstone != 0 || s.prazoVencido(id) {
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
	if err := s.entra(); err != nil {
		return 0, false, err
	}
	defer s.sai()
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

// instanteDeEscrita extrai o campo de sistema do payload. Ausente devolve
// ok=false, que o índice registra como DESCONHECIDO — nunca como vencido.
func instanteDeEscrita(payload []byte, campo types.FieldID) (int64, bool) {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return 0, false
		}
		if id == campo {
			return v.I64, v.Type == types.TypeTimestamp
		}
		if id > campo {
			return 0, false // payload é ordenado por FieldID: passou, não tem
		}
	}
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
func (s *Store) prazoVencido(rowID types.RowID) bool {
	if s.escritas == nil {
		return false
	}
	ms, conhecido := s.escritas.Get(rowID)
	if !conhecido {
		return false
	}
	return s.agora().UnixMilli()-ms >= s.expireAfterWrite.Milliseconds()
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
	// CarregaI64 e não um switch local: tipo de largura fixa esquecido no
	// ramo de bytes compara nil com nil e devolve true para valores
	// diferentes, e o diff conclui em silêncio que o campo não mudou.
	if a.Type.CarregaI64() {
		return a.I64 == b.I64
	}
	return bytes.Equal(a.Bytes, b.Bytes)
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
		if it.s.prazoVencido(id) {
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

// bitmapIterator embrulha o iterador do índice bitmap pelo MESMO motivo do
// liveIterator: ele sobrevive à chamada que o criou e lê o mmap a cada avanço,
// então precisa reentrar no Store a cada Next — devolver o iterador cru deixava
// o Close desmapear a região sob os pés da varredura, e o bounds check da
// região (que passa a comparar contra size=0) virava panic.
//
// Só o Next reentra. RowID lê um campo já materializado, e o Close do iterador
// de bitmap não toca a região.
type bitmapIterator struct {
	s   *Store
	it  *bitmap.Iterator
	err error
}

func (it *bitmapIterator) Next() bool {
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

func (it *bitmapIterator) RowID() types.RowID { return it.it.RowID() }

func (it *bitmapIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Err()
}

func (it *bitmapIterator) Close() error { return it.it.Close() }

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
	if err := s.entra(); err != nil {
		return &liveIterator{err: err}
	}
	defer s.sai()
	return &liveIterator{s: s, limit: types.RowID(s.nextRowID.Load())}
}

// LookupBitmap consulta o índice bitmap do campo; valor nunca visto devolve
// iterador vazio; campo não indexado é erro. O resultado é um SUPERSET de
// candidatos: rows tombstoned/sem location podem aparecer — valide liveness
// e predicate via GetByRowID (spec §44).
//
// O iterador devolvido é reentrante: um Close do dataset no meio da varredura
// a interrompe com ErrDatasetClosed em Err(), sem panic.
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
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
	return &bitmapIterator{s: s, it: it}, nil
}

// Sequence devolve a última sequence aplicada à row (ok=false para row sem
// location publicada — a sequence só tem significado com estado visível).
// Pode estar momentaneamente À FRENTE do record visível: ela é gravada no
// passo 6 do Put, antes do publish (passo 7).
func (s *Store) Sequence(id types.RowID) (int64, types.Presenca) {
	if err := s.entra(); err != nil {
		return 0, types.PresencaAusente
	}
	defer s.sai()
	packed, ok := s.locs.Get(id)
	if !ok {
		return 0, types.PresencaAusente
	}
	// A sequence de uma row expirada VOLTA, e isso é o débito 25. Escondê-la
	// atrás de um ok=false repetiria na outra porta o defeito do GetByKey:
	// é exatamente ela que o §58 manda o tombstone preservar como
	// lastSequence, e é ela que impede um evento antigo de reabrir a
	// entidade. Quem pergunta a sequence de uma row está decidindo sobre
	// ordem, e "não sei" é a pior resposta possível.
	if _, _, flags := rowindex.Unpack(packed); flags&types.FlagTombstone != 0 || s.prazoVencido(id) {
		return s.seqs.Get(id), types.PresencaExpirada
	}
	return s.seqs.Get(id), types.PresencaViva
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
	if slots := s.prim.SlotCount(); slots > 0 {
		lf = float64(s.prim.Used()) / float64(slots)
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
	for _, ix := range s.bitmaps {
		if ix != nil {
			errs = append(errs, ix.Close())
		}
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
	if s.dict != nil {
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
