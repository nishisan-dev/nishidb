package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Exit codes — CONTRATO com quem chama o binário de um script. Cada um
// corresponde a uma decisão diferente do operador: reprocessar, corrigir a
// query, investigar o dataset, reabrir, ou paginar.
const (
	ExitOK              = 0
	ExitUsageError      = 1 // erro de uso ou de configuração
	ExitQuery           = 2 // sintaxe ou bind da query
	ExitDataset         = 3 // dataset corrompido, travado ou inacessível
	ExitWriter          = 4 // writer em modo falho: reprocessar é inútil até reabrir
	ExitResultTruncated = 5 // resultado truncado pelo teto de maxRows
	ExitInterrupted     = 6 // interrompido por sinal
	// ExitDataLoss: a ingestão terminou e o dataset está íntegro, mas
	// eventos NÃO entraram por falha que não é do evento (§101, balde 2).
	// Distinto do 4: aqui não há modo falho, reprocessar depois de investigar
	// é útil. Distinto do 0: houve perda, e um pipeline que testa $? precisa
	// saber disso.
	ExitDataLoss = 7
	// ExitPolicyStopped: a ingestão PAROU porque a política do dataset
	// mandou (`errors.invalidEvent: stop`, §101).
	//
	// É código próprio, e não uma reciclagem do 7, porque a causa é outra e a
	// ação também: no 7 os eventos não entraram por falha NOSSA e a resposta é
	// investigar o motor; aqui a parada foi PEDIDA pela configuração diante de
	// um evento ruim, e a resposta é olhar o evento e decidir — possivelmente
	// pulando-o com `nishidb offsets`. Reusar o 7 mentiria sobre a causa.
	ExitPolicyStopped = 8
)

// CLIContext são as dependências de processo, injetadas para o teste rodar o
// CLI como função em vez de precisar de um binário compilado.
type CLIContext struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Ctx é cancelado no sinal de interrupção (o main liga no signal.Notify).
	Ctx context.Context
}

const usage = `nishidb — engine de materialização e consulta

USO
  nishidb ingest  --config <nishidb.yaml> --dataset <nome> --input <arquivo|-> [--checkpoint-every N]
  nishidb query   --config <nishidb.yaml> --dataset <nome> [--limit N] '<SQL>'
  nishidb query   --server <host:porta>    --dataset <nome> [--limit N] '<SQL>'
  nishidb server  --config <nishidb.yaml> [--log-format json|texto] [--log-level N] [--pprof <addr>]
  nishidb offsets --config <nishidb.yaml> --dataset <nome> (--show | --reset earliest|latest | --set p:o,...) [--yes]
  nishidb status   --server <host:porta>
  nishidb datasets --server <host:porta>
  nishidb inspect  --server <host:porta> <dataset>
  nishidb compact    --server <host:porta> [dataset]
  nishidb checkpoint --server <host:porta> [dataset]
  nishidb counters   (--config <nishidb.yaml> | --server <host:porta>) --dataset <nome> [--audit | --rebuild]
  nishidb --version | version

INGEST
  Lê NDJSON (um evento JSON por linha) e materializa no dataset.
  --checkpoint-every N   checkpoint a cada N eventos (0 desliga, com aviso).
                         O padrão é ligado: sem checkpoint o consumo de memória
                         acompanha o tamanho do dataset.

QUERY
  Executa o SQL e escreve NDJSON no stdout, uma linha por row, em streaming.
  --limit N              sobrepõe o LIMIT da query. O maxRows do dataset
                         continua valendo como teto.
  --server <host:porta>  consulta pelo protocolo remoto (§67) em vez de abrir
                         o dataset local. É o caminho para consultar COM o
                         nishidb server no ar: o flock do dataset é exclusivo
                         por processo, e a consulta local sai com exit 3
                         enquanto o servidor estiver de pé. Exclusivo com
                         --config, que só vale para a consulta local.

SERVER
  Processo de longa duração: consultas e ingestão Kafka no MESMO processo.
  Consome os datasets com source.kafka.enabled: true; os demais ficam só
  consultáveis. Expõe POST /v1/query, GET /v1/metadata, GET /v1/stats,
  GET /v1/health e GET /metrics.
  --pprof <addr>         endpoints de debug num listener próprio. Modo
                         administrativo: não exponha esta porta.

OFFSETS
  Lê e reposiciona a posição no tópico. Exige o servidor PARADO (toma o
  flock do dataset). --reset e --set pedem --yes.

STATUS, DATASETS E INSPECT
  Leitura do servidor NO AR (§96), pelo mesmo protocolo remoto do
  "query --server". Não existe caminho local para eles: o flock do dataset é
  exclusivo por processo, e um diagnóstico que exigisse o servidor parado
  responderia sobre o produto que justamente não está rodando.
  status                 rows, segmentos, disco, garbage, compaction,
                         consultas e ingestão de cada dataset.
  datasets               o que este servidor serve.
  inspect <dataset>      colunas com tipo, alias e índice declarado.
  Os três são SÓ leitura.

COMPACT E CHECKPOINT
  As rotas MUTANTES do §96 (Fase 11, decisão D3 do roadmap), atrás do papel
  ADMIN — as primeiras que mudam estado do dataset pelo protocolo remoto.
  Síncronas: a saída só aparece depois do trabalho feito. [dataset] ausente
  atua sobre TODOS os datasets do servidor, na ordem em que ele subiu.
  compact [dataset]      dispara uma passada de compaction manual (funciona
                         com storage.compaction.enabled: false — é o modo
                         do operador que desligou o laço automático e opera
                         na mão). 409 se já houver uma passada em voo, do
                         laço ou de outra chamada.
  checkpoint [dataset]   força um checkpoint. Idempotente por EFEITO: repetir
                         sobre estado inalterado custa o stall real da
                         ingestão e não muda nada — não rode em loop apertado.

COUNTERS
  Os contadores mantidos na escrita (bloco counters: do YAML do dataset). A
  LEITURA dos números é SQL: SELECT olt, total FROM _counters.<nome>. Este
  comando responde "posso confiar neles?".
  (sem ação)             lista os contadores: grupos vivos, teto, aposentados,
                         recusados pelo teto, órfãs e a última auditoria.
  --audit                recalcula por varredura e compara com o mantido.
                         Com --server, corre sob um snapshot de leitura SEM
                         parar a ingestão. Divergência sai com exit 3;
                         auditoria inconclusiva (snapshot estourado) sai com 5.
  --rebuild              reconstrói os contadores varrendo as linhas vivas.
                         Segura o writer pela varredura inteira: a ingestão
                         para até acabar.
  --config exige o servidor PARADO (toma o flock do dataset, como offsets);
  --server usa as rotas ADMIN do servidor no ar.

CREDENCIAL E TLS (query --server, status, datasets, inspect, compact, checkpoint, counters --server)
  --user <nome>          usuário HTTP Basic (§74-77). A senha NUNCA é uma
                         flag — apareceria em "ps" para qualquer usuário da
                         máquina — e vem de exatamente UMA das duas fontes
                         abaixo; as duas ao mesmo tempo, ou --user sem
                         nenhuma delas, são erro de uso.
  NISHIDB_PASSWORD       a senha, por variável de ambiente.
  --password-file <path> a senha, por arquivo (quebra de linha final
                         aparada).
  --tls-ca <path>        CA (PEM) para validar o certificado do servidor —
                         liga TLS neste cliente. Sem --insecure: quem precisa
                         depurar um handshake usa openssl s_client.

EXIT CODES
  0  sucesso
  1  erro de uso ou de configuração
  2  erro de sintaxe ou de bind da query
  3  dataset corrompido, travado por outro processo, ou inacessível
  4  writer em modo falho (reabra o dataset; reprocessar antes disso é inútil)
  5  resultado truncado pelo teto de query.maxRows
  6  interrompido por sinal
  7  eventos não entraram por falha (perda de dado): investigue e reprocesse
  8  ingestão parada por errors.invalidEvent: stop — veja o evento e decida
`

// Run executa o CLI e devolve o exit code. Não chama os.Exit: quem decide
// terminar o processo é o main, e o teste precisa poder chamar isto direto.
func Run(args []string, amb CLIContext) int {
	if len(args) < 1 {
		fmt.Fprint(amb.Stderr, usage)
		return ExitUsageError
	}
	switch args[0] {
	case "ingest":
		return runIngest(args[1:], amb)
	case "query":
		return runQuery(args[1:], amb)
	case "server":
		return runServer(args[1:], amb)
	case "offsets":
		return runOffsets(args[1:], amb)
	case "status":
		return runStatus(args[1:], amb)
	case "datasets":
		return runDatasets(args[1:], amb)
	case "inspect":
		return runInspect(args[1:], amb)
	case "compact":
		return runCompact(args[1:], amb)
	case "checkpoint":
		return runCheckpoint(args[1:], amb)
	case "counters":
		return runCounters(args[1:], amb)
	case "--version", "version":
		_, _ = fmt.Fprintln(amb.Stdout, buildinfo.String())
		return ExitOK
	case "-h", "--help", "help":
		fmt.Fprint(amb.Stdout, usage)
		return ExitOK
	default:
		fmt.Fprintf(amb.Stderr, "comando desconhecido: %q\n\n%s", args[0], usage)
		return ExitUsageError
	}
}

// errorCode traduz o erro na decisão que o operador precisa tomar. A
// ordem importa: um erro pode casar mais de um sentinela, e o mais específico
// tem de vencer.
func errorCode(err error) int {
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ExitInterrupted
	case errors.Is(err, parser.ErrSyntax), errors.Is(err, binder.ErrBind),
		errors.Is(err, executor.ErrInvalidPlan):
		return ExitQuery
	case errors.Is(err, executor.ErrMaxRowsExceeded), errors.Is(err, storage.ErrExpiredSnapshot):
		// ErrSnapshotExpirado divide o 5 com o teto de linhas, e não ganha
		// código próprio, porque a DECISÃO do operador é a mesma: o resultado
		// que saiu está incompleto e não deve ser tratado como completo. O que
		// muda é o remédio — ali, subir o maxRows ou estreitar a projeção;
		// aqui, repetir (a consulta nova abre um snapshot que ainda não retém
		// nada) ou subir runtime.query.snapshotMaxBytes. Um código novo mudaria
		// a tabela de exit codes, que é contrato, para separar dois casos que
		// pedem o mesmo do script que os lê.
		return ExitResultTruncated
	case errors.Is(err, types.ErrWriterFailed):
		return ExitWriter
	case errors.Is(err, storage.ErrDatasetLocked), errors.Is(err, storage.ErrDatasetCorrupt),
		errors.Is(err, storage.ErrDatasetClosed), errors.Is(err, encoding.ErrCorrupt),
		errors.Is(err, schema.ErrObsoleteIndexSpelling),
		errors.Is(err, schema.ErrInvalidIndexWindow):
		// encoding.ErrCorrupt é o sentinela que chega de VERDADE quando um byte
		// da região durável está adulterado: a detecção é lazy, no read, e o
		// erro sobe pelo executor. Sem ele aqui, corrupção de dataset caía no
		// default e saía como "erro de uso" — o único código que manda o
		// operador olhar a linha de comando em vez do dataset.
		//
		// schema.ErrObsoleteIndexSpelling (D5: `index: hash`/`index: bitmap`
		// recusados) entra na MESMA categoria por razão idêntica: é config do
		// DATASET recusada na subida, não erro de uso da linha de comando — e
		// sem o errors.Is aqui ela cairia no default (ExitUso, 1) igual a
		// qualquer flag digitada errado, escondendo que o problema está no
		// YAML do dataset.
		//
		// schema.ErrInvalidIndexWindow (§4.1: `history.index` com janela
		// ambígua, bloco vazio ou escrito fora do `history:`) entra pela mesma
		// porta e pelo mesmo motivo.
		return ExitDataset
	default:
		return ExitUsageError
	}
}

// openDataset resolve a configuração, carrega o schema do dataset e abre o
// Store com os índices que o YAML declara.
//
// Os campos indexados saem do PRÓPRIO YAML, e são passados por NOME: os
// FieldIDs nascem do dicionário, que vive dentro do Store — exigir IDs aqui
// obrigaria a abrir sem índice, compilar, fechar e reabrir.
func openDataset(cfg *nishiconfig.Config, name string) (*storage.Store, *schema.Compiled, *schema.DatasetSpec, error) {
	spec, err := schema.Load(cfg.DatasetSpecPath(name), cfg.QueryDefaults())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dataset %q: %w", name, err)
	}
	if spec.Name != name {
		return nil, nil, nil, fmt.Errorf("dataset %q: o arquivo declara name: %q", name, spec.Name)
	}

	indexedFieldNames := schema.IndexedFieldNames(spec)

	// O schema compila DENTRO do Open, pelo callback dos contadores — o mesmo
	// caminho do servidor: um dataset com `counters:` aberto pelo CLI tem de
	// manter os contadores igual ao servidor, senão o `nishidb ingest` deixaria
	// o vetor parado enquanto o Put anda.
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             cfg.DatasetDir(name),
		SegmentSize:                     cfg.SegmentSize(),
		IndexedFieldsByName:             indexedFieldNames,
		DerivedColumnNames:              spec.DerivedColumnNames(),
		ActiveTTL:                       len(spec.Materialization.TTLRules) > 0,
		FallbackDuration:                spec.Materialization.FallbackDuration,
		WriteInstantFieldName:           schema.WriteInstantFieldName,
		TTLFieldName:                    schema.TTLFieldName,
		SnapshotMaxBytes:                cfg.SnapshotMaxBytes(),
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("abrindo o dataset %q: %w", name, err)
	}
	compiled := comp.Compiled()
	if compiled == nil {
		st.Close()
		return nil, nil, nil, fmt.Errorf("dataset %q: o Open não compilou o schema", name)
	}
	return st, compiled, spec, nil
}

// openHistory abre a tabela irmã do dataset, ou devolve (nil, nil) quando
// não há bloco `history`.
//
// A ordem é a mesma do servidor, e pela mesma razão: o Store da irmã recebe o
// dicionário do base, a spec derivada compila contra ele, e o Compiled do
// base passa a espelhar o dela. Sem esta função, `nishidb ingest` gravaria só
// o dataset base e a tabela declarada no YAML ficaria vazia — a configuração
// aceita e sem efeito que o débito 37 nomeia.
func openHistory(cfg *nishiconfig.Config, spec *schema.DatasetSpec, base *schema.Compiled, dict *fielddict.Dict) (*storage.Store, *schema.CompiledField, error) {
	h := spec.History
	if h == nil {
		return nil, nil, nil
	}
	derivedHistory := spec.DeriveHistory()
	segmentSize := cfg.SegmentSize()
	if h.SegmentSize > 0 {
		segmentSize = h.SegmentSize
	}
	if err := h.ValidateAgainstSegment(segmentSize); err != nil {
		return nil, nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                   cfg.DatasetDir(derivedHistory.Name),
		Dataset:               derivedHistory.Name,
		SegmentSize:           segmentSize,
		Mode:                  storage.HistoryMode,
		SharedDictionary:      dict,
		IndexedFieldsByName:   schema.IndexedFieldNames(derivedHistory),
		DerivedColumnNames:    derivedHistory.DerivedColumnNames(),
		WriteInstantFieldName: schema.WriteInstantFieldName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("tabela histórica de %q: %w", spec.Name, err)
	}
	compiled, err := derivedHistory.Compile(dict)
	if err != nil {
		return nil, nil, closingStorage(st, fmt.Errorf("tabela histórica de %q: %w", spec.Name, err))
	}
	base.AddMirror(compiled)

	var raw *schema.CompiledField
	if h.RawPayload {
		f, ok := compiled.FieldByName([]byte(schema.RawFieldName))
		if !ok {
			return nil, nil, closingStorage(st,
				fmt.Errorf("tabela histórica de %q: a coluna %q não compilou", spec.Name, schema.RawFieldName))
		}
		raw = f
	}
	return st, raw, nil
}

// closingStorage devolve o erro original, fechando o Store antes. O erro do
// Close é agregado e nunca substitui a causa: quem lê precisa do motivo de a
// abertura ter falhado, não do que aconteceu na limpeza.
//
// É o gêmeo do helper de mesmo nome do servidor. Ele mora nos dois lugares
// porque os dois abrem dataset por caminhos próprios, e extrair um terceiro
// pacote para seis linhas custaria mais do que a duplicação.
func closingStorage(st *storage.Store, cause error) error {
	if err := st.Close(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// openForQuery resolve o nome pedido contra os YAMLs de
// `datasets.directory` e devolve o Store da tabela — que pode ser a IRMÃ de um
// dataset, e não um dataset declarado.
//
// A tabela irmã não tem YAML próprio: ela nasce do bloco `history` do base. Sem
// esta resolução, `--dataset events_history` responderia "arquivo não existe"
// sobre uma tabela que demonstravelmente está lá — o comando mentiria sobre o
// que o dataset tem, que é o defeito que este projeto trata como tal.
//
// O fechamento devolve os DOIS Stores, na ordem certa: a irmã usa o dicionário
// do base, então ela fecha primeiro.
// A spec devolvida é a do dataset DECLARADO, e é nil quando o que se abriu é a
// tabela IRMÃ: ela não tem YAML próprio nem fonte própria, e é isso que faz o
// `_meta.kafka` não ser registrado sobre ela — a ingestão dela é a do base.
func openForQuery(cfg *nishiconfig.Config, name string) (*storage.Store, *schema.Compiled, *schema.DatasetSpec, func(), error) {
	if fileExists(cfg.DatasetSpecPath(name)) {
		st, compiled, spec, err := openDataset(cfg, name)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		return st, compiled, spec, func() { _ = st.Close() }, nil
	}
	base, err := datasetDerivedFromSibling(cfg, name)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	stBase, basedCompiled, spec, err := openDataset(cfg, base)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	stIrma, _, err := openHistory(cfg, spec, basedCompiled, stBase.FieldDict())
	if err != nil {
		return nil, nil, nil, nil, closingStorage(stBase, err)
	}
	derivedCompiled, err := spec.DeriveHistory().Compile(stBase.FieldDict())
	if err != nil {
		return nil, nil, nil, nil, errors.Join(closingStorage(stIrma, err), stBase.Close())
	}
	return stIrma, derivedCompiled, nil, func() {
		_ = stIrma.Close()
		_ = stBase.Close()
	}, nil
}

// datasetDerivedFromSibling acha o dataset cujo bloco `history` produz este nome.
//
// A varredura é sobre o diretório inteiro porque o sufixo é declarável: não dá
// para inverter o nome sem saber qual sufixo cada dataset escolheu. São alguns
// arquivos YAML lidos uma vez, no caminho de um comando interativo.
func datasetDerivedFromSibling(cfg *nishiconfig.Config, name string) (string, error) {
	names, err := cfg.ListDatasets()
	if err != nil {
		return "", err
	}
	for _, nameCandidate := range names {
		spec, err := schema.Load(cfg.DatasetSpecPath(nameCandidate), cfg.QueryDefaults())
		if err != nil {
			// Um YAML quebrado no diretório não pode impedir a consulta a
			// OUTRO dataset: ele já será recusado quando alguém o pedir.
			continue
		}
		if spec.History != nil && spec.History.TableName(nameCandidate) == name {
			return nameCandidate, nil
		}
	}
	return "", fmt.Errorf("dataset %q: não há %s nem nenhum dataset cujo bloco `history` derive esse nome",
		name, cfg.DatasetSpecPath(name))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
