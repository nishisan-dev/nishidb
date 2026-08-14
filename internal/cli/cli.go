package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Exit codes — CONTRATO com quem chama o binário de um script. Cada um
// corresponde a uma decisão diferente do operador: reprocessar, corrigir a
// query, investigar o dataset, reabrir, ou paginar.
const (
	ExitOK        = 0
	ExitUso       = 1 // erro de uso ou de configuração
	ExitQuery     = 2 // sintaxe ou bind da query
	ExitDataset   = 3 // dataset corrompido, travado ou inacessível
	ExitWriter    = 4 // writer em modo falho: reprocessar é inútil até reabrir
	ExitTruncado  = 5 // resultado truncado pelo teto de maxRows
	ExitInterromp = 6 // interrompido por sinal
	// ExitPerdaDeDados: a ingestão terminou e o dataset está íntegro, mas
	// eventos NÃO entraram por falha que não é do evento (§101, balde 2).
	// Distinto do 4: aqui não há modo falho, reprocessar depois de investigar
	// é útil. Distinto do 0: houve perda, e um pipeline que testa $? precisa
	// saber disso.
	ExitPerdaDeDados = 7
	// ExitParadoPorPolitica: a ingestão PAROU porque a política do dataset
	// mandou (`errors.invalidEvent: stop`, §101).
	//
	// É código próprio, e não uma reciclagem do 7, porque a causa é outra e a
	// ação também: no 7 os eventos não entraram por falha NOSSA e a resposta é
	// investigar o motor; aqui a parada foi PEDIDA pela configuração diante de
	// um evento ruim, e a resposta é olhar o evento e decidir — possivelmente
	// pulando-o com `nishidb offsets`. Reusar o 7 mentiria sobre a causa.
	ExitParadoPorPolitica = 8
)

// Ambiente são as dependências de processo, injetadas para o teste rodar o
// CLI como função em vez de precisar de um binário compilado.
type Ambiente struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Ctx é cancelado no sinal de interrupção (o main liga no signal.Notify).
	Ctx context.Context
}

const uso = `nishidb — engine de materialização e consulta

USO
  nishidb ingest  --config <nishidb.yaml> --dataset <nome> --input <arquivo|-> [--checkpoint-every N]
  nishidb query   --config <nishidb.yaml> --dataset <nome> [--limit N] '<SQL>'
  nishidb server  --config <nishidb.yaml> [--log-format json|texto] [--log-level N] [--pprof <addr>]
  nishidb offsets --config <nishidb.yaml> --dataset <nome> (--show | --reset earliest|latest | --set p:o,...) [--yes]

INGEST
  Lê NDJSON (um evento JSON por linha) e materializa no dataset.
  --checkpoint-every N   checkpoint a cada N eventos (0 desliga, com aviso).
                         O padrão é ligado: sem checkpoint o consumo de memória
                         acompanha o tamanho do dataset.

QUERY
  Executa o SQL e escreve NDJSON no stdout, uma linha por row, em streaming.
  --limit N              sobrepõe o LIMIT da query. O maxRows do dataset
                         continua valendo como teto.

SERVER
  Processo de longa duração: consultas e ingestão Kafka no MESMO processo.
  Consome os datasets com source.kafka.enabled: true; os demais ficam só
  consultáveis. Expõe GET /metrics e GET /v1/health.
  --pprof <addr>         endpoints de debug num listener próprio. Modo
                         administrativo: não exponha esta porta.

OFFSETS
  Lê e reposiciona a posição no tópico. Exige o servidor PARADO (toma o
  flock do dataset). --reset e --set pedem --yes.

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
func Run(args []string, amb Ambiente) int {
	if len(args) < 1 {
		fmt.Fprint(amb.Stderr, uso)
		return ExitUso
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
	case "-h", "--help", "help":
		fmt.Fprint(amb.Stdout, uso)
		return ExitOK
	default:
		fmt.Fprintf(amb.Stderr, "comando desconhecido: %q\n\n%s", args[0], uso)
		return ExitUso
	}
}

// codigoDoErro traduz o erro na decisão que o operador precisa tomar. A
// ordem importa: um erro pode casar mais de um sentinela, e o mais específico
// tem de vencer.
func codigoDoErro(err error) int {
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ExitInterromp
	case errors.Is(err, parser.ErrSyntax), errors.Is(err, binder.ErrBind),
		errors.Is(err, executor.ErrInvalidPlan):
		return ExitQuery
	case errors.Is(err, executor.ErrMaxRowsExceeded):
		return ExitTruncado
	case errors.Is(err, types.ErrWriterFailed):
		return ExitWriter
	case errors.Is(err, storage.ErrDatasetLocked), errors.Is(err, storage.ErrDatasetCorrupt),
		errors.Is(err, storage.ErrDatasetClosed), errors.Is(err, encoding.ErrCorrupt):
		// encoding.ErrCorrupt é o sentinela que chega de VERDADE quando um byte
		// da região durável está adulterado: a detecção é lazy, no read, e o
		// erro sobe pelo executor. Sem ele aqui, corrupção de dataset caía no
		// default e saía como "erro de uso" — o único código que manda o
		// operador olhar a linha de comando em vez do dataset.
		return ExitDataset
	default:
		return ExitUso
	}
}

// abreDataset resolve a configuração, carrega o schema do dataset e abre o
// Store com os índices que o YAML declara.
//
// Os bitmaps saem do PRÓPRIO YAML, e são passados por NOME: os FieldIDs
// nascem do dicionário, que vive dentro do Store — exigir IDs aqui obrigaria
// a abrir sem índice, compilar, fechar e reabrir.
func abreDataset(cfg *nishiconfig.Config, nome string) (*storage.Store, *schema.Compiled, error) {
	spec, err := schema.Load(cfg.SpecDoDataset(nome), cfg.DefaultsDeQuery())
	if err != nil {
		return nil, nil, fmt.Errorf("dataset %q: %w", nome, err)
	}
	if spec.Name != nome {
		return nil, nil, fmt.Errorf("dataset %q: o arquivo declara name: %q", nome, spec.Name)
	}

	var bitmaps []string
	for campo, f := range spec.Schema.Fields {
		if f.Index == "bitmap" {
			bitmaps = append(bitmaps, campo)
		}
	}

	st, err := storage.Open(storage.Options{
		Dir:              cfg.DirDoDataset(nome),
		SegmentSize:      cfg.SegmentSize(),
		BitmapFieldNames: bitmaps,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("abrindo o dataset %q: %w", nome, err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("dataset %q: %w", nome, err)
	}
	return st, compilado, nil
}
