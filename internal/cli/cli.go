package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
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
  nishidb query   --server <host:porta>    --dataset <nome> [--limit N] '<SQL>'
  nishidb server  --config <nishidb.yaml> [--log-format json|texto] [--log-level N] [--pprof <addr>]
  nishidb offsets --config <nishidb.yaml> --dataset <nome> (--show | --reset earliest|latest | --set p:o,...) [--yes]
  nishidb status   --server <host:porta>
  nishidb datasets --server <host:porta>
  nishidb inspect  --server <host:porta> <dataset>
  nishidb compact    --server <host:porta> [dataset]
  nishidb checkpoint --server <host:porta> [dataset]
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

CREDENCIAL E TLS (query --server, status, datasets, inspect, compact, checkpoint)
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
	case "--version", "version":
		_, _ = fmt.Fprintln(amb.Stdout, buildinfo.String())
		return ExitOK
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
		errors.Is(err, storage.ErrDatasetClosed), errors.Is(err, encoding.ErrCorrupt),
		errors.Is(err, schema.ErrGrafiaDeIndiceObsoleta):
		// encoding.ErrCorrupt é o sentinela que chega de VERDADE quando um byte
		// da região durável está adulterado: a detecção é lazy, no read, e o
		// erro sobe pelo executor. Sem ele aqui, corrupção de dataset caía no
		// default e saía como "erro de uso" — o único código que manda o
		// operador olhar a linha de comando em vez do dataset.
		//
		// schema.ErrGrafiaDeIndiceObsoleta (D5: `index: hash`/`index: bitmap`
		// recusados) entra na MESMA categoria por razão idêntica: é config do
		// DATASET recusada na subida, não erro de uso da linha de comando — e
		// sem o errors.Is aqui ela cairia no default (ExitUso, 1) igual a
		// qualquer flag digitada errado, escondendo que o problema está no
		// YAML do dataset.
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
func abreDataset(cfg *nishiconfig.Config, nome string) (*storage.Store, *schema.Compiled, *schema.DatasetSpec, error) {
	spec, err := schema.Load(cfg.SpecDoDataset(nome), cfg.DefaultsDeQuery())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dataset %q: %w", nome, err)
	}
	if spec.Name != nome {
		return nil, nil, nil, fmt.Errorf("dataset %q: o arquivo declara name: %q", nome, spec.Name)
	}

	indexados := schema.CamposComIndice(spec)

	st, err := storage.Open(storage.Options{
		Dir:                    cfg.DirDoDataset(nome),
		SegmentSize:            cfg.SegmentSize(),
		CamposIndexadosPorNome: indexados,
		ColunasDerivadas:       spec.NomesDasExtracoes(),
		TTLAtivo:               len(spec.Materialization.RegrasTTL) > 0,
		DuracaoDeFundo:         spec.Materialization.DuracaoDeFundo,
		CampoDeEscrita:         schema.CampoInstanteDeEscrita,
		CampoDePrazo:           schema.CampoDePrazo,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("abrindo o dataset %q: %w", nome, err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		st.Close()
		return nil, nil, nil, fmt.Errorf("dataset %q: %w", nome, err)
	}
	return st, compilado, spec, nil
}

// abreHistorico abre a tabela irmã do dataset, ou devolve (nil, nil) quando
// não há bloco `history`.
//
// A ordem é a mesma do servidor, e pela mesma razão: o Store da irmã recebe o
// dicionário do base, a spec derivada compila contra ele, e o Compiled do
// base passa a espelhar o dela. Sem esta função, `nishidb ingest` gravaria só
// o dataset base e a tabela declarada no YAML ficaria vazia — a configuração
// aceita e sem efeito que o débito 37 nomeia.
func abreHistorico(cfg *nishiconfig.Config, spec *schema.DatasetSpec, base *schema.Compiled, dict *fielddict.Dict) (*storage.Store, *schema.CompiledField, error) {
	h := spec.Historico
	if h == nil {
		return nil, nil, nil
	}
	irma := spec.DerivaHistorico()
	segmento := cfg.SegmentSize()
	if h.SegmentSize > 0 {
		segmento = h.SegmentSize
	}
	if err := h.ValidaContraSegmento(segmento); err != nil {
		return nil, nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                    cfg.DirDoDataset(irma.Name),
		Dataset:                irma.Name,
		SegmentSize:            segmento,
		Modo:                   storage.ModoHistorico,
		DictCompartilhado:      dict,
		CamposIndexadosPorNome: schema.CamposComIndice(irma),
		ColunasDerivadas:       irma.NomesDasExtracoes(),
		CampoDeEscrita:         schema.CampoInstanteDeEscrita,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("tabela histórica de %q: %w", spec.Name, err)
	}
	compilado, err := irma.Compile(dict)
	if err != nil {
		return nil, nil, fechandoStore(st, fmt.Errorf("tabela histórica de %q: %w", spec.Name, err))
	}
	base.Espelha(compilado)

	var raw *schema.CompiledField
	if h.RawPayload {
		f, ok := compilado.FieldByName([]byte(schema.CampoRaw))
		if !ok {
			return nil, nil, fechandoStore(st,
				fmt.Errorf("tabela histórica de %q: a coluna %q não compilou", spec.Name, schema.CampoRaw))
		}
		raw = f
	}
	return st, raw, nil
}

// fechandoStore devolve o erro original, fechando o Store antes. O erro do
// Close é agregado e nunca substitui a causa: quem lê precisa do motivo de a
// abertura ter falhado, não do que aconteceu na limpeza.
//
// É o gêmeo do helper de mesmo nome do servidor. Ele mora nos dois lugares
// porque os dois abrem dataset por caminhos próprios, e extrair um terceiro
// pacote para seis linhas custaria mais do que a duplicação.
func fechandoStore(st *storage.Store, causa error) error {
	if err := st.Close(); err != nil {
		return errors.Join(causa, err)
	}
	return causa
}

// abreParaConsulta resolve o nome pedido contra os YAMLs de
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
func abreParaConsulta(cfg *nishiconfig.Config, nome string) (*storage.Store, *schema.Compiled, func(), error) {
	if arquivoExiste(cfg.SpecDoDataset(nome)) {
		st, compilado, _, err := abreDataset(cfg, nome)
		if err != nil {
			return nil, nil, nil, err
		}
		return st, compilado, func() { _ = st.Close() }, nil
	}
	base, err := datasetQueDerivaAIrma(cfg, nome)
	if err != nil {
		return nil, nil, nil, err
	}
	stBase, compiladoBase, spec, err := abreDataset(cfg, base)
	if err != nil {
		return nil, nil, nil, err
	}
	stIrma, _, err := abreHistorico(cfg, spec, compiladoBase, stBase.FieldDict())
	if err != nil {
		return nil, nil, nil, fechandoStore(stBase, err)
	}
	compiladoIrma, err := spec.DerivaHistorico().Compile(stBase.FieldDict())
	if err != nil {
		return nil, nil, nil, errors.Join(fechandoStore(stIrma, err), stBase.Close())
	}
	return stIrma, compiladoIrma, func() {
		_ = stIrma.Close()
		_ = stBase.Close()
	}, nil
}

// datasetQueDerivaAIrma acha o dataset cujo bloco `history` produz este nome.
//
// A varredura é sobre o diretório inteiro porque o sufixo é declarável: não dá
// para inverter o nome sem saber qual sufixo cada dataset escolheu. São alguns
// arquivos YAML lidos uma vez, no caminho de um comando interativo.
func datasetQueDerivaAIrma(cfg *nishiconfig.Config, nome string) (string, error) {
	nomes, err := cfg.ListaDatasets()
	if err != nil {
		return "", err
	}
	for _, candidato := range nomes {
		spec, err := schema.Load(cfg.SpecDoDataset(candidato), cfg.DefaultsDeQuery())
		if err != nil {
			// Um YAML quebrado no diretório não pode impedir a consulta a
			// OUTRO dataset: ele já será recusado quando alguém o pedir.
			continue
		}
		if spec.Historico != nil && spec.Historico.NomeDaTabela(candidato) == nome {
			return candidato, nil
		}
	}
	return "", fmt.Errorf("dataset %q: não há %s nem nenhum dataset cujo bloco `history` derive esse nome",
		nome, cfg.SpecDoDataset(nome))
}

func arquivoExiste(caminho string) bool {
	_, err := os.Stat(caminho)
	return err == nil
}
