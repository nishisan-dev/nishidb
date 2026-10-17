package cli

import (
	"flag"
	"io"
	"net/http"
	"net/url"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/metareplication"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// `nishidb replication` é a superfície operacional da replicação do master.
// Hoje tem um subcomando, `resync`: o snapshot sob demanda de UM destino
// Kafka, pela rota ADMIN POST /v1/replication/resync.
//
// Só existe o caminho remoto, e não o `--config`: o pedido é atendido pelo
// publicador, que só roda dentro do servidor, e quem lê o `<destino>.resync`
// é ele. Um pedido gravado com o servidor parado seria atendido na subida
// seguinte do mesmo jeito, mas exigiria derrubar o servidor para fazê-lo, que
// é o oposto do que o operador quer quando pede um snapshot.
//
// A saída é uma linha de confirmação, com o estado do publicador. O 202 diz
// que o pedido está gravado de forma durável, não que o snapshot terminou: o
// andamento se lê em `_meta.replication` (lastResetReason, lastSnapshotAt) ou
// no /v1/stats. Com o publicador parado (blocked, failed, stopped) o pedido só
// é atendido na próxima subida, e o estado impresso é o que avisa isso.

const replicationUsage = "replication: use nishidb replication resync --server <host:porta> --dataset <nome> --destination <destino>\n"

func runReplication(args []string, amb Context) int {
	if len(args) < 1 || args[0] != "resync" {
		notify(amb, "%s", replicationUsage)
		return ExitUsageError
	}
	return runReplicationResync(args[1:], amb)
}

func runReplicationResync(args []string, amb Context) int {
	const cmd = "replication resync"
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		server      = fs.String("server", "", "nishidb server address (host:port)")
		dataset     = fs.String("dataset", "", "master dataset name")
		destination = fs.String("destination", "", "Kafka destination name in replication.targets")
	)
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	if fs.NArg() != 0 {
		notify(amb, "%s: unexpected positional argument %q\n", cmd, fs.Arg(0))
		return ExitUsageError
	}
	if code := requiresServer(cmd, *server, amb); code != ExitOK {
		return code
	}
	if *dataset == "" || *destination == "" {
		notify(amb, "%s: --dataset and --destination are required\n", cmd)
		return ExitUsageError
	}
	cred, err := credFlags.resolve()
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return ExitUsageError
	}
	query := url.Values{"dataset": {*dataset}, "destination": {*destination}}
	var resp protocol.ReplicationResyncResponse
	if code := postJSONExpecting(cmd, *server, "/v1/replication/resync?"+query.Encode(), nil, http.StatusAccepted, cred, &resp, amb); code != ExitOK {
		return code
	}
	return writeOutputWithContext(cmd, amb, func(w io.Writer) {
		line(w, "DATASET", "DESTINATION", "REQUESTED", "PUBLISHER STATE")
		requested := "no"
		if resp.Requested {
			requested = "yes"
		}
		line(w, resp.Dataset, resp.Destination, requested, resp.State)
	})
}

// offlineReplicationSource é a `_meta.replication` do `nishidb query
// --config`: o que existe no disco com o servidor parado. O feed sai do
// `state` (só com a marca limpa, replication.ReadClosedStatus) e cada destino
// de `replication.targets` sai do seu .ckpt, com o tópico da configuração.
// Sem publicador, Destinations fica nil, e as colunas que só um processo vivo
// mede são NULL.
//
// Só o master com replicação ligada tem feed e publicadores: os outros
// datasets respondem a tabela vazia, como no servidor. O `state` ilegível vira
// ausência de feed (NULL), como o checkpoint ilegível da `_meta.kafka`; o
// .ckpt ilegível vira o lastError da linha do destino.
func offlineReplicationSource(cfg *nishiconfig.Config, spec *schema.DatasetSpec) metareplication.Source {
	if !spec.Replication.Enabled || spec.Replication.Role != "master" {
		return metareplication.Source{}
	}
	root := cfg.DatasetRootDir(spec.Name)
	feedDir := replication.FeedDir(root)
	checkpointDir := replicationkafka.CheckpointDir(root)
	topics := make(map[string]string, len(cfg.KafkaTargets))
	for _, k := range cfg.KafkaTargets {
		topics[k.Name] = k.Topic
	}
	targets := append([]string(nil), spec.Replication.Targets...)
	return metareplication.Source{
		Feed: func() (replication.Status, bool) {
			st, ok, err := replication.ReadClosedStatus(feedDir)
			if err != nil {
				return replication.Status{}, false
			}
			return st, ok
		},
		Checkpoints: func() []metareplication.CheckpointRow {
			out := make([]metareplication.CheckpointRow, 0, len(targets))
			for _, name := range targets {
				c, found, err := replicationkafka.LoadCheckpointRow(checkpointDir, name)
				out = append(out, metareplication.CheckpointRow{Destination: name, Topic: topics[name], Found: found, Checkpoint: c, Err: err})
			}
			return out
		},
	}
}
