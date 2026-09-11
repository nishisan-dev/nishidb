package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// Os dois comandos MUTANTES do §96 — `compact` e `checkpoint` — que a Task B5
// (Fase 11, decisão D3 do roadmap) liga por trás do papel ADMIN. Nascem
// remotos como os três comandos de leitura de operation.go, pela MESMA razão
// (flock exclusivo por processo), e falam POST em vez de GET: o corpo é
// {"dataset":"..."} opcional (ausente = todos os datasets do servidor, na
// ordem determinística da subida) e o padrão de erro é o do query_remota —
// ErrorBody decodificado, mensagem no stderr, código traduzido pela MESMA
// tabela de exit codes do resto do binário.
//
// Credencial e TLS (Task B6): os dois comandos aceitam --user, --tls-ca e
// senha por NISHIDB_PASSWORD/--password-file, no MESMO ponto de costura que
// remoteQuery (query_remote.go) e searchDocument (operation.go) usam —
// postMutation, abaixo, chama remoteClient/aplicaCredencial, nunca monta
// Authorization ou Transport por conta própria. Contra um servidor com
// server.authentication DESLIGADA, rodar sem --user continua byte a byte
// igual a antes desta task.

func runCompact(args []string, amb Context) int {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	server := fs.String("server", "", "endereço do nishidb server (host:porta)")
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	cred, err := credFlags.resolve()
	if err != nil {
		notify(amb, "compact: %v\n", err)
		return ExitUsageError
	}
	if fs.NArg() > 1 {
		notify(amb, "compact: no máximo um dataset posicional (ausente = todos)\n")
		notify(amb, "  as flags vêm antes: nishidb compact --server <host:porta> [dataset]\n")
		return ExitUsageError
	}
	if code := requiresServer("compact", *server, amb); code != ExitOK {
		return code
	}
	dataset := ""
	if fs.NArg() == 1 {
		dataset = fs.Arg(0)
	}

	var resp protocol.CompactResponse
	if code := postMutation("compact", *server, "/v1/compact", dataset, cred, &resp, amb); code != ExitOK {
		return code
	}
	return writeOutputWithContext("compact", amb, func(w io.Writer) { writeCompact(w, *server, resp) })
}

func runCheckpoint(args []string, amb Context) int {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	server := fs.String("server", "", "endereço do nishidb server (host:porta)")
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	cred, err := credFlags.resolve()
	if err != nil {
		notify(amb, "checkpoint: %v\n", err)
		return ExitUsageError
	}
	if fs.NArg() > 1 {
		notify(amb, "checkpoint: no máximo um dataset posicional (ausente = todos)\n")
		notify(amb, "  as flags vêm antes: nishidb checkpoint --server <host:porta> [dataset]\n")
		return ExitUsageError
	}
	if code := requiresServer("checkpoint", *server, amb); code != ExitOK {
		return code
	}
	dataset := ""
	if fs.NArg() == 1 {
		dataset = fs.Arg(0)
	}

	var resp protocol.CheckpointResponse
	if code := postMutation("checkpoint", *server, "/v1/checkpoint", dataset, cred, &resp, amb); code != ExitOK {
		return code
	}
	return writeOutputWithContext("checkpoint", amb, func(w io.Writer) { writeCheckpoint(w, *server, resp) })
}

// postMutation faz o POST de uma rota mutante (compact/checkpoint) e
// decodifica o corpo de SUCESSO no destino. dataset == "" omite o corpo por
// completo — não manda "{}" — para exercitar o mesmo caminho "todos os
// datasets" que um corpo ausente já cobre no servidor (mutations.go,
// readRequisicaoDataset).
//
// O tratamento de erro é o do query_remota (remoteRefusal, que operation.go
// já generaliza com um parâmetro `cmd`): ErrorBody decodificado,
// mensagem no stderr, código traduzido por codigoOfProtocolo — a MESMA
// tabela que WRITER_FAILED (→ ExitWriter) e COMPACTION_BUSY (→ ExitUsageError, com
// justificativa no comentário de codigoOfProtocolo) alimentam.
//
// cred entra pelo MESMO remoteClient/aplicaCredencial que remoteQuery e
// searchDocument usam (Task B6) — este É o ponto de costura único que a B5
// já apontava para a credencial entrar.
func postMutation(cmd, endereco, path, dataset string, cred remoteCredential, destination any, amb Context) int {
	var corpo io.Reader
	if dataset != "" {
		b, err := json.Marshal(protocol.DatasetRequest{Dataset: dataset})
		if err != nil {
			notify(amb, "%s: %v\n", cmd, err)
			return ExitUsageError
		}
		corpo = bytes.NewReader(b)
	}

	client, err := remoteClient(cred)
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return ExitUsageError
	}
	req, err := http.NewRequestWithContext(amb.Ctx, http.MethodPost, normalizeAddress(endereco)+path, corpo)
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return ExitUsageError
	}
	if corpo != nil {
		req.Header.Set("Content-Type", protocol.ContentTypeJSON)
	}
	applyCredential(req, cred)

	resp, err := client.Do(req)
	if err != nil {
		notify(amb, "%s: falando com %s: %v\n", cmd, endereco, err)
		return ExitUsageError
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			notify(amb, "%s: fechando a resposta: %v\n", cmd, err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return remoteRefusal(cmd, resp, amb)
	}
	if err := json.NewDecoder(resp.Body).Decode(destination); err != nil {
		notify(amb, "%s: resposta ilegível de %s: %v\n", cmd, endereco, err)
		return ExitDataset
	}
	return ExitOK
}

func writeCompact(w io.Writer, endereco string, resp protocol.CompactResponse) {
	title(w, "servidor %s", endereco)
	title(w, "")
	line(w, "DATASET", "SEGMENTOS", "BYTES LIDOS", "BYTES ESCRITOS", "DURAÇÃO")
	for _, d := range resp.Datasets {
		line(w, d.Name, strconv.Itoa(d.SegmentsRetired),
			bytesToReadableString(d.BytesRead), bytesToReadableString(d.BytesWritten), duration(d.DurationMs))
	}
}

func writeCheckpoint(w io.Writer, endereco string, resp protocol.CheckpointResponse) {
	title(w, "servidor %s", endereco)
	title(w, "")
	line(w, "DATASET", "DURAÇÃO")
	for _, d := range resp.Datasets {
		line(w, d.Name, duration(d.DurationMs))
	}
}
