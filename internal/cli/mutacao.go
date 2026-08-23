package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
)

// Os dois comandos MUTANTES do §96 — `compact` e `checkpoint` — que a Task B5
// (Fase 11, decisão D3 do roadmap) liga por trás do papel ADMIN. Nascem
// remotos como os três comandos de leitura de operacao.go, pela MESMA razão
// (flock exclusivo por processo), e falam POST em vez de GET: o corpo é
// {"dataset":"..."} opcional (ausente = todos os datasets do servidor, na
// ordem determinística da subida) e o padrão de erro é o do query_remota —
// CorpoErro decodificado, mensagem no stderr, código traduzido pela MESMA
// tabela de exit codes do resto do binário.
//
// FORA DE ESCOPO desta task (B6): nenhuma credencial sai daqui. Os dois
// comandos falam HTTP puro, sem Authorization — funcionam hoje só contra um
// servidor com server.authentication DESLIGADA, ou por trás de algo que
// injete a credencial. O ponto de costura para a B6 é postaMutacao, abaixo:
// é aqui que um `req.SetBasicAuth(...)` (ou um transporte com TLS/CA
// custom) entraria, no MESMO lugar em que a B6 já vai mexer em
// consultaRemota e buscaDocumento — uma função a mais na mesma lista.

func runCompact(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	servidor := fs.String("server", "", "endereço do nishidb server (host:porta)")
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	if fs.NArg() > 1 {
		avisa(amb, "compact: no máximo um dataset posicional (ausente = todos)\n")
		avisa(amb, "  as flags vêm antes: nishidb compact --server <host:porta> [dataset]\n")
		return ExitUso
	}
	if code := exigeServidor("compact", *servidor, amb); code != ExitOK {
		return code
	}
	dataset := ""
	if fs.NArg() == 1 {
		dataset = fs.Arg(0)
	}

	var resp protocolo.RespostaCompact
	if code := postaMutacao("compact", *servidor, "/v1/compact", dataset, &resp, amb); code != ExitOK {
		return code
	}
	return escreveSaida("compact", amb, func(w io.Writer) { escreveCompact(w, *servidor, resp) })
}

func runCheckpoint(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	servidor := fs.String("server", "", "endereço do nishidb server (host:porta)")
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	if fs.NArg() > 1 {
		avisa(amb, "checkpoint: no máximo um dataset posicional (ausente = todos)\n")
		avisa(amb, "  as flags vêm antes: nishidb checkpoint --server <host:porta> [dataset]\n")
		return ExitUso
	}
	if code := exigeServidor("checkpoint", *servidor, amb); code != ExitOK {
		return code
	}
	dataset := ""
	if fs.NArg() == 1 {
		dataset = fs.Arg(0)
	}

	var resp protocolo.RespostaCheckpoint
	if code := postaMutacao("checkpoint", *servidor, "/v1/checkpoint", dataset, &resp, amb); code != ExitOK {
		return code
	}
	return escreveSaida("checkpoint", amb, func(w io.Writer) { escreveCheckpoint(w, *servidor, resp) })
}

// postaMutacao faz o POST de uma rota mutante (compact/checkpoint) e
// decodifica o corpo de SUCESSO no destino. dataset == "" omite o corpo por
// completo — não manda "{}" — para exercitar o mesmo caminho "todos os
// datasets" que um corpo ausente já cobre no servidor (mutacoes.go,
// leRequisicaoDataset).
//
// O tratamento de erro é o do query_remota (recusaRemota, que operacao.go
// já generaliza com um parâmetro `cmd`): CorpoErro decodificado,
// mensagem no stderr, código traduzido por codigoDoProtocolo — a MESMA
// tabela que WRITER_FAILED (→ ExitWriter) e COMPACTION_BUSY (→ ExitUso, com
// justificativa no comentário de codigoDoProtocolo) alimentam.
func postaMutacao(cmd, endereco, caminho, dataset string, destino any, amb Ambiente) int {
	var corpo io.Reader
	if dataset != "" {
		b, err := json.Marshal(protocolo.RequisicaoDataset{Dataset: dataset})
		if err != nil {
			avisa(amb, "%s: %v\n", cmd, err)
			return ExitUso
		}
		corpo = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(amb.Ctx, http.MethodPost, normalizaEndereco(endereco)+caminho, corpo)
	if err != nil {
		avisa(amb, "%s: %v\n", cmd, err)
		return ExitUso
	}
	if corpo != nil {
		req.Header.Set("Content-Type", protocolo.ContentTypeJSON)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		avisa(amb, "%s: falando com %s: %v\n", cmd, endereco, err)
		return ExitUso
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			avisa(amb, "%s: fechando a resposta: %v\n", cmd, err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return recusaRemota(cmd, resp, amb)
	}
	if err := json.NewDecoder(resp.Body).Decode(destino); err != nil {
		avisa(amb, "%s: resposta ilegível de %s: %v\n", cmd, endereco, err)
		return ExitDataset
	}
	return ExitOK
}

func escreveCompact(w io.Writer, endereco string, resp protocolo.RespostaCompact) {
	titulo(w, "servidor %s", endereco)
	titulo(w, "")
	linha(w, "DATASET", "SEGMENTOS", "BYTES LIDOS", "BYTES ESCRITOS", "DURAÇÃO")
	for _, d := range resp.Datasets {
		linha(w, d.Name, strconv.Itoa(d.SegmentsRetired),
			bytesLegiveis(d.BytesRead), bytesLegiveis(d.BytesWritten), duracao(d.DurationMs))
	}
}

func escreveCheckpoint(w io.Writer, endereco string, resp protocolo.RespostaCheckpoint) {
	titulo(w, "servidor %s", endereco)
	titulo(w, "")
	linha(w, "DATASET", "DURAÇÃO")
	for _, d := range resp.Datasets {
		linha(w, d.Name, duracao(d.DurationMs))
	}
}
