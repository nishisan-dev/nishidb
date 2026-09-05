package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// consultaRemota executa a query pelo protocolo do §67 em vez de abrir o
// dataset local.
//
// Existe porque o flock do dataset é exclusivo por processo: com o `nishidb
// server` no ar, toda consulta local sai com exit 3. Até esta fase o operador
// ALTERNAVA — parava o servidor para consultar —, e é isso que o débito 54
// descreve. O driver JDBC da Fase 7 vai falar este mesmo protocolo; o CLI só
// chegou primeiro.
//
// O contrato de saída é o MESMO do caminho local: NDJSON cru no stdout, uma
// linha por row, e o exit code dizendo o que aconteceu. Um script que troca
// `--config` por `--server` não muda mais nada.
func consultaRemota(endereco, dataset, sql string, params []protocolo.Parametro, cred credencialRemota, amb Ambiente) int {
	corpo, err := json.Marshal(protocolo.Requisicao{Dataset: dataset, SQL: sql, Parameters: params})
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}

	client, err := clienteRemoto(cred)
	if err != nil {
		// `_, _ =`: código NOVO desta task segue a disciplina de avisa
		// (operacao.go) — 0 apontamentos de lint novos; as chamadas bare a
		// fmt.Fprintln já existentes neste arquivo são débito anterior.
		_, _ = fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}

	url := normalizaEndereco(endereco) + "/v1/query"
	req, err := http.NewRequestWithContext(amb.Ctx, http.MethodPost, url, strings.NewReader(string(corpo)))
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}
	req.Header.Set("Content-Type", protocolo.ContentTypeJSON)
	aplicaCredencial(req, cred)

	// Sem timeout no cliente: o teto de uma consulta é o query.timeout do
	// dataset, que o servidor aplica. Um prazo local mataria um stream longo
	// legítimo e faria o operador culpar o servidor.
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(amb.Stderr, "query: falando com %s: %v\n", endereco, err)
		return ExitUso
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Fprintln(amb.Stderr, "query: fechando a resposta:", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return recusaDoServidor(resp, amb)
	}
	return leStream(resp.Body, amb)
}

// normalizaEndereco aceita host:porta e URL completa. O operador digita
// `--server db01:9142`, que não é URL; exigir o esquema seria cerimônia.
func normalizaEndereco(e string) string {
	if strings.HasPrefix(e, "http://") || strings.HasPrefix(e, "https://") {
		return strings.TrimRight(e, "/")
	}
	return "http://" + strings.TrimRight(e, "/")
}

// corpoHTTPQuandoEsperavaHTTPS é o texto EXATO que o net/http do Go escreve
// (server.go, tratamento de tls.RecordHeaderError) quando um cliente fala
// texto puro com uma porta que só aceita TLS: o handshake nunca chega a
// acontecer, e o servidor devolve isto cru — HTTP/1.0, sem JSON — na única
// oportunidade que tem antes de fechar a conexão.
const corpoHTTPQuandoEsperavaHTTPS = "Client sent an HTTP request to an HTTPS server"

// leCorpoErro lê o corpo bruto de uma resposta de erro e tenta decodificá-lo
// como protocolo.CorpoErro. ok=false cobre tanto falha de leitura quanto
// corpo que não é JSON — só o segundo caso preserva os BYTES originais para
// dicaDeEsquema examinar; uma falha de leitura não tem corpo nenhum a
// examinar.
func leCorpoErro(resp *http.Response) (corpo protocolo.CorpoErro, dica string, ok bool) {
	bruto, err := io.ReadAll(resp.Body)
	if err != nil {
		return corpo, "", false
	}
	if err := json.Unmarshal(bruto, &corpo); err != nil {
		return corpo, dicaDeEsquema(resp.StatusCode, bruto), false
	}
	return corpo, "", true
}

// dicaDeEsquema troca "servidor respondeu 400 sem corpo legível" — mensagem
// sem nenhuma pista — pela causa mais provável: a porta fala TLS
// (server.tls.enabled: true) e o operador esqueceu o https://.
// normalizaEndereco (acima) já ACEITA o esquema quando ele vem escrito; só
// não assume sozinho — a dica fecha essa distância.
func dicaDeEsquema(status int, corpoBruto []byte) string {
	if status != http.StatusBadRequest || !bytes.Contains(corpoBruto, []byte(corpoHTTPQuandoEsperavaHTTPS)) {
		return ""
	}
	return "  a porta parece falar TLS: tente --server https://host:porta\n"
}

// recusaDoServidor trata o erro que chegou ANTES de qualquer linha: o corpo é
// um JSON único e o status é de verdade.
func recusaDoServidor(resp *http.Response, amb Ambiente) int {
	corpo, dica, ok := leCorpoErro(resp)
	if !ok {
		fmt.Fprintf(amb.Stderr, "query: o servidor respondeu %s sem corpo legível\n", resp.Status)
		if dica != "" {
			_, _ = fmt.Fprint(amb.Stderr, dica)
		}
		return ExitUso
	}
	fmt.Fprintf(amb.Stderr, "query: %s\n", corpo.Error.Message)
	if corpo.Error.Line > 0 {
		fmt.Fprintf(amb.Stderr, "  linha %d, coluna %d\n", corpo.Error.Line, corpo.Error.Column)
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		if espera := resp.Header.Get("Retry-After"); espera != "" {
			fmt.Fprintf(amb.Stderr, "  tente de novo em %s s\n", espera)
		}
	}
	return codigoDoProtocolo(corpo.Error.Code)
}

// leStream repassa as rows para o stdout e traduz o fecho em exit code.
//
// As linhas vão CRUAS: decodificá-las para recodificá-las mudaria a grafia dos
// doubles e não acrescentaria nada — o servidor já emitiu o formato final.
func leStream(corpo io.Reader, amb Ambiente) int {
	l := protocolo.NovoLeitor(corpo)
	for l.Next() {
		if _, err := amb.Stdout.Write(append(l.Linha(), '\n')); err != nil {
			fmt.Fprintln(amb.Stderr, "query: escrevendo o resultado:", err)
			return ExitUso
		}
	}
	if err := l.Err(); err != nil {
		// Falha de TRANSPORTE: o stream acabou sem fecho, ou veio malformado.
		// É distinta da falha da consulta, e o operador precisa saber qual das
		// duas aconteceu — aqui o resultado pode estar incompleto sem ninguém
		// ter dito nada.
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitDataset
	}
	if e := l.Erro(); e != nil {
		fmt.Fprintf(amb.Stderr, "query: %s (%d linhas entregues)\n", e.Message, e.Rows)
		return codigoDoProtocolo(e.Code)
	}
	// Fecho normal. O truncated aqui só acontece quando o PRÓPRIO cliente pediu
	// menos linhas, e atender ao que foi pedido não é falha.
	return ExitOK
}

// codigoDoProtocolo traduz o código do wire no exit code do contrato.
//
// A tabela é a mesma do caminho local, e é isso que permite trocar `--config`
// por `--server` num script sem reescrever o tratamento de $?.
func codigoDoProtocolo(codigo string) int {
	switch codigo {
	case protocolo.CodSintaxe, protocolo.CodBind, protocolo.CodBadParameter,
		protocolo.CodClausulaNaoSuportada:
		// UNSUPPORTED_CLAUSE fica junto dos outros erros de QUERY, e não no
		// default: o que o operador faz diante dele é reescrever o SQL, que é
		// o significado deste código. Cair no vala-comum o classificaria como
		// "olhe o comando ou o servidor", que é conselho errado.
		return ExitQuery
	case protocolo.CodMaxRows:
		return ExitTruncado
	case protocolo.CodSnapshotExpirado:
		// O caminho LOCAL já mapeia storage.ErrSnapshotExpirado para 5
		// (codigoDoErro): a consulta — ou a auditoria dos contadores — não
		// chegou ao fim porque o journal de versões estourou o teto. O
		// resultado é desconhecido, não errado; a mesma classe do truncado.
		return ExitTruncado
	case protocolo.CodTimeout:
		return ExitInterromp
	case protocolo.CodDatasetCorrompido:
		return ExitDataset
	case protocolo.CodWriterFailed:
		// Fase 11, Task B5 (POST /v1/compact e /v1/checkpoint): o MESMO
		// sentinela types.ErrWriterFailed que o caminho LOCAL mapeia para 4
		// em codigoDoErro (cli.go) — o writer está em modo falho sticky, e só
		// reabrir o dataset (reiniciar o servidor) o tira de lá. Coerência
		// com a tabela: o exit code não pode depender de a consulta ter
		// chegado local ou remota.
		return ExitWriter
	default:
		// Inclui BAD_REQUEST, UNKNOWN_DATASET, TOO_MANY_QUERIES, INTERNAL,
		// UNAUTHENTICATED e FORBIDDEN (§74-77, Fase 11, Task B3): credencial
		// recusada ou papel insuficiente são erro de quem chamou o comando,
		// não do servidor — a flag de credencial no CLI (--user/
		// --password-file/NISHIDB_PASSWORD, Task B6) já existe, e a
		// degradação com a mensagem do servidor vale para ela também. O que o
		// operador faz com todos eles é olhar o comando ou o servidor, que é
		// o significado do 1.
		//
		// COMPACTION_BUSY (Fase 11, Task B5) cai aqui TAMBÉM de propósito, e
		// não ganha um exit code próprio: é retentável, mas a ação do
		// operador diante dele é a MESMA do resto do vala-comum — olhar o
		// comando (ele mandou compactar de novo cedo demais) ou o servidor
		// (o laço automático está de pé, e o modo manual não se aplica) —,
		// nunca "reprocesse" (7), "reabra o dataset" (4) nem qualquer outro
		// código que já tem significado próprio na tabela. TOO_MANY_QUERIES
		// é o precedente exato: outro 4xx "tente de novo" que também cai no
		// 1.
		return ExitUso
	}
}

// dicaDeServidor sugere o `--server` quando o flock barrou a consulta local.
//
// A mensagem do exit 3 já nomeia o pid que segura o dataset; até esta fase ela
// parava aí, e o operador não tinha para onde ir. Agora tem.
func dicaDeServidor(err error, cfgPath, dataset string, amb Ambiente) {
	if err == nil {
		return
	}
	if !errors.Is(err, storage.ErrDatasetLocked) {
		return
	}
	fmt.Fprintf(amb.Stderr,
		"  o dataset está aberto por um `nishidb server`: consulte por ele com\n"+
			"    nishidb query --server <host>:<porta> --dataset %s '<SQL>'\n"+
			"  (o --config %s vale para a consulta local, que exige o dataset livre)\n",
		dataset, cfgPath)
}
