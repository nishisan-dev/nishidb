package cli

import (
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
func consultaRemota(endereco, dataset, sql string, amb Ambiente) int {
	corpo, err := json.Marshal(protocolo.Requisicao{Dataset: dataset, SQL: sql})
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}

	url := normalizaEndereco(endereco) + "/v1/query"
	req, err := http.NewRequestWithContext(amb.Ctx, http.MethodPost, url, strings.NewReader(string(corpo)))
	if err != nil {
		fmt.Fprintln(amb.Stderr, "query:", err)
		return ExitUso
	}
	req.Header.Set("Content-Type", protocolo.ContentTypeJSON)

	// Sem timeout no cliente: o teto de uma consulta é o query.timeout do
	// dataset, que o servidor aplica. Um prazo local mataria um stream longo
	// legítimo e faria o operador culpar o servidor.
	resp, err := http.DefaultClient.Do(req)
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

// recusaDoServidor trata o erro que chegou ANTES de qualquer linha: o corpo é
// um JSON único e o status é de verdade.
func recusaDoServidor(resp *http.Response, amb Ambiente) int {
	var corpo protocolo.CorpoErro
	if err := json.NewDecoder(resp.Body).Decode(&corpo); err != nil {
		fmt.Fprintf(amb.Stderr, "query: o servidor respondeu %s sem corpo legível\n", resp.Status)
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
	case protocolo.CodSintaxe, protocolo.CodBind, protocolo.CodBadParameter:
		return ExitQuery
	case protocolo.CodMaxRows:
		return ExitTruncado
	case protocolo.CodTimeout:
		return ExitInterromp
	case protocolo.CodDatasetCorrompido:
		return ExitDataset
	default:
		// Inclui BAD_REQUEST, UNKNOWN_DATASET, TOO_MANY_QUERIES, INTERNAL,
		// UNAUTHENTICATED e FORBIDDEN (§74-77, Fase 11, Task B3): credencial
		// recusada ou papel insuficiente são erro de quem chamou o comando,
		// não do servidor — a flag de credencial no CLI é da B6, mas a
		// degradação com a mensagem do servidor já vale hoje. O que o
		// operador faz com todos eles é olhar o comando ou o servidor, que é
		// o significado do 1.
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
