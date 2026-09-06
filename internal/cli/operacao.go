package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// Os três comandos de LEITURA do §96 — `status`, `datasets` e `inspect`.
//
// Eles nascem REMOTOS, e a decisão é a mesma que a consulta remota tomou ao
// fechar o débito 54: o flock do dataset é exclusivo por processo, então um
// comando de diagnóstico que só soubesse abrir o dataset local exigiria
// DERRUBAR o servidor para perguntar o estado dele. A pergunta e a resposta se
// excluiriam — e um `status` que só responde com o produto parado descreve
// justamente o momento em que não há nada para diagnosticar.
//
// Eles são SÓ leitura, por decisão D3 do roadmap: o `compact` e o `checkpoint`
// que o §96 também lista são rotas MUTANTES, atrás do papel ADMIN — vivem em
// mutacao.go (Task B5), não aqui. Um comando que muda o dataset não vai para
// o mesmo arquivo de um que só imprime números.
//
// O que sai daqui é uma VISTA do /v1/stats e do /v1/metadata, não uma segunda
// contabilidade: nenhum número é recalculado no cliente. É a mesma doutrina que
// mantém o /metrics e o JSON de acordo — duas fontes para o mesmo número
// divergem, e a divergência aparece como um operador que discorda do painel sem
// nada estar quebrado.

func runStatus(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	servidor := fs.String("server", "", "endereço do nishidb server (host:porta)")
	credFlags := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	cred, err := credFlags.resolve()
	if err != nil {
		avisa(amb, "status: %v\n", err)
		return ExitUso
	}
	if fs.NArg() != 0 {
		avisa(amb, "status: não recebe argumentos posicionais\n")
		return ExitUso
	}
	if code := exigeServidor("status", *servidor, amb); code != ExitOK {
		return code
	}

	var e protocol.Estatisticas
	if code := buscaDocumento("status", *servidor, "/v1/stats", cred, &e, amb); code != ExitOK {
		return code
	}
	return escreveSaida("status", amb, func(w io.Writer) { escreveStatus(w, *servidor, e) })
}

func runDatasets(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("datasets", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	servidor := fs.String("server", "", "endereço do nishidb server (host:porta)")
	credFlags := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	cred, err := credFlags.resolve()
	if err != nil {
		avisa(amb, "datasets: %v\n", err)
		return ExitUso
	}
	if fs.NArg() != 0 {
		avisa(amb, "datasets: não recebe argumentos posicionais\n")
		return ExitUso
	}
	if code := exigeServidor("datasets", *servidor, amb); code != ExitOK {
		return code
	}

	var m protocol.Metadados
	if code := buscaDocumento("datasets", *servidor, "/v1/metadata", cred, &m, amb); code != ExitOK {
		return code
	}
	return escreveSaida("datasets", amb, func(w io.Writer) { escreveDatasets(w, *servidor, m) })
}

func runInspect(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	servidor := fs.String("server", "", "endereço do nishidb server (host:porta)")
	credFlags := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	cred, err := credFlags.resolve()
	if err != nil {
		avisa(amb, "inspect: %v\n", err)
		return ExitUso
	}
	// A flag vem ANTES do dataset porque é assim que o flag da stdlib para de
	// parsear — no primeiro argumento que não é flag. A mensagem diz a ordem
	// em vez de deixar o operador diante de "informe um dataset" tendo
	// informado um.
	if fs.NArg() != 1 {
		avisa(amb, "inspect: informe exatamente um dataset\n")
		avisa(amb, "  as flags vêm antes: nishidb inspect --server <host:porta> <dataset>\n")
		return ExitUso
	}
	if code := exigeServidor("inspect", *servidor, amb); code != ExitOK {
		return code
	}
	nome := fs.Arg(0)

	var m protocol.Metadados
	caminho := "/v1/metadata?dataset=" + url.QueryEscape(nome)
	if code := buscaDocumento("inspect", *servidor, caminho, cred, &m, amb); code != ExitOK {
		return code
	}
	// O filtro do §67 responde a MESMA forma com um elemento. Zero elementos
	// não é resposta legítima — o servidor recusa dataset desconhecido com 404
	// —, então chegar aqui vazio é o protocolo tendo mudado sob o comando.
	if len(m.Datasets) != 1 {
		avisa(amb, "inspect: o servidor respondeu %d datasets para %q; esperava exatamente um\n", len(m.Datasets), nome)
		return ExitDataset
	}
	return escreveSaida("inspect", amb, func(w io.Writer) { escreveInspect(w, m.Datasets[0]) })
}

// exigeServidor recusa o comando sem `--server` explicando POR QUE não existe
// caminho local — sem isso o operador procuraria um `--config` que estes
// comandos não têm.
func exigeServidor(cmd, endereco string, amb Ambiente) int {
	if endereco != "" {
		return ExitOK
	}
	avisa(amb, "%s: --server <host:porta> é obrigatório\n", cmd)
	avisa(amb, "  os comandos de leitura do §96 nascem remotos: o flock do dataset é\n"+
		"  exclusivo por processo, e perguntar pelo caminho local exigiria derrubar\n"+
		"  o servidor sobre o qual se pergunta\n")
	return ExitUso
}

// buscaDocumento faz o GET e decodifica o corpo no destino.
//
// Reusa o `normalizaEndereco` e a tradução código→exit code da consulta
// remota, e é isso que mantém UMA tabela de $? para o binário inteiro: um
// script que decide pelo código de saída não pode precisar de uma segunda
// tabela porque trocou `query` por `status`.
//
// Sem timeout no cliente, pelo mesmo argumento do consultaRemota: quem impõe
// prazo é o servidor, e um prazo local faria o operador culpá-lo por uma
// decisão da máquina de onde ele pergunta.
//
// cred entra pelo MESMO clienteRemoto/aplicaCredencial que consultaRemota e
// postaMutacao usam (Task B6) — o client HTTP compartilhado com Proxy: nil e
// o CertPool de --tls-ca, e o Authorization quando --user foi dado.
func buscaDocumento(cmd, endereco, caminho string, cred credencialRemota, destino any, amb Ambiente) int {
	client, err := clienteRemoto(cred)
	if err != nil {
		avisa(amb, "%s: %v\n", cmd, err)
		return ExitUso
	}
	req, err := http.NewRequestWithContext(amb.Ctx, http.MethodGet, normalizaEndereco(endereco)+caminho, nil)
	if err != nil {
		avisa(amb, "%s: %v\n", cmd, err)
		return ExitUso
	}
	aplicaCredencial(req, cred)
	resp, err := client.Do(req)
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
		// Corpo 200 que não decodifica é falha de TRANSPORTE, não de uso: o
		// comando estava certo e a resposta veio quebrada. É a mesma distinção
		// que o leStream faz, e o mesmo código.
		avisa(amb, "%s: resposta ilegível de %s: %v\n", cmd, endereco, err)
		return ExitDataset
	}
	return ExitOK
}

// recusaRemota trata a resposta de erro de QUALQUER rota do protocolo
// remoto — leitura (status/datasets/inspect) e mutante (compact/checkpoint,
// Task B5) por igual: um JSON único com o código estável, e o cliente casa
// por código, nunca pelo texto. Renomeada na revisão da B5 (Minor 6): o
// nome antigo, recusaDeLeitura, já não descrevia os dois comandos mutantes
// que a B5 acrescentou como chamadores.
func recusaRemota(cmd string, resp *http.Response, amb Ambiente) int {
	corpo, dica, ok := leCorpoErro(resp)
	if !ok {
		avisa(amb, "%s: o servidor respondeu %s sem corpo legível\n", cmd, resp.Status)
		if dica != "" {
			avisa(amb, "%s", dica)
		}
		return ExitUso
	}
	avisa(amb, "%s: %s\n", cmd, corpo.Error.Message)
	return codigoDoProtocolo(corpo.Error.Code)
}

// escreveSaida monta a saída inteira em memória e a entrega ao stdout num
// write só.
//
// Ela NÃO é streaming, ao contrário da consulta, e a diferença é de tamanho: o
// resultado de uma query é ilimitado por construção — o §128 proíbe memória
// proporcional ao número de linhas —, enquanto estes três comandos escrevem um
// punhado de linhas por dataset. Montar antes compra duas coisas: o tabwriter
// alinha as colunas, que ele não consegue fazer sem ver a maior célula, e a
// falha de escrita ganha UM ponto de tratamento em vez de um por linha.
func escreveSaida(cmd string, amb Ambiente, corpo func(w io.Writer)) int {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	corpo(tw)
	// O destino é um bytes.Buffer, cujo Write não falha: aqui não há erro
	// possível para tratar, e o `_` explícito diz isso em vez de escondê-lo
	// atrás de um `if err != nil` que nunca roda.
	_ = tw.Flush()

	if _, err := amb.Stdout.Write(buf.Bytes()); err != nil {
		avisa(amb, "%s: escrevendo a saída: %v\n", cmd, err)
		return ExitUso
	}
	return ExitOK
}

func escreveStatus(w io.Writer, endereco string, e protocol.Estatisticas) {
	titulo(w, "servidor %s", endereco)
	titulo(w, "")

	// Os contadores do motor são do PROCESSO e não de um dataset (ver o godoc
	// de protocolo.Estatisticas): repeti-los por dataset publicaria o mesmo
	// número N vezes, e alguém somaria os N.
	titulo(w, "processo")
	q := e.Process.Query
	campo(w, "full scans", strconv.FormatInt(q.FullScans, 10))
	campo(w, "point lookups", strconv.FormatInt(q.PointLookups, 10))
	campo(w, "bitmap scans", strconv.FormatInt(q.BitmapScans, 10))
	campo(w, "index unions", strconv.FormatInt(q.IndexUnions, 10))
	campo(w, "linhas emitidas", strconv.FormatInt(q.Emitted, 10))
	campo(w, "linhas truncadas", strconv.FormatInt(q.Truncated, 10))
	escreveEmVoo(w, e.Process.InFlight)

	for _, d := range e.Datasets {
		titulo(w, "")
		titulo(w, "dataset %s", d.Name)
		campo(w, "rows", strconv.FormatUint(d.Storage.Rows, 10))
		campo(w, "segmentos", strconv.FormatUint(uint64(d.Storage.Segments), 10))
		// O segmento CORRENTE já não é o número de segmentos: o ID nunca
		// regride e a compaction aposenta arquivos, então a distância entre os
		// dois é o que ela devolveu. Publicar só um dos dois esconderia isso.
		campo(w, "corrente", strconv.FormatUint(uint64(d.Storage.CurrentSegment), 10))
		campo(w, "carga do primário", strconv.FormatFloat(d.Storage.PrimaryLoadFactor, 'f', 2, 64))
		campo(w, "overlay", strconv.Itoa(d.Storage.Overlay))
		campo(w, "disco", bytesLegiveis(d.Storage.Bytes))
		campo(w, "garbage (>=)", bytesLegiveis(int64(d.Storage.GarbageBytes)))
		campo(w, "compaction", estadoDaCompaction(d.Storage.Compaction))
		campo(w, "bytes lidos", bytesLegiveis(d.Storage.Compaction.BytesRead))
		campo(w, "bytes escritos", bytesLegiveis(d.Storage.Compaction.BytesWritten))
		campo(w, "consultas", fmt.Sprintf("%d (%d ativas, %d com erro)",
			d.Query.Total, d.Query.Active, d.Query.Errors))
		campo(w, "ingestão", resumoDaIngestao(d.Ingestion))
	}

	titulo(w, "")
	// A ressalva viaja JUNTO do número, e não num manual: `garbage` é limite
	// inferior porque a liveness é limite superior (débitos 38 e 45), e sem
	// isto escrito o operador dimensionaria compaction por um número que não
	// promete o que ele leria nele.
	titulo(w, "(>=) o garbage é limite INFERIOR: a liveness é limite superior, então há")
	titulo(w, "     PELO MENOS isto de lixo em disco — nunca menos.")
}

func escreveDatasets(w io.Writer, endereco string, m protocol.Metadados) {
	titulo(w, "servidor %s", endereco)
	titulo(w, "")
	linha(w, "DATASET", "KEY", "COLUNAS", "MAXROWS", "TIMEOUT")
	for _, d := range m.Datasets {
		linha(w, d.Name, d.Key, strconv.Itoa(len(d.Columns)),
			strconv.Itoa(d.MaxRows), duracao(d.TimeoutMs))
	}
}

func escreveInspect(w io.Writer, d protocol.DatasetMeta) {
	titulo(w, "dataset %s", d.Name)
	campo(w, "key", d.Key)
	// Os dois limites são os EFETIVOS — o do dataset, ou o default global que
	// ele herdou. Quem consulta precisa do teto que VAI VALER, não de onde ele
	// foi declarado.
	campo(w, "maxRows", strconv.Itoa(d.MaxRows))
	campo(w, "timeout", duracao(d.TimeoutMs))
	campo(w, "colunas", strconv.Itoa(len(d.Columns)))
	titulo(w, "")

	linha(w, "COLUNA", "ALIAS", "TIPO", "JDBC", "ÍNDICE", "AUTO")
	for _, c := range d.Columns {
		alias := "-"
		if c.Alias != nil {
			alias = *c.Alias
		}
		auto := "-"
		if c.Auto {
			auto = "sim"
		}
		// O índice sai com a palavra do wire ("bitmap" ou "none") e não como
		// sim/não: é ela que diz se a consulta faz lookup ou cai em full scan,
		// que é a informação que o operador veio buscar.
		//
		// Eram TRÊS palavras, e a terceira distinguia os motores: "hash" saiu
		// quando o motor único tornou a distinção inexistente. As outras duas
		// ficaram por serem contrato — ver o godoc de protocolo.MotorStats e de
		// schema.CompiledField.NomeDoIndice.
		linha(w, c.Name, alias, c.Type, c.JDBC, c.Index, auto)
	}
}

// estadoDaCompaction traduz o 0/1 do §92 em palavra.
//
// "ligada" afirma que o LAÇO está de pé para o dataset, não que há varredura
// em voo neste instante — a passada dura dezenas de milissegundos contra um
// cochilo de segundos. O caso que importa é o inverso: "desligada" com a chave
// ligada no YAML é a falha sticky do writer (§14.14), em que a goroutine parou
// sozinha e o disco voltou a crescer com o processo de pé.
func estadoDaCompaction(c protocol.CompactionStats) string {
	if c.Running {
		return "ligada"
	}
	return "desligada"
}

// resumoDaIngestao usa o vocabulário do §94 — o mesmo do /v1/health —, para
// que as duas leituras não descrevam a mesma realidade com palavras
// diferentes.
func resumoDaIngestao(i protocol.IngestaoStats) string {
	if !i.Enabled {
		return i.State
	}
	s := fmt.Sprintf("%s — %d eventos, %d aplicados, %d stale, %d descartados, %d falhas",
		i.State, i.Events, i.Applied, i.Stale, i.Discarded, i.Failed)
	if i.Paused != "" {
		s += "; parada: " + i.Paused
	}
	return s
}

// bytesLegiveis escreve o número EXATO e, ao lado, a grandeza que se lê de
// relance.
//
// O exato vem primeiro de propósito. O §86 raciocina em GB/dia e o operador
// precisa da ordem de grandeza para saber se o disco aguenta; mas quem compara
// DUAS medições — antes e depois de uma passada de compaction — precisa do
// byte, e "1.2 MiB" antes e "1.2 MiB" depois esconderia a diferença que ele
// veio medir.
func bytesLegiveis(n int64) string {
	if n < 1024 {
		return strconv.FormatInt(n, 10) + " B"
	}
	unidades := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	valor, i := float64(n)/1024, 0
	for valor >= 1024 && i < len(unidades)-1 {
		valor /= 1024
		i++
	}
	return fmt.Sprintf("%d B (%.1f %s)", n, valor, unidades[i])
}

func duracao(ms int64) string { return (time.Duration(ms) * time.Millisecond).String() }

// escreveEmVoo lista as consultas que estão correndo NESTE instante.
//
// Existe porque `consultas: 12 (3 ativas, 0 com erro)` é a mesma frase para
// três point lookups de 2 ms e para um full scan de oito minutos, e as duas
// situações pedem ações opostas. Com o prazo longo virando cidadão de primeira
// classe, a segunda deixou de ser exceção — e o operador precisa ver QUAL
// consulta está lá e HÁ QUANTO TEMPO antes de decidir se reinicia o processo.
//
// A lista vem do servidor já ordenada da mais antiga para a mais nova, e a
// ordem é preservada: a linha que interessa é a primeira, não uma que se
// procura.
//
// Silêncio quando não há nenhuma: um bloco "em voo" vazio em toda chamada de
// `nishidb status` seria ruído, e ruído treina o operador a pular a seção
// justamente no dia em que ela tem conteúdo.
func escreveEmVoo(w io.Writer, voos []protocol.ConsultaEmVoo) {
	if len(voos) == 0 {
		return
	}
	titulo(w, "")
	titulo(w, "consultas em voo (%d)", len(voos))
	// O SQL não aparece — nem aqui nem no /v1/stats: ele pode carregar
	// literais com dado do cliente, e o §95 proíbe publicar payload por
	// default. O hash responde o que o operador pergunta de fato, que é se as
	// consultas presas são a mesma ou várias diferentes.
	linha(w, "DATASET", "SQL", "HÁ", "LONGA")
	for _, v := range voos {
		marca := "-"
		if v.Long {
			marca = "sim"
		}
		linha(w, v.Dataset, v.SQLHash, duracao(v.RunningMs), marca)
	}
}

// titulo, campo e linha são os três formatos desta saída: cabeçalho sem
// coluna, par rótulo/valor e linha de tabela.
//
// Os três descartam o erro de escrita com `_` explícito porque o destino é o
// buffer do escreveSaida, que não falha — o Write de verdade acontece uma vez
// só, lá, e é lá que o erro é tratado.
func titulo(w io.Writer, formato string, args ...any) {
	_, _ = fmt.Fprintf(w, formato+"\n", args...)
}

func campo(w io.Writer, rotulo, valor string) {
	_, _ = fmt.Fprintf(w, "  %s\t%s\n", rotulo, valor)
}

func linha(w io.Writer, celulas ...string) {
	for i, c := range celulas {
		if i > 0 {
			_, _ = io.WriteString(w, "\t")
		}
		_, _ = io.WriteString(w, c)
	}
	_, _ = io.WriteString(w, "\n")
}

// avisa escreve no stderr do comando.
//
// O erro do write é descartado com `_` EXPLÍCITO, e não engolido: não há para
// onde reportar a falha de escrever uma mensagem de falha, e o gate de lint
// deste projeto cobra 0 apontamentos novos — um `fmt.Fprintf` solto aqui viraria
// mais um errcheck na conta que a Task 0 acabou de tornar visível.
func avisa(amb Ambiente, formato string, args ...any) {
	_, _ = fmt.Fprintf(amb.Stderr, formato, args...)
}
