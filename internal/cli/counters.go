package cli

import (
	"flag"
	"io"
	"strconv"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/storage/contador"
)

// `nishidb counters` é a superfície operacional dos contadores mantidos na
// escrita (planning/contadores-mantidos-na-escrita.md §5.6): lista o estado
// de cada contador, audita (recalcula por varredura e compara com o mantido)
// e reconstrói.
//
// Dois caminhos, o MESMO contrato de saída: `--config` abre o dataset local e
// exige o servidor PARADO (toma o flock, como `offsets`); `--server` fala com
// as rotas ADMIN do servidor no ar, que é justamente quando a pergunta da
// auditoria faz sentido. A leitura dos NÚMEROS continua sendo SQL
// (`SELECT … FROM _counters.<nome>`) — este comando responde "posso confiar
// neles?", não "quanto é".
//
// Auditoria com divergência sai com exit 3: um contador que discorda do
// recalculado é corrupção de índice derivado, e o exit code é o que um script
// de operação lê. Auditoria INCONCLUSIVA (o snapshot estourou) sai com 5,
// como a consulta truncada — o número não é confiável nem inconfiável, é
// desconhecido.

func runCounters(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("counters", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig  = fs.String("config", "", "caminho do nishidb.yaml (caminho local: exige o servidor parado)")
		servidor   = fs.String("server", "", "endereço do nishidb server (host:porta)")
		dataset    = fs.String("dataset", "", "nome do dataset")
		audita     = fs.Bool("audit", false, "recalcula os contadores por varredura e compara com o mantido")
		reconstroi = fs.Bool("rebuild", false, "reconstrói os contadores varrendo as linhas vivas (segura o writer)")
	)
	credFlags := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	if fs.NArg() != 0 {
		avisa(amb, "counters: argumento posicional inesperado %q\n", fs.Arg(0))
		return ExitUso
	}
	if (*argConfig == "") == (*servidor == "") {
		avisa(amb, "counters: informe exatamente um de --config (servidor parado) ou --server (servidor no ar)\n")
		return ExitUso
	}
	if *dataset == "" {
		avisa(amb, "counters: --dataset é obrigatório\n")
		return ExitUso
	}
	if *audita && *reconstroi {
		avisa(amb, "counters: --audit e --rebuild são exclusivos\n")
		return ExitUso
	}
	if *servidor != "" {
		cred, err := credFlags.resolve()
		if err != nil {
			avisa(amb, "counters: %v\n", err)
			return ExitUso
		}
		return countersRemoto(*servidor, *dataset, *audita, *reconstroi, cred, amb)
	}
	return countersLocal(*argConfig, *dataset, *audita, *reconstroi, amb)
}

func countersLocal(argConfig, dataset string, audita, reconstroi bool, amb Ambiente) int {
	cfg, err := nishiconfig.CarregaConfig(argConfig)
	if err != nil {
		avisa(amb, "counters: %v\n", err)
		return ExitUso
	}
	st, _, _, err := abreDataset(cfg, dataset)
	if err != nil {
		avisa(amb, "counters: %v\n", err)
		dicaDeServidor(err, argConfig, dataset, amb)
		return codigoDoErro(err)
	}
	defer func() { _ = st.Close() }()

	switch {
	case audita:
		rs, err := st.AuditaContadores(amb.Ctx)
		if err != nil {
			avisa(amb, "counters: auditoria: %v\n", err)
			return codigoDoErro(err)
		}
		linhas := make([]linhaDeAuditoria, 0, len(rs))
		for _, r := range rs {
			linhas = append(linhas, auditoriaDoStorage(r))
		}
		code := escreveSaida("counters", amb, func(w io.Writer) { escreveAuditoria(w, dataset, linhas) })
		return desfechoDaAuditoria(linhas, code, amb)
	case reconstroi:
		recs, err := st.ReconstroiContadores(amb.Ctx)
		if err != nil {
			avisa(amb, "counters: reconstrução: %v\n", err)
			return codigoDoErro(err)
		}
		linhas := make([]linhaDeReconstrucao, 0, len(recs))
		for _, r := range recs {
			linhas = append(linhas, linhaDeReconstrucao{nome: r.Nome, linhas: r.Linhas, grupos: r.Grupos, duracaoMs: r.Duracao.Milliseconds()})
		}
		return escreveSaida("counters", amb, func(w io.Writer) { escreveReconstrucao(w, dataset, linhas) })
	default:
		ests := st.EstatisticasDeContadores()
		linhas := make([]linhaDeContador, 0, len(ests))
		for _, e := range ests {
			linhas = append(linhas, contadorDoStorage(e))
		}
		return escreveSaida("counters", amb, func(w io.Writer) { escreveContadores(w, dataset, linhas) })
	}
}

func countersRemoto(servidor, dataset string, audita, reconstroi bool, cred credencialRemota, amb Ambiente) int {
	switch {
	case audita:
		var resp protocolo.RespostaAuditoriaDeContadores
		if code := postaMutacao("counters", servidor, "/v1/counters/audit", dataset, cred, &resp, amb); code != ExitOK {
			return code
		}
		var linhas []linhaDeAuditoria
		for _, d := range resp.Datasets {
			for _, c := range d.Counters {
				linhas = append(linhas, auditoriaDoWire(c))
			}
		}
		code := escreveSaida("counters", amb, func(w io.Writer) { escreveAuditoria(w, dataset, linhas) })
		return desfechoDaAuditoria(linhas, code, amb)
	case reconstroi:
		var resp protocolo.RespostaReconstrucaoDeContadores
		if code := postaMutacao("counters", servidor, "/v1/counters/rebuild", dataset, cred, &resp, amb); code != ExitOK {
			return code
		}
		var linhas []linhaDeReconstrucao
		for _, d := range resp.Datasets {
			for _, c := range d.Counters {
				linhas = append(linhas, linhaDeReconstrucao{nome: c.Name, linhas: c.Rows, grupos: c.Groups, duracaoMs: c.DurationMs})
			}
		}
		return escreveSaida("counters", amb, func(w io.Writer) { escreveReconstrucao(w, dataset, linhas) })
	default:
		// A listagem é leitura, e sai do /v1/stats — a MESMA estrutura que
		// alimenta as séries nishidb_counter_*.
		var e protocolo.Estatisticas
		if code := buscaDocumento("counters", servidor, "/v1/stats", cred, &e, amb); code != ExitOK {
			return code
		}
		for _, ds := range e.Datasets {
			if ds.Name != dataset {
				continue
			}
			linhas := make([]linhaDeContador, 0, len(ds.Counters))
			for _, c := range ds.Counters {
				linhas = append(linhas, contadorDoWire(c))
			}
			return escreveSaida("counters", amb, func(w io.Writer) { escreveContadores(w, dataset, linhas) })
		}
		avisa(amb, "counters: o dataset %q não está registrado em %s\n", dataset, servidor)
		return ExitUso
	}
}

// As três formas de saída, comuns aos dois caminhos: a tabela é a mesma quer
// o número venha do Store aberto localmente, quer do servidor.

type linhaDeContador struct {
	nome                        string
	linhas, total               int64
	grupos, teto, aposentados   uint32
	recusados, orfas            uint64
	bytes                       int64
	auditDivergentes, auditUnix int64
}

func contadorDoStorage(e contador.Estatisticas) linhaDeContador {
	return linhaDeContador{nome: e.Nome, linhas: e.Linhas, total: e.Total,
		grupos: e.Grupos, teto: e.Teto, aposentados: e.Aposentados,
		recusados: e.Recusados, orfas: e.Orfas, bytes: e.Bytes,
		auditDivergentes: e.AuditoriaDivergentes, auditUnix: e.AuditoriaUltimaUnix}
}

func contadorDoWire(c protocolo.ContadorStats) linhaDeContador {
	return linhaDeContador{nome: c.Name, linhas: c.Rows, total: c.Total,
		grupos: c.Groups, teto: c.MaxGroups, aposentados: c.Retired,
		recusados: c.Refused, orfas: c.OrphanEntries, bytes: c.Bytes,
		auditDivergentes: c.AuditDivergentGroups, auditUnix: c.AuditLastSuccessUnix}
}

func escreveContadores(w io.Writer, dataset string, linhas []linhaDeContador) {
	titulo(w, "dataset %s", dataset)
	titulo(w, "")
	if len(linhas) == 0 {
		titulo(w, "  (o dataset não declara counters)")
		return
	}
	// LINHAS e TOTAL vêm logo depois do nome porque são a pergunta do
	// operador — quantas linhas o contador cobre, e quantas ele conta —; os
	// números de inventário (grupos, teto) e de saúde vêm depois.
	linha(w, "CONTADOR", "LINHAS", "TOTAL", "GRUPOS", "TETO", "APOSENTADOS", "RECUSADOS", "ÓRFÃS", "BYTES", "ÚLTIMA AUDITORIA")
	for _, l := range linhas {
		auditoria := "nunca"
		if l.auditUnix != 0 {
			auditoria = strconv.FormatInt(l.auditDivergentes, 10) + " divergente(s)"
		}
		linha(w, l.nome, strconv.FormatInt(l.linhas, 10), strconv.FormatInt(l.total, 10),
			u32(l.grupos), u32(l.teto), u32(l.aposentados),
			strconv.FormatUint(l.recusados, 10), strconv.FormatUint(l.orfas, 10),
			bytesLegiveis(l.bytes), auditoria)
	}
}

type amostraDeDivergencia struct {
	groupKey                                           string
	mantido, recalculado, refsMantido, refsRecalculado int64
}

type linhaDeAuditoria struct {
	nome                 string
	grupos               uint32
	mantido, recalculado int64
	divergentes          int
	naoInternadas        uint64
	// agregadosDivergentes é 0, 1 ou 2 e sai ao lado da divergência de grupo,
	// no mesmo padrão das linhas sem grupo internado: a coluna LINHAS desta
	// tabela já significa linhas VARRIDAS, e uma coluna nova com outro
	// sentido para a mesma palavra confundiria quem lê.
	agregadosDivergentes int
	linhasVarridas       uint64
	duracaoMs            int64
	amostra              []amostraDeDivergencia
}

func auditoriaDoStorage(r contador.Relatorio) linhaDeAuditoria {
	l := linhaDeAuditoria{nome: r.Nome, grupos: r.Grupos, mantido: r.Mantido, recalculado: r.Recalculado,
		divergentes: r.Divergentes, naoInternadas: r.NaoInternadas,
		agregadosDivergentes: r.AgregadosDivergentes, linhasVarridas: r.LinhasVarridas,
		duracaoMs: r.Duracao.Milliseconds()}
	for _, d := range r.Amostra {
		l.amostra = append(l.amostra, amostraDeDivergencia{d.Texto, d.Mantido, d.Recalculado, d.RefsMantido, d.RefsRecalculado})
	}
	return l
}

func auditoriaDoWire(c protocolo.AuditoriaContador) linhaDeAuditoria {
	l := linhaDeAuditoria{nome: c.Name, grupos: c.Groups, mantido: c.Maintained, recalculado: c.Recomputed,
		divergentes: c.DivergentGroups, naoInternadas: c.NotInterned,
		agregadosDivergentes: c.DivergentAggregates, linhasVarridas: c.RowsScanned,
		duracaoMs: c.DurationMs}
	for _, d := range c.Sample {
		l.amostra = append(l.amostra, amostraDeDivergencia{d.GroupKey, d.Maintained, d.Recomputed, d.RefsMaintained, d.RefsRecomputed})
	}
	return l
}

func escreveAuditoria(w io.Writer, dataset string, linhas []linhaDeAuditoria) {
	titulo(w, "dataset %s", dataset)
	titulo(w, "")
	if len(linhas) == 0 {
		titulo(w, "  (o dataset não declara counters)")
		return
	}
	linha(w, "CONTADOR", "GRUPOS", "MANTIDO", "RECALCULADO", "DIVERGÊNCIA", "LINHAS", "DURAÇÃO")
	for _, l := range linhas {
		divergencia := strconv.Itoa(l.divergentes)
		if l.naoInternadas > 0 {
			divergencia += " (+" + strconv.FormatUint(l.naoInternadas, 10) + " linha(s) sem grupo internado)"
		}
		if l.agregadosDivergentes > 0 {
			divergencia += " (+" + strconv.Itoa(l.agregadosDivergentes) + " agregado(s) divergente(s))"
		}
		linha(w, l.nome, u32(l.grupos), strconv.FormatInt(l.mantido, 10), strconv.FormatInt(l.recalculado, 10),
			divergencia, strconv.FormatUint(l.linhasVarridas, 10), duracao(l.duracaoMs))
	}
	for _, l := range linhas {
		if len(l.amostra) == 0 {
			continue
		}
		titulo(w, "")
		titulo(w, "divergências de %s (até %d):", l.nome, contador.TetoDaAmostra)
		linha(w, "  GRUPO", "MANTIDO", "RECALCULADO", "REFS MANTIDO", "REFS RECALCULADO")
		for _, d := range l.amostra {
			linha(w, "  "+d.groupKey, strconv.FormatInt(d.mantido, 10), strconv.FormatInt(d.recalculado, 10),
				strconv.FormatInt(d.refsMantido, 10), strconv.FormatInt(d.refsRecalculado, 10))
		}
	}
}

// desfechoDaAuditoria decide o exit code depois de a tabela ter saído:
// divergência é corrupção de índice derivado, e sai com ExitDataset (3) — o
// mesmo código de "dataset corrompido", que é o que um script de operação lê.
func desfechoDaAuditoria(linhas []linhaDeAuditoria, code int, amb Ambiente) int {
	if code != ExitOK {
		return code
	}
	divergentes := 0
	for _, l := range linhas {
		if l.divergentes > 0 || l.naoInternadas > 0 || l.agregadosDivergentes > 0 {
			divergentes++
		}
	}
	if divergentes == 0 {
		return ExitOK
	}
	avisa(amb, "counters: %d contador(es) divergem do recalculado; reconstrua com --rebuild e investigue o caminho que esqueceu de descontar\n", divergentes)
	return ExitDataset
}

type linhaDeReconstrucao struct {
	nome      string
	linhas    uint64
	grupos    uint32
	duracaoMs int64
}

func escreveReconstrucao(w io.Writer, dataset string, linhas []linhaDeReconstrucao) {
	titulo(w, "dataset %s", dataset)
	titulo(w, "")
	if len(linhas) == 0 {
		titulo(w, "  (o dataset não declara counters)")
		return
	}
	linha(w, "CONTADOR", "LINHAS", "GRUPOS", "DURAÇÃO")
	for _, l := range linhas {
		linha(w, l.nome, strconv.FormatUint(l.linhas, 10), u32(l.grupos), duracao(l.duracaoMs))
	}
}

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
