package cli

import (
	"flag"
	"io"
	"strconv"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
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

func runCounters(args []string, amb Context) int {
	fs := flag.NewFlagSet("counters", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig = fs.String("config", "", "caminho do nishidb.yaml (caminho local: exige o servidor parado)")
		server    = fs.String("server", "", "endereço do nishidb server (host:porta)")
		dataset   = fs.String("dataset", "", "nome do dataset")
		audit     = fs.Bool("audit", false, "recalcula os contadores por varredura e compara com o mantido")
		rebuild   = fs.Bool("rebuild", false, "reconstrói os contadores varrendo as linhas vivas (segura o writer)")
	)
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	if fs.NArg() != 0 {
		notify(amb, "counters: argumento posicional inesperado %q\n", fs.Arg(0))
		return ExitUsageError
	}
	if (*argConfig == "") == (*server == "") {
		notify(amb, "counters: informe exatamente um de --config (servidor parado) ou --server (servidor no ar)\n")
		return ExitUsageError
	}
	if *dataset == "" {
		notify(amb, "counters: --dataset é obrigatório\n")
		return ExitUsageError
	}
	if *audit && *rebuild {
		notify(amb, "counters: --audit e --rebuild são exclusivos\n")
		return ExitUsageError
	}
	if *server != "" {
		cred, err := credFlags.resolve()
		if err != nil {
			notify(amb, "counters: %v\n", err)
			return ExitUsageError
		}
		return countersRemote(*server, *dataset, *audit, *rebuild, cred, amb)
	}
	return countersLocal(*argConfig, *dataset, *audit, *rebuild, amb)
}

func countersLocal(argConfig, dataset string, audita, reconstroi bool, amb Context) int {
	cfg, err := nishiconfig.Load(argConfig)
	if err != nil {
		notify(amb, "counters: %v\n", err)
		return ExitUsageError
	}
	st, _, _, err := openDataset(cfg, dataset)
	if err != nil {
		notify(amb, "counters: %v\n", err)
		serverHint(err, argConfig, dataset, amb)
		return errorCode(err)
	}
	defer func() { _ = st.Close() }()

	switch {
	case audita:
		rs, err := st.AuditaCounters(amb.Ctx)
		if err != nil {
			notify(amb, "counters: auditoria: %v\n", err)
			return errorCode(err)
		}
		rows := make([]rowOfAudit, 0, len(rs))
		for _, r := range rs {
			rows = append(rows, auditOfStorage(r))
		}
		code := writeOutputWithContext("counters", amb, func(w io.Writer) { writeAudit(w, dataset, rows) })
		return auditOutcome(rows, code, amb)
	case reconstroi:
		recs, err := st.ReconstroiCounters(amb.Ctx)
		if err != nil {
			notify(amb, "counters: reconstrução: %v\n", err)
			return errorCode(err)
		}
		rows := make([]rowOfRebuild, 0, len(recs))
		for _, r := range recs {
			rows = append(rows, rowOfRebuild{name: r.Name, rows: r.Rows, groups: r.Groups, durationMs: r.Duration.Milliseconds()})
		}
		return writeOutputWithContext("counters", amb, func(w io.Writer) { writeReconstruction(w, dataset, rows) })
	default:
		ests := st.EstatisticasOfCounters()
		rows := make([]rowOfCounter, 0, len(ests))
		for _, e := range ests {
			rows = append(rows, storageCounter(e))
		}
		return writeOutputWithContext("counters", amb, func(w io.Writer) { writeCounters(w, dataset, rows) })
	}
}

func countersRemote(server, dataset string, audita, reconstroi bool, cred remoteCredential, amb Context) int {
	switch {
	case audita:
		var resp protocol.CountersAuditResponse
		if code := postMutation("counters", server, "/v1/counters/audit", dataset, cred, &resp, amb); code != ExitOK {
			return code
		}
		var rows []rowOfAudit
		for _, d := range resp.Datasets {
			for _, c := range d.Counters {
				rows = append(rows, auditOfWire(c))
			}
		}
		code := writeOutputWithContext("counters", amb, func(w io.Writer) { writeAudit(w, dataset, rows) })
		return auditOutcome(rows, code, amb)
	case reconstroi:
		var resp protocol.CountersRebuildResponse
		if code := postMutation("counters", server, "/v1/counters/rebuild", dataset, cred, &resp, amb); code != ExitOK {
			return code
		}
		var rows []rowOfRebuild
		for _, d := range resp.Datasets {
			for _, c := range d.Counters {
				rows = append(rows, rowOfRebuild{name: c.Name, rows: c.Rows, groups: c.Groups, durationMs: c.DurationMs})
			}
		}
		return writeOutputWithContext("counters", amb, func(w io.Writer) { writeReconstruction(w, dataset, rows) })
	default:
		// A listagem é leitura, e sai do /v1/stats — a MESMA estrutura que
		// alimenta as séries nishidb_counter_*.
		var e protocol.Stats
		if code := searchDocument("counters", server, "/v1/stats", cred, &e, amb); code != ExitOK {
			return code
		}
		for _, ds := range e.Datasets {
			if ds.Name != dataset {
				continue
			}
			rows := make([]rowOfCounter, 0, len(ds.Counters))
			for _, c := range ds.Counters {
				rows = append(rows, wireCounter(c))
			}
			return writeOutputWithContext("counters", amb, func(w io.Writer) { writeCounters(w, dataset, rows) })
		}
		notify(amb, "counters: o dataset %q não está registrado em %s\n", dataset, server)
		return ExitUsageError
	}
}

// As três formas de saída, comuns aos dois caminhos: a tabela é a mesma quer
// o número venha do Store aberto localmente, quer do servidor.

type rowOfCounter struct {
	name                         string
	rows, total                  int64
	groups, ceiling, aposentados uint32
	recusados, orfas             uint64
	bytes                        int64
	auditDivergentes, auditUnix  int64
}

func storageCounter(e counter.Stats) rowOfCounter {
	return rowOfCounter{name: e.Name, rows: e.Rows, total: e.Total,
		groups: e.Groups, ceiling: e.Ceiling, aposentados: e.Retired,
		recusados: e.Refused, orfas: e.Orphans, bytes: e.Bytes,
		auditDivergentes: e.AuditDivergences, auditUnix: e.AuditLastUnix}
}

func wireCounter(c protocol.CounterStats) rowOfCounter {
	return rowOfCounter{name: c.Name, rows: c.Rows, total: c.Total,
		groups: c.Groups, ceiling: c.MaxGroups, aposentados: c.Retired,
		recusados: c.Refused, orfas: c.OrphanEntries, bytes: c.Bytes,
		auditDivergentes: c.AuditDivergentGroups, auditUnix: c.AuditLastSuccessUnix}
}

func writeCounters(w io.Writer, dataset string, rows []rowOfCounter) {
	title(w, "dataset %s", dataset)
	title(w, "")
	if len(rows) == 0 {
		title(w, "  (o dataset não declara counters)")
		return
	}
	// LINHAS e TOTAL vêm logo depois do nome porque são a pergunta do
	// operador — quantas linhas o contador cobre, e quantas ele conta —; os
	// números de inventário (grupos, teto) e de saúde vêm depois.
	line(w, "CONTADOR", "LINHAS", "TOTAL", "GRUPOS", "TETO", "APOSENTADOS", "RECUSADOS", "ÓRFÃS", "BYTES", "ÚLTIMA AUDITORIA")
	for _, l := range rows {
		audit := "nunca"
		if l.auditUnix != 0 {
			audit = strconv.FormatInt(l.auditDivergentes, 10) + " divergente(s)"
		}
		line(w, l.name, strconv.FormatInt(l.rows, 10), strconv.FormatInt(l.total, 10),
			u32(l.groups), u32(l.ceiling), u32(l.aposentados),
			strconv.FormatUint(l.recusados, 10), strconv.FormatUint(l.orfas, 10),
			bytesToReadableString(l.bytes), audit)
	}
}

type amostraOfDivergencia struct {
	groupKey                                           string
	mantido, recalculado, refsMantido, refsRecalculado int64
}

type rowOfAudit struct {
	name                 string
	groups               uint32
	mantido, recalculado int64
	divergentes          int
	naoInternadas        uint64
	// agregadosDivergentes é 0, 1 ou 2 e sai ao lado da divergência de grupo,
	// no mesmo padrão das linhas sem grupo internado: a coluna LINHAS desta
	// tabela já significa linhas VARRIDAS, e uma coluna nova com outro
	// sentido para a mesma palavra confundiria quem lê.
	agregadosDivergentes int
	rowsVarridas         uint64
	durationMs           int64
	amostra              []amostraOfDivergencia
}

func auditOfStorage(r counter.Report) rowOfAudit {
	l := rowOfAudit{name: r.Name, groups: r.Groups, mantido: r.Maintained, recalculado: r.Recomputed,
		divergentes: r.DivergentGroups, naoInternadas: r.NotInterned,
		agregadosDivergentes: r.DivergentAggregates, rowsVarridas: r.RowsScanned,
		durationMs: r.Duration.Milliseconds()}
	for _, d := range r.Sample {
		l.amostra = append(l.amostra, amostraOfDivergencia{d.Text, d.Maintained, d.Recomputed, d.RefsMaintained, d.RefsRecomputed})
	}
	return l
}

func auditOfWire(c protocol.CounterAudit) rowOfAudit {
	l := rowOfAudit{name: c.Name, groups: c.Groups, mantido: c.Maintained, recalculado: c.Recomputed,
		divergentes: c.DivergentGroups, naoInternadas: c.NotInterned,
		agregadosDivergentes: c.DivergentAggregates, rowsVarridas: c.RowsScanned,
		durationMs: c.DurationMs}
	for _, d := range c.Sample {
		l.amostra = append(l.amostra, amostraOfDivergencia{d.GroupKey, d.Maintained, d.Recomputed, d.RefsMaintained, d.RefsRecomputed})
	}
	return l
}

func writeAudit(w io.Writer, dataset string, rows []rowOfAudit) {
	title(w, "dataset %s", dataset)
	title(w, "")
	if len(rows) == 0 {
		title(w, "  (o dataset não declara counters)")
		return
	}
	line(w, "CONTADOR", "GRUPOS", "MANTIDO", "RECALCULADO", "DIVERGÊNCIA", "LINHAS", "DURAÇÃO")
	for _, l := range rows {
		divergencia := strconv.Itoa(l.divergentes)
		if l.naoInternadas > 0 {
			divergencia += " (+" + strconv.FormatUint(l.naoInternadas, 10) + " linha(s) sem grupo internado)"
		}
		if l.agregadosDivergentes > 0 {
			divergencia += " (+" + strconv.Itoa(l.agregadosDivergentes) + " agregado(s) divergente(s))"
		}
		line(w, l.name, u32(l.groups), strconv.FormatInt(l.mantido, 10), strconv.FormatInt(l.recalculado, 10),
			divergencia, strconv.FormatUint(l.rowsVarridas, 10), duration(l.durationMs))
	}
	for _, l := range rows {
		if len(l.amostra) == 0 {
			continue
		}
		title(w, "")
		title(w, "divergências de %s (até %d):", l.name, counter.SampleCeiling)
		line(w, "  GRUPO", "MANTIDO", "RECALCULADO", "REFS MANTIDO", "REFS RECALCULADO")
		for _, d := range l.amostra {
			line(w, "  "+d.groupKey, strconv.FormatInt(d.mantido, 10), strconv.FormatInt(d.recalculado, 10),
				strconv.FormatInt(d.refsMantido, 10), strconv.FormatInt(d.refsRecalculado, 10))
		}
	}
}

// desfechoDaAuditoria decide o exit code depois de a tabela ter saído:
// divergência é corrupção de índice derivado, e sai com ExitDataset (3) — o
// mesmo código de "dataset corrompido", que é o que um script de operação lê.
func auditOutcome(rows []rowOfAudit, code int, amb Context) int {
	if code != ExitOK {
		return code
	}
	divergentes := 0
	for _, l := range rows {
		if l.divergentes > 0 || l.naoInternadas > 0 || l.agregadosDivergentes > 0 {
			divergentes++
		}
	}
	if divergentes == 0 {
		return ExitOK
	}
	notify(amb, "counters: %d contador(es) divergem do recalculado; reconstrua com --rebuild e investigue o caminho que esqueceu de descontar\n", divergentes)
	return ExitDataset
}

type rowOfRebuild struct {
	name       string
	rows       uint64
	groups     uint32
	durationMs int64
}

func writeReconstruction(w io.Writer, dataset string, rows []rowOfRebuild) {
	title(w, "dataset %s", dataset)
	title(w, "")
	if len(rows) == 0 {
		title(w, "  (o dataset não declara counters)")
		return
	}
	line(w, "CONTADOR", "LINHAS", "GRUPOS", "DURAÇÃO")
	for _, l := range rows {
		line(w, l.name, strconv.FormatUint(l.rows, 10), u32(l.groups), duration(l.durationMs))
	}
}

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
