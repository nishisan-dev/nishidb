package alarm

import (
	"log/slog"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// Eventos de log dos alarmes. São CONTRATO: um alarme externo casa a string.
const (
	eventFiring   = "alarm.firing"
	eventResolved = "alarm.resolved"
)

// logSink publica o evento como uma linha de log estruturada.
//
// # Por que os campos são atributos soltos, e não um objeto
//
// O handler de log SUPRIME chaves sensíveis (`token`, `senha`, `value`,
// `record`, `payload`, entre outras) e faz isso em qualquer profundidade de
// grupo — mas NÃO dentro de um valor serializado por slog.Any, porque ali o
// encoding/json toma conta e o handler não vê mais atributo nenhum. Emitir o
// evento inteiro como um objeto só o tiraria do alcance da supressão.
//
// Como as colunas do groupBy têm nome vindo do YAML do cliente, uma coluna
// chamada `token` cai na supressão — e é o desfecho certo: perde-se aquele
// valor, não o evento.
type logSink struct{ log *logging.Logger }

// NewLogSink devolve o Sink que escreve no log estruturado.
func NewLogSink(log *logging.Logger) Sink {
	if log == nil {
		log = logging.Discard()
	}
	return &logSink{log: log.Component("alarm")}
}

func (s *logSink) Emit(e Event) {
	args := make([]any, 0, 24+2*len(e.Columns))
	args = append(args,
		"counter", e.Counter,
		"alarm", e.Alarm,
		"groupKey", e.GroupKey,
		"total", e.Total,
		"condition", e.Condition,
		"sustainedFor", e.SustainedFor.String(),
		"since", e.Since.UTC().Format(logTimeLayout),
		"at", e.At.UTC().Format(logTimeLayout),
	)
	if e.Reason != "" {
		args = append(args, "reason", e.Reason)
	}
	// A marca de restauro e a lacuna saem SÓ quando houve restauro: um campo
	// sempre presente, com zero, diria "o processo não ficou fora" para todo
	// alarme observado de ponta a ponta — verdade indistinguível de "não sei".
	if e.Restored {
		args = append(args, "restored", true, "downtimeMs", e.DowntimeMs)
	}
	// As colunas do groupBy entram como campos próprios porque quem consome o
	// log filtra por uma delas, e repartir o texto do grupo exigiria conhecer
	// o keyFormat com que ele foi montado.
	for _, c := range e.Columns {
		args = append(args, c.Name, c.Value)
	}
	if g, ok := enrichmentAttr(e.Enrichment); ok {
		args = append(args, g)
	}
	switch e.State {
	case StateFiring:
		s.log.Warn(eventFiring,
			"a condição do alarme se sustentou pelo prazo declarado", args...)
	case StateResolved:
		s.log.Info(eventResolved,
			"o alarme deixou de valer", args...)
	}
}

// enrichmentAttr monta o atributo aninhado dos enriquecimentos, indexado pelo
// NOME declarado de cada consulta.
//
// Um evento sem enriquecimento declarado não ganha o campo: presente e vazio,
// ele obrigaria quem consome a distinguir "não declarei" de "declarei e não veio
// nada" pelo conteúdo.
//
// As linhas entram como json.RawMessage para chegarem ao log como JSON de
// verdade, navegável — e não como uma string com o JSON escapado dentro, que é
// o que uma serialização ingênua produziria e o que impediria um `jq` de
// indexar dentro do resultado.
func enrichmentAttr(es []Enrichment) (slog.Attr, bool) {
	if len(es) == 0 {
		return slog.Attr{}, false
	}
	blocos := make([]any, 0, len(es))
	for _, e := range es {
		attrs := make([]any, 0, 2)
		if len(e.Rows) > 0 {
			attrs = append(attrs, slog.Any("rows", e.Rows))
		}
		if e.Note != "" {
			attrs = append(attrs, slog.String("note", e.Note))
		}
		blocos = append(blocos, slog.Group(e.Name, attrs...))
	}
	return slog.Group("enrichment", blocos...), true
}

// logTimeLayout é o formato de instante das chaves do evento: o mesmo RFC3339
// com milissegundos que o timestamp da própria linha usa, para que os três
// instantes de um evento sejam comparáveis sem conversão.
const logTimeLayout = "2006-01-02T15:04:05.000Z"
