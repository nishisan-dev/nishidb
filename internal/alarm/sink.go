package alarm

import (
	"encoding/json"
	"log/slog"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// Eventos de log dos alarmes. São CONTRATO: um alarme externo casa a string,
// e o mesmo nome vai no campo `event` do corpo e no cabeçalho `event` do
// registro Kafka (internal/alarmdelivery).
const (
	EventFiring   = "alarm.firing"
	EventResolved = "alarm.resolved"
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

func eventArguments(e Event) []any {
	args := make([]any, 0, 32+2*len(e.Columns))
	var total any = e.Total
	if e.TotalDisabled {
		total = nil
	}
	args = append(args,
		"eventId", e.ID,
		"counter", e.Counter,
		"alarm", e.Alarm,
		"groupKey", e.GroupKey,
		"total", total,
		"rows", e.Rows,
		"condition", e.Condition,
		"sustainedFor", e.SustainedFor.String(),
		"since", e.Since.UTC().Format(logTimeLayout),
		"at", e.At.UTC().Format(logTimeLayout),
	)
	if e.Reason != "" {
		args = append(args, "reason", e.Reason)
	}
	// Só quando declarada, pela razão da lacuna abaixo: presente e vazia, ela
	// obrigaria quem consome a distinguir "não declarei" de "declarei vazio".
	if e.QualifyCondition != "" {
		args = append(args, "qualifyCondition", e.QualifyCondition)
	}
	// valueColumn e firingValue só saem no disparo — Event.ValueColumn fica
	// vazio na resolução, de propósito (ver o godoc do campo): ela não
	// descreve um valor novo. firingValue some sozinho quando a coluna não
	// tinha amostra, pela mesma razão da lacuna abaixo: presente e zerado, ele
	// diria "o valor era zero" para "ninguém observou o valor".
	if e.ValueColumn != "" {
		args = append(args, "valueColumn", e.ValueColumn)
		if e.HasFiringValue {
			args = append(args, "firingValue", e.FiringValue)
		}
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
	if g, ok := metricsAttr(e.Metrics); ok {
		args = append(args, g)
	}
	if g, ok := enrichmentAttr(e.Enrichment); ok {
		args = append(args, g)
	}
	return args
}

func (s *logSink) Emit(e Event) {
	args := eventArguments(e)
	log := s.log.Dataset(e.Dataset)
	switch e.State {
	case StateFiring:
		log.Warn(EventFiring, "a condição do alarme se sustentou pelo prazo declarado", args...)
	case StateResolved:
		log.Info(EventResolved, "o alarme deixou de valer", args...)
	}
}

// MarshalJSON usa a mesma projeção e a mesma supressão de chaves do log.
// Os metadados do logger (nível, timestamp e prosa) não pertencem ao evento.
func (e Event) MarshalJSON() ([]byte, error) {
	fields := map[string]any{"dataset": e.Dataset}
	switch e.State {
	case StateFiring:
		fields["event"] = EventFiring
	case StateResolved:
		fields["event"] = EventResolved
	}
	var record slog.Record
	record.Add(eventArguments(e)...)
	record.Attrs(func(a slog.Attr) bool { fields[a.Key] = eventAttributeValue(a); return true })
	return json.Marshal(fields)
}

func eventAttributeValue(a slog.Attr) any {
	if a.Value.Kind() == slog.KindGroup {
		group := make(map[string]any)
		for _, child := range a.Value.Group() {
			group[child.Key] = eventAttributeValue(child)
		}
		return group
	}
	if logging.IsSensitiveKey(a.Key) {
		return logging.Redacted
	}
	return a.Value.Any()
}

// ReservedEventKey impede JSON ambíguo nos alarmes que habilitam push.
func ReservedEventKey(key string) bool {
	switch key {
	case "eventId", "event", "dataset", "counter", "alarm", "groupKey", "total", "rows",
		"condition", "sustainedFor", "since", "at", "reason", "qualifyCondition",
		"valueColumn", "firingValue", "restored", "downtimeMs", "metrics", "enrichment",
		"timestamp", "level", "component", "message", "time", "msg":
		return true
	}
	return false
}

// metricsAttr monta o grupo `metrics` com as colunas das métricas de janela.
//
// Grupo, e não atributos soltos como as colunas do groupBy, porque os dois
// conjuntos de nomes vêm do YAML e só um deles tem a colisão recusada na
// subida: a recusa garante que `auth_5m_avg` não repete coluna do contador, mas
// nada impede um groupBy chamado `reason` ou `total` de bater num campo do
// próprio evento. Aninhadas, as métricas ficam fora desse espaço de nomes.
//
// Continua sendo slog.Group, e não slog.Any, pelo motivo do logSink: a supressão
// de chaves sensíveis alcança atributos dentro de grupo, e não dentro de um valor
// serializado.
func metricsAttr(ms []Column) (slog.Attr, bool) {
	if len(ms) == 0 {
		return slog.Attr{}, false
	}
	attrs := make([]any, 0, len(ms))
	for _, m := range ms {
		attrs = append(attrs, slog.String(m.Name, m.Value))
	}
	return slog.Group("metrics", attrs...), true
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
