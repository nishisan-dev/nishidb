package alarmdelivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

type route struct{ counter, alarm string }

// Manager é o Sink de um dataset: log mais filas independentes por destino.
// Rotas e filas são imutáveis depois de Open; a concorrência mora nas filas.
type Manager struct {
	logSink alarm.Sink
	queues  map[string]*queue
	names   []string
	routes  map[route][]string
}

// Validate recusa referências e segredos inválidos antes de abrir datasets.
// Resolve o token de cada webhook e a segurança de cada destino Kafka (CA e
// credenciais por variável de ambiente) sem falar com o broker: credencial
// ausente recusa a subida, broker fora do ar não. O valor de uma credencial
// nunca entra no erro.
func Validate(cfg *config.Config, specs map[string]*schema.DatasetSpec) error {
	if err := cfg.ValidateAlarmDelivery(); err != nil {
		return err
	}
	names := make(map[string]bool)
	for _, w := range cfg.AlarmDelivery.Webhooks {
		names[w.Name] = true
		if _, err := bearerToken(w); err != nil {
			return err
		}
	}
	for _, k := range cfg.AlarmDelivery.Kafka {
		names[k.Name] = true
		if _, _, err := kafka.ResolveSecurity(k.Security); err != nil {
			return fmt.Errorf("%w: destination %q: %v", config.ErrAlarmDelivery, k.Name, err)
		}
	}
	for _, spec := range specs {
		for _, counter := range spec.Counters {
			for _, a := range counter.Alarms {
				seen := make(map[string]bool)
				for _, name := range a.Notify {
					if !names[name] || seen[name] {
						return fmt.Errorf("%w: dataset %q alarm %q notify: unknown or duplicate destination %q", config.ErrAlarmDelivery, spec.Name, a.Name, name)
					}
					seen[name] = true
				}
				if len(a.Notify) > 0 {
					for _, column := range counter.GroupBy {
						if alarm.ReservedEventKey(column) {
							return fmt.Errorf("%w: dataset %q alarm %q: group column %q collides with a reserved event key", config.ErrAlarmDelivery, spec.Name, a.Name, column)
						}
					}
				}
			}
		}
	}
	return nil
}

func bearerToken(w config.WebhookSpec) (string, error) {
	if w.BearerTokenEnv == "" {
		return "", nil
	}
	token := os.Getenv(w.BearerTokenEnv)
	if strings.TrimSpace(token) == "" || strings.ContainsFunc(token, func(r rune) bool { return r < 32 || r == 127 }) {
		return "", fmt.Errorf("%w: webhook %q bearerTokenEnv %q is missing, empty or invalid", config.ErrAlarmDelivery, w.Name, w.BearerTokenEnv)
	}
	return token, nil
}

// Open recupera inclusive filas cujo alarme deixou de existir. O dono já
// precisa ter o flock do dataset; Close deve ocorrer antes de soltá-lo.
//
// Cada fila ganha o transporte do destino configurado com o seu nome: um
// http.Client para o webhook, um cliente kgo próprio para o Kafka (um por
// dataset × destino, fechado com a fila). A fila é do NOME, não do tipo: um
// destino que troca de tipo mantendo o nome entrega pelo tipo novo o que
// estava pendente, porque o corpo é o mesmo. Nome que saiu do config pausa a
// fila e preserva os arquivos.
func Open(datasetDir string, spec *schema.DatasetSpec, cfg *config.Config, log *logging.Logger) (*Manager, error) {
	if err := Validate(cfg, map[string]*schema.DatasetSpec{spec.Name: spec}); err != nil {
		return nil, err
	}
	if log == nil {
		log = logging.Discard()
	}
	log = log.Dataset(spec.Name).Component("alarm")
	m := &Manager{logSink: alarm.NewLogSink(log), queues: make(map[string]*queue), routes: make(map[route][]string)}
	needed := make(map[string]bool)
	for _, counter := range spec.Counters {
		for _, a := range counter.Alarms {
			m.routes[route{counter.Name, a.Name}] = append([]string(nil), a.Notify...)
			for _, name := range a.Notify {
				needed[name] = true
			}
		}
	}
	root := filepath.Join(datasetDir, "alarm-delivery")
	dirs, err := os.ReadDir(root)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %v", ErrQueue, err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() || !config.ValidDestinationName(dir.Name()) {
			return nil, fmt.Errorf("%w: unexpected destination directory %q", ErrQueue, dir.Name())
		}
		needed[dir.Name()] = true
	}
	webhooks := make(map[string]config.WebhookSpec)
	for _, w := range cfg.AlarmDelivery.Webhooks {
		webhooks[w.Name] = w
	}
	kafkas := make(map[string]config.AlarmKafkaSpec)
	for _, k := range cfg.AlarmDelivery.Kafka {
		kafkas[k.Name] = k
	}
	for name := range needed {
		m.names = append(m.names, name)
	}
	sort.Strings(m.names)
	for _, name := range m.names {
		target, maxBytes, err := destinationTransport(name, webhooks, kafkas)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		q, err := openQueue(filepath.Join(root, name), name, maxBytes, target, log)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.queues[name] = q
		if target == nil {
			log.Warn("alarm.delivery.paused", "destination removed; pending events retained", "destination", name)
		}
	}
	return m, nil
}

// destinationTransport monta o transporte do destino configurado com o nome,
// ou nenhum (fila pausada) quando o nome saiu do config. O teto da fila
// pausada é o default.
func destinationTransport(name string, webhooks map[string]config.WebhookSpec, kafkas map[string]config.AlarmKafkaSpec) (transport, int64, error) {
	if w, ok := webhooks[name]; ok {
		timeout, size, err := w.Limits()
		if err != nil {
			return nil, 0, err
		}
		token, err := bearerToken(w)
		if err != nil {
			return nil, 0, err
		}
		return newHTTPTransport(w.URL, token, timeout), size, nil
	}
	if k, ok := kafkas[name]; ok {
		timeout, size, err := k.Limits()
		if err != nil {
			return nil, 0, err
		}
		target, err := newKafkaTransport(k, timeout)
		if err != nil {
			return nil, 0, err
		}
		return target, size, nil
	}
	return nil, config.DefaultAlarmQueueBytes, nil
}

// Emit preserva o log mesmo quando um ou todos os destinos recusam o evento.
func (m *Manager) Emit(e alarm.Event) {
	m.logSink.Emit(e)
	destinations := m.routes[route{e.Counter, e.Alarm}]
	if len(destinations) == 0 {
		return
	}
	body, err := json.Marshal(e)
	for _, name := range destinations {
		q := m.queues[name]
		if err != nil {
			q.drop(DropEncoding)
			continue
		}
		q.offer(body, e.At)
	}
}

// Start liga somente os remetentes; o gravador já aceita emissões depois de Open.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	for _, q := range m.queues {
		q.start(ctx)
	}
}

// Close cancela a entrega (HTTP ou Kafka) e drena a memória para disco, sem
// esperar pelo destino.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	var errs []error
	for _, q := range m.queues {
		errs = append(errs, q.close())
	}
	return errors.Join(errs...)
}

// Stats lê a mesma estrutura para o wire e para as métricas.
func (m *Manager) Stats() []Stats {
	if m == nil {
		return nil
	}
	out := make([]Stats, 0, len(m.names))
	for _, name := range m.names {
		if q := m.queues[name]; q != nil {
			out = append(out, q.snapshot())
		}
	}
	return out
}
