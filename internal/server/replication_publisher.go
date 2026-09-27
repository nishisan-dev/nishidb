package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// publisherCloseBudget é o orçamento PRÓPRIO da drenagem dos publicadores no
// encerramento. Próprio, e não o contexto de 15 s do http.Shutdown: um
// long-poll ou uma consulta longa que gastasse aquele prazo deixaria o
// publicador sem tempo nenhum para entregar a cauda do feed. Os publicadores
// drenam em paralelo, então o teto vale para todos juntos. Server.publisherBudget
// o substitui só em teste; zero vale este.
const publisherCloseBudget = 10 * time.Second

// validateReplicationTargets confere os `replication.targets` de todos os
// datasets contra os `kafkaTargets:` do nishidb.yaml e contra os outros usos
// de Kafka do diretório, ANTES de qualquer lock de dataset — no mesmo ponto e
// com o mesmo sentinela (config.ErrKafkaTarget, exit de uso) da conferência do
// `emit.target` dos contadores.
//
// Cinco recusas, cada uma com a própria mensagem:
//   - destino que não existe em `kafkaTargets:`;
//   - destino que também é `emit.target` de algum contador: o tópico levaria
//     dois formatos de envelope, e o consumidor de um leria o outro como lixo;
//   - destino usado pela replicação de dois datasets: os controles (`reset`,
//     `snapshot_end`) de um poderiam podar as linhas do outro;
//   - destino que, sob OUTRO nome, aponta para o mesmo tópico de outro destino
//     em uso (replicação de qualquer dataset ou `emit.target` de contador) no
//     mesmo cluster — é a mistura das duas regras acima escondida atrás de um
//     apelido;
//   - destino cujo tópico é o `source.kafka.topic` de algum dataset no mesmo
//     cluster: o feed reingeriria o próprio envelope como evento.
//
// "Mesmo cluster" é a interseção dos `bootstrapServers`, comparados como
// host:porta sem espaço e em minúsculas. Apelidos de DNS (dois nomes para o
// mesmo broker, ou IP de um lado e nome do outro) NÃO são detectados: a
// conferência é textual, e resolver nomes na subida a tornaria dependente da
// rede.
//
// A forma de cada nome, o papel master e o `enabled` já foram conferidos pelo
// parse da spec; o que só dá para conferir aqui é o que cruza arquivos.
func validateReplicationTargets(cfg *config.Config, specs map[string]*schema.DatasetSpec) error {
	targets := make(map[string]config.KafkaTargetSpec, len(cfg.KafkaTargets))
	for _, k := range cfg.KafkaTargets {
		targets[k.Name] = k
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	// Alfabética, como a abertura dos datasets: a recusa que aparece é a
	// mesma em duas subidas com a mesma configuração.
	sort.Strings(names)
	type counterUse struct{ dataset, counter string }
	counterTargets := make(map[string]counterUse)
	for _, name := range names {
		for _, c := range specs[name].Counters {
			if c.Emit == nil {
				continue
			}
			for _, target := range c.Emit.Targets {
				if _, seen := counterTargets[target]; !seen {
					counterTargets[target] = counterUse{dataset: name, counter: c.Name}
				}
			}
		}
	}
	type replicationUse struct{ dataset, target string }
	var replicated []replicationUse
	replicatedBy := make(map[string]string)
	for _, name := range names {
		for _, target := range specs[name].Replication.Targets {
			if _, ok := targets[target]; !ok {
				return fmt.Errorf("%w: dataset %q replication target %q is not declared in kafkaTargets",
					config.ErrKafkaTarget, name, target)
			}
			if use, shared := counterTargets[target]; shared {
				return fmt.Errorf("%w: dataset %q replication target %q is also the emit target of counter %q in dataset %q; a topic carries a single envelope format",
					config.ErrKafkaTarget, name, target, use.counter, use.dataset)
			}
			if other, shared := replicatedBy[target]; shared {
				return fmt.Errorf("%w: replication target %q is used by datasets %q and %q; each dataset needs its own topic",
					config.ErrKafkaTarget, target, other, name)
			}
			replicatedBy[target] = name
			replicated = append(replicated, replicationUse{dataset: name, target: target})
		}
	}
	// Os outros destinos EM USO, pelo nome: a replicação de qualquer dataset e
	// o emit.target de qualquer contador. "file" não é destino Kafka, e o nome
	// desconhecido de contador é recusado pelo seriesdelivery.
	inUse := make(map[string]string, len(replicatedBy)+len(counterTargets))
	for target, name := range replicatedBy {
		inUse[target] = fmt.Sprintf("the replication target of dataset %q", name)
	}
	for target, use := range counterTargets {
		if _, ok := targets[target]; ok {
			inUse[target] = fmt.Sprintf("the emit target of counter %q in dataset %q", use.counter, use.dataset)
		}
	}
	others := make([]string, 0, len(inUse))
	for target := range inUse {
		others = append(others, target)
	}
	sort.Strings(others)
	for _, r := range replicated {
		spec := targets[r.target]
		for _, other := range others {
			o := targets[other]
			if other == r.target || o.Topic != spec.Topic || !sameCluster(spec.BootstrapServers, o.BootstrapServers) {
				continue
			}
			return fmt.Errorf("%w: dataset %q replication target %q writes to topic %q on the same cluster as target %q, %s; a topic carries a single envelope format",
				config.ErrKafkaTarget, r.dataset, r.target, spec.Topic, other, inUse[other])
		}
		for _, name := range names {
			src := specs[name].Source.Kafka
			if src == nil || src.Topic != spec.Topic || !sameCluster(spec.BootstrapServers, src.BootstrapServers) {
				continue
			}
			return fmt.Errorf("%w: dataset %q replication target %q writes to topic %q, which dataset %q consumes on the same cluster; the feed would ingest its own envelopes",
				config.ErrKafkaTarget, r.dataset, r.target, spec.Topic, name)
		}
	}
	return nil
}

// sameCluster diz se as duas listas de bootstrap têm algum host:porta em
// comum, comparados sem espaço e em minúsculas. Textual de propósito: ver o
// godoc de validateReplicationTargets sobre apelidos de DNS.
func sameCluster(a, b []string) bool {
	seen := make(map[string]bool, len(a))
	for _, s := range a {
		seen[strings.ToLower(strings.TrimSpace(s))] = true
	}
	for _, s := range b {
		if seen[strings.ToLower(strings.TrimSpace(s))] {
			return true
		}
	}
	return false
}

// openReplicationPublishers monta um publicador por destino de cada dataset
// master com feed. É chamado depois de todos os datasets abertos, e não dentro do
// openDataset: a tabela irmã, que o publicador exporta junto com o base, só
// existe no fim da abertura dele.
//
// Não inicia nada: o Start é do Run, logo depois do Start do feed. Um broker
// fora do ar não impede a subida (o New não fala com o broker); a falha de
// construção — segurança que não resolve, por exemplo — impede, com o
// sentinela do destino.
func (s *Server) openReplicationPublishers(log *logging.Logger) error {
	targets := make(map[string]config.KafkaTargetSpec, len(s.cfg.KafkaTargets))
	for _, k := range s.cfg.KafkaTargets {
		targets[k.Name] = k
	}
	for _, d := range s.datasets {
		// O feed só existe no master (openDataset): o seguidor e a tabela
		// irmã nunca publicam, qualquer que seja a spec.
		if d.replication == nil || len(d.spec.Replication.Targets) == 0 {
			continue
		}
		tables := []replication.SnapshotTable{{Name: d.nome, Source: d.st}}
		historyName := ""
		var history replicationkafka.FieldSource
		if d.irma != nil {
			tables = append(tables, replication.SnapshotTable{Name: d.irma.nome, History: true, Source: d.irma.st})
			historyName = d.irma.nome
			// O Compiled PRÓPRIO da irmã (openHistory), e não o do base: é
			// ele que resolve as colunas da histórica pelo FieldID.
			history = d.irma.sch
		}
		for _, name := range d.spec.Replication.Targets {
			target, ok := targets[name]
			if !ok {
				// validateReplicationTargets recusa isto antes dos locks;
				// chegar aqui é bug, e a mensagem é outra de propósito.
				return fmt.Errorf("dataset %q: replication target %q reached the publisher without startup validation", d.nome, name)
			}
			// Um Renderer por publicador: ele não é seguro para uso
			// concorrente, e cada publicador é uma goroutine.
			renderer, err := replicationkafka.NewRenderer(d.nome, historyName, d.sch, history)
			if err != nil {
				return fmt.Errorf("dataset %q: replication target %q: %w", d.nome, name, err)
			}
			p, err := replicationkafka.New(replicationkafka.Options{
				Dataset:  d.nome,
				Target:   target,
				Feed:     d.replication,
				Tables:   tables,
				Renderer: renderer,
				Logger:   log,
				Now:      s.now,
			})
			if err != nil {
				return fmt.Errorf("dataset %q: replication target %q: %w", d.nome, name, err)
			}
			d.publishers = append(d.publishers, p)
		}
	}
	return nil
}

// startPublishers inicia os publicadores de um dataset. Vem DEPOIS do Start
// do feed, no Run: antes dele o feed não fecha lote por intervalo.
func startPublishers(d *dataset) {
	for _, p := range d.publishers {
		p.Start()
	}
}

// closeFeeds fecha os feeds de replicação. Vem ANTES do http.Shutdown no
// encerra(): o Close acorda o long-poll de todo seguidor, que recebe o fim do
// feed na hora em vez de segurar a requisição até o prazo do Shutdown; e é
// o Close que faz o flush do intervalo ativo e deixa os publicadores drenarem
// a cauda e saírem. Idempotente, como o Close do feed.
//
// O que for publicado DEPOIS do Close é descartado pelo feed: passadas de
// expiração, retenção ou reconstrução ainda em voo, e as rotas ADMIN que
// escrevem até o http.Shutdown. Nada se perde do dataset — a mudança está no
// disco —, e a próxima subida abre uma epoch nova e publica `reset` e um
// snapshot completo, que a inclui.
func (s *Server) closeFeeds() {
	for _, d := range s.datasets {
		if d.replication != nil {
			d.replication.Close()
		}
	}
}

// closePublishers drena e para todos os publicadores, em paralelo, com o
// orçamento próprio publisherCloseBudget. Tem de vir DEPOIS do closeFeeds
// (senão cada publicador espera o prazo inteiro por um fim de feed que não
// vem) e ANTES do closeDatasets (o snapshot em curso lê os Stores).
// Idempotente, como o Close do publicador.
//
// A drenagem que não termina no prazo (broker fora, tópico lento) vira WARN,
// e não erro do encerramento: nada do dataset se perde — o estado está no
// disco, e a próxima subida publica `reset` e um snapshot completo —, e um
// broker fora no instante do SIGTERM não é falha do processo.
func (s *Server) closePublishers() {
	budget := s.publisherBudget
	if budget <= 0 {
		budget = publisherCloseBudget
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var wg sync.WaitGroup
	for _, d := range s.datasets {
		for _, p := range d.publishers {
			wg.Add(1)
			go func(name string, p *replicationkafka.Publisher) {
				defer wg.Done()
				if err := p.Close(ctx); err != nil {
					s.log.Warn("replication.publisher.close_failed",
						"the replication publisher did not drain the feed tail before the deadline; the next start publishes a reset and a full snapshot",
						"dataset", name, "destination", p.Stats().Destination, "error", err.Error())
				}
			}(d.nome, p)
		}
	}
	wg.Wait()
}

// replicationStats é o retrato do feed e dos publicadores de um dataset, ou
// nil no dataset sem feed. É a fonte ÚNICA do bloco `replication` do
// /v1/stats e das séries nishidb_replication_*: dois caminhos lendo o mesmo
// fato por conta própria divergem no dia em que um deles muda.
func replicationStats(d *dataset) *protocol.ReplicationStats {
	if d.replication == nil {
		return nil
	}
	st := d.replication.Status()
	out := &protocol.ReplicationStats{
		Feed: protocol.ReplicationFeedStats{
			Epoch:           st.Epoch,
			Produced:        st.Produced,
			AvailableAfter:  st.AvailableAfter,
			BufferedBytes:   st.BufferedBytes,
			InputEntries:    st.InputMessages,
			OutputEntries:   st.OutputMessages,
			EvictedBatches:  st.EvictedBatches,
			Flushes:         st.Flushes,
			LastFlushReason: st.LastFlushReason,
			Invalidations:   st.Invalidations,
		},
		Delivery: make([]protocol.ReplicationDeliveryStats, 0, len(d.publishers)),
	}
	for _, p := range d.publishers {
		ps := p.Stats()
		out.Delivery = append(out.Delivery, protocol.ReplicationDeliveryStats{
			Destination:          ps.Destination,
			State:                ps.State,
			Epoch:                ps.Epoch,
			Cursor:               ps.Cursor,
			LagSequences:         ps.LagSequences,
			Delivered:            ps.Delivered,
			Snapshots:            ps.Snapshots,
			Errors:               ps.Errors,
			LastSuccessTimestamp: ps.LastSuccessTimestamp,
			LastError:            ps.LastError,
		})
	}
	return out
}
