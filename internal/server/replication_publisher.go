package server

import (
	"context"
	"errors"
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
	"github.com/nishisan-dev/nishidb/internal/storage"
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
// Recusas, cada uma com a própria mensagem:
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
	alarmTargets := make(map[string]bool)
	counterTargets := make(map[string]counterUse)
	for _, name := range names {
		for _, c := range specs[name].Counters {
			for _, alarm := range c.Alarms {
				for _, target := range alarm.Notify {
					alarmTargets[target] = true
				}
			}
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
		// Alarm delivery has its own configuration block and envelope.
		for _, alarm := range cfg.AlarmDelivery.Kafka {
			if alarmTargets[alarm.Name] && alarm.Topic == spec.Topic && sameCluster(spec.BootstrapServers, alarm.BootstrapServers) {
				return fmt.Errorf("%w: dataset %q replication target %q writes to topic %q on the same cluster as alarm Kafka target %q; a topic carries a single envelope format",
					config.ErrKafkaTarget, r.dataset, r.target, spec.Topic, alarm.Name)
			}
		}
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
		// Só a histórica PUBLICADA entra: com `publishHistory: false` ela
		// fica fora do snapshot do publicador, como já fica fora do tail.
		if h := d.publishedHistory(); h != nil {
			tables = append(tables, replication.SnapshotTable{Name: h.nome, History: true, Source: h.st})
			historyName = h.nome
			// O Compiled PRÓPRIO da irmã (openHistory), e não o do base: é
			// ele que resolve as colunas da histórica pelo FieldID.
			history = h.sch
		}
		renderHash := replicationkafka.RenderHash(renderSpecOf(d))
		checkpointDir := replicationkafka.CheckpointDir(s.cfg.DatasetRootDir(d.nome))
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
				Dataset:          d.nome,
				Target:           target,
				Feed:             d.replication,
				Tables:           tables,
				Renderer:         renderer,
				Logger:           log,
				Now:              s.now,
				CheckpointDir:    checkpointDir,
				RenderHash:       renderHash,
				PeriodicSnapshot: d.spec.Replication.PeriodicSnapshot,
			})
			if err != nil {
				return fmt.Errorf("dataset %q: replication target %q: %w", d.nome, name, err)
			}
			d.publishers = append(d.publishers, p)
		}
	}
	return nil
}

// renderSpecOf é a renderização DECLARADA do dataset, que o publicador
// compara com a do .ckpt na subida: as tabelas publicadas (o base e, só
// quando publicada, a histórica), publishHistory e os aliases declarados em
// `schema.fields` de cada tabela publicada. Sai só da spec, e nunca do
// Compiled nem do dicionário de campos: as colunas descobertas em runtime
// (`include: ["*"]`) não mudam o rótulo de coluna nenhuma que já exista, e
// contá-las daria config_changed na primeira subida depois de uma descoberta.
func renderSpecOf(d *dataset) replicationkafka.RenderSpec {
	spec := replicationkafka.RenderSpec{Tables: []string{d.nome}, Aliases: declaredAliases(d.nome, d.spec)}
	if h := d.publishedHistory(); h != nil {
		spec.Tables = append(spec.Tables, h.nome)
		spec.Aliases = append(spec.Aliases, declaredAliases(h.nome, h.spec)...)
		spec.PublishHistory = true
	}
	return spec
}

// declaredAliases lista os aliases de `schema.fields` da spec, ordenados
// pelo nome do campo.
func declaredAliases(table string, spec *schema.DatasetSpec) []replicationkafka.FieldAlias {
	var out []replicationkafka.FieldAlias
	for field, f := range spec.Schema.Fields {
		if f.Alias != "" {
			out = append(out, replicationkafka.FieldAlias{Table: table, Field: field, Alias: f.Alias})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	return out
}

// startPublishers inicia os publicadores de um dataset. Vem DEPOIS do Start
// do feed, no Run: antes dele o feed não fecha lote por intervalo.
func startPublishers(d *dataset) {
	for _, p := range d.publishers {
		p.Start()
	}
}

// closeFeeds fecha os feeds de replicação com a impressão dos Stores e a marca
// limpa: a próxima subida, com a impressão igual, retoma a MESMA epoch e
// sequence, e nenhum consumidor refaz o snapshot. No encerra() vem DEPOIS do
// http.Shutdown, do stopStoreProducers e do checkpointStores, porque só aí a
// posição de escrita está estável: uma mudança publicada depois da leitura da
// impressão a desmentiria, e uma depois do Close derruba a marca limpa (o feed
// emite o ERROR replication.feed.publish_after_close). Na subida abortada vem
// direto, porque nada mutou os Stores desde o Resume.
//
// O Close faz o flush do intervalo ativo e acorda os leitores: os
// publicadores drenam a cauda em seguida (closePublishers). Sem a marca limpa
// — posição de um Store publicado ilegível (o writer falho do checkpoint que
// não fez o fsync), gravador além do prazo ou sync do feed falho — a próxima
// subida abre uma epoch nova, e os consumidores refazem o snapshot; o WARN
// diz qual dos casos.
//
// Idempotente, como o Close do feed: num segundo encerramento os Stores já
// estão fechados, o Close devolve o desfecho do primeiro, e nada é avisado
// de novo.
func (s *Server) closeFeeds() {
	for _, d := range s.datasets {
		if d.replication == nil {
			continue
		}
		var fp *replication.Fingerprint
		f, cause := s.feedFingerprint(d)
		switch {
		case errors.Is(cause, storage.ErrDatasetClosed):
			_ = d.replication.Close(nil, 0)
			continue
		case cause == nil:
			fp = &f
		}
		if err := d.replication.Close(fp, feedCloseBudget); err != nil {
			s.log.Warn("replication.feed.close_failed",
				"the replication feed did not close cleanly (writer past the deadline, disk sync or state write failure, or a change that reached it after the close); the next start opens a new generation",
				"dataset", d.nome, "error", err.Error())
			continue
		}
		if fp == nil {
			s.log.Warn("replication.feed.closed_unclean",
				"the replication feed closed without its clean mark because the dataset position is not trustworthy; the next start opens a new generation",
				"dataset", d.nome, "error", cause.Error())
		}
	}
}

// feedCloseBudget é o prazo para o gravador do feed drenar a fila no Close.
const feedCloseBudget = 10 * time.Second

// waitFeeds é o fim da vida dos feeds: junta as goroutines e fecha o log de
// segmentos. Vem DEPOIS do closePublishers, que ainda lê do disco a cauda que
// os publicadores não receberam.
func (s *Server) waitFeeds() {
	for _, d := range s.datasets {
		if d.replication != nil {
			d.replication.Wait()
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
// e não erro do encerramento: nada do dataset se perde — o publicador grava
// no .ckpt o último ponto confirmado ao parar, e a próxima subida retoma dali
// e entrega de novo o que faltou (ou publica `reset` com snapshot_failed, se
// um snapshot estava em curso) —, e um broker fora no instante do SIGTERM não
// é falha do processo.
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
						"the replication publisher did not drain the feed tail before the deadline; the next start resumes from the destination checkpoint and delivers the rest again",
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
