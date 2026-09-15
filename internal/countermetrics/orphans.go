package countermetrics

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// removeOrphans remove, antes da abertura, os arquivos de métricas que saíram
// do YAML e os diretórios de contadores que não declaram mais métrica, com
// WARN counter_metric.orphans nomeando o que saiu.
//
// Só apaga o que é deste pacote, e nada mais: arquivo REGULAR com a extensão
// .win (ou .win.tmp, a sobra de um crescimento interrompido do metricwindow)
// num diretório imediatamente abaixo de Dir, e o próprio diretório do contador
// órfão quando fica vazio. Link simbólico nunca é seguido nem apagado. Um
// diretório órfão com conteúdo alheio fica onde está, e o WARN o nomeia em
// "kept": apagar o que não criamos para limpar um diretório seria trocar
// telemetria órfã por dado de outra pessoa.
//
// Falha ao remover um arquivo nosso é de infra e recusa a abertura: o disco
// que não deixa apagar um arquivo também não vai deixar gravar a janela.
func (s *Sampler) removeOrphans(counters []CounterInput) error {
	declared := make(map[string]map[string]bool, len(counters))
	for _, c := range counters {
		metrics := make(map[string]bool, len(c.Spec.Metrics))
		for _, m := range c.Spec.Metrics {
			metrics[m.Name] = true
		}
		declared[c.Spec.Name] = metrics
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("countermetrics: dataset %s: %w", s.dataset, err)
	}
	var removed, kept []string
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		counter := ent.Name()
		metrics, isDeclared := declared[counter]
		dir := filepath.Join(s.dir, counter)
		files, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("countermetrics: dataset %s: %w", s.dataset, err)
		}
		for _, f := range files {
			metric, ours := metricOfFileName(f.Name())
			if !ours || !f.Type().IsRegular() || metrics[metric] {
				continue
			}
			if err := os.Remove(filepath.Join(dir, f.Name())); err != nil {
				return fmt.Errorf("countermetrics: dataset %s: remove orphan: %w", s.dataset, err)
			}
			removed = append(removed, filepath.Join(counter, f.Name()))
		}
		if isDeclared {
			continue
		}
		switch err := os.Remove(dir); {
		case err == nil:
			removed = append(removed, counter+string(filepath.Separator))
		case errors.Is(err, syscall.ENOTEMPTY), errors.Is(err, syscall.EEXIST):
			kept = append(kept, counter+string(filepath.Separator))
		default:
			return fmt.Errorf("countermetrics: dataset %s: remove orphan: %w", s.dataset, err)
		}
	}
	if len(removed) > 0 || len(kept) > 0 {
		s.log.Warn("counter_metric.orphans",
			"window metric files of metrics or counters no longer declared were removed; directories with foreign content were kept",
			"dataset", s.dataset, "removed", removed, "kept", kept)
	}
	return nil
}

// metricOfFileName devolve o nome da métrica de um arquivo deste pacote
// (<metric>.win ou <metric>.win.tmp), e false para qualquer outro nome.
func metricOfFileName(name string) (string, bool) {
	metric, ok := strings.CutSuffix(strings.TrimSuffix(name, tmpSuffix), fileExtension)
	return metric, ok && metric != ""
}
