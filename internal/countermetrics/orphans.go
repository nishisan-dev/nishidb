package countermetrics

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// RemoveOrphans remove de dir os arquivos de métricas de janela e os
// diretórios de contadores que não estão em declared (contador → métricas
// declaradas), com as MESMAS garantias da limpeza do Open (ver
// removeOrphanFiles).
//
// É a limpeza de quem NÃO abre amostrador: o servidor a roda quando nenhum
// contador do dataset declara métrica, e é ela que faz sumir o que a última
// métrica deixou ao sair do YAML — sem ela esses arquivos ficariam no disco
// para sempre, porque o Open, que limpa, nunca mais roda. Com métrica
// declarada, quem limpa é o Open. Quem só lê (a CLI local) nunca chama isto.
//
// dir inexistente não tem o que limpar: devolve nil sem criá-lo. dir que é
// link simbólico não é seguido, pela mesma regra que protege os diretórios de
// contador. log nil vira Discard.
func RemoveOrphans(dataset, dir string, declared map[string][]string, log *logging.Logger) error {
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("countermetrics: dataset %s: %w", dataset, err)
	case info.Mode()&fs.ModeSymlink != 0:
		return nil
	case !info.IsDir():
		return fmt.Errorf("countermetrics: dataset %s: %s is not a directory", dataset, dir)
	}
	if log == nil {
		log = logging.Discard()
	}
	sets := make(map[string]map[string]bool, len(declared))
	for counter, metrics := range declared {
		set := make(map[string]bool, len(metrics))
		for _, m := range metrics {
			set[m] = true
		}
		sets[counter] = set
	}
	return removeOrphanFiles(dataset, dir, sets, log)
}

// removeOrphans é a limpeza do Open, sobre os contadores passados a ele. O
// Open já criou s.dir, então o diretório existe.
func (s *Sampler) removeOrphans(counters []CounterInput) error {
	declared := make(map[string]map[string]bool, len(counters))
	for _, c := range counters {
		metrics := make(map[string]bool, len(c.Spec.Metrics))
		for _, m := range c.Spec.Metrics {
			metrics[m.Name] = true
		}
		declared[c.Spec.Name] = metrics
	}
	return removeOrphanFiles(s.dataset, s.dir, declared, s.log)
}

// removeOrphanFiles remove os arquivos de métricas que saíram do YAML e os
// diretórios de contadores que não declaram mais métrica, com WARN
// counter_metric.orphans nomeando o que saiu.
//
// Só apaga o que é deste pacote, e nada mais: arquivo REGULAR com a extensão
// .win (ou .win.tmp, a sobra de um crescimento interrompido do metricwindow)
// num diretório imediatamente abaixo de dir, e o próprio diretório do contador
// órfão quando fica vazio. Link simbólico nunca é seguido nem apagado. Um
// diretório órfão com conteúdo alheio fica onde está, e o WARN o nomeia em
// "kept": apagar o que não criamos para limpar um diretório seria trocar
// telemetria órfã por dado de outra pessoa.
//
// Falha ao remover um arquivo nosso é de infra e recusa a abertura: o disco
// que não deixa apagar um arquivo também não vai deixar gravar a janela.
func removeOrphanFiles(dataset, dir string, declared map[string]map[string]bool, log *logging.Logger) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("countermetrics: dataset %s: %w", dataset, err)
	}
	var removed, kept []string
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		counter := ent.Name()
		metrics, isDeclared := declared[counter]
		counterDir := filepath.Join(dir, counter)
		files, err := os.ReadDir(counterDir)
		if err != nil {
			return fmt.Errorf("countermetrics: dataset %s: %w", dataset, err)
		}
		for _, f := range files {
			metric, ours := metricOfFileName(f.Name())
			if !ours || !f.Type().IsRegular() || metrics[metric] {
				continue
			}
			if err := os.Remove(filepath.Join(counterDir, f.Name())); err != nil {
				return fmt.Errorf("countermetrics: dataset %s: remove orphan: %w", dataset, err)
			}
			removed = append(removed, filepath.Join(counter, f.Name()))
		}
		if isDeclared {
			continue
		}
		switch err := os.Remove(counterDir); {
		case err == nil:
			removed = append(removed, counter+string(filepath.Separator))
		case errors.Is(err, syscall.ENOTEMPTY), errors.Is(err, syscall.EEXIST):
			kept = append(kept, counter+string(filepath.Separator))
		default:
			return fmt.Errorf("countermetrics: dataset %s: remove orphan: %w", dataset, err)
		}
	}
	if len(removed) > 0 || len(kept) > 0 {
		log.Warn("counter_metric.orphans",
			"window metric files of metrics or counters no longer declared were removed; directories with foreign content were kept",
			"dataset", dataset, "removed", removed, "kept", kept)
	}
	return nil
}

// metricOfFileName devolve o nome da métrica de um arquivo deste pacote
// (<metric>.win ou <metric>.win.tmp), e false para qualquer outro nome.
func metricOfFileName(name string) (string, bool) {
	metric, ok := strings.CutSuffix(strings.TrimSuffix(name, tmpSuffix), fileExtension)
	return metric, ok && metric != ""
}
