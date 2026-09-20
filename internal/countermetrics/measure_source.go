package countermetrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

type measureReader interface {
	MeasureCount(uint32, int, int64) int64
}

// reconcileSource preserva o formato inteiro dos baldes e identifica a grandeza
// amostrada por uma descrição exata. Remover o .win antes de publicar a nova
// descrição impede adotar dados antigos após crash entre as duas operações.
func (s *Sampler) reconcileSource(path string, c *counterState, spec schema.CounterMetricSpec) (bool, error) {
	var expected []byte
	if spec.Rate != nil {
		var err error
		expected, err = json.Marshal(struct {
			Rate    *schema.CounterMeasureSpec
			GroupBy []string
			Where   string
		}{spec.Rate, c.groupBy, c.where})
		if err != nil {
			return false, err
		}
	}
	old, err := os.ReadFile(path + ".source")
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if bytes.Equal(old, expected) {
		return true, nil
	}
	if s.readOnly {
		return false, nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := atomicfile.SyncDir(filepath.Dir(path)); err != nil {
		return false, err
	}
	if len(expected) > 0 {
		if err := atomicfile.Write(path+".source", expected, 0o644); err != nil {
			return false, err
		}
	} else if err := os.Remove(path + ".source"); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	s.log.Warn("counter_metric.source_changed", "window metric source changed; history was reset", "counter", c.name, "metric", spec.Name)
	return true, nil
}

func bindMeasure(c *counterState, spec schema.CounterMetricSpec) (int, error) {
	if spec.Rate == nil {
		return -1, nil
	}
	reader, ok := c.reader.(measureReader)
	if !ok {
		return -1, fmt.Errorf("counter %s: measure reader required", c.name)
	}
	c.measureReader = reader
	for i, m := range c.measures {
		if m.Name == spec.Source {
			return i, nil
		}
	}
	return -1, fmt.Errorf("counter %s: measure source %q not found", c.name, spec.Source)
}
