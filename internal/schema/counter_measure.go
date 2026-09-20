package schema

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// MaxCounterMeasures limita o estado temporal e as colunas por contador.
const MaxCounterMeasures = 8

// CounterMeasureSpec descreve uma taxa de eventos aceitos sobre uma janela.
type CounterMeasureSpec struct {
	Name   string
	Window time.Duration
	Unit   string
}

// UnitDuration é a duração exata da unidade validada.
func (m CounterMeasureSpec) UnitDuration() time.Duration { return measureUnit(m.Unit) }

// Scale converte a contagem da janela na unidade declarada.
func (m CounterMeasureSpec) Scale() float64 {
	return float64(measureUnit(m.Unit)) / float64(m.Window)
}

func measureUnit(unit string) time.Duration {
	switch unit {
	case "second":
		return time.Second
	case "minute":
		return time.Minute
	case "hour":
		return time.Hour
	case "day":
		return 24 * time.Hour
	default:
		return 0
	}
}

func measuresFromNode(n *yaml.Node, path string) ([]CounterMeasureSpec, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.SequenceNode || len(n.Content) == 0 || len(n.Content) > MaxCounterMeasures {
		return nil, specErrf("%s must contain 1 to %d measures", path, MaxCounterMeasures)
	}
	out := make([]CounterMeasureSpec, 0, len(n.Content))
	for i, item := range n.Content {
		p := fmt.Sprintf("%s[%d]", path, i)
		keys, err := block(item, p, "name", "window", "unit")
		if err != nil {
			return nil, err
		}
		var m CounterMeasureSpec
		var window string
		for k, v := range keys {
			var target *string
			switch k {
			case "name":
				target = &m.Name
			case "window":
				target = &window
			case "unit":
				target = &m.Unit
			}
			if err := decodeInto(v, p+"."+k, target); err != nil {
				return nil, err
			}
		}
		if !isMetricName(m.Name) {
			return nil, specErrf("%s.name %q invalid (expected [a-z_][a-z0-9_]*)", p, m.Name)
		}
		if m.Window, err = metricWindow(p, window); err != nil {
			return nil, err
		}
		if measureUnit(m.Unit) == 0 {
			return nil, specErrf("%s.unit %q invalid (expected second, minute, hour or day)", p, m.Unit)
		}
		out = append(out, m)
	}
	return out, nil
}

func validateMeasureSources(path string, c *CounterSpec) error {
	seen := make(map[string]bool)
	for i, m := range c.Measures {
		if seen[m.Name] {
			return specErrf("%s.measures[%d].name %q repeated", path, i, m.Name)
		}
		seen[m.Name] = true
		for _, col := range CounterColumns(c.GroupBy, c.Metrics) {
			if m.Name == col.Name {
				return specErrf("%s.measures[%d].name %q collides with %s", path, i, m.Name, col)
			}
		}
		for _, metric := range c.Metrics {
			if m.Name == metric.Name {
				return specErrf("%s.measures[%d].name %q collides with a metric", path, i, m.Name)
			}
		}
		for _, alarm := range c.Alarms {
			if m.Name == alarm.Name {
				return specErrf("%s.measures[%d].name %q collides with an alarm", path, i, m.Name)
			}
		}
	}
	if c.DisableTotal && len(c.Measures) == 0 {
		return specErrf("%s: total: false requires at least one measure", path)
	}
	for i := range c.Metrics {
		m := &c.Metrics[i]
		if m.Source == MetricSourceTotal && !c.DisableTotal {
			continue
		}
		for j := range c.Measures {
			if c.Measures[j].Name == m.Source {
				m.Rate = &c.Measures[j]
				break
			}
		}
		if m.Rate == nil {
			return specErrf("%s.metrics[%d].source %q unknown or disabled: not materialized by this counter", path, i, m.Source)
		}
	}
	return nil
}
