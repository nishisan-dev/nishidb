package protocol

import "encoding/json"

func enabledTotal(disabled bool, value *int64) *int64 {
	if disabled {
		return nil
	}
	return value
}

// MarshalJSON mantém total presente e nulo quando sua materialização foi desativada.
func (c CounterStats) MarshalJSON() ([]byte, error) {
	type wire CounterStats
	var total *int64
	if !c.TotalDisabled {
		total = &c.Total
	}
	return json.Marshal(struct {
		wire
		Total *int64 `json:"total"`
	}{wire(c), total})
}

// UnmarshalJSON preserva a ausência de total no cliente administrativo.
func (c *CounterStats) UnmarshalJSON(data []byte) error {
	type wire CounterStats
	v := struct {
		*wire
		Total *int64 `json:"total"`
	}{wire: (*wire)(c)}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	c.TotalDisabled = v.Total == nil
	c.Total = 0
	if v.Total != nil {
		c.Total = *v.Total
	}
	return nil
}

// MarshalJSON distingue a auditoria dos refs de totais não materializados.
func (c CounterAudit) MarshalJSON() ([]byte, error) {
	type wire CounterAudit
	return json.Marshal(struct {
		wire
		Maintained     *int64 `json:"maintained"`
		Recomputed     *int64 `json:"recomputed"`
		AggregateTotal *int64 `json:"aggregateTotal"`
	}{wire(c), enabledTotal(c.TotalDisabled, &c.Maintained), enabledTotal(c.TotalDisabled, &c.Recomputed), enabledTotal(c.TotalDisabled, &c.AggregateTotal)})
}

// UnmarshalJSON preserva o total desativado na resposta administrativa.
func (c *CounterAudit) UnmarshalJSON(data []byte) error {
	type wire CounterAudit
	v := struct {
		*wire
		Maintained     *int64 `json:"maintained"`
		Recomputed     *int64 `json:"recomputed"`
		AggregateTotal *int64 `json:"aggregateTotal"`
	}{wire: (*wire)(c)}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	c.TotalDisabled = v.Maintained == nil
	for i := range c.Sample {
		c.Sample[i].TotalDisabled = c.TotalDisabled
	}
	c.Maintained, c.Recomputed, c.AggregateTotal = 0, 0, 0
	if v.Maintained != nil {
		c.Maintained = *v.Maintained
	}
	if v.Recomputed != nil {
		c.Recomputed = *v.Recomputed
	}
	if v.AggregateTotal != nil {
		c.AggregateTotal = *v.AggregateTotal
	}
	return nil
}

// MarshalJSON mantém refs auditáveis mesmo quando não há total.
func (g GroupDivergence) MarshalJSON() ([]byte, error) {
	type wire GroupDivergence
	return json.Marshal(struct {
		wire
		Maintained *int64 `json:"maintained"`
		Recomputed *int64 `json:"recomputed"`
	}{wire(g), enabledTotal(g.TotalDisabled, &g.Maintained), enabledTotal(g.TotalDisabled, &g.Recomputed)})
}
