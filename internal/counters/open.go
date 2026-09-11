package counters

import (
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
)

// Compiler prepara o callback de abertura do storage (Options.CompilaContadores).
//
// Dentro do callback o schema é compilado — spec.Compile(dict) — no MESMO
// ponto em que hoje é compilado depois do Open, só que antes do replay da
// recovery, porque as definições precisam do schema e o replay precisa das
// definições. Compiled() devolve o resultado, e é ele que o chamador registra
// no motor de consulta: um segundo Compile daria um segundo *schema.Compiled
// com contadores de campo (descartes, divergências) separados dos que a
// ingestão alimenta.
//
// Todo chamador de storage.Open de um dataset BASE passa o callback, tenha o
// YAML contadores ou não — é o que faz um dataset que ganhou `counters:`
// abrir com eles pelo `nishidb ingest`, pelo `query --config` e pelo servidor
// igualmente. A tabela irmã (histórico) não passa: ela nunca tem contadores.
type Compiler struct {
	spec     *schema.DatasetSpec
	compiled *schema.Compiled
}

// NewCompiler prepara o compilador para a spec.
func NewCompiler(spec *schema.DatasetSpec) *Compiler {
	return &Compiler{spec: spec}
}

// Callback devolve o CompilaContadores para as Options do storage.
func (c *Compiler) Callback() func(*fielddict.Dict) ([]counter.Definition, error) {
	return func(dict *fielddict.Dict) ([]counter.Definition, error) {
		compiled, err := c.spec.Compile(dict)
		if err != nil {
			return nil, err
		}
		c.compiled = compiled
		return Compile(c.spec, compiled)
	}
}

// Compiled devolve o schema que o callback compilou; nil antes do Open.
func (c *Compiler) Compiled() *schema.Compiled { return c.compiled }
