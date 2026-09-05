package contadores

import (
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/contador"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
)

// Compilador prepara o callback de abertura do storage (Options.CompilaContadores).
//
// Dentro do callback o schema é compilado — spec.Compile(dict) — no MESMO
// ponto em que hoje é compilado depois do Open, só que antes do replay da
// recovery, porque as definições precisam do schema e o replay precisa das
// definições. Compilado() devolve o resultado, e é ele que o chamador registra
// no motor de consulta: um segundo Compile daria um segundo *schema.Compiled
// com contadores de campo (descartes, divergências) separados dos que a
// ingestão alimenta.
//
// Todo chamador de storage.Open de um dataset BASE passa o callback, tenha o
// YAML contadores ou não — é o que faz um dataset que ganhou `counters:`
// abrir com eles pelo `nishidb ingest`, pelo `query --config` e pelo servidor
// igualmente. A tabela irmã (histórico) não passa: ela nunca tem contadores.
type Compilador struct {
	spec      *schema.DatasetSpec
	compilado *schema.Compiled
}

// NovoCompilador prepara o compilador para a spec.
func NovoCompilador(spec *schema.DatasetSpec) *Compilador {
	return &Compilador{spec: spec}
}

// Callback devolve o CompilaContadores para as Options do storage.
func (c *Compilador) Callback() func(*fielddict.Dict) ([]contador.Definicao, error) {
	return func(dict *fielddict.Dict) ([]contador.Definicao, error) {
		compilado, err := c.spec.Compile(dict)
		if err != nil {
			return nil, err
		}
		c.compilado = compilado
		return Compila(c.spec, compilado)
	}
}

// Compilado devolve o schema que o callback compilou; nil antes do Open.
func (c *Compilador) Compilado() *schema.Compiled { return c.compilado }
