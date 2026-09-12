package alarm

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// grupo é um grupo do contador falso: a chave canônica, o texto e o total.
type grupo struct {
	olt   string
	slot  int64
	total int64
	live  bool
}

// fakeCounter satisfaz o Reader que a tabela virtual do contador pede, com um
// conjunto de grupos fixo. Um contador de verdade exigiria diretório, mmap e
// checkpoint para exercitar o que é lógica de varredura e de estado.
//
// O texto de cada grupo é RENDERIZADO pelo molde da definição, com a mesma
// função que o contador real usa. Não é detalhe: um fake que devolvesse o texto
// montado à mão como "olt|slot" ignoraria o keyFormat, e o teste que troca o
// molde para provar que o estado sobrevive passaria sem exercitar nada.
type fakeCounter struct {
	groups []grupo // índice i = id i+1
	keys   [][]byte
	texts  [][]byte
}

func newFakeCounter(t *testing.T, def counter.Definition, gs ...grupo) *fakeCounter {
	t.Helper()
	f := &fakeCounter{groups: gs}
	for _, g := range gs {
		key, err := valuekey.AppendComposite(nil, []types.Value{
			{Type: types.TypeString, Bytes: []byte(g.olt)},
			{Type: types.TypeInt64, I64: g.slot},
		})
		if err != nil {
			t.Fatal(err)
		}
		text, _, err := counter.KeyText(nil, def.Format, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		f.keys = append(f.keys, key)
		f.texts = append(f.texts, text)
	}
	return f
}

func (f *fakeCounter) Limit() uint32 { return uint32(len(f.groups)) + 1 }

func (f *fakeCounter) Group(id uint32) ([]byte, []byte, int64, int64, bool) {
	i := int(id) - 1
	if i < 0 || i >= len(f.groups) || !f.groups[i].live {
		return nil, nil, 0, 0, false
	}
	g := f.groups[i]
	return f.keys[i], f.texts[i], g.total, g.total, true
}

func (f *fakeCounter) ByText(text []byte) (uint32, bool) {
	for i, tx := range f.texts {
		if string(tx) == string(text) && f.groups[i].live {
			return uint32(i + 1), true
		}
	}
	return 0, false
}

// setTotal muda o total de um grupo entre duas passadas, que é o que faz a
// condição passar a valer ou deixar de valer.
func (f *fakeCounter) setTotal(olt string, slot, total int64) {
	for i, g := range f.groups {
		if g.olt == olt && g.slot == slot {
			f.groups[i].total = total
			return
		}
	}
}

// retire tira o grupo do universo do contador, como a aposentadoria faz.
func (f *fakeCounter) retire(olt string, slot int64) {
	for i, g := range f.groups {
		if g.olt == olt && g.slot == slot {
			f.groups[i].live = false
			return
		}
	}
}

// sinkOfTest guarda os eventos emitidos, na ordem.
type sinkOfTest struct{ events []Event }

func (s *sinkOfTest) Emit(e Event) { s.events = append(s.events, e) }

func (s *sinkOfTest) last() Event {
	if len(s.events) == 0 {
		return Event{}
	}
	return s.events[len(s.events)-1]
}

// clockOfTest é o relógio que o teste avança sem dormir, no molde do
// relogioOfTest do storage: um teste de prazo precisa avançar o tempo sem
// dormir, e a alternativa seria uma suíte que leva minutos.
type clockOfTest struct{ now time.Time }

func (c *clockOfTest) Now() time.Time          { return c.now }
func (c *clockOfTest) advance(d time.Duration) { c.now = c.now.Add(d) }

// moldeDe compila um keyFormat para a definição dada, pelo mesmo parser que o
// contador usa.
func moldeDe(t *testing.T, mold string, def counter.Definition) *keyformat.Format {
	t.Helper()
	f, err := keyformat.Parse(mold, keyformat.Columns(def.Names, def.Types))
	if err != nil {
		t.Fatal(err)
	}
	return f
}
