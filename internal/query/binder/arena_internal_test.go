package binder

import (
	"testing"
	"unsafe"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const arenaYAML = `
version: 1
name: ds
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    ip1:
      type: ipv4
    ip2:
      type: ipv4
    v6:
      type: ipv6
    rede:
      type: ipv4_prefix
`

// Todos os valores parseados precisam viver no MESMO array: o do BoundQuery.
//
// Em Go, emitir a janela durante os appends não corromperia o valor — o array
// antigo continua vivo e imutável, então o teste de conteúdo passa de
// qualquer jeito. O que se perde sem a disciplina de "offsets primeiro,
// janelas depois" é MEMÓRIA: cada realocação deixa um array intermediário
// preso por uma janela, e o BoundQuery passa a reter N arrays em vez de um.
// Este teste é o único capaz de ver isso.
func TestJanelasVivemNoArenaDoBoundQuery(t *testing.T) {
	spec, err := schema.Parse([]byte(arenaYAML), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	// Muitos valores parseados, para forçar o arena a crescer várias vezes a
	// partir do nil.
	st, err := parser.Parse("SELECT id FROM ds WHERE ip1 = '10.0.0.1' AND v6 = '2001:db8::1' " +
		"AND rede = '192.168.0.0/16' AND ip2 = '172.16.0.1' AND ip1 != '10.0.0.2' " +
		"AND v6 != '2001:db8::2' AND ip2 != '172.16.0.2'")
	if err != nil {
		t.Fatal(err)
	}
	bq, err := Bind(st, sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bq.arena) == 0 {
		t.Fatal("arena vazio: nenhum valor foi parseado")
	}

	base := uintptr(unsafe.Pointer(unsafe.SliceData(bq.arena)))
	fim := base + uintptr(cap(bq.arena))

	var fora int
	var walk func(BoundExpr)
	walk = func(e BoundExpr) {
		switch n := e.(type) {
		case *BoundLogical:
			walk(n.Left)
			walk(n.Right)
		case *BoundComparison:
			if len(n.Val.Bytes) == 0 || n.Val.Type == types.TypeString {
				return // string é cópia própria, não vive no arena
			}
			p := uintptr(unsafe.Pointer(unsafe.SliceData(n.Val.Bytes)))
			if p < base || p >= fim {
				fora++
				t.Errorf("valor de %q vive fora do arena do BoundQuery: cada realocação "+
					"intermediária ficaria retida por uma janela", n.Field.Name)
			}
			// A janela tem cap limitado: um append futuro no arena não pode
			// escrever dentro dela.
			if cap(n.Val.Bytes) != len(n.Val.Bytes) {
				t.Errorf("janela de %q com cap %d > len %d", n.Field.Name, cap(n.Val.Bytes), len(n.Val.Bytes))
			}
		}
	}
	walk(bq.Where)
	if fora > 0 {
		t.Errorf("%d valores fora do arena", fora)
	}
}
