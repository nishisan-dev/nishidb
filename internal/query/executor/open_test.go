package executor_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O contrato com o storage deixando de ser papel: enquanto estas asserções
// compilarem, o Store real e o dublê são intercambiáveis em qualquer
// consumidor. Uma divergência de assinatura — ainda que só uint64 no lugar de
// types.RowID — torna os dois mutuamente inutilizáveis.
//
// A terceira asserção era `_ executor.HashLookuper = (*fakestorage.Store)(nil)`
// e cobria a capability OPCIONAL do índice hash, que o Store real não
// implementava e o dublê sim. Saiu porque o Lookup entrou na própria interface
// Store — as duas linhas acima já cobram dos DOIS o método que ela cobrava de
// um.
var (
	_ executor.Store = (*storage.Store)(nil)
	_ executor.Store = (*fakestorage.Store)(nil)
)

const yamlTeste = `
version: 1
name: t
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
    nome:
      type: string
    doc:
      type: json
`

func compilaSchema(t *testing.T) *schema.Compiled {
	t.Helper()
	spec, err := schema.Parse([]byte(yamlTeste), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// campo aceita testing.TB, e não *testing.T, porque as bancadas do top-N
// montam o mesmo schema que os testes: um helper por sabor de T duplicaria a
// resolução de campo, que é justamente onde uma divergência entre teste e
// benchmark passaria despercebida.
func campo(tb testing.TB, c *schema.Compiled, nome string) *schema.CompiledField {
	tb.Helper()
	f, ok := c.FieldByName([]byte(nome))
	if !ok {
		tb.Fatalf("campo %q", nome)
	}
	return f
}

// bqBase é uma query ligada mínima e VÁLIDA: os casos abaixo a corrompem em um
// ponto de cada vez.
func bqBase(t *testing.T, c *schema.Compiled) *binder.BoundQuery {
	t.Helper()
	id := campo(t, c, "id")
	return &binder.BoundQuery{
		Dataset:        "t",
		Schema:         c,
		Columns:        []binder.BoundColumn{{Field: id, Name: "id", Slot: 0}},
		Slots:          binder.Slots{IDs: []types.FieldID{id.ID}},
		MaxRows:        100,
		EffectiveLimit: 100,
		// O prazo faz parte do MÍNIMO válido: o binder sempre o copia do
		// schema compilado, e o Open recusa a query ligada sem ele em vez de
		// cair num default próprio. Enquanto esse default existia, esta base
		// podia esquecer o campo e nada acusava.
		Timeout: 30 * time.Second,
	}
}

// Plano ou query ligada inválidos são BUG do binder/planner, não erro do
// usuário — por isso saem em Open, antes de qualquer leitura, e com o
// sentinela que o cliente distingue de erro de dados.
func TestPlanoInvalido(t *testing.T) {
	c := compilaSchema(t)
	st := fakestorage.New(fakestorage.Indexes{})
	id, doc := campo(t, c, "id"), campo(t, c, "doc")

	casos := []struct {
		nome  string
		bq    func() *binder.BoundQuery
		plan  planner.Plan
		store executor.Store
		msg   string
	}{
		{"query ligada nula", func() *binder.BoundQuery { return nil }, planner.FullScan(), st, "query ligada nula"},
		{"plano nulo", func() *binder.BoundQuery { return bqBase(t, c) }, nil, st, "plano nulo"},
		{"store nulo", func() *binder.BoundQuery { return bqBase(t, c) }, planner.FullScan(), nil, "store nulo"},
		{"projeção vazia", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Columns = nil
			return bq
		}, planner.FullScan(), st, "projeção vazia"},
		{"coluna com campo nulo", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Columns[0].Field = nil
			return bq
		}, planner.FullScan(), st, "campo nulo"},
		// Prazo ausente é BUG do binder, e tem de sair aqui: o executor deixou
		// de ter um timeoutPadrao próprio, que era uma segunda fonte da verdade
		// capaz de vencer em silêncio o `query.maxTimeout` configurado.
		{"query ligada sem prazo", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Timeout = 0
			return bq
		}, planner.FullScan(), st, "sem prazo"},
		{"coluna com slot fora de faixa", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Columns[0].Slot = 7
			return bq
		}, planner.FullScan(), st, "fora de"},
		{"point lookup sem bytes na key", func() *binder.BoundQuery { return bqBase(t, c) },
			&planner.PointLookupPlan{Field: id, Key: types.Value{Type: types.TypeString}}, st, "key sem bytes"},
		// O caso "hash sem capability no store" saiu daqui, junto com o dublê
		// semHash que ele usava: os dois cobriam o ramo em que o executor
		// recebia um HashIndexPlan e o store NÃO implementava a capability
		// OPCIONAL HashLookuper. Com o Lookup dentro da interface Store, esse
		// store não compila — o erro deixou de ser alcançável em runtime.
		{"comparação sobre coluna json", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Slots = binder.Slots{IDs: []types.FieldID{id.ID, doc.ID}}
			bq.Where = &binder.BoundComparison{Field: doc, Slot: 1, Op: parser.CmpEq,
				Kind: binder.CompareBytes, Val: types.Value{Type: types.TypeJSON, Bytes: []byte("{}")}}
			return bq
		}, planner.FullScan(), st, "não é suportada"},
		{"modo de comparação não definido", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Where = &binder.BoundComparison{Field: id, Slot: 0, Op: parser.CmpEq,
				Val: types.Value{Type: types.TypeString, Bytes: []byte("x")}}
			return bq
		}, planner.FullScan(), st, "modo de comparação não definido"},
		{"comparação com campo nulo", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Where = &binder.BoundComparison{Slot: 0, Op: parser.CmpEq, Kind: binder.CompareBytes}
			return bq
		}, planner.FullScan(), st, "campo nulo"},
		{"IS NULL com campo nulo", func() *binder.BoundQuery {
			bq := bqBase(t, c)
			bq.Where = &binder.BoundNullCheck{Slot: 0}
			return bq
		}, planner.FullScan(), st, "campo nulo"},
	}

	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			it, err := executor.Open(context.Background(), tc.bq(), tc.plan,
				executor.Options{Store: tc.store})
			if err == nil {
				it.Close()
				t.Fatal("Open aceitou plano inválido")
			}
			if !errors.Is(err, executor.ErrInvalidPlan) {
				t.Errorf("erro = %v, want ErrInvalidPlan", err)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("mensagem = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// Open não lê NADA: a primeira row só é buscada no primeiro Next. É o que
// torna o LIMIT 0 gratuito e o streaming honesto.
func TestOpenNaoLeDados(t *testing.T) {
	c := compilaSchema(t)
	st := &storeQueExplode{t: t}

	it, err := executor.Open(context.Background(), bqBase(t, c), planner.FullScan(),
		executor.Options{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if st.leituras != 0 {
		t.Errorf("Open fez %d leituras, want 0", st.leituras)
	}
}

type storeQueExplode struct {
	t        *testing.T
	leituras int
}

func (s *storeQueExplode) GetByRowID(types.RowID, []byte) (types.Record, []byte, bool, error) {
	s.leituras++
	s.t.Fatal("Open leu uma row")
	return types.Record{}, nil, false, nil
}

func (s *storeQueExplode) GetByKey([]byte, []byte) (types.Record, []byte, bool, error) {
	s.leituras++
	s.t.Fatal("Open leu uma row por key")
	return types.Record{}, nil, false, nil
}

func (s *storeQueExplode) ScanLive() types.RowIDIterator { return vazio{} }

func (s *storeQueExplode) Lookup(types.FieldID, types.Value) (types.RowIDIterator, error) {
	return vazio{}, nil
}

type vazio struct{}

func (vazio) Next() bool         { return false }
func (vazio) RowID() types.RowID { return 0 }
func (vazio) Err() error         { return nil }
func (vazio) Close() error       { return nil }
