package query_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// BenchmarkExecuteFromQualified mede o que a forma QUALIFICADA do FROM custa a
// mais que a simples, em função do número de campos do dataset.
//
// Ela existe porque o custo da resolução é linear no que o Catalog monta, e o
// que ele monta cresce com o dicionário: a primeira versão pedia o catálogo
// COMPLETO — colunas inclusive — só para ler `(Schema, Name, Namespace)`, e as
// colunas saem de FieldsForMetadata, que aloca um slice de todos os campos e o
// ORDENA. Nesta bancada, com 4.000 campos, a forma qualificada custava 479 µs
// contra 4,2 µs da simples — 113x —, e ela é justamente a que o DBeaver gera
// sozinho a partir do catálogo: o caminho quente do cliente-alvo.
//
// Com a enumeração separada da montagem de colunas, a razão entre as duas
// formas ficou em ~1,6x nos três tamanhos, e o custo da qualificada parou de
// crescer com o dicionário (5,7 µs e 2.216 B/op com 4.000 campos).
//
// O tamanho do dicionário não é hipótese: o teto do fielddict é 65.535 campos e
// o piloto roda com `unknownFields: include`, de modo que o universo de colunas
// cresce com os eventos.
//
// As duas formas correm contra o MESMO dataset e o mesmo SQL, mudando só o
// qualificador: a diferença entre as duas linhas é o roteamento, e nada mais.
func BenchmarkExecuteFromQualified(b *testing.B) {
	for _, fields := range []int{40, 400, 4000} {
		eng, name, schemaName := engineOfWideDataset(b, fields)
		simple := "SELECT id FROM " + name
		qualified := "SELECT id FROM " + schemaName + "." + name

		b.Run(fmt.Sprintf("fields=%d/simple", fields), func(b *testing.B) {
			benchExecute(b, eng, name, simple)
		})
		b.Run(fmt.Sprintf("fields=%d/qualified", fields), func(b *testing.B) {
			benchExecute(b, eng, name, qualified)
		})
	}
}

// benchExecute mede o Execute até o iterador — parse, roteamento, bind e plano
// —, e não a varredura: é o roteamento que a forma qualificada muda.
func benchExecute(b *testing.B, eng *query.Engine, handle, sql string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := eng.Execute(context.Background(), handle, sql, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// engineOfWideDataset monta um motor com UM dataset de `fields` colunas
// declaradas, mais um contador — a tabela virtual é o que faz o catálogo ter
// mais de uma linha a montar por dataset.
func engineOfWideDataset(tb testing.TB, fields int) (*query.Engine, string, string) {
	tb.Helper()
	const name, schemaName = "wide", "largo"

	var sb strings.Builder
	sb.WriteString("version: 1\nname: " + name + "\nschemaName: " + schemaName + "\n")
	sb.WriteString("materialization:\n  key:\n    field: id\nschema:\n  include: [id")
	for i := 0; i < fields; i++ {
		fmt.Fprintf(&sb, ", f%d", i)
	}
	sb.WriteString("]\n  fields:\n    id:\n      type: string\n")
	for i := 0; i < fields; i++ {
		fmt.Fprintf(&sb, "    f%d:\n      type: string\n", i)
	}

	spec, err := schema.Parse([]byte(sb.String()), schema.BuiltinDefaults)
	if err != nil {
		tb.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register(name, sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		tb.Fatal(err)
	}
	registerVirtualOfWideDataset(tb, eng, name)
	return eng, name, schemaName
}

// registerVirtualOfWideDataset pendura um contador sintético no dataset, pelo mesmo
// RegisterVirtual que o pacote counters usa.
func registerVirtualOfWideDataset(tb testing.TB, eng *query.Engine, host string) {
	tb.Helper()
	spec, err := schema.Parse([]byte(`version: 1
name: por_regiao
schemaName: `+schema.SyntheticSchemaName+`
materialization:
  key:
    field: groupKey
schema:
  include: [groupKey, total]
  fields:
    groupKey:
      type: string
    total:
      type: int64
`), schema.BuiltinDefaults)
	if err != nil {
		tb.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	if err := eng.RegisterVirtual(host, schema.CountersNamespace, "por_regiao", sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		tb.Fatal(err)
	}
}
