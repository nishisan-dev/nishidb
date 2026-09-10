package schema_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestDicionarioEmMemoriaDaIdsEstaveisEEnumera: os ids saem na ordem em que os
// nomes aparecem, começam em 1 (o zero é reservado) e o mesmo nome sempre
// resolve para o mesmo id — é disso que o Compile depende para dar ao schema
// sintético uma ordem determinística de FieldID.
func TestDicionarioEmMemoriaDaIdsEstaveisEEnumera(t *testing.T) {
	d := schema.NewInMemoryDict()
	first, err := d.LookupOrCreate([]byte("groupKey"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.LookupOrCreate([]byte("total"))
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("ids = %d e %d, quero 1 e 2", first, second)
	}
	if again, _ := d.LookupOrCreate([]byte("groupKey")); again != first {
		t.Fatalf("o mesmo nome deu id %d depois de %d", again, first)
	}
	if d.Len() != 2 {
		t.Fatalf("Len = %d, quero 2", d.Len())
	}
	if name, ok := d.Name(second); !ok || name != "total" {
		t.Fatalf("Name(%d) = %q, %v", second, name, ok)
	}
	if _, ok := d.Name(0); ok {
		t.Fatal("o id 0 é reservado e não nomeia coluna nenhuma")
	}
	if _, ok := d.Name(99); ok {
		t.Fatal("id além do fim não pode nomear")
	}
}

// TestDicionarioEmMemoriaRecusaFixarTipo: o tipo de cada coluna sintética está
// escrito no YAML da tabela virtual. Um hint que o sobrescrevesse seria a spec
// deixando de mandar.
func TestDicionarioEmMemoriaRecusaFixarTipo(t *testing.T) {
	d := schema.NewInMemoryDict()
	id, err := d.LookupOrCreate([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.TypeHint(id); ok {
		t.Fatal("dicionário em memória não guarda hint")
	}
	if err := d.SetTypeHint(id, types.TypeString); err == nil {
		t.Fatal("fixar tipo tem de ser recusado")
	}
}
