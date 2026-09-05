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
	d := schema.NovoDicionarioEmMemoria()
	primeiro, err := d.LookupOrCreate([]byte("groupKey"))
	if err != nil {
		t.Fatal(err)
	}
	segundo, err := d.LookupOrCreate([]byte("total"))
	if err != nil {
		t.Fatal(err)
	}
	if primeiro != 1 || segundo != 2 {
		t.Fatalf("ids = %d e %d, quero 1 e 2", primeiro, segundo)
	}
	if deNovo, _ := d.LookupOrCreate([]byte("groupKey")); deNovo != primeiro {
		t.Fatalf("o mesmo nome deu id %d depois de %d", deNovo, primeiro)
	}
	if d.Len() != 2 {
		t.Fatalf("Len = %d, quero 2", d.Len())
	}
	if nome, ok := d.Name(segundo); !ok || nome != "total" {
		t.Fatalf("Name(%d) = %q, %v", segundo, nome, ok)
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
	d := schema.NovoDicionarioEmMemoria()
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
