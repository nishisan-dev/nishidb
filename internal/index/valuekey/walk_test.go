package valuekey

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestPercorreCompostaVisitaEmOrdemSemAlocar(t *testing.T) {
	chave, err := AppendComposta(nil, []types.Value{str("A"), i64(1), booleano(true)})
	if err != nil {
		t.Fatal(err)
	}
	var vistos []types.Value
	if err := PercorreComposta(chave, func(i int, v types.Value) error {
		if i != len(vistos) {
			t.Fatalf("índice %d fora de ordem", i)
		}
		vistos = append(vistos, v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(vistos) != 3 || string(vistos[0].Bytes) != "A" || vistos[1].I64 != 1 || vistos[2].Type != types.TypeBool {
		t.Fatalf("vistos = %+v", vistos)
	}
	// Erro do visitante interrompe e sobe.
	sentinela := errors.New("pare")
	if err := PercorreComposta(chave, func(int, types.Value) error { return sentinela }); !errors.Is(err, sentinela) {
		t.Fatalf("erro do visitante tem de subir: %v", err)
	}
	if err := PercorreComposta(nil, func(int, types.Value) error { return nil }); err == nil {
		t.Fatal("chave vazia tem de ser recusada")
	}
}
