package valuekey

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestPercorreCompostaVisitaEmOrdemSemAlocar(t *testing.T) {
	key, err := AppendComposite(nil, []types.Value{str("A"), i64(1), boolean(true)})
	if err != nil {
		t.Fatal(err)
	}
	var visited []types.Value
	if err := WalkComposite(key, func(i int, v types.Value) error {
		if i != len(visited) {
			t.Fatalf("índice %d fora de ordem", i)
		}
		visited = append(visited, v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 3 || string(visited[0].Bytes) != "A" || visited[1].I64 != 1 || visited[2].Type != types.TypeBool {
		t.Fatalf("visited = %+v", visited)
	}
	// Erro do visitante interrompe e sobe.
	sentinel := errors.New("pare")
	if err := WalkComposite(key, func(int, types.Value) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("erro do visitante tem de subir: %v", err)
	}
	if err := WalkComposite(nil, func(int, types.Value) error { return nil }); err == nil {
		t.Fatal("chave vazia tem de ser recusada")
	}
}
