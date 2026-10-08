package counter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// defBySlot é um contador de uma coluna int64 — a forma mais curta de exercitar
// a guarda de colisão.
func defBySlot(name string) Definition {
	d := Definition{
		Name:      name,
		Columns:   []types.FieldID{cSlot},
		Names:     []string{"slot"},
		Types:     []types.FieldType{types.TypeInt64},
		MaxGroups: 100,
	}
	d.Format = keyformat.Default(keyformat.Columns(d.Names, d.Types))
	return d
}

// internRaw põe uma chave no dicionário SEM passar pela guarda, como o
// Transition faz depois de conferir.
func internRaw(t *testing.T, c *Counter, v types.Value) uint32 {
	t.Helper()
	key, err := valuekey.AppendComposite(nil, []types.Value{v})
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.dict.Intern(key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Duas chaves DISTINTAS que rendem o mesmo texto: a string "1" e o inteiro 1.
// É o que um renderizador com defeito produziria, e a única forma de alcançar a
// guarda — com as recusas de keyformat.Parse no lugar, molde válido não colide,
// e o Derive nunca monta uma chave com tipo diferente do declarado.
func chaveDeTipoTrocado() types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte("1")}
}

// TestAberturaRecusaDoisGruposComOMesmoTexto: se duas chaves distintas rendem o
// mesmo groupKey, o point lookup deixaria de identificar UM grupo. A abertura
// recusa em vez de deixar o segundo sobrescrever o primeiro no mapa — um
// sobrescrito calado responderia errado para sempre.
func TestAberturaRecusaDoisGruposComOMesmoTexto(t *testing.T) {
	dir := t.TempDir()
	c := openCounter(t, dir, defBySlot("colisao"), false)
	internRaw(t, c, chaveDeTipoTrocado())
	internRaw(t, c, types.Value{Type: types.TypeInt64, I64: 1})

	err := c.catchUpWithDict()
	if err == nil {
		t.Fatal("dois grupos com o mesmo groupKey têm de ser recusados")
	}
	for _, esperado := range []string{`groupKey "1"`, "{slot}", "keyFormat"} {
		if !strings.Contains(err.Error(), esperado) {
			t.Errorf("a mensagem não diz %q — o operador precisa do texto, do molde e do remédio: %v", esperado, err)
		}
	}
}

// TestColisaoRecusaAntesDeAChaveIrAoDisco é a invariante que separa um erro de
// um dataset perdido: o Intern grava a chave no arquivo, o próximo checkpoint a
// torna durável, e id durável nunca é reemitido. Uma chave colidente que
// chegasse ao disco
// faria TODA abertura seguinte falhar na mesma colisão, sem remédio que não
// fosse apagar o contador.
func TestColisaoRecusaAntesDeAChaveIrAoDisco(t *testing.T) {
	dir := t.TempDir()
	c := openCounter(t, dir, defBySlot("colisao"), false)
	internRaw(t, c, chaveDeTipoTrocado())
	if err := c.catchUpWithDict(); err != nil {
		t.Fatal(err)
	}
	dictPath := filepath.Join(dir, dictFileName)
	antes := sizeOf(t, dictPath)
	limiteAntes := c.dict.Limit()

	// slot = 1 rende o mesmo "1" do grupo já internado.
	err := c.Transition(0, payload(t, "OLT", 1, 9, true), false)
	if err == nil {
		t.Fatal("a transição que colide tem de ser recusada")
	}
	if depois := sizeOf(t, dictPath); depois != antes {
		t.Errorf("o dicionário cresceu de %d para %d bytes: a chave colidente foi ao disco, e a próxima abertura não abriria mais", antes, depois)
	}
	if limite := c.dict.Limit(); limite != limiteAntes {
		t.Errorf("o limite de grupos foi de %d para %d: a chave colidente ganhou id", limiteAntes, limite)
	}
}

// TestGrupoConhecidoNaoPassaPelaGuarda: a conferência é do caminho de grupo
// NOVO. Com grupo conhecido o ByKey resolve na hora e nada é renderizado — é o
// que mantém o caminho quente sem alocação.
func TestGrupoConhecidoNaoPassaPelaGuarda(t *testing.T) {
	dir := t.TempDir()
	c := openCounter(t, dir, defBySlot("quente"), false)
	if err := c.Transition(0, payload(t, "OLT", 7, 1, true), false); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(200, func() {
		if err := c.Transition(1, payload2(t, 7), false); err != nil {
			t.Fatal(err)
		}
		if err := c.Transition(1, payload2(t, 7), true); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("transição com grupo conhecido alocou %v vezes; a guarda de colisão não pode entrar no caminho quente", allocs)
	}
}

// payload2 é o payload mínimo com a coluna de agrupamento, reaproveitado entre
// as rodadas do gate de alocação.
var payload2Buf []byte

func payload2(t *testing.T, slot int64) []byte {
	t.Helper()
	if payload2Buf == nil {
		var err error
		if payload2Buf, err = encoding.AppendField(nil, cSlot, types.Value{Type: types.TypeInt64, I64: slot}); err != nil {
			t.Fatal(err)
		}
	}
	return payload2Buf
}

func sizeOf(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
