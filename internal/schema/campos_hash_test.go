package schema

import (
	"slices"
	"strings"
	"testing"
)

// CamposComHash é o análogo do CamposComBitmap para o segundo motor, e existe
// pelo MESMO modo de falha cruzado — que agora é alcançável, e antes não era.
//
// Enquanto o índice hash não tinha motor físico, `index: hash` era só um
// rótulo: ninguém precisava listar os campos, e esquecê-los não custava nada.
// Com o motor, o Store abre um arquivo por campo e grava o conjunto no
// manifest — então um sítio que não passasse a lista criaria o dataset COM o
// índice pelo servidor e faria a abertura seguinte (o `nishidb query`) RECUSAR,
// porque o Store não deixa passar manifest que declara índice que a abertura
// não conhece.
func TestCamposComHashIncluiADerivada(t *testing.T) {
	yml := strings.Replace(extractYAML,
		"          as: probe_ip\n          type: string",
		"          as: probe_ip\n          type: string\n          index: hash", 1)
	yml = strings.Replace(yml, "    id:\n      type: string",
		"    id:\n      type: string\n    node:\n      type: string\n      index: hash", 1)
	s := parseOK(t, yml)

	got := CamposComHash(s)
	quer := []string{"node", "probe_ip"} // ordenado
	if !slices.Equal(got, quer) {
		t.Fatalf("CamposComHash = %v, quer %v (a ordem é contrato: ela decide o FieldID de um dataset novo)", got, quer)
	}
}

func TestCamposComHashVazioSemIndice(t *testing.T) {
	if got := CamposComHash(parseOK(t, extractYAML)); len(got) != 0 {
		t.Errorf("CamposComHash = %v, quer vazio", got)
	}
}

// TestOsDoisMotoresNaoSeMisturam trava o que a parametrização por motor poderia
// quebrar em silêncio: um campo declarado com hash não pode aparecer na lista
// de bitmap nem o contrário. Se aparecesse, o Store abriria o motor errado e a
// query degradaria — ou pior, o manifest declararia um índice que o schema não
// pede e a abertura seguinte seria recusada.
func TestOsDoisMotoresNaoSeMisturam(t *testing.T) {
	yml := strings.Replace(extractYAML, "    id:\n      type: string",
		"    id:\n      type: string\n"+
			"    status:\n      type: string\n      index: bitmap\n"+
			"    node:\n      type: string\n      index: hash", 1)
	s := parseOK(t, yml)

	if got := CamposComBitmap(s); !slices.Equal(got, []string{"status"}) {
		t.Fatalf("CamposComBitmap = %v, quer [status]", got)
	}
	if got := CamposComHash(s); !slices.Equal(got, []string{"node"}) {
		t.Fatalf("CamposComHash = %v, quer [node]", got)
	}
}
