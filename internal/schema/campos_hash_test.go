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
//
// A grafia mudou com a D5 (`index: true`, não mais `index: hash`), mas a
// função e o modo de falha que ela impede continuam os mesmos.
func TestCamposComHashIncluiADerivada(t *testing.T) {
	yml := strings.Replace(extractYAML,
		"          as: probe_ip\n          type: string",
		"          as: probe_ip\n          type: string\n          index: true", 1)
	yml = strings.Replace(yml, "    id:\n      type: string",
		"    id:\n      type: string\n    node:\n      type: string\n      index: true", 1)
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

// TestCamposComBitmapEComHashDevolvemOMesmoConjunto substitui o antigo
// TestOsDoisMotoresNaoSeMisturam: aquele teste travava a garantia de que um
// campo declarado com hash não aparecia na lista de bitmap nem o contrário —
// uma garantia que fazia sentido enquanto as duas eram grafias DIFERENTES.
//
// A D5 aposentou as duas em favor de `index: true`, e com uma grafia só não há
// mais "motor errado" para o campo cair: CamposComBitmap e CamposComHash leem
// a MESMA chave e por isso devolvem o MESMO conjunto sempre. Elas continuam
// sendo duas funções (e não uma só) porque storage.Options ainda tem
// BitmapFieldNames e HashFieldNames separados — unificar isso é a Tarefa 11,
// junto com o planner — e o Store trata a união das duas listas (hoje
// idênticas) como idempotente.
func TestCamposComBitmapEComHashDevolvemOMesmoConjunto(t *testing.T) {
	yml := strings.Replace(extractYAML, "    id:\n      type: string",
		"    id:\n      type: string\n"+
			"    status:\n      type: string\n      index: true\n"+
			"    node:\n      type: string\n      index: true", 1)
	s := parseOK(t, yml)

	quer := []string{"node", "status"} // ordenado
	if got := CamposComBitmap(s); !slices.Equal(got, quer) {
		t.Fatalf("CamposComBitmap = %v, quer %v", got, quer)
	}
	if got := CamposComHash(s); !slices.Equal(got, quer) {
		t.Fatalf("CamposComHash = %v, quer %v", got, quer)
	}
}
