package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// listaOfParametros implementa flag.Value para --param <tipo>:<valor>,
// repetível.
//
// A ordem da line de comando é a ordem posicional dos `?` na query, e flag
// chama Set na ordem em que os argumentos aparecem — o append preserva. Nada
// aqui pode virar map.
type listaOfParametros []protocol.Param

// String é chamada pelo pacote flag no valor ZERO para montar o texto do
// --help, antes de qualquer Set. Receptor por valor e tolerante ao nil: um
// String que desreferenciasse panicaria em `nishidb query --help`, que é a
// primeira coisa que alguém faz ao descobrir a flag.
func (l listaOfParametros) String() string {
	if len(l) == 0 {
		return ""
	}
	partes := make([]string, len(l))
	for i, p := range l {
		partes[i] = p.Type + ":" + string(p.Value)
	}
	return strings.Join(partes, " ")
}

// Set decompõe <tipo>:<valor> e monta o Parametro do wire.
func (l *listaOfParametros) Set(s string) error {
	tipo, value, ok := strings.Cut(s, ":")
	if !ok {
		return fmt.Errorf("esperado <tipo>:<valor>, com tipo em {%s}", tiposOfParametro)
	}
	// strings.Cut corta no PRIMEIRO ':', e é o que faz
	// `--param string:http://x:9142` ser a string "http://x:9142" em vez de um
	// erro. Dois-pontos é comum demais em valor real para outra escolha.
	bruto, err := wireValue(tipo, value)
	if err != nil {
		return err
	}
	*l = append(*l, protocol.Param{Type: tipo, Value: bruto})
	return nil
}

// valorDeWire monta o json.RawMessage que o field "value" do protocolo espera.
//
// O int64 e o double vão VERBATIM, sem passar por strconv aqui. É o mesmo
// motivo de protocol.Param.Value ser json.RawMessage e não any: decodificar
// para float64 no caminho faria `--param int64:9223372036854775808` chegar ao
// servidor como um número arredondado e ACEITO, em vez de receber a mensagem de
// overflow. A validação é a do protocolo, uma só, e ela roda sobre o texto
// original.
func wireValue(tipo, value string) (json.RawMessage, error) {
	switch tipo {
	case protocol.WireTypeString:
		// json.Marshal e não aspas na mão: é aqui que ", \ e os controles
		// viram escapes JSON válidos.
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return b, nil
	case protocol.WireTypeInt64, protocol.WireTypeDouble, protocol.WireTypeBoolean:
		return json.RawMessage(value), nil
	case "":
		return nil, fmt.Errorf("tipo ausente (aceitos: %s)", tiposOfParametro)
	default:
		return nil, fmt.Errorf("tipo %q não é um tipo de wire (aceitos: %s)", tipo, tiposOfParametro)
	}
}

// tiposOfParametro repete a lista do protocolo para a mensagem de USO, que
// precisa dela antes de qualquer valor ser validado.
const tiposOfParametro = protocol.WireTypeString + ", " + protocol.WireTypeInt64 + ", " +
	protocol.WireTypeDouble + ", " + protocol.WireTypeBoolean
