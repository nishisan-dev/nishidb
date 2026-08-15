package protocolo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrParametro é a categoria de falha na conversão do `parameters` do §68.
// Quem está na fronteira HTTP classifica com errors.Is e responde 400.
var ErrParametro = errors.New("protocolo: parâmetro inválido")

// Valores converte o `parameters` do §68 na representação de wire que o
// binder aceita.
//
// A conversão é DELIBERADAMENTE burra: não infere tipo, não coage entre formas
// JSON, não arredonda. O tipo declarado manda, e o que não casa é recusado com
// uma mensagem que nomeia a alternativa. Quem coage é o binder, contra o tipo
// da COLUNA, pela mesma tabela dos literais SQL — duplicar essa lógica aqui
// criaria um segundo caminho de coerção e, com ele, a chance de o mesmo valor
// significar coisas diferentes conforme tenha entrado por parâmetro ou escrito
// na query.
//
// Os índices nas mensagens são 1-based, como no binder: quem escreve a query
// conta os `?` a partir do primeiro.
func Valores(ps []Parametro) ([]types.Value, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	out := make([]types.Value, len(ps))
	for i, p := range ps {
		v, err := p.valor()
		if err != nil {
			return nil, fmt.Errorf("%w: parameters[%d]: %w", ErrParametro, i+1, err)
		}
		out[i] = v
	}
	return out, nil
}

// valor converte UM parâmetro.
func (p Parametro) valor() (types.Value, error) {
	raw := bytes.TrimSpace(p.Value)
	if len(raw) == 0 {
		return types.Value{}, errors.New(`"value" ausente`)
	}
	if bytes.Equal(raw, []byte("null")) {
		// Espelha errParamNulo do binder, e pela mesma razão: `col = ?` com ?
		// nulo não tem semântica definida sem lógica trivalente, e o que o
		// usuário quer dizer é IS NULL.
		return types.Value{}, errors.New(`"value" é null; para testar ausência escreva IS NULL na query, não um parâmetro nulo`)
	}

	switch p.Type {
	case TipoString:
		return valorString(raw)
	case TipoInt64:
		return valorInt64(raw)
	case TipoDouble:
		return valorDouble(raw)
	case TipoBoolean:
		return valorBoolean(raw)
	case "":
		return types.Value{}, fmt.Errorf(`"type" ausente (aceitos: %s)`, tiposAceitos)
	default:
		return types.Value{}, fmt.Errorf(
			`"type" %q não é um tipo de wire (aceitos: %s); timestamp, ipv4, ipv6, prefixos, bytes e json viajam como %q `+
				`e são convertidos contra o tipo da coluna, pela mesma tabela dos literais SQL`,
			p.Type, tiposAceitos, TipoString)
	}
}

const tiposAceitos = TipoString + ", " + TipoInt64 + ", " + TipoDouble + ", " + TipoBoolean

func valorString(raw []byte) (types.Value, error) {
	if raw[0] != '"' {
		return types.Value{}, fmt.Errorf(`type %q espera uma string JSON entre aspas, e "value" é %s`, TipoString, formaDe(raw))
	}
	// encoding/json e não um decodificador próprio: é aqui que os escapes
	// (\n, \uXXXX, pares surrogados) viram bytes, e reimplementá-los seria uma
	// segunda gramática de string JSON no produto. Não é caminho quente — roda
	// uma vez por parâmetro, não uma vez por row.
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return types.Value{}, fmt.Errorf(`"value" não é uma string JSON válida: %w`, err)
	}
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}, nil
}

func valorInt64(raw []byte) (types.Value, error) {
	if !typeparse.JSONNumber(raw) {
		return types.Value{}, fmt.Errorf(`type %q espera um número JSON, e "value" é %s`, TipoInt64, formaDe(raw))
	}
	if !typeparse.IntegralNumber(raw) {
		return types.Value{}, fmt.Errorf(
			`type %q espera um inteiro, e "value" é %s: escreva o valor sem fração nem expoente, ou declare type %q`,
			TipoInt64, raw, TipoDouble)
	}
	n, err := typeparse.Int64(raw)
	if err != nil {
		// O caminho real aqui é overflow: 9223372036854775808 casa a gramática
		// de número e não cabe em int64.
		return types.Value{}, fmt.Errorf(`"value" %s não cabe em int64: %w`, raw, err)
	}
	return types.Value{Type: types.TypeInt64, I64: n}, nil
}

func valorDouble(raw []byte) (types.Value, error) {
	if !typeparse.JSONNumber(raw) {
		return types.Value{}, fmt.Errorf(`type %q espera um número JSON, e "value" é %s`, TipoDouble, formaDe(raw))
	}
	f, err := typeparse.Float64(raw)
	if err != nil {
		return types.Value{}, fmt.Errorf(`"value" %s não é um double válido: %w`, raw, err)
	}
	// A representação é a mesma do resto do produto: os bits do float em I64
	// (ver o godoc de types.Value e o AppendValue do encoder).
	return types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(f))}, nil
}

func valorBoolean(raw []byte) (types.Value, error) {
	switch {
	case bytes.Equal(raw, []byte("true")):
		return types.Value{Type: types.TypeBool, I64: 1}, nil
	case bytes.Equal(raw, []byte("false")):
		return types.Value{Type: types.TypeBool, I64: 0}, nil
	}
	return types.Value{}, fmt.Errorf(`type %q espera true ou false, e "value" é %s`, TipoBoolean, formaDe(raw))
}

// formaDe nomeia a FORMA JSON do valor recebido, sem ecoá-lo por inteiro.
//
// Não ecoar é deliberado: o valor pode carregar dado do cliente, e o §95
// proíbe registrar payload. A forma é o que o remetente precisa para achar o
// erro — "esperava número, veio string" resolve sem expor o conteúdo.
func formaDe(raw []byte) string {
	switch raw[0] {
	case '"':
		return "uma string"
	case '{':
		return "um objeto"
	case '[':
		return "uma lista"
	case 't', 'f':
		return "um booleano"
	case 'n':
		return "null"
	default:
		return "um número"
	}
}
