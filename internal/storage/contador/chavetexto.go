package contador

import (
	"fmt"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TextoDaChave renderiza a chave composta de um grupo como o `groupKey` da
// tabela virtual: os componentes na ordem do groupBy, separados por `|`.
//
// A string escapa `\` como `\\` e `|` como `\|`, para que dois grupos distintos
// nunca rendam o mesmo texto — o texto é a key da tabela, e o point lookup por
// ela tem de identificar UM grupo. int64 sai em decimal, boolean como
// `true`/`false`. Um componente só sai sem separador: `groupKey` de um
// contador por OLT é a própria OLT.
//
// Só os três tipos que o groupBy aceita têm renderização; qualquer outro é
// erro, e não um fallback, porque um fallback silencioso é como o texto de um
// tipo novo divergiria do que o operador espera.
//
// dst e scratch são do chamador e os retornos têm de ser reatribuídos: a
// função roda a cada grupo NOVO no writer e a cada linha na tabela virtual.
func TextoDaChave(dst []byte, chave []byte, scratch []types.Value) ([]byte, []types.Value, error) {
	vals, err := valuekey.DecodeComposta(chave, scratch[:0])
	if err != nil {
		return dst, vals, err
	}
	dst = dst[:0]
	for i, v := range vals {
		if i > 0 {
			dst = append(dst, '|')
		}
		switch v.Type {
		case types.TypeString:
			for _, b := range v.Bytes {
				if b == '\\' || b == '|' {
					dst = append(dst, '\\')
				}
				dst = append(dst, b)
			}
		case types.TypeInt64:
			dst = strconv.AppendInt(dst, v.I64, 10)
		case types.TypeBool:
			if v.I64 != 0 {
				dst = append(dst, "true"...)
			} else {
				dst = append(dst, "false"...)
			}
		default:
			return dst, vals, fmt.Errorf("contador: componente %d da chave tem tipo %#x, sem renderização (groupBy aceita string, int64 e boolean)", i, v.Type)
		}
	}
	return dst, vals, nil
}
