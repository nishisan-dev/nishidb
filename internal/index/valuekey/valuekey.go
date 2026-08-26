// Package valuekey é o DONO ÚNICO da chave canônica de um valor indexado —
// os bytes [tipo][payload] com que bitmap e hash endereçam um valor de coluna.
//
// Ele existe porque a chave tem DOIS consumidores e não pode ter duas
// implementações. O byte de TIPO faz parte dela, e o planner depende disso: o
// eqTermOf (internal/query/planner/planner.go) exige `Val.Type == Field.Type`
// justamente porque um literal promovido — coluna int64 comparada a um float,
// que vira double dos dois lados — geraria uma chave que nunca casa com as
// postings gravadas, e o lookup devolveria vazio em silêncio. Duas cópias
// desta serialização divergiriam no primeiro tipo novo, e o sintoma seria
// resultado incompleto num motor de índice e não no outro.
//
// A serialização é FORMATO DE DISCO: ela é a chave gravada em cada entrada do
// values.dict do bitmap, e mudá-la invalidaria todo índice já materializado
// sem que nada acusasse na abertura. É por isso que o movimento deste código
// veio acompanhado de um golden de bytes (TestChaveCanonicaEhGoldenDeDisco).
package valuekey

import (
	"encoding/binary"
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Validate informa se o valor pode ser indexado.
//
// O Store precisa chamar isto ANTES de tornar o record durável. Descobrir a
// violação depois do append é fatal e não apenas inconveniente: o record
// fica no segmento, o replay do tail tenta reindexá-lo a cada abertura,
// falha pelo mesmo motivo, e o dataset inteiro passa a ser INABRÍVEL — um
// único evento externo bastaria para isso, já que o teto é atingível com a
// configuração default.
func Validate(v types.Value) error {
	switch v.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		return nil // largura fixa, sempre cabe
	case types.TypeString, types.TypeBytes, types.TypeJSON,
		types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
		if len(v.Bytes) > types.MaxIndexedValueBytes {
			// types.ErrTooLarge: é culpa do EVENTO, e é por esse sentinela
			// que o materializer o separa do balde de "bug nosso" (§101).
			return fmt.Errorf("%w: índice: valor de %d bytes excede o teto de %d do values.dict",
				types.ErrTooLarge, len(v.Bytes), types.MaxIndexedValueBytes)
		}
		return nil
	default:
		return fmt.Errorf("índice: tipo %#x não indexável", v.Type)
	}
}

// Append serializa a Value em bytes canônicos [type][payload] sobre dst, para
// chavear o dicionário de valores do bitmap e as postings do hash.
//
// dst é um buffer do CHAMADOR e o retorno tem de ser reatribuído: os dois
// motores reusam um scratch por writer para não alocar por campo aplicado, e
// perder o retorno faria a chave sair de um buffer que ninguém mais conhece.
func Append(dst []byte, v types.Value) ([]byte, error) {
	switch v.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		dst = append(dst, byte(v.Type))
		return binary.LittleEndian.AppendUint64(dst, uint64(v.I64)), nil
	case types.TypeString, types.TypeBytes, types.TypeJSON,
		types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
		dst = append(dst, byte(v.Type))
		return append(dst, v.Bytes...), nil
	default:
		return nil, fmt.Errorf("índice: tipo %#x não indexável", v.Type)
	}
}
