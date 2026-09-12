package counter

import (
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// KeyText renderiza a chave composta de um grupo como o `groupKey` da tabela
// virtual: decodifica os componentes e entrega ao molde, que decide separador,
// ordem, literais e zero à esquerda (internal/keyformat).
//
// A decodificação mora aqui, e não no keyformat, porque a chave composta é
// formato de disco do dicionário de grupos; o molde só conhece valores.
//
// Duas chaves distintas nunca rendem o mesmo texto — o texto é a key da tabela,
// e o point lookup por ela tem de identificar UM grupo. Quem garante isso são
// as recusas de keyformat.Parse; o argumento completo está no godoc daquele
// pacote, e a guarda de colisão de catchUpWithDict é a rede embaixo dele.
//
// dst e scratch são do chamador e os retornos têm de ser reatribuídos: a função
// roda a cada grupo NOVO no writer e sobre TODOS os grupos na abertura do
// dataset. Depois disso a tabela virtual lê o texto já pronto de `texts`, e
// não chama esta função por linha.
func KeyText(dst []byte, format *keyformat.Format, key []byte, scratch []types.Value) ([]byte, []types.Value, error) {
	vals, err := valuekey.DecodeComposite(key, scratch[:0])
	if err != nil {
		return dst, vals, err
	}
	dst, err = format.Append(dst[:0], vals)
	return dst, vals, err
}
