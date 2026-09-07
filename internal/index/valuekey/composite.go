package valuekey

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidCompositeKey marca uma chave composta que não decodifica: rasgada,
// com componente de tipo desconhecido ou com tamanho que não fecha.
var ErrInvalidCompositeKey = errors.New("valuekey: chave composta inválida")

// fixedWidth é o tamanho do payload dos tipos de largura fixa da chave
// simples — os 8 bytes do I64, sempre os oito (ver o golden da chave simples).
const fixedWidth = 8

// AppendComposite serializa uma lista de valores como a chave canônica de um
// GRUPO — a chave do dicionário de grupos do contador mantido na escrita.
//
// Mora aqui, e não no contador, pela razão que dá nome ao pacote: a chave de um
// valor já tem dono único, e uma segunda codificação do mesmo formato em outro
// lugar é como as duas divergem. O que a composta acrescenta é só a DELIMITAÇÃO:
// cada componente sai como `u16 LE tamanho | chave simples`, porque a chave
// simples de largura variável não carrega tamanho — concatenar "ab"+"c" e
// "a"+"bc" daria os mesmos bytes.
//
// É FORMATO DE DISCO (golden em TestChaveCompostaEhGoldenDeDisco), e dst é um
// buffer do chamador cujo retorno tem de ser reatribuído, como no Append: a
// chave é montada a cada evento no writer e não pode alocar.
func AppendComposite(dst []byte, vs []types.Value) ([]byte, error) {
	if len(vs) == 0 {
		return dst, fmt.Errorf("%w: chave sem componente", ErrInvalidCompositeKey)
	}
	for i := range vs {
		// O prefixo é gravado como placeholder e preenchido DEPOIS do Append:
		// é o que permite reusar a serialização simples sem um scratch
		// intermediário, e o tamanho de uma string só se conhece de fato pelo
		// que o Append emitiu.
		start := len(dst)
		dst = append(dst, 0, 0)
		var err error
		if dst, err = Append(dst, vs[i]); err != nil {
			return dst, err
		}
		size := len(dst) - start - 2
		if size > int(^uint16(0)) {
			return dst, fmt.Errorf("%w: componente %d com %d bytes excede u16", ErrInvalidCompositeKey, i, size)
		}
		binary.LittleEndian.PutUint16(dst[start:], uint16(size))
	}
	return dst, nil
}

// WalkComposite visita os componentes da chave, em ordem, sem alocar. Os
// Bytes de um componente de largura variável apontam para DENTRO de chave e só
// valem durante a visita. É a forma que a tabela virtual usa para montar uma
// linha por grupo sem um scratch por consulta.
func WalkComposite(key []byte, visit func(i int, v types.Value) error) error {
	pos, i := 0, 0
	for pos < len(key) {
		if len(key)-pos < 2 {
			return fmt.Errorf("%w: prefixo de tamanho truncado no byte %d", ErrInvalidCompositeKey, pos)
		}
		n := int(binary.LittleEndian.Uint16(key[pos:]))
		pos += 2
		if n < 1 || pos+n > len(key) {
			return fmt.Errorf("%w: componente de %d bytes no byte %d, chave tem %d", ErrInvalidCompositeKey, n, pos, len(key))
		}
		comp := key[pos : pos+n : pos+n]
		pos += n
		v := types.Value{Type: types.FieldType(comp[0])}
		switch v.Type {
		case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
			if len(comp) != 1+fixedWidth {
				return fmt.Errorf("%w: componente de tipo %#x com %d bytes, esperado %d", ErrInvalidCompositeKey, v.Type, len(comp), 1+fixedWidth)
			}
			v.I64 = int64(binary.LittleEndian.Uint64(comp[1:]))
		case types.TypeString, types.TypeBytes, types.TypeJSON,
			types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
			v.Bytes = comp[1:]
		default:
			return fmt.Errorf("%w: tipo %#x não indexável", ErrInvalidCompositeKey, v.Type)
		}
		if err := visit(i, v); err != nil {
			return err
		}
		i++
	}
	if i == 0 {
		return fmt.Errorf("%w: chave sem componente", ErrInvalidCompositeKey)
	}
	return nil
}

// DecodeComposite desfaz o AppendComposite. dst é o scratch do chamador (reusado
// com dst[:0]); os Bytes dos componentes de largura variável apontam para
// DENTRO de chave, sem cópia — quem retiver o valor além da vida da chave copia.
func DecodeComposite(key []byte, dst []types.Value) ([]types.Value, error) {
	dst = dst[:0]
	err := WalkComposite(key, func(_ int, v types.Value) error {
		dst = append(dst, v)
		return nil
	})
	return dst, err
}
