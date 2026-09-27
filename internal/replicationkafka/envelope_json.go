package replicationkafka

import "unicode/utf8"

// maxJSONDepth é o aninhamento máximo aceito, o MESMO teto do scanner do
// encoding/json (que o json.Valid da ingestão aplica). Documento mais fundo
// que isso não entrou pela ingestão de coluna json; se chegar aqui (o `_raw`
// de um evento ruim não passa por validação nenhuma), sai como string, igual
// a qualquer json inválido.
const maxJSONDepth = 10000

// validJSON informa se b é exatamente UM valor JSON (RFC 8259), com espaço em
// branco opcional em volta, E se é UTF-8 válido de ponta a ponta.
//
// Existe porque o valor json é emitido INLINE no `record`, e o inline só é
// seguro se os bytes forem JSON. A ingestão garante isso para as colunas json
// (json.Valid no typeconv), mas não para o `_raw` do evento que não parseou:
// ele vai para a histórica com os bytes crus do evento — JSON quebrado ou
// UTF-8 inválido. Emitido verbatim, ele faria do envelope inteiro um JSON
// inválido: uma mensagem venenosa para todo consumidor do tópico.
//
// Não aloca e não recursa: a pilha de contêineres é um bitset de tamanho fixo
// no stack (bit 1 = objeto, bit 0 = array). O UTF-8 é conferido só dentro de
// string, porque fora de string todo byte >= 0x80 já é erro de sintaxe.
//
// Paridade com json.Valid && utf8.Valid, inclusive no escape de surrogate
// solitário (`"\ud800"`), que a gramática da RFC aceita e o encoding/json
// também: o texto do escape é ASCII, e o que um decodificador faz com o
// surrogate é decisão dele.
func validJSON(b []byte) bool {
	var stack [(maxJSONDepth + 63) / 64]uint64
	depth := 0
	i := skipSpace(b, 0)
	for {
		// Fase 1: um valor começa em i.
		if i >= len(b) {
			return false
		}
		ok := false
		switch c := b[i]; c {
		case '{', '[':
			if depth == maxJSONDepth {
				return false
			}
			isObject := c == '{'
			if isObject {
				stack[depth>>6] |= 1 << (depth & 63)
			} else {
				stack[depth>>6] &^= 1 << (depth & 63)
			}
			depth++
			i = skipSpace(b, i+1)
			if i < len(b) && (b[i] == '}' && isObject || b[i] == ']' && !isObject) {
				// Contêiner vazio: fecha e segue para a fase 2.
				depth--
				i++
				ok = true
				break
			}
			if isObject {
				if i, ok = objectKey(b, i); !ok {
					return false
				}
			}
			continue
		case '"':
			i, ok = scanString(b, i)
		case 't':
			i, ok = scanLiteral(b, i, "true")
		case 'f':
			i, ok = scanLiteral(b, i, "false")
		case 'n':
			i, ok = scanLiteral(b, i, "null")
		default:
			i, ok = scanNumber(b, i)
		}
		if !ok {
			return false
		}
		// Fase 2: depois de um valor, fecha contêineres ou pede o próximo.
		for {
			i = skipSpace(b, i)
			if depth == 0 {
				return i == len(b)
			}
			if i >= len(b) {
				return false
			}
			top := depth - 1
			isObject := stack[top>>6]&(1<<(top&63)) != 0
			c := b[i]
			if c == ',' {
				i = skipSpace(b, i+1)
				if isObject {
					if i, ok = objectKey(b, i); !ok {
						return false
					}
				}
				break
			}
			if c == '}' && isObject || c == ']' && !isObject {
				depth--
				i++
				continue
			}
			return false
		}
	}
}

// objectKey consome `"chave"` + `:` + espaço, deixando i no início do valor.
func objectKey(b []byte, i int) (int, bool) {
	if i >= len(b) || b[i] != '"' {
		return i, false
	}
	i, ok := scanString(b, i)
	if !ok {
		return i, false
	}
	i = skipSpace(b, i)
	if i >= len(b) || b[i] != ':' {
		return i, false
	}
	return skipSpace(b, i+1), true
}

func skipSpace(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanString consome a string que começa em b[i] == '"' e devolve o índice
// depois da aspa de fechamento.
func scanString(b []byte, i int) (int, bool) {
	i++ // aspa de abertura
	for i < len(b) {
		c := b[i]
		switch {
		case c == '"':
			return i + 1, true
		case c == '\\':
			if i+1 >= len(b) {
				return i, false
			}
			switch b[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(b) {
					return i, false
				}
				for _, h := range b[i+2 : i+6] {
					if !isHex(h) {
						return i, false
					}
				}
				i += 6
			default:
				return i, false
			}
		case c < 0x20:
			// Control char cru dentro de string: a RFC exige escape.
			return i, false
		case c < utf8.RuneSelf:
			i++
		default:
			// DecodeRune recusa sequência truncada, overlong e surrogate
			// codificado em UTF-8 (ED A0 80), devolvendo (RuneError, 1).
			r, size := utf8.DecodeRune(b[i:])
			if r == utf8.RuneError && size == 1 {
				return i, false
			}
			i += size
		}
	}
	return i, false
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func scanLiteral(b []byte, i int, lit string) (int, bool) {
	if len(b)-i < len(lit) {
		return i, false
	}
	for j := 0; j < len(lit); j++ {
		if b[i+j] != lit[j] {
			return i, false
		}
	}
	return i + len(lit), true
}

// scanNumber consome `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
func scanNumber(b []byte, i int) (int, bool) {
	if i < len(b) && b[i] == '-' {
		i++
	}
	switch {
	case i >= len(b):
		return i, false
	case b[i] == '0':
		i++
	case '1' <= b[i] && b[i] <= '9':
		i = skipDigits(b, i+1)
	default:
		return i, false
	}
	if i < len(b) && b[i] == '.' {
		j := skipDigits(b, i+1)
		if j == i+1 {
			return i, false
		}
		i = j
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		j := skipDigits(b, i)
		if j == i {
			return i, false
		}
		i = j
	}
	return i, true
}

func skipDigits(b []byte, i int) int {
	for i < len(b) && '0' <= b[i] && b[i] <= '9' {
		i++
	}
	return i
}
