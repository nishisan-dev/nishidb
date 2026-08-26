package binder

import (
	"bytes"
	"unicode/utf8"
)

// TipoDePadrao classifica o padrão de LIKE no BIND, para que o caminho por
// linha seja o mais barato possível.
//
// A grande maioria dos padrões reais é 'prefixo%' ou '%trecho%', e nenhum dos
// dois precisa do glob geral: viram bytes.HasPrefix e bytes.Contains, que são
// otimizados na stdlib. O glob de dois ponteiros existe para o resto.
type TipoDePadrao uint8

const (
	// PadraoInvalido é o zero value, de propósito: padrão mal construído
	// aparece como falha em vez de casar tudo por omissão.
	PadraoInvalido TipoDePadrao = iota
	// PadraoTudo é '%' ou '%%': casa qualquer valor, inclusive o vazio.
	PadraoTudo
	// PadraoIgual é o padrão sem curinga nenhum.
	PadraoIgual
	// PadraoPrefixo é 'abc%'.
	PadraoPrefixo
	// PadraoSufixo é '%abc'.
	PadraoSufixo
	// PadraoContem é '%abc%'.
	PadraoContem
	// PadraoGeral é qualquer outra combinação, e o único que usa o glob.
	PadraoGeral
)

// LikePattern é um padrão de LIKE já compilado.
//
// COMPILADO NO BIND, uma vez por query, e nunca por linha. O padrão vem de um
// literal ou de um parâmetro, e os dois estão disponíveis no bind — foi por
// isso que `LIKE ?` não precisou de caminho separado.
//
// Os bytes são COPIADOS na compilação. O literal do parser é uma string Go
// (imutável, segura de reter), mas o parâmetro é um types.Value cujo Bytes
// pertence a quem chamou; reter aquela janela seria a mesma armadilha de
// lifetime que o ColView.Val.Bytes tem no executor.
type LikePattern struct {
	Tipo TipoDePadrao
	// Lit é o trecho literal de PadraoIgual, Prefixo, Sufixo e Contem.
	Lit []byte
	// Raw é o padrão inteiro, só para PadraoGeral.
	Raw []byte
	// MinBytes é um piso do tamanho que um valor precisa ter para ter chance
	// de casar: a soma dos bytes literais do padrão, contando cada `_` como 1.
	// Serve para rejeitar em O(1) antes de qualquer varredura.
	//
	// É PISO e não igualdade porque `_` casa um caractere, que pode ter até 4
	// bytes — um valor com 3 bytes ainda pode casar `___`. Errar para o lado
	// permissivo aqui é obrigatório: um piso alto demais produziria falso
	// negativo, que é o defeito que este motor não tolera.
	MinBytes int
}

// CompilaLike classifica o padrão. Não devolve erro: todo padrão é válido —
// sem ESCAPE não há sintaxe a violar, e `%` e `_` literais são inatingíveis
// (limitação conhecida e documentada).
func CompilaLike(pat []byte) LikePattern {
	p := LikePattern{MinBytes: minBytesDe(pat)}
	primeiro := bytes.IndexByte(pat, '%')
	ultimo := bytes.LastIndexByte(pat, '%')
	temSublinhado := bytes.IndexByte(pat, '_') >= 0

	switch {
	case primeiro < 0 && !temSublinhado:
		p.Tipo, p.Lit = PadraoIgual, copia(pat)
	case temSublinhado:
		p.Tipo, p.Raw = PadraoGeral, copia(pat)
	case ehSoPorcento(pat):
		p.Tipo = PadraoTudo
	case primeiro == ultimo && primeiro == len(pat)-1:
		p.Tipo, p.Lit = PadraoPrefixo, copia(pat[:len(pat)-1])
	case primeiro == ultimo && primeiro == 0:
		p.Tipo, p.Lit = PadraoSufixo, copia(pat[1:])
	case primeiro == 0 && ultimo == len(pat)-1 && bytes.Count(pat, []byte{'%'}) == 2:
		p.Tipo, p.Lit = PadraoContem, copia(pat[1:len(pat)-1])
	default:
		p.Tipo, p.Raw = PadraoGeral, copia(pat)
	}
	return p
}

func copia(b []byte) []byte { return append([]byte(nil), b...) }

func ehSoPorcento(pat []byte) bool {
	for _, c := range pat {
		if c != '%' {
			return false
		}
	}
	return len(pat) > 0
}

// minBytesDe soma os bytes que o valor precisa ter no mínimo: tudo que não é
// `%`, com cada `_` valendo 1 (o menor tamanho de um caractere).
func minBytesDe(pat []byte) int {
	n := 0
	for _, c := range pat {
		switch c {
		case '%':
		case '_':
			n++
		default:
			n++
		}
	}
	return n
}

// Casa reporta se v satisfaz o padrão.
//
// ZERO ALOCAÇÃO por chamada, e é requisito e não estilo: roda uma vez por linha
// varrida. Sem regexp — que aloca e é ordens de grandeza mais lenta — e sem
// converter v para string.
func (p *LikePattern) Casa(v []byte) bool {
	if len(v) < p.MinBytes {
		return false // rejeição O(1), o caso mais comum numa varredura
	}
	switch p.Tipo {
	case PadraoTudo:
		return true
	case PadraoIgual:
		return bytes.Equal(v, p.Lit)
	case PadraoPrefixo:
		return bytes.HasPrefix(v, p.Lit)
	case PadraoSufixo:
		return bytes.HasSuffix(v, p.Lit)
	case PadraoContem:
		return bytes.Contains(v, p.Lit)
	case PadraoGeral:
		return glob(p.Raw, v)
	default:
		// PadraoInvalido: padrão que não passou pelo CompilaLike. Casar nada é
		// a resposta segura — casar tudo transformaria um bug de construção em
		// resultado silenciosamente errado.
		return false
	}
}

// glob casa `%` (qualquer sequência) e `_` (UM CARACTERE) com dois ponteiros e
// UM ponto de backtrack. Sem recursão: uma cadeia de `%` num valor longo
// gastaria um frame por curinga.
//
// `_` casa um CARACTERE e não um byte, e isso é decisão de produto: o JSON é
// UTF-8 por definição, e um `_` casando meio `ç` seria indefensável num banco
// que guarda texto. utf8.DecodeRune não aloca, e byte inválido devolve
// RuneError com tamanho 1 — o que mantém a função TOTAL sobre qualquer entrada,
// inclusive um valor que não seja UTF-8 válido.
//
// Os literais comparam BYTE a byte, o que é equivalente a comparar runes para
// UTF-8 válido e mais barato. Só o avanço do `_` e o do backtrack precisam
// decodificar.
func glob(pat, s []byte) bool {
	var (
		p, i     int
		estrelaP = -1
		estrelaI int
	)
	for i < len(s) {
		switch {
		case p < len(pat) && pat[p] == '_':
			_, w := utf8.DecodeRune(s[i:])
			i += w
			p++
		case p < len(pat) && pat[p] == '%':
			// Guarda o ponto de retomada e tenta casar o resto com zero
			// caracteres consumidos.
			estrelaP, estrelaI = p, i
			p++
		case p < len(pat) && pat[p] == s[i]:
			p++
			i++
		case estrelaP >= 0:
			// Não casou: o `%` mais recente engole mais um caractere.
			_, w := utf8.DecodeRune(s[estrelaI:])
			estrelaI += w
			i, p = estrelaI, estrelaP+1
		default:
			return false
		}
	}
	// Sobrou padrão: só `%` pode casar o vazio. Um `_` pendente reprova, que é
	// o correto — ele exige um caractere que já não existe.
	for p < len(pat) && pat[p] == '%' {
		p++
	}
	return p == len(pat)
}
