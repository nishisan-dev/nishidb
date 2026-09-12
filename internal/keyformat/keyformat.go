// Package keyformat compila o MOLDE que decide como a chave composta de um
// grupo de contador vira o texto `groupKey` — a coluna key de
// `_counters.<name>`, e portanto o que a aplicação escreve num
// `WHERE groupKey = '…'`.
//
// É pacote folha, dependendo só de internal/types, pelo mesmo motivo de
// internal/typeparse: o MESMO parser serve à recusa na subida (internal/schema,
// que só tem os nomes e os tipos declarados) e à renderização
// (internal/storage/counter, que tem os valores). Duas cópias da gramática é
// como as duas divergem.
//
// # Não-ambiguidade
//
// Dois grupos distintos nunca podem render o mesmo texto: o texto é a key da
// tabela, e o point lookup por ela tem de identificar UM grupo. Quem garante
// isso é o conjunto de recusas de Parse mais o escape de Append, e o argumento
// é que o texto tem uma única leitura da esquerda para a direita:
//
//   - o literal de prefixo e o de sufixo têm comprimento fixo, e se descascam;
//   - componente string escapa `\` e o primeiro caractere do literal que o
//     segue, então a primeira ocorrência NÃO escapada daquele caractere é a
//     fronteira — e nenhum literal pode conter `\` (senão um `\` do molde se
//     confundiria com um `\` de escape, e `{a}\{b}` colidiria para
//     (`x\`,`y`) e (`x`,`\y`), que rendem os dois `x\\\y`);
//   - componente int64 é `-?[0-9]+` e o literal seguinte não pode começar com
//     dígito nem com `-`, então a varredura gulosa para exatamente nele;
//   - componente boolean é `true` ou `false`, que se distinguem no primeiro
//     caractere;
//   - dois marcadores nunca ficam colados, então toda fronteira tem literal.
//
// O último componente não escapa nada além de `\`: ali não há fronteira a
// proteger, e escapar o separador seria sujar o texto sem nada ganhar.
package keyformat

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const (
	// MaxMoldBytes é o teto do molde declarado. Existe porque o texto de cada
	// grupo é guardado DUAS vezes em heap (o slice por id e a chave do mapa de
	// lookup): sem teto, um molde de 1 KiB com maxGroups no default seriam
	// gigabytes decididos por uma linha de YAML.
	MaxMoldBytes = 256
	// maxPad é a maior largura de zero à esquerda declarável: int64 tem no
	// máximo 19 dígitos e um sinal.
	maxPad = 20
	// defaultSeparator é o literal entre os componentes do molde default — o
	// separador histórico do groupKey.
	defaultSeparator = '|'
)

// Column é uma coluna do groupBy, na ordem DECLARADA.
type Column struct {
	Name string
	Type types.FieldType
}

// Columns monta as colunas a partir dos nomes e tipos do groupBy, que é como o
// contador e o schema já os carregam — índice a índice, na ordem declarada.
func Columns(names []string, kinds []types.FieldType) []Column {
	out := make([]Column, len(names))
	for i := range names {
		out[i] = Column{Name: names[i], Type: kinds[i]}
	}
	return out
}

// Format é um molde compilado. É imutável depois de Parse e pode ser lido por
// vários leitores ao mesmo tempo.
type Format struct {
	mold   string
	prefix []byte
	items  []item
}

// item é um marcador e o literal que vem DEPOIS dele. O literal do último item
// é o sufixo do molde, e pode ser vazio.
type item struct {
	col   int    // índice da coluna na ordem do groupBy
	pad   int    // largura do zero à esquerda; 0 é sem preenchimento
	after []byte // literal que segue o marcador
	esc   byte   // after[0], ou 0 quando não há literal depois
}

// Len é quantos componentes o molde renderiza — sempre o tamanho do groupBy,
// porque toda coluna entra exatamente uma vez.
func (f *Format) Len() int { return len(f.items) }

// String devolve o molde efetivo — o declarado, ou o default já expandido. É o
// que `_meta.counters` mostra, para o operador nunca ter de adivinhar qual
// formato está valendo.
func (f *Format) String() string { return f.mold }

// Default é o molde de quem não declarou `keyFormat`: os componentes na ordem
// do groupBy, separados por `|`.
//
// Ele NÃO passa por Parse de propósito. Nome de coluna no NishiDB não é
// restrito a minúsculas nem a um alfabeto (a spec tem `Acct-Status-Type`), e um
// nome com `:`, `{` ou `}` não teria como ser escrito num molde — fazer o
// default atravessar a gramática quebraria um dataset que hoje funciona.
func Default(cols []Column) *Format {
	f := &Format{items: make([]item, len(cols))}
	var sb strings.Builder
	for j := range cols {
		if j > 0 {
			sb.WriteByte(defaultSeparator)
		}
		sb.WriteByte('{')
		sb.WriteString(cols[j].Name)
		sb.WriteByte('}')
		f.items[j] = item{col: j}
		if j < len(cols)-1 {
			f.items[j].after = []byte{defaultSeparator}
			f.items[j].esc = defaultSeparator
		}
	}
	f.mold = sb.String()
	return f
}

// Parse compila um molde declarado contra as colunas do groupBy.
//
// As recusas são todas aqui, e todas na subida: molde é config, e config recusa
// em vez de ignorar em silêncio.
func Parse(mold string, cols []Column) (*Format, error) {
	if mold == "" {
		return nil, fmt.Errorf("keyFormat vazio — omita a chave para usar o formato default")
	}
	if len(mold) > MaxMoldBytes {
		return nil, fmt.Errorf("keyFormat com %d bytes excede o teto de %d: o texto de cada grupo mora duas vezes em heap", len(mold), MaxMoldBytes)
	}
	f := &Format{mold: mold}
	used := make([]int, len(cols))
	var lit []byte
	for i := 0; i < len(mold); {
		switch c := mold[i]; {
		case c == '{' && i+1 < len(mold) && mold[i+1] == '{':
			lit, i = append(lit, '{'), i+2
		case c == '}' && i+1 < len(mold) && mold[i+1] == '}':
			lit, i = append(lit, '}'), i+2
		case c == '}':
			return nil, fmt.Errorf("keyFormat %q: `}` sozinho na posição %d — use `}}` para a chave literal", mold, i)
		case c == '{':
			end := strings.IndexByte(mold[i:], '}')
			if end < 0 {
				return nil, fmt.Errorf("keyFormat %q: `{` na posição %d sem o `}` que o fecha", mold, i)
			}
			it, err := parseMarker(mold, mold[i+1:i+end], cols, used)
			if err != nil {
				return nil, err
			}
			if len(f.items) == 0 {
				f.prefix = lit
			} else {
				f.items[len(f.items)-1].after = lit
			}
			lit, i = nil, i+end+1
			f.items = append(f.items, it)
		default:
			lit, i = append(lit, c), i+1
		}
	}
	if len(f.items) == 0 {
		return nil, fmt.Errorf("keyFormat %q não tem nenhum marcador — o texto seria o mesmo para todo grupo", mold)
	}
	f.items[len(f.items)-1].after = lit
	if err := f.checkColumns(cols, used); err != nil {
		return nil, err
	}
	return f, f.checkLiterals(cols)
}

// parseMarker lê o miolo de um `{…}`. O nome é casado LITERALMENTE contra as
// colunas do groupBy: nome de coluna não tem alfabeto declarado, e inventar um
// aqui recusaria coluna legítima.
func parseMarker(mold, body string, cols []Column, used []int) (item, error) {
	name, spec := body, ""
	if k := strings.IndexByte(body, ':'); k >= 0 {
		name, spec = body[:k], body[k+1:]
	}
	if name == "" {
		return item{}, fmt.Errorf("keyFormat %q: marcador `{%s}` sem nome de coluna", mold, body)
	}
	col := -1
	for j := range cols {
		if cols[j].Name == name {
			col = j
			break
		}
	}
	if col < 0 {
		return item{}, fmt.Errorf("keyFormat %q: `{%s}` nomeia %q, que não é coluna do groupBy (o groupBy é: %s)", mold, body, name, namesOf(cols))
	}
	used[col]++
	it := item{col: col}
	if spec == "" {
		return it, nil
	}
	if cols[col].Type != types.TypeInt64 {
		return item{}, fmt.Errorf("keyFormat %q: `{%s}` pede zero à esquerda numa coluna que não é int64 — só número tem largura", mold, body)
	}
	if len(spec) < 2 || len(spec) > 3 || spec[0] != '0' {
		return item{}, fmt.Errorf("keyFormat %q: `{%s}` tem formato %q; o esperado é `0` seguido da largura, como `02`", mold, body, spec)
	}
	n, err := strconv.Atoi(spec[1:])
	if err != nil || n < 1 || n > maxPad {
		return item{}, fmt.Errorf("keyFormat %q: `{%s}` pede largura %q fora de [1, %d]", mold, body, spec[1:], maxPad)
	}
	it.pad = n
	return it, nil
}

// checkColumns é a regra que fecha a ambiguidade pela raiz: coluna do groupBy
// que não entra no texto faria dois grupos distintos renderem o mesmo groupKey.
func (f *Format) checkColumns(cols []Column, used []int) error {
	for j := range cols {
		switch {
		case used[j] == 0:
			return fmt.Errorf("keyFormat %q não usa a coluna %q do groupBy — dois grupos distintos renderiam o mesmo groupKey", f.mold, cols[j].Name)
		case used[j] > 1:
			return fmt.Errorf("keyFormat %q usa a coluna %q %d vezes; cada coluna do groupBy aparece exatamente uma vez", f.mold, cols[j].Name, used[j])
		}
	}
	return nil
}

// checkLiterals confere os literais e, de passagem, resolve o caractere que
// cada componente string tem de escapar.
func (f *Format) checkLiterals(cols []Column) error {
	if err := f.checkLiteral(f.prefix, "o literal antes do primeiro marcador"); err != nil {
		return err
	}
	for k := range f.items {
		it := &f.items[k]
		onde := fmt.Sprintf("o literal depois de `{%s}`", cols[it.col].Name)
		if err := f.checkLiteral(it.after, onde); err != nil {
			return err
		}
		if len(it.after) == 0 {
			if k != len(f.items)-1 {
				return fmt.Errorf("keyFormat %q tem dois marcadores colados depois de `{%s}` — sem literal entre eles, não há como saber onde um componente acaba e o outro começa", f.mold, cols[it.col].Name)
			}
			continue
		}
		if b := it.after[0]; cols[it.col].Type == types.TypeInt64 && (b == '-' || (b >= '0' && b <= '9')) {
			return fmt.Errorf("keyFormat %q: %s começa com %q, e `{%s}` é int64 — o número não teria fim reconhecível", f.mold, onde, string(b), cols[it.col].Name)
		}
		it.esc = it.after[0]
	}
	return nil
}

func (f *Format) checkLiteral(lit []byte, onde string) error {
	for _, b := range lit {
		switch {
		case b == '\\':
			return fmt.Errorf("keyFormat %q: %s contém `\\`, que é o caractere de escape do texto — dois grupos distintos renderiam o mesmo groupKey", f.mold, onde)
		case b < 0x20 || b == 0x7f:
			return fmt.Errorf("keyFormat %q: %s contém o caractere de controle %#x — o groupKey ficaria impossível de escrever num WHERE", f.mold, onde, b)
		}
	}
	return nil
}

func namesOf(cols []Column) string {
	out := make([]string, len(cols))
	for j := range cols {
		out[j] = cols[j].Name
	}
	return strings.Join(out, ", ")
}

// Append renderiza o texto de um grupo. vals vem na ordem DECLARADA do groupBy;
// o molde é quem decide a ordem de saída.
//
// dst é do chamador e o retorno tem de ser reatribuído: a função roda a cada
// grupo NOVO no writer e em toda a abertura do dataset, e não pode alocar.
func (f *Format) Append(dst []byte, vals []types.Value) ([]byte, error) {
	if len(vals) != len(f.items) {
		return dst, fmt.Errorf("keyformat: %d valores para um molde de %d componentes", len(vals), len(f.items))
	}
	dst = append(dst, f.prefix...)
	for k := range f.items {
		it := &f.items[k]
		v := vals[it.col]
		switch v.Type {
		case types.TypeString:
			for _, b := range v.Bytes {
				if b == '\\' || (it.esc != 0 && b == it.esc) {
					dst = append(dst, '\\')
				}
				dst = append(dst, b)
			}
		case types.TypeInt64:
			dst = appendInt(dst, v.I64, it.pad)
		case types.TypeBool:
			if v.I64 != 0 {
				dst = append(dst, "true"...)
			} else {
				dst = append(dst, "false"...)
			}
		default:
			return dst, fmt.Errorf("keyformat: componente %d tem tipo %#x, sem renderização (groupBy aceita string, int64 e boolean)", it.col, v.Type)
		}
		dst = append(dst, it.after...)
	}
	return dst, nil
}

// appendInt escreve o decimal, com zeros à esquerda até width se declarado. A
// largura conta o sinal, como o %0Nd do fmt. Valor mais largo que width sai
// inteiro: truncar perderia informação, e o zero à esquerda é cosmético.
func appendInt(dst []byte, n int64, width int) []byte {
	if width == 0 {
		return strconv.AppendInt(dst, n, 10)
	}
	var buf [maxPad]byte
	digits := strconv.AppendInt(buf[:0], n, 10)
	if len(digits) >= width {
		return append(dst, digits...)
	}
	zeros := width - len(digits)
	if digits[0] == '-' {
		dst, digits = append(dst, '-'), digits[1:]
	}
	for ; zeros > 0; zeros-- {
		dst = append(dst, '0')
	}
	return append(dst, digits...)
}
