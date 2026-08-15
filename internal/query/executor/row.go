package executor

import "github.com/nishisan-dev/nishidb/internal/types"

// ColView é uma coluna projetada de UMA row.
//
// Val.Bytes é ZERO-COPY sobre o buffer interno do iterador: vale somente até o
// próximo Next(). Quem precisar reter copia.
type ColView struct {
	// Name é o nome de SAÍDA: o alias quando existe, senão o nome original.
	Name string
	// Type é o tipo DECLARADO no schema, constante por query — serve a
	// metadados (ResultSetMetaData), não à formatação. É types.TypeNull quando
	// a coluna é um campo auto que o dataset nunca observou.
	Type types.FieldType
	// Null indica campo AUSENTE da row (D2: NULL é ausência, não um valor).
	Null bool
	// Val é o valor lido do payload; zero quando Null. Val.Type é o tipo
	// FÍSICO gravado, e é ele que o encoder usa para formatar.
	Val types.Value
}

// ColMeta é o metadado ESTÁVEL de uma coluna projetada: o que vale para a
// query inteira, e não para uma row.
//
// É tipo próprio, e não uma ColView com os campos de valor zerados, porque
// ColView carrega Val/Null válidos SOMENTE durante uma row — entregá-la como
// metadado convidaria o chamador a ler um valor que não existe mais.
type ColMeta struct {
	// Name é o nome de SAÍDA: o alias quando existe, senão o nome original.
	Name string
	// Type é o tipo DECLARADO no schema. É types.TypeNull quando a coluna é um
	// campo auto que o dataset nunca observou — o tipo não é desconhecido por
	// falha, é desconhecido porque nenhum evento o fixou ainda.
	Type types.FieldType
}

// Row é a visão de uma linha do resultado. É barata de copiar, mas o slice
// interno é REUTILIZADO a cada Next: copiar a Row não desacopla os valores.
type Row struct{ cols []ColView }

// Len é o número de colunas projetadas.
func (r Row) Len() int { return len(r.cols) }

// Col devolve a coluna i na ordem de saída.
func (r Row) Col(i int) ColView { return r.cols[i] }

// Cols expõe as colunas para leitura em lote (o encoder NDJSON as percorre).
// Somente leitura: escrever aqui corrompe a row seguinte.
func (r Row) Cols() []ColView { return r.cols }
