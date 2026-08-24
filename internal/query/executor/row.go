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
	// Nullable é o metadado do CAMPO, não da row: false só para a chave e
	// para a sequence. Vive aqui junto de Name e Type, que também são
	// constantes por query, e pela mesma razão que eles — o slice de ColView
	// é alocado uma vez no Open, e carregar o flag aqui evita um segundo
	// slice por consulta só para respondê-lo em Columns().
	Nullable bool
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
	// Nullable é o único campo deste metadado que NÃO é função do tipo: é
	// função do papel do campo. false para a chave e para a sequence, true
	// para todo o resto.
	//
	// A semântica não é uma convenção de conveniência, é o que o produto
	// garante: o materializer RECUSA o evento cuja chave venha ausente, nula
	// ou vazia, e cuja sequence venha ausente ou nula, então nenhuma row viva
	// tem nulo nessas duas colunas. É por isso que o flag precisa vir DAQUI e
	// não ser derivado no ponto de serialização: só o executor tem o
	// *schema.CompiledField, e sem esse canal o servidor teria de re-resolver
	// nome-de-saída para campo, refazendo a resolução de alias que o binder já
	// fez.
	//
	// O campo de sistema _writtenAt é nullable: ele só passa a existir quando
	// o dataset liga expireAfterWrite, e as rows gravadas antes disso não o
	// têm.
	Nullable bool
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
