// Package parser converte o subset SQL v1 do NishiDB em AST, por descida
// recursiva LL(1) sobre os tokens do lexer (spec §59-66).
//
// O parser é SINTÁTICO: não conhece schema, tipos nem storage. Coluna que não
// existe, literal incomparável com o tipo da coluna e dataset desconhecido são
// erros do binder (Q13) — aqui, `SELECT inexistente FROM nada` é uma query
// perfeitamente válida.
//
// A AST é imutável por convenção e segura para compartilhar entre goroutines:
// as strings são fatias de uma string Go, e Parse é puro.
package parser

// SelectStmt é a raiz da AST.
//
// HasLimit distingue os dois estados que um int sozinho confundiria: LIMIT
// ausente e LIMIT 0 (zero linhas) são coisas diferentes, e usar -1 como
// sentinela obrigaria todo consumidor a lembrar disso.
type SelectStmt struct {
	Star       bool        // true ⇒ Projection == nil
	Projection []ColumnRef // nomes como escritos, já unescapados; nil quando Star
	// Namespace é o qualificador antes do ponto (`_meta` em `_meta.fields`),
	// vazio quando o FROM traz um nome simples. É ADITIVO: nenhuma query
	// existente muda de forma, e o campo zero significa "sem namespace".
	Namespace string
	// Dataset é sempre o nome DEPOIS do ponto, quando há ponto. DatasetPos
	// aponta para ele, e não para o qualificador: é dele que sai o
	// "dataset desconhecido" do binder.
	Dataset    string
	DatasetPos int
	Where      Expr  // nil se ausente
	HasLimit   bool  // LIMIT presente na query
	Limit      int64 // válido só com HasLimit; sempre >= 0
	LimitPos   int   // posição do literal do LIMIT; válido só com HasLimit
	ParamCount int
}

// ColumnRef é um item da projeção com a posição de byte do token no SQL — é
// dela que sai o "coluna desconhecida %q na posição %d" do binder.
type ColumnRef struct {
	// Qualifier é o campo json à esquerda do ponto (`doc` em `doc.x`), vazio
	// na projeção simples. Com ele, Name é o CAMINHO dentro do documento, e
	// não o nome da coluna — quem resolve um no outro é o binder.
	Qualifier string
	Name      string
	// Star marca `pai.*`: expande as colunas extraídas daquele pai. Elas
	// ficam fora do SELECT *, então esta é a forma de pedi-las sem saber os
	// nomes. Com Star, Name é vazio.
	Star bool
	Pos  int
}

// Expr é selada: só os três nós deste pacote a implementam, então um type
// switch exaustivo no binder e no planner é suficiente e à prova de futuro.
type Expr interface{ isExpr() }

// LogicalExpr é um conectivo AND/OR. Cadeias são LEFT-LEANING
// (AND(AND(a,b),c)), o que o flatten do planner percorre pela espinha à
// esquerda.
type LogicalExpr struct {
	Op          LogicalOp
	Left, Right Expr
}

// Comparison é `coluna <op> literal`. Comparação coluna×coluna e literal à
// esquerda não existem na v1.
type Comparison struct {
	// Qualifier é o campo json à esquerda do ponto; vazio no filtro simples.
	// Com ele, Column é o CAMINHO dentro do documento, como na projeção.
	Qualifier string
	Column    string
	ColumnPos int // posição de byte do token da COLUNA
	Op        CmpOp
	Operand   Operand // Operand.Pos traz a posição do literal/placeholder
}

// NullCheck é `coluna IS [NOT] NULL`. NULL é ausência do campo no record
// (decisão D2), então isto vira teste de presença no executor.
type NullCheck struct {
	Qualifier string
	Column    string
	ColumnPos int
	Not       bool
}

// LikeExpr é `coluna [NOT] LIKE <padrão>`.
//
// É um NÓ e não um CmpOp novo, e a escolha não é estética. As três funções de
// ordem do executor (ordemInt, ordemFloat, ordemBytes) terminam em
// `default: // parser.CmpLte`: um CmpLike que chegasse ali seria avaliado como
// `<=`, silenciosamente, com a suíte inteira verde. Já os `default` de `folha`
// (binder) e `emite` (executor) DEVOLVEM ERRO para nó desconhecido. Nó novo cai
// no caminho barulhento; operador novo, no silencioso.
//
// O padrão viaja como Operand porque `LIKE ?` precisa valer: o binder recebe os
// parâmetros e compila o padrão no bind, nunca por linha.
type LikeExpr struct {
	Qualifier string
	Column    string
	ColumnPos int
	Not       bool
	Pattern   Operand
}

// Operand é o literal já DECODIFICADO (nunca o texto cru): o binder coere a
// partir daqui, sem reparsear.
type Operand struct {
	Kind     OperandKind
	S        string  // OperandString
	I        int64   // OperandInt
	F        float64 // OperandFloat
	B        bool    // OperandBool
	ParamIdx int     // OperandParam: índice 0-based
	Pos      int     // posição do token do literal/placeholder
}

// LogicalOp e as demais constantes começam em 1: o zero value é inválido de
// propósito, para que um nó mal construído apareça no teste em vez de passar
// como "AND".
type LogicalOp uint8

const (
	LogicalAnd LogicalOp = iota + 1
	LogicalOr
)

type CmpOp uint8

const (
	CmpEq CmpOp = iota + 1
	CmpNe
	CmpGt
	CmpGte
	CmpLt
	CmpLte
)

type OperandKind uint8

const (
	OperandString OperandKind = iota + 1
	OperandInt
	OperandFloat
	OperandBool
	OperandParam
)

// String devolve "AND" ou "OR".
func (op LogicalOp) String() string {
	switch op {
	case LogicalAnd:
		return "AND"
	case LogicalOr:
		return "OR"
	default:
		return "LogicalOp(?)"
	}
}

// String devolve o operador SQL, para o Plan.String() do planner.
func (op CmpOp) String() string {
	switch op {
	case CmpEq:
		return "="
	case CmpNe:
		return "!="
	case CmpGt:
		return ">"
	case CmpGte:
		return ">="
	case CmpLt:
		return "<"
	case CmpLte:
		return "<="
	default:
		return "CmpOp(?)"
	}
}

// String devolve o vocabulário de tipos da tabela de coerção do binder.
func (k OperandKind) String() string {
	switch k {
	case OperandString:
		return "string"
	case OperandInt:
		return "int"
	case OperandFloat:
		return "float"
	case OperandBool:
		return "bool"
	case OperandParam:
		return "param"
	default:
		return "OperandKind(?)"
	}
}

func (*LogicalExpr) isExpr() {}
func (*Comparison) isExpr()  {}
func (*NullCheck) isExpr()   {}
func (*LikeExpr) isExpr()    {}
