package protocolo

import "github.com/nishisan-dev/nishidb/internal/types"

// Os nomes JDBC do §73. São os nomes de java.sql.Types, literais.
const (
	JDBCVarchar   = "VARCHAR"
	JDBCBigint    = "BIGINT"
	JDBCDouble    = "DOUBLE"
	JDBCBoolean   = "BOOLEAN"
	JDBCTimestamp = "TIMESTAMP"
	JDBCVarbinary = "VARBINARY"
	// JDBCOther é o destino da coluna cujo tipo o dataset ainda não fixou.
	// Chamá-la de VARCHAR seria mentir para o driver: ele criaria um
	// ResultSetMetaData afirmando String para uma coluna que pode vir a ser
	// int64 no evento seguinte.
	JDBCOther = "OTHER"
)

// JDBCDeTipo é a tabela do §73, e é a ÚNICA no produto.
//
// Mora aqui, e não no servidor, porque tem dois consumidores que precisam
// concordar: a linha de schema de /v1/query e o corpo de /v1/metadata. Duas
// cópias divergiriam no dia em que um tipo novo entrasse, e o sintoma seria um
// driver que descreve a mesma coluna de dois jeitos conforme a rota que
// consultou.
//
// O switch é exaustivo de propósito, sem default silencioso para os tipos
// conhecidos: acrescentar um FieldType sem passar por aqui tem de ser uma
// decisão, não um esquecimento que sai como OTHER em produção.
func JDBCDeTipo(t types.FieldType) string {
	switch t {
	case types.TypeString, types.TypeIPv4, types.TypeIPv6,
		types.TypeIPv4Prefix, types.TypeIPv6Prefix, types.TypeJSON:
		// Os cinco saem como VARCHAR porque é assim que o §73 os mapeia, e é
		// coerente com o encoder: o NDJSON já os emite como string.
		return JDBCVarchar
	case types.TypeInt64:
		return JDBCBigint
	case types.TypeDouble:
		return JDBCDouble
	case types.TypeBool:
		return JDBCBoolean
	case types.TypeTimestamp:
		return JDBCTimestamp
	case types.TypeBytes:
		return JDBCVarbinary
	case types.TypeNull:
		return JDBCOther
	default:
		return JDBCOther
	}
}

// ColunaDe monta a descrição de uma coluna a partir do nome de saída e do tipo
// declarado.
func ColunaDe(nome string, t types.FieldType) Coluna {
	return Coluna{Name: nome, Type: NomeDeTipo(t), JDBC: JDBCDeTipo(t)}
}
