package protocolo_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A tabela do §73, transcrita LITERALMENTE. É o contrato que o
// ResultSetMetaData do driver vai publicar, e uma linha errada aqui vira um
// getInt() sobre uma coluna que o servidor chamou de VARCHAR.
func TestATabelaDeTiposDoSpec73(t *testing.T) {
	casos := []struct {
		ft   types.FieldType
		tipo string
		jdbc string
	}{
		{types.TypeString, "string", protocolo.JDBCVarchar},
		{types.TypeInt64, "int64", protocolo.JDBCBigint},
		{types.TypeDouble, "double", protocolo.JDBCDouble},
		{types.TypeBool, "boolean", protocolo.JDBCBoolean},
		{types.TypeTimestamp, "timestamp", protocolo.JDBCTimestamp},
		{types.TypeIPv4, "ipv4", protocolo.JDBCVarchar},
		{types.TypeIPv6, "ipv6", protocolo.JDBCVarchar},
		{types.TypeIPv4Prefix, "ipv4_prefix", protocolo.JDBCVarchar},
		{types.TypeIPv6Prefix, "ipv6_prefix", protocolo.JDBCVarchar},
		{types.TypeBytes, "bytes", protocolo.JDBCVarbinary},
		{types.TypeJSON, "json", protocolo.JDBCVarchar},
	}
	for _, c := range casos {
		t.Run(c.tipo, func(t *testing.T) {
			col := protocolo.ColunaDe("x", c.ft)
			if col.Type != c.tipo {
				t.Fatalf("Type = %q, quer %q", col.Type, c.tipo)
			}
			if col.JDBC != c.jdbc {
				t.Fatalf("JDBC = %q, quer %q", col.JDBC, c.jdbc)
			}
		})
	}
}

// Campo auto que o dataset nunca observou não tem tipo. Chamá-lo de VARCHAR
// seria mentir para o driver: o próximo evento pode fixá-lo como int64.
func TestCampoAutoNaoObservadoSaiComoUnknown(t *testing.T) {
	col := protocolo.ColunaDe("attr", types.TypeNull)
	if col.Type != protocolo.TipoDesconhecido {
		t.Fatalf("Type = %q, quer %q", col.Type, protocolo.TipoDesconhecido)
	}
	if col.JDBC != protocolo.JDBCOther {
		t.Fatalf("JDBC = %q, quer %q", col.JDBC, protocolo.JDBCOther)
	}
}

func TestTagDesconhecidaNaoViraTipoPlausivel(t *testing.T) {
	col := protocolo.ColunaDe("x", types.FieldType(0xEE))
	if col.Type != protocolo.TipoDesconhecido || col.JDBC != protocolo.JDBCOther {
		t.Fatalf("coluna = %+v, quer unknown/OTHER", col)
	}
}
