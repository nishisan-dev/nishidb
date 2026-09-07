package protocol_test

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A tabela do §73, transcrita LITERALMENTE. É o contrato que o
// ResultSetMetaData do driver vai publicar, e uma linha errada aqui vira um
// getInt() sobre uma coluna que o servidor chamou de VARCHAR.
func TestATabelaDeTiposDoSpec73(t *testing.T) {
	cases := []struct {
		ft       types.FieldType
		typeName string
		jdbc     string
	}{
		{types.TypeString, "string", protocol.JDBCVarchar},
		{types.TypeInt64, "int64", protocol.JDBCBigint},
		{types.TypeDouble, "double", protocol.JDBCDouble},
		{types.TypeBool, "boolean", protocol.JDBCBoolean},
		{types.TypeTimestamp, "timestamp", protocol.JDBCTimestamp},
		{types.TypeIPv4, "ipv4", protocol.JDBCVarchar},
		{types.TypeIPv6, "ipv6", protocol.JDBCVarchar},
		{types.TypeIPv4Prefix, "ipv4_prefix", protocol.JDBCVarchar},
		{types.TypeIPv6Prefix, "ipv6_prefix", protocol.JDBCVarchar},
		{types.TypeBytes, "bytes", protocol.JDBCVarbinary},
		{types.TypeJSON, "json", protocol.JDBCVarchar},
		{types.TypeDateTime, "datetime", protocol.JDBCTimestamp},
		{types.TypeTime, "time", protocol.JDBCTime},
	}
	for _, c := range cases {
		t.Run(c.typeName, func(t *testing.T) {
			col := protocol.NewColumn("x", c.ft, true)
			if col.Type != c.typeName {
				t.Fatalf("Type = %q, quer %q", col.Type, c.typeName)
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
	col := protocol.NewColumn("attr", types.TypeNull, true)
	if col.Type != protocol.TypeUnknown {
		t.Fatalf("Type = %q, quer %q", col.Type, protocol.TypeUnknown)
	}
	if col.JDBC != protocol.JDBCOther {
		t.Fatalf("JDBC = %q, quer %q", col.JDBC, protocol.JDBCOther)
	}
}

func TestTagDesconhecidaNaoViraTipoPlausivel(t *testing.T) {
	col := protocol.NewColumn("x", types.FieldType(0xEE), true)
	if col.Type != protocol.TypeUnknown || col.JDBC != protocol.JDBCOther {
		t.Fatalf("coluna = %+v, quer unknown/OTHER", col)
	}
}

// O comentário de JDBCName promete que acrescentar um FieldType sem passar
// por ali "tem de ser uma decisão, não um esquecimento que sai como OTHER em
// produção". Nada cobrava essa promessa: a tabela acima é escrita à mão e não
// quebra quando uma tag entra. Este teste a cobra.
//
// O sintoma que ele previne é discreto: o driver publica OTHER, o
// ResultSetMetaData não sabe descrever a coluna, e quem usa a ferramenta vê
// um tipo genérico sem nenhum erro em lugar nenhum.
func TestTodaTagComNomeTemJDBCEspecifico(t *testing.T) {
	for tag := 0; tag < 0x20; tag++ {
		ft := types.FieldType(tag)
		name := ft.String()
		if strings.HasPrefix(name, "FieldType(") {
			continue // tag sem nome: não é tipo de campo declarável
		}
		if ft == types.TypeNull {
			continue // campo auto sem observação: OTHER é a resposta certa
		}
		if got := protocol.JDBCName(ft); got == protocol.JDBCOther {
			t.Errorf("tipo %q (0x%02x) sai como OTHER: acrescente a linha em JDBCName", name, tag)
		}
	}
}

// A tabela de precision/scale/displaySize, caso a caso.
//
// Estes números NÃO estão no §73 — a spec não diz "precision" em lugar
// nenhum. São decisão do produto, tomada olhando para o consumidor: o
// ResultSetMetaData que o DBeaver lê para dimensionar a coluna do grid antes
// da primeira row e para escrever o DDL do "Generate SQL". A semântica é a do
// javadoc: precision em dígitos para numérico, em caracteres para texto e
// data/hora; scale em dígitos de fração; displaySize em largura NORMAL máxima.
//
// Os de texto sem teto declarado valem 0, que é a convenção do JDBC para
// "desconhecido". Integer.MAX_VALUE seria pior que 0: o DBeaver escreveria
// VARCHAR(2147483647) no DDL gerado, que nenhum banco de destino aceita — e o
// schema do NishiDB não declara comprimento de coluna nenhum, então o número
// seria inventado.
func TestODescritorDeCadaTipoTemPrecisionScaleEDisplaySize(t *testing.T) {
	cases := []struct {
		ft                            types.FieldType
		jdbc                          string
		precision, scale, displaySize int
	}{
		{types.TypeInt64, protocol.JDBCBigint, 19, 0, 20},
		{types.TypeDouble, protocol.JDBCDouble, 15, 0, 24},
		{types.TypeBool, protocol.JDBCBoolean, 1, 0, 5},
		{types.TypeTimestamp, protocol.JDBCTimestamp, 24, 3, 24},
		{types.TypeDateTime, protocol.JDBCTimestamp, 24, 3, 24},
		{types.TypeTime, protocol.JDBCTime, 12, 3, 12},
		{types.TypeString, protocol.JDBCVarchar, 0, 0, 0},
		{types.TypeJSON, protocol.JDBCVarchar, 0, 0, 0},
		{types.TypeBytes, protocol.JDBCVarbinary, 0, 0, 0},
		{types.TypeIPv4, protocol.JDBCVarchar, 15, 0, 15},
		{types.TypeIPv6, protocol.JDBCVarchar, 39, 0, 39},
		{types.TypeIPv4Prefix, protocol.JDBCVarchar, 18, 0, 18},
		{types.TypeIPv6Prefix, protocol.JDBCVarchar, 43, 0, 43},
		{types.TypeNull, protocol.JDBCOther, 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(protocol.TypeName(c.ft), func(t *testing.T) {
			d := protocol.DescriptorOf(c.ft)
			if d.JDBC != c.jdbc {
				t.Errorf("JDBC = %q, quer %q", d.JDBC, c.jdbc)
			}
			if d.Precision != c.precision {
				t.Errorf("Precision = %d, quer %d", d.Precision, c.precision)
			}
			if d.Scale != c.scale {
				t.Errorf("Scale = %d, quer %d", d.Scale, c.scale)
			}
			if d.DisplaySize != c.displaySize {
				t.Errorf("DisplaySize = %d, quer %d", d.DisplaySize, c.displaySize)
			}
		})
	}
}

// O oráculo que faz o displaySize valer alguma coisa: o número publicado é
// medido contra o que o ENCODER de fato emite, não contra um manual.
//
// Sem ele a tabela acima é opinião — e a opinião erra. O ipv6 de manual é 45,
// que é a largura da forma mista x:x:x:x:x:x:d.d.d.d; o nosso encoder só usa
// forma mista para endereço 4-in-6, e essa é MAIS CURTA
// (::ffff:255.255.255.255 = 22). O que sai do fio no pior caso é ffff:...:ffff
// = 39, e é o que este teste prova.
//
// Só os tipos de largura limitada entram: string, json e bytes não têm teto
// declarado e publicam 0 por decisão.
func TestDisplaySizeDeCadaTipoCasaComALarguraQueOEncoderEmite(t *testing.T) {
	ipv6AllOnes := bytes.Repeat([]byte{0xff}, 16)
	cases := []struct {
		name string
		v    types.Value
	}{
		// MinInt64 e não MaxInt64: o display inclui o sinal.
		{"int64", types.Value{Type: types.TypeInt64, I64: math.MinInt64}},
		// O 'g' de -MaxFloat64 é a forma mais larga que AppendFloat produz.
		{"double", types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(-math.MaxFloat64))}},
		{"boolean", types.Value{Type: types.TypeBool, I64: 0}}, // "false" é mais largo que "true"
		{"timestamp", types.Value{Type: types.TypeTimestamp, I64: 1755990784123}},
		{"datetime", types.Value{Type: types.TypeDateTime, I64: 1755990784123}},
		{"time", types.Value{Type: types.TypeTime, I64: types.MillisPerDay - 1}},
		{"ipv4", types.Value{Type: types.TypeIPv4, Bytes: []byte{255, 255, 255, 255}}},
		{"ipv6", types.Value{Type: types.TypeIPv6, Bytes: ipv6AllOnes}},
		{"ipv4_prefix", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{255, 255, 255, 255, 32}}},
		{"ipv6_prefix", types.Value{Type: types.TypeIPv6Prefix, Bytes: append(append([]byte{}, ipv6AllOnes...), 128)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := executor.AppendValue(nil, c.v)
			width := len(out)
			if len(out) >= 2 && out[0] == '"' && out[len(out)-1] == '"' {
				width -= 2 // displaySize é do VALOR, não da grafia JSON
			}
			if got := protocol.DescriptorOf(c.v.Type).DisplaySize; got != width {
				t.Errorf("DisplaySize = %d, mas o encoder emitiu %d caracteres: %s", got, width, out)
			}
		})
	}
}

// Tag que não é tipo de campo não ganha número plausível. Um precision
// inventado para um tipo que não conhecemos seria pior que zero: o driver
// dimensionaria a coluna por ele.
func TestTipoSemNomeTemDescritorZerado(t *testing.T) {
	d := protocol.DescriptorOf(types.FieldType(0xEE))
	if d.JDBC != protocol.JDBCOther || d.Precision != 0 || d.Scale != 0 || d.DisplaySize != 0 {
		t.Fatalf("descritor = %+v, quer OTHER com 0/0/0", d)
	}
}
