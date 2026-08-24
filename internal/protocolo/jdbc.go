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
	// JDBCTime é java.sql.Types.TIME: hora do dia sem data e sem fuso. Não é
	// TIMESTAMP porque o driver montaria um java.sql.Timestamp em 1970 para
	// um valor que nunca foi um instante.
	JDBCTime = "TIME"
	// JDBCOther é o destino da coluna cujo tipo o dataset ainda não fixou.
	// Chamá-la de VARCHAR seria mentir para o driver: ele criaria um
	// ResultSetMetaData afirmando String para uma coluna que pode vir a ser
	// int64 no evento seguinte.
	JDBCOther = "OTHER"
)

// DescritorJDBC é tudo o que o ResultSetMetaData do §71 precisa saber sobre um
// tipo — e nada além.
//
// Os três números NÃO vêm do §73: a spec não diz "precision" em lugar nenhum.
// São decisão deste projeto, tomada olhando para o consumidor. O DBeaver usa
// DisplaySize para dimensionar a coluna do grid ANTES da primeira row (é a
// única largura que ele tem enquanto o stream não começou) e usa
// Precision/Scale para escrever o tipo no painel de metadados e no DDL do
// "Generate SQL".
//
// A semântica é a do javadoc de java.sql.ResultSetMetaData, não uma nossa:
// Precision em dígitos decimais para numérico e em caracteres para texto e
// data/hora; Scale em dígitos à direita do ponto (para data/hora, os dígitos
// de fração de segundo); DisplaySize em largura NORMAL máxima — "normal", e
// não teto duro, que é o que permite publicar 24 para o timestamp sabendo que
// um ano fora de [0,9999] sairia mais largo.
type DescritorJDBC struct {
	JDBC        string
	Precision   int
	Scale       int
	DisplaySize int
}

// DescritorDeTipo é a tabela do §73 mais os três números do ResultSetMetaData,
// e é a ÚNICA no produto.
//
// Mora aqui, e não no servidor, porque tem dois consumidores que precisam
// concordar: a linha de schema de /v1/query e o corpo de /v1/metadata. Duas
// cópias divergiriam no dia em que um tipo novo entrasse, e o sintoma seria um
// driver que descreve a mesma coluna de dois jeitos conforme a rota que
// consultou. Os quatro campos saem do MESMO switch pela mesma razão, um nível
// abaixo: três funções paralelas poderiam discordar entre si sem nada acusar.
//
// O switch é exaustivo de propósito, sem default silencioso para os tipos
// conhecidos: acrescentar um FieldType sem passar por aqui tem de ser uma
// decisão, não um esquecimento que sai como OTHER em produção.
//
// AS LARGURAS SÃO MEDIDAS, NÃO CONSULTADAS. Cada DisplaySize abaixo é o que o
// encoder de fato emite para o valor mais largo do tipo, e
// TestDisplaySizeDeCadaTipoCasaComALarguraQueOEncoderEmite renderiza esse
// valor por executor.AppendValue e compara. É o que separa esta tabela de uma
// opinião — e a opinião erra: o número de manual para ipv6 é 45, e o certo
// aqui é 39 (ver o caso).
func DescritorDeTipo(t types.FieldType) DescritorJDBC {
	switch t {
	case types.TypeInt64:
		// 19 dígitos em MaxInt64; o display inclui o sinal de MinInt64.
		return DescritorJDBC{JDBCBigint, 19, 0, 20}

	case types.TypeDouble:
		// 15 é o DBL_DIG: os dígitos decimais que o binary64 garante. O
		// display é a forma mais larga que AppendFloat(_, 'g', -1, 64)
		// produz, que é -1.7976931348623157e+308.
		return DescritorJDBC{JDBCDouble, 15, 0, 24}

	case types.TypeBool:
		// "false" é mais largo que "true".
		return DescritorJDBC{JDBCBoolean, 1, 0, 5}

	case types.TypeTimestamp, types.TypeDateTime:
		// datetime é um instante depois da ingestão, e sai no fio byte a byte
		// como o timestamp: o driver não tem por que distinguir. Os dois
		// medem 2026-08-23T21:53:04.123Z = 24, e a fração é SEMPRE de três
		// dígitos (append3), que é de onde vem o scale 3 — é ele que faz o
		// DBeaver escrever TIMESTAMP(3) em vez de TIMESTAMP.
		return DescritorJDBC{JDBCTimestamp, 24, 3, 24}

	case types.TypeTime:
		// 23:59:59.999, sem Z: não há instante aqui, e não há fuso a anunciar.
		return DescritorJDBC{JDBCTime, 12, 3, 12}

	// Os cinco casos abaixo saíam agrupados numa cláusula só, quando a única
	// pergunta era o nome JDBC e os cinco respondiam VARCHAR. Com as larguras
	// eles DEIXAM DE SER IGUAIS, e manter o agrupamento publicaria precision 0
	// para todo endereço IP — em silêncio, porque o nome JDBC continuaria
	// certo. Um caso por tipo, e o teste cobra cada um contra o encoder.
	case types.TypeString:
		// Sem teto declarado: o schema não diz comprimento de coluna. 0 é a
		// convenção do JDBC para desconhecido, e é melhor que
		// Integer.MAX_VALUE — com o MAX_VALUE o DBeaver escreve
		// VARCHAR(2147483647) no DDL gerado, que nenhum destino aceita.
		return DescritorJDBC{JDBCVarchar, 0, 0, 0}

	case types.TypeJSON:
		// Idem string. E o json sai INLINE no NDJSON, não como string: mesmo
		// que houvesse teto, ele seria do documento, não da coluna.
		return DescritorJDBC{JDBCVarchar, 0, 0, 0}

	case types.TypeBytes:
		// Idem. Aqui há uma razão extra para não inventar teto: o wire é
		// base64, então precision (que o javadoc mede em BYTES para binário) e
		// displaySize (em caracteres) teriam grandezas diferentes.
		return DescritorJDBC{JDBCVarbinary, 0, 0, 0}

	case types.TypeIPv4:
		// 255.255.255.255
		return DescritorJDBC{JDBCVarchar, 15, 0, 15}

	case types.TypeIPv6:
		// 39, e não os 45 do manual. Os 45 são a forma mista
		// x:x:x:x:x:x:d.d.d.d, que netip.Addr.AppendTo só usa para endereço
		// 4-in-6 — e essa forma é MAIS CURTA (::ffff:255.255.255.255 = 22). O
		// pior caso do que sai do nosso fio é ffff:...:ffff = 8*4 + 7.
		return DescritorJDBC{JDBCVarchar, 39, 0, 39}

	case types.TypeIPv4Prefix:
		// 15 + "/32"
		return DescritorJDBC{JDBCVarchar, 18, 0, 18}

	case types.TypeIPv6Prefix:
		// 39 + "/128". Sem Masked() no encoder, os host bits ficam — não
		// encurta.
		return DescritorJDBC{JDBCVarchar, 43, 0, 43}

	case types.TypeNull:
		return DescritorJDBC{JDBCOther, 0, 0, 0}

	default:
		// Não sabemos o tipo; muito menos a largura. Um número plausível aqui
		// seria pior que zero — o driver dimensionaria a coluna por ele.
		return DescritorJDBC{JDBCOther, 0, 0, 0}
	}
}

// JDBCDeTipo é a tabela do §73. Casca fina sobre DescritorDeTipo, de propósito:
// os chamadores que só querem o nome não precisam saber das larguras, e um
// switch paralelo aqui é exatamente a divergência que o descritor único evita.
func JDBCDeTipo(t types.FieldType) string {
	return DescritorDeTipo(t).JDBC
}

// ColunaDe monta a descrição de uma coluna a partir do nome de saída, do tipo
// declarado e da nulabilidade.
//
// Um DescritorDeTipo por coluna, e não por row: isto roda uma vez por consulta,
// no momento de escrever a linha de schema, fora do laço que emite as linhas de
// dados. É por isso que executor.ColMeta não precisa carregar os três números —
// derivá-los aqui não custa alocação nenhuma no caminho quente.
//
// nullable é PARÂMETRO e não derivação porque é a única coisa aqui que não sai
// do tipo: sai do papel do campo no dataset, que só o executor conhece. Quem
// chama passa executor.ColMeta.Nullable.
func ColunaDe(nome string, t types.FieldType, nullable bool) Coluna {
	d := DescritorDeTipo(t)
	return Coluna{
		Name:        nome,
		Type:        NomeDeTipo(t),
		JDBC:        d.JDBC,
		Nullable:    nullable,
		Precision:   d.Precision,
		Scale:       d.Scale,
		DisplaySize: d.DisplaySize,
	}
}
