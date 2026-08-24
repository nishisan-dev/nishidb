package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * O driver contra o BINÁRIO REAL, servindo um dataset com um campo de cada
 * tipo.
 *
 * <p>Os testes unitários provam que o driver faz o que quem o escreveu ACHA que
 * o servidor emite — eles falam com um servidor HTTP de mentira, com respostas
 * escritas à mão. Estes fecham a outra metade: se o fio suposto divergir do fio
 * real, é aqui que aparece.
 *
 * <p>O ambiente vem de {@code scripts/jdbc-it.sh}. A condição de ambiente
 * existe para que um {@code mvnw test} avulso continue funcionando sem
 * servidor; o que impede o SKIP de virar um verde mentiroso é
 * {@link IntegracaoObrigatoriaTest}, que NÃO é condicionado.
 */
@EnabledIfEnvironmentVariable(named = "NISHIDB_IT_URL", matches = ".+")
class DriverIT {

    private static Connection conecta() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", IntegracaoObrigatoriaTest.SENHA);
        return DriverManager.getConnection(IntegracaoObrigatoriaTest.URL, p);
    }

    /**
     * A matriz de conversão sobre os BYTES QUE O SERVIDOR DE FATO EMITE.
     *
     * <p>A linha de borda foi ingerida com os valores mais incômodos de cada
     * tipo: int64 no limite, double na grafia 'g' do Go, endereço no pior caso,
     * prefixo COM host bits (o encoder não mascara), json aninhado e os dois
     * formatos temporais do schema.
     */
    @Test
    void aMatrizDeConversaoSobreOFioReal() throws SQLException {
        try (Connection c = conecta();
                PreparedStatement st = c.prepareStatement(
                        "SELECT id, texto, inteiro, real, booleano, quando, v4, v6, rede4, rede6, "
                                + "cru, doc, momento, momento_iso, hora, hora_iso "
                                + "FROM todos_os_tipos WHERE id = ?")) {
            st.setString(1, "borda-1");
            try (ResultSet rs = st.executeQuery()) {
                assertTrue(rs.next(), "a linha de borda não voltou do servidor");

                assertEquals("linha com \"aspas\" e \\barra", rs.getString("texto"));
                assertEquals(Long.MAX_VALUE, rs.getLong("inteiro"));
                assertEquals(Double.doubleToLongBits(1e-07),
                        Double.doubleToLongBits(rs.getDouble("real")));
                // A grafia 'g' do Go, verbatim: é o que torna getString lossless.
                assertEquals("1e-07", rs.getString("real"));
                assertTrue(rs.getBoolean("booleano"));

                assertEquals(1787570000123L, rs.getTimestamp("quando").getTime());
                assertEquals(Instant.ofEpochMilli(1787570000123L),
                        rs.getObject("quando", Instant.class));

                assertEquals("255.255.255.255", rs.getString("v4"));
                assertEquals("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", rs.getString("v6"));
                // SEM Masked(): o host bit sobrevive ao round-trip.
                assertEquals("10.0.0.1/24", rs.getString("rede4"));
                assertEquals("2001:db8::1/32", rs.getString("rede6"));

                assertArrayEqualsHello(rs.getBytes("cru"));
                // json sai INLINE e verbatim, não como string escapada.
                assertEquals("{\"a\":[1,2,{\"b\":null}]}", rs.getString("doc"));

                // Os dois formatos temporais resolvem para o MESMO instante:
                // datetime é wall clock resolvido no fuso do host do servidor.
                assertEquals(rs.getTimestamp("momento").getTime(),
                        rs.getTimestamp("momento_iso").getTime());
                // A precisão da hora é do FORMATO declarado no schema, não do
                // tipo: `hora` é "hh:mm:ss a" e não tem onde guardar a fração,
                // enquanto `hora_iso` é "HH:mm:ss.SSS" e tem. Os dois campos
                // receberam o mesmo instante na ingestão, e o que os separa é o
                // formato — não o driver.
                assertEquals(LocalTime.of(23, 59, 59), rs.getObject("hora", LocalTime.class));
                assertEquals(LocalTime.of(23, 59, 59, 999_000_000),
                        rs.getObject("hora_iso", LocalTime.class));

                assertFalse(rs.next());
            }
        }
    }

    private static void assertArrayEqualsHello(byte[] b) {
        assertEquals("Hello", new String(b, java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * O ResultSetMetaData vem do servidor, com os números que o descritor único
     * calcula — o driver não os recalcula.
     */
    @Test
    void osNumerosDoMetadataVemDoServidor() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT id, inteiro, quando, hora, v6, cru FROM todos_os_tipos LIMIT 1")) {
            ResultSetMetaData m = rs.getMetaData();
            assertEquals(6, m.getColumnCount());
            assertEquals(Types.VARCHAR, m.getColumnType(1));
            // id é a chave do dataset: é a única coluna que o produto garante
            // não-nula, e o servidor a publica assim.
            assertEquals(ResultSetMetaData.columnNoNulls, m.isNullable(1));

            assertEquals(Types.BIGINT, m.getColumnType(2));
            assertEquals(19, m.getPrecision(2));
            assertEquals(20, m.getColumnDisplaySize(2));

            assertEquals(Types.TIMESTAMP, m.getColumnType(3));
            assertEquals(3, m.getScale(3), "a fração de segundo é sempre de três dígitos");

            assertEquals(Types.TIME, m.getColumnType(4));
            assertEquals(12, m.getColumnDisplaySize(4));

            // 39, e não os 45 do manual: o encoder nunca usa a forma mista
            // longa, e este número foi MEDIDO contra ele.
            assertEquals(39, m.getPrecision(5));

            assertEquals(Types.VARBINARY, m.getColumnType(6));
        }
    }

    /**
     * A sequência que uma FERRAMENTA faz ao abrir a conexão, na ordem em que
     * ela faz.
     *
     * <p>Não substitui o DBeaver — ele tem suposições de interface que só
     * aparecem na tela —, mas faz a maioria das surpresas aparecer aqui, onde o
     * diagnóstico custa um stack trace em vez de uma sessão gráfica.
     */
    @Test
    void aSequenciaQueUmaFerramentaFazAoAbrirAConexao() throws SQLException {
        try (Connection c = conecta()) {
            DatabaseMetaData m = c.getMetaData();
            assertEquals("NishiDB", m.getDatabaseProductName());
            assertNotNull(m.getDatabaseProductVersion());
            assertTrue(m.getDatabaseProductVersion().startsWith("v"),
                    "a versão do processo devia vir do /v1/stats: " + m.getDatabaseProductVersion());
            assertEquals("\"", m.getIdentifierQuoteString());
            assertFalse(m.supportsTransactions());

            // Catálogo e schema: vazios, mas com a forma certa.
            try (ResultSet rs = m.getCatalogs()) {
                assertEquals(1, rs.getMetaData().getColumnCount());
            }
            try (ResultSet rs = m.getSchemas()) {
                assertFalse(rs.next());
            }
            try (ResultSet rs = m.getTableTypes()) {
                assertTrue(rs.next());
                assertEquals("TABLE", rs.getString(1));
            }

            List<String> tabelas = new ArrayList<>();
            try (ResultSet rs = m.getTables(null, null, null, null)) {
                while (rs.next()) {
                    tabelas.add(rs.getString("TABLE_NAME"));
                }
            }
            assertTrue(tabelas.contains("todos_os_tipos"), "getTables não listou o dataset: " + tabelas);

            // getColumns publica o nome do CAMPO; o alias vai no REMARKS. O
            // dataset tem um campo com alias justamente para provar isso.
            boolean viuAlias = false;
            int colunas = 0;
            try (ResultSet rs = m.getColumns(null, null, "todos_os_tipos", null)) {
                while (rs.next()) {
                    colunas++;
                    if ("texto".equals(rs.getString("COLUMN_NAME"))) {
                        viuAlias = rs.getString("REMARKS").contains("alias=t");
                    }
                }
            }
            assertTrue(colunas > 10, "getColumns devolveu " + colunas + " colunas");
            assertTrue(viuAlias, "o alias do campo não apareceu no REMARKS de getColumns");

            try (ResultSet rs = m.getPrimaryKeys(null, null, "todos_os_tipos")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
            }
            try (ResultSet rs = m.getIndexInfo(null, null, "todos_os_tipos", false, true)) {
                assertTrue(rs.next(), "o dataset declara índice em dois campos");
            }
            try (ResultSet rs = m.getTypeInfo()) {
                assertTrue(rs.next(), "getTypeInfo saiu vazio");
            }

            // E então a consulta, com o grid lendo célula a célula por getObject
            // e conferindo nulo — que é o padrão da ferramenta.
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM todos_os_tipos LIMIT 5")) {
                ResultSetMetaData rm = rs.getMetaData();
                int lidas = 0;
                while (rs.next()) {
                    lidas++;
                    for (int i = 1; i <= rm.getColumnCount(); i++) {
                        rs.getObject(i);
                        rs.wasNull();
                        rs.getString(i);
                    }
                }
                assertEquals(5, lidas);
            }
        }
    }

    /**
     * O nome de saída do ResultSetMetaData é o ALIAS; o do getColumns é o do
     * campo.
     *
     * <p>É a divergência que a execução da Fase E revelou, e este é o único
     * lugar em que ela pode ser provada contra o servidor de verdade.
     */
    @Test
    void oNomeDeSaidaEhOAliasEODoCatalogoEhODoCampo() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM todos_os_tipos LIMIT 1")) {
            List<String> saida = new ArrayList<>();
            ResultSetMetaData m = rs.getMetaData();
            for (int i = 1; i <= m.getColumnCount(); i++) {
                saida.add(m.getColumnName(i));
            }
            assertTrue(saida.contains("t"), "SELECT * devia publicar o alias 't': " + saida);
            assertFalse(saida.contains("texto"), "o nome do campo não devia sair na projeção: " + saida);
        }
    }

    /** O erro do servidor chega com o SQLState certo, e não como falha genérica. */
    @Test
    void oErroDoServidorChegaTraduzido() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement()) {
            SQLException clausula = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM todos_os_tipos ORDER BY id"));
            assertEquals("0A000", clausula.getSQLState(), clausula.getMessage());
            assertTrue(clausula.getMessage().contains("ORDER BY"), clausula.getMessage());

            SQLException sintaxe = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM"));
            assertEquals("42601", sintaxe.getSQLState(), sintaxe.getMessage());

            SQLException tabela = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM nao_existe"));
            assertEquals("42S02", tabela.getSQLState(), tabela.getMessage());
        }
    }

    /**
     * SEM a CA, a conexão FALHA — a prova de que não sobrou nenhum caminho que
     * ignore a verificação de certificado.
     *
     * <p>É o teste que não pode passar por acidente: se um dia alguém
     * acrescentar uma propriedade "insecure", ou o driver cair para um
     * TrustManager permissivo, este teste vira verde e é o único que acusa.
     */
    @Test
    void semACAAConexaoFalha() {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", IntegracaoObrigatoriaTest.SENHA);
        SQLException e = assertThrows(SQLException.class,
                () -> DriverManager.getConnection(IntegracaoObrigatoriaTest.URL_SEM_CA, p));
        assertEquals("08001", e.getSQLState(), e.getMessage());
    }

    /** Credencial errada é 28000, e não um erro de conexão genérico. */
    @Test
    void credencialErradaEh28000() {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", "senha-errada");
        SQLException e = assertThrows(SQLException.class,
                () -> DriverManager.getConnection(IntegracaoObrigatoriaTest.URL, p));
        assertEquals("28000", e.getSQLState(), e.getMessage());
    }

    /**
     * O §128 no cliente, contra o servidor real: o resultado inteiro atravessa
     * o driver com heap FIXO.
     *
     * <p>O surefire roda com {@code -Xmx64m}. Um driver que materializasse o
     * resultado estouraria o heap muito antes do fim — e é por isso que este
     * teste percorre TODAS as linhas em vez de amostrar.
     */
    @Test
    void oResultadoInteiroAtravessaODriverComHeapFixo() throws SQLException {
        long esperadas = Long.parseLong(System.getenv("NISHIDB_IT_LINHAS")) + 2;
        try (Connection c = conecta(); Statement st = c.createStatement()) {
            // maxRows zero e teto do dataset alto: o objetivo é trazer tudo.
            st.setMaxRows(0);
            long lidas = 0;
            long soma = 0;
            try (ResultSet rs = st.executeQuery("SELECT id, inteiro, texto FROM todos_os_tipos")) {
                while (rs.next()) {
                    lidas++;
                    soma += rs.getLong("inteiro");
                }
            }
            assertEquals(esperadas, lidas,
                    "o driver não entregou todas as linhas ingeridas (soma lida: " + soma + ")");
        }
    }
}
