package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class NishiDatabaseMetaDataTest {

    private static String coluna(String nome, String alias, String tipo, String jdbc, boolean nulavel,
            int precision, int scale, String indice, boolean auto) {
        return "{\"name\":\"" + nome + "\",\"alias\":" + (alias == null ? "null" : "\"" + alias + "\"")
                + ",\"type\":\"" + tipo + "\",\"jdbc\":\"" + jdbc + "\",\"nullable\":" + nulavel
                + ",\"precision\":" + precision + ",\"scale\":" + scale + ",\"displaySize\":0"
                + ",\"index\":\"" + indice + "\",\"auto\":" + auto + "}";
    }

    private static final String META = "{\"datasets\":["
            + "{\"name\":\"zebras\",\"key\":\"id\",\"maxRows\":10,\"timeoutMs\":100,\"columns\":["
            + coluna("id", null, "string", "VARCHAR", false, 0, 0, "none", false) + "]},"
            + "{\"name\":\"eventos\",\"key\":\"service_id\",\"maxRows\":1000,\"timeoutMs\":30000,"
            + "\"columns\":["
            + coluna("service_id", null, "string", "VARCHAR", false, 0, 0, "none", false) + ","
            + coluna("status", "estado", "string", "VARCHAR", true, 0, 0, "bitmap", false) + ","
            + coluna("payload", null, "json", "VARCHAR", true, 0, 0, "none", true) + ","
            + coluna("quando", null, "timestamp", "TIMESTAMP", true, 24, 3, "none", false)
            + "]}]}";

    private static final String STATS = "{\"process\":{\"version\":\"v1.4.2-3-gabc\",\"query\":{}},"
            + "\"datasets\":[]}";

    private String stats = STATS;

    private void comMetadata(Consumer<DatabaseMetaData> corpo) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", (HttpExchange ex) -> {
            String c = ex.getRequestURI().getPath();
            responde(ex, "/v1/stats".equals(c) ? stats : META);
        });
        srv.start();
        try {
            String url = "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/eventos";
            try (Connection c = DriverManager.getConnection(url, new Properties())) {
                corpo.accept(c.getMetaData());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        } finally {
            srv.stop(0);
        }
    }

    private static void responde(HttpExchange ex, String corpo) {
        try {
            byte[] b = corpo.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> coluna1(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        while (rs.next()) {
            out.add(rs.getString(1));
        }
        return out;
    }

    /** As tabelas saem ORDENADAS por nome, como o contrato manda. */
    @Test
    void asTabelasSaemNaOrdemDoContrato() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getTables(null, null, null, null)) {
                assertEquals(10, rs.getMetaData().getColumnCount());
                List<String> nomes = new ArrayList<>();
                while (rs.next()) {
                    nomes.add(rs.getString("TABLE_NAME"));
                    assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                }
                assertEquals(List.of("eventos", "zebras"), nomes);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O padrão LIKE é implementado no driver, e {@code null} casa TUDO.
     *
     * <p>Confundir null com "nenhum" faria a árvore da ferramenta aparecer
     * vazia justamente na primeira chamada, que é sem filtro.
     */
    @Test
    void oPadraoLikeCasaComoOJdbcManda() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(List.of("eventos"), nomesDe(m.getTables(null, null, "even%", null)));
                assertEquals(List.of("zebras"), nomesDe(m.getTables(null, null, "_ebras", null)));
                assertEquals(List.of(), nomesDe(m.getTables(null, null, "nada%", null)));
                assertEquals(2, nomesDe(m.getTables(null, null, "%", null)).size());
                // Um tipo que não é TABLE não devolve nada, e não devolve erro.
                assertEquals(List.of(), nomesDe(m.getTables(null, null, null, new String[] {"VIEW"})));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static List<String> nomesDe(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        try (rs) {
            while (rs.next()) {
                out.add(rs.getString("TABLE_NAME"));
            }
        }
        return out;
    }

    /**
     * getColumns publica o nome do CAMPO, e o alias vai no REMARKS.
     *
     * <p>É a divergência que quem ligar as duas rotas precisa conhecer: para o
     * campo com alias, aqui sai {@code status} e o ResultSetMetaData de um
     * SELECT * sai {@code estado}. Sem o alias no REMARKS, ligar um ao outro
     * seria impossível.
     */
    @Test
    void getColumnsPublicaONomeDoCampoEOAliasVaiNoRemarks() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getColumns(null, null, "eventos", null)) {
                assertEquals(24, rs.getMetaData().getColumnCount());

                assertTrue(rs.next());
                assertEquals("service_id", rs.getString("COLUMN_NAME"));
                assertEquals(1, rs.getInt("ORDINAL_POSITION"));
                assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
                assertEquals("NO", rs.getString("IS_NULLABLE"));

                assertTrue(rs.next());
                assertEquals("status", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("REMARKS").contains("alias=estado"), rs.getString("REMARKS"));
                assertTrue(rs.getString("REMARKS").contains("index=bitmap"), rs.getString("REMARKS"));
                assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));

                assertTrue(rs.next());
                assertEquals("payload", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("REMARKS").contains("auto=true"), rs.getString("REMARKS"));

                assertTrue(rs.next());
                assertEquals("quando", rs.getString("COLUMN_NAME"));
                assertEquals(Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
                assertEquals("timestamp", rs.getString("TYPE_NAME"));
                assertEquals(24, rs.getInt("COLUMN_SIZE"));
                assertEquals(3, rs.getInt("DECIMAL_DIGITS"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** A chave do dataset é ganho real: é ela que a ferramenta desenha. */
    @Test
    void aChaveDoDatasetSaiComoChavePrimaria() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getPrimaryKeys(null, null, "eventos")) {
                assertTrue(rs.next());
                assertEquals("eventos", rs.getString("TABLE_NAME"));
                assertEquals("service_id", rs.getString("COLUMN_NAME"));
                assertEquals(1, rs.getInt("KEY_SEQ"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** O índice sai com a ressalva de que é DECLARADO, não plano escolhido. */
    @Test
    void oIndiceSaiComARessalvaDeQueEhDeclarado() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getIndexInfo(null, null, "eventos", false, true)) {
                assertTrue(rs.next());
                assertEquals("status", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("INDEX_NAME").contains("bitmap"), rs.getString("INDEX_NAME"));
                assertTrue(rs.getString("INDEX_NAME").contains("declarado"), rs.getString("INDEX_NAME"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * getTypeInfo sai do fio, e o SEARCHABLE é honesto.
     *
     * <p>json e bytes não são pesquisáveis porque o binder recusa comparação
     * sobre eles: anunciá-los pesquisáveis faria a ferramenta oferecer um
     * filtro que o servidor recusa.
     */
    @Test
    void getTypeInfoSaiDoFioEODizSeEhPesquisavel() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getTypeInfo()) {
                boolean viuJson = false;
                boolean viuString = false;
                while (rs.next()) {
                    if ("json".equals(rs.getString("TYPE_NAME"))) {
                        viuJson = true;
                        assertEquals(DatabaseMetaData.typePredNone, rs.getInt("SEARCHABLE"));
                    }
                    if ("string".equals(rs.getString("TYPE_NAME"))) {
                        viuString = true;
                        assertEquals(DatabaseMetaData.typeSearchable, rs.getInt("SEARCHABLE"));
                    }
                }
                assertTrue(viuJson && viuString, "os tipos do fio não apareceram no getTypeInfo");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * Catálogo e schema saem VAZIOS COM A FORMA CERTA — a errata D3.
     *
     * <p>O produto não tem nenhum dos dois. Lançar aqui faria a ferramenta
     * mostrar erro ao abrir a árvore de navegação, antes da primeira consulta.
     */
    @Test
    void catalogoESchemaSaemVaziosComAFormaCerta() throws Exception {
        comMetadata(m -> {
            try (ResultSet cat = m.getCatalogs(); ResultSet sch = m.getSchemas();
                    ResultSet tipos = m.getTableTypes()) {
                assertEquals(1, cat.getMetaData().getColumnCount());
                assertEquals("TABLE_CAT", cat.getMetaData().getColumnName(1));
                assertEquals(List.of(), coluna1(cat));

                assertEquals(2, sch.getMetaData().getColumnCount());
                assertEquals(List.of(), coluna1(sch));

                assertEquals(List.of("TABLE"), coluna1(tipos));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Todo getter de catálogo que não existe no produto devolve VAZIO, nunca exceção. */
    @Test
    void osGettersSemConteudoDevolvemVazioENaoExcecao() throws Exception {
        comMetadata(m -> {
            try {
                for (ResultSet rs : List.of(m.getProcedures(null, null, null),
                        m.getProcedureColumns(null, null, null, null),
                        m.getFunctions(null, null, null),
                        m.getFunctionColumns(null, null, null, null),
                        m.getImportedKeys(null, null, "eventos"),
                        m.getExportedKeys(null, null, "eventos"),
                        m.getCrossReference(null, null, "a", null, null, "b"),
                        m.getTablePrivileges(null, null, null),
                        m.getColumnPrivileges(null, null, "eventos", null),
                        m.getBestRowIdentifier(null, null, "eventos", 0, false),
                        m.getVersionColumns(null, null, "eventos"),
                        m.getUDTs(null, null, null, null),
                        m.getSuperTypes(null, null, null),
                        m.getSuperTables(null, null, null),
                        m.getAttributes(null, null, null, null),
                        m.getClientInfoProperties(),
                        m.getPseudoColumns(null, null, null, null))) {
                    ResultSetMetaData meta = rs.getMetaData();
                    assertTrue(meta.getColumnCount() > 0, "ResultSet vazio precisa ter as colunas certas");
                    assertFalse(rs.next());
                    rs.close();
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** As capacidades que o produto não tem respondem FALSE, e não lançam. */
    @Test
    void asCapacidadesAusentesRespondemFalse() throws Exception {
        comMetadata(m -> {
            try {
                assertFalse(m.supportsGroupBy());
                assertFalse(m.supportsOuterJoins());
                assertFalse(m.supportsSubqueriesInComparisons());
                assertFalse(m.supportsUnion());
                assertFalse(m.supportsTransactions());
                assertFalse(m.supportsStoredProcedures());
                assertFalse(m.supportsBatchUpdates());
                assertFalse(m.supportsColumnAliasing());
                assertFalse(m.supportsANSI92EntryLevelSQL());
                assertFalse(m.supportsSavepoints());
                assertFalse(m.supportsPositionedUpdate());
                assertFalse(m.supportsPositionedDelete());
                assertEquals(Connection.TRANSACTION_NONE, m.getDefaultTransactionIsolation());

                assertTrue(m.allTablesAreSelectable());
                assertTrue(m.supportsMixedCaseIdentifiers());
                assertTrue(m.isReadOnly());
                assertTrue(m.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
                assertFalse(m.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
                assertEquals("\"", m.getIdentifierQuoteString());
                assertEquals(java.sql.RowIdLifetime.ROWID_UNSUPPORTED, m.getRowIdLifetime());
                assertEquals(DatabaseMetaData.sqlStateSQL, m.getSQLStateType());
                assertEquals(1, m.getMaxTablesInSelect());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O ORDER BY existe, e o metadado precisa dizer as três coisas certas
     * sobre ele — nem uma a menos, nem uma a mais.
     *
     * <p>As três foram MEDIDAS contra o servidor, e não deduzidas da spec:
     * ordenar por coluna fora da projeção funciona, ordenar por posição
     * ({@code ORDER BY 1}) e por expressão ({@code ORDER BY a + 1}) são
     * recusados na sintaxe, e o parser cobra teto de 16 chaves.
     *
     * <p>Enquanto {@code supportsOrderByUnrelated} respondia false, um
     * construtor de query gráfico desabilitava a ordenação por coluna não
     * projetada — o usuário concluía que o banco não faz o que ele faz. E
     * enquanto {@code getMaxColumnsInOrderBy} respondia zero ("sem limite"),
     * quem monta ORDER BY por programa descobriria o 16 por erro de sintaxe
     * em produção.
     */
    @Test
    void oMetadadoDescreveOOrderByComoEleE() throws Exception {
        comMetadata(m -> {
            try {
                assertTrue(m.supportsOrderByUnrelated(),
                        "o servidor ordena por coluna fora da projeção");
                assertFalse(m.supportsExpressionsInOrderBy(),
                        "expressão no ORDER BY é erro de sintaxe no servidor");
                assertEquals(16, m.getMaxColumnsInOrderBy(),
                        "o teto é o MaxOrderKeys do parser, não 'sem limite'");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** A versão do produto sai do /v1/stats, e o número não parseável vira ZERO. */
    @Test
    void aVersaoSaiDoStatsESemNumeroViraZero() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals("v1.4.2-3-gabc", m.getDatabaseProductVersion());
                assertEquals(1, m.getDatabaseMajorVersion());
                assertEquals(4, m.getDatabaseMinorVersion());
                assertEquals("NishiDB", m.getDatabaseProductName());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });

        stats = "{\"process\":{\"version\":\"desenvolvimento\"},\"datasets\":[]}";
        comMetadata(m -> {
            try {
                assertEquals(0, m.getDatabaseMajorVersion());
                assertEquals(0, m.getDatabaseMinorVersion());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
