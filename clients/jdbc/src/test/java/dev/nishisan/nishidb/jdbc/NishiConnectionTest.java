package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * A conexão contra um servidor HTTP de verdade — o do próprio JDK, para que a
 * doutrina de zero dependência valha também no escopo de teste.
 */
class NishiConnectionTest {

    private static final String META = "{\"datasets\":[{\"name\":\"eventos\",\"key\":\"id\","
            + "\"maxRows\":1000,\"timeoutMs\":30000,\"columns\":["
            + "{\"name\":\"id\",\"alias\":null,\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0,\"index\":\"none\",\"auto\":false}]}]}";

    private static final String STREAM = "{\"nishidb\":\"schema\",\"dataset\":\"eventos\",\"columns\":["
            + "{\"name\":\"id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0}]}\n"
            + "{\"id\":\"a\"}\n{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}\n";

    /** Os corpos que o driver enviou em POST /v1/query, na ordem. */
    private final List<String> enviados = new ArrayList<>();

    /** As query strings que o driver mandou em GET /v1/metadata, na ordem. */
    private final List<String> metadataQueries = new ArrayList<>();

    private void comServidor(Consumer<Connection> corpo) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", (HttpExchange ex) -> {
            String caminho = ex.getRequestURI().getPath();
            if ("/v1/query".equals(caminho)) {
                try (InputStream in = ex.getRequestBody()) {
                    enviados.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                }
                responde(ex, 200, STREAM);
            } else if ("/v1/metadata".equals(caminho)) {
                metadataQueries.add(ex.getRequestURI().getQuery());
                responde(ex, 200, META);
            } else {
                responde(ex, 200, "{\"status\":\"UP\"}");
            }
        });
        srv.start();
        try {
            String url = "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/telecom";
            try (Connection c = DriverManager.getConnection(url, new Properties())) {
                corpo.accept(c);
            }
        } finally {
            srv.stop(0);
        }
    }

    private static void responde(HttpExchange ex, int status, String corpo) {
        try {
            byte[] b = corpo.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** O caminho feliz ponta a ponta, pelo DriverManager. */
    @Test
    void aConsultaVaiEVolta() throws Exception {
        comServidor(c -> {
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT id FROM eventos")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString("id"));
                assertFalse(rs.next());
                assertEquals(st, rs.getStatement());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertEquals(1, enviados.size());
        assertTrue(enviados.get(0).contains("\"schema\":\"telecom\""), enviados.get(0));
        assertTrue(enviados.get(0).contains("\"sql\":\"SELECT id FROM eventos\""), enviados.get(0));
        assertEquals(List.of("schema=telecom"), metadataQueries);
    }

    /**
     * O schema da URL vai em TODA consulta, qualquer que seja o FROM, e o
     * corpo nunca leva {@code dataset}.
     *
     * <p>Quem resolve o FROM é o servidor, dentro do schema da conexão: o
     * driver não palpita nome nenhum a partir do SQL — nem da tabela simples,
     * nem do nome qualificado, nem do citado.
     */
    @Test
    void oSchemaDaUrlVaiEmTodaConsulta() throws Exception {
        List<String> consultas = List.of("SELECT id FROM outro", "SELECT id FROM fault.events",
                "SELECT * FROM \"_meta\".tables");
        comServidor(c -> {
            for (String sql : consultas) {
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    rs.next();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
        });
        assertEquals(consultas.size(), enviados.size());
        for (String corpo : enviados) {
            assertTrue(corpo.contains("\"schema\":\"telecom\""), corpo);
            assertFalse(corpo.contains("\"dataset\""), corpo);
        }
    }

    /**
     * getSchema devolve o schema da URL, e setSchema só aceita o mesmo.
     *
     * <p>Aceitar outro em silêncio faria a ferramenta acreditar que trocou de
     * schema enquanto o servidor continua resolvendo no da URL.
     */
    @Test
    void oSchemaDaConexaoEhFixadoPelaUrl() throws Exception {
        comServidor(c -> {
            try {
                assertEquals("telecom", c.getSchema());
                c.setSchema("telecom");
                c.setSchema(null);
                SQLException e = assertThrows(SQLException.class, () -> c.setSchema("fault"));
                assertEquals("3F000", e.getSQLState());
                assertTrue(e.getMessage().contains("telecom") && e.getMessage().contains("/fault"),
                        e.getMessage());
                assertEquals("telecom", c.getSchema());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * maxRows: abaixo do teto vai, acima do teto e zero são OMITIDOS.
     *
     * <p>O servidor RECUSA maxRows acima do teto do dataset em vez de aparar, e
     * o DBeaver manda 200 por default. Omitir quando o pedido é maior que o
     * teto é o que atende à pergunta — o teto do dataset já garante "no máximo
     * n" — sem transformar um default de ferramenta em erro na cara do usuário.
     */
    @Test
    void oMaxRowsRespeitaOTetoDoDataset() throws Exception {
        comServidor(c -> {
            try (Statement st = c.createStatement()) {
                st.setMaxRows(200);
                st.executeQuery("SELECT id FROM eventos").close();
                st.setMaxRows(5000);
                st.executeQuery("SELECT id FROM eventos").close();
                st.setMaxRows(0);
                st.executeQuery("SELECT id FROM eventos").close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTrue(enviados.get(0).contains("\"maxRows\":200"), enviados.get(0));
        assertFalse(enviados.get(1).contains("maxRows"), enviados.get(1));
        assertFalse(enviados.get(2).contains("maxRows"), enviados.get(2));
    }

    /**
     * queryTimeout é CLAMPADO contra o teto do dataset.
     *
     * <p>O servidor recusa acima do teto, e o teto é o máximo que existe: pedir
     * mais e receber erro seria pior que receber o máximo disponível.
     */
    @Test
    void oQueryTimeoutEhClampadoContraOTeto() throws Exception {
        comServidor(c -> {
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(5);
                st.executeQuery("SELECT id FROM eventos").close();
                st.setQueryTimeout(600);
                st.executeQuery("SELECT id FROM eventos").close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTrue(enviados.get(0).contains("\"timeoutMs\":5000"), enviados.get(0));
        assertTrue(enviados.get(1).contains("\"timeoutMs\":30000"), enviados.get(1));
    }

    /** O NishiDB não escreve por SQL, e o driver diz isso em vez de fingir. */
    @Test
    void escritaPorSqlNaoExiste() throws Exception {
        comServidor(c -> {
            try (Statement st = c.createStatement()) {
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> st.executeUpdate("SELECT id FROM eventos"));
                assertThrows(SQLFeatureNotSupportedException.class, () -> st.addBatch("x"));
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> st.executeLargeUpdate("SELECT id FROM eventos"));
                assertEquals(-1, st.getUpdateCount());
                assertEquals(-1L, st.getLargeUpdateCount());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Não há transação, e a conexão é somente leitura por construção. */
    @Test
    void naoHaTransacaoNemEscrita() throws Exception {
        comServidor(c -> {
            try {
                assertTrue(c.getAutoCommit());
                assertTrue(c.isReadOnly());
                assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
                assertThrows(SQLFeatureNotSupportedException.class, () -> c.setAutoCommit(false));
                assertThrows(SQLFeatureNotSupportedException.class, c::commit);
                assertThrows(SQLFeatureNotSupportedException.class, c::rollback);
                assertThrows(SQLFeatureNotSupportedException.class, () -> c.setReadOnly(false));
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
                // Repetir o que já é verdade não é erro.
                c.setAutoCommit(true);
                c.setReadOnly(true);
                c.setTransactionIsolation(Connection.TRANSACTION_NONE);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * Fechar a conexão fecha o que ela abriu, na ordem.
     *
     * <p>ResultSet antes de Statement, Statement antes do cliente HTTP: um
     * stream não consumido nem fechado nunca devolve a conexão, e é assim que
     * uma ferramenta trava depois de algumas dezenas de consultas.
     */
    @Test
    void fecharAConexaoFechaOQueElaAbriu() throws Exception {
        Statement[] guardado = new Statement[1];
        ResultSet[] resultado = new ResultSet[1];
        comServidor(c -> {
            try {
                guardado[0] = c.createStatement();
                resultado[0] = guardado[0].executeQuery("SELECT id FROM eventos");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTrue(resultado[0].isClosed());
        assertTrue(guardado[0].isClosed());
    }

    /** Cursor rolável não existe, e pedir um é erro na criação — não depois. */
    @Test
    void cursorRolavelEhRecusadoNaCriacao() throws Exception {
        comServidor(c -> {
            assertThrows(SQLException.class, () -> c.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
            assertThrows(SQLException.class, () -> c.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        });
    }

    /** isValid pergunta ao servidor de verdade, em vez de olhar só o próprio estado. */
    @Test
    void isValidPerguntaAoServidor() throws Exception {
        comServidor(c -> {
            try {
                assertTrue(c.isValid(2));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O schema da URL é conferido no connect, e não na primeira consulta — e a
     * recusa ensina a forma da URL.
     *
     * <p>O servidor responde UNKNOWN_SCHEMA com a mensagem que nomeia o schema
     * certo quando o caminho é o nome de um dataset (a URL da forma anterior);
     * o driver acrescenta o que só ele sabe, a forma da URL.
     */
    @Test
    void schemaInexistenteFalhaNoConnectEnsinandoAFormaDaUrl() throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", ex -> responde(ex, 404, "{\"error\":{\"code\":\"UNKNOWN_SCHEMA\","
                + "\"message\":\"schema \\\"eventos\\\" is not registered on this server: \\\"eventos\\\" is a "
                + "dataset, and the connection names its schema; use \\\"telecom\\\"\"}}"));
        srv.start();
        try {
            String url = "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/eventos";
            SQLException e = assertThrows(SQLException.class,
                    () -> DriverManager.getConnection(url, new Properties()));
            assertEquals("3F000", e.getSQLState(), e.getMessage());
            assertTrue(e.getMessage().contains("use \"telecom\""), e.getMessage());
            assertTrue(e.getMessage().contains("jdbc:nishidb://host[:porta]/schema"), e.getMessage());
        } finally {
            srv.stop(0);
        }
    }

}
