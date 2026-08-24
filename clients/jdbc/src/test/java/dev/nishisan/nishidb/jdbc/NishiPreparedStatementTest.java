package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.nishisan.nishidb.jdbc.wire.Temporais;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class NishiPreparedStatementTest {

    private static final String META = "{\"datasets\":[{\"name\":\"radius_sessions\",\"key\":\"service_id\","
            + "\"maxRows\":1000,\"timeoutMs\":30000,\"columns\":[]}]}";

    private static final String STREAM = "{\"nishidb\":\"schema\",\"dataset\":\"radius_sessions\",\"columns\":["
            + "{\"name\":\"service_id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0},"
            + "{\"name\":\"ip\",\"type\":\"ipv4\",\"jdbc\":\"VARCHAR\",\"nullable\":true,"
            + "\"precision\":15,\"scale\":0,\"displaySize\":15}]}\n"
            + "{\"service_id\":\"s-1\",\"ip\":\"10.0.0.1\"}\n"
            + "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}\n";

    private final List<String> enviados = new ArrayList<>();

    private void comServidor(Consumer<Connection> corpo) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", (HttpExchange ex) -> {
            if ("/v1/query".equals(ex.getRequestURI().getPath())) {
                try (InputStream in = ex.getRequestBody()) {
                    enviados.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                }
                responde(ex, STREAM);
            } else {
                responde(ex, META);
            }
        });
        srv.start();
        try {
            String url = "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/radius_sessions";
            try (Connection c = DriverManager.getConnection(url, new Properties())) {
                corpo.accept(c);
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

    private static <T> T em(String zona, Callable<T> corpo) throws Exception {
        TimeZone anterior = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zona));
            return corpo.call();
        } finally {
            TimeZone.setDefault(anterior);
        }
    }

    /** O exemplo do §130, executado literalmente como está escrito na spec. */
    @Test
    void oExemploDoSpecRodaLiteralmente() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("""
                    SELECT
                        service_id,
                        ip,
                        nas,
                        status
                    FROM radius_sessions
                    WHERE status = ?
                      AND nas = ?
                    LIMIT 100
                    """)) {
                st.setString(1, "Stop");
                st.setString(2, "i-br-sp-cmp-cmp-hl4-01");
                try (ResultSet rs = st.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("s-1", rs.getString("service_id"));
                    assertEquals("10.0.0.1", rs.getString("ip"));
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        String corpo = enviados.get(0);
        assertTrue(corpo.contains("\"dataset\":\"radius_sessions\""), corpo);
        assertTrue(corpo.contains("{\"type\":\"string\",\"value\":\"Stop\"}"), corpo);
        assertTrue(corpo.contains("{\"type\":\"string\",\"value\":\"i-br-sp-cmp-cmp-hl4-01\"}"), corpo);
    }

    /** Os quatro tipos do fio, e nenhum a mais: é o contrato do binder. */
    @Test
    void osQuatroTiposDoFio() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st =
                    c.prepareStatement("SELECT * FROM radius_sessions WHERE a=? AND b=? AND c=? AND d=?")) {
                st.setString(1, "x");
                st.setLong(2, 9223372036854775807L);
                st.setDouble(3, 1.5);
                st.setBoolean(4, true);
                st.executeQuery().close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        String corpo = enviados.get(0);
        assertTrue(corpo.contains("\"parameters\":[{\"type\":\"string\",\"value\":\"x\"},"
                + "{\"type\":\"int64\",\"value\":9223372036854775807},"
                + "{\"type\":\"double\",\"value\":1.5},"
                + "{\"type\":\"boolean\",\"value\":true}]"), corpo);
    }

    /**
     * O int64 viaja como texto de dígitos, e não como número de ponto
     * flutuante.
     *
     * <p>É o espelho do campo "value" ser RawMessage do lado Go: por double,
     * 9223372036854775807 chegaria ao servidor como ...5808, errado e sem erro.
     */
    @Test
    void oInt64NoLimiteChegaExato() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                st.setLong(1, Long.MIN_VALUE);
                st.executeQuery().close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTrue(enviados.get(0).contains("\"value\":-9223372036854775808"), enviados.get(0));
    }

    /**
     * O índice fora de faixa é recusado NO CLIENTE, com o número 1-based.
     *
     * <p>Deixar passar faria o servidor responder um erro de bind sobre um
     * parâmetro que a consulta não tem, e a mensagem falaria de posição interna
     * em vez do número que a aplicação escreveu.
     */
    @Test
    void indiceForaDeFaixaEhRecusadoNoCliente() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                SQLException e = assertThrows(SQLException.class, () -> st.setString(2, "x"));
                assertTrue(e.getMessage().contains("2"), e.getMessage());
                assertTrue(e.getMessage().contains("1"), e.getMessage());
                assertThrows(SQLException.class, () -> st.setString(0, "x"));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * Executar sem preencher todos os parâmetros é erro, e o erro diz QUAL.
     *
     * <p>Mandar menos parâmetros do que a query tem faria o servidor recusar
     * com uma mensagem sobre contagem, que não aponta o esquecido.
     */
    @Test
    void parametroNaoPreenchidoEhRecusadoNomeandoOIndice() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st =
                    c.prepareStatement("SELECT * FROM radius_sessions WHERE a=? AND b=?")) {
                st.setString(1, "x");
                SQLException e = assertThrows(SQLException.class, st::executeQuery);
                assertTrue(e.getMessage().contains("2"), e.getMessage());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * setTimestamp viaja como INT64 de epoch millis, nunca como texto.
     *
     * <p>Uma string contra uma coluna datetime é interpretada no format
     * DECLARADO do campo, resolvido no fuso do host do SERVIDOR: um
     * setTimestamp que enviasse ISO-8601 funcionaria contra timestamp e
     * falharia contra datetime, pela mesma chamada. O inteiro é o único
     * caminho uniforme.
     */
    @Test
    void setTimestampViajaComoEpochMillis() throws Exception {
        long millis = Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli();
        for (String zona : new String[] {"UTC", "Asia/Tokyo"}) {
            enviados.clear();
            em(zona, () -> {
                comServidor(c -> {
                    try (PreparedStatement st =
                            c.prepareStatement("SELECT * FROM radius_sessions WHERE t=?")) {
                        st.setTimestamp(1, new Timestamp(millis));
                        st.executeQuery().close();
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                });
                return null;
            });
            assertTrue(enviados.get(0).contains("{\"type\":\"int64\",\"value\":" + millis + "}"),
                    zona + ": " + enviados.get(0));
        }
    }

    /**
     * setTime viaja como millis DESDE A MEIA-NOITE, e não como epoch.
     *
     * <p>É a mesma palavra ("inteiro") significando outra coisa, e o binder
     * documenta isso: um epoch mandado numa coluna time devolve zero linhas sem
     * erro nenhum. A conversão desfaz exatamente o que getTime faz.
     */
    @Test
    void setTimeViajaComoMillisDoDia() throws Exception {
        for (String zona : new String[] {"UTC", "Asia/Tokyo", "America/Sao_Paulo"}) {
            enviados.clear();
            em(zona, () -> {
                comServidor(c -> {
                    try (PreparedStatement st =
                            c.prepareStatement("SELECT * FROM radius_sessions WHERE h=?")) {
                        st.setTime(1, Temporais.timeDe(86_399_999L, ZoneId.systemDefault()));
                        st.executeQuery().close();
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                });
                return null;
            });
            assertTrue(enviados.get(0).contains("\"value\":86399999"), zona + ": " + enviados.get(0));
        }
    }

    /** Com Calendar, o relógio de parede é lido no fuso pedido. */
    @Test
    void setTimeComCalendarUsaOFusoPedido() throws Exception {
        em("UTC", () -> {
            comServidor(c -> {
                try (PreparedStatement st =
                        c.prepareStatement("SELECT * FROM radius_sessions WHERE h=?")) {
                    Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
                    st.setTime(1, Temporais.timeDe(3_600_000L, ZoneId.of("Asia/Tokyo")), cal);
                    st.executeQuery().close();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
            return null;
        });
        assertTrue(enviados.get(0).contains("\"value\":3600000"), enviados.get(0));
    }

    /**
     * setNull NÃO EXISTE, e a mensagem diz o que usar.
     *
     * <p>Não há sintaxe de parâmetro que produza NULL no protocolo: quem quer
     * comparar com nulo escreve IS NULL no SQL. O modo de falha que este teste
     * fecha é o pior possível — aceitar o setNull e mandar algo que casa nada
     * devolveria um resultado VAZIO, que a aplicação leria como "não há linhas".
     */
    @Test
    void setNullNaoExisteENaoDevolveResultadoVazio() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                SQLException e = assertThrows(SQLFeatureNotSupportedException.class,
                        () -> st.setNull(1, Types.VARCHAR));
                assertTrue(e.getMessage().contains("IS NULL"), e.getMessage());
                assertThrows(SQLFeatureNotSupportedException.class, () -> st.setObject(1, null));
                assertEquals(0, enviados.size(), "nenhuma consulta pode ter sido executada");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Binário não é comparável no binder, e o driver diz isso em vez de tentar. */
    @Test
    void setBytesNaoExistePorqueOBinderNaoComparaBinario() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> st.setBytes(1, new byte[] {1, 2}));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O tipo de destino de setObject é IGNORADO, e o teste torna isso
     * verificável.
     *
     * <p>Ele seria uma terceira opinião sobre a mesma coisa: o valor já diz o
     * tipo de fio, e a coluna diz a coerção final — que é do servidor. Os dois
     * caminhos produzem os MESMOS bytes.
     */
    @Test
    void oTipoDeDestinoDeSetObjectNaoMudaOQueVaiNoFio() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                st.setObject(1, 42L);
                st.executeQuery().close();
                st.setObject(1, 42L, Types.BIGINT);
                st.executeQuery().close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertEquals(enviados.get(0), enviados.get(1));
    }

    /** Os métodos que recebem SQL não valem num PreparedStatement — é o contrato. */
    @Test
    void osMetodosComSqlNaoValemNumPreparedStatement() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions")) {
                assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1"));
                assertThrows(SQLException.class, () -> st.execute("SELECT 1"));
                assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** clearParameters volta ao estado de "nenhum preenchido". */
    @Test
    void clearParametersVoltaAoEstadoInicial() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM radius_sessions WHERE a=?")) {
                st.setString(1, "x");
                st.clearParameters();
                assertThrows(SQLException.class, st::executeQuery);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O metadado de parâmetro conta, e admite que não sabe o tipo.
     *
     * <p>Quem decide o tipo de um parâmetro é a COLUNA contra a qual ele é
     * comparado, e isso acontece no servidor, no bind. Inventar um tipo aqui
     * faria a ferramenta oferecer o editor errado.
     */
    @Test
    void oMetadadoDeParametroContaEAdmiteQueNaoSabeOTipo() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st =
                    c.prepareStatement("SELECT * FROM radius_sessions WHERE a=? AND b=?")) {
                assertEquals(2, st.getParameterMetaData().getParameterCount());
                assertEquals(Types.OTHER, st.getParameterMetaData().getParameterType(1));
                assertEquals(java.sql.ParameterMetaData.parameterModeIn,
                        st.getParameterMetaData().getParameterMode(1));
                assertThrows(SQLException.class, () -> st.getParameterMetaData().getParameterType(3));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** '?' dentro de literal não é parâmetro, e a contagem tem de saber disso. */
    @Test
    void placeholderDentroDeLiteralNaoContaParaOPreparedStatement() throws Exception {
        comServidor(c -> {
            try (PreparedStatement st =
                    c.prepareStatement("SELECT * FROM radius_sessions WHERE a='?' AND b=?")) {
                assertEquals(1, st.getParameterMetaData().getParameterCount());
                st.setString(1, "x");
                st.executeQuery().close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTrue(enviados.get(0).contains("\"parameters\":[{\"type\":\"string\",\"value\":\"x\"}]"),
                enviados.get(0));
    }
}
