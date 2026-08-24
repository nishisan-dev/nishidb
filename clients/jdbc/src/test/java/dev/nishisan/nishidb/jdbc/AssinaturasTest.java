package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * O teste literal do §107: TODO método da superfície JDBC é classificado, e a
 * classificação é OBSERVADA em vez de escrita à mão.
 *
 * <p>Uma tabela de quinhentas assinaturas mantida por humano envelhece na
 * primeira versão do JDBC que acrescente um método — e envelhece em silêncio,
 * porque a tabela continua compilando. Aqui a classificação é o COMPORTAMENTO:
 * cada método é invocado por reflexão, e o único desfecho aceito é "respondeu"
 * ou "lançou uma SQLException". Um método novo que o driver não sobrescreva cai
 * no default da interface, que lança {@code UnsupportedOperationException}, e
 * este teste reprova.
 *
 * <p><b>O que ele fecha não é teórico.</b> Uma exceção que não é SQLException
 * atravessa o {@code catch (SQLException)} da ferramenta e vira stack trace na
 * cara do usuário em vez de mensagem — e um NullPointerException vindo de um
 * argumento nulo faz o mesmo. Este teste passa {@code null} em todo argumento de
 * objeto exatamente por isso.
 */
class AssinaturasTest {

    private static final String META = "{\"datasets\":[{\"name\":\"eventos\",\"key\":\"id\","
            + "\"maxRows\":1000,\"timeoutMs\":30000,\"columns\":["
            + "{\"name\":\"id\",\"alias\":null,\"type\":\"string\",\"jdbc\":\"VARCHAR\","
            + "\"nullable\":false,\"precision\":0,\"scale\":0,\"displaySize\":0,"
            + "\"index\":\"none\",\"auto\":false}]}]}";

    private static final String STREAM = "{\"nishidb\":\"schema\",\"dataset\":\"eventos\",\"columns\":["
            + "{\"name\":\"id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0}]}\n"
            + "{\"id\":\"a\"}\n{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}\n";

    /**
     * Os métodos que uma ferramenta chama ao abrir um resultado, e que NÃO
     * podem lançar.
     *
     * <p>Se qualquer um destes lançar, o grid não abre — e a mensagem que o
     * usuário vê é "erro ao executar a consulta", que manda procurar no lugar
     * errado.
     */
    private static final List<String> OBRIGATORIOS = List.of(
            "ResultSet.next()", "ResultSet.close()", "ResultSet.getMetaData()", "ResultSet.wasNull()",
            "ResultSet.getType()", "ResultSet.getConcurrency()", "ResultSet.getWarnings()",
            "ResultSet.getStatement()", "ResultSet.isClosed()", "ResultSet.getRow()",
            "Statement.getConnection()", "Statement.getResultSet()", "Statement.getUpdateCount()",
            "Statement.getMoreResults()", "Statement.getWarnings()", "Statement.close()",
            "Statement.isClosed()", "Statement.getResultSetType()",
            "Statement.getResultSetConcurrency()",
            "Connection.getMetaData()", "Connection.getAutoCommit()", "Connection.isReadOnly()",
            "Connection.getCatalog()", "Connection.getSchema()", "Connection.getWarnings()",
            "Connection.getTransactionIsolation()", "Connection.getHoldability()",
            "Connection.createStatement()", "Connection.isClosed()",
            "DatabaseMetaData.getCatalogs()", "DatabaseMetaData.getSchemas()",
            "DatabaseMetaData.getTableTypes()", "DatabaseMetaData.getTypeInfo()",
            "DatabaseMetaData.getIdentifierQuoteString()",
            "DatabaseMetaData.getDatabaseProductName()", "DatabaseMetaData.getDriverName()",
            "ResultSetMetaData.getColumnCount()");

    /** O que o driver expõe, e uma fábrica de instância NOVA para cada método. */
    private interface Fabrica {
        Object nova(Connection c) throws SQLException;
    }

    private HttpServer srv;

    private void sobe() throws IOException {
        srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", (HttpExchange ex) -> {
            String caminho = ex.getRequestURI().getPath();
            String corpo;
            if ("/v1/query".equals(caminho)) {
                corpo = STREAM;
            } else if ("/v1/stats".equals(caminho)) {
                corpo = "{\"process\":{\"version\":\"v1.0.0\"},\"datasets\":[]}";
            } else {
                corpo = META;
            }
            try {
                byte[] b = corpo.getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, b.length);
                ex.getResponseBody().write(b);
                ex.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        srv.start();
    }

    private Connection conecta() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/eventos", new Properties());
    }

    /**
     * Percorre a superfície inteira e cobra o contrato.
     *
     * <p>Instância NOVA por método: metade deles fecha, cancela ou consome o
     * objeto, e reaproveitar faria o resultado depender da ordem em que a
     * reflexão os devolve — que não é estável entre JVMs.
     */
    @Test
    void todoMetodoDaSuperficieOuRespondeOuLancaSqlException() throws Exception {
        sobe();
        try {
            List<String> problemas = new ArrayList<>();
            TreeSet<String> vistos = new TreeSet<>();

            percorre(Connection.class, c -> c, problemas, vistos);
            percorre(Statement.class, Connection::createStatement, problemas, vistos);
            percorre(PreparedStatement.class,
                c -> c.prepareStatement("SELECT id FROM eventos WHERE id = ?"), problemas, vistos);
            percorre(ResultSet.class,
                c -> c.createStatement().executeQuery("SELECT id FROM eventos"), problemas, vistos);
            percorre(ResultSetMetaData.class,
                c -> c.createStatement().executeQuery("SELECT id FROM eventos").getMetaData(),
                problemas, vistos);
            percorre(DatabaseMetaData.class, Connection::getMetaData, problemas, vistos);
            percorre(java.sql.ParameterMetaData.class,
                c -> c.prepareStatement("SELECT id FROM eventos WHERE id = ?").getParameterMetaData(),
                problemas, vistos);

            assertEquals(List.of(), problemas, "métodos que não respeitam o contrato do JDBC");
            assertTrue(vistos.size() > 400,
                    "só " + vistos.size() + " métodos percorridos: a varredura quebrou, "
                            + "e uma varredura que não acha nada passa sempre");
        } finally {
            srv.stop(0);
        }
    }

    private void percorre(Class<?> interfaceJdbc, Fabrica fabrica, List<String> problemas,
            TreeSet<String> vistos) throws Exception {
        for (Method m : interfaceJdbc.getMethods()) {
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            String nome = interfaceJdbc.getSimpleName() + "." + m.getName() + assinatura(m);
            vistos.add(nome);

            Object alvo;
            try (Connection c = conecta()) {
                try {
                    alvo = fabrica.nova(c);
                } catch (SQLException e) {
                    problemas.add(nome + ": não foi possível construir o alvo: " + e);
                    continue;
                }
                invoca(m, alvo, nome, problemas);
            }
        }
    }

    private void invoca(Method m, Object alvo, String nome, List<String> problemas) {
        Object[] args = new Object[m.getParameterCount()];
        for (int i = 0; i < args.length; i++) {
            args[i] = padrao(m.getParameterTypes()[i]);
        }
        try {
            m.invoke(alvo, args);
        } catch (InvocationTargetException e) {
            Throwable causa = e.getCause();
            if (causa instanceof SQLException) {
                if (OBRIGATORIOS.contains(nome)) {
                    problemas.add(nome + " lançou, e é dos que a ferramenta chama para abrir o grid: "
                            + causa);
                }
                return;
            }
            problemas.add(nome + " lançou " + causa.getClass().getName()
                    + ", que não é SQLException e atravessa o catch da aplicação: " + causa);
        } catch (IllegalAccessException e) {
            problemas.add(nome + ": " + e);
        }
    }

    private static String assinatura(Method m) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < m.getParameterCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(m.getParameterTypes()[i].getSimpleName());
        }
        return sb.append(')').toString();
    }

    /**
     * O argumento default de cada tipo — e o de objeto é {@code null} DE
     * PROPÓSITO.
     *
     * <p>Argumento nulo é o que uma ferramenta passa o tempo todo (o padrão de
     * catálogo, o de schema), e um NullPointerException vindo daí atravessa o
     * catch da aplicação exatamente como o UnsupportedOperationException.
     */
    private static Object padrao(Class<?> tipo) {
        if (!tipo.isPrimitive()) {
            // Class é a exceção: unwrap e getObject(int, Class) precisam de um
            // alvo real para poderem sequer decidir.
            return tipo == Class.class ? String.class : null;
        }
        if (tipo == boolean.class) {
            return Boolean.FALSE;
        }
        if (tipo == long.class) {
            return 1L;
        }
        if (tipo == double.class) {
            return 1d;
        }
        if (tipo == float.class) {
            return 1f;
        }
        if (tipo == short.class) {
            return (short) 1;
        }
        if (tipo == byte.class) {
            return (byte) 1;
        }
        if (tipo == char.class) {
            return 'a';
        }
        return 1;
    }
}
