package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Os prazos de espera do cliente (issue #91) contra um servidor dublê que
 * ACEITA a requisição e não responde.
 *
 * <p><b>Nenhum teste aqui pode travar a suíte.</b> Toda chamada que espera o
 * servidor roda em outra thread com prazo do PRÓPRIO teste ({@link #within}),
 * e um mutante que tire o prazo do driver reprova com a mensagem de quem
 * ficou preso, em vez de pendurar o Maven. O dublê é fechado no fim de cada
 * teste, o que derruba as conexões e destrava a thread que ficou presa.
 */
class RequestTimeoutTest {

    private static final String META = "{\"datasets\":[{\"name\":\"eventos\",\"key\":\"id\","
            + "\"maxRows\":1000,\"timeoutMs\":30000,\"columns\":["
            + "{\"name\":\"id\",\"alias\":null,\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0,\"index\":\"none\",\"auto\":false}]}]}";

    private static final String SCHEMA_LINE = "{\"nishidb\":\"schema\",\"dataset\":\"eventos\",\"columns\":["
            + "{\"name\":\"id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0}]}\n";

    private static final String STREAM = SCHEMA_LINE
            + "{\"id\":\"a\"}\n{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}\n";

    /** Threads daemon: um mutante que trave não segura o fim da JVM do surefire. */
    private static final ExecutorService WORKERS = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "request-timeout-test");
        t.setDaemon(true);
        return t;
    });

    /** O que o dublê faz com cada rota. */
    private enum Mode {
        /** Responde na hora. */
        ANSWER,
        /** Aceita e nunca responde: nem cabeçalho. */
        HANG,
        /** Manda os cabeçalhos e o começo do corpo, e para. */
        HEADERS_THEN_STALL,
        /** Responde depois de {@link StubServer#queryDelayMs}. */
        DELAYED,
        /** Manda uma linha a cada {@link StubServer#rowIntervalMs}. */
        TRICKLE
    }

    /** O servidor dublê, com o comportamento de cada rota trocável no meio do teste. */
    private static final class StubServer implements AutoCloseable {

        volatile Mode health = Mode.ANSWER;
        volatile Mode metadata = Mode.ANSWER;
        volatile Mode query = Mode.ANSWER;
        volatile long queryDelayMs;
        volatile long rowIntervalMs;
        volatile int trickleRows;

        final List<String> queryBodies = Collections.synchronizedList(new ArrayList<>());
        /** Uma permissão por requisição de consulta que CHEGOU ao dublê. */
        final Semaphore queryArrived = new Semaphore(0);
        /** Solta os handlers presos; contado no close. */
        private final CountDownLatch release = new CountDownLatch(1);
        private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "stub-server");
            t.setDaemon(true);
            return t;
        });
        private final HttpServer srv;

        StubServer() throws IOException {
            srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            // Executor próprio e não o default: o default é UMA thread, e um
            // handler preso impediria o dublê de responder à requisição
            // seguinte — que é justamente a que prova a conexão utilizável.
            srv.setExecutor(pool);
            srv.createContext("/", this::handle);
            srv.start();
        }

        String url(String params) {
            return "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/telecom" + params;
        }

        private void handle(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getPath();
            Mode mode;
            String body;
            if ("/v1/query".equals(path)) {
                try (InputStream in = ex.getRequestBody()) {
                    queryBodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                }
                queryArrived.release();
                mode = query;
                body = STREAM;
            } else if ("/v1/metadata".equals(path)) {
                mode = metadata;
                body = META;
            } else {
                mode = health;
                body = "{\"status\":\"UP\"}";
            }
            switch (mode) {
                case ANSWER -> answer(ex, body);
                case HANG -> hold(ex);
                case HEADERS_THEN_STALL -> {
                    ex.sendResponseHeaders(200, 0);
                    OutputStream out = ex.getResponseBody();
                    // Para o stream, a linha de schema chega: executeQuery
                    // volta, e quem fica preso é o next(). Para o documento,
                    // um JSON pela metade.
                    out.write(("/v1/query".equals(path) ? SCHEMA_LINE : "{\"status\":")
                            .getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    hold(ex);
                }
                case DELAYED -> {
                    sleep(queryDelayMs);
                    answer(ex, body);
                }
                case TRICKLE -> {
                    ex.sendResponseHeaders(200, 0);
                    try (OutputStream out = ex.getResponseBody()) {
                        out.write(SCHEMA_LINE.getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        for (int i = 0; i < trickleRows; i++) {
                            sleep(rowIntervalMs);
                            out.write(("{\"id\":\"r" + i + "\"}\n").getBytes(StandardCharsets.UTF_8));
                            out.flush();
                        }
                        out.write(("{\"nishidb\":\"end\",\"rows\":" + trickleRows + ",\"truncated\":false}\n")
                                .getBytes(StandardCharsets.UTF_8));
                    }
                }
                default -> throw new IllegalStateException(mode.name());
            }
        }

        private static void answer(HttpExchange ex, String body) throws IOException {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        }

        /** Segura a requisição até o fim do teste — o servidor que não responde. */
        private void hold(HttpExchange ex) {
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ex.close();
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            release.countDown();
            srv.stop(0);
            pool.shutdownNow();
        }
    }

    // --------------------------------------------------------------- apoio

    @FunctionalInterface
    private interface SqlAction {
        void run() throws Exception;
    }

    /**
     * Roda {@code call} em outra thread e espera no máximo {@code limitMs}.
     *
     * <p>É o que impede um mutante sem prazo de travar a suíte: estourado o
     * prazo do teste, ele reprova nomeando quem ficou preso.
     */
    private static <T> T within(long limitMs, String what, Callable<T> call) throws Exception {
        Future<T> f = WORKERS.submit(call);
        try {
            return f.get(limitMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            return fail(what + " não voltou em " + limitMs + " ms: a requisição ficou presa sem prazo no cliente");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    /** Como {@link #within}, exigindo que a ação LANCE uma SQLException, que é devolvida. */
    private static SQLException failureWithin(long limitMs, String what, SqlAction action) throws Exception {
        return within(limitMs, what, () -> {
            try {
                action.run();
            } catch (SQLException e) {
                return e;
            }
            return fail(what + " voltou sem lançar: o estouro do prazo não chegou à aplicação");
        });
    }

    private static Connection connect(StubServer stub, String params) throws SQLException {
        return DriverManager.getConnection(stub.url(params), new Properties());
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static void readAll(Connection c, String what) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT id FROM eventos")) {
            assertTrue(rs.next(), what + ": a consulta seguinte não trouxe a linha");
            assertEquals("a", rs.getString("id"));
            assertFalse(rs.next());
        }
    }

    // -------------------------------------------------------------- isValid

    /**
     * {@code isValid(1)} contra um servidor que aceita e não responde volta
     * {@code false} em cerca de um segundo — e não fica preso.
     *
     * <p>É o caso do pool: o HikariCP valida com isValid antes de entregar a
     * conexão, e sem prazo a thread que pediu a conexão ficava presa até o
     * pool esgotar.
     */
    @Test
    void isValidReturnsFalseWithinItsTimeoutWhenTheServerNeverAnswers() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            stub.health = Mode.HANG;
            long start = System.nanoTime();
            boolean valid = within(5_000, "isValid(1)", () -> c.isValid(1));
            long ms = elapsedMs(start);
            assertFalse(valid, "isValid(1) disse que a conexão vale com o servidor mudo");
            assertTrue(ms >= 900, "isValid(1) voltou em " + ms + " ms: antes do próprio prazo");
            assertTrue(ms <= 2_500, "isValid(1) voltou em " + ms + " ms: muito depois do prazo de 1 s");
            assertFalse(c.isClosed(), "o estouro do isValid fechou a conexão; ele só responde false");
        }
    }

    /**
     * O prazo do isValid é da requisição INTEIRA: cabeçalhos que chegam e um
     * corpo que para também dão {@code false} em cerca de um segundo.
     *
     * <p>O {@code HttpRequest.timeout} do JDK deixa de contar quando os
     * cabeçalhos chegam; sem o prazo no corpo, este caso esperava para sempre.
     */
    @Test
    void isValidReturnsFalseWithinItsTimeoutWhenTheBodyStalls() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            stub.health = Mode.HEADERS_THEN_STALL;
            long start = System.nanoTime();
            boolean valid = within(5_000, "isValid(1) com o corpo parado", () -> c.isValid(1));
            long ms = elapsedMs(start);
            assertFalse(valid, "isValid(1) disse que a conexão vale com o corpo parado");
            assertTrue(ms >= 900 && ms <= 2_500, "isValid(1) com o corpo parado voltou em " + ms + " ms");
        }
    }

    /**
     * {@code isValid(0)} — sem prazo próprio, pelo contrato — usa o prazo de
     * rede, e o estouro continua sendo só {@code false}: a conexão NÃO é
     * fechada, ao contrário do estouro do prazo de rede numa consulta.
     */
    @Test
    void isValidZeroUsesTheNetworkTimeoutAndDoesNotCloseTheConnection() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=500")) {
            stub.health = Mode.HANG;
            long start = System.nanoTime();
            boolean valid = within(5_000, "isValid(0) com socketTimeoutMs=500", () -> c.isValid(0));
            long ms = elapsedMs(start);
            assertFalse(valid);
            assertTrue(ms >= 400 && ms <= 2_000, "isValid(0) voltou em " + ms + " ms, e o prazo de rede é 500");
            assertFalse(c.isClosed(), "o estouro do isValid fechou a conexão; ele só responde false");
        }
    }

    // --------------------------------------------------------- queryTimeout

    /**
     * {@code setQueryTimeout(1)} contra um servidor que nunca responde lança
     * {@code SQLTimeoutException} 57014 no prazo do servidor mais a folga — e
     * a conexão continua utilizável depois.
     */
    @Test
    void queryTimeoutThrowsSqlTimeoutExceptionAndKeepsTheConnectionUsable() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            stub.query = Mode.HANG;
            long start = System.nanoTime();
            long limit = 1_000 + NishiStatement.QUERY_TIMEOUT_GRACE_MS;
            SQLException e = failureWithin(limit + 4_000, "executeQuery com setQueryTimeout(1)", () -> {
                try (Statement st = c.createStatement()) {
                    st.setQueryTimeout(1);
                    st.executeQuery("SELECT id FROM eventos");
                }
            });
            long ms = elapsedMs(start);
            assertInstanceOf(SQLTimeoutException.class, e, e.toString());
            assertEquals("57014", e.getSQLState(), e.getMessage());
            assertTrue(ms >= limit - 200, "o cliente desistiu em " + ms + " ms, antes do prazo de " + limit);
            assertTrue(ms <= limit + 2_000, "o cliente desistiu em " + ms + " ms, muito depois de " + limit);
            assertTrue(stub.queryBodies.get(0).contains("\"timeoutMs\":1000"),
                    "o prazo deixou de ir ao servidor: " + stub.queryBodies.get(0));

            assertFalse(c.isClosed(), "o estouro do queryTimeout fechou a conexão");
            stub.query = Mode.ANSWER;
            within(5_000, "a consulta depois do estouro", () -> {
                readAll(c, "depois do estouro do queryTimeout");
                return null;
            });
        }
    }

    /**
     * O queryTimeout também cobre o STREAM: cabeçalhos e schema chegam, o
     * corpo para, e a leitura lança {@code SQLTimeoutException} em vez de
     * esperar para sempre. A conexão continua utilizável.
     *
     * <p>Quem estoura é o próprio executeQuery, e não o primeiro next(): o
     * cursor lê adiante da linha de schema ao abrir. O que o teste prova é o
     * caminho — o prazo chegou ao corpo, depois dos cabeçalhos.
     */
    @Test
    void queryTimeoutCoversTheStreamAndKeepsTheConnectionUsable() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            stub.query = Mode.HEADERS_THEN_STALL;
            long limit = 1_000 + NishiStatement.QUERY_TIMEOUT_GRACE_MS;
            long start = System.nanoTime();
            SQLException e = failureWithin(limit + 4_000, "a consulta com setQueryTimeout(1) e o corpo parado", () -> {
                try (Statement st = c.createStatement()) {
                    st.setQueryTimeout(1);
                    st.executeQuery("SELECT id FROM eventos").next();
                }
            });
            long ms = elapsedMs(start);
            assertInstanceOf(SQLTimeoutException.class, e, e.toString());
            assertEquals("57014", e.getSQLState(), e.getMessage());
            assertTrue(ms >= limit - 200, "o cliente desistiu em " + ms + " ms, antes do prazo de " + limit);
            assertTrue(ms <= limit + 2_000, "o cliente desistiu em " + ms + " ms, muito depois de " + limit);

            assertFalse(c.isClosed(), "o estouro do queryTimeout fechou a conexão");
            stub.query = Mode.ANSWER;
            within(5_000, "a consulta depois do estouro no stream", () -> {
                readAll(c, "depois do estouro no stream");
                return null;
            });
        }
    }

    /**
     * O queryTimeout SUBSTITUI um prazo de rede menor: quem declarou que a
     * consulta pode levar 2 s não a vê morta no prazo de rede de 300 ms.
     */
    @Test
    void queryTimeoutReplacesAShorterNetworkTimeout() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=300")) {
            stub.query = Mode.DELAYED;
            stub.queryDelayMs = 1_000;
            within(10_000, "a consulta com queryTimeout de 2 s e resposta em 1 s", () -> {
                try (Statement st = c.createStatement()) {
                    st.setQueryTimeout(2);
                    try (ResultSet rs = st.executeQuery("SELECT id FROM eventos")) {
                        assertTrue(rs.next());
                    }
                }
                return null;
            });
            assertFalse(c.isClosed());
        }
    }

    // ----------------------------------------------- socketTimeoutMs / rede

    /**
     * {@code socketTimeoutMs} limita uma requisição de metadata, e o estouro
     * FECHA a conexão — o contrato do {@code setNetworkTimeout} — com
     * {@code SQLRecoverableException} 08006.
     */
    @Test
    void socketTimeoutBoundsMetadataRequestsAndClosesTheConnection() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=500")) {
            stub.metadata = Mode.HANG;
            long start = System.nanoTime();
            SQLException e = failureWithin(5_000, "getTables com socketTimeoutMs=500",
                    () -> c.getMetaData().getTables(null, null, "%", null));
            long ms = elapsedMs(start);
            assertInstanceOf(SQLRecoverableException.class, e, e.toString());
            assertEquals("08006", e.getSQLState(), e.getMessage());
            assertTrue(ms >= 400 && ms <= 2_000, "getTables desistiu em " + ms + " ms, e o prazo é 500");
            assertTrue(c.isClosed(), "o estouro do prazo de rede não fechou a conexão");
            assertThrows(SQLException.class, c::createStatement);
        }
    }

    /** {@code socketTimeoutMs} limita a consulta que não tem queryTimeout. */
    @Test
    void socketTimeoutBoundsQueriesWithoutQueryTimeout() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=500")) {
            stub.query = Mode.HANG;
            long start = System.nanoTime();
            SQLException e = failureWithin(5_000, "executeQuery com socketTimeoutMs=500", () -> {
                try (Statement st = c.createStatement()) {
                    st.executeQuery("SELECT id FROM eventos");
                }
            });
            long ms = elapsedMs(start);
            assertInstanceOf(SQLRecoverableException.class, e, e.toString());
            assertEquals("08006", e.getSQLState(), e.getMessage());
            assertTrue(ms >= 400 && ms <= 2_000, "executeQuery desistiu em " + ms + " ms, e o prazo é 500");
            assertTrue(c.isClosed());
        }
    }

    /** {@code socketTimeoutMs} cobre cada espera do stream, depois dos cabeçalhos. */
    @Test
    void socketTimeoutBoundsEachWaitForTheStream() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=500")) {
            stub.query = Mode.HEADERS_THEN_STALL;
            SQLException e = failureWithin(5_000, "next() com socketTimeoutMs=500 e o corpo parado", () -> {
                try (Statement st = c.createStatement()) {
                    st.executeQuery("SELECT id FROM eventos").next();
                }
            });
            assertInstanceOf(SQLRecoverableException.class, e, e.toString());
            assertEquals("08006", e.getSQLState(), e.getMessage());
            assertTrue(e.getMessage().contains("prazo de rede"), e.getMessage());
            assertTrue(c.isClosed());
        }
    }

    /**
     * O prazo limita o SILÊNCIO do servidor, e não a duração: um stream que
     * leva bem mais que o prazo, mandando uma linha a cada 150 ms, e uma
     * aplicação que demora entre uma linha e outra, terminam inteiros com
     * {@code socketTimeoutMs=400}.
     */
    @Test
    void socketTimeoutDoesNotLimitTheDurationOfAFlowingStream() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=400")) {
            stub.query = Mode.TRICKLE;
            stub.trickleRows = 10;
            stub.rowIntervalMs = 150;
            long start = System.nanoTime();
            int rows = within(10_000, "o stream que flui devagar", () -> {
                int n = 0;
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT id FROM eventos")) {
                    while (rs.next()) {
                        n++;
                        if (n == 3) {
                            // A aplicação processando: não é espera pelo servidor.
                            Thread.sleep(700);
                        }
                    }
                }
                return n;
            });
            long ms = elapsedMs(start);
            assertEquals(10, rows);
            assertTrue(ms > 1_000, "o stream levou " + ms + " ms: não passou do prazo, e o teste não prova nada");
            assertFalse(c.isClosed());
        }
    }

    /**
     * {@code setNetworkTimeout} muda o prazo efetivo, e {@code getNetworkTimeout}
     * o devolve. É o que o HikariCP chama em volta da validação.
     */
    @Test
    void setNetworkTimeoutChangesTheEffectiveTimeout() throws Exception {
        Executor direct = Runnable::run;
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            assertEquals(0, c.getNetworkTimeout(), "sem socketTimeoutMs, o prazo de rede tem de ser zero");
            c.setNetworkTimeout(direct, 500);
            assertEquals(500, c.getNetworkTimeout());
            stub.query = Mode.HANG;
            long start = System.nanoTime();
            SQLException e = failureWithin(5_000, "executeQuery depois de setNetworkTimeout(500)", () -> {
                try (Statement st = c.createStatement()) {
                    st.executeQuery("SELECT id FROM eventos");
                }
            });
            long ms = elapsedMs(start);
            assertEquals("08006", e.getSQLState(), e.getMessage());
            assertTrue(ms >= 400 && ms <= 2_000, "executeQuery desistiu em " + ms + " ms, e o prazo é 500");
        }
    }

    /** O socketTimeoutMs da URL é o valor inicial do getNetworkTimeout. */
    @Test
    void socketTimeoutIsTheInitialNetworkTimeout() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "?socketTimeoutMs=1234")) {
            assertEquals(1234, c.getNetworkTimeout());
        }
    }

    @Test
    void setNetworkTimeoutRejectsNegativeTimeoutAndNullExecutor() throws Exception {
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            SQLException negative = assertThrows(SQLException.class, () -> c.setNetworkTimeout(Runnable::run, -1));
            assertEquals("22003", negative.getSQLState());
            assertThrows(SQLException.class, () -> c.setNetworkTimeout(null, 100));
            assertEquals(0, c.getNetworkTimeout(), "a recusa mudou o prazo assim mesmo");
        }
    }

    /**
     * Sem parâmetro nenhum, o comportamento é o de antes: a requisição ESPERA.
     *
     * <p>O prazo é do teste, curto, e a asserção é de que a consulta ainda não
     * voltou — quem não pediu prazo não pode ganhar um.
     */
    @Test
    void withoutAnyTimeoutTheRequestKeepsWaiting() throws Exception {
        Future<SQLException> pending;
        try (StubServer stub = new StubServer()) {
            Connection c = connect(stub, "");
            stub.query = Mode.HANG;
            pending = WORKERS.submit(() -> {
                try (Statement st = c.createStatement()) {
                    st.executeQuery("SELECT id FROM eventos");
                    return null;
                } catch (SQLException e) {
                    return e;
                }
            });
            assertTrue(stub.queryArrived.tryAcquire(5, TimeUnit.SECONDS), "a consulta não chegou ao dublê");
            assertThrows(TimeoutException.class, () -> pending.get(1_500, TimeUnit.MILLISECONDS),
                    "a consulta sem prazo nenhum voltou sozinha: o default deixou de ser sem prazo");
            c.close();
        }
        // Fechado o dublê, a consulta presa sai por queda de conexão.
        assertTrue(pending.get(10, TimeUnit.SECONDS) != null, "a consulta presa voltou sem erro");
    }

    // ----------------------------------------------------------------- abort

    /**
     * {@code abort} destrava a requisição presa em OUTRA thread, sem esperar o
     * servidor, e a conexão já está fechada quando ele volta.
     */
    @Test
    void abortUnblocksARequestStuckInAnotherThread() throws Exception {
        AtomicInteger executed = new AtomicInteger();
        Executor counting = r -> {
            executed.incrementAndGet();
            WORKERS.execute(r);
        };
        try (StubServer stub = new StubServer(); Connection c = connect(stub, "")) {
            stub.query = Mode.HANG;
            Future<SQLException> stuck = WORKERS.submit(() -> {
                try (Statement st = c.createStatement()) {
                    st.executeQuery("SELECT id FROM eventos");
                    return null;
                } catch (SQLException e) {
                    return e;
                }
            });
            assertTrue(stub.queryArrived.tryAcquire(5, TimeUnit.SECONDS), "a consulta não chegou ao dublê");

            long start = System.nanoTime();
            c.abort(counting);
            assertTrue(c.isClosed(), "abort voltou sem marcar a conexão fechada");
            SQLException e;
            try {
                e = stuck.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException t) {
                e = fail("abort não destravou a consulta presa em 3 s: só marcou a conexão fechada");
            }
            long ms = elapsedMs(start);
            assertTrue(e != null, "a consulta presa voltou sem erro depois do abort");
            assertTrue(e.getMessage().contains("fechada"), e.getMessage());
            assertTrue(ms <= 2_000, "abort levou " + ms + " ms para destravar a consulta");
            assertEquals(1, executed.get(), "abort não usou o executor recebido");
            assertThrows(SQLException.class, c::createStatement);
            assertFalse(c.isValid(1), "conexão abortada não pode ser válida");
        }
    }

    @Test
    void abortOnAClosedConnectionIsANoOpAndNullExecutorIsRejected() throws Exception {
        try (StubServer stub = new StubServer()) {
            Connection c = connect(stub, "");
            SQLException e = assertThrows(SQLException.class, () -> c.abort(null));
            assertFalse(c.isClosed(), "o abort recusado fechou a conexão");
            c.close();
            AtomicInteger executed = new AtomicInteger();
            c.abort(r -> executed.incrementAndGet());
            assertEquals(0, executed.get(), "abort numa conexão fechada fez alguma coisa");
            assertTrue(e.getMessage().contains("executor"), e.getMessage());
        }
    }

    // ------------------------------------------------------------- parâmetro

    @Test
    void socketTimeoutDefaultsToZeroAndPropertiesWinOverTheUrl() throws SQLException {
        assertEquals(0, ConnectionConfig.de("jdbc:nishidb://h/ds", null).socketTimeoutMs());
        assertEquals(250, ConnectionConfig.de("jdbc:nishidb://h/ds?socketTimeoutMs=250", null).socketTimeoutMs());
        Properties p = new Properties();
        p.setProperty("socketTimeoutMs", "900");
        assertEquals(900, ConnectionConfig.de("jdbc:nishidb://h/ds?socketTimeoutMs=250", p).socketTimeoutMs());
        assertEquals(0, ConnectionConfig.de("jdbc:nishidb://h/ds?socketTimeoutMs=0", null).socketTimeoutMs());
    }

    /**
     * Valor errado é recusado, e não lido como "sem prazo": um {@code -1}
     * digitado por engano desligaria o prazo em silêncio.
     */
    @Test
    void invalidSocketTimeoutIsRejected() {
        SQLException negative = assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?socketTimeoutMs=-1", null));
        assertTrue(negative.getMessage().contains("socketTimeoutMs"), negative.getMessage());
        SQLException notANumber = assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?socketTimeoutMs=5s", null));
        assertTrue(notANumber.getMessage().contains("socketTimeoutMs"), notANumber.getMessage());
    }
}
