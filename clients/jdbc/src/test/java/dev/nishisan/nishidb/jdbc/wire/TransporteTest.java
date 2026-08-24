package dev.nishisan.nishidb.jdbc.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Testes do transporte contra um servidor HTTP DE VERDADE.
 *
 * <p>O servidor vem do próprio JDK (com.sun.net.httpserver), então não há
 * dependência de teste nova — a doutrina de zero dependência vale para o
 * escopo de teste também, onde puder.
 */
class TransporteTest {

    /** Sobe um servidor efêmero, roda o corpo do teste e o derruba. */
    private static void comServidor(Consumer<HttpExchange> handler, Consumer<Transporte> corpo) throws IOException {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", handler::accept);
        srv.start();
        try {
            String base = "http://127.0.0.1:" + srv.getAddress().getPort();
            try (Transporte t = new Transporte(
                    Transporte.clienteHttp(Duration.ofSeconds(5), null), base, null)) {
                corpo.accept(t);
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

    /**
     * O cliente NUNCA honra proxy, e isso é verificável antes de qualquer
     * requisição.
     *
     * <p>É a doutrina que mais custa quando falha: o DBeaver seta as
     * propriedades de proxy globalmente, e no piloto o proxy corporativo
     * responde 403 a 127.0.0.1 — o sintoma parece o servidor fora do ar, e o
     * diagnóstico vai para o lugar errado.
     */
    @Test
    void oClienteNaoHonraProxyNemSegueRedirect() {
        HttpClient c = Transporte.clienteHttp(Duration.ofSeconds(5), null);
        Optional<ProxySelector> proxy = c.proxy();
        assertTrue(proxy.isEmpty() || proxy.get() == HttpClient.Builder.NO_PROXY,
                "o cliente honraria as propriedades de proxy da JVM");
        assertEquals(HttpClient.Redirect.NEVER, c.followRedirects());
    }

    /** O caminho feliz: o corpo do stream chega para o leitor. */
    @Test
    void aConsultaDevolveOCorpoParaOLeitor() throws IOException {
        comServidor(ex -> responde(ex, 200, "{\"nishidb\":\"schema\",\"dataset\":\"d\",\"columns\":[]}\n"
                + "{\"nishidb\":\"end\",\"rows\":0,\"truncated\":false}\n"), t -> {
                    try (LeitorNdjson l = new LeitorNdjson(t.consulta("{}"))) {
                        while (l.proxima()) {
                            // consome
                        }
                        assertNotNull(l.schema());
                        assertEquals(0, l.fim().rows());
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    /** O corpo de erro do servidor vira a SQLException certa, com a posição. */
    @Test
    void oCorpoDeErroViraSqlExceptionComPosicao() throws IOException {
        comServidor(ex -> responde(ex, 400,
                "{\"error\":{\"code\":\"UNSUPPORTED_CLAUSE\",\"message\":\"ORDER BY\",\"line\":1,\"column\":18}}"),
                t -> {
                    SQLException e = assertThrows(SQLException.class, () -> t.consulta("{}"));
                    assertEquals("0A000", e.getSQLState());
                    assertTrue(e.getMessage().contains("ORDER BY"), e.getMessage());
                    assertTrue(e.getMessage().contains("linha 1"), e.getMessage());
                    assertTrue(e.getMessage().contains("coluna 18"), e.getMessage());
                });
    }

    /**
     * Redirect não é seguido NEM aceito como resposta.
     *
     * <p>Seguido, reenviaria o header Authorization para outro host. Aceito
     * como corpo válido, daria um ResultSet vazio — que é pior, porque não
     * parece erro nenhum.
     */
    @Test
    void redirectEhRecusadoEmVezDeViraResultadoVazio() throws IOException {
        comServidor(ex -> {
            ex.getResponseHeaders().add("Location", "http://outro-host/v1/query");
            responde(ex, 302, "");
        }, t -> {
            SQLException e = assertThrows(SQLException.class, () -> t.consulta("{}"));
            assertEquals("08P01", e.getSQLState());
            assertTrue(e.getMessage().contains("credencial"), e.getMessage());
        });
    }

    /**
     * Corpo que não é o JSON esperado sugere o esquema.
     *
     * <p>É o espelho da dica do CLI: uma porta TLS respondendo a texto puro
     * devolve "Client sent an HTTP request to an HTTPS server", e sem a dica o
     * usuário do DBeaver vê "resposta ilegivel" e culpa o servidor.
     */
    @Test
    void corpoNaoJsonSugereOEsquema() throws IOException {
        comServidor(ex -> responde(ex, 400, "Client sent an HTTP request to an HTTPS server."), t -> {
            SQLException e = assertThrows(SQLException.class, () -> t.consulta("{}"));
            assertTrue(e.getMessage().contains("tls"), e.getMessage());
        });
    }

    /** A credencial vai PREEMPTIVA, no primeiro request. */
    @Test
    void aCredencialVaiNoPrimeiroRequest() throws IOException {
        List<String> vistos = new ArrayList<>();
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", ex -> {
            vistos.add(String.valueOf(ex.getRequestHeaders().getFirst("Authorization")));
            responde(ex, 200, "{\"ok\":true}");
        });
        srv.start();
        try {
            String base = "http://127.0.0.1:" + srv.getAddress().getPort();
            try (Transporte t = new Transporte(Transporte.clienteHttp(Duration.ofSeconds(5), null), base,
                    Transporte.basic("leitor", "segredo"))) {
                t.documento("/v1/metadata");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        } finally {
            srv.stop(0);
        }
        assertEquals(1, vistos.size(), "houve desafio-resposta: a credencial nao foi preemptiva");
        assertEquals("Basic bGVpdG9yOnNlZ3JlZG8=", vistos.get(0));
    }

    /** GET de documento pequeno devolve o DOM. */
    @Test
    void oDocumentoPequenoVoltaComoJsonValue() throws IOException {
        comServidor(ex -> responde(ex, 200, "{\"process\":{\"version\":\"v1.2.3 (abc)\"}}"), t -> {
            try {
                assertEquals("v1.2.3 (abc)", t.documento("/v1/stats").campo("process").textoOu("version", null));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Servidor inalcançável vira erro de CONEXÃO, não de consulta. */
    @Test
    void servidorInalcancavelEhErroDeConexao() throws IOException {
        // Porta fechada: sobe e derruba para garantir que ninguém a ocupa.
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int porta = srv.getAddress().getPort();
        srv.start();
        srv.stop(0);
        try (Transporte t = new Transporte(Transporte.clienteHttp(Duration.ofSeconds(2), null),
                "http://127.0.0.1:" + porta, null)) {
            SQLException e = assertThrows(SQLException.class, () -> t.consulta("{}"));
            assertEquals("08001", e.getSQLState());
        }
    }

    /** InputStream do corpo de erro é FECHADO: senão a conexão nunca volta. */
    @Test
    void oCorpoDeErroNaoVazaConexao() throws IOException {
        comServidor(ex -> responde(ex, 500, "{\"error\":{\"code\":\"INTERNAL\",\"message\":\"x\"}}"), t -> {
            // Vinte erros seguidos: com o corpo vazando, o pool interno do
            // HttpClient acabaria e a vigésima chamada travaria.
            for (int i = 0; i < 20; i++) {
                assertThrows(SQLException.class, () -> t.consulta("{}"));
            }
        });
    }

    /** Consumir o stream até o fim libera a conexão para a consulta seguinte. */
    @Test
    void streamsConsecutivosNaoEsgotamOPool() throws IOException {
        comServidor(ex -> responde(ex, 200, "{\"nishidb\":\"schema\",\"dataset\":\"d\",\"columns\":[]}\n"
                + "{\"nishidb\":\"end\",\"rows\":0,\"truncated\":false}\n"), t -> {
                    for (int i = 0; i < 20; i++) {
                        try (InputStream in = t.consulta("{}"); LeitorNdjson l = new LeitorNdjson(in)) {
                            while (l.proxima()) {
                                // consome
                            }
                        } catch (SQLException | IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                });
    }
}
