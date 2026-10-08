package dev.nishisan.nishidb.jdbc.wire;

import dev.nishisan.nishidb.jdbc.json.JsonException;
import dev.nishisan.nishidb.jdbc.json.JsonParser;
import dev.nishisan.nishidb.jdbc.json.JsonValue;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.util.Base64;

/**
 * O ÚNICO ponto do driver que monta uma requisição HTTP.
 *
 * <p>Único de propósito: as doutrinas abaixo não são opcionais, e espalhá-las
 * por vários chamadores garantiria que uma delas fosse esquecida em algum.
 * Todas vêm do cliente de referência do CLI (internal/cli/credential.go), e
 * cada uma existe por causa de um incidente concreto:
 *
 * <ul>
 *   <li><b>NO_PROXY sempre.</b> O HttpClient default honra as propriedades
 *       http.proxyHost e http.nonProxyHosts — que o DBeaver seta globalmente
 *       quando o usuário configura proxy. No piloto há proxy corporativo que
 *       responde 403 a 127.0.0.1, e o sintoma engana: parece o servidor fora do
 *       ar. É a tradução literal do {@code Proxy: nil} do cliente Go.
 *   <li><b>HTTP/1.1 fixo.</b> O servidor sobe TLS sem NextProtos próprio, então
 *       o Go habilita h2 via ALPN e o HttpClient prefere HTTP/2. Não ganhamos
 *       nada com multiplexação — uma conexão JDBC executa uma consulta por vez
 *       — e perdemos o controle de fluxo simples num stream longo.
 *   <li><b>Sem redirect.</b> Um 3xx seguido cegamente reenviaria o header
 *       Authorization para outro host. O default do HttpClient já é NEVER, mas
 *       aí o 3xx chega como RESPOSTA — e tratá-lo como corpo válido daria um
 *       ResultSet vazio em vez de erro.
 *   <li><b>Nenhum trustAll, nem por propriedade.</b> Uma opção "insecure" vira
 *       o modo permanente de operação em três semanas. Quem depura handshake
 *       usa openssl s_client.
 *   <li><b>Toda requisição declara o seu prazo.</b> {@link #consulta} e
 *       {@link #documento} exigem um {@link RequestTimeout}, sem sobrecarga
 *       que o omita: foi a falta de prazo em TODA requisição que deixou um
 *       servidor que aceita e não responde travar a thread do cliente sem
 *       limite (issue #91), e um chamador novo não pode esquecê-lo de novo.
 *       {@link RequestTimeout#NONE} continua existindo, mas tem de ser
 *       escrito.
 * </ul>
 */
public final class Transporte implements AutoCloseable {

    /** Tipo do corpo que o servidor exige no POST /v1/query. */
    private static final String TIPO_JSON = "application/json";

    private final HttpClient http;
    private final String base;
    private final String autorizacao;

    /**
     * Ligado por {@link #abandon()}: a falha de I/O que vier depois é a da
     * conexão derrubada de propósito, e não do servidor.
     */
    private volatile boolean abandoned;

    /**
     * @param base URL base, já com esquema — {@code http://host:porta} ou
     *             {@code https://host:porta}
     * @param autorizacao valor do header Authorization, ou null
     */
    public Transporte(HttpClient http, String base, String autorizacao) {
        this.http = http;
        this.base = base;
        this.autorizacao = autorizacao;
    }

    /**
     * Constrói o HttpClient com as doutrinas da classe aplicadas.
     *
     * <p>É AQUI que NO_PROXY e a recusa de redirect entram, e não em cada
     * chamador: são as duas que, esquecidas, falham de forma que parece outra
     * coisa — proxy corporativo respondendo 403 parece servidor fora do ar, e
     * redirect seguido vaza credencial sem nenhum sintoma.
     *
     * @param contexto SSLContext para TLS com CA própria, ou null para o padrão
     */
    public static HttpClient clienteHttp(Duration timeoutDeConexao, javax.net.ssl.SSLContext contexto) {
        HttpClient.Builder b = HttpClient.newBuilder()
                .proxy(HttpClient.Builder.NO_PROXY)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(timeoutDeConexao);
        if (contexto != null) {
            b.sslContext(contexto);
        }
        return b.build();
    }

    /** O valor do header Basic, montado sem manter a senha em campo. */
    public static String basic(String usuario, String senha) {
        String par = usuario + ":" + (senha == null ? "" : senha);
        return "Basic " + Base64.getEncoder().encodeToString(par.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Executa a consulta e devolve o corpo para o {@link LeitorNdjson}.
     *
     * <p>{@code BodyHandlers.ofInputStream} é o único handler correto: ele
     * retorna nos headers e deixa o corpo fluir. {@code ofString} e
     * {@code ofByteArray} bufferizam o resultado inteiro, que é violação direta
     * do §128; {@code ofLines} aloca uma String por linha e destrói os spans.
     *
     * <p>O prazo vale para a espera pelos cabeçalhos e para CADA espera por
     * bytes do corpo, e não para a duração do stream — ver
     * {@link RequestTimeout}.
     */
    public InputStream consulta(String corpoJson, RequestTimeout timeout) throws SQLException {
        HttpRequest req = requisicao("/v1/query", timeout)
                .header("Content-Type", TIPO_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(corpoJson, StandardCharsets.UTF_8))
                .build();

        HttpResponse<InputStream> resp = envia(req, HttpResponse.BodyHandlers.ofInputStream(), timeout);
        InputStream body = body(resp.body(), timeout, null);
        if (resp.statusCode() == 200) {
            return body;
        }
        // O corpo do erro é pequeno e vem ANTES da primeira linha: fechá-lo
        // aqui, depois de lido, libera a conexão para a próxima consulta. O
        // javadoc do HttpClient é explícito de que um InputStream não
        // consumido nem fechado nunca devolve a conexão ao pool.
        try (InputStream corpo = body) {
            throw falhaDoServidor(resp.statusCode(), corpo);
        } catch (IOException e) {
            throw new SQLRecoverableException("lendo o corpo de erro: " + e.getMessage(),
                    SqlStates.CONEXAO_INTERROMPIDA);
        }
    }

    /**
     * GET de um documento pequeno (/v1/metadata, /v1/stats, /v1/health).
     *
     * <p>Aqui o prazo é da requisição INTEIRA — cabeçalhos e corpo somados —,
     * e não de cada espera: o documento é pequeno, e é este o caminho do
     * {@code isValid(s)}, cujo contrato é voltar em {@code s}.
     */
    public JsonValue documento(String caminho, RequestTimeout timeout) throws SQLException {
        long start = System.nanoTime();
        HttpRequest req = requisicao(caminho, timeout).GET().build();
        HttpResponse<InputStream> resp = envia(req, HttpResponse.BodyHandlers.ofInputStream(), timeout);
        Long deadline = start + Duration.ofMillis(timeout.millis()).toNanos();
        try (InputStream corpo = body(resp.body(), timeout, deadline)) {
            if (resp.statusCode() != 200) {
                throw falhaDoServidor(resp.statusCode(), corpo);
            }
            return JsonParser.le(corpo);
        } catch (ResponseFailureException e) {
            throw e.failure();
        } catch (JsonException e) {
            throw new SQLRecoverableException(caminho + ": resposta ilegivel: " + e.getMessage(),
                    SqlStates.CONEXAO_INTERROMPIDA);
        } catch (IOException e) {
            throw new SQLRecoverableException(caminho + ": " + e.getMessage(), SqlStates.CONEXAO_INTERROMPIDA);
        }
    }

    /**
     * Derruba o cliente HTTP SEM esperar: as requisições em voo, em qualquer
     * thread, saem com {@code SQLException} de conexão fechada.
     *
     * <p>É o caminho do {@code Connection.abort} e do estouro do prazo de
     * rede. Diferente de {@link #close()}, não chama
     * {@code awaitTermination}: quem aborta não pode ficar esperando o
     * servidor que justamente parou de responder.
     */
    public void abandon() {
        abandoned = true;
        http.shutdownNow();
    }

    @Override
    public void close() {
        // shutdownNow + espera curta, e não close(): o close() do HttpClient
        // BLOQUEIA até todo exchange terminar, e Connection.close() é chamado
        // da thread de UI de uma ferramenta de SQL — que travaria enquanto um
        // stream longo drena.
        http.shutdownNow();
        try {
            http.awaitTermination(Duration.ofSeconds(2));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private HttpRequest.Builder requisicao(String caminho, RequestTimeout timeout) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + caminho))
                // Fixo, e não negociado: ver a doutrina na classe.
                .version(HttpClient.Version.HTTP_1_1);
        if (timeout.isSet()) {
            // Cobre a espera até os CABEÇALHOS, e só ela: com ofInputStream o
            // send volta nos cabeçalhos e o prazo do JDK para de contar. O
            // corpo fica com o TimedInputStream.
            b.timeout(Duration.ofMillis(timeout.millis()));
        }
        if (autorizacao != null) {
            // PREEMPTIVO: sem isto, o primeiro request levaria 401 e só o
            // segundo a credencial. O servidor responde 401 com um atraso
            // fixo de 100 ms em toda falha, então o desafio-resposta custaria
            // esse atraso em CADA consulta.
            b.header("Authorization", autorizacao);
        }
        return b;
    }

    /**
     * O corpo como quem lê o recebe: com o prazo, quando há, e com a falha de
     * leitura depois de um {@link #abandon()} traduzida em conexão fechada.
     */
    private InputStream body(InputStream raw, RequestTimeout timeout, Long deadlineNanos) {
        InputStream in = timeout.isSet() ? new TimedInputStream(raw, timeout, deadlineNanos) : raw;
        return new AbandonAwareInputStream(in);
    }

    /**
     * A exceção da requisição que estava em voo quando a conexão foi
     * derrubada de propósito — esperando os cabeçalhos ou lendo o corpo.
     */
    private static SQLException closedInFlight(Throwable cause) {
        return new SQLNonTransientConnectionException("esta conexao foi fechada (abort ou estouro do prazo de rede) "
                + "com a requisicao em voo", SqlStates.CONNECTION_DOES_NOT_EXIST, cause);
    }

    /**
     * Traduz a falha de leitura do corpo depois de um {@link #abandon()}.
     *
     * <p>Sem isto, a thread presa no corpo quando o abort chega sairia com a
     * queda genérica do {@link LeitorNdjson} — 08006 "lendo a resposta:
     * closed" —, que manda procurar um problema de rede onde houve um abort.
     * A falha que já vem montada ({@link ResponseFailureException}, do prazo)
     * passa intacta: o estouro do prazo de rede também abandona o transporte,
     * e a exceção certa ali é a do prazo.
     */
    private final class AbandonAwareInputStream extends FilterInputStream {

        AbandonAwareInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            try {
                return in.read();
            } catch (IOException e) {
                throw translate(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                return in.read(b, off, len);
            } catch (IOException e) {
                throw translate(e);
            }
        }

        @Override
        public long skip(long n) throws IOException {
            try {
                return in.skip(n);
            } catch (IOException e) {
                throw translate(e);
            }
        }

        private IOException translate(IOException e) {
            if (e instanceof ResponseFailureException || !abandoned) {
                return e;
            }
            return new ResponseFailureException(closedInFlight(e));
        }
    }

    private <T> HttpResponse<T> envia(HttpRequest req, HttpResponse.BodyHandler<T> handler, RequestTimeout timeout)
            throws SQLException {
        HttpResponse<T> resp;
        try {
            resp = http.send(req, handler);
        } catch (HttpConnectTimeoutException e) {
            // Também é um HttpTimeoutException, mas é o handshake, que tem
            // prazo próprio (connectTimeoutMs): não é o silêncio de um
            // servidor que aceitou a requisição.
            throw new SQLNonTransientConnectionException(
                    "falando com " + base + ": " + e.getMessage(), SqlStates.CONEXAO_NAO_ESTABELECIDA, e);
        } catch (HttpTimeoutException e) {
            SQLException falha = timeout.onExpiry().onExpiry(
                    "o servidor nao respondeu em " + timeout.millis() + " ms");
            if (falha.getCause() == null) {
                falha.initCause(e);
            }
            throw falha;
        } catch (IOException e) {
            if (abandoned) {
                throw closedInFlight(e);
            }
            throw new SQLNonTransientConnectionException(
                    "falando com " + base + ": " + e.getMessage(), SqlStates.CONEXAO_NAO_ESTABELECIDA, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLNonTransientConnectionException("consulta interrompida",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA, e);
        }
        int status = resp.statusCode();
        if (status >= 300 && status < 400) {
            // O redirect não é seguido, e também não é aceito como resposta:
            // tratá-lo como corpo válido daria um ResultSet vazio, e segui-lo
            // reenviaria o Authorization para outro host.
            throw new SQLNonTransientConnectionException(
                    "o servidor respondeu um redirecionamento (" + status + "), que este driver nao segue: "
                            + "a credencial nao pode ser reenviada para outro host",
                    SqlStates.PROTOCOLO_VIOLADO);
        }
        return resp;
    }

    /**
     * Traduz uma resposta de erro do servidor.
     *
     * <p>Quando o corpo não é o JSON esperado, a mensagem SUGERE o esquema — é
     * o espelho da dica que o CLI dá: uma porta TLS respondendo a texto puro
     * devolve "Client sent an HTTP request to an HTTPS server", e sem a dica o
     * usuário vê "resposta ilegivel" e culpa o servidor.
     */
    private SQLException falhaDoServidor(int status, InputStream corpo) {
        String texto = "";
        try {
            JsonValue v = JsonParser.le(corpo);
            JsonValue erro = v.campo("error");
            if (erro != null) {
                String codigo = erro.textoOu("code", Codigos.INTERNAL);
                String mensagem = erro.textoOu("message", "sem mensagem");
                int linha = (int) erro.inteiroOu("line", 0);
                if (linha > 0) {
                    mensagem += " (linha " + linha + ", coluna " + erro.inteiroOu("column", 0) + ")";
                }
                return SqlStates.excecaoDe(codigo, mensagem);
            }
        } catch (ResponseFailureException e) {
            return e.failure();
        } catch (JsonException | IOException e) {
            texto = e.getMessage();
        }
        String dica = base.startsWith("http://")
                ? " — se a porta fala TLS, ligue a propriedade tls"
                : "";
        return new SQLNonTransientConnectionException(
                "o servidor respondeu " + status + " sem um corpo de erro reconhecivel" + dica
                        + (texto.isEmpty() ? "" : " [" + texto + "]"),
                SqlStates.PROTOCOLO_VIOLADO);
    }
}
