package dev.nishisan.nishidb.jdbc.wire;

import dev.nishisan.nishidb.jdbc.json.JsonException;
import dev.nishisan.nishidb.jdbc.json.JsonParser;
import dev.nishisan.nishidb.jdbc.json.JsonValue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
 * </ul>
 */
public final class Transporte implements AutoCloseable {

    /** Tipo do corpo que o servidor exige no POST /v1/query. */
    private static final String TIPO_JSON = "application/json";

    private final HttpClient http;
    private final String base;
    private final String autorizacao;

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
     */
    public InputStream consulta(String corpoJson) throws SQLException {
        HttpRequest req = requisicao("/v1/query")
                .header("Content-Type", TIPO_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(corpoJson, StandardCharsets.UTF_8))
                .build();

        HttpResponse<InputStream> resp = envia(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() == 200) {
            return resp.body();
        }
        // O corpo do erro é pequeno e vem ANTES da primeira linha: fechá-lo
        // aqui, depois de lido, libera a conexão para a próxima consulta. O
        // javadoc do HttpClient é explícito de que um InputStream não
        // consumido nem fechado nunca devolve a conexão ao pool.
        try (InputStream corpo = resp.body()) {
            throw falhaDoServidor(resp.statusCode(), corpo);
        } catch (IOException e) {
            throw new SQLRecoverableException("lendo o corpo de erro: " + e.getMessage(),
                    SqlStates.CONEXAO_INTERROMPIDA);
        }
    }

    /** GET de um documento pequeno (/v1/metadata, /v1/stats, /v1/health). */
    public JsonValue documento(String caminho) throws SQLException {
        HttpRequest req = requisicao(caminho).GET().build();
        HttpResponse<InputStream> resp = envia(req, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream corpo = resp.body()) {
            if (resp.statusCode() != 200) {
                throw falhaDoServidor(resp.statusCode(), corpo);
            }
            return JsonParser.le(corpo);
        } catch (JsonException e) {
            throw new SQLRecoverableException(caminho + ": resposta ilegivel: " + e.getMessage(),
                    SqlStates.CONEXAO_INTERROMPIDA);
        } catch (IOException e) {
            throw new SQLRecoverableException(caminho + ": " + e.getMessage(), SqlStates.CONEXAO_INTERROMPIDA);
        }
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

    private HttpRequest.Builder requisicao(String caminho) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + caminho))
                // Fixo, e não negociado: ver a doutrina na classe.
                .version(HttpClient.Version.HTTP_1_1);
        if (autorizacao != null) {
            // PREEMPTIVO: sem isto, o primeiro request levaria 401 e só o
            // segundo a credencial. O servidor responde 401 com um atraso
            // fixo de 100 ms em toda falha, então o desafio-resposta custaria
            // esse atraso em CADA consulta.
            b.header("Authorization", autorizacao);
        }
        return b;
    }

    private <T> HttpResponse<T> envia(HttpRequest req, HttpResponse.BodyHandler<T> handler) throws SQLException {
        HttpResponse<T> resp;
        try {
            resp = http.send(req, handler);
        } catch (IOException e) {
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
