package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.Transporte;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * A configuração de uma conexão, resolvida de uma vez a partir da URL e das
 * {@link Properties}.
 *
 * <p><b>Properties vence a URL</b>, e a precedência é explícita porque as duas
 * fontes existem por motivos diferentes: a URL é o que o usuário digita e o que
 * aparece em log, e as Properties são o que a aplicação passa em código —
 * inclusive a senha, que por isso NÃO é aceita na URL.
 *
 * <p><b>Propriedade desconhecida é ignorada; valor errado numa propriedade
 * nossa é RECUSADO.</b> A assimetria é deliberada. As ferramentas passam
 * propriedades próprias na mesma Properties da conexão, e recusá-las impediria
 * a conexão de abrir por causa de algo que não é nosso. Já um {@code tls=1}
 * tratado como "não é true, então é falso" conectaria em texto puro em
 * silêncio, e quem escreveu acharia que estava em TLS — o modo de falha de uma
 * configuração de segurança não pode ser o silêncio.
 */
public final class ConnectionConfig {

    /** As propriedades que este driver conhece, com o texto que a ferramenta mostra. */
    private static final String[][] CONHECIDAS = {
        {"user", "usuário do Basic (§74). Sem ele, a conexão vai sem credencial."},
        {"password", "senha do Basic. NÃO é aceita na URL: ela apareceria em log e em stack trace."},
        {"tls", "true para falar HTTPS. Default false."},
        {"tlsCaFile", "arquivo PEM com a CA que assina o certificado do servidor. Exige tls=true."},
        {"connectTimeoutMs", "prazo do handshake, em ms. Default 10000. NÃO limita a duração da consulta."},
        {"socketTimeoutMs", "prazo de espera pelo servidor em cada requisição, em ms: cabeçalhos e cada leitura"
                + " do corpo, não a duração do stream. Default 0, sem prazo. Estourado, fecha a conexão."
                + " É o valor inicial de getNetworkTimeout."},
    };

    private final String url;
    private final String host;
    private final int porta;
    private final String schema;
    private final String usuario;
    private final String senha;
    private final boolean tls;
    private final String tlsCaFile;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    private ConnectionConfig(String url, String host, int porta, String schema, String usuario, String senha,
            boolean tls, String tlsCaFile, int connectTimeoutMs, int socketTimeoutMs) {
        this.url = url;
        this.host = host;
        this.porta = porta;
        this.schema = schema;
        this.usuario = usuario;
        this.senha = senha;
        this.tls = tls;
        this.tlsCaFile = tlsCaFile;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public static ConnectionConfig de(String url, Properties props) throws SQLException {
        NishiUrl u = NishiUrl.parse(url);
        Map<String, String> daUrl = u.propriedades();

        String usuario = escolhe(props, daUrl, "user");
        String senha = escolhe(props, daUrl, "password");
        boolean tls = booleano(escolhe(props, daUrl, "tls"));
        String ca = escolhe(props, daUrl, "tlsCaFile");
        if (ca != null && !ca.isEmpty() && !tls) {
            throw new SQLException("tlsCaFile foi declarado sem tls=true: a conexão iria em texto puro "
                    + "e o certificado não seria verificado", SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        int prazo = prazo(escolhe(props, daUrl, "connectTimeoutMs"));
        int socketTimeout = parseSocketTimeout(escolhe(props, daUrl, "socketTimeoutMs"));
        return new ConnectionConfig(url, u.host(), u.porta(), u.schema(), usuario, senha, tls, ca, prazo,
                socketTimeout);
    }

    /** A URL como o usuário a escreveu — é o que getURL do metadata publica. */
    public String url() {
        return url;
    }

    public String host() {
        return host;
    }

    public int porta() {
        return porta;
    }

    /** O schema da conexão, o caminho da URL. */
    public String schema() {
        return schema;
    }

    public String usuario() {
        return usuario;
    }

    public boolean tls() {
        return tls;
    }

    public int connectTimeoutMs() {
        return connectTimeoutMs;
    }

    /**
     * O prazo de espera pelo servidor em cada requisição, em ms; zero é sem
     * prazo. É o valor inicial do {@code getNetworkTimeout} da conexão.
     */
    public int socketTimeoutMs() {
        return socketTimeoutMs;
    }

    /** A base HTTP, com o esquema que o tls decide. */
    public String base() {
        return (tls ? "https://" : "http://") + host + ":" + porta;
    }

    /** O header Authorization, ou null quando não há usuário. */
    public String autorizacao() {
        return usuario == null ? null : Transporte.basic(usuario, senha);
    }

    /**
     * O SSLContext da CA declarada, ou {@code null} para usar o do JDK.
     *
     * <p>Null significa "confie em quem o truststore do JDK confia", que é o
     * certo para um certificado de CA pública. NÃO existe caminho para
     * desligar a verificação: uma propriedade "insecure" vira o modo permanente
     * de operação em três semanas, e quem depura handshake usa openssl.
     */
    public SSLContext contextoTls() throws SQLException {
        if (tlsCaFile == null || tlsCaFile.isEmpty()) {
            return null;
        }
        Path caminho = Path.of(tlsCaFile);
        try (InputStream in = Files.newInputStream(caminho)) {
            Collection<? extends Certificate> certs =
                    CertificateFactory.getInstance("X.509").generateCertificates(in);
            if (certs.isEmpty()) {
                throw new SQLException("nenhum certificado em " + tlsCaFile
                        + ": o arquivo precisa ser um PEM com a CA do servidor",
                        SqlStates.CONEXAO_NAO_ESTABELECIDA);
            }
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            int i = 0;
            for (Certificate c : certs) {
                ks.setCertificateEntry("ca-" + i++, c);
            }
            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            return ctx;
        } catch (IOException e) {
            throw new SQLException("lendo a CA em " + tlsCaFile + ": " + e.getMessage(),
                    SqlStates.CONEXAO_NAO_ESTABELECIDA, e);
        } catch (GeneralSecurityException e) {
            throw new SQLException("a CA em " + tlsCaFile + " não serve: " + e.getMessage(),
                    SqlStates.CONEXAO_NAO_ESTABELECIDA, e);
        }
    }

    /** O que a ferramenta mostra na tela de conexão. */
    public static DriverPropertyInfo[] propriedades(Properties props) {
        DriverPropertyInfo[] out = new DriverPropertyInfo[CONHECIDAS.length];
        for (int i = 0; i < CONHECIDAS.length; i++) {
            String nome = CONHECIDAS[i][0];
            DriverPropertyInfo p = new DriverPropertyInfo(nome,
                    props == null ? null : props.getProperty(nome));
            p.description = CONHECIDAS[i][1];
            p.required = false;
            if ("tls".equals(nome)) {
                p.choices = new String[] {"true", "false"};
            }
            out[i] = p;
        }
        return out;
    }

    private static String escolhe(Properties props, Map<String, String> daUrl, String nome) {
        if (props != null) {
            String v = props.getProperty(nome);
            if (v != null) {
                return v;
            }
        }
        return daUrl.get(nome);
    }

    private static boolean booleano(String v) throws SQLException {
        if (v == null || v.isEmpty()) {
            return false;
        }
        if ("true".equalsIgnoreCase(v)) {
            return true;
        }
        if ("false".equalsIgnoreCase(v)) {
            return false;
        }
        throw new SQLException("tls=\"" + v + "\" não é true nem false; um valor não reconhecido "
                + "tratado como falso conectaria em texto puro em silêncio",
                SqlStates.CONEXAO_NAO_ESTABELECIDA);
    }

    private static int prazo(String v) throws SQLException {
        if (v == null || v.isEmpty()) {
            return 10_000;
        }
        int ms;
        try {
            ms = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new SQLException("connectTimeoutMs=\"" + v + "\" não é um número",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        if (ms <= 0) {
            throw new SQLException("connectTimeoutMs precisa ser positivo (recebido " + ms + ")",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        return ms;
    }

    /**
     * Zero é aceito, e é o default: "sem prazo" é o comportamento que o
     * driver sempre teve, e quem não pediu prazo não pode ganhar um. Negativo
     * é recusado, e não lido como zero — um {@code -1} digitado por engano
     * desligaria o prazo em silêncio.
     */
    private static int parseSocketTimeout(String v) throws SQLException {
        if (v == null || v.isEmpty()) {
            return 0;
        }
        int ms;
        try {
            ms = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new SQLException("socketTimeoutMs=\"" + v + "\" não é um número",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        if (ms < 0) {
            throw new SQLException("socketTimeoutMs não pode ser negativo (recebido " + ms + "); 0 é sem prazo",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        return ms;
    }
}
