package dev.nishisan.nishidb.jdbc;

import java.nio.charset.StandardCharsets;
import java.net.URLDecoder;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A URL de conexão do §70: {@code jdbc:nishidb://host[:porta]/dataset[?k=v]}.
 *
 * <p>Parser à mão, e não {@link java.net.URI}: a URI do JDK exige um esquema
 * único no começo, e {@code jdbc:nishidb://...} tem dois. Quem usa URI acaba
 * fatiando a string antes de chamá-la, que é o que esta classe faz de forma
 * direta e testável.
 *
 * <p>A porta default é 9142, a mesma que o servidor usa quando o YAML não
 * declara {@code server.port} — a URL mais curta que alguém digita no DBeaver
 * tem de funcionar contra um servidor de configuração default.
 */
public final class NishiUrl {

    /** O prefixo do §70. */
    public static final String PREFIXO = "jdbc:nishidb://";

    /** A porta default, igual ao serverPortPadrao do servidor. */
    public static final int PORTA_PADRAO = 9142;

    private final String host;
    private final int porta;
    private final String dataset;
    private final Map<String, String> propriedades;

    private NishiUrl(String host, int porta, String dataset, Map<String, String> propriedades) {
        this.host = host;
        this.porta = porta;
        this.dataset = dataset;
        this.propriedades = Collections.unmodifiableMap(propriedades);
    }

    public String host() {
        return host;
    }

    public int porta() {
        return porta;
    }

    public String dataset() {
        return dataset;
    }

    /** As propriedades da query string, na ordem em que apareceram. */
    public Map<String, String> propriedades() {
        return propriedades;
    }

    /** {@code host:porta}, pronto para montar a URL HTTP. */
    public String autoridade() {
        return host + ":" + porta;
    }

    /**
     * Responde se a URL é nossa. NUNCA lança, nem para null.
     *
     * <p>É o contrato de {@link java.sql.Driver#acceptsURL}: o DriverManager
     * percorre todos os drivers registrados perguntando a cada um, e um que
     * lance aborta a busca inteira — o driver do NishiDB quebraria a conexão
     * que o usuário estava abrindo com outro banco.
     */
    public static boolean aceita(String url) {
        return url != null && url.startsWith(PREFIXO);
    }

    /**
     * Decompõe a URL, ou explica por que ela não serve.
     */
    public static NishiUrl parse(String url) throws SQLException {
        if (!aceita(url)) {
            throw new SQLException("a URL não é do NishiDB; a forma é " + PREFIXO + "host[:porta]/dataset",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        String resto = url.substring(PREFIXO.length());

        String query = "";
        int interrogacao = resto.indexOf('?');
        if (interrogacao >= 0) {
            query = resto.substring(interrogacao + 1);
            resto = resto.substring(0, interrogacao);
        }

        // Credencial embutida é RECUSADA, e não ignorada: URL de JDBC aparece
        // em log de aplicação, em stack trace e na tela de conexão da
        // ferramenta. Ignorá-la em silêncio seria pior que recusar — o usuário
        // acharia que autenticou.
        if (resto.indexOf('@') >= 0) {
            throw new SQLException(
                    "credencial na URL não é aceita: a senha apareceria em log e em stack trace; "
                            + "use as Properties user e password",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }

        int barra = resto.indexOf('/');
        if (barra < 0) {
            throw new SQLException("a URL não nomeia o dataset; a forma é " + PREFIXO + "host[:porta]/dataset",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        String autoridade = resto.substring(0, barra);
        String caminho = resto.substring(barra + 1);

        if (caminho.isEmpty()) {
            throw new SQLException("a URL não nomeia o dataset; a forma é " + PREFIXO + "host[:porta]/dataset",
                    SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        if (caminho.indexOf('/') >= 0) {
            // O NishiDB não tem catálogo nem schema. Aceitar /a/b faria o
            // usuário supor uma hierarquia que o produto não tem, e a falha
            // só apareceria como "dataset desconhecido" depois de conectar.
            throw new SQLException("o caminho da URL tem mais de um segmento: o NishiDB não tem catálogo nem schema, "
                    + "e o único segmento é o nome do dataset", SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }

        String host = autoridade;
        int porta = PORTA_PADRAO;
        // Colchetes primeiro: num IPv6 sem eles, o ':' do endereço viraria
        // separador de porta e o host sairia truncado.
        int doisPontos = autoridade.startsWith("[")
                ? autoridade.indexOf(':', autoridade.indexOf(']'))
                : autoridade.indexOf(':');
        if (doisPontos >= 0) {
            host = autoridade.substring(0, doisPontos);
            porta = portaDe(autoridade.substring(doisPontos + 1));
        }
        if (host.isEmpty()) {
            throw new SQLException("a URL não nomeia o host", SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        return new NishiUrl(host, porta, decodifica(caminho), propriedadesDe(query));
    }

    private static int portaDe(String texto) throws SQLException {
        int porta;
        try {
            porta = Integer.parseInt(texto);
        } catch (NumberFormatException e) {
            throw new SQLException("porta inválida na URL: " + texto, SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        if (porta < 1 || porta > 65535) {
            throw new SQLException("porta fora de [1,65535] na URL: " + porta, SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
        return porta;
    }

    private static Map<String, String> propriedadesDe(String query) {
        Map<String, String> out = new LinkedHashMap<>();
        if (query.isEmpty()) {
            return out;
        }
        for (String par : query.split("&")) {
            if (par.isEmpty()) {
                continue;
            }
            int igual = par.indexOf('=');
            if (igual < 0) {
                out.put(decodifica(par), "");
            } else {
                out.put(decodifica(par.substring(0, igual)), decodifica(par.substring(igual + 1)));
            }
        }
        return out;
    }

    private static String decodifica(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
