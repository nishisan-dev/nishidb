package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class NishiDriverTest {

    /**
     * O DriverManager acha o driver pelo ServiceLoader, sem Class.forName.
     *
     * <p>É o que faz uma ferramenta gráfica funcionar só com o jar no
     * classpath. Sem o arquivo de serviço, o usuário teria de saber o nome da
     * classe — e a mensagem de erro não diria isso.
     */
    @Test
    void oServiceLoaderAchaODriver() {
        boolean achou = false;
        for (Driver d : ServiceLoader.load(Driver.class)) {
            if (d instanceof NishiDriver) {
                achou = true;
            }
        }
        assertTrue(achou, "nenhum NishiDriver publicado em META-INF/services/java.sql.Driver");
    }

    @Test
    void oDriverSeRegistraNoDriverManager() throws SQLException {
        assertNotNull(DriverManager.getDriver("jdbc:nishidb://127.0.0.1:9142/eventos"));
    }

    /**
     * acceptsURL NUNCA lança, nem para null.
     *
     * <p>O DriverManager pergunta a TODOS os drivers registrados; um que lance
     * aborta a busca inteira e quebra a conexão que o usuário estava abrindo
     * com outro banco.
     */
    @Test
    void acceptsUrlNuncaLanca() throws SQLException {
        NishiDriver d = new NishiDriver();
        assertTrue(d.acceptsURL("jdbc:nishidb://h/ds"));
        assertFalse(d.acceptsURL("jdbc:postgresql://h/ds"));
        assertFalse(d.acceptsURL(null));
        assertFalse(d.acceptsURL(""));
    }

    /** connect devolve null para URL de outro banco — é o contrato do JDBC. */
    @Test
    void connectDevolveNuloParaUrlDeOutroBanco() throws SQLException {
        assertNull(new NishiDriver().connect("jdbc:postgresql://h/ds", new Properties()));
    }

    /** As propriedades são anunciadas, para que a ferramenta as ofereça na tela. */
    @Test
    void asPropriedadesSaoAnunciadas() throws SQLException {
        DriverPropertyInfo[] info = new NishiDriver().getPropertyInfo("jdbc:nishidb://h/ds", null);
        StringBuilder nomes = new StringBuilder();
        for (DriverPropertyInfo p : info) {
            nomes.append(p.name).append(' ');
            assertNotNull(p.description, p.name);
        }
        String todos = nomes.toString();
        for (String exigida : new String[] {"user", "password", "tls", "tlsCaFile", "connectTimeoutMs"}) {
            assertTrue(todos.contains(exigida), todos);
        }
    }

    /**
     * jdbcCompliant é FALSO, e dizer o contrário seria mentira verificável.
     *
     * <p>O contrato exige suporte a SQL-92 Entry Level; o NishiDB não tem JOIN,
     * GROUP BY nem transação.
     */
    @Test
    void oDriverNaoSeDeclaraCompliant() {
        assertFalse(new NishiDriver().jdbcCompliant());
    }

    /** A versão do driver é a do pom, e um gate do lado Go cobra que continue sendo. */
    @Test
    void aVersaoEhADoArtefato() {
        NishiDriver d = new NishiDriver();
        assertEquals(0, d.getMajorVersion());
        assertEquals(1, d.getMinorVersion());
    }

    // ------------------------------------------------------- ConnectionConfig

    /** A Properties tem precedência sobre a query string da URL. */
    @Test
    void aPropertiesVenceAUrl() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "da-properties");
        ConnectionConfig c = ConnectionConfig.de("jdbc:nishidb://h:9143/ds?user=da-url&tls=true", p);
        assertEquals("da-properties", c.usuario());
        assertTrue(c.tls());
        assertEquals("h", c.host());
        assertEquals(9143, c.porta());
        assertEquals("ds", c.dataset());
    }

    /** Sem nada declarado, os defaults são os do §70 e do servidor. */
    @Test
    void osDefaultsSaoOsDoProduto() throws SQLException {
        ConnectionConfig c = ConnectionConfig.de("jdbc:nishidb://h/ds", null);
        assertEquals(9142, c.porta());
        assertFalse(c.tls());
        assertNull(c.usuario());
        assertEquals("http://h:9142", c.base());
        assertEquals(10_000, c.connectTimeoutMs());
    }

    @Test
    void comTlsABaseEhHttps() throws SQLException {
        assertEquals("https://h:9142", ConnectionConfig.de("jdbc:nishidb://h/ds?tls=true", null).base());
    }

    /**
     * Valor de tls que não é true nem false é RECUSADO.
     *
     * <p>Um {@code tls=1} tratado como falso conectaria em texto puro
     * silenciosamente, e quem escreveu acharia que estava em TLS. O modo de
     * falha de uma configuração de segurança não pode ser o silêncio.
     */
    @Test
    void valorDeTlsInvalidoEhRecusado() {
        SQLException e = assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?tls=1", null));
        assertTrue(e.getMessage().contains("tls"), e.getMessage());
    }

    /**
     * CA declarada sem TLS é contradição, e é recusada.
     *
     * <p>Aceitar em silêncio faria alguém que apontou a CA certa conectar em
     * texto puro achando que o certificado estava sendo verificado.
     */
    @Test
    void caSemTlsEhRecusada() {
        SQLException e = assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?tlsCaFile=/tmp/ca.pem", null));
        assertTrue(e.getMessage().contains("tls"), e.getMessage());
    }

    @Test
    void connectTimeoutInvalidoEhRecusado() {
        assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?connectTimeoutMs=abc", null));
        assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?connectTimeoutMs=0", null));
    }

    /**
     * Propriedade desconhecida é IGNORADA, e é escolha.
     *
     * <p>As ferramentas passam propriedades próprias na Properties da conexão
     * (nome da aplicação, ajustes de UI), e recusá-las impediria a conexão de
     * abrir. O que NÃO é ignorado é valor errado numa propriedade NOSSA, que é
     * onde mora o risco.
     */
    @Test
    void propriedadeDesconhecidaEhIgnorada() throws SQLException {
        Properties p = new Properties();
        p.setProperty("ApplicationName", "DBeaver");
        assertNotNull(ConnectionConfig.de("jdbc:nishidb://h/ds", p));
    }

    /** Sem arquivo de CA no caminho declarado, a falha nomeia o caminho. */
    @Test
    void caInexistenteFalhaNomeandoOCaminho() {
        SQLException e = assertThrows(SQLException.class,
                () -> ConnectionConfig.de("jdbc:nishidb://h/ds?tls=true&tlsCaFile=/nao/existe.pem", null)
                        .contextoTls());
        assertTrue(e.getMessage().contains("/nao/existe.pem"), e.getMessage());
        assertEquals("08001", e.getSQLState());
    }
}
