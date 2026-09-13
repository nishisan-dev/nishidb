package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class NishiUrlTest {

    /**
     * A forma do §70, com a porta default do servidor.
     */
    @Test
    void formaCanonicaDoSpec70() throws SQLException {
        NishiUrl u = NishiUrl.parse("jdbc:nishidb://db01:9142/radius_sessions");
        assertEquals("db01", u.host());
        assertEquals(9142, u.porta());
        assertEquals("radius_sessions", u.schema());
    }

    /**
     * A porta é opcional e cai no default do servidor (serverPortPadrao). Sem
     * isto, a URL mais curta que alguém digita no DBeaver não funcionaria.
     */
    @Test
    void portaOmitidaUsaODefaultDoServidor() throws SQLException {
        assertEquals(9142, NishiUrl.parse("jdbc:nishidb://db01/eventos").porta());
    }

    /**
     * IPv6 entre colchetes: sem o tratamento, o ':' do endereço seria lido
     * como separador de porta e o host sairia truncado.
     */
    @Test
    void hostIPv6EntreColchetes() throws SQLException {
        NishiUrl u = NishiUrl.parse("jdbc:nishidb://[2001:db8::1]:9143/d");
        assertEquals("[2001:db8::1]", u.host());
        assertEquals(9143, u.porta());
    }

    /**
     * As propriedades da query string entram, e o Driver as funde com as
     * Properties — que vencem (ver ConnectionConfig).
     */
    @Test
    void propriedadesDaQueryString() throws SQLException {
        NishiUrl u = NishiUrl.parse("jdbc:nishidb://h/d?tls=true&user=leitor");
        assertEquals("true", u.propriedades().get("tls"));
        assertEquals("leitor", u.propriedades().get("user"));
        assertEquals("d", u.schema());
    }

    /**
     * CREDENCIAL NA URL É RECUSADA, e não ignorada.
     *
     * <p>A doutrina do §95 e do CLI é que senha nunca viaja em lugar que o
     * sistema registre — e URL de JDBC aparece em log de aplicação, em stack
     * trace e na tela de conexão do DBeaver. Ignorar o userinfo em silêncio
     * seria pior: o usuário acharia que autenticou.
     */
    @Test
    void credencialEmbutidaNaUrlEhRecusada() {
        SQLException e = assertThrows(SQLException.class,
                () -> NishiUrl.parse("jdbc:nishidb://leitor:segredo@db01:9142/d"));
        assertTrue(e.getMessage().contains("user"), () -> "a mensagem não ensina o caminho: " + e.getMessage());
    }

    @Test
    void urlSemSchemaEhRecusada() {
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01:9142/"));
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01:9142"));
    }

    /**
     * Um caminho com mais de um segmento não é um schema: o NishiDB não tem
     * catálogo, e aceitar `/a/b` faria o usuário supor que tem.
     */
    @Test
    void caminhoComMaisDeUmSegmentoEhRecusado() {
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01/a/b"));
    }

    @Test
    void portaForaDaFaixaEhRecusada() {
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01:0/d"));
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01:70000/d"));
        assertThrows(SQLException.class, () -> NishiUrl.parse("jdbc:nishidb://db01:abc/d"));
    }

    /**
     * aceita() responde FALSE para URL de outro driver, e nunca lança.
     *
     * <p>É o contrato de Driver.acceptsURL: o DriverManager percorre todos os
     * drivers registrados perguntando a cada um, e um que lance aborta a busca
     * inteira — o usuário veria o driver do NishiDB quebrar a conexão com o
     * PostgreSQL dele.
     */
    @Test
    void aceitaSoAsNossasENuncaLanca() {
        assertTrue(NishiUrl.aceita("jdbc:nishidb://h/d"));
        assertFalse(NishiUrl.aceita("jdbc:postgresql://h/d"));
        assertFalse(NishiUrl.aceita("jdbc:mysql://h/d"));
        assertFalse(NishiUrl.aceita(""));
        assertFalse(NishiUrl.aceita(null));
        // Prefixo parecido, mas não é o nosso.
        assertFalse(NishiUrl.aceita("jdbc:nishidbx://h/d"));
    }
}
