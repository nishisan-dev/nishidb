package dev.nishisan.nishidb.jdbc.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LeitorNdjsonTest {

    private static final String SCHEMA = "{\"nishidb\":\"schema\",\"dataset\":\"d\",\"columns\":["
            + "{\"name\":\"id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
            + "\"precision\":0,\"scale\":0,\"displaySize\":0}]}";

    private static LeitorNdjson leitorDe(String... linhas) {
        String corpo = String.join("\n", linhas) + "\n";
        return new LeitorNdjson(new ByteArrayInputStream(corpo.getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> rows(LeitorNdjson l) throws SQLException {
        List<String> out = new ArrayList<>();
        while (l.proxima()) {
            out.add(new String(l.buffer(), l.inicio(), l.fimDaLinha() - l.inicio(), StandardCharsets.UTF_8));
        }
        return out;
    }

    /** A moldura completa: schema, rows e fecho de sucesso. */
    @Test
    void molduraCompleta() throws SQLException {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"id\":\"a\"}", "{\"id\":\"b\"}",
                "{\"nishidb\":\"end\",\"rows\":2,\"truncated\":false}");
        assertEquals(List.of("{\"id\":\"a\"}", "{\"id\":\"b\"}"), rows(l));

        assertNotNull(l.schema());
        assertEquals("d", l.schema().dataset());
        Molduras.Coluna c = l.schema().columns().get(0);
        assertEquals("id", c.name());
        assertEquals("VARCHAR", c.jdbc());
        assertFalse(c.nullable());

        assertNotNull(l.fim());
        assertEquals(2, l.fim().rows());
        assertFalse(l.fim().truncated());
        assertNull(l.erro());
        l.exigeDesfechoFeliz();
    }

    /** Resultado vazio TAMBÉM tem schema: é o que o ResultSetMetaData usa. */
    @Test
    void resultadoVazioAindaTemSchema() throws SQLException {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"nishidb\":\"end\",\"rows\":0,\"truncated\":false}");
        assertEquals(List.of(), rows(l));
        assertNotNull(l.schema());
        assertEquals(1, l.schema().columns().size());
    }

    /**
     * O ERRO chega depois de N rows já entregues, e a exceção diz quantas.
     *
     * <p>É o caso que a moldura existe para tornar possível: o status HTTP
     * viajou com o primeiro byte, então uma falha no meio do resultado só pode
     * ser dita no corpo. Devolver false aqui apresentaria um resultado parcial
     * como completo.
     */
    @Test
    void erroDepoisDeAlgumasRowsCitaQuantasForamEntregues() throws SQLException {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"id\":\"a\"}",
                "{\"nishidb\":\"error\",\"code\":\"MAX_ROWS_EXCEEDED\",\"rows\":1,\"message\":\"teto\"}");
        assertEquals(1, rows(l).size());

        assertNotNull(l.erro());
        assertNull(l.fim());
        SQLException e = assertThrows(SQLException.class, l::exigeDesfechoFeliz);
        assertEquals("53400", e.getSQLState());
        assertTrue(e.getMessage().contains("1 linha"), e.getMessage());
    }

    /** O código do fecho vira a subclasse certa, como em qualquer outro erro. */
    @Test
    void oCodigoDoFechoDeErroViraASubclasseCerta() throws SQLException {
        LeitorNdjson l = leitorDe(SCHEMA,
                "{\"nishidb\":\"error\",\"code\":\"UNSUPPORTED_CLAUSE\",\"rows\":0,\"message\":\"ORDER BY\"}");
        rows(l);
        assertInstanceOf(SQLFeatureNotSupportedException.class,
                assertThrows(SQLException.class, l::exigeDesfechoFeliz));
    }

    /**
     * STREAM SEM FECHO É FALHA — a asserção central do porte.
     *
     * <p>Um servidor que morreu, uma rede que caiu ou um proxy que truncou o
     * corpo produzem exatamente isto. Sem a checagem, a aplicação receberia as
     * rows que chegaram e concluiria que era o resultado inteiro.
     */
    @Test
    void streamQueAcabaSemFechoEhFalhaDeTransporte() {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"id\":\"a\"}");
        SQLException e = assertThrows(SQLException.class, () -> rows(l));
        assertInstanceOf(SQLRecoverableException.class, e);
        assertEquals("08006", e.getSQLState());
        assertTrue(e.getMessage().contains("fecho"), e.getMessage());
    }

    /** Corpo vazio é a mesma coisa: nem schema, nem fecho. */
    @Test
    void corpoVazioEhFalha() {
        LeitorNdjson l = new LeitorNdjson(new ByteArrayInputStream(new byte[0]));
        assertThrows(SQLException.class, () -> rows(l));
    }

    /** A primeira linha TEM de ser o schema. */
    @Test
    void primeiraLinhaQueNaoEhSchemaEhFalha() {
        LeitorNdjson l = leitorDe("{\"id\":\"a\"}", "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}");
        SQLException e = assertThrows(SQLException.class, () -> rows(l));
        assertTrue(e.getMessage().contains("schema"), e.getMessage());
    }

    @Test
    void segundoSchemaNoMesmoStreamEhFalha() {
        LeitorNdjson l = leitorDe(SCHEMA, SCHEMA, "{\"nishidb\":\"end\",\"rows\":0,\"truncated\":false}");
        assertThrows(SQLException.class, () -> rows(l));
    }

    @Test
    void molduraDesconhecidaEhFalha() {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"nishidb\":\"invencao\"}");
        SQLException e = assertThrows(SQLException.class, () -> rows(l));
        assertTrue(e.getMessage().contains("invencao"), e.getMessage());
    }

    @Test
    void molduraIlegivelEhFalha() {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"nishidb\":\"end\",\"rows\":}");
        assertThrows(SQLException.class, () -> rows(l));
    }

    /**
     * Uma linha maior que o buffer de leitura é remontada inteira.
     *
     * <p>O buffer de entrada tem 64 KiB e uma row pode ter megabytes: sem a
     * remontagem, a linha sairia partida e o JSON viria truncado — com a linha
     * seguinte ainda parecendo bem formada.
     */
    @Test
    void linhaMaiorQueOBufferDeLeituraEhRemontada() throws SQLException {
        String grande = "x".repeat(300 * 1024);
        String rowEsperada = "{\"id\":\"" + grande + "\"}";
        LeitorNdjson l = leitorDe(SCHEMA, rowEsperada, "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}");
        List<String> rows = rows(l);
        assertEquals(1, rows.size());
        // Comparação com a linha INTEIRA, e não com um comprimento calculado:
        // aritmética de overhead erra por um e o teste passaria a afirmar o
        // tamanho errado em vez do conteúdo certo.
        assertEquals(rowEsperada, rows.get(0));
    }

    /**
     * O stream é lido em pedaços de tamanho arbitrário, e o leitor não pode
     * supor que uma leitura traz uma linha inteira.
     *
     * <p>É o que acontece de verdade sobre TCP, e é o modo de falha que só
     * aparece em produção se o teste alimentar o leitor de forma folgada.
     */
    @Test
    void leituraFragmentadaByteAByteProduzOMesmoResultado() throws SQLException {
        String corpo = SCHEMA + "\n{\"id\":\"a\"}\n{\"id\":\"b\"}\n"
                + "{\"nishidb\":\"end\",\"rows\":2,\"truncated\":false}\n";
        InputStream gotejante = new InputStream() {
            private final byte[] b = corpo.getBytes(StandardCharsets.UTF_8);
            private int i;

            @Override
            public int read() {
                return i < b.length ? b[i++] & 0xFF : -1;
            }

            @Override
            public int read(byte[] destino, int off, int len) throws IOException {
                if (i >= b.length) {
                    return -1;
                }
                destino[off] = b[i++]; // UM byte por chamada, de propósito
                return 1;
            }
        };
        LeitorNdjson l = new LeitorNdjson(gotejante);
        assertEquals(List.of("{\"id\":\"a\"}", "{\"id\":\"b\"}"), rows(l));
        assertEquals(2, l.fim().rows());
    }

    /** truncated=true é SUCESSO: foi o cliente que pediu o teto. */
    @Test
    void truncadoPeloProprioClienteNaoEhErro() throws SQLException {
        LeitorNdjson l = leitorDe(SCHEMA, "{\"id\":\"a\"}",
                "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":true}");
        rows(l);
        assertTrue(l.fim().truncated());
        l.exigeDesfechoFeliz(); // não lança
    }
}
