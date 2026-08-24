package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArrayCursorTest {

    private static Molduras.Coluna coluna(String nome, String jdbc) {
        return new Molduras.Coluna(nome, "string", jdbc, true, 0, 0, 0);
    }

    private static final List<Molduras.Coluna> COLUNAS =
            List.of(coluna("TABLE_NAME", "VARCHAR"), coluna("ORDINAL_POSITION", "BIGINT"),
                    coluna("NULLABLE", "BOOLEAN"));

    /**
     * O cursor sintético fala a MESMA linguagem que o do stream.
     *
     * <p>É o que permite ao NishiResultSet ser uma implementação só: sem esta
     * SPI, o ResultSet do DatabaseMetaData seria uma segunda classe, e uma
     * correção de conversão passaria a valer só num dos dois.
     */
    @Test
    void oCursorSinteticoFalaOMesmoVocabularioDoStream() throws SQLException {
        ArrayCursor c = new ArrayCursor(COLUNAS, List.of(
                new Object[] {"eventos", 1L, Boolean.TRUE},
                new Object[] {null, 2L, Boolean.FALSE}));

        assertEquals(3, c.colunas().size());
        assertTrue(c.proxima());
        assertEquals(JsonTipo.TEXTO, c.tipo(0));
        assertEquals("eventos", c.texto(0));
        assertEquals(JsonTipo.NUMERO, c.tipo(1));
        assertEquals(1, c.inteiro(1));
        assertTrue(c.integral(1));
        assertEquals(JsonTipo.BOOLEANO, c.tipo(2));
        assertTrue(c.booleano(2));

        assertTrue(c.proxima());
        assertEquals(JsonTipo.NULO, c.tipo(0));
        assertNull(c.texto(0));
        assertFalse(c.booleano(2));

        assertFalse(c.proxima());
        assertNull(c.aviso());
        c.close();
    }

    /** Sem linhas, mas COM colunas: é o que os getters vazios do §71 devolvem. */
    @Test
    void cursorVazioAindaPublicaAsColunas() throws SQLException {
        ArrayCursor c = new ArrayCursor(COLUNAS, List.of());
        assertEquals(3, c.colunas().size());
        assertFalse(c.proxima());
    }

    /** Double não é inteiro, e o cursor não mente sobre isso. */
    @Test
    void doubleNaoEhIntegral() throws SQLException {
        ArrayCursor c = new ArrayCursor(List.of(coluna("x", "DOUBLE")), List.<Object[]>of(new Object[] {2.5d}));
        assertTrue(c.proxima());
        assertEquals(JsonTipo.NUMERO, c.tipo(0));
        assertFalse(c.integral(0));
        assertEquals(2.5, c.numero(0));
    }

    /**
     * Tipo que o driver não produz falha ALTO.
     *
     * <p>Quem monta as linhas deste cursor é o próprio driver, então um valor
     * inesperado aqui é defeito nosso — e convertê-lo em silêncio por
     * String.valueOf esconderia o defeito atrás de um dado plausível.
     */
    @Test
    void valorDeTipoInesperadoFalhaAlto() throws SQLException {
        ArrayCursor c = new ArrayCursor(List.of(coluna("x", "OTHER")),
                List.<Object[]>of(new Object[] {new java.util.ArrayList<>()}));
        assertTrue(c.proxima());
        SQLException e = assertThrows(SQLException.class, () -> c.tipo(0));
        assertEquals("XX000", e.getSQLState());
    }
}
