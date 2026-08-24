package dev.nishisan.nishidb.jdbc.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import org.junit.jupiter.api.Test;

class TiposJdbcTest {

    /** Os oito nomes que o servidor emite hoje, um a um. */
    @Test
    void cadaNomeDoFioTemOCodigoDeJavaSqlTypes() {
        assertEquals(Types.VARCHAR, TiposJdbc.codigoDe("VARCHAR"));
        assertEquals(Types.BIGINT, TiposJdbc.codigoDe("BIGINT"));
        assertEquals(Types.DOUBLE, TiposJdbc.codigoDe("DOUBLE"));
        assertEquals(Types.BOOLEAN, TiposJdbc.codigoDe("BOOLEAN"));
        assertEquals(Types.TIMESTAMP, TiposJdbc.codigoDe("TIMESTAMP"));
        assertEquals(Types.TIME, TiposJdbc.codigoDe("TIME"));
        assertEquals(Types.VARBINARY, TiposJdbc.codigoDe("VARBINARY"));
        assertEquals(Types.OTHER, TiposJdbc.codigoDe("OTHER"));
    }

    /**
     * Nome que este driver não conhece vira OTHER, e NUNCA exceção.
     *
     * <p>Um servidor mais novo pode emitir um tipo que o jar em uso não
     * conhece. OTHER é a resposta honesta — "não sei o que é isto, e getString
     * continua funcionando" —, enquanto uma exceção quebraria a consulta
     * inteira por causa de uma coluna que talvez ninguém leia. É o espelho da
     * regra de HY000 para código de erro desconhecido.
     */
    @Test
    void nomeDesconhecidoViraOtherENaoExcecao() {
        assertEquals(Types.OTHER, TiposJdbc.codigoDe("DECIMAL128"));
        assertEquals(Types.OTHER, TiposJdbc.codigoDe(null));
        assertFalse(TiposJdbc.conhece("DECIMAL128"));
        assertTrue(TiposJdbc.conhece("BIGINT"));
    }

    /** A classe é a que getObject devolve — é o contrato de getColumnClassName. */
    @Test
    void aClasseEhAQueGetObjectDevolve() {
        assertEquals("java.lang.String", TiposJdbc.classeDe("VARCHAR"));
        assertEquals("java.lang.Long", TiposJdbc.classeDe("BIGINT"));
        assertEquals("java.lang.Double", TiposJdbc.classeDe("DOUBLE"));
        assertEquals("java.lang.Boolean", TiposJdbc.classeDe("BOOLEAN"));
        assertEquals("java.sql.Timestamp", TiposJdbc.classeDe("TIMESTAMP"));
        assertEquals("java.sql.Time", TiposJdbc.classeDe("TIME"));
        assertEquals("[B", TiposJdbc.classeDe("VARBINARY"));
        assertEquals("java.lang.Object", TiposJdbc.classeDe("OTHER"));
        assertEquals("java.lang.Object", TiposJdbc.classeDe("DECIMAL128"));
    }

    /** Só os dois numéricos são assinados; o resto é texto, binário ou data. */
    @Test
    void apenasOsNumericosSaoAssinados() {
        assertTrue(TiposJdbc.assinado("BIGINT"));
        assertTrue(TiposJdbc.assinado("DOUBLE"));
        assertFalse(TiposJdbc.assinado("VARCHAR"));
        assertFalse(TiposJdbc.assinado("TIMESTAMP"));
        assertFalse(TiposJdbc.assinado("OTHER"));
    }

    /**
     * bytes e json NÃO são pesquisáveis, e a pergunta é sobre o tipo do
     * PRODUTO, não sobre o nome JDBC.
     *
     * <p>Os dois viajam como VARCHAR/VARBINARY, então o nome JDBC não os
     * distingue de uma string comum — mas o binder recusa comparação sobre
     * eles. Uma ferramenta que os oferecesse num filtro montaria um WHERE que
     * o servidor recusa.
     */
    @Test
    void bytesEJsonNaoSaoPesquisaveis() {
        assertFalse(TiposJdbc.pesquisavel("bytes"));
        assertFalse(TiposJdbc.pesquisavel("json"));
        assertTrue(TiposJdbc.pesquisavel("string"));
        assertTrue(TiposJdbc.pesquisavel("ipv4"));
        assertTrue(TiposJdbc.pesquisavel("int64"));
    }
}
