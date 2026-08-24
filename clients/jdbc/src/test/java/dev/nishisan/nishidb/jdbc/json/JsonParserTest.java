package dev.nishisan.nishidb.jdbc.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class JsonParserTest {

    /**
     * A linha de schema REAL, com os campos que a Fase 7 acrescentou.
     *
     * <p>É o primeiro documento que o driver lê em toda consulta, e é de onde
     * sai o ResultSetMetaData inteiro.
     */
    @Test
    void linhaDeSchemaDoProtocolo() throws JsonException {
        String json = "{\"nishidb\":\"schema\",\"dataset\":\"radius_sessions\",\"columns\":["
                + "{\"name\":\"id\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",\"nullable\":false,"
                + "\"precision\":0,\"scale\":0,\"displaySize\":0},"
                + "{\"name\":\"porta\",\"type\":\"int64\",\"jdbc\":\"BIGINT\",\"nullable\":true,"
                + "\"precision\":19,\"scale\":0,\"displaySize\":20}]}";
        JsonValue v = JsonParser.de(json);

        assertEquals("schema", v.textoOu("nishidb", null));
        assertEquals("radius_sessions", v.textoOu("dataset", null));

        List<JsonValue> colunas = v.itensDe("columns");
        assertEquals(2, colunas.size());

        JsonValue id = colunas.get(0);
        assertEquals("id", id.textoOu("name", null));
        assertEquals("VARCHAR", id.textoOu("jdbc", null));
        assertEquals(false, id.booleanoOu("nullable", true));
        assertEquals(0, id.inteiroOu("displaySize", -1));

        JsonValue porta = colunas.get(1);
        assertEquals(true, porta.booleanoOu("nullable", false));
        assertEquals(19, porta.inteiroOu("precision", -1));
        assertEquals(20, porta.inteiroOu("displaySize", -1));
    }

    /** O corpo de erro, que vem antes da primeira linha e carrega a posição. */
    @Test
    void corpoDeErroComPosicao() throws JsonException {
        JsonValue v = JsonParser.de("{\"error\":{\"code\":\"UNSUPPORTED_CLAUSE\","
                + "\"message\":\"ORDER BY nao e suportado\",\"line\":1,\"column\":18}}");
        JsonValue erro = v.campo("error");
        assertEquals("UNSUPPORTED_CLAUSE", erro.textoOu("code", null));
        assertTrue(erro.textoOu("message", "").contains("ORDER BY"));
        assertEquals(1, erro.inteiroOu("line", -1));
        assertEquals(18, erro.inteiroOu("column", -1));
    }

    /**
     * O alias do /v1/metadata é ponteiro no lado Go, e sai como null explícito.
     *
     * <p>Confundir "campo nulo" com "campo ausente" é o que faz um driver
     * publicar "sem alias" quando o servidor disse "alias nulo" — e o
     * DatabaseMetaData usa esse campo para nomear a coluna.
     */
    @Test
    void aliasNuloDoMetadataEhNuloExplicito() throws JsonException {
        JsonValue v = JsonParser.de("{\"name\":\"id\",\"alias\":null}");
        assertInstanceOf(JsonValue.Nulo.class, v.campo("alias"));
        assertNull(v.campo("inexistente"));
        // Já o ACESSO por padrão trata os dois igual, de propósito: nem um
        // servidor mais novo com campos a mais, nem um mais velho com campos a
        // menos, pode derrubar a conexão.
        assertEquals("x", v.textoOu("alias", "x"));
        assertEquals("x", v.textoOu("inexistente", "x"));
    }

    /**
     * O número guarda o TEXTO, e o inteiro sai exato.
     *
     * <p>maxRows e os contadores de row do /v1/stats passam por aqui. Um double
     * no caminho perderia precisão acima de 2^53, em silêncio.
     */
    @Test
    void inteiroGrandeDoStatsNaoPerdePrecisao() throws JsonException {
        JsonValue v = JsonParser.de("{\"rows\":9007199254740993}");
        assertEquals(9007199254740993L, v.inteiroOu("rows", -1));
        assertInstanceOf(JsonValue.Numero.class, v.campo("rows"));
    }

    @Test
    void aninhamentoDeObjetosEArrays() throws JsonException {
        JsonValue v = JsonParser.de("{\"a\":{\"b\":[1,{\"c\":\"d\"},[2,3]]},\"e\":[]}");
        List<JsonValue> b = v.campo("a").itensDe("b");
        assertEquals(3, b.size());
        assertEquals("d", b.get(1).textoOu("c", null));
        assertEquals(2, b.get(2) instanceof JsonValue.Arranjo a ? a.itens().size() : -1);
        assertEquals(List.of(), v.itensDe("e"));
    }

    @Test
    void arrayDeTopoTambemEhDocumentoValido() throws JsonException {
        JsonValue v = JsonParser.de("[1,2,3]");
        assertEquals(3, v instanceof JsonValue.Arranjo a ? a.itens().size() : -1);
    }

    /**
     * A gramática é a MESMA do scanner, e este teste garante que não nasceu
     * uma segunda por descuido — o modo de falha seria um documento que uma
     * rota aceita e a outra recusa.
     */
    @Test
    void aGramaticaEhAMesmaDoScanner() {
        for (String caso : new String[] {
            "{\"a\":NaN}", "{\"a\":007}", "{'a':1}", "{\"a\":1,}", "{\"a\":1}{\"b\":2}", "{\"a\":}"
        }) {
            assertThrows(JsonException.class, () -> JsonParser.de(caso), () -> "aceitou: " + caso);
        }
    }
}
