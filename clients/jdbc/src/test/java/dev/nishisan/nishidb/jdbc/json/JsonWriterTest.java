package dev.nishisan.nishidb.jdbc.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonWriterTest {

    /** Uma barra invertida, montada em runtime (ver JsonScannerTest.B). */
    private static final String BARRA = String.valueOf((char) 92);

    /** Um byte de controle, montado em runtime: escrevê-lo no fonte é ilegível. */
    private static final String CONTROLE = String.valueOf((char) 1);

    /** O corpo do POST /v1/query, na forma que o servidor espera. */
    @Test
    void corpoDaRequisicaoDeConsulta() throws JsonException {
        String corpo = new JsonWriter()
                .texto("dataset", "radius_sessions")
                .texto("sql", "SELECT id FROM radius_sessions WHERE status = ?")
                .bruto("parameters", "[{" + q("type") + ":" + q("string") + "," + q("value") + ":" + q("Stop") + "}]")
                .inteiro("maxRows", 200)
                .fecha();

        JsonValue v = JsonParser.de(corpo);
        assertEquals("radius_sessions", v.textoOu("dataset", null));
        assertTrue(v.textoOu("sql", "").contains("WHERE status = ?"));
        assertEquals(200, v.inteiroOu("maxRows", -1));
        assertEquals("Stop", v.itensDe("parameters").get(0).textoOu("value", null));
    }

    private static String q(String s) {
        return "\"" + s + "\"";
    }

    /**
     * O que a aplicação escreve tem de voltar igual.
     *
     * <p>O round-trip é feito pelo NOSSO parser de propósito: escritor e leitor
     * são as duas metades do mesmo contrato, e um teste que comparasse a saída
     * com uma string esperada à mão só provaria que ela não mudou.
     */
    @Test
    void oTextoFazRoundTripPeloProprioParser() throws JsonException {
        String[] casos = {
            "simples",
            "com \"aspas\" dentro",
            "barra invertida: " + BARRA,
            "quebra\nde\tlinha",
            "acentuacao e emoji 😀",
            "controle: " + CONTROLE,
            "",
        };
        for (String original : casos) {
            String corpo = new JsonWriter().texto("v", original).fecha();
            assertEquals(original, JsonParser.de(corpo).textoOu("v", null), () -> "round-trip falhou");
        }
    }

    /**
     * Controle cru NÃO sai no fio.
     *
     * <p>JSON não o permite dentro de string, e o servidor recusaria o corpo
     * inteiro com uma mensagem sobre sintaxe — sem apontar para o caractere que
     * a aplicação passou.
     */
    @Test
    void controleSaiEscapadoENuncaCru() {
        String corpo = new JsonWriter().texto("v", "a" + CONTROLE + "b").fecha();
        assertTrue(corpo.contains("u0001"), corpo);
        for (byte b : corpo.getBytes(StandardCharsets.UTF_8)) {
            assertFalse((b & 0xFF) < 0x20, "byte de controle cru no corpo");
        }
    }

    /**
     * Não escapamos &lt; &gt; &amp;, e isso é alinhamento com o encoder do
     * servidor: duas políticas de escaping fariam o MESMO texto sair diferente
     * conforme a direção.
     */
    @Test
    void naoEscapaHtmlPorqueOConsumidorNaoEhUmNavegador() {
        assertEquals("\"a<b>c&d\"", JsonWriter.textoComoJson("a<b>c&d"));
    }

    @Test
    void naoExisteJsonParaNaNEInfinito() {
        for (double d : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> JsonWriter.numeroComoJson(d));
        }
    }

    /** Double finito sobrevive à ida e à volta com os MESMOS bits. */
    @Test
    void doubleFinitoFazRoundTripDeBits() throws JsonException {
        for (double d : new double[] {0.1, -0.0, 1e-7, 1.7976931348623157e308, 4.9e-324}) {
            String corpo = new JsonWriter().bruto("v", JsonWriter.numeroComoJson(d)).fecha();
            JsonValue.Numero n = (JsonValue.Numero) JsonParser.de(corpo).campo("v");
            byte[] b = n.cru().getBytes(StandardCharsets.US_ASCII);
            assertEquals(Double.doubleToLongBits(d), Double.doubleToLongBits(JsonScanner.numero(b, 0, b.length)));
        }
    }
}
