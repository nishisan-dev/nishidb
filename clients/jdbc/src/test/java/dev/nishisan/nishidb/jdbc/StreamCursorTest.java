package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.LeitorNdjson;
import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLWarning;
import org.junit.jupiter.api.Test;

class StreamCursorTest {

    /** Uma linha de schema com as colunas nomeadas, todas VARCHAR. */
    private static String schema(String... nomes) {
        StringBuilder sb = new StringBuilder("{\"nishidb\":\"schema\",\"dataset\":\"d\",\"columns\":[");
        for (int i = 0; i < nomes.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"name\":\"").append(nomes[i]).append("\",\"type\":\"string\",\"jdbc\":\"VARCHAR\",")
                    .append("\"nullable\":true,\"precision\":0,\"scale\":0,\"displaySize\":0}");
        }
        return sb.append("]}").toString();
    }

    private static final String FIM = "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":false}";

    private static StreamCursor cursorDe(String... linhas) throws SQLException {
        String corpo = String.join("\n", linhas) + "\n";
        return new StreamCursor(new LeitorNdjson(
                new ByteArrayInputStream(corpo.getBytes(StandardCharsets.UTF_8))));
    }

    /**
     * As colunas ficam disponíveis ANTES da primeira linha.
     *
     * <p>É requisito do JDBC: {@code rs.getMetaData()} tem de responder sem que
     * {@code next()} tenha sido chamado, e uma ferramenta desenha o cabeçalho
     * do grid antes de a primeira linha chegar. É o que o flush da linha de
     * schema no servidor existe para tornar barato.
     */
    @Test
    void asColunasEstaoDisponiveisAntesDaPrimeiraLinha() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a", "b"), "{\"a\":1,\"b\":\"x\"}", FIM)) {
            assertEquals(2, c.colunas().size());
            assertEquals("a", c.colunas().get(0).name());
            assertEquals("d", c.dataset());
        }
    }

    /** Resultado vazio ainda tem schema, e nenhuma linha. */
    @Test
    void resultadoVazioTemSchemaENenhumaLinha() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a"), "{\"nishidb\":\"end\",\"rows\":0,\"truncated\":false}")) {
            assertEquals(1, c.colunas().size());
            assertFalse(c.proxima());
            assertNull(c.aviso());
        }
    }

    /** Cada token do fio é reconhecido pelo que ele é, e o texto sai verbatim. */
    @Test
    void cadaTokenDaLinhaEhReconhecidoPeloQueEle() throws SQLException {
        try (StreamCursor c = cursorDe(schema("i", "d", "b", "s", "n"),
                "{\"i\":42,\"d\":1.5,\"b\":true,\"s\":\"oi\",\"n\":null}", FIM)) {
            assertTrue(c.proxima());
            assertEquals(JsonTipo.NUMERO, c.tipo(0));
            assertEquals(42, c.inteiro(0));
            assertTrue(c.integral(0));

            assertEquals(JsonTipo.NUMERO, c.tipo(1));
            assertEquals(1.5, c.numero(1));
            assertFalse(c.integral(1));

            assertEquals(JsonTipo.BOOLEANO, c.tipo(2));
            assertTrue(c.booleano(2));

            assertEquals(JsonTipo.TEXTO, c.tipo(3));
            assertEquals("oi", c.texto(3));

            assertEquals(JsonTipo.NULO, c.tipo(4));
            assertNull(c.texto(4));
        }
    }

    /**
     * O texto sai com os escapes DESFEITOS.
     *
     * <p>O span de uma string aponta para o conteúdo ainda escapado, e entregar
     * isso ao chamador faria uma coluna que contém uma quebra de linha chegar à
     * aplicação com dois caracteres no lugar dela.
     */
    @Test
    void oTextoSaiComOsEscapesDesfeitos() throws SQLException {
        String barra = String.valueOf((char) 92); // ver a armadilha em JsonScannerTest
        String linha = "{\"s\":\"a" + barra + "nb" + barra + "u00e9\"}";
        try (StreamCursor c = cursorDe(schema("s"), linha, FIM)) {
            assertTrue(c.proxima());
            assertEquals("a\nbé", c.texto(0));
        }
    }

    /**
     * A coluna json chega INLINE, e sai verbatim.
     *
     * <p>Um cursor que só esperasse escalares leria o '{' aninhado como fim do
     * objeto de topo e todas as colunas seguintes sairiam erradas — sem erro
     * nenhum, que é o que torna este caso perigoso.
     */
    @Test
    void aColunaJsonChegaInlineESaiVerbatim() throws SQLException {
        try (StreamCursor c = cursorDe(schema("j", "depois"),
                "{\"j\":{\"k\":[1,2,{\"z\":null}]},\"depois\":7}", FIM)) {
            assertTrue(c.proxima());
            assertEquals(JsonTipo.BRUTO, c.tipo(0));
            assertEquals("{\"k\":[1,2,{\"z\":null}]}", c.texto(0));
            assertEquals(7, c.inteiro(1));
        }
    }

    /**
     * Chave repetida é LEGAL: {@code SELECT a, a} produz a chave duas vezes.
     *
     * <p>É por isto que o casamento é posicional e nunca por mapa de nome — um
     * mapa entregaria o mesmo valor nas duas colunas.
     */
    @Test
    void chaveRepetidaCasaPosicionalmente() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a", "a"), "{\"a\":1,\"a\":2}", FIM)) {
            assertTrue(c.proxima());
            assertEquals(1, c.inteiro(0));
            assertEquals(2, c.inteiro(1));
        }
    }

    /**
     * Chave que não casa nenhuma coluna restante é PROTOCOLO VIOLADO.
     *
     * <p>Ignorá-la entregaria uma linha com colunas deslocadas, que a aplicação
     * leria como dado bom.
     */
    @Test
    void chaveQueNaoCasaNenhumaColunaRestanteFalhaAlto() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a", "b"), "{\"a\":1,\"z\":2}", FIM)) {
            SQLException e = assertThrows(SQLRecoverableException.class, c::proxima);
            assertEquals("08006", e.getSQLState());
            assertTrue(e.getMessage().contains("z"), e.getMessage());
        }
    }

    /**
     * Coluna ausente na linha TAMBÉM é protocolo violado.
     *
     * <p>NULL vem como null explícito — é invariante do encoder, com oráculo do
     * lado Go. Uma coluna que simplesmente não veio significa outra coisa, e
     * apresentá-la como nula esconderia a diferença.
     */
    @Test
    void colunaAusenteNaLinhaFalhaAlto() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a", "b"), "{\"a\":1}", FIM)) {
            SQLException e = assertThrows(SQLRecoverableException.class, c::proxima);
            assertEquals("08006", e.getSQLState());
            assertTrue(e.getMessage().contains("b"), e.getMessage());
        }
    }

    /**
     * MAX_ROWS_EXCEEDED vira AVISO, e nunca exceção.
     *
     * <p>O caso de fronteira — resultado com exatamente maxRows linhas — é
     * falso positivo conhecido, e distingui-lo no servidor custaria varrer o
     * dataset inteiro, que é justamente o que o teto existe para evitar.
     * Lançar aqui transformaria um resultado bom em falha.
     */
    @Test
    void tetoDoDatasetViraAvisoENaoExcecao() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a"), "{\"a\":1}",
                "{\"nishidb\":\"error\",\"code\":\"MAX_ROWS_EXCEEDED\",\"rows\":1,\"message\":\"teto\"}")) {
            assertTrue(c.proxima());
            assertFalse(c.proxima());
            SQLWarning w = c.aviso();
            assertNotNull(w);
            assertEquals("53400", w.getSQLState());
            assertTrue(w.getMessage().contains("teto"), w.getMessage());
        }
    }

    /** Qualquer OUTRO fecho de erro continua sendo exceção, com o SQLState do código. */
    @Test
    void outroFechoDeErroContinuaSendoExcecao() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a"), "{\"a\":1}",
                "{\"nishidb\":\"error\",\"code\":\"DATASET_CORRUPT\",\"rows\":1,\"message\":\"segmento\"}")) {
            assertTrue(c.proxima());
            SQLException e = assertThrows(SQLException.class, c::proxima);
            assertEquals("XX001", e.getSQLState());
            assertTrue(e.getMessage().contains("1 linha"), e.getMessage());
        }
    }

    /** truncated é SUCESSO: o cliente pediu um teto e o servidor o atendeu. */
    @Test
    void truncadoPeloProprioPedidoNaoEhAvisoNemErro() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a"), "{\"a\":1}",
                "{\"nishidb\":\"end\",\"rows\":1,\"truncated\":true}")) {
            assertTrue(c.proxima());
            assertFalse(c.proxima());
            assertNull(c.aviso());
        }
    }

    /** Stream cortado no meio continua sendo falha de transporte, e não fim. */
    @Test
    void streamSemFechoContinuaSendoFalhaDeTransporte() throws SQLException {
        try (StreamCursor c = cursorDe(schema("a"), "{\"a\":1}")) {
            assertTrue(c.proxima());
            SQLException e = assertThrows(SQLRecoverableException.class, c::proxima);
            assertEquals("08006", e.getSQLState());
        }
    }

    /**
     * O §128 aplicado ao CLIENTE: o custo por linha não cresce com o resultado.
     *
     * <p>Mede bytes ALOCADOS na thread, que é a métrica que não depende de GC
     * nem de quanto heap sobrou. Um cursor que materializasse a linha alocaria
     * uma String por coluna — centenas de bytes por linha —, e é essa
     * regressão que o teto de 64 bytes por linha pega. Zero seria teto frágil:
     * a JVM contabiliza a alocação em blocos.
     */
    @Test
    void percorrerLinhasNaoAlocaProporcionalmenteAoNumeroDeLinhas() throws Exception {
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        // Nunca SKIP: um gate que some quando o instrumento falta não é gate.
        assertTrue(mx.isThreadAllocatedMemorySupported(),
                "esta JVM não mede alocação por thread; o gate do §128 não pode rodar");
        mx.setThreadAllocatedMemoryEnabled(true);

        byte[] poucas = corpo(2_000);
        byte[] muitas = corpo(20_000);
        percorre(poucas);
        percorre(muitas); // aquece o JIT antes de medir

        long id = Thread.currentThread().threadId();
        long a0 = mx.getThreadAllocatedBytes(id);
        percorre(poucas);
        long a1 = mx.getThreadAllocatedBytes(id);
        percorre(muitas);
        long a2 = mx.getThreadAllocatedBytes(id);

        long porLinha = ((a2 - a1) - (a1 - a0)) / (20_000 - 2_000);
        assertTrue(porLinha < 64, "alocação por linha = " + porLinha + " bytes; o cursor está materializando");
    }

    private static byte[] corpo(int linhas) {
        StringBuilder sb = new StringBuilder(linhas * 48);
        sb.append(schema("id", "nome", "ativo")).append('\n');
        for (int i = 0; i < linhas; i++) {
            sb.append("{\"id\":").append(i).append(",\"nome\":\"linha ").append(i)
                    .append("\",\"ativo\":true}\n");
        }
        sb.append("{\"nishidb\":\"end\",\"rows\":").append(linhas).append(",\"truncated\":false}\n");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Percorre lendo só o inteiro: nenhum String nasce no caminho medido. */
    private static long percorre(byte[] corpo) throws SQLException {
        long soma = 0;
        try (StreamCursor c = new StreamCursor(new LeitorNdjson(new ByteArrayInputStream(corpo)))) {
            while (c.proxima()) {
                soma += c.inteiro(0);
            }
        }
        return soma;
    }
}
