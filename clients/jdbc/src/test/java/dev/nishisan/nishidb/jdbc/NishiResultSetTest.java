package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.nishisan.nishidb.jdbc.wire.LeitorNdjson;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

/**
 * A matriz de conversão, sobre a grafia REAL do fio.
 *
 * <p>Os valores das linhas são os que o encoder do servidor de fato emite —
 * inclusive os incômodos: o double inteiro que sai sem ponto, a grafia 'g' do
 * Go com expoente de dois dígitos, o json inline e o int64 no limite do tipo.
 */
class NishiResultSetTest {

    /** Uma coluna da linha de schema. */
    private static String col(String nome, String tipo, String jdbc) {
        return col(nome, tipo, jdbc, true, 0, 0, 0);
    }

    private static String col(String nome, String tipo, String jdbc, boolean nulavel,
            int precision, int scale, int displaySize) {
        return "{\"name\":\"" + nome + "\",\"type\":\"" + tipo + "\",\"jdbc\":\"" + jdbc
                + "\",\"nullable\":" + nulavel + ",\"precision\":" + precision + ",\"scale\":" + scale
                + ",\"displaySize\":" + displaySize + "}";
    }

    private static NishiResultSet resultado(String colunas, String... linhas) throws SQLException {
        StringBuilder sb = new StringBuilder("{\"nishidb\":\"schema\",\"dataset\":\"eventos\",\"columns\":[")
                .append(colunas).append("]}\n");
        for (String l : linhas) {
            sb.append(l).append('\n');
        }
        sb.append("{\"nishidb\":\"end\",\"rows\":").append(linhas.length).append(",\"truncated\":false}\n");
        return new NishiResultSet(new StreamCursor(new LeitorNdjson(
                new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)))), null);
    }

    private static <T> T em(String zona, Callable<T> corpo) throws Exception {
        TimeZone anterior = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zona));
            return corpo.call();
        } finally {
            TimeZone.setDefault(anterior);
        }
    }

    /** Todas as colunas do produto, com a grafia que o encoder emite. */
    private static final String TODAS = String.join(",",
            col("i", "int64", "BIGINT"), col("d", "double", "DOUBLE"), col("b", "boolean", "BOOLEAN"),
            col("ts", "timestamp", "TIMESTAMP"), col("t", "time", "TIME"), col("s", "string", "VARCHAR"),
            col("j", "json", "VARCHAR"), col("bin", "bytes", "VARBINARY"), col("ip", "ipv4", "VARCHAR"));

    private static final String LINHA = "{\"i\":9223372036854775807,\"d\":1e-07,\"b\":true,"
            + "\"ts\":\"2026-08-23T21:53:04.123Z\",\"t\":\"23:59:59.999\",\"s\":\"ola\","
            + "\"j\":{\"a\":[1,2]},\"bin\":\"SGVsbG8=\",\"ip\":\"255.255.255.255\"}";

    // ------------------------------------------------------------ getString

    /**
     * getString devolve O TEXTO DO FIO, para todo tipo.
     *
     * <p>É a regra que resolve o problema de fuso sem mentir e que dá à
     * aplicação uma saída lossless de qualquer coluna: a grafia exata que o
     * servidor emitiu, inclusive para o double, cujo texto NÃO deve ser
     * recomposto por Double.toString (as duas grafias decodificam para o mesmo
     * binary64, mas só uma é a que o servidor escreveu).
     */
    @Test
    void getStringDevolveOTextoDoFio() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            assertEquals("9223372036854775807", rs.getString("i"));
            assertEquals("1e-07", rs.getString("d"));
            assertEquals("true", rs.getString("b"));
            assertEquals("2026-08-23T21:53:04.123Z", rs.getString("ts"));
            assertEquals("23:59:59.999", rs.getString("t"));
            assertEquals("ola", rs.getString("s"));
            assertEquals("{\"a\":[1,2]}", rs.getString("j"));
            assertEquals("SGVsbG8=", rs.getString("bin"));
            assertEquals("255.255.255.255", rs.getString("ip"));
        }
    }

    // ------------------------------------------------------------ numéricos

    /** O int64 no limite do tipo sai EXATO — o caminho nunca passa por double. */
    @Test
    void oInt64NoLimiteDoTipoSaiExato() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            assertEquals(Long.MAX_VALUE, rs.getLong("i"));
        }
    }

    /**
     * getLong NÃO arredonda em silêncio.
     *
     * <p>É o espelho da recusa do servidor a um parâmetro int64 com valor 3.5:
     * quem pediu um inteiro e recebeu 3 não teria como saber que perdeu meio.
     * O valor com fração é recusado; o double que é inteiro em valor passa.
     */
    @Test
    void getLongRecusaFracaoEAceitaODoubleInteiro() throws SQLException {
        String colunas = String.join(",", col("a", "double", "DOUBLE"), col("b", "double", "DOUBLE"));
        try (NishiResultSet rs = resultado(colunas, "{\"a\":3.5,\"b\":1e3}")) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLDataException.class, () -> rs.getLong("a"));
            assertEquals("22003", e.getSQLState());
            assertEquals(1000L, rs.getLong("b"));
        }
    }

    /** getInt recusa o que não cabe em int, em vez de truncar os bits altos. */
    @Test
    void getIntRecusaOQueNaoCabeEmInt() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLDataException.class, () -> rs.getInt("i"));
            assertEquals("22003", e.getSQLState());
        }
    }

    /** O double vem da grafia 'g' do Go, e faz round-trip de BITS. */
    @Test
    void oDoubleFazRoundTripDeBits() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            assertEquals(Double.doubleToLongBits(1e-07), Double.doubleToLongBits(rs.getDouble("d")));
            assertEquals(new BigDecimal("1e-07"), rs.getBigDecimal("d"));
        }
    }

    // ------------------------------------------------------------- temporais

    /**
     * O instante é o mesmo em qualquer fuso da JVM, e o Calendar é IGNORADO.
     *
     * <p>A divergência é deliberada: o contrato do Calendar é situar um relógio
     * de parede sem fuso, e um timestamp do NishiDB já é um instante absoluto.
     * Honrá-lo deslocaria o instante e faria a mesma linha valer coisas
     * diferentes em duas máquinas.
     */
    @Test
    void oTimestampEhOMesmoInstanteEmQualquerFusoEIgnoraOCalendar() throws Exception {
        long esperado = Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli();
        for (String zona : new String[] {"UTC", "Asia/Tokyo"}) {
            em(zona, () -> {
                try (NishiResultSet rs = resultado(TODAS, LINHA)) {
                    assertTrue(rs.next());
                    Timestamp ts = rs.getTimestamp("ts");
                    assertEquals(esperado, ts.getTime(), zona);
                    Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("America/Sao_Paulo"));
                    assertEquals(esperado, rs.getTimestamp("ts", cal).getTime(), zona);
                    assertEquals(Instant.ofEpochMilli(esperado), rs.getObject("ts", Instant.class), zona);
                }
                return null;
            });
        }
    }

    /**
     * A hora do dia sai no RELÓGIO DE PAREDE, com os milissegundos, em Tóquio.
     *
     * <p>É o teste que separa a construção correta das duas óbvias: uma
     * deslocaria o relógio pelo offset da JVM, a outra descartaria os
     * milissegundos.
     */
    @Test
    void aHoraDoDiaSaiNoRelogioDeParedeEmTokyo() throws Exception {
        em("Asia/Tokyo", () -> {
            try (NishiResultSet rs = resultado(TODAS, LINHA)) {
                assertTrue(rs.next());
                Time t = rs.getTime("t");
                assertEquals("23:59:59", t.toString());
                assertEquals(999, Math.floorMod(t.getTime(), 1000L));
                assertEquals(LocalTime.of(23, 59, 59, 999_000_000), rs.getObject("t", LocalTime.class));
            }
            return null;
        });
    }

    /** No getTime o Calendar SIM tem função: ele situa o relógio de parede. */
    @Test
    void oCalendarDoGetTimeSituaORelogioDeParede() throws Exception {
        em("UTC", () -> {
            try (NishiResultSet rs = resultado(TODAS, LINHA)) {
                assertTrue(rs.next());
                Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
                // 23:59:59.999 em Tóquio é 14:59:59.999 UTC do mesmo dia.
                assertEquals(86_399_999L - 9 * 3_600_000L, rs.getTime("t", cal).getTime());
            }
            return null;
        });
    }

    /**
     * A data é o DIA UTC do instante, e é a mesma em qualquer fuso da JVM.
     *
     * <p>Dividir no fuso da JVM faria o mesmo instante cair em dias diferentes
     * conforme a máquina que rodou o relatório: 21:53Z é dia 23 em São Paulo e
     * dia 24 em Tóquio. É a mesma razão de o encoder se recusar a renderizar em
     * hora local.
     */
    @Test
    void aDataEhODiaUtcDoInstante() throws Exception {
        for (String zona : new String[] {"UTC", "Asia/Tokyo", "America/Sao_Paulo"}) {
            em(zona, () -> {
                try (NishiResultSet rs = resultado(TODAS, LINHA)) {
                    assertTrue(rs.next());
                    assertEquals("2026-08-23", rs.getDate("ts").toString(), zona);
                    assertEquals(LocalDate.of(2026, 8, 23), rs.getObject("ts", LocalDate.class), zona);
                    // Com Calendar, o calendário pedido manda — é o uso legítimo
                    // do parâmetro, e o único getter temporal em que ele muda a
                    // divisão em dias.
                    Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
                    assertEquals("2026-08-24", rs.getDate("ts", cal).toString(), zona);
                }
                return null;
            });
        }
    }

    /** Hora do dia não é instante: getTimestamp sobre ela é erro, não uma data em 1970. */
    @Test
    void getTimestampSobreHoraDoDiaEhErro() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLDataException.class, () -> rs.getTimestamp("t"));
            assertEquals("22007", e.getSQLState());
        }
    }

    // --------------------------------------------------------------- bytes

    /** O binário vem em base64 padrão com padding, espelhando o encoder. */
    @Test
    void oBinarioVemEmBase64() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), rs.getBytes("bin"));
        }
    }

    /**
     * getBytes só se aplica à coluna binária.
     *
     * <p>Devolver os bytes UTF-8 de uma coluna de texto seria a convenção do
     * JDBC, mas aqui ela é ambígua de um jeito perigoso: uma coluna bytes que o
     * dataset ainda não fixou chega como OTHER, e o mesmo método devolveria ora
     * o binário decodificado, ora os caracteres do base64. Falhar com o nome do
     * método a usar é a única resposta que não engana.
     */
    @Test
    void getBytesSobreColunaDeTextoFalhaEmVezDeAdivinhar() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLDataException.class, () -> rs.getBytes("s"));
            assertTrue(e.getMessage().contains("getString"), e.getMessage());
        }
    }

    /** Base64 com lixo é erro, e nunca um array truncado. */
    @Test
    void base64InvalidoEhErroENaoArrayTruncado() throws SQLException {
        try (NishiResultSet rs = resultado(col("bin", "bytes", "VARBINARY"), "{\"bin\":\"SGVsbG8=??\"}")) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLDataException.class, () -> rs.getBytes("bin"));
            assertEquals("22018", e.getSQLState());
        }
    }

    // -------------------------------------------------------------- nulos

    /**
     * wasNull grava a flag ao ver null e a LIMPA ao ver valor.
     *
     * <p>Manter a flag entre chamadas é o bug clássico: a coluna seguinte, com
     * valor legítimo, seria lida como nula pela aplicação.
     */
    @Test
    void wasNullGravaELimpaAFlag() throws SQLException {
        String colunas = String.join(",", col("a", "int64", "BIGINT"), col("b", "int64", "BIGINT"));
        try (NishiResultSet rs = resultado(colunas, "{\"a\":null,\"b\":7}")) {
            assertTrue(rs.next());
            assertEquals(0L, rs.getLong("a"));
            assertTrue(rs.wasNull());
            assertEquals(7L, rs.getLong("b"));
            assertFalse(rs.wasNull());
            assertNull(rs.getString("a"));
            assertTrue(rs.wasNull());
        }
    }

    // -------------------------------------------------------------- OTHER

    /**
     * A coluna OTHER é a única cujo metadado pode DISCORDAR do dado.
     *
     * <p>Um campo auto que nenhum evento fixou é anunciado OTHER no schema, e a
     * ingestão é concorrente: a linha seguinte pode trazer um inteiro. getObject
     * responde pelo token efetivo e getString sempre funciona.
     */
    @Test
    void aColunaOtherRespondePeloTokenEfetivo() throws SQLException {
        String colunas = String.join(",", col("a", "unknown", "OTHER"), col("b", "unknown", "OTHER"),
                col("c", "unknown", "OTHER"), col("d", "unknown", "OTHER"));
        try (NishiResultSet rs = resultado(colunas, "{\"a\":42,\"b\":1.5,\"c\":\"x\",\"d\":true}")) {
            assertTrue(rs.next());
            assertEquals(42L, rs.getObject("a"));
            assertEquals(1.5d, rs.getObject("b"));
            assertEquals("x", rs.getObject("c"));
            assertEquals(Boolean.TRUE, rs.getObject("d"));
            assertEquals("42", rs.getString("a"));
        }
    }

    /** getObject de coluna declarada segue o TIPO, não o token. */
    @Test
    void getObjectDeColunaDeclaradaSegueOTipo() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            assertInstanceOf(Long.class, rs.getObject("i"));
            // O double 1e-07 chega como número; um dataset com valor inteiro
            // emitiria "1", e sem o tipo declarado sairia Long.
            assertInstanceOf(Double.class, rs.getObject("d"));
            assertInstanceOf(Boolean.class, rs.getObject("b"));
            assertInstanceOf(Timestamp.class, rs.getObject("ts"));
            assertInstanceOf(Time.class, rs.getObject("t"));
            assertInstanceOf(String.class, rs.getObject("s"));
            assertInstanceOf(byte[].class, rs.getObject("bin"));
        }
    }

    /** O double inteiro sai Double, e não Long: quem declarou o tipo foi o schema. */
    @Test
    void oDoubleInteiroNaoViraLong() throws SQLException {
        try (NishiResultSet rs = resultado(col("d", "double", "DOUBLE"), "{\"d\":1}")) {
            assertTrue(rs.next());
            assertInstanceOf(Double.class, rs.getObject("d"));
            assertEquals(1.0d, rs.getDouble("d"));
        }
    }

    // ---------------------------------------------------------- findColumn

    /**
     * findColumn casa EXATO primeiro, e só depois sem caixa.
     *
     * <p>Os identificadores do NishiDB são case-sensitive, e o JDBC promete
     * casamento sem caixa. A ordem resolve o conflito sem perder nenhuma das
     * duas: quem escreveu o nome exato recebe a coluna exata; quem escreveu
     * outra caixa recebe a única que casa. Duas colunas casando sem caixa é
     * AMBIGUIDADE, e responder "a primeira" entregaria a coluna errada em
     * silêncio.
     */
    @Test
    void findColumnCasaExatoPrimeiroEDepoisSemCaixa() throws SQLException {
        String colunas = String.join(",", col("Nome", "string", "VARCHAR"), col("nome", "string", "VARCHAR"),
                col("idade", "int64", "BIGINT"));
        try (NishiResultSet rs = resultado(colunas, "{\"Nome\":\"a\",\"nome\":\"b\",\"idade\":1}")) {
            assertTrue(rs.next());
            assertEquals("a", rs.getString("Nome"));
            assertEquals("b", rs.getString("nome"));
            assertEquals(1L, rs.getLong("IDADE"));
            SQLException e = assertThrows(SQLException.class, () -> rs.findColumn("NOME"));
            assertTrue(e.getMessage().contains("ambígu") || e.getMessage().contains("ambigu"), e.getMessage());
            SQLException f = assertThrows(SQLException.class, () -> rs.findColumn("inexistente"));
            assertTrue(f.getMessage().contains("inexistente"), f.getMessage());
        }
    }

    // ---------------------------------------------------------- metadados

    /** O ResultSetMetaData publica o que a linha de schema trouxe, sem recalcular. */
    @Test
    void oMetadataPublicaOQueALinhaDeSchemaTrouxe() throws SQLException {
        String colunas = String.join(",",
                col("id", "string", "VARCHAR", false, 0, 0, 0),
                col("quando", "timestamp", "TIMESTAMP", true, 24, 3, 24),
                col("doc", "json", "VARCHAR", true, 0, 0, 0));
        try (NishiResultSet rs = resultado(colunas, "{\"id\":\"a\",\"quando\":\"2026-01-01T00:00:00.000Z\",\"doc\":{}}")) {
            ResultSetMetaData m = rs.getMetaData();
            assertEquals(3, m.getColumnCount());
            assertEquals("id", m.getColumnName(1));
            assertEquals("id", m.getColumnLabel(1));
            assertEquals("eventos", m.getTableName(1));
            assertEquals(ResultSetMetaData.columnNoNulls, m.isNullable(1));
            assertEquals(ResultSetMetaData.columnNullable, m.isNullable(2));

            assertEquals(Types.TIMESTAMP, m.getColumnType(2));
            assertEquals("timestamp", m.getColumnTypeName(2));
            assertEquals(24, m.getPrecision(2));
            assertEquals(3, m.getScale(2));
            assertEquals(24, m.getColumnDisplaySize(2));
            assertEquals("java.sql.Timestamp", m.getColumnClassName(2));

            // json não é pesquisável: o binder recusa comparação sobre ele.
            assertTrue(m.isSearchable(1));
            assertFalse(m.isSearchable(3));
            assertTrue(m.isReadOnly(1));
            assertFalse(m.isWritable(1));

            SQLException e = assertThrows(SQLException.class, () -> m.getColumnName(4));
            assertTrue(e.getMessage().contains("4"), e.getMessage());
        }
    }

    /** O metadata responde ANTES do primeiro next(): é o cabeçalho do grid. */
    @Test
    void oMetadataRespondeAntesDoPrimeiroNext() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertEquals(9, rs.getMetaData().getColumnCount());
        }
    }

    // ------------------------------------------------------------- estado

    /** Sem linha corrente, um getter é erro de cursor — nunca um valor default. */
    @Test
    void getterSemLinhaCorrenteEhErroDeCursor() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            SQLException antes = assertThrows(SQLException.class, () -> rs.getString(1));
            assertEquals("24000", antes.getSQLState());
            assertTrue(rs.next());
            assertFalse(rs.next());
            SQLException depois = assertThrows(SQLException.class, () -> rs.getString(1));
            assertEquals("24000", depois.getSQLState());
        }
    }

    @Test
    void oCursorEhSoParaFrenteEDizIsso() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
            assertThrows(SQLFeatureNotSupportedException.class, rs::previous);
            assertThrows(SQLFeatureNotSupportedException.class, () -> rs.absolute(1));
            assertThrows(SQLFeatureNotSupportedException.class, rs::isBeforeFirst);
            assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateInt(1, 1));
            assertEquals(0, rs.getRow());
            assertTrue(rs.next());
            assertEquals(1, rs.getRow());
        }
    }

    /**
     * setFetchSize é aceito em SILÊNCIO, e é a única exceção legítima à regra.
     *
     * <p>O contrato do JDBC diz que ele é uma DICA; o stream do NishiDB não tem
     * como atendê-lo, e lançar aqui quebraria toda ferramenta que o chama por
     * hábito antes de executar.
     */
    @Test
    void setFetchSizeEhDicaEEhAceitoEmSilencio() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            rs.setFetchSize(500);
            assertEquals(0, rs.getFetchSize());
            assertThrows(SQLException.class, () -> rs.setFetchSize(-1));
            assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            assertThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
        }
    }

    /** Fechar é idempotente, e depois de fechado todo método de dado é erro. */
    @Test
    void fecharEhIdempotente() throws SQLException {
        NishiResultSet rs = resultado(TODAS, LINHA);
        rs.close();
        rs.close();
        assertTrue(rs.isClosed());
        SQLException e = assertThrows(SQLException.class, rs::next);
        assertEquals("24000", e.getSQLState());
    }

    /** O aviso do teto do dataset chega à aplicação pelo caminho do JDBC. */
    @Test
    void oAvisoDoTetoChegaPeloGetWarnings() throws SQLException {
        String corpo = "{\"nishidb\":\"schema\",\"dataset\":\"eventos\",\"columns\":["
                + col("a", "int64", "BIGINT") + "]}\n{\"a\":1}\n"
                + "{\"nishidb\":\"error\",\"code\":\"MAX_ROWS_EXCEEDED\",\"rows\":1,\"message\":\"teto\"}\n";
        try (NishiResultSet rs = new NishiResultSet(new StreamCursor(new LeitorNdjson(
                new ByteArrayInputStream(corpo.getBytes(StandardCharsets.UTF_8)))), null)) {
            assertTrue(rs.next());
            assertFalse(rs.next());
            assertNotNull(rs.getWarnings());
            assertEquals("53400", rs.getWarnings().getSQLState());
            rs.clearWarnings();
            assertNull(rs.getWarnings());
        }
    }

    /** Conversão para um alvo que o driver não faz é 0A000, e nomeia o alvo. */
    @Test
    void alvoNaoSuportadoEmGetObjectEhNomeado() throws SQLException {
        try (NishiResultSet rs = resultado(TODAS, LINHA)) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLFeatureNotSupportedException.class,
                    () -> rs.getObject("s", java.util.regex.Pattern.class));
            assertEquals("0A000", e.getSQLState());
            assertTrue(e.getMessage().contains("Pattern"), e.getMessage());
        }
    }
}
