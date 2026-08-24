package dev.nishisan.nishidb.jdbc.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

/**
 * O fuso da JVM é o assunto desta classe.
 *
 * <p>Metade dos testes roda DUAS vezes, em UTC e em Asia/Tokyo, porque os dois
 * construtores óbvios de {@code java.sql.Time} estão errados e os dois
 * funcionam por acidente em UTC — que é o fuso da máquina de quem escreve o
 * driver e nunca o de quem o usa.
 */
class TemporaisTest {

    /** Roda o corpo com o fuso default trocado, e o devolve ao fim. */
    private static <T> T em(String zona, Callable<T> corpo) throws Exception {
        TimeZone anterior = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zona));
            return corpo.call();
        } finally {
            TimeZone.setDefault(anterior);
        }
    }

    // ----------------------------------------------------------- leitura

    /** A grafia exata que o encoder do servidor emite, com a fração sempre em 3 dígitos. */
    @Test
    void instanteNaGrafiaDoFio() throws SQLException {
        assertEquals(Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli(),
                Temporais.instante("2026-08-23T21:53:04.123Z"));
        assertEquals(0L, Temporais.instante("1970-01-01T00:00:00.000Z"));
        assertEquals(-1L, Temporais.instante("1969-12-31T23:59:59.999Z"));
    }

    /**
     * Ano fora de [0,9999] tem grafia própria no encoder, e o driver a lê.
     *
     * <p>O formatador do servidor emite o ano com no MÍNIMO quatro dígitos e
     * com sinal quando negativo — não com o '+' que o ISO-8601 estendido usa.
     * Um parser que exigisse exatamente quatro dígitos falharia num dado que o
     * servidor sabe emitir.
     */
    @Test
    void anoForaDaFaixaDeQuatroDigitos() throws SQLException {
        assertEquals(LocalDate.of(10000, 1, 1).toEpochDay() * 86_400_000L,
                Temporais.instante("10000-01-01T00:00:00.000Z"));
        assertEquals(LocalDate.of(-1, 1, 1).toEpochDay() * 86_400_000L,
                Temporais.instante("-0001-01-01T00:00:00.000Z"));
    }

    /** A fração é opcional: o fio sempre a manda, mas texto de humano nem sempre. */
    @Test
    void fracaoOpcional() throws SQLException {
        assertEquals(Instant.parse("2026-01-01T00:00:00Z").toEpochMilli(),
                Temporais.instante("2026-01-01T00:00:00Z"));
        assertEquals(Instant.parse("2026-01-01T00:00:00.120Z").toEpochMilli(),
                Temporais.instante("2026-01-01T00:00:00.12Z"));
    }

    /**
     * O que NÃO é a grafia do fio é recusado, e nomeado.
     *
     * <p>Inclusive o offset explícito: o fio é sempre UTC com Z, e aceitar
     * ±hh:mm aqui abriria a porta para o driver interpretar como instante um
     * texto que veio de outro lugar. Quem tem um desses usa getString.
     */
    @Test
    void grafiaQueNaoEhADoFioEhRecusada() {
        for (String ruim : new String[] {
                "2026-08-23 21:53:04.123Z", "2026-08-23T21:53:04.123", "2026-08-23T21:53:04.123-03:00",
                "2026-13-01T00:00:00.000Z", "2026-02-30T00:00:00.000Z", "26-01-01T00:00:00.000Z",
                "", "abc", "2026-01-01T24:00:00.000Z"}) {
            SQLException e = assertThrows(SQLException.class, () -> Temporais.instante(ruim), ruim);
            assertEquals("22007", e.getSQLState(), ruim);
        }
    }

    /** A hora do dia não tem fuso nem data: são milissegundos desde a meia-noite. */
    @Test
    void horaDoDiaNaGrafiaDoFio() throws SQLException {
        assertEquals(0L, Temporais.horaDoDia("00:00:00.000"));
        assertEquals(86_399_999L, Temporais.horaDoDia("23:59:59.999"));
        assertEquals(3_600_000L, Temporais.horaDoDia("01:00:00"));
    }

    @Test
    void horaDoDiaForaDeFaixaEhRecusada() {
        for (String ruim : new String[] {"24:00:00.000", "23:60:00.000", "23:59:60.000", "1:00:00", "", "23:59"}) {
            SQLException e = assertThrows(SQLException.class, () -> Temporais.horaDoDia(ruim), ruim);
            assertEquals("22007", e.getSQLState(), ruim);
        }
    }

    // ----------------------------------------------------------- construção

    /**
     * O timestamp preserva o INSTANTE, em qualquer fuso da JVM.
     *
     * <p>É o que permite ao driver ignorar o Calendar de
     * {@code getTimestamp(int, Calendar)} sem perder informação: não há relógio
     * de parede a reinterpretar, só um instante — e ele é o mesmo em Tóquio e
     * em Londres. Só o toString() muda.
     */
    @Test
    void oTimestampPreservaOInstanteEmQualquerFuso() throws Exception {
        long millis = Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli();
        for (String zona : new String[] {"UTC", "Asia/Tokyo", "America/Sao_Paulo"}) {
            Timestamp ts = em(zona, () -> Temporais.timestampDe(millis));
            assertEquals(millis, ts.getTime(), zona);
            assertEquals(123_000_000, ts.getNanos(), zona);
        }
    }

    /**
     * A HORA DO DIA é onde os dois construtores óbvios erram.
     *
     * <p>{@code new Time(millisDoDia)} interpreta os milissegundos como
     * instante e desloca o relógio pelo offset da JVM: em Tóquio, 23:59:59
     * viraria 08:59:59. E {@code Time.valueOf(LocalTime)} DESCARTA os
     * milissegundos, porque java.sql.Time não os modela na conversão. O teste
     * cobra as duas coisas ao mesmo tempo, e em fuso não-UTC — onde o primeiro
     * defeito aparece.
     */
    @Test
    void aHoraDoDiaSaiNoRelogioDeParedeEComOsMilissegundos() throws Exception {
        for (String zona : new String[] {"UTC", "Asia/Tokyo", "America/Sao_Paulo"}) {
            // As asserções rodam DENTRO do fuso: o toString() de java.sql.Time
            // renderiza no fuso corrente da JVM, e conferi-lo depois de o fuso
            // ser restaurado mediria outra coisa.
            em(zona, () -> {
                Time t = Temporais.timeDe(86_399_999L, ZoneId.systemDefault());
                assertEquals("23:59:59", t.toString(), zona);
                assertEquals(999, Math.floorMod(t.getTime(), 1000L), zona);
                return null;
            });
        }
    }

    /**
     * O Calendar de getTime SIM tem o que fazer: ele diz em que fuso o relógio
     * de parede deve ser situado.
     */
    @Test
    void aHoraDoDiaHonraOFusoPedido() throws Exception {
        Time emTokyo = em("UTC", () -> Temporais.timeDe(0L, ZoneId.of("Asia/Tokyo")));
        // Meia-noite em Tóquio é 15:00 do dia anterior em UTC.
        assertEquals(-9 * 3_600_000L, emTokyo.getTime());
    }

    /** O LocalTime é o caminho lossless recomendado ao usuário. */
    @Test
    void oLocalTimeCarregaOsMilissegundos() {
        assertEquals(LocalTime.of(23, 59, 59, 999_000_000), Temporais.localTimeDe(86_399_999L));
    }

    /**
     * A data é o dia UTC do instante, e é a MESMA em qualquer fuso da JVM.
     *
     * <p>Duas decisões aqui, e as duas são deliberadas. A primeira: o
     * java.sql.Date sai NORMALIZADO — hora, minuto, segundo e milissegundo em
     * zero —, porque é o que o contrato do tipo exige; devolver o instante cru
     * faria o toString() mostrar a data certa e o equals() contra um
     * Date.valueOf falhar, que é um defeito que se esconde.
     *
     * <p>A segunda: a divisão em dia é feita em UTC, e não no fuso da JVM. É a
     * mesma doutrina do encoder, que se recusa a renderizar em hora local
     * porque o MESMO dado servido de dois hosts produziria respostas
     * diferentes. Um instante de 21:53Z cairia no dia seguinte em Tóquio e no
     * mesmo dia em São Paulo, e o relatório mudaria de dia conforme a máquina
     * que o rodou.
     */
    @Test
    void aDataEhODiaUtcDoInstanteEmQualquerFuso() throws Exception {
        long millis = Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli();
        for (String zona : new String[] {"UTC", "Asia/Tokyo", "America/Sao_Paulo"}) {
            em(zona, () -> {
                Date d = Temporais.dateDe(millis, ZoneOffset.UTC);
                assertEquals("2026-08-23", d.toString(), zona);
                assertEquals(Date.valueOf(LocalDate.of(2026, 8, 23)).getTime(), d.getTime(), zona);
                return null;
            });
        }
    }

    /** Com fuso pedido, a divisão em dia o honra: é o caso legítimo do Calendar. */
    @Test
    void aDataHonraOFusoPedido() throws Exception {
        long millis = Instant.parse("2026-08-23T21:53:04.123Z").toEpochMilli();
        em("UTC", () -> {
            assertEquals("2026-08-24", Temporais.dateDe(millis, ZoneId.of("Asia/Tokyo")).toString());
            return null;
        });
    }
}
