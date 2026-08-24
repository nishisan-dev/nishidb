package dev.nishisan.nishidb.jdbc.wire;

import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * A ponte entre a grafia temporal do fio e os tipos do {@code java.sql}.
 *
 * <p>Vive num lugar só porque é onde o driver mais tem como errar em silêncio:
 * os dois construtores óbvios de {@link Time} estão errados, e os dois
 * funcionam por acidente em UTC — que é o fuso da máquina de quem escreve o
 * driver e quase nunca o de quem o usa.
 *
 * <p><b>Timestamp e datetime são INSTANTES.</b> Saem do fio como
 * {@code 2026-08-23T21:53:04.123Z}, sempre em UTC, sempre com três dígitos de
 * fração. {@code new Timestamp(millis)} preserva o instante exatamente; só o
 * {@code toString()} o renderiza no fuso da JVM. É por isso que
 * {@code getTimestamp(int, Calendar)} pode IGNORAR o Calendar sem perder nada:
 * o contrato do Calendar é situar um relógio de parede que não tem fuso, e aqui
 * não há relógio de parede a situar.
 *
 * <p><b>Time é hora do dia, e NÃO é instante.</b> Sai como
 * {@code 23:59:59.999}, sem Z, porque não há fuso a anunciar. Aqui o Calendar
 * SIM tem função — é ele que diz em que fuso aquele relógio de parede deve ser
 * situado —, e é por isso que {@link #timeDe} recebe a zona em vez de assumir a
 * default.
 */
public final class Temporais {

    private static final long MILLIS_POR_DIA = 86_400_000L;

    private Temporais() {
    }

    // -------------------------------------------------------------- leitura

    /**
     * Milissegundos de epoch a partir da grafia de {@code timestamp} e
     * {@code datetime}.
     *
     * <p>Aceita o ano com QUATRO OU MAIS dígitos e com sinal quando negativo,
     * que é o que o formatador do servidor emite — e não o {@code '+'} do
     * ISO-8601 estendido. Um parser que exigisse exatamente quatro dígitos
     * falharia num dado que o servidor sabe produzir.
     *
     * <p>Aceita a fração ausente ou com até nove dígitos, e exige o {@code Z}.
     * Offset explícito é RECUSADO de propósito: o fio é sempre UTC, e aceitar
     * {@code ±hh:mm} aqui faria o driver interpretar como instante um texto que
     * veio de outro lugar. Quem tem um desses usa {@code getString}.
     */
    public static long instante(String texto) throws SQLException {
        if (texto == null) {
            throw temporal("null", "instante");
        }
        Cursor c = new Cursor(texto);
        try {
            boolean anoNegativo = c.consome('-');
            int ano = c.digitos(4, 9);
            if (anoNegativo) {
                ano = -ano;
            }
            c.exige('-');
            int mes = c.digitos(2, 2);
            c.exige('-');
            int dia = c.digitos(2, 2);
            c.exige('T');
            long millisDoDia = c.horaDoDia();
            if (!c.consome('Z') || !c.fim()) {
                throw temporal(texto, "instante");
            }
            long dias = LocalDate.of(ano, mes, dia).toEpochDay();
            return Math.addExact(Math.multiplyExact(dias, MILLIS_POR_DIA), millisDoDia);
        } catch (DateTimeException | ArithmeticException | FormatoInvalido e) {
            throw temporal(texto, "instante");
        }
    }

    /**
     * Milissegundos desde a meia-noite a partir da grafia de {@code time}.
     *
     * <p>Sem data e sem fuso: é o que o tipo separado existe para dizer.
     */
    public static long horaDoDia(String texto) throws SQLException {
        if (texto == null) {
            throw temporal("null", "hora do dia");
        }
        Cursor c = new Cursor(texto);
        try {
            long millis = c.horaDoDia();
            if (!c.fim()) {
                throw temporal(texto, "hora do dia");
            }
            return millis;
        } catch (FormatoInvalido e) {
            throw temporal(texto, "hora do dia");
        }
    }

    // ----------------------------------------------------------- construção

    /** O instante, exato. Só o toString() o renderiza no fuso da JVM. */
    public static Timestamp timestampDe(long millisUtc) {
        return new Timestamp(millisUtc);
    }

    /**
     * A data do instante, preservando o instante.
     *
     * <p>Consequência declarada: em Tóquio, um instante de 21:53Z aparece como
     * o dia SEGUINTE no {@code toString()}. É a mesma regra do timestamp, e ter
     * duas regras seria pior — o usuário veria a data e a hora discordarem.
     */
    public static Date dateDe(long millisUtc) {
        return new Date(millisUtc);
    }

    /**
     * A hora do dia como {@link Time}, situada na zona informada.
     *
     * <p>As duas construções óbvias estão erradas, e é por isto que esta função
     * existe. {@code new Time(millisDoDia)} interpreta os milissegundos como
     * instante e desloca o relógio pelo offset da JVM — em Tóquio, 23:59:59
     * viraria 08:59:59. E {@code Time.valueOf(LocalTime)} DESCARTA os
     * milissegundos. O caminho correto passa pelo relógio de parede: a hora vai
     * para o dia zero do epoch e é situada na zona pedida.
     */
    public static Time timeDe(long millisDoDia, ZoneId zona) {
        LocalTime hora = localTimeDe(millisDoDia);
        return new Time(LocalDate.EPOCH.atTime(hora).atZone(zona).toInstant().toEpochMilli());
    }

    /** O caminho LOSSLESS recomendado à aplicação: {@code getObject(col, LocalTime.class)}. */
    public static LocalTime localTimeDe(long millisDoDia) {
        return LocalTime.ofNanoOfDay(millisDoDia * 1_000_000L);
    }

    // ------------------------------------------------------------- interno

    /** Sinaliza gramática errada dentro do cursor, sem custo de mensagem. */
    private static final class FormatoInvalido extends RuntimeException {
        private static final long serialVersionUID = 1L;

        FormatoInvalido() {
            super(null, null, false, false);
        }
    }

    /** Um cursor de dígitos sobre o texto — sem regex, sem alocação por campo. */
    private static final class Cursor {
        private final String s;
        private int i;

        Cursor(String s) {
            this.s = s;
        }

        boolean fim() {
            return i >= s.length();
        }

        boolean consome(char c) {
            if (i < s.length() && s.charAt(i) == c) {
                i++;
                return true;
            }
            return false;
        }

        void exige(char c) {
            if (!consome(c)) {
                throw new FormatoInvalido();
            }
        }

        /** Lê entre {@code min} e {@code max} dígitos, e devolve o valor. */
        int digitos(int min, int max) {
            int valor = 0;
            int lidos = 0;
            while (lidos < max && i < s.length()) {
                char c = s.charAt(i);
                if (c < '0' || c > '9') {
                    break;
                }
                valor = valor * 10 + (c - '0');
                i++;
                lidos++;
            }
            if (lidos < min) {
                throw new FormatoInvalido();
            }
            return valor;
        }

        /** {@code hh:mm:ss[.f{1,9}]} → milissegundos desde a meia-noite. */
        long horaDoDia() {
            int hora = digitos(2, 2);
            exige(':');
            int minuto = digitos(2, 2);
            exige(':');
            int segundo = digitos(2, 2);
            // Faixa conferida à mão: LocalTime aceitaria 24:00 como fim de dia,
            // e o fio nunca o emite — aceitá-lo faria a hora do dia valer
            // MILLIS_POR_DIA, que é o valor que o executor recusa.
            if (hora > 23 || minuto > 59 || segundo > 59) {
                throw new FormatoInvalido();
            }
            int millis = 0;
            if (consome('.')) {
                int ini = i;
                int valor = digitos(1, 9);
                int lidos = i - ini;
                // Normaliza para três casas: 12 é 120 ms, 1234 é 123 ms.
                for (; lidos < 3; lidos++) {
                    valor *= 10;
                }
                for (; lidos > 3; lidos--) {
                    valor /= 10;
                }
                millis = valor;
            }
            return ((hora * 60L + minuto) * 60L + segundo) * 1000L + millis;
        }
    }

    private static SQLException temporal(String texto, String oQue) {
        return new SQLDataException("\"" + texto + "\" nao e " + (oQue.equals("instante")
                ? "um instante na grafia do fio (YYYY-MM-DDThh:mm:ss.sssZ, sempre UTC)"
                : "uma hora do dia na grafia do fio (hh:mm:ss.sss)"),
                SqlStates.FORMATO_TEMPORAL_INVALIDO);
    }
}
