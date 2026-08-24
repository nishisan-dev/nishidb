package dev.nishisan.nishidb.jdbc.wire;

import java.util.List;

/** As linhas de moldura da resposta de {@code POST /v1/query}. */
public final class Molduras {

    /** A chave que discrimina uma linha de moldura de uma linha de dados. */
    public static final String CHAVE = "nishidb";

    public static final String SCHEMA = "schema";
    public static final String FIM = "end";
    public static final String ERRO = "error";

    /**
     * Uma coluna da projeção, como a linha de schema a descreve.
     *
     * <p>É o ResultSetMetaData inteiro, e vem do servidor em vez de ser
     * recalculado aqui: assim um tipo novo no servidor não exige jar novo no
     * cliente.
     */
    public record Coluna(String name, String type, String jdbc, boolean nullable,
                         int precision, int scale, int displaySize) {}

    /** A primeira linha, sempre. */
    public record LinhaSchema(String dataset, List<Coluna> columns) {}

    /**
     * O fecho de SUCESSO.
     *
     * <p>{@code truncated} distingue "acabou" de "parou no maxRows que o
     * PRÓPRIO cliente pediu" — e atender ao que foi pedido não é falha.
     */
    public record LinhaFim(long rows, boolean truncated) {}

    /** O fecho de FALHA, depois de N rows já entregues. */
    public record LinhaErro(String code, long rows, String message) {}

    private Molduras() {
    }
}
