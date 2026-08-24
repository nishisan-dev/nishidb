package dev.nishisan.nishidb.jdbc.wire;

/**
 * Os códigos de erro do protocolo, como LITERAIS.
 *
 * <p>São estáveis por contrato: o cliente casa por código, nunca pelo texto da
 * mensagem, que é prosa para humanos e pode ser reescrita.
 *
 * <p>As strings estão escritas à mão aqui de propósito, e é isso que torna o
 * gate do lado Go possível: ele lê as constantes do internal/protocolo e
 * procura cada literal NESTE arquivo. Um código novo no servidor sem tradução
 * aqui reprova a suíte Go, antes de qualquer JVM subir.
 */
public final class Codigos {

    public static final String BAD_REQUEST = "BAD_REQUEST";
    public static final String BAD_PARAMETER = "BAD_PARAMETER";
    public static final String MAX_ROWS_ABOVE_LIMIT = "MAX_ROWS_ABOVE_LIMIT";
    public static final String RESERVED_COLUMN = "RESERVED_COLUMN";
    public static final String SYNTAX = "SYNTAX";
    public static final String BIND = "BIND";
    public static final String UNSUPPORTED_CLAUSE = "UNSUPPORTED_CLAUSE";
    public static final String UNKNOWN_DATASET = "UNKNOWN_DATASET";
    public static final String METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";
    public static final String UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE";
    public static final String REQUEST_TOO_LARGE = "REQUEST_TOO_LARGE";
    public static final String TOO_MANY_QUERIES = "TOO_MANY_QUERIES";
    public static final String TIMEOUT = "TIMEOUT";
    public static final String MAX_ROWS_EXCEEDED = "MAX_ROWS_EXCEEDED";
    public static final String DATASET_CORRUPT = "DATASET_CORRUPT";
    public static final String INTERNAL = "INTERNAL";
    public static final String UNAUTHENTICATED = "UNAUTHENTICATED";
    public static final String FORBIDDEN = "FORBIDDEN";
    public static final String WRITER_FAILED = "WRITER_FAILED";
    public static final String COMPACTION_BUSY = "COMPACTION_BUSY";

    private Codigos() {
    }
}
