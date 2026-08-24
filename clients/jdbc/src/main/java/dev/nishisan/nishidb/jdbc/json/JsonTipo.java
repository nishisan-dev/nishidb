package dev.nishisan.nishidb.jdbc.json;

/**
 * O tipo de um valor JSON, como o scanner o encontrou no fio.
 *
 * <p>É o tipo do TOKEN, e não o tipo da coluna: uma coluna declarada
 * {@code int64} chega como {@link #NUMERO}, e uma coluna {@code auto} que
 * nenhum evento fixou pode chegar como qualquer um. Quem decide o que fazer com
 * o valor é a conversão, que conhece as duas coisas.
 */
public enum JsonTipo {
    /** Texto entre aspas. O span aponta para o CONTEÚDO, ainda escapado. */
    TEXTO,
    /** Número. O span cobre o token inteiro, e nada foi convertido ainda. */
    NUMERO,
    /** {@code true} ou {@code false}. */
    BOOLEANO,
    /** {@code null} explícito — que é como o servidor emite coluna nula. */
    NULO,
    /**
     * Objeto ou array aninhado, entregue CRU.
     *
     * <p>Existe por causa da coluna {@code json}, que o servidor emite inline e
     * verbatim, e não como string. Um parser que só esperasse valores escalares
     * dessincronizaria a linha inteira ao encontrar o primeiro documento.
     */
    BRUTO
}
