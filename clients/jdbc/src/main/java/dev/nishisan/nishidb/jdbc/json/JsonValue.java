package dev.nishisan.nishidb.jdbc.json;

import java.util.List;
import java.util.Map;

/**
 * Um valor JSON já materializado — o DOM que os documentos PEQUENOS do
 * protocolo usam.
 *
 * <p>Pequenos é a palavra: a linha de schema, /v1/metadata, /v1/stats,
 * /v1/health e o corpo de erro. Todos são lidos uma vez por conexão ou por
 * consulta, e nenhum cresce com o resultado. As LINHAS DE DADOS não passam por
 * aqui — elas usam o scanner com spans, porque materializá-las seria
 * exatamente o buffering proporcional ao resultado que o §128 proíbe.
 *
 * <p>{@link Numero} guarda o TEXTO original, e não um {@code double} já
 * convertido. Um maxRows ou um contador de rows acima de 2^53 perderia
 * precisão na conversão, e o erro seria silencioso — é a mesma razão de o
 * campo "value" do parâmetro ser {@code json.RawMessage} do lado Go.
 */
public sealed interface JsonValue {

    record Objeto(Map<String, JsonValue> campos) implements JsonValue {}

    record Arranjo(List<JsonValue> itens) implements JsonValue {}

    record Texto(String valor) implements JsonValue {}

    record Numero(String cru) implements JsonValue {}

    record Booleano(boolean valor) implements JsonValue {}

    record Nulo() implements JsonValue {}

    /** O campo do objeto, ou null quando não existe (ou quando isto não é objeto). */
    default JsonValue campo(String nome) {
        return this instanceof Objeto o ? o.campos().get(nome) : null;
    }

    /**
     * O texto do campo, ou {@code padrao} quando ausente ou nulo.
     *
     * <p>Campo AUSENTE e campo {@code null} caem no mesmo lugar de propósito:
     * um servidor mais novo pode acrescentar campos, e um mais velho pode não
     * ter o que o driver pede. Nenhum dos dois é motivo para derrubar a
     * conexão.
     */
    default String textoOu(String nome, String padrao) {
        return campo(nome) instanceof Texto t ? t.valor() : padrao;
    }

    default boolean booleanoOu(String nome, boolean padrao) {
        return campo(nome) instanceof Booleano b ? b.valor() : padrao;
    }

    /** O inteiro do campo, EXATO, ou {@code padrao} quando ausente/ilegível. */
    default long inteiroOu(String nome, long padrao) {
        if (!(campo(nome) instanceof Numero n)) {
            return padrao;
        }
        try {
            byte[] b = n.cru().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            return JsonScanner.inteiro(b, 0, b.length);
        } catch (JsonException e) {
            return padrao;
        }
    }

    /** Os itens do array, ou lista vazia quando ausente ou de outro tipo. */
    default List<JsonValue> itensDe(String nome) {
        return campo(nome) instanceof Arranjo a ? a.itens() : List.of();
    }
}
