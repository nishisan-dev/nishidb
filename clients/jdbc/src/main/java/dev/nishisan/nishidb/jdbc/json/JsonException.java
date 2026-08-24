package dev.nishisan.nishidb.jdbc.json;

/**
 * Falha ao ler JSON, com o OFFSET do byte que a causou.
 *
 * <p>O offset não é enfeite: quando o servidor emite algo que o driver não
 * entende, a diferença entre "resposta malformada" e "resposta malformada no
 * byte 4.812, era esperado ',' ou '}'" é a diferença entre um chamado sem
 * saída e um diagnóstico.
 *
 * <p>É exceção CHECADA de propósito. Quem a captura tem de decidir qual
 * SQLState publicar — falha de transporte (08006) quando veio do fio, erro de
 * dado (22018) quando veio de uma conversão —, e uma exceção não-checada
 * atravessaria essa decisão sem que nada obrigasse a tomá-la.
 */
public class JsonException extends Exception {

    private static final long serialVersionUID = 1L;

    private final int offset;

    public JsonException(int offset, String mensagem) {
        super(mensagem + " (byte " + offset + ")");
        this.offset = offset;
    }

    /** O offset do byte que causou a falha, 0-based. */
    public int offset() {
        return offset;
    }
}
