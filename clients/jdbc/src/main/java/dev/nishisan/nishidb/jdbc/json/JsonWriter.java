package dev.nishisan.nishidb.jdbc.json;

/**
 * Escreve JSON — o corpo do {@code POST /v1/query} e nada além disso.
 *
 * <p>É pequeno de propósito: o driver produz um documento só, de forma
 * conhecida ({@code dataset}, {@code sql}, {@code parameters}, {@code maxRows},
 * {@code timeoutMs}). Um serializador genérico por reflexão seria mais código
 * para menos garantia.
 */
public final class JsonWriter {

    private final StringBuilder sb = new StringBuilder(256);
    private boolean primeiroCampo = true;

    /** Abre o objeto de topo. */
    public JsonWriter() {
        sb.append('{');
    }

    /** Campo de texto, com o valor escapado. */
    public JsonWriter texto(String nome, String valor) {
        chave(nome);
        escreveTexto(sb, valor);
        return this;
    }

    /** Campo inteiro. */
    public JsonWriter inteiro(String nome, long valor) {
        chave(nome);
        sb.append(valor);
        return this;
    }

    /**
     * Campo cujo valor já é JSON pronto.
     *
     * <p>É por aqui que o array de parâmetros entra: o "value" de cada um é
     * texto JSON construído a partir do que a aplicação passou em setLong ou
     * setDouble, e reconvertê-lo aqui seria uma segunda chance de errar a
     * grafia de um número.
     */
    public JsonWriter bruto(String nome, String jsonPronto) {
        chave(nome);
        sb.append(jsonPronto);
        return this;
    }

    /** Fecha o objeto e devolve o documento. */
    public String fecha() {
        sb.append('}');
        return sb.toString();
    }

    private void chave(String nome) {
        if (primeiroCampo) {
            primeiroCampo = false;
        } else {
            sb.append(',');
        }
        escreveTexto(sb, nome);
        sb.append(':');
    }

    /**
     * Escreve uma string JSON, escapando o que a RFC 8259 exige.
     *
     * <p>Escapa aspas, barra invertida e TODO caractere abaixo de 0x20 — os
     * com forma curta pela forma curta, o resto pela forma unicode. Controle
     * cru dentro de string é JSON inválido, e o servidor recusaria o corpo
     * inteiro com uma mensagem sobre sintaxe, que não apontaria para o
     * caractere que a aplicação passou.
     *
     * <p>NÃO escapa {@code <}, {@code >} nem {@code &}: o consumidor é um
     * servidor, não uma página HTML, e o encoder do lado Go também não os
     * escapa. Duas políticas de escaping fariam o mesmo texto sair diferente
     * conforme a direção.
     */
    public static void escreveTexto(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append('\\').append('"');
                case '\\' -> sb.append('\\').append('\\');
                case '\b' -> sb.append('\\').append('b');
                case '\f' -> sb.append('\\').append('f');
                case '\n' -> sb.append('\\').append('n');
                case '\r' -> sb.append('\\').append('r');
                case '\t' -> sb.append('\\').append('t');
                default -> {
                    if (c < 0x20) {
                        sb.append('\\').append('u');
                        for (int shift = 12; shift >= 0; shift -= 4) {
                            sb.append("0123456789abcdef".charAt((c >> shift) & 0xF));
                        }
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** O texto escapado, entre aspas, pronto para ser um "value" de parâmetro. */
    public static String textoComoJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        escreveTexto(sb, s);
        return sb.toString();
    }

    /**
     * A grafia JSON de um double.
     *
     * <p>NaN e infinitos são RECUSADOS aqui, no cliente, e não mandados para o
     * servidor descobrir: não existe representação deles em JSON, então o corpo
     * sairia inválido e o erro de volta falaria de sintaxe, não do valor que a
     * aplicação passou em setDouble.
     */
    public static String numeroComoJson(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new IllegalArgumentException("JSON nao representa " + d + "; nenhuma coluna pode receber esse valor");
        }
        return Double.toString(d);
    }
}
