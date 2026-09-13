package dev.nishisan.nishidb.jdbc;

/**
 * Lê o SQL do lado do CLIENTE para responder UMA pergunta — e nenhuma outra:
 * quantos {@code ?} a query tem. É o que o {@link NishiPreparedStatement}
 * precisa para validar o índice de cada {@code setXxx} antes de ir ao servidor.
 *
 * <p>O scanner já respondeu também "qual dataset o {@code FROM} nomeia", porque
 * o corpo de {@code POST /v1/query} carregava o dataset. Com a conexão por
 * schema o corpo leva o schema da URL e o servidor resolve o {@code FROM}
 * dentro dele — não há mais palpite a fazer, e manter o extrator seria uma
 * segunda gramática sem consumidor.
 *
 * <p><b>Não é um parser, e não deve virar um.</b> A gramática de verdade mora
 * em internal/query/parser. O que este código copia do lexer é só o mínimo para
 * não contar errado: as regras de literal ({@code '...'} com aspa duplicada
 * como escape) e de identificador entre aspas ({@code "..."}, mesma regra de
 * escape) — um {@code ?} dentro de qualquer um dos dois é texto, não
 * parâmetro. Comentário não entra na lista porque a linguagem não tem
 * comentário. Ele não recusa SQL nenhum e não lança para entrada nenhuma:
 * recusar aqui trocaria a mensagem do servidor, que tem posição de byte, por
 * uma do cliente, que não tem.
 */
public final class SqlScanner {

    private final int placeholders;

    private SqlScanner(int placeholders) {
        this.placeholders = placeholders;
    }

    /** Quantos {@code ?} a query tem fora de literal e de identificador entre aspas. */
    public int placeholders() {
        return placeholders;
    }

    /** Varre o SQL uma vez. */
    public static SqlScanner de(String sql) {
        int placeholders = 0;
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                int fim = fimDoTextoEntre(sql, i, c);
                if (fim < 0) {
                    // Texto sem fechamento: o resto da entrada está DENTRO
                    // dele. Continuar a varrer contaria como placeholder um
                    // '?' que é texto, e o servidor vai recusar a query de
                    // qualquer forma.
                    break;
                }
                i = fim;
                continue;
            }
            if (c == '?') {
                placeholders++;
            }
            i++;
        }
        return new SqlScanner(placeholders);
    }

    /**
     * O índice logo APÓS a aspa de fechamento do texto que começa em
     * {@code ini}, ou -1 quando não há fechamento.
     *
     * <p>A regra de escape é a do lexer do servidor: a aspa duplicada é um
     * escape, não um fechamento seguido de abertura. Sem isso, {@code 'a''?''b'}
     * teria o {@code ?} lido como estando FORA do literal, e ele viraria um
     * placeholder que o servidor não tem.
     */
    private static int fimDoTextoEntre(String sql, int ini, char aspa) {
        int n = sql.length();
        int i = ini + 1;
        while (i < n) {
            if (sql.charAt(i) != aspa) {
                i++;
                continue;
            }
            if (i + 1 < n && sql.charAt(i + 1) == aspa) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        return -1;
    }
}
