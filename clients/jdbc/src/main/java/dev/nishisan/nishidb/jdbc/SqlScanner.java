package dev.nishisan.nishidb.jdbc;

/**
 * Lê o SQL do lado do CLIENTE para responder duas perguntas — e nenhuma outra:
 * qual dataset o {@code FROM} nomeia, e quantos {@code ?} a query tem.
 *
 * <p><b>O dataset precisa sair daqui</b> porque o corpo de {@code POST
 * /v1/query} traz o nome do dataset num campo separado do SQL, e o servidor
 * RECUSA a consulta quando os dois divergem. Sem este extrator, uma ferramenta
 * que lista todos os datasets do servidor só conseguiria consultar aquele que a
 * URL de conexão nomeia — todos os outros morreriam no bind.
 *
 * <p><b>O palpite errado nunca devolve dado de outra tabela.</b> É a garantia
 * que torna este atalho aceitável: quem confere é o servidor, e a divergência
 * vira erro, nunca resultado. Por isso este scanner é um EXTRATOR DE PALPITE e
 * jamais um validador — ele não recusa SQL nenhum, não lança para entrada
 * nenhuma e devolve {@code null} quando não sabe. Recusar aqui trocaria a
 * mensagem do servidor, que tem posição de byte e nome de coluna, por uma do
 * cliente, que não tem nem uma coisa nem outra.
 *
 * <p><b>Não é um parser, e não deve virar um.</b> A gramática de verdade mora
 * em internal/query/parser, e uma segunda gramática aqui divergiria da
 * primeira no dia em que a v2 acrescentasse qualquer coisa. O que este código
 * copia do lexer é só o mínimo para não se enganar: as regras de literal
 * ({@code '...'} com aspa duplicada como escape), de identificador entre aspas
 * ({@code "..."}, mesma regra de escape) e de identificador bare (ASCII, começa
 * com letra ou {@code _}). Comentário não entra na lista porque a linguagem
 * não tem comentário — o lexer não os reconhece em lugar nenhum.
 */
public final class SqlScanner {

    private final String dataset;
    private final int placeholders;

    private SqlScanner(String dataset, int placeholders) {
        this.dataset = dataset;
        this.placeholders = placeholders;
    }

    /**
     * O dataset nomeado no {@code FROM}, com a caixa exata em que foi escrito,
     * ou {@code null} quando a query não o nomeia de forma reconhecível.
     *
     * <p>A caixa é preservada de propósito: identificadores do NishiDB são
     * case-sensitive e o binder resolve nome EXATO. Normalizar aqui faria o
     * driver pedir um dataset que não existe.
     */
    public String dataset() {
        return dataset;
    }

    /** Quantos {@code ?} a query tem fora de literal e de identificador entre aspas. */
    public int placeholders() {
        return placeholders;
    }

    /** Varre o SQL uma vez e responde as duas perguntas. */
    public static SqlScanner de(String sql) {
        String dataset = null;
        int placeholders = 0;
        // Ligado quando a palavra FROM acaba de ser lida: o PRÓXIMO token tem
        // de ser o nome. Se for outra coisa, o palpite é abandonado — e não
        // adiado para um FROM posterior, porque a v1 não tem subquery nem
        // JOIN e um segundo FROM é SQL inválido, não outro escopo.
        boolean esperandoNome = false;
        // vistoFrom separa "ainda não achei o FROM" de "achei e decidi não
        // palpitar". Sem ele, o dataset nulo do nome qualificado faria um FROM
        // posterior no texto voltar a capturar um nome.
        boolean vistoFrom = false;

        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);

            if (espaco(c)) {
                i++;
                continue;
            }

            if (c == '\'') {
                int fim = fimDoTextoEntre(sql, i, '\'');
                if (fim < 0) {
                    // Literal sem fechamento: o resto da entrada está DENTRO
                    // dele. Continuar a varrer contaria como placeholder um
                    // '?' que é texto, e o servidor vai recusar a query de
                    // qualquer forma.
                    break;
                }
                i = fim;
                esperandoNome = false;
                continue;
            }

            if (c == '"') {
                int fim = fimDoTextoEntre(sql, i, '"');
                if (fim < 0) {
                    break;
                }
                if (esperandoNome) {
                    dataset = desescapa(sql.substring(i + 1, fim - 1), '"');
                    esperandoNome = false;
                }
                i = fim;
                continue;
            }

            if (c == '?') {
                placeholders++;
                i++;
                esperandoNome = false;
                continue;
            }

            if (inicioDeIdent(c)) {
                int fim = i + 1;
                while (fim < n && parteDeIdent(sql.charAt(fim))) {
                    fim++;
                }
                String palavra = sql.substring(i, fim);
                if (esperandoNome) {
                    // O dataset NÃO encerra a varredura: os '?' vêm depois
                    // dele, no WHERE. Devolver aqui zeraria a contagem de
                    // placeholders de toda query com parâmetro — e o
                    // PreparedStatement recusaria o setString do índice 1.
                    vistoFrom = true;
                    esperandoNome = false;
                    // Nome QUALIFICADO (`_meta.fields`) abandona o palpite. O
                    // ponto não é parte de identificador aqui, então capturar
                    // `palavra` entregaria "_meta" — um dataset que não existe,
                    // e o servidor responderia 404 na primeira consulta ao
                    // metadado. Devolver null faz o driver usar o dataset da
                    // conexão, que é exatamente o que a tabela descreve.
                    if (!seguidoDePonto(sql, fim, n)) {
                        dataset = palavra;
                    }
                    i = fim;
                    continue;
                }
                // Comparação ASCII, e não equalsIgnoreCase: o folding do Java
                // é Unicode, e o do lexer do servidor é ASCII puro (o godoc de
                // keywordType registra o caso do sinal de Kelvin, que dobra
                // para 'k'). Duas regras de caixa fariam o cliente reconhecer
                // uma cláusula que o servidor não reconhece.
                if (!vistoFrom && igualSemCaixaASCII(palavra, "FROM")) {
                    esperandoNome = true;
                }
                i = fim;
                continue;
            }

            // Operador, pontuação, número, byte estranho: nada disso muda as
            // duas respostas. Um byte só de cada vez, porque o único risco
            // seria pular por cima de uma aspa.
            i++;
            esperandoNome = false;
        }
        return new SqlScanner(dataset, placeholders);
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

    /** Desfaz a aspa duplicada, a única forma de escape que o lexer conhece. */
    private static String desescapa(String corpo, char aspa) {
        int dupla = corpo.indexOf("" + aspa + aspa);
        if (dupla < 0) {
            return corpo;
        }
        StringBuilder sb = new StringBuilder(corpo.length());
        for (int i = 0; i < corpo.length(); i++) {
            char c = corpo.charAt(i);
            sb.append(c);
            if (c == aspa) {
                i++; // a segunda aspa do par
            }
        }
        return sb.toString();
    }

    private static boolean igualSemCaixaASCII(String a, String b) {
        if (a.length() != b.length()) {
            return false;
        }
        for (int i = 0; i < a.length(); i++) {
            if (minusculaASCII(a.charAt(i)) != minusculaASCII(b.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static char minusculaASCII(char c) {
        return c >= 'A' && c <= 'Z' ? (char) (c + 32) : c;
    }

    /** Os cinco brancos do lexer, mais o vertical tab e o form feed. */
    private static boolean espaco(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 0x0B || c == '\f';
    }

    private static boolean inicioDeIdent(char c) {
        return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean parteDeIdent(char c) {
        return inicioDeIdent(c) || (c >= '0' && c <= '9');
    }

    /** Se o próximo caractere não-branco a partir de {@code de} é um ponto. */
    private static boolean seguidoDePonto(String sql, int de, int n) {
        int j = de;
        // espaco(), e não Character.isWhitespace: o folding e a classificação
        // do Java são Unicode, e o lexer do servidor é ASCII puro. Duas regras
        // de branco fariam o cliente enxergar uma fronteira que o servidor não
        // enxerga — a mesma razão pela qual a comparação de FROM é ASCII.
        while (j < n && espaco(sql.charAt(j))) {
            j++;
        }
        return j < n && sql.charAt(j) == '.';
    }
}
