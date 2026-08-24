package dev.nishisan.nishidb.jdbc.json;

import java.nio.charset.StandardCharsets;

/**
 * Percorre JSON sobre um {@code byte[]} entregando SPANS, sem alocar.
 *
 * <p>É o desenho que sustenta o §128 no cliente: "nada do resultado é
 * materializado". Uma linha de resultado vira três índices por coluna — início,
 * fim e tipo — e nenhum {@code String}, {@code Long} ou {@code byte[]} nasce
 * até que um getter seja chamado. Um ResultSet de 8 milhões de linhas não vira
 * heap.
 *
 * <p>Trabalha em BYTES, e não em {@code char}. Além de evitar a decodificação
 * de uma linha inteira que talvez ninguém leia, é o que permite quebrar o
 * stream por {@code '\n'} com segurança: o encoder do servidor escapa todo byte
 * abaixo de 0x20 dentro de string e troca controles por espaço no json inline,
 * então nenhum LF cru sobrevive dentro de um valor.
 *
 * <p>A instância é REUTILIZÁVEL por {@link #reinicia}: o cursor de linhas troca
 * o buffer a cada row e não realoca nada.
 */
public final class JsonScanner {

    /**
     * Teto de aninhamento. Existe para que um documento hostil vire
     * JsonException e nunca StackOverflowError — que é um Error, não é
     * capturado por quem trata SQLException, e derruba a thread da aplicação.
     * 200 é folgado para dado real e barato de sustentar.
     */
    private static final int PROFUNDIDADE_MAXIMA = 200;

    private byte[] buf;
    private int fim;
    private int pos;

    private int chaveIni;
    private int chaveFim;
    private int valorIni;
    private int valorFim;
    private JsonTipo tipo;
    private boolean primeiroCampo;

    // Saída de escaneiaTexto. São CAMPOS, e não um int[] devolvido, porque
    // escaneiaTexto roda uma vez por chave e uma por valor de texto: um array
    // por chamada seriam duas alocações por coluna por row, e o §128 aplicado
    // ao cliente não sobrevive a isso.
    private int textoIni;
    private int textoFim;

    public JsonScanner(byte[] buf, int ini, int fim) {
        reinicia(buf, ini, fim);
    }

    /** Reaproveita a instância para outro buffer, sem alocar. */
    public void reinicia(byte[] buf, int ini, int fim) {
        this.buf = buf;
        this.fim = fim;
        this.pos = ini;
        this.primeiroCampo = true;
    }

    public JsonTipo tipo() {
        return tipo;
    }

    /** Início do CONTEÚDO da chave, sem as aspas. */
    public int chaveIni() {
        return chaveIni;
    }

    public int chaveFim() {
        return chaveFim;
    }

    /**
     * Início do valor. Para {@link JsonTipo#TEXTO} aponta para o conteúdo, sem
     * as aspas e ainda escapado; para os demais, para o token inteiro.
     */
    public int valorIni() {
        return valorIni;
    }

    public int valorFim() {
        return valorFim;
    }

    /**
     * Consome o {@code '{'} do objeto de topo.
     *
     * <p>Uma linha de dados cujo valor de topo não seja objeto é protocolo
     * violado, e falha aqui — em vez de virar uma row vazia que a aplicação
     * leria como "não havia dado".
     */
    public void abreObjeto() throws JsonException {
        pulaEspacos();
        exigeByte('{', "esperado '{': a linha de dados tem de ser um objeto JSON");
        primeiroCampo = true;
    }

    /**
     * Avança para o próximo par chave/valor. {@code false} no {@code '}'}.
     */
    public boolean proximoCampo() throws JsonException {
        pulaEspacos();
        if (pos >= fim) {
            throw erro(pos, "o objeto terminou sem '}'");
        }
        if (buf[pos] == '}') {
            pos++;
            return false;
        }
        if (primeiroCampo) {
            primeiroCampo = false;
        } else {
            exigeByte(',', "esperado ',' ou '}' entre os campos");
            pulaEspacos();
            // Vírgula sobrando: `{"a":1,}` é inválido, e aceitá-la deixaria o
            // driver mais tolerante que o servidor sobre a forma do PRÓPRIO
            // protocolo.
            if (pos < fim && buf[pos] == '}') {
                throw erro(pos, "vírgula sobrando antes de '}'");
            }
        }

        pulaEspacos();
        if (pos >= fim || buf[pos] != '"') {
            throw erro(pos, "esperado nome de campo entre aspas");
        }
        escaneiaTexto();
        chaveIni = textoIni;
        chaveFim = textoFim;

        pulaEspacos();
        exigeByte(':', "esperado ':' depois do nome do campo");
        pulaEspacos();
        escaneiaValor(0);
        return true;
    }

    /**
     * Valida e delimita UM valor a partir de {@code de}, sem exigir que ele
     * esteja dentro de um objeto.
     *
     * <p>Depois da chamada, {@link #tipo()}, {@link #valorIni()} e
     * {@link #valorFim()} descrevem o valor, e o retorno é a posição logo
     * depois dele.
     *
     * <p>Existe para que o parser de documentos (JsonParser) percorra a
     * estrutura sem reimplementar a gramática. O que ele NÃO poderia duplicar
     * sem risco é a leitura de string e de número — escapes, controles crus,
     * zero à esquerda, expoente — e é exatamente isso que esta porta
     * compartilha.
     */
    public int valorEm(int de) throws JsonException {
        pos = de;
        pulaEspacos();
        escaneiaValor(0);
        return pos;
    }

    /**
     * Exige que não haja nada além de espaços depois do valor de topo.
     *
     * <p>Sem esta checagem, {@code {"a":1}{"b":2}} passaria como um objeto só e
     * a segunda metade sumiria em silêncio.
     */
    public void exigeFimDaEntrada() throws JsonException {
        pulaEspacos();
        if (pos < fim) {
            throw erro(pos, "conteúdo depois do fim do valor de topo");
        }
    }

    // ------------------------------------------------------------ scanner

    private void escaneiaValor(int profundidade) throws JsonException {
        if (profundidade > PROFUNDIDADE_MAXIMA) {
            throw erro(pos, "aninhamento acima de " + PROFUNDIDADE_MAXIMA + " níveis");
        }
        if (pos >= fim) {
            throw erro(pos, "esperado um valor");
        }
        byte b = buf[pos];
        switch (b) {
            case '"' -> {
                escaneiaTexto();
                valorIni = textoIni;
                valorFim = textoFim;
                tipo = JsonTipo.TEXTO;
            }
            case '{', '[' -> {
                // O início vai para uma LOCAL, e não direto para valorIni: a
                // varredura do composto chama escaneiaValor recursivamente
                // para cada elemento de dentro, e cada uma dessas chamadas
                // sobrescreve valorIni. Sem a local, o span do documento
                // começaria no último valor aninhado — e a coluna json sairia
                // truncada, com a linha inteira ainda parecendo bem formada.
                int ini = pos;
                escaneiaComposto(profundidade);
                valorIni = ini;
                valorFim = pos;
                tipo = JsonTipo.BRUTO;
            }
            case 't' -> {
                valorIni = pos;
                exigeLiteral("true");
                valorFim = pos;
                tipo = JsonTipo.BOOLEANO;
            }
            case 'f' -> {
                valorIni = pos;
                exigeLiteral("false");
                valorFim = pos;
                tipo = JsonTipo.BOOLEANO;
            }
            case 'n' -> {
                valorIni = pos;
                exigeLiteral("null");
                valorFim = pos;
                tipo = JsonTipo.NULO;
            }
            default -> {
                valorIni = pos;
                escaneiaNumero();
                valorFim = pos;
                tipo = JsonTipo.NUMERO;
            }
        }
    }

    /** Percorre objeto ou array aninhado, sem interpretar o conteúdo. */
    private void escaneiaComposto(int profundidade) throws JsonException {
        boolean objeto = buf[pos] == '{';
        pos++;
        pulaEspacos();
        if (pos < fim && buf[pos] == (objeto ? '}' : ']')) {
            pos++;
            return;
        }
        while (true) {
            pulaEspacos();
            if (objeto) {
                if (pos >= fim || buf[pos] != '"') {
                    throw erro(pos, "esperado nome de campo entre aspas");
                }
                escaneiaTexto();
                pulaEspacos();
                exigeByte(':', "esperado ':' depois do nome do campo");
                pulaEspacos();
            }
            escaneiaValor(profundidade + 1);
            pulaEspacos();
            if (pos >= fim) {
                throw erro(pos, objeto ? "o objeto terminou sem '}'" : "o array terminou sem ']'");
            }
            if (buf[pos] == (objeto ? '}' : ']')) {
                pos++;
                return;
            }
            exigeByte(',', objeto ? "esperado ',' ou '}'" : "esperado ',' ou ']'");
            pulaEspacos();
            if (pos < fim && buf[pos] == (objeto ? '}' : ']')) {
                throw erro(pos, "vírgula sobrando");
            }
        }
    }

    /** Consome uma string, deixando o span do conteúdo em textoIni/textoFim. */
    private void escaneiaTexto() throws JsonException {
        pos++; // a aspa de abertura
        int ini = pos;
        while (pos < fim) {
            byte b = buf[pos];
            if (b == '"') {
                textoIni = ini;
                textoFim = pos;
                pos++;
                return;
            }
            if (b == '\\') {
                pos++;
                if (pos >= fim) {
                    throw erro(pos, "escape truncado no fim da entrada");
                }
                byte e = buf[pos];
                if (e == 'u') {
                    if (pos + 4 >= fim) {
                        throw erro(pos, "escape unicode truncado");
                    }
                    for (int i = 1; i <= 4; i++) {
                        if (hex(buf[pos + i]) < 0) {
                            throw erro(pos + i, "escape unicode com dígito não-hexadecimal");
                        }
                    }
                    pos += 4;
                } else if ("\"\\/bfnrt".indexOf((char) e) < 0) {
                    throw erro(pos, "escape desconhecido \\" + (char) e);
                }
                pos++;
                continue;
            }
            // A RFC proíbe controle CRU dentro de string, e o encoder do
            // servidor os escapa — um controle aqui significa resposta que não
            // veio de onde deveria, ou veio corrompida.
            if ((b & 0xFF) < 0x20) {
                throw erro(pos, "byte de controle cru dentro de string");
            }
            pos++;
        }
        throw erro(pos, "string sem aspa de fechamento");
    }

    /**
     * Valida a gramática de número da RFC 8259 e consome o token.
     *
     * <p>Não converte nada: a conversão é dos métodos {@link #inteiro} e
     * {@link #numero}, que trabalham sobre o span já validado.
     */
    private void escaneiaNumero() throws JsonException {
        int ini = pos;
        if (pos < fim && buf[pos] == '-') {
            pos++;
        }
        // A RFC não tem '+' à frente, e não tem zero à esquerda: 007 muda de
        // valor em quem interpreta como octal.
        int digitosInteiros = consomeDigitos();
        if (digitosInteiros == 0) {
            throw erro(ini, "número mal formado");
        }
        if (buf[ini] == '-' ? (buf[ini + 1] == '0' && digitosInteiros > 1) : (buf[ini] == '0' && digitosInteiros > 1)) {
            throw erro(ini, "número com zero à esquerda");
        }
        if (pos < fim && buf[pos] == '.') {
            pos++;
            if (consomeDigitos() == 0) {
                throw erro(pos, "número com '.' sem dígitos depois");
            }
        }
        if (pos < fim && (buf[pos] == 'e' || buf[pos] == 'E')) {
            pos++;
            if (pos < fim && (buf[pos] == '+' || buf[pos] == '-')) {
                pos++;
            }
            if (consomeDigitos() == 0) {
                throw erro(pos, "expoente sem dígitos");
            }
        }
    }

    private int consomeDigitos() {
        int n = 0;
        while (pos < fim && buf[pos] >= '0' && buf[pos] <= '9') {
            pos++;
            n++;
        }
        return n;
    }

    private void exigeLiteral(String literal) throws JsonException {
        if (pos + literal.length() > fim) {
            throw erro(pos, "esperado " + literal);
        }
        for (int i = 0; i < literal.length(); i++) {
            if (buf[pos + i] != (byte) literal.charAt(i)) {
                throw erro(pos, "esperado " + literal);
            }
        }
        pos += literal.length();
    }

    private void exigeByte(char esperado, String mensagem) throws JsonException {
        if (pos >= fim || buf[pos] != (byte) esperado) {
            throw erro(pos, mensagem);
        }
        pos++;
    }

    private void pulaEspacos() {
        // Só os quatro da RFC. Comentário não é espaço: `// x` cai no scanner
        // de valor e falha, que é o certo.
        while (pos < fim) {
            byte b = buf[pos];
            if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                pos++;
            } else {
                return;
            }
        }
    }

    private JsonException erro(int offset, String mensagem) {
        return new JsonException(offset, mensagem);
    }

    // ---------------------------------------------------------- conversões

    /**
     * Converte o span de uma string JSON em texto, desfazendo os escapes.
     *
     * <p>Caminho comum sem escape nenhum: um único {@code new String} sobre os
     * bytes, sem varredura extra.
     */
    public static String texto(byte[] buf, int ini, int fim) throws JsonException {
        int barra = -1;
        for (int i = ini; i < fim; i++) {
            if (buf[i] == '\\') {
                barra = i;
                break;
            }
        }
        if (barra < 0) {
            return new String(buf, ini, fim - ini, StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder(fim - ini);
        sb.append(new String(buf, ini, barra - ini, StandardCharsets.UTF_8));
        int i = barra;
        int bloco = -1;
        while (i < fim) {
            byte b = buf[i];
            if (b != '\\') {
                if (bloco < 0) {
                    bloco = i;
                }
                i++;
                continue;
            }
            if (bloco >= 0) {
                sb.append(new String(buf, bloco, i - bloco, StandardCharsets.UTF_8));
                bloco = -1;
            }
            i++;
            if (i >= fim) {
                throw new JsonException(i, "escape truncado");
            }
            byte e = buf[i];
            switch (e) {
                case '"' -> sb.append('"');
                case '\\' -> sb.append('\\');
                case '/' -> sb.append('/');
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (i + 4 >= fim) {
                        throw new JsonException(i, "escape unicode truncado");
                    }
                    int cp = 0;
                    for (int k = 1; k <= 4; k++) {
                        int d = hex(buf[i + k]);
                        if (d < 0) {
                            throw new JsonException(i + k, "escape unicode com dígito não-hexadecimal");
                        }
                        cp = (cp << 4) | d;
                    }
                    // Appendado como char: um par de surrogates escapado vira
                    // duas sequências de escape consecutivas, e appendar cada
                    // metade como char reconstrói o code point sem trabalho
                    // extra. (A grafia do escape não aparece neste comentário
                    // de propósito: o pré-processador do Java a interpretaria
                    // aqui dentro, antes do compilador.)
                    sb.append((char) cp);
                    i += 4;
                }
                default -> throw new JsonException(i, "escape desconhecido \\" + (char) e);
            }
            i++;
        }
        if (bloco >= 0) {
            sb.append(new String(buf, bloco, fim - bloco, StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    /**
     * Converte o span de um número JSON em {@code long}, EXATO.
     *
     * <p>Parseia os dígitos ASCII com detecção de overflow, e nunca passa por
     * {@code double}. É o espelho do lado Go, onde o campo "value" do parâmetro
     * é {@code json.RawMessage} e não {@code any} pela mesma razão: por double,
     * {@code 9223372036854775807} chega como ...5808 — errado e sem erro.
     */
    public static long inteiro(byte[] buf, int ini, int fim) throws JsonException {
        if (ini >= fim) {
            throw new JsonException(ini, "número vazio");
        }
        int i = ini;
        boolean negativo = buf[i] == '-';
        if (negativo) {
            i++;
        }
        if (i >= fim) {
            throw new JsonException(ini, "número mal formado");
        }
        // Acumula em NEGATIVO: a faixa do long é assimétrica, e acumular em
        // positivo não representaria Long.MIN_VALUE.
        long acc = 0;
        for (; i < fim; i++) {
            byte b = buf[i];
            if (b < '0' || b > '9') {
                throw new JsonException(i, "valor não é um inteiro: " + (char) b);
            }
            int d = b - '0';
            if (acc < (Long.MIN_VALUE + d) / 10) {
                throw new JsonException(ini, "inteiro fora do intervalo de int64");
            }
            acc = acc * 10 - d;
        }
        if (!negativo) {
            if (acc == Long.MIN_VALUE) {
                throw new JsonException(ini, "inteiro fora do intervalo de int64");
            }
            return -acc;
        }
        return acc;
    }

    /**
     * Converte o span de um número JSON em {@code double}.
     *
     * <p>A divisão de trabalho é: NÓS VALIDAMOS, O JDK ARREDONDA. A gramática
     * é conferida aqui contra a RFC 8259 e a conversão é do
     * {@code Double.parseDouble}, que é corretamente arredondado por IEEE-754 —
     * escrever o arredondamento à mão seria reimplementar Grisu, errado.
     *
     * <p>A validação prévia é OBRIGATÓRIA, e não zelo: o parseDouble aceita
     * {@code NaN}, {@code Infinity}, {@code 0x1p3} e os sufixos {@code d}/{@code f},
     * nenhum dos quais existe em JSON. Sem ela, um proxy que reescrevesse o
     * corpo injetaria valores que o driver entregaria como número legítimo.
     */
    public static double numero(byte[] buf, int ini, int fim) throws JsonException {
        JsonScanner s = new JsonScanner(buf, ini, fim);
        s.escaneiaNumero();
        if (s.pos != fim) {
            throw new JsonException(s.pos, "conteúdo depois do número");
        }
        return Double.parseDouble(new String(buf, ini, fim - ini, StandardCharsets.US_ASCII));
    }

    private static int hex(byte b) {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        if (b >= 'a' && b <= 'f') {
            return b - 'a' + 10;
        }
        if (b >= 'A' && b <= 'F') {
            return b - 'A' + 10;
        }
        return -1;
    }
}
