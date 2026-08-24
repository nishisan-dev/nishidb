package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonException;
import dev.nishisan.nishidb.jdbc.json.JsonScanner;
import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.Codigos;
import dev.nishisan.nishidb.jdbc.wire.LeitorNdjson;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.List;

/**
 * O cursor sobre o stream NDJSON — o §128 aplicado ao cliente.
 *
 * <p>Uma linha vira TRÊS ÍNDICES POR COLUNA sobre o buffer que o
 * {@link LeitorNdjson} reusa: início, fim e tipo do token. Nenhum
 * {@code String}, {@code Long} ou {@code byte[]} nasce até que um getter seja
 * chamado, e os três arrays são alocados uma vez, no construtor. Um resultado
 * de oito milhões de linhas não vira heap — é isso que
 * {@code percorrerLinhasNaoAlocaProporcionalmenteAoNumeroDeLinhas} cobra, com
 * bytes alocados por thread, que é a medida que não depende de GC.
 *
 * <p><b>O casamento com o schema é POSICIONAL.</b> Chave repetida é legal —
 * {@code SELECT a, a} produz a chave duas vezes —, então um mapa de nome
 * entregaria o mesmo valor nas duas colunas. O ponteiro do schema avança até o
 * nome bater, o que absorve a repetição; e as duas formas de não bater são
 * PROTOCOLO VIOLADO, não linha vazia: uma chave que não casa nenhuma coluna
 * restante, e uma coluna que a linha não trouxe. A segunda merece a explicação:
 * NULL vem como {@code null} EXPLÍCITO no NDJSON — invariante do encoder, com
 * oráculo do lado Go —, então a ausência significa outra coisa, e apresentá-la
 * como nula esconderia a diferença.
 *
 * <p><b>O construtor já lê a primeira linha.</b> Não é otimização: o JDBC exige
 * que {@code getMetaData()} responda antes de {@code next()}, e o schema é a
 * primeira linha do stream. Sem essa leitura antecipada, uma ferramenta que
 * desenha o cabeçalho do grid antes da primeira linha receberia nulo.
 */
public final class StreamCursor implements RowCursor {

    private final LeitorNdjson leitor;
    private final List<Molduras.Coluna> colunas;
    private final String dataset;

    /** Os nomes em UTF-8, para casar a chave da linha sem alocar uma String. */
    private final byte[][] nomes;

    /** Os três índices por coluna, alocados UMA vez. */
    private final int[] ini;
    private final int[] fim;
    private final JsonTipo[] tipo;

    /** Reusado a cada linha: reinicia() não aloca. */
    private final JsonScanner scanner = new JsonScanner(new byte[0], 0, 0);

    private boolean linhaPendente;
    private boolean acabou;
    private SQLWarning aviso;

    public StreamCursor(LeitorNdjson leitor) throws SQLException {
        this.leitor = leitor;
        // A primeira chamada consome a linha de schema e pode já trazer a
        // primeira linha de dados junto; guardá-la é o que evita perdê-la.
        this.linhaPendente = leitor.proxima();
        Molduras.LinhaSchema sch = leitor.schema();
        if (sch == null) {
            // Inalcançável pelo caminho normal — o leitor falha antes se a
            // primeira linha não for o schema —, e a checagem existe para que
            // uma mudança lá não vire NullPointerException aqui.
            throw protocolo("a resposta nao trouxe a linha de schema");
        }
        this.colunas = sch.columns();
        this.dataset = sch.dataset();
        int n = colunas.size();
        this.nomes = new byte[n][];
        for (int i = 0; i < n; i++) {
            nomes[i] = colunas.get(i).name().getBytes(StandardCharsets.UTF_8);
        }
        this.ini = new int[n];
        this.fim = new int[n];
        this.tipo = new JsonTipo[n];
        if (!linhaPendente) {
            desfecho();
        }
    }

    @Override
    public List<Molduras.Coluna> colunas() {
        return colunas;
    }

    @Override
    public String dataset() {
        return dataset;
    }

    @Override
    public boolean proxima() throws SQLException {
        if (linhaPendente) {
            linhaPendente = false;
            escaneia();
            return true;
        }
        if (acabou) {
            return false;
        }
        if (leitor.proxima()) {
            escaneia();
            return true;
        }
        desfecho();
        return false;
    }

    @Override
    public SQLWarning aviso() {
        return aviso;
    }

    @Override
    public void close() throws SQLException {
        leitor.close();
    }

    // ------------------------------------------------------------- valores

    @Override
    public JsonTipo tipo(int coluna) {
        return tipo[coluna];
    }

    @Override
    public String texto(int coluna) throws SQLException {
        if (tipo[coluna] == JsonTipo.NULO) {
            return null;
        }
        byte[] buf = leitor.buffer();
        if (tipo[coluna] != JsonTipo.TEXTO) {
            // Número, booleano e json inline saem VERBATIM: é a grafia que o
            // servidor emitiu, e é o que torna getString lossless.
            return new String(buf, ini[coluna], fim[coluna] - ini[coluna], StandardCharsets.UTF_8);
        }
        try {
            return JsonScanner.texto(buf, ini[coluna], fim[coluna]);
        } catch (JsonException e) {
            throw protocolo("coluna " + nomeDe(coluna) + ": " + e.getMessage());
        }
    }

    @Override
    public long inteiro(int coluna) throws SQLException {
        try {
            return JsonScanner.inteiro(leitor.buffer(), ini[coluna], fim[coluna]);
        } catch (JsonException e) {
            throw new SQLDataException("coluna " + nomeDe(coluna) + ": " + e.getMessage(),
                    SqlStates.VALOR_FORA_DE_FAIXA);
        }
    }

    @Override
    public double numero(int coluna) throws SQLException {
        try {
            return JsonScanner.numero(leitor.buffer(), ini[coluna], fim[coluna]);
        } catch (JsonException e) {
            throw new SQLDataException("coluna " + nomeDe(coluna) + ": " + e.getMessage(),
                    SqlStates.CONVERSAO_INVALIDA);
        }
    }

    @Override
    public boolean booleano(int coluna) throws SQLException {
        byte[] buf = leitor.buffer();
        if (igual(buf, ini[coluna], fim[coluna], VERDADE)) {
            return true;
        }
        if (igual(buf, ini[coluna], fim[coluna], FALSIDADE)) {
            return false;
        }
        throw new SQLDataException("coluna " + nomeDe(coluna) + ": " + texto(coluna)
                + " nao e um booleano", SqlStates.CONVERSAO_INVALIDA);
    }

    @Override
    public boolean integral(int coluna) {
        byte[] buf = leitor.buffer();
        for (int i = ini[coluna]; i < fim[coluna]; i++) {
            byte b = buf[i];
            if (b == '.' || b == 'e' || b == 'E') {
                return false;
            }
        }
        return true;
    }

    private static final byte[] VERDADE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSIDADE = "false".getBytes(StandardCharsets.US_ASCII);

    private String nomeDe(int coluna) {
        return colunas.get(coluna).name();
    }

    // ------------------------------------------------------------ varredura

    /** Preenche os três arrays a partir da linha corrente do leitor. */
    private void escaneia() throws SQLException {
        byte[] buf = leitor.buffer();
        Arrays.fill(tipo, null);
        scanner.reinicia(buf, leitor.inicio(), leitor.fimDaLinha());
        int proxima = 0;
        try {
            scanner.abreObjeto();
            while (scanner.proximoCampo()) {
                int col = casa(buf, proxima, scanner.chaveIni(), scanner.chaveFim());
                if (col < 0) {
                    throw protocolo("a linha traz a chave \"" + chaveLegivel(buf) + "\", que nao casa "
                            + "nenhuma coluna restante do schema");
                }
                ini[col] = scanner.valorIni();
                fim[col] = scanner.valorFim();
                tipo[col] = scanner.tipo();
                proxima = col + 1;
            }
        } catch (JsonException e) {
            throw protocolo("linha de dados ilegivel: " + e.getMessage());
        }
        for (int i = 0; i < tipo.length; i++) {
            if (tipo[i] == null) {
                throw protocolo("a linha nao trouxe a coluna \"" + nomeDe(i)
                        + "\"; NULL viaja como null explicito, entao a ausencia e protocolo violado");
            }
        }
    }

    /**
     * A coluna que a chave nomeia, procurando a partir de {@code de}.
     *
     * <p>Compara BYTES contra o nome já em UTF-8: uma String por chave por
     * linha seria uma alocação por coluna por linha, e o §128 do cliente não
     * sobrevive a isso. O caminho lento — desfazer os escapes da chave — só
     * roda quando a comparação crua falhou em todas, isto é, na fronteira do
     * erro, onde alocar não custa nada.
     */
    private int casa(byte[] buf, int de, int chaveIni, int chaveFim) {
        for (int i = de; i < nomes.length; i++) {
            if (igual(buf, chaveIni, chaveFim, nomes[i])) {
                return i;
            }
        }
        String chave;
        try {
            chave = JsonScanner.texto(buf, chaveIni, chaveFim);
        } catch (JsonException e) {
            return -1;
        }
        for (int i = de; i < nomes.length; i++) {
            if (colunas.get(i).name().equals(chave)) {
                return i;
            }
        }
        return -1;
    }

    private String chaveLegivel(byte[] buf) {
        try {
            return JsonScanner.texto(buf, scanner.chaveIni(), scanner.chaveFim());
        } catch (JsonException e) {
            return "?";
        }
    }

    private static boolean igual(byte[] buf, int ini, int fim, byte[] esperado) {
        if (fim - ini != esperado.length) {
            return false;
        }
        for (int i = 0; i < esperado.length; i++) {
            if (buf[ini + i] != esperado[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cobra o desfecho do stream, uma vez só.
     *
     * <p>MAX_ROWS_EXCEEDED é o único fecho de erro que NÃO vira exceção. O teto
     * é do dataset, e o servidor o publica assim que a varredura o alcança —
     * inclusive quando o resultado tinha exatamente aquele número de linhas e
     * estava completo. Distinguir os dois casos custaria varrer o dataset
     * inteiro, que é o trabalho que o teto existe para evitar; então o driver
     * entrega as linhas e avisa, em vez de transformar um resultado bom em
     * falha.
     */
    private void desfecho() throws SQLException {
        if (acabou) {
            return;
        }
        acabou = true;
        Molduras.LinhaErro erro = leitor.erro();
        if (erro == null) {
            return;
        }
        if (Codigos.MAX_ROWS_EXCEEDED.equals(erro.code())) {
            aviso = new SQLWarning(erro.message() + " [" + erro.rows() + " linha(s) entregue(s)]",
                    SqlStates.estadoDe(Codigos.MAX_ROWS_EXCEEDED));
            return;
        }
        // O formato da mensagem — com a contagem de linhas já entregues — mora
        // no leitor, num lugar só.
        leitor.exigeDesfechoFeliz();
    }

    private static SQLException protocolo(String mensagem) {
        return new SQLRecoverableException(mensagem, SqlStates.CONEXAO_INTERROMPIDA);
    }
}
