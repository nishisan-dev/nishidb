package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonWriter;
import dev.nishisan.nishidb.jdbc.wire.LeitorNdjson;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * O {@code Statement} do driver: monta o corpo de {@code POST /v1/query} e
 * devolve o stream como {@link NishiResultSet}.
 *
 * <p><b>O dataset sai do FROM da própria consulta</b>, com o da URL como
 * fallback. O corpo do protocolo traz o nome do dataset num campo separado do
 * SQL e o servidor recusa quando os dois divergem; sem a extração, uma
 * ferramenta que lista os datasets do servidor conseguiria abrir todos e
 * consultar só o que a URL nomeia.
 *
 * <p><b>Três relógios distintos, e confundi-los mata stream legítimo.</b> O
 * {@code connectTimeoutMs} da conexão vale só para o handshake. O
 * {@code HttpRequest.timeout()} NÃO é setado, e é deliberado: ele estoura mesmo
 * com bytes fluindo, o que mataria toda consulta longa que estivesse indo bem.
 * O prazo da CONSULTA é {@link #setQueryTimeout}, que viaja no corpo e é o
 * servidor quem honra — do lado onde a varredura acontece.
 */
public class NishiStatement implements Statement {

    private final NishiConnection conexao;

    private NishiResultSet resultado;
    private boolean fechado;
    private boolean fecharAoCompletar;
    /**
     * Ligado enquanto o resultado ANTERIOR é fechado por uma execução nova.
     *
     * <p>Sem ele, um Statement com closeOnCompletion se fecharia sozinho ao
     * executar a segunda consulta — porque trocar de resultado passa pelo mesmo
     * close() que o usuário chamaria.
     */
    private boolean trocandoResultado;
    private long maxRows;
    private int queryTimeoutSegundos;
    private boolean poolable = true;

    NishiStatement(NishiConnection conexao) {
        this.conexao = conexao;
    }

    // ------------------------------------------------------------- execução

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executa(sql, null);
    }

    /**
     * Executa e monta o ResultSet, fechando o anterior.
     *
     * @param parametros o array JSON de parâmetros já pronto, ou null
     */
    final ResultSet executa(String sql, String parametros) throws SQLException {
        exigeAberto();
        if (sql == null) {
            // Sem esta guarda o scanner de SQL estoura com NullPointerException,
            // que NÃO é SQLException e atravessa o catch da aplicação.
            throw new SQLException("a consulta é nula", SqlStates.CONVERSAO_INVALIDA);
        }
        fechaResultadoAnterior();

        // O schema da conexão vai em TODA consulta, e o servidor resolve o
        // FROM dentro dele: o driver não palpita dataset nenhum a partir do SQL.
        JsonWriter corpo = new JsonWriter().texto("schema", conexao.schema()).texto("sql", sql);
        if (parametros != null) {
            corpo.bruto("parameters", parametros);
        }
        Integer teto = maxRowsParaOFio();
        if (teto != null) {
            corpo.inteiro("maxRows", teto);
        }
        Long prazo = timeoutParaOFio();
        if (prazo != null) {
            corpo.inteiro("timeoutMs", prazo);
        }

        InputStream stream = conexao.transporte().consulta(corpo.fecha());
        LeitorNdjson leitor = new LeitorNdjson(stream);
        StreamCursor cursor;
        try {
            cursor = new StreamCursor(leitor);
        } catch (SQLException e) {
            // O stream tem de ser fechado mesmo quando o schema não chega:
            // sem isto, uma consulta que falha na primeira linha vaza a
            // conexão, e o vazamento só aparece dezenas de consultas depois.
            try {
                leitor.close();
            } catch (SQLException fechando) {
                e.addSuppressed(fechando);
            }
            throw e;
        }
        resultado = new NishiResultSet(cursor, this);
        return resultado;
    }

    /**
     * Abaixo do teto do dataset o número VAI; igual, acima ou zero, ele é
     * OMITIDO.
     *
     * <p>O servidor RECUSA um maxRows acima do teto do dataset em vez de
     * apará-lo, e a ferramenta manda 200 por hábito. Omitir quando o pedido é
     * maior que o teto responde exatamente à pergunta que foi feita — o teto do
     * dataset já garante "no máximo n" — sem transformar um default de
     * ferramenta em erro na cara do usuário.
     */
    private Integer maxRowsParaOFio() {
        int teto = conexao.maxRowsDoDataset();
        if (maxRows <= 0 || (teto > 0 && maxRows >= teto)) {
            return null;
        }
        return (int) maxRows;
    }

    /**
     * Clampado contra o teto do dataset, e não recusado.
     *
     * <p>O servidor recusa acima do teto, e o teto é o máximo que existe: pedir
     * mais e receber erro seria pior para quem chamou do que receber o máximo
     * disponível.
     */
    private Long timeoutParaOFio() {
        if (queryTimeoutSegundos <= 0) {
            return null;
        }
        long ms = queryTimeoutSegundos * 1000L;
        long teto = conexao.timeoutMsDoDataset();
        return teto > 0 ? Math.min(ms, teto) : ms;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public boolean execute(String sql, int chavesGeradas) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] colunas) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] colunas) throws SQLException {
        return execute(sql);
    }

    /**
     * Não existe: os dados do NishiDB entram por ingestão, não por SQL.
     *
     * <p>Devolver 0 seria pior que lançar — a aplicação concluiria que o
     * comando rodou e não afetou nada.
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public int executeUpdate(String sql, int chavesGeradas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public int executeUpdate(String sql, int[] colunas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public int executeUpdate(String sql, String[] colunas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    /**
     * Sobrescritos, e não herdados do default da interface.
     *
     * <p>O default de {@code executeLargeUpdate} lança
     * {@code UnsupportedOperationException} — que NÃO é SQLException e
     * atravessa o {@code catch} da ferramenta, virando stack trace em vez de
     * mensagem.
     */
    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public long executeLargeUpdate(String sql, int chavesGeradas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] colunas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] colunas) throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        exigeAberto();
        return -1L;
    }

    private static final String SEM_ESCRITA =
            "o NishiDB não aceita escrita por SQL: os dados entram por ingestão";

    // ---------------------------------------------------------- resultados

    @Override
    public ResultSet getResultSet() throws SQLException {
        exigeAberto();
        return resultado;
    }

    /** -1: não houve comando de atualização, porque não existe um. */
    @Override
    public int getUpdateCount() throws SQLException {
        exigeAberto();
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        exigeAberto();
        fechaResultadoAnterior();
        return false;
    }

    @Override
    public boolean getMoreResults(int oQueFazerComOAtual) throws SQLException {
        return getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw naoSuportado("o NishiDB não gera chave: a chave da linha vem do evento ingerido");
    }

    // ------------------------------------------------------------- limites

    @Override
    public int getMaxRows() throws SQLException {
        exigeAberto();
        return maxRows > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxRows;
    }

    @Override
    public void setMaxRows(int linhas) throws SQLException {
        setLargeMaxRows(linhas);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        exigeAberto();
        return maxRows;
    }

    @Override
    public void setLargeMaxRows(long linhas) throws SQLException {
        exigeAberto();
        if (linhas < 0) {
            throw new SQLException("maxRows negativo: " + linhas, SqlStates.VALOR_FORA_DE_FAIXA);
        }
        maxRows = linhas;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        exigeAberto();
        return queryTimeoutSegundos;
    }

    @Override
    public void setQueryTimeout(int segundos) throws SQLException {
        exigeAberto();
        if (segundos < 0) {
            throw new SQLException("queryTimeout negativo: " + segundos, SqlStates.VALOR_FORA_DE_FAIXA);
        }
        queryTimeoutSegundos = segundos;
    }

    /**
     * DICA pelo contrato, e aceita em silêncio.
     *
     * <p>É a mesma exceção legítima do ResultSet: o stream entrega as linhas no
     * ritmo em que o servidor as escreve, não há lote a dimensionar, e lançar
     * quebraria toda ferramenta que chama isto por hábito antes de executar.
     */
    @Override
    public void setFetchSize(int linhas) throws SQLException {
        exigeAberto();
        if (linhas < 0) {
            throw new SQLException("fetchSize negativo: " + linhas, SqlStates.VALOR_FORA_DE_FAIXA);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        exigeAberto();
        return 0;
    }

    @Override
    public void setFetchDirection(int direcao) throws SQLException {
        exigeAberto();
        if (direcao != ResultSet.FETCH_FORWARD) {
            throw naoSuportado("o cursor deste driver é só para frente");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        exigeAberto();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getResultSetType() throws SQLException {
        exigeAberto();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        exigeAberto();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        exigeAberto();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        exigeAberto();
        return 0;
    }

    /**
     * Não há truncamento de campo a configurar: o valor vem inteiro do fio, e
     * cortá-lo aqui entregaria à aplicação um dado que parece completo.
     */
    @Override
    public void setMaxFieldSize(int bytes) throws SQLException {
        throw naoSuportado("este driver não trunca valor: um valor cortado pareceria completo");
    }

    /**
     * Aceito em silêncio, e o SQL vai VERBATIM.
     *
     * <p>Este driver não traduz a sintaxe de escape do JDBC ({@code {fn …}}), e
     * desligar o processamento não muda nada — porque não há processamento. O
     * caso em que isso importa falha ALTO: quem escrever {@code {fn now()}}
     * recebe o erro de sintaxe do servidor, com a posição do byte.
     */
    @Override
    public void setEscapeProcessing(boolean ligado) throws SQLException {
        exigeAberto();
    }

    /**
     * Cancelar exigiria derrubar o cliente HTTP, que é da conexão inteira.
     *
     * <p>O caminho certo é {@link #setQueryTimeout}, que o servidor honra do
     * lado onde a varredura acontece — e cancelar lá libera o recurso de
     * verdade, em vez de só largar o socket aqui.
     */
    @Override
    public void cancel() throws SQLException {
        throw naoSuportado("use setQueryTimeout: o cancelamento acontece no servidor, "
                + "onde a varredura está");
    }

    @Override
    public void setCursorName(String nome) throws SQLException {
        throw naoSuportado("este driver não nomeia cursores");
    }

    // --------------------------------------------------------------- lotes

    @Override
    public void addBatch(String sql) throws SQLException {
        throw naoSuportado(SEM_ESCRITA + "; e um lote de SELECT não é o que o batch do JDBC faz");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw naoSuportado(SEM_ESCRITA);
    }

    // ---------------------------------------------------------------- ciclo

    @Override
    public Connection getConnection() throws SQLException {
        exigeAberto();
        return conexao;
    }

    @Override
    public void close() throws SQLException {
        if (fechado) {
            return;
        }
        fechado = true;
        conexao.esqueceStatement(this);
        NishiResultSet aberto = resultado;
        resultado = null;
        if (aberto != null) {
            aberto.close();
        }
    }

    @Override
    public boolean isClosed() {
        return fechado;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        exigeAberto();
        fecharAoCompletar = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        exigeAberto();
        return fecharAoCompletar;
    }

    /** Chamado pelo ResultSet ao fechar, para honrar o closeOnCompletion. */
    void resultadoFechado(NishiResultSet rs) throws SQLException {
        if (resultado == rs) {
            resultado = null;
        }
        if (fecharAoCompletar && !trocandoResultado) {
            close();
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        exigeAberto();
        return poolable;
    }

    @Override
    public void setPoolable(boolean valor) throws SQLException {
        exigeAberto();
        poolable = valor;
    }

    /**
     * Nulo: o único aviso que este driver produz nasce no FIM do stream — o
     * teto de linhas do dataset —, e o Statement já devolveu quando ele
     * aparece. Ele vive no ResultSet, que é quem ainda está na mão de quem lê.
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        exigeAberto();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        exigeAberto();
    }

    // ------------------------------------------------------- citação de nome
    //
    // Os quatro abaixo têm implementação DEFAULT na interface, e ela estoura
    // com NullPointerException diante de um argumento nulo — que não é
    // SQLException e atravessa o catch da aplicação. Sobrescrevê-los também
    // troca as regras do SQL:2016 pelas do lexer DESTE servidor, que são as
    // que valem quando o texto voltar para cá.

    /** Literal entre aspas simples, com a aspa duplicada como escape — a regra do lexer. */
    @Override
    public String enquoteLiteral(String valor) throws SQLException {
        if (valor == null) {
            throw new SQLException("nao ha literal a citar: o valor e nulo",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        return "'" + valor.replace("'", "''") + "'";
    }

    @Override
    public String enquoteNCharLiteral(String valor) throws SQLException {
        // Nao existe prefixo N no dialeto: toda string ja e UTF-8.
        return enquoteLiteral(valor);
    }

    /**
     * SEMPRE cita, mesmo quando {@code alwaysQuote} e falso.
     *
     * <p>Citar e sempre seguro neste dialeto — um identificador entre aspas
     * nunca e keyword —, e o driver NAO carrega a lista de palavras reservadas
     * do servidor. Devolver um nome sem aspas por ache-lo simples quebraria
     * justamente nos casos em que ele coincide com uma keyword, que e onde a
     * citacao importa.
     */
    @Override
    public String enquoteIdentifier(String identificador, boolean sempre) throws SQLException {
        if (identificador == null || identificador.isEmpty()) {
            throw new SQLException("nao ha identificador a citar", SqlStates.CONVERSAO_INVALIDA);
        }
        return "\"" + identificador.replace("\"", "\"\"") + "\"";
    }

    /**
     * Responde a pergunta LEXICA: comeca com letra ou sublinhado e segue com
     * ASCII alfanumerico ou sublinhado, que e o identificador bare do lexer.
     *
     * <p>Nao consulta lista de keywords, e por isso um nome simples que
     * COINCIDA com uma palavra reservada responde true aqui — e a razao de
     * {@link #enquoteIdentifier} citar sempre.
     */
    @Override
    public boolean isSimpleIdentifier(String identificador) throws SQLException {
        if (identificador == null || identificador.isEmpty()) {
            throw new SQLException("nao ha identificador a examinar", SqlStates.CONVERSAO_INVALIDA);
        }
        char primeiro = identificador.charAt(0);
        if (primeiro != '_' && !(primeiro >= 'a' && primeiro <= 'z')
                && !(primeiro >= 'A' && primeiro <= 'Z')) {
            return false;
        }
        for (int i = 1; i < identificador.length(); i++) {
            char c = identificador.charAt(i);
            boolean parte = c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9');
            if (!parte) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("este Statement não embrulha " + alvo.getName(), SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    private void fechaResultadoAnterior() throws SQLException {
        NishiResultSet anterior = resultado;
        if (anterior == null) {
            return;
        }
        trocandoResultado = true;
        try {
            anterior.close();
        } finally {
            trocandoResultado = false;
            resultado = null;
        }
    }

    final void exigeAberto() throws SQLException {
        if (fechado) {
            throw new SQLException("este Statement já foi fechado", SqlStates.CURSOR_INVALIDO);
        }
    }

    static SQLFeatureNotSupportedException naoSuportado(String porque) {
        return new SQLFeatureNotSupportedException(porque, SqlStates.NAO_SUPORTADO);
    }
}
