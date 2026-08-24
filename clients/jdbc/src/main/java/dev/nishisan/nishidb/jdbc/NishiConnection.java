package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonValue;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.Transporte;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A conexão do §70 — um {@link Transporte} e os tetos do dataset.
 *
 * <p><b>O connect consulta {@code /v1/metadata}</b>, e não por curiosidade: é
 * de lá que saem o teto de linhas e o de tempo do dataset, que o driver precisa
 * saber ANTES da primeira consulta para não mandar ao servidor um pedido que
 * ele vai recusar. O efeito colateral é o bom: um dataset inexistente falha ao
 * ABRIR a conexão, com o nome errado na mensagem, em vez de falhar na primeira
 * consulta com um erro que parece de SQL.
 *
 * <p>Não é segura para uso concorrente, como o contrato do JDBC permite. Uma
 * conexão executa uma consulta por vez — é por isso que o transporte fixa
 * HTTP/1.1 e não perde nada com isso.
 */
public final class NishiConnection implements Connection {

    private final Transporte transporte;
    private final ConnectionConfig cfg;
    private final String datasetPadrao;
    private final int maxRowsDoDataset;
    private final long timeoutMsDoDataset;
    private final List<NishiStatement> abertos = new ArrayList<>();

    private boolean fechado;

    NishiConnection(ConnectionConfig cfg) throws SQLException {
        this.transporte = new Transporte(
                Transporte.clienteHttp(Duration.ofMillis(cfg.connectTimeoutMs()), cfg.contextoTls()),
                cfg.base(), cfg.autorizacao());
        this.cfg = cfg;
        this.datasetPadrao = cfg.dataset();

        JsonValue meta;
        try {
            meta = transporte.documento("/v1/metadata?dataset="
                    + java.net.URLEncoder.encode(cfg.dataset(), java.nio.charset.StandardCharsets.UTF_8));
        } catch (SQLException e) {
            transporte.close();
            throw e;
        }
        List<JsonValue> datasets = meta.itensDe("datasets");
        if (datasets.isEmpty()) {
            transporte.close();
            throw new SQLException("o dataset \"" + cfg.dataset() + "\" não está registrado neste servidor",
                    "42S02");
        }
        JsonValue d = datasets.get(0);
        this.maxRowsDoDataset = (int) d.inteiroOu("maxRows", 0);
        this.timeoutMsDoDataset = d.inteiroOu("timeoutMs", 0);
    }

    Transporte transporte() {
        return transporte;
    }

    ConnectionConfig configuracao() {
        return cfg;
    }

    /** O dataset da URL — o palpite de quando o SQL não nomeia um no FROM. */
    String datasetPadrao() {
        return datasetPadrao;
    }

    int maxRowsDoDataset() {
        return maxRowsDoDataset;
    }

    long timeoutMsDoDataset() {
        return timeoutMsDoDataset;
    }

    void esqueceStatement(NishiStatement st) {
        abertos.remove(st);
    }

    // ------------------------------------------------------------ statements

    @Override
    public Statement createStatement() throws SQLException {
        exigeAberta();
        NishiStatement st = new NishiStatement(this);
        abertos.add(st);
        return st;
    }

    /**
     * Só {@code TYPE_FORWARD_ONLY} e {@code CONCUR_READ_ONLY}, e a recusa é na
     * CRIAÇÃO.
     *
     * <p>O contrato do JDBC permitiria criar assim mesmo e rebaixar com um
     * SQLWarning. Recusar aqui é melhor: a aplicação que pediu um cursor
     * rolável vai chamar {@code previous()} adiante, e o erro apareceria longe
     * do lugar onde a decisão foi tomada.
     */
    @Override
    public Statement createStatement(int tipo, int concorrencia) throws SQLException {
        exigeTipoDeResultado(tipo, concorrencia);
        return createStatement();
    }

    @Override
    public Statement createStatement(int tipo, int concorrencia, int holdability) throws SQLException {
        exigeTipoDeResultado(tipo, concorrencia);
        return createStatement();
    }

    private void exigeTipoDeResultado(int tipo, int concorrencia) throws SQLException {
        if (tipo != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("este driver só tem cursor para frente (TYPE_FORWARD_ONLY): "
                    + "o resultado é um stream, e voltar exigiria materializá-lo",
                    SqlStates.NAO_SUPORTADO);
        }
        if (concorrencia != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException("o NishiDB não aceita escrita por SQL (CONCUR_READ_ONLY): "
                    + "os dados entram por ingestão", SqlStates.NAO_SUPORTADO);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        exigeAberta();
        if (sql == null) {
            // O scanner de SQL estouraria com NullPointerException, que NÃO é
            // SQLException e atravessa o catch da aplicação virando stack
            // trace em vez de mensagem.
            throw new SQLException("a consulta é nula", SqlStates.CONVERSAO_INVALIDA);
        }
        NishiPreparedStatement st = new NishiPreparedStatement(this, sql);
        abertos.add(st);
        return st;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int tipo, int concorrencia) throws SQLException {
        exigeTipoDeResultado(tipo, concorrencia);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int tipo, int concorrencia, int holdability)
            throws SQLException {
        exigeTipoDeResultado(tipo, concorrencia);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int chavesGeradas) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] colunas) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] colunas) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw naoSuportado("o NishiDB não tem procedimento armazenado");
    }

    @Override
    public CallableStatement prepareCall(String sql, int tipo, int concorrencia) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int tipo, int concorrencia, int holdability)
            throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        exigeAberta();
        return new NishiDatabaseMetaData(this);
    }

    /** O SQL vai VERBATIM ao servidor: não há dialeto a traduzir. */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        exigeAberta();
        return sql;
    }

    // ---------------------------------------------------------- transações

    /**
     * Não há transação, e o driver diz "sim" ao que já é verdade.
     *
     * <p>{@code setAutoCommit(true)} e {@code setReadOnly(true)} são no-ops
     * porque descrevem o estado real; os inversos lançam, porque prometê-los
     * seria dizer que uma escrita pode ser desfeita.
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        exigeAberta();
        if (!autoCommit) {
            throw naoSuportado("o NishiDB não tem transação: autoCommit é sempre true");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        exigeAberta();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw naoSuportado("o NishiDB não tem transação a confirmar");
    }

    @Override
    public void rollback() throws SQLException {
        throw naoSuportado("o NishiDB não tem transação a desfazer");
    }

    @Override
    public void rollback(Savepoint ponto) throws SQLException {
        rollback();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw naoSuportado("o NishiDB não tem transação, logo não tem savepoint");
    }

    @Override
    public Savepoint setSavepoint(String nome) throws SQLException {
        return setSavepoint();
    }

    @Override
    public void releaseSavepoint(Savepoint ponto) throws SQLException {
        throw naoSuportado("o NishiDB não tem transação, logo não tem savepoint");
    }

    @Override
    public void setReadOnly(boolean somenteLeitura) throws SQLException {
        exigeAberta();
        if (!somenteLeitura) {
            throw naoSuportado("esta conexão é somente leitura: o NishiDB não escreve por SQL");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        exigeAberta();
        return true;
    }

    @Override
    public void setTransactionIsolation(int nivel) throws SQLException {
        exigeAberta();
        if (nivel != TRANSACTION_NONE) {
            throw naoSuportado("o NishiDB não tem transação: o único nível é TRANSACTION_NONE");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        exigeAberta();
        return TRANSACTION_NONE;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        exigeAberta();
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw naoSuportado("sem transação não há cursor que sobreviva a um commit");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        exigeAberta();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // --------------------------------------------------------- catálogo

    /** O NishiDB não tem catálogo nem schema: as duas respostas são vazio. */
    @Override
    public void setCatalog(String catalogo) throws SQLException {
        exigeAberta();
    }

    @Override
    public String getCatalog() throws SQLException {
        exigeAberta();
        return "";
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        exigeAberta();
    }

    @Override
    public String getSchema() throws SQLException {
        exigeAberta();
        return "";
    }

    // ------------------------------------------------------------ ciclo

    /**
     * Fecha na ORDEM: os ResultSet abertos, depois os Statement, depois o
     * cliente HTTP.
     *
     * <p>Um stream não consumido nem fechado nunca devolve a conexão, e é assim
     * que uma ferramenta trava depois de algumas dezenas de consultas. O
     * transporte usa shutdownNow porque este método é chamado da thread de
     * interface — o close() do HttpClient bloqueia até todo exchange terminar.
     */
    @Override
    public void close() throws SQLException {
        if (fechado) {
            return;
        }
        fechado = true;
        SQLException primeira = null;
        for (NishiStatement st : new ArrayList<>(abertos)) {
            try {
                st.close();
            } catch (SQLException e) {
                if (primeira == null) {
                    primeira = e;
                } else {
                    primeira.addSuppressed(e);
                }
            }
        }
        abertos.clear();
        transporte.close();
        if (primeira != null) {
            throw primeira;
        }
    }

    @Override
    public boolean isClosed() {
        return fechado;
    }

    /**
     * Pergunta ao SERVIDOR, e não só ao próprio estado.
     *
     * <p>É o que um pool de conexões usa para decidir se devolve uma conexão
     * ociosa a quem a pediu. Responder "não estou fechada" sem falar com o
     * servidor entregaria ao chamador uma conexão cuja outra ponta já morreu.
     */
    @Override
    public boolean isValid(int segundos) throws SQLException {
        if (segundos < 0) {
            throw new SQLException("timeout negativo em isValid: " + segundos,
                    SqlStates.VALOR_FORA_DE_FAIXA);
        }
        if (fechado) {
            return false;
        }
        try {
            return transporte.documento("/v1/health") != null;
        } catch (SQLException e) {
            return false;
        }
    }

    // ----------------------------------------------------------- o resto

    @Override
    public SQLWarning getWarnings() throws SQLException {
        exigeAberta();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        exigeAberta();
    }

    @Override
    public void setClientInfo(String nome, String valor) throws SQLClientInfoException {
        throw new SQLClientInfoException("este driver não guarda client info", Map.of());
    }

    @Override
    public void setClientInfo(Properties props) throws SQLClientInfoException {
        throw new SQLClientInfoException("este driver não guarda client info", Map.of());
    }

    @Override
    public String getClientInfo(String nome) {
        return null;
    }

    @Override
    public Properties getClientInfo() {
        return new Properties();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        exigeAberta();
        return Map.of();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> mapa) throws SQLException {
        throw naoSuportado("este driver não usa mapa de tipos: os tipos vêm da linha de schema");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public Array createArrayOf(String tipo, Object[] itens) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; um array vem dentro da coluna json");
    }

    @Override
    public Struct createStruct(String tipo, Object[] campos) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    private static final String SEM_TIPO =
            "o NishiDB não tem este tipo; as colunas são escalares, texto, binário e json";

    /**
     * Abortar exigiria derrubar o cliente HTTP inteiro, que é compartilhado
     * pela conexão. Quem precisa limitar a duração de UMA consulta usa
     * {@code Statement.setQueryTimeout}, que o servidor honra do lado dele.
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        throw naoSuportado("use Statement.setQueryTimeout para limitar uma consulta, ou close()");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        throw naoSuportado("o prazo de rede é fixado na conexão por connectTimeoutMs; "
                + "prazo de CONSULTA é Statement.setQueryTimeout");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        exigeAberta();
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("esta Connection não embrulha " + alvo.getName(), SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    private void exigeAberta() throws SQLException {
        if (fechado) {
            throw new SQLException("esta conexão já foi fechada", SqlStates.CONEXAO_NAO_ESTABELECIDA);
        }
    }

    private static SQLFeatureNotSupportedException naoSuportado(String porque) {
        return new SQLFeatureNotSupportedException(porque, SqlStates.NAO_SUPORTADO);
    }
}
