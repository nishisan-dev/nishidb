package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonValue;
import dev.nishisan.nishidb.jdbc.wire.RequestTimeout;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.Transporte;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
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
import java.util.concurrent.RejectedExecutionException;

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
 * HTTP/1.1 e não perde nada com isso. As exceções são {@link #abort} e
 * {@link #isClosed}, que o contrato prevê chamadas de OUTRA thread — a que
 * destrava uma requisição presa —, e por isso o estado de fechada é volátil.
 *
 * <p><b>Os prazos de espera pelo servidor</b> (issue #91), do mais específico
 * para o mais geral — o específico SUBSTITUI o geral naquela requisição, nos
 * dois sentidos, maior ou menor:
 *
 * <ul>
 *   <li>{@code isValid(s)}, com {@code s > 0}: a requisição inteira em
 *       {@code s}; estouro é {@code false}.
 *   <li>{@code Statement.setQueryTimeout(s)}: o prazo que vai ao servidor mais
 *       {@link NishiStatement#QUERY_TIMEOUT_GRACE_MS}; estouro é
 *       {@code SQLTimeoutException} 57014, e a conexão continua utilizável.
 *   <li>O prazo de rede — {@code socketTimeoutMs} na URL, depois
 *       {@link #setNetworkTimeout}: toda requisição sem prazo próprio
 *       (metadata, catálogo, stats, consulta sem queryTimeout,
 *       {@code isValid(0)}). Estouro FECHA a conexão, que é o contrato do
 *       {@code setNetworkTimeout}. Zero, o default, é sem prazo.
 * </ul>
 *
 * <p>O que cada prazo mede depende do caminho, e não da origem: na consulta,
 * a espera pelos cabeçalhos e cada espera por bytes do stream (o silêncio,
 * nunca a duração); nos documentos pequenos (health, metadata, catálogo,
 * stats), a requisição inteira — ver {@link RequestTimeout}.
 */
public final class NishiConnection implements Connection {

    private final Transporte transporte;
    private final ConnectionConfig cfg;
    private final String schema;
    private final int maxRowsDoDataset;
    private final long timeoutMsDoDataset;
    private final List<NishiStatement> abertos = new ArrayList<>();

    /**
     * Volátil: {@link #abort} e o estouro do prazo de rede a ligam de outra
     * thread, ou no meio de uma leitura.
     */
    private volatile boolean fechado;
    /**
     * Se o {@link #close()} já fechou os Statement e o cliente HTTP.
     * Separado de {@link #fechado} porque o abort marca a conexão fechada sem
     * fechar os Statement — que podem estar em uso na thread presa —, e o
     * close() que vier depois ainda tem de fazê-lo.
     */
    private boolean resourcesReleased;
    /** O prazo de rede em ms, zero sem prazo; nasce do socketTimeoutMs. */
    private int networkTimeoutMs;

    NishiConnection(ConnectionConfig cfg) throws SQLException {
        this.transporte = new Transporte(
                Transporte.clienteHttp(Duration.ofMillis(cfg.connectTimeoutMs()), cfg.contextoTls()),
                cfg.base(), cfg.autorizacao());
        this.cfg = cfg;
        this.schema = cfg.schema();
        this.networkTimeoutMs = cfg.socketTimeoutMs();

        JsonValue meta;
        try {
            meta = transporte.documento(metadataPath(schema), networkTimeout());
        } catch (SQLException e) {
            transporte.close();
            if (SqlStates.INVALID_SCHEMA_NAME.equals(e.getSQLState())) {
                // A mensagem do servidor já diz qual schema usar quando o
                // caminho é o nome de um dataset — a URL da forma anterior. O
                // que só o driver sabe é a forma da URL, e é ela que vai junto.
                throw new SQLNonTransientConnectionException(
                        e.getMessage() + "; a URL nomeia o schema: " + NishiUrl.FORMA,
                        e.getSQLState(), e);
            }
            throw e;
        }
        List<JsonValue> datasets = meta.itensDe("datasets");
        if (datasets.isEmpty()) {
            // O servidor responde 404 UNKNOWN_SCHEMA para o schema que não
            // tem, então um 200 vazio é o servidor fora do protocolo.
            transporte.close();
            throw new SQLException("o servidor devolveu o schema \"" + schema + "\" sem dataset nenhum",
                    SqlStates.PROTOCOLO_VIOLADO);
        }
        // O primeiro elemento é o dataset BASE do schema (o servidor publica
        // o base antes da irmã do histórico): é dele que saem os tetos.
        JsonValue d = datasets.get(0);
        this.maxRowsDoDataset = (int) d.inteiroOu("maxRows", 0);
        this.timeoutMsDoDataset = d.inteiroOu("timeoutMs", 0);
    }

    Transporte transporte() {
        return transporte;
    }

    /**
     * O prazo de rede, para a requisição que não tem prazo próprio.
     *
     * <p>O estouro FECHA a conexão: é o contrato do
     * {@code Connection.setNetworkTimeout} ("the Connection ... will be marked
     * as closed"), e é o certo — um servidor que ficou o prazo de rede inteiro
     * sem responder não pode voltar ao pool como se nada tivesse acontecido. A
     * exceção é {@code SQLRecoverableException} 08006: a classe que diz "abra
     * outra conexão e repita", a mesma da queda no meio do stream.
     */
    RequestTimeout networkTimeout() {
        int ms = networkTimeoutMs;
        if (ms == 0) {
            return RequestTimeout.NONE;
        }
        return new RequestTimeout(ms, detail -> {
            abandon();
            return new SQLRecoverableException(detail + " (prazo de rede de " + ms
                    + " ms: socketTimeoutMs ou setNetworkTimeout); a conexao foi fechada, abra outra",
                    SqlStates.CONEXAO_INTERROMPIDA);
        });
    }

    /** Marca fechada e derruba o cliente HTTP sem esperar o servidor. */
    private void abandon() {
        fechado = true;
        transporte.abandon();
    }

    ConnectionConfig configuracao() {
        return cfg;
    }

    /** O schema da conexão — o caminho da URL, que vai em toda consulta. */
    String schema() {
        return schema;
    }

    /** O /v1/metadata recortado pelo schema da conexão. */
    static String metadataPath(String schema) {
        return "/v1/metadata?schema=" + URLEncoder.encode(schema, StandardCharsets.UTF_8);
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

    /** O NishiDB não tem catálogo: a resposta é vazio, e o pedido não muda nada. */
    @Override
    public void setCatalog(String catalogo) throws SQLException {
        exigeAberta();
    }

    @Override
    public String getCatalog() throws SQLException {
        exigeAberta();
        return "";
    }

    /**
     * O schema da conexão é FIXADO pela URL: pedir o mesmo não faz nada, e
     * pedir outro é recusado.
     *
     * <p>Aceitar em silêncio faria a ferramenta acreditar que trocou de schema
     * enquanto o servidor continua resolvendo o {@code FROM} no da URL — a
     * consulta seguinte falharia com um erro que parece de SQL. {@code null} é
     * ignorado: não pede schema nenhum.
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        exigeAberta();
        if (schema != null && !schema.equals(this.schema)) {
            throw new SQLException("o schema da conexão é \"" + this.schema + "\", fixado pela URL; para consultar \""
                    + schema + "\", abra uma conexão a " + NishiUrl.PREFIXO + "host[:porta]/" + schema,
                    SqlStates.INVALID_SCHEMA_NAME);
        }
    }

    @Override
    public String getSchema() throws SQLException {
        exigeAberta();
        return schema;
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
     *
     * <p>Depois de um {@link #abort} ou de um estouro do prazo de rede, a
     * conexão já está marcada fechada, mas os Statement ainda não: este
     * close() os fecha do mesmo jeito.
     */
    @Override
    public void close() throws SQLException {
        if (resourcesReleased) {
            return;
        }
        resourcesReleased = true;
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
     *
     * <p><b>Volta em {@code segundos}</b>, como manda o contrato: o prazo é da
     * requisição inteira ao {@code /v1/health}, e o estouro é {@code false},
     * nunca exceção. É o que o pool (o HikariCP valida com isto) precisa:
     * sem o prazo, um servidor que aceita e não responde prendia a thread que
     * pediu a conexão, e o pool esgotava. {@code segundos == 0} é "sem prazo
     * próprio" pelo contrato, e aí vale o prazo de rede — que, zero, é sem
     * prazo nenhum.
     *
     * <p>O estouro aqui NÃO fecha a conexão, nem quando o prazo é o de rede:
     * responder {@code false} já entrega a decisão a quem perguntou, e o
     * contrato permite chamar isValid numa conexão em qualquer estado.
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
        long ms = segundos > 0 ? segundos * 1000L : networkTimeoutMs;
        RequestTimeout prazo = ms == 0 ? RequestTimeout.NONE
                : new RequestTimeout(ms, detail -> new SQLTimeoutException(detail, SqlStates.CANCELADO));
        try {
            return transporte.documento("/v1/health", prazo) != null;
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
     * Derruba a conexão sem esperar o servidor, de qualquer thread.
     *
     * <p>Marca a conexão fechada AQUI, antes de voltar — quem chamou abort
     * pode contar com {@code isClosed()} verdadeiro na linha seguinte — e
     * derruba o cliente HTTP no {@code executor}, como o contrato pede. A
     * requisição presa em outra thread — esperando os cabeçalhos ou já lendo o
     * corpo — sai com {@code SQLException} 08003 de conexão fechada. Os
     * Statement não são fechados aqui, porque podem estar em uso justamente
     * nela; eles passam a se recusar a executar (08003) desde já, e o
     * {@link #close()} que vier depois os fecha de fato. Se o executor recusar
     * a tarefa, o cliente HTTP é derrubado na thread de quem abortou — é um
     * {@code shutdownNow}, que não espera —, e a recusa não vaza. Chamado numa
     * conexão já fechada, não faz nada.
     *
     * <p>Não consulta {@code SecurityManager} pela permissão
     * {@code SQLPermission("callAbort")}: o piso do driver é o JDK 21, onde o
     * SecurityManager está condenado à remoção, e a partir do JDK 24 (JEP 486)
     * ele não pode mais ser ligado — a checagem seria código morto.
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("abort exige um executor: é nele que o cliente HTTP é derrubado",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        if (fechado) {
            return;
        }
        fechado = true;
        try {
            executor.execute(transporte::abandon);
        } catch (RejectedExecutionException e) {
            // O executor recusou (pool desligado, fila cheia). A conexão já
            // está marcada fechada, e deixá-la assim com o cliente HTTP vivo
            // manteria presa a requisição que o abort veio destravar. O
            // abandon é um shutdownNow, que não espera nada: roda aqui mesmo,
            // e a recusa não vaza como RuntimeException para quem abortou.
            transporte.abandon();
        }
    }

    /**
     * Ajusta o prazo de rede — o mesmo que {@code socketTimeoutMs} inicia.
     *
     * <p>Vale para toda requisição que não tem prazo próprio, e o estouro fecha
     * a conexão (ver {@link #networkTimeout()}). O {@code executor} é exigido
     * pelo contrato, mas não é usado: o prazo é contado pelo relógio do
     * HttpClient e pelo do driver, e nenhum dos dois precisa de thread de quem
     * chama.
     */
    @Override
    public void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        exigeAberta();
        if (executor == null) {
            throw new SQLException("setNetworkTimeout exige um executor, pelo contrato do JDBC",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        if (ms < 0) {
            throw new SQLException("prazo de rede negativo: " + ms + "; 0 é sem prazo",
                    SqlStates.VALOR_FORA_DE_FAIXA);
        }
        networkTimeoutMs = ms;
    }

    /** O prazo de rede em vigor, em ms; zero é sem prazo. */
    @Override
    public int getNetworkTimeout() throws SQLException {
        exigeAberta();
        return networkTimeoutMs;
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
