package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonWriter;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;

/**
 * O {@code PreparedStatement} do §130.
 *
 * <p><b>Quatro tipos de fio, e nenhum a mais</b> — string, int64, double e
 * boolean. Não é escassez: é o contrato do binder do servidor, que recusa
 * parâmetro já em representação binária para manter UM caminho de coerção.
 * Endereços, prefixos e temporais viajam por um desses quatro e são convertidos
 * pela MESMA tabela que interpreta os literais escritos no SQL.
 *
 * <p><b>Temporal viaja como int64</b>, e a razão é concreta: uma string contra
 * uma coluna {@code datetime} é interpretada no {@code format} DECLARADO do
 * campo, resolvido no fuso do host do SERVIDOR. Um {@code setTimestamp} que
 * enviasse ISO-8601 funcionaria contra uma coluna {@code timestamp} e falharia
 * contra uma {@code datetime}, pela mesma chamada — e o erro dependeria de qual
 * coluna a query citou. Epoch millis (e millis do dia para {@code time}) é o
 * único caminho uniforme.
 *
 * <p><b>Não existe setNull.</b> O protocolo não tem parâmetro nulo, e não é
 * esquecimento: comparar com nulo em SQL é {@code IS NULL}, que a v1 tem. O
 * modo de falha que a recusa evita é o pior — aceitar o setNull e mandar algo
 * que não casa nada devolveria um resultado VAZIO, que a aplicação leria como
 * "não há linhas".
 */
public final class NishiPreparedStatement extends NishiStatement implements PreparedStatement {

    private final String sql;
    private final String[] valores;

    NishiPreparedStatement(NishiConnection conexao, String sql) {
        super(conexao);
        this.sql = sql;
        this.valores = new String[SqlScanner.de(sql).placeholders()];
    }

    // ------------------------------------------------------------- execução

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executa(sql, arrayDeParametros());
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();
        return true;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw naoSuportado("o NishiDB não aceita escrita por SQL: os dados entram por ingestão");
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        throw naoSuportado("o NishiDB não aceita escrita por SQL: os dados entram por ingestão");
    }

    /**
     * Monta o array de parâmetros, recusando o que ficou de fora.
     *
     * <p>Mandar menos parâmetros do que a query tem faria o servidor recusar
     * com uma mensagem sobre CONTAGEM, que não aponta qual foi esquecido.
     */
    private String arrayDeParametros() throws SQLException {
        StringBuilder sb = new StringBuilder(valores.length * 32 + 2).append('[');
        for (int i = 0; i < valores.length; i++) {
            if (valores[i] == null) {
                throw new SQLException("o parâmetro " + (i + 1) + " de " + valores.length
                        + " não foi preenchido", SqlStates.CONVERSAO_INVALIDA);
            }
            if (i > 0) {
                sb.append(',');
            }
            sb.append(valores[i]);
        }
        return sb.append(']').toString();
    }

    /**
     * Os métodos que recebem SQL não valem aqui — é o contrato do JDBC.
     *
     * <p>Um PreparedStatement carrega a SUA consulta; aceitar outra por estes
     * métodos deixaria os parâmetros já preenchidos apontando para uma query
     * que não é a deles.
     */
    @Override
    public ResultSet executeQuery(String outro) throws SQLException {
        throw comSqlNaoVale();
    }

    @Override
    public boolean execute(String outro) throws SQLException {
        throw comSqlNaoVale();
    }

    @Override
    public int executeUpdate(String outro) throws SQLException {
        throw comSqlNaoVale();
    }

    @Override
    public long executeLargeUpdate(String outro) throws SQLException {
        throw comSqlNaoVale();
    }

    @Override
    public void addBatch(String outro) throws SQLException {
        throw comSqlNaoVale();
    }

    private static SQLException comSqlNaoVale() {
        return new SQLException("um PreparedStatement carrega a própria consulta: "
                + "use o método sem argumento de SQL", SqlStates.NAO_SUPORTADO);
    }

    @Override
    public void addBatch() throws SQLException {
        throw naoSuportado("o NishiDB não aceita escrita por SQL, e um lote de SELECT "
                + "não é o que o batch do JDBC faz");
    }

    // ------------------------------------------------------------ os quatro

    @Override
    public void setString(int indice, String valor) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        poe(indice, "string", JsonWriter.textoComoJson(valor));
    }

    @Override
    public void setBoolean(int indice, boolean valor) throws SQLException {
        poe(indice, "boolean", valor ? "true" : "false");
    }

    @Override
    public void setByte(int indice, byte valor) throws SQLException {
        setLong(indice, valor);
    }

    @Override
    public void setShort(int indice, short valor) throws SQLException {
        setLong(indice, valor);
    }

    @Override
    public void setInt(int indice, int valor) throws SQLException {
        setLong(indice, valor);
    }

    @Override
    public void setLong(int indice, long valor) throws SQLException {
        // Texto de dígitos, e nunca um número de ponto flutuante: é o espelho
        // do campo "value" ser RawMessage do lado Go. Por double,
        // 9223372036854775807 chegaria ao servidor como ...5808.
        poe(indice, "int64", Long.toString(valor));
    }

    @Override
    public void setFloat(int indice, float valor) throws SQLException {
        setDouble(indice, valor);
    }

    @Override
    public void setDouble(int indice, double valor) throws SQLException {
        poe(indice, "double", JsonWriter.numeroComoJson(valor));
    }

    /**
     * Vai como int64 quando é inteiro exato, e como double quando não é.
     *
     * <p>Não há tipo decimal no fio, e não haveria o que ganhar com um: a
     * coluna do outro lado é int64 ou binary64. Preservar a exatidão no caso
     * inteiro é o que impede um identificador grande de perder o último dígito.
     */
    @Override
    public void setBigDecimal(int indice, BigDecimal valor) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        try {
            setLong(indice, valor.longValueExact());
        } catch (ArithmeticException naoEhInteiro) {
            setDouble(indice, valor.doubleValue());
        }
    }

    // ------------------------------------------------------------ temporais

    @Override
    public void setTimestamp(int indice, Timestamp valor) throws SQLException {
        setTimestamp(indice, valor, null);
    }

    /**
     * O Calendar é IGNORADO, pelo mesmo motivo que em getTimestamp: o valor já
     * é um instante absoluto, e não há relógio de parede a situar.
     */
    @Override
    public void setTimestamp(int indice, Timestamp valor, Calendar cal) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        setLong(indice, valor.getTime());
    }

    @Override
    public void setDate(int indice, Date valor) throws SQLException {
        setDate(indice, valor, null);
    }

    @Override
    public void setDate(int indice, Date valor, Calendar cal) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        setLong(indice, valor.getTime());
    }

    @Override
    public void setTime(int indice, Time valor) throws SQLException {
        setTime(indice, valor, null);
    }

    /**
     * Millis DESDE A MEIA-NOITE, e não epoch — e aqui o Calendar tem função.
     *
     * <p>É a mesma palavra ("inteiro") significando outra coisa, e o binder
     * registra isso: um epoch mandado numa coluna {@code time} devolve zero
     * linhas sem erro nenhum. A conversão desfaz exatamente o que o getTime
     * faz — lê o relógio de parede do instante na zona pedida.
     */
    @Override
    public void setTime(int indice, Time valor, Calendar cal) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        ZoneId zona = cal == null ? ZoneId.systemDefault() : cal.getTimeZone().toZoneId();
        LocalTime hora = Instant.ofEpochMilli(valor.getTime()).atZone(zona).toLocalTime();
        setLong(indice, hora.toNanoOfDay() / 1_000_000L);
    }

    // -------------------------------------------------------------- objeto

    @Override
    public void setObject(int indice, Object valor) throws SQLException {
        if (valor == null) {
            throw semNulo();
        }
        if (valor instanceof String s) {
            setString(indice, s);
        } else if (valor instanceof Boolean b) {
            setBoolean(indice, b);
        } else if (valor instanceof Long || valor instanceof Integer
                || valor instanceof Short || valor instanceof Byte) {
            setLong(indice, ((Number) valor).longValue());
        } else if (valor instanceof BigDecimal d) {
            setBigDecimal(indice, d);
        } else if (valor instanceof Number n) {
            setDouble(indice, n.doubleValue());
        } else if (valor instanceof Timestamp t) {
            setTimestamp(indice, t);
        } else if (valor instanceof Time t) {
            setTime(indice, t);
        } else if (valor instanceof Date d) {
            setDate(indice, d);
        } else if (valor instanceof Instant i) {
            setLong(indice, i.toEpochMilli());
        } else if (valor instanceof LocalTime h) {
            setLong(indice, h.toNanoOfDay() / 1_000_000L);
        } else {
            // LocalDate e LocalDateTime caem AQUI de propósito: os dois são
            // relógio de parede sem fuso, e mandá-los exigiria o driver
            // escolher uma zona pela aplicação. Quem tem um deles converte
            // para Instant, dizendo em que fuso.
            throw naoSuportado("este driver não converte " + valor.getClass().getName()
                    + " para parâmetro; o fio tem string, int64, double e boolean");
        }
    }

    /**
     * O tipo de destino é IGNORADO, e a razão é que ele seria uma TERCEIRA
     * opinião sobre a mesma coisa.
     *
     * <p>O valor já determina o tipo de fio, e a coerção final é da coluna — do
     * lado do servidor, pela mesma tabela que interpreta os literais do SQL.
     * Um teste compara os bytes das duas formas, para que este "ignorar" seja
     * verificável em vez de silencioso.
     */
    @Override
    public void setObject(int indice, Object valor, int tipoDestino) throws SQLException {
        setObject(indice, valor);
    }

    @Override
    public void setObject(int indice, Object valor, int tipoDestino, int escala) throws SQLException {
        setObject(indice, valor);
    }

    @Override
    public void setObject(int indice, Object valor, SQLType tipoDestino) throws SQLException {
        setObject(indice, valor);
    }

    @Override
    public void setObject(int indice, Object valor, SQLType tipoDestino, int escala) throws SQLException {
        setObject(indice, valor);
    }

    // ---------------------------------------------------- o que não existe

    @Override
    public void setNull(int indice, int tipoSql) throws SQLException {
        throw semNulo();
    }

    @Override
    public void setNull(int indice, int tipoSql, String nomeDoTipo) throws SQLException {
        throw semNulo();
    }

    private static SQLFeatureNotSupportedException semNulo() {
        return naoSuportado("o protocolo não tem parâmetro nulo: para comparar com nulo, "
                + "escreva IS NULL no SQL. Aceitar o nulo aqui devolveria um resultado vazio, "
                + "que parece \"não há linhas\"");
    }

    @Override
    public void setBytes(int indice, byte[] valor) throws SQLException {
        throw naoSuportado("o binder não compara coluna binária: não há WHERE sobre bytes");
    }

    @Override
    public void setAsciiStream(int indice, InputStream v, int tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int indice, InputStream v, int tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setBinaryStream(int indice, InputStream v, int tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setCharacterStream(int indice, Reader v, int tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setAsciiStream(int indice, InputStream v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setBinaryStream(int indice, InputStream v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setCharacterStream(int indice, Reader v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setAsciiStream(int indice, InputStream v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setBinaryStream(int indice, InputStream v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setCharacterStream(int indice, Reader v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setNCharacterStream(int indice, Reader v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setNCharacterStream(int indice, Reader v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setClob(int indice, Reader v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setClob(int indice, Reader v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setNClob(int indice, Reader v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setNClob(int indice, Reader v) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setBlob(int indice, InputStream v, long tamanho) throws SQLException {
        throw semFluxo();
    }

    @Override
    public void setBlob(int indice, InputStream v) throws SQLException {
        throw semFluxo();
    }

    private static SQLFeatureNotSupportedException semFluxo() {
        return naoSuportado("um parâmetro do NishiDB é um escalar pequeno, não um fluxo: "
                + "a comparação acontece célula a célula");
    }

    @Override
    public void setNString(int indice, String valor) throws SQLException {
        setString(indice, valor);
    }

    @Override
    public void setRef(int indice, Ref v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setBlob(int indice, Blob v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setClob(int indice, Clob v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setNClob(int indice, NClob v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setArray(int indice, Array v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setURL(int indice, URL v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setRowId(int indice, RowId v) throws SQLException {
        throw semTipo();
    }

    @Override
    public void setSQLXML(int indice, SQLXML v) throws SQLException {
        throw semTipo();
    }

    private static SQLFeatureNotSupportedException semTipo() {
        return naoSuportado("o fio tem quatro tipos de parâmetro: string, int64, double e boolean");
    }

    // ------------------------------------------------------------ metadados

    /**
     * Nulo antes de executar, e é a resposta honesta.
     *
     * <p>As colunas do resultado vêm da LINHA DE SCHEMA, que o servidor emite
     * ao começar a responder. Descobri-las antes exigiria uma segunda chamada
     * só para isso — e ela poderia divergir da consulta real, porque o schema
     * de um dataset com campo auto muda com a ingestão.
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        exigeAberto();
        ResultSet atual = getResultSet();
        return atual == null ? null : atual.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        exigeAberto();
        return new NishiParameterMetaData(valores.length);
    }

    @Override
    public void clearParameters() throws SQLException {
        exigeAberto();
        java.util.Arrays.fill(valores, null);
    }

    /** Guarda o parâmetro já como o JSON que vai no fio, com o índice 1-based do JDBC. */
    private void poe(int indice, String tipo, String valorJson) throws SQLException {
        exigeAberto();
        if (indice < 1 || indice > valores.length) {
            throw new SQLException("o parâmetro " + indice + " não existe nesta consulta, "
                    + "que tem " + valores.length + " (o primeiro é 1)", SqlStates.CONVERSAO_INVALIDA);
        }
        valores[indice - 1] = "{\"type\":\"" + tipo + "\",\"value\":" + valorJson + "}";
    }
}
