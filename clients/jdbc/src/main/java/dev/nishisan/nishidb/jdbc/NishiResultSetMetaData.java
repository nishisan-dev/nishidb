package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.wire.Molduras;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.TiposJdbc;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * O {@code ResultSetMetaData} do §71 — e ele NÃO recalcula nada.
 *
 * <p>Tipo, nome JDBC, nulabilidade, precision, scale e displaySize vêm da linha
 * de schema, que o servidor monta a partir de uma tabela única. É essa decisão
 * que faz um tipo novo no servidor não exigir um jar novo no cliente: o driver
 * repassa números que ele não precisa saber calcular. O que ele traduz é só o
 * vocabulário — o nome JDBC para o inteiro de {@code java.sql.Types} —, e essa
 * tradução tem gate dos dois lados.
 *
 * <p>O nome publicado aqui é o nome de SAÍDA da projeção, que para uma coluna
 * com alias é o ALIAS — porque é ele que é chave do objeto JSON de cada linha.
 * O {@code /v1/metadata} publica o nome do CAMPO e o alias à parte, por
 * descrever schema e não projeção. Os dois não se casam por igualdade de nome,
 * e quem for ligar um ao outro precisa saber disso.
 */
public final class NishiResultSetMetaData implements ResultSetMetaData {

    private final List<Molduras.Coluna> colunas;
    private final String dataset;

    NishiResultSetMetaData(List<Molduras.Coluna> colunas, String dataset) {
        this.colunas = colunas;
        this.dataset = dataset;
    }

    @Override
    public int getColumnCount() {
        return colunas.size();
    }

    @Override
    public boolean isAutoIncrement(int coluna) throws SQLException {
        de(coluna);
        return false;
    }

    /**
     * Texto é sensível a caixa; número, data e binário não são.
     *
     * <p>OTHER responde true porque a coluna pode vir a ser texto — e anunciar
     * false faria uma ferramenta comparar sem caixa um valor que o servidor
     * compara com caixa.
     */
    @Override
    public boolean isCaseSensitive(int coluna) throws SQLException {
        String jdbc = de(coluna).jdbc();
        return "VARCHAR".equals(jdbc) || "OTHER".equals(jdbc);
    }

    @Override
    public boolean isSearchable(int coluna) throws SQLException {
        return TiposJdbc.pesquisavel(de(coluna).type());
    }

    @Override
    public boolean isCurrency(int coluna) throws SQLException {
        de(coluna);
        return false;
    }

    /**
     * Vem do fio, e é verdade estreita: só a chave e a sequence são não-nuláveis
     * — são as duas colunas que o materializer garante recusando o evento que as
     * traga ausentes ou nulas.
     */
    @Override
    public int isNullable(int coluna) throws SQLException {
        return de(coluna).nullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int coluna) throws SQLException {
        return TiposJdbc.assinado(de(coluna).jdbc());
    }

    @Override
    public int getColumnDisplaySize(int coluna) throws SQLException {
        return de(coluna).displaySize();
    }

    @Override
    public String getColumnLabel(int coluna) throws SQLException {
        return de(coluna).name();
    }

    @Override
    public String getColumnName(int coluna) throws SQLException {
        return de(coluna).name();
    }

    /** O NishiDB não tem schema nem catálogo: vazio é a resposta honesta. */
    @Override
    public String getSchemaName(int coluna) throws SQLException {
        de(coluna);
        return "";
    }

    @Override
    public int getPrecision(int coluna) throws SQLException {
        return de(coluna).precision();
    }

    @Override
    public int getScale(int coluna) throws SQLException {
        return de(coluna).scale();
    }

    @Override
    public String getTableName(int coluna) throws SQLException {
        de(coluna);
        return dataset;
    }

    @Override
    public String getCatalogName(int coluna) throws SQLException {
        de(coluna);
        return "";
    }

    @Override
    public int getColumnType(int coluna) throws SQLException {
        return TiposJdbc.codigoDe(de(coluna).jdbc());
    }

    /**
     * O nome do tipo do PRODUTO — {@code ipv4}, {@code json}, {@code datetime}
     * —, e não o nome JDBC.
     *
     * <p>É o que o javadoc pede ("database-specific type name") e é o único
     * lugar em que a ferramenta consegue ver a diferença entre um
     * {@code timestamp} e um {@code datetime}, que viajam com o mesmo tipo JDBC
     * porque são o mesmo instante depois da ingestão.
     */
    @Override
    public String getColumnTypeName(int coluna) throws SQLException {
        return de(coluna).type();
    }

    @Override
    public boolean isReadOnly(int coluna) throws SQLException {
        de(coluna);
        return true;
    }

    @Override
    public boolean isWritable(int coluna) throws SQLException {
        de(coluna);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int coluna) throws SQLException {
        de(coluna);
        return false;
    }

    /**
     * A classe que {@code getObject} devolve.
     *
     * <p>Para OTHER é {@code java.lang.Object}, e é a única resposta possível:
     * a coluna não tem tipo fixado, então o que sai depende do token de cada
     * linha. Anunciar uma classe concreta faria a ferramenta fazer um cast que
     * quebraria na primeira linha de tipo diferente.
     */
    @Override
    public String getColumnClassName(int coluna) throws SQLException {
        return TiposJdbc.classeDe(de(coluna).jdbc());
    }

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("este ResultSetMetaData nao embrulha " + alvo.getName(),
                SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    /** Traduz o índice 1-based do JDBC, recusando fora de faixa com o número que veio. */
    private Molduras.Coluna de(int coluna) throws SQLException {
        if (coluna < 1 || coluna > colunas.size()) {
            throw new SQLException("coluna " + coluna + " fora de [1," + colunas.size() + "]",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        return colunas.get(coluna - 1);
    }
}
