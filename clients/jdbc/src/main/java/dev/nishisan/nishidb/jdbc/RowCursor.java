package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * A fonte de linhas de um {@link NishiResultSet} — e a razão de existir UM
 * ResultSet só.
 *
 * <p>O driver tem duas fontes de linhas que não se parecem em nada: o stream
 * NDJSON de uma consulta ({@link StreamCursor}) e as tabelas sintéticas do
 * DatabaseMetaData ({@link ArrayCursor}), que nascem de listas em memória. Sem
 * esta SPI, cada uma teria o seu ResultSet — e uma correção de conversão
 * passaria a valer só num dos dois, com o sintoma de {@code getTimestamp}
 * funcionar na consulta e errar o fuso na listagem de colunas.
 *
 * <p><b>O contrato é de TOKEN, não de tipo declarado.</b> O cursor diz o que o
 * valor É no fio ({@link #tipo}) e entrega o valor cru; quem decide o que fazer
 * com ele — e quem sabe que a coluna foi declarada {@code timestamp} — é o
 * ResultSet. A divisão existe porque a coluna {@code auto} que nenhum evento
 * fixou é anunciada OTHER no schema e pode trazer qualquer token na linha: um
 * cursor que decidisse pelo tipo declarado não teria resposta para ela.
 *
 * <p>Os índices de coluna são <b>0-based</b>, ao contrário dos do JDBC. A
 * tradução acontece num lugar só, na fronteira do ResultSet, e é lá que o
 * índice fora de faixa é recusado com o número 1-based que a aplicação usou.
 */
public interface RowCursor extends AutoCloseable {

    /** As colunas, na ordem da projeção. Disponível ANTES da primeira linha. */
    List<Molduras.Coluna> colunas();

    /** O dataset a que as linhas pertencem, ou vazio para um cursor sintético. */
    String dataset();

    /**
     * Avança para a próxima linha. {@code false} quando não há mais.
     *
     * <p>É aqui que o desfecho do stream é cobrado: um {@code false} só sai
     * quando a resposta terminou bem. Erro do servidor no fecho vira exceção
     * NESTA chamada — jamais um {@code false}, que apresentaria um resultado
     * parcial como completo.
     */
    boolean proxima() throws SQLException;

    /** O tipo do token da coluna na linha corrente. */
    JsonTipo tipo(int coluna) throws SQLException;

    /**
     * O texto do valor como ele está no fio, com os escapes desfeitos, ou
     * {@code null} quando o token é nulo.
     *
     * <p>Para número, booleano e documento json inline é a grafia literal que o
     * servidor emitiu — é o que torna {@code getString} lossless e o que
     * resolve o problema de fuso sem mentir: a aplicação vê exatamente o que o
     * servidor escreveu.
     */
    String texto(int coluna) throws SQLException;

    /**
     * O valor como {@code long} EXATO, sem passar por {@code double}.
     *
     * <p>Serve tanto ao token de número quanto ao de texto que contenha um
     * inteiro. Overflow é exceção, nunca um valor errado em silêncio.
     */
    long inteiro(int coluna) throws SQLException;

    /** O valor como {@code double}, com a gramática validada antes da conversão. */
    double numero(int coluna) throws SQLException;

    /** O valor como {@code boolean}. Só aceita a grafia do fio: true ou false. */
    boolean booleano(int coluna) throws SQLException;

    /**
     * Se o número da coluna é escrito SEM fração e SEM expoente.
     *
     * <p>Existe para a coluna OTHER, a única cujo tipo o schema não fixa: é
     * este bit que decide se {@code getObject} devolve Long ou Double. A
     * pergunta é sobre a GRAFIA, e não sobre o valor — {@code 1e3} é inteiro em
     * valor e não é integral aqui, porque quem o escreveu assim escreveu um
     * double.
     */
    boolean integral(int coluna) throws SQLException;

    /**
     * O aviso acumulado, ou {@code null}.
     *
     * <p>Só um caso o preenche hoje, e é decisão de produto: o teto de linhas
     * do DATASET (MAX_ROWS_EXCEEDED) chega como fecho de erro e vira aviso, não
     * exceção.
     */
    SQLWarning aviso();

    @Override
    void close() throws SQLException;
}
