package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonException;
import dev.nishisan.nishidb.jdbc.json.JsonScanner;
import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * O cursor sobre linhas que o próprio driver montou.
 *
 * <p>É a fonte das tabelas sintéticas do {@code DatabaseMetaData} — a lista de
 * tabelas, a de colunas, a chave primária —, que nascem do documento de
 * {@code /v1/metadata} e não de um stream. Existir aqui, falando o mesmo
 * vocabulário do {@link StreamCursor}, é o que permite ao driver ter UM
 * ResultSet: sem isso, o do metadata seria uma segunda implementação, e uma
 * correção de conversão valeria só num dos dois.
 *
 * <p>Sem streaming e sem preguiça de propósito: estas listas são pequenas e
 * conhecidas por inteiro antes de a primeira linha ser pedida.
 */
public final class ArrayCursor implements RowCursor {

    private final List<Molduras.Coluna> colunas;
    private final List<Object[]> linhas;
    private int atual = -1;

    public ArrayCursor(List<Molduras.Coluna> colunas, List<Object[]> linhas) {
        this.colunas = List.copyOf(colunas);
        this.linhas = List.copyOf(linhas);
    }

    @Override
    public List<Molduras.Coluna> colunas() {
        return colunas;
    }

    /**
     * Vazio: as linhas do DatabaseMetaData descrevem datasets, não saem de um.
     *
     * <p>É o que faz {@code getTableName()} de um ResultSet de metadados sair
     * em branco em vez de nomear uma tabela que não foi consultada.
     */
    @Override
    public String dataset() {
        return "";
    }

    @Override
    public boolean proxima() {
        if (atual + 1 >= linhas.size()) {
            return false;
        }
        atual++;
        return true;
    }

    @Override
    public JsonTipo tipo(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v == null) {
            return JsonTipo.NULO;
        }
        if (v instanceof String) {
            return JsonTipo.TEXTO;
        }
        if (v instanceof Boolean) {
            return JsonTipo.BOOLEANO;
        }
        if (v instanceof Number) {
            return JsonTipo.NUMERO;
        }
        // Quem monta estas linhas é o próprio driver, então um valor de outro
        // tipo é defeito NOSSO. Convertê-lo por String.valueOf esconderia o
        // defeito atrás de um dado plausível, que é a pior forma de bug: a
        // aplicação veria "java.lang.Object@1f2a" numa coluna e culparia o
        // servidor.
        throw new SQLException("valor de tipo inesperado no cursor sintetico: "
                + v.getClass().getName(), SqlStates.INTERNO);
    }

    @Override
    public String texto(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v == null) {
            return null;
        }
        tipo(coluna); // recusa o tipo inesperado antes de formatá-lo
        return String.valueOf(v);
    }

    @Override
    public long inteiro(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v instanceof Long l) {
            return l;
        }
        if (v instanceof Integer i) {
            return i;
        }
        if (v instanceof Short s) {
            return s;
        }
        if (v instanceof Byte b) {
            return b;
        }
        // Double e texto passam pela mesma gramática do stream, para que o
        // valor com fração seja recusado dos dois lados com a mesma mensagem.
        String t = texto(coluna);
        try {
            byte[] b = t == null ? new byte[0] : t.getBytes(StandardCharsets.US_ASCII);
            return JsonScanner.inteiro(b, 0, b.length);
        } catch (JsonException e) {
            throw new SQLDataException("coluna " + colunas.get(coluna).name() + ": " + e.getMessage(),
                    SqlStates.VALOR_FORA_DE_FAIXA);
        }
    }

    @Override
    public double numero(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        String t = texto(coluna);
        try {
            byte[] b = t == null ? new byte[0] : t.getBytes(StandardCharsets.US_ASCII);
            return JsonScanner.numero(b, 0, b.length);
        } catch (JsonException e) {
            throw new SQLDataException("coluna " + colunas.get(coluna).name() + ": " + e.getMessage(),
                    SqlStates.CONVERSAO_INVALIDA);
        }
    }

    @Override
    public boolean booleano(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v instanceof Boolean b) {
            return b;
        }
        throw new SQLDataException("coluna " + colunas.get(coluna).name() + ": "
                + texto(coluna) + " nao e um booleano", SqlStates.CONVERSAO_INVALIDA);
    }

    @Override
    public boolean integral(int coluna) throws SQLException {
        Object v = valor(coluna);
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return true;
        }
        if (v instanceof Number) {
            return false;
        }
        String t = texto(coluna);
        return t != null && t.indexOf('.') < 0 && t.indexOf('e') < 0 && t.indexOf('E') < 0;
    }

    /** Nunca há aviso: estas linhas não vêm de um stream que possa ser cortado. */
    @Override
    public SQLWarning aviso() {
        return null;
    }

    @Override
    public void close() {
        // Nada a soltar: não há stream nem conexão por trás destas linhas.
    }

    private Object valor(int coluna) throws SQLException {
        if (atual < 0 || atual >= linhas.size()) {
            throw new SQLException("nenhuma linha corrente", SqlStates.INTERNO);
        }
        Object[] linha = linhas.get(atual);
        if (coluna < 0 || coluna >= linha.length) {
            throw new SQLException("a linha sintetica tem " + linha.length + " valores para "
                    + colunas.size() + " colunas", SqlStates.INTERNO);
        }
        return linha[coluna];
    }
}
