package dev.nishisan.nishidb.jdbc.wire;

import java.sql.Types;
import java.util.Map;

/**
 * Traduz o nome de tipo que o fio publica para o vocabulário do
 * {@code java.sql}.
 *
 * <p>A linha de schema traz o nome ({@code "BIGINT"}, {@code "TIMESTAMP"}), e
 * quem precisa do inteiro é o {@code ResultSetMetaData}. A tabela mora aqui, e
 * não espalhada pelo ResultSet, pela mesma razão que a de SQLState: um gate do
 * lado Go lê as constantes de internal/protocol/jdbc.go e cobra que cada nome
 * apareça NESTE arquivo, antes de qualquer JVM subir.
 *
 * <p><b>Nome desconhecido vira OTHER, e nunca exceção.</b> Um servidor mais
 * novo pode publicar um tipo que este jar não conhece; OTHER é a resposta
 * honesta — não sabemos o que é, e {@code getString} continua funcionando —,
 * enquanto uma exceção quebraria a consulta inteira por causa de uma coluna que
 * talvez ninguém leia. É a mesma doutrina do HY000 para código de erro que o
 * driver não conhece: degradar sem apagar a novidade.
 */
public final class TiposJdbc {

    private record Entrada(int codigo, String classe, boolean assinado) {}

    private static final Entrada DESCONHECIDO = new Entrada(Types.OTHER, "java.lang.Object", false);

    private static final Map<String, Entrada> TABELA = Map.of(
            "VARCHAR", new Entrada(Types.VARCHAR, "java.lang.String", false),
            "BIGINT", new Entrada(Types.BIGINT, "java.lang.Long", true),
            "DOUBLE", new Entrada(Types.DOUBLE, "java.lang.Double", true),
            "BOOLEAN", new Entrada(Types.BOOLEAN, "java.lang.Boolean", false),
            "TIMESTAMP", new Entrada(Types.TIMESTAMP, "java.sql.Timestamp", false),
            "TIME", new Entrada(Types.TIME, "java.sql.Time", false),
            // "[B" é o nome binário de byte[], que é o que getColumnClassName
            // tem de devolver: Class.forName("byte[]") não resolve, e uma
            // ferramenta que instancie a classe pelo nome falharia.
            "VARBINARY", new Entrada(Types.VARBINARY, "[B", false),
            "OTHER", DESCONHECIDO);

    /**
     * Os dois tipos do PRODUTO sobre os quais o binder recusa comparação.
     *
     * <p>A pergunta é sobre o tipo do produto e não sobre o nome JDBC: os dois
     * viajam como VARCHAR/VARBINARY, e por esse nome são indistinguíveis de uma
     * string comum. Uma ferramenta que os anunciasse pesquisáveis montaria um
     * WHERE que o servidor recusa — e o usuário culparia a ferramenta.
     *
     * <p>Um gate do lado Go amarra estes dois literais ao nome que
     * protocolo.NomeDeTipo produz, para que renomear o tipo lá não os
     * ressuscite como pesquisáveis aqui.
     */
    private static final java.util.Set<String> NAO_PESQUISAVEIS = java.util.Set.of("bytes", "json");

    private TiposJdbc() {
    }

    /** O código de {@link Types} do nome, ou {@link Types#OTHER}. */
    public static int codigoDe(String nomeJdbc) {
        return entrada(nomeJdbc).codigo();
    }

    /** O nome da classe que {@code getObject} devolve para o tipo. */
    public static String classeDe(String nomeJdbc) {
        return entrada(nomeJdbc).classe();
    }

    /** Se o tipo tem sinal. Só os dois numéricos têm. */
    public static boolean assinado(String nomeJdbc) {
        return entrada(nomeJdbc).assinado();
    }

    /** Se este driver conhece o nome. Usado pelo teste de cobertura. */
    public static boolean conhece(String nomeJdbc) {
        return nomeJdbc != null && TABELA.containsKey(nomeJdbc);
    }

    /**
     * Se uma coluna do tipo do PRODUTO informado pode aparecer num WHERE.
     *
     * @param nomeDoTipo o campo {@code type} do fio, não o {@code jdbc}
     */
    public static boolean pesquisavel(String nomeDoTipo) {
        return !NAO_PESQUISAVEIS.contains(nomeDoTipo);
    }

    private static Entrada entrada(String nomeJdbc) {
        Entrada e = nomeJdbc == null ? null : TABELA.get(nomeJdbc);
        return e == null ? DESCONHECIDO : e;
    }
}
