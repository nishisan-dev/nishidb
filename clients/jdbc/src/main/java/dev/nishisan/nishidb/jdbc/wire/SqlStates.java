package dev.nishisan.nishidb.jdbc.wire;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.Map;

/**
 * Traduz o código de erro do protocolo em {@link SQLException} — SQLState E
 * subclasse.
 *
 * <p>A subclasse importa tanto quanto os cinco caracteres. É por ela que um
 * pool de conexões decide descartar a conexão, e que uma aplicação decide
 * tentar de novo: {@link SQLTransientException} diz "repita", e
 * {@link SQLNonTransientException} diz "repetir não adianta".
 *
 * <p>O agrupamento segue o mesmo critério da tabela de exit codes do CLI: o que
 * o CHAMADOR FAZ diante do erro. Não é o critério "parece sintaxe, vai para a
 * classe 42" — três decisões abaixo quebram essa regra mecânica de propósito, e
 * estão nomeadas.
 *
 * <p>A cobertura é cobrada dos DOIS lados. Do lado Go,
 * internal/gates/jdbc_test.go lê as constantes do protocolo e falha se alguma
 * não aparecer aqui — código novo em Go sem SQLState reprova no CI Go, antes de
 * qualquer JVM subir.
 */
public final class SqlStates {

    // ---- classe 08: conexão
    /** 08001 — o cliente não conseguiu estabelecer a conexão. */
    public static final String CONEXAO_NAO_ESTABELECIDA = "08001";
    /** 08006 — a conexão terminou no meio. */
    public static final String CONEXAO_INTERROMPIDA = "08006";
    /** 08P01 — violação de protocolo: o driver falou errado, ou não é o servidor esperado. */
    public static final String PROTOCOLO_VIOLADO = "08P01";

    // ---- classe 22: o dado em si
    //
    // Os três abaixo são PÚBLICOS porque não vêm do fio: nascem na conversão
    // do lado do cliente, quando a aplicação pede de uma coluna um tipo que o
    // valor não representa. O SQLState continua sendo o mesmo vocabulário — é
    // por ele que uma ferramenta distingue "o dado não cabe" de "a consulta
    // está errada", e inventar um código próprio para o caminho do cliente
    // criaria duas linguagens para o mesmo assunto.
    /** 22003 — o valor não cabe no tipo pedido (overflow, ou fração onde só cabe inteiro). */
    public static final String VALOR_FORA_DE_FAIXA = "22003";
    /** 22018 — o valor não tem a forma do tipo pedido. */
    public static final String CONVERSAO_INVALIDA = "22018";
    /** 22007 — texto que deveria ser data/hora e não é. */
    public static final String FORMATO_TEMPORAL_INVALIDO = "22007";

    /** 24000 — não há linha corrente, ou o ResultSet já foi fechado. */
    public static final String CURSOR_INVALIDO = "24000";

    /** 0A000 — o driver não implementa este método ou esta conversão. */
    public static final String NAO_SUPORTADO = "0A000";

    /**
     * 3F000 — o schema da conexão não existe no servidor, ou a aplicação pediu
     * outro que a URL não fixou.
     */
    public static final String INVALID_SCHEMA_NAME = "3F000";

    /** XX000 — defeito do próprio driver, não do servidor nem da aplicação. */
    public static final String INTERNO = "XX000";

    // ---- os demais, na tabela abaixo
    private static final String SINTAXE = "42601";
    private static final String BIND = "42000";
    private static final String TABELA_INEXISTENTE = "42S02";
    private static final String SEM_PRIVILEGIO = "42501";
    private static final String DADO_INVALIDO = "22023";
    private static final String RECURSO_EXCEDIDO = "53400";
    private static final String RECURSO_INSUFICIENTE = "53000";
    private static final String LIMITE_DE_PROGRAMA = "54000";
    /**
     * 57014 — a consulta passou do prazo e foi cancelada.
     *
     * <p>PÚBLICO porque nasce também no cliente: quando o servidor não
     * responde até o {@code queryTimeout} mais a folga, o driver desiste com
     * este MESMO estado, e a aplicação trata os dois estouros da mesma forma.
     */
    public static final String CANCELADO = "57014";
    private static final String OBJETO_EM_USO = "55006";
    private static final String AUTENTICACAO = "28000";
    private static final String IO_DO_SISTEMA = "58030";
    private static final String OBJETO_DUPLICADO = "42710";
    private static final String OBJETO_INEXISTENTE = "42704";
    private static final String PRE_REQUISITO_DESATENDIDO = "55000";
    private static final String CORRUPCAO = "XX001";
    /** HY000 — código que este driver não conhece. Nunca degrada para INTERNAL. */
    public static final String DESCONHECIDO = "HY000";

    /** Como construir a exceção de uma família. */
    private interface Fabrica {
        SQLException nova(String mensagem, String estado);
    }

    private record Entrada(String estado, Fabrica fabrica) {}

    private static final Map<String, Entrada> TABELA = Map.ofEntries(
            // Erros de QUERY: o que o chamador faz é reescrever o SQL.
            Map.entry(Codigos.SYNTAX, new Entrada(SINTAXE, SQLSyntaxErrorException::new)),
            Map.entry(Codigos.BIND, new Entrada(BIND, SQLSyntaxErrorException::new)),
            Map.entry(Codigos.RESERVED_COLUMN, new Entrada(BIND, SQLSyntaxErrorException::new)),
            Map.entry(Codigos.UNKNOWN_DATASET, new Entrada(TABELA_INEXISTENTE, SQLSyntaxErrorException::new)),
            Map.entry(Codigos.FOLLOWER_NOT_READY, new Entrada(RECURSO_INSUFICIENTE, SQLTransientException::new)),
            // O schema da conexão (o caminho da URL) que o servidor não tem. É
            // a classe 3F (invalid schema name), e não 42S02: não é a tabela do
            // SQL que falta, é o endereço da própria conexão.
            Map.entry(Codigos.UNKNOWN_SCHEMA, new Entrada(INVALID_SCHEMA_NAME, SQLNonTransientException::new)),

            // A cláusula ausente é FEATURE, não sintaxe: é a distinção inteira
            // que o código UNSUPPORTED_CLAUSE veio criar do lado do servidor, e
            // publicá-la como 42601 a jogaria fora no último metro.
            Map.entry(Codigos.UNSUPPORTED_CLAUSE,
                    new Entrada(NAO_SUPORTADO, SQLFeatureNotSupportedException::new)),

            // Dado que a aplicação forneceu e o servidor não aceita.
            Map.entry(Codigos.BAD_PARAMETER, new Entrada(DADO_INVALIDO, SQLDataException::new)),
            Map.entry(Codigos.MAX_ROWS_ABOVE_LIMIT, new Entrada(DADO_INVALIDO, SQLDataException::new)),

            // Tetos do servidor. TOO_MANY_QUERIES e COMPACTION_BUSY são
            // TRANSIENT — repetir resolve — e MAX_ROWS_EXCEEDED não é: o
            // resultado não vai encolher sozinho.
            Map.entry(Codigos.MAX_ROWS_EXCEEDED, new Entrada(RECURSO_EXCEDIDO, SQLNonTransientException::new)),
            Map.entry(Codigos.TOO_MANY_QUERIES, new Entrada(RECURSO_INSUFICIENTE, SQLTransientException::new)),

            // TOO_MANY_LONG_QUERIES divide o SQLState com TOO_MANY_QUERIES —
            // 53000 é "recurso insuficiente", e é o que os dois são — mas fica
            // como entrada PRÓPRIA porque a mensagem que viaja é outra: o pool
            // geral esvazia em milissegundos e o de consulta longa pode levar
            // minutos. Continua TRANSIENT: repetir resolve, só que mais tarde.
            Map.entry(Codigos.TOO_MANY_LONG_QUERIES,
                    new Entrada(RECURSO_INSUFICIENTE, SQLTransientException::new)),
            Map.entry(Codigos.COMPACTION_BUSY, new Entrada(OBJETO_EM_USO, SQLTransientException::new)),
            Map.entry(Codigos.REQUEST_TOO_LARGE, new Entrada(LIMITE_DE_PROGRAMA, SQLNonTransientException::new)),

            // ROW_TOO_LARGE divide o 54000 com REQUEST_TOO_LARGE: os dois são
            // "não coube num limite deste programa", um na entrada e outro na
            // saída. NÃO é transient — repetir a mesma consulta produz a mesma
            // linha grande —, e o que resolve é projetar menos colunas ou o
            // operador subir runtime.query.maxBufferPerQuery.
            Map.entry(Codigos.ROW_TOO_LARGE, new Entrada(LIMITE_DE_PROGRAMA, SQLNonTransientException::new)),

            // SNAPSHOT_TOO_OLD é TRANSIENT, e é a única entrada desta família
            // em que repetir de fato resolve: a consulta nova abre um snapshot
            // atual, que nada retém ainda. Ela cai em 53000 — recurso
            // insuficiente — porque foi memória que faltou, e não um limite do
            // programa como o ROW_TOO_LARGE ao lado: o servidor não conseguiu
            // guardar o passado por tempo suficiente, e o resultado que ele
            // teria de entregar não existe mais.
            Map.entry(Codigos.SNAPSHOT_TOO_OLD,
                    new Entrada(RECURSO_INSUFICIENTE, SQLTransientException::new)),

            // TIMEOUT ganha a única subclasse que o JDBC dedica a um caso
            // nosso, e é o que faz uma ferramenta oferecer "tentar de novo com
            // mais tempo" em vez de mostrar erro genérico.
            Map.entry(Codigos.TIMEOUT, new Entrada(CANCELADO, SQLTimeoutException::new)),

            // Autenticação e autorização.
            Map.entry(Codigos.UNAUTHENTICATED,
                    new Entrada(AUTENTICACAO, SQLInvalidAuthorizationSpecException::new)),
            // FORBIDDEN quebra de propósito a regra "classe 42 vira
            // SQLSyntaxErrorException": papel insuficiente não é erro de
            // sintaxe, e uma ferramenta que tratasse assim sugeriria corrigir o
            // SQL para quem precisa é de outra credencial.
            Map.entry(Codigos.FORBIDDEN, new Entrada(SEM_PRIVILEGIO, SQLNonTransientException::new)),

            // Gestão de usuário. O driver é só leitura e nunca chama essas
            // rotas, mas o código pode chegar a ele por qualquer caminho que
            // repasse a resposta, e a tabela cobre o protocolo inteiro. Os
            // três da classe 42/55 quebram a regra "classe 42 vira
            // SQLSyntaxErrorException" pela mesma razão do FORBIDDEN: nada ali
            // se resolve reescrevendo SQL.
            Map.entry(Codigos.USER_EXISTS, new Entrada(OBJETO_DUPLICADO, SQLNonTransientException::new)),
            Map.entry(Codigos.UNKNOWN_USER, new Entrada(OBJETO_INEXISTENTE, SQLNonTransientException::new)),
            Map.entry(Codigos.LAST_ADMIN,
                    new Entrada(PRE_REQUISITO_DESATENDIDO, SQLNonTransientException::new)),
            Map.entry(Codigos.WEAK_PASSWORD, new Entrada(DADO_INVALIDO, SQLDataException::new)),

            // As três "o driver falou errado o protocolo" viram classe 08, e
            // não 42: o defeito é do CLIENTE ou do caminho de rede, não da
            // consulta. Um 405 ou um media type recusado significa que quem
            // respondeu não é o servidor que esperávamos — um proxy, por
            // exemplo.
            Map.entry(Codigos.BAD_REQUEST, new Entrada(PROTOCOLO_VIOLADO, SQLNonTransientConnectionException::new)),
            Map.entry(Codigos.METHOD_NOT_ALLOWED,
                    new Entrada(PROTOCOLO_VIOLADO, SQLNonTransientConnectionException::new)),
            Map.entry(Codigos.UNSUPPORTED_MEDIA_TYPE,
                    new Entrada(PROTOCOLO_VIOLADO, SQLNonTransientConnectionException::new)),

            // Falhas do servidor.
            Map.entry(Codigos.DATASET_CORRUPT, new Entrada(CORRUPCAO, SQLNonTransientException::new)),
            Map.entry(Codigos.WRITER_FAILED, new Entrada(IO_DO_SISTEMA, SQLNonTransientException::new)),
            Map.entry(Codigos.INTERNAL, new Entrada(INTERNO, SQLNonTransientException::new)));

    private SqlStates() {
    }

    /** O SQLState do código, ou HY000 quando o código não é conhecido. */
    public static String estadoDe(String codigo) {
        Entrada e = TABELA.get(codigo);
        return e == null ? DESCONHECIDO : e.estado();
    }

    /**
     * Constrói a exceção do código, já com a subclasse certa.
     *
     * <p>Código DESCONHECIDO vira HY000 com o código cru na mensagem, e jamais
     * degrada para INTERNAL: um servidor mais novo que este driver vai emitir
     * códigos que ele não conhece, e apagar a novidade transformaria "há um
     * caso novo aqui" em "deu erro interno" — que manda procurar no lugar
     * errado.
     */
    public static SQLException excecaoDe(String codigo, String mensagem) {
        Entrada e = TABELA.get(codigo);
        if (e == null) {
            return new SQLException(mensagem + " [codigo do servidor: " + codigo + "]", DESCONHECIDO);
        }
        return e.fabrica().nova(mensagem, e.estado());
    }

    /** Se o driver conhece este código. Usado pelo teste de cobertura. */
    public static boolean conhece(String codigo) {
        return TABELA.containsKey(codigo);
    }
}
