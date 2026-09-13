package dev.nishisan.nishidb.jdbc.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SqlStatesTest {

    /** Todos os códigos declarados em Codigos, por reflexão — nada escrito à mão. */
    private static List<String> todosOsCodigos() throws IllegalAccessException {
        List<String> out = new ArrayList<>();
        for (Field f : Codigos.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType() == String.class) {
                out.add((String) f.get(null));
            }
        }
        return out;
    }

    /**
     * Nenhum código declarado fica sem tradução.
     *
     * <p>É o espelho em Java do gate que roda do lado Go. Os dois existem
     * porque fecham buracos diferentes: o de Go pega o código novo que o
     * SERVIDOR ganhou, este pega a constante que alguém acrescentou AQUI e
     * esqueceu de pôr na tabela.
     */
    @Test
    void todoCodigoDeclaradoTemEntradaNaTabela() throws IllegalAccessException {
        List<String> semTraducao = new ArrayList<>();
        for (String c : todosOsCodigos()) {
            if (!SqlStates.conhece(c)) {
                semTraducao.add(c);
            }
        }
        assertEquals(List.of(), semTraducao, "codigos sem SQLState");
    }

    /** SQLState tem cinco caracteres. Um de quatro faz ferramenta classificar errado. */
    @Test
    void todoSqlStateTemCincoCaracteres() throws IllegalAccessException {
        for (String c : todosOsCodigos()) {
            String estado = SqlStates.estadoDe(c);
            assertEquals(5, estado.length(), () -> c + " -> " + estado);
        }
    }

    /**
     * A subclasse é o que uma aplicação usa para decidir se repete.
     *
     * <p>Um pool de conexões descarta a conexão diante de
     * SQLNonTransientConnectionException e a devolve ao pool diante de
     * SQLTransientException. Publicar SQLException crua em tudo tiraria essa
     * decisão de quem a toma.
     */
    @Test
    void aSubclasseDizOQueOChamadorDeveFazer() {
        assertInstanceOf(SQLFeatureNotSupportedException.class,
                SqlStates.excecaoDe(Codigos.UNSUPPORTED_CLAUSE, "ORDER BY"));
        assertInstanceOf(SQLDataException.class, SqlStates.excecaoDe(Codigos.BAD_PARAMETER, "x"));
        assertInstanceOf(SQLTimeoutException.class, SqlStates.excecaoDe(Codigos.TIMEOUT, "x"));
        assertInstanceOf(SQLInvalidAuthorizationSpecException.class,
                SqlStates.excecaoDe(Codigos.UNAUTHENTICATED, "x"));
        assertInstanceOf(SQLTransientException.class, SqlStates.excecaoDe(Codigos.TOO_MANY_QUERIES, "x"));
        assertInstanceOf(SQLTransientException.class, SqlStates.excecaoDe(Codigos.COMPACTION_BUSY, "x"));
        assertInstanceOf(SQLNonTransientConnectionException.class,
                SqlStates.excecaoDe(Codigos.METHOD_NOT_ALLOWED, "x"));
    }

    /**
     * A cláusula ausente é FEATURE, não sintaxe.
     *
     * <p>Publicá-la como 42601 jogaria fora, no último metro, a distincao
     * inteira que o servidor criou ao separar UNSUPPORTED_CLAUSE de SYNTAX.
     */
    @Test
    void clausulaAusenteNaoEhSintaxe() {
        assertEquals("0A000", SqlStates.estadoDe(Codigos.UNSUPPORTED_CLAUSE));
        assertEquals("42601", SqlStates.estadoDe(Codigos.SYNTAX));
    }

    /**
     * O schema desconhecido é a classe 3F (invalid schema name), e não 42S02:
     * não é a tabela do SQL que falta, é o endereço da própria conexão.
     */
    @Test
    void schemaDesconhecidoEhDaClasse3F() {
        assertEquals("3F000", SqlStates.estadoDe(Codigos.UNKNOWN_SCHEMA));
        assertEquals(SqlStates.INVALID_SCHEMA_NAME, SqlStates.estadoDe(Codigos.UNKNOWN_SCHEMA));
    }

    /**
     * Codigo desconhecido vira HY000 COM O CODIGO NA MENSAGEM, e jamais degrada
     * para INTERNAL.
     *
     * <p>Um servidor mais novo que este driver vai emitir codigos que ele nao
     * conhece. Apagar a novidade transformaria "ha um caso novo aqui" em "deu
     * erro interno", que manda procurar no lugar errado.
     */
    @Test
    void codigoDesconhecidoPreservaOCodigoCru() {
        SQLException e = SqlStates.excecaoDe("CODIGO_DO_FUTURO", "algo aconteceu");
        assertEquals("HY000", e.getSQLState());
        assertTrue(e.getMessage().contains("CODIGO_DO_FUTURO"), e.getMessage());
        assertTrue(e.getMessage().contains("algo aconteceu"), e.getMessage());
    }

    @Test
    void aMensagemDoServidorChegaInteiraAoChamador() {
        SQLException e = SqlStates.excecaoDe(Codigos.SYNTAX, "erro de sintaxe na posicao 7");
        assertEquals("erro de sintaxe na posicao 7", e.getMessage());
        assertEquals("42601", e.getSQLState());
    }
}
