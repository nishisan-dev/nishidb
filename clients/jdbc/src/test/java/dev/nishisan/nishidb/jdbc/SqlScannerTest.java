package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SqlScannerTest {

    private static int placeholders(String sql) {
        return SqlScanner.de(sql).placeholders();
    }

    /** A contagem de '?' é a que o PreparedStatement usa para validar índice. */
    @Test
    void contaOsPlaceholdersForaDeLiteral() {
        assertEquals(0, placeholders("SELECT * FROM t"));
        assertEquals(3, placeholders("SELECT * FROM t WHERE a = ? AND b = ? OR c = ?"));
        assertEquals(0, placeholders(""));
    }

    /**
     * '?' dentro de literal NÃO é placeholder.
     *
     * <p>A contagem errada faria o PreparedStatement aceitar um setString em
     * índice que o servidor não tem, e o erro sairia como BIND no meio da
     * execução.
     */
    @Test
    void placeholderDentroDeLiteralNaoConta() {
        assertEquals(0, placeholders("SELECT * FROM t WHERE s = '?'"));
        assertEquals(0, placeholders("SELECT \"a?b\" FROM t"));
        assertEquals(1, placeholders("SELECT * FROM t WHERE s = 'a''?''b' AND x = ?"));
        assertEquals(1, placeholders("SELECT \"a\"\"?\" FROM t WHERE x = ?"));
    }

    /**
     * Literal sem fechamento não faz o scanner quebrar nem inventar.
     *
     * <p>Quem recusa SQL inválido é o servidor, com posição de byte. Um scanner
     * que lançasse aqui trocaria a mensagem boa do servidor por uma do cliente.
     */
    @Test
    void literalSemFechamentoNaoQuebraOScanner() {
        assertEquals(0, placeholders("SELECT * FROM t WHERE s = 'abc ?"));
        assertEquals(0, placeholders("SELECT * FROM \"abc ?"));
    }

    /**
     * A contagem de '?' não muda em nenhuma das formas de escrever o nome da
     * tabela — simples, qualificado, com cada parte citada ou citado inteiro.
     */
    @Test
    void aContagemDePlaceholdersNaoMudaComAFormaDoNome() {
        String cauda = " WHERE a = ? AND b = ?";
        assertEquals(2, placeholders("SELECT * FROM _meta.counters" + cauda));
        assertEquals(2, placeholders("SELECT * FROM \"_meta\".counters" + cauda));
        assertEquals(2, placeholders("SELECT * FROM \"telecom\".\"service_view\"" + cauda));
        assertEquals(2, placeholders("SELECT * FROM \"_meta.counters\"" + cauda));
        assertEquals(2, placeholders("SELECT * FROM \"eventos\"" + cauda));
    }
}
