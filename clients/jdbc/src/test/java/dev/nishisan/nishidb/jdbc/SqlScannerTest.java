package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SqlScannerTest {

    private static String dataset(String sql) {
        return SqlScanner.de(sql).dataset();
    }

    private static int placeholders(String sql) {
        return SqlScanner.de(sql).placeholders();
    }

    /** O caso que sustenta a decisão D2: o dataset sai do FROM, não da URL. */
    @Test
    void oDatasetSaiDoFrom() {
        assertEquals("eventos", dataset("SELECT * FROM eventos"));
        assertEquals("eventos", dataset("SELECT a, b FROM eventos WHERE a = 1 LIMIT 10"));
    }

    /**
     * A palavra FROM casa sem caixa, mas o NOME sai com a caixa que foi
     * escrita.
     *
     * <p>Identificadores do NishiDB são case-sensitive e o binder resolve nome
     * exato: normalizar aqui faria o servidor recusar um dataset que existe.
     */
    @Test
    void aPalavraFromCasaSemCaixaMasONomeSaiComACaixaEscrita() {
        assertEquals("Eventos", dataset("select a from Eventos where x = 1"));
        assertEquals("Eventos", dataset("SELECT a FrOm Eventos"));
    }

    /** Dataset entre aspas, com a aspa duplicada como escape (regra do lexer). */
    @Test
    void oDatasetEntreAspasSaiSemAsAspasEComOEscapeDesfeito() {
        assertEquals("meu dataset", dataset("SELECT * FROM \"meu dataset\""));
        assertEquals("a\"b", dataset("SELECT * FROM \"a\"\"b\""));
    }

    /**
     * FROM dentro de literal NÃO é a cláusula.
     *
     * <p>Sem pular os literais, o scanner leria a palavra dentro do texto e o
     * driver mandaria ao servidor um dataset que a query não nomeia — e a
     * consulta morreria no bind com uma mensagem sobre um nome que o usuário
     * não escreveu.
     */
    @Test
    void fromDentroDeLiteralNaoContaComoClausula() {
        assertEquals("t", dataset("SELECT * FROM t WHERE s = 'FROM outro'"));
        assertEquals("t", dataset("SELECT * FROM t WHERE s = 'a''FROM outro''b'"));
    }

    /** Coluna chamada "from" entre aspas é nome, não cláusula. */
    @Test
    void colunaChamadaFromEntreAspasNaoDisparaAClausula() {
        assertEquals("t", dataset("SELECT \"from\" FROM t"));
    }

    /** Só o PRIMEIRO FROM conta: a v1 não tem subquery nem JOIN. */
    @Test
    void apenasOPrimeiroFromConta() {
        assertEquals("a", dataset("SELECT * FROM a FROM b"));
    }

    /** Sem FROM não há palpite: quem responde é a URL da conexão. */
    @Test
    void queryQueNaoNomeiaDatasetDevolveNulo() {
        assertNull(dataset("SELECT 1"));
        assertNull(dataset("SELECT * FROM"));
        assertNull(dataset("SELECT * FROM 123"));
        assertNull(dataset(""));
        assertNull(dataset("   "));
    }

    /** A contagem de '?' é a que o PreparedStatement usa para validar índice. */
    @Test
    void contaOsPlaceholdersForaDeLiteral() {
        assertEquals(0, placeholders("SELECT * FROM t"));
        assertEquals(3, placeholders("SELECT * FROM t WHERE a = ? AND b = ? OR c = ?"));
    }

    /**
     * '?' dentro de literal NÃO é placeholder.
     *
     * <p>É o mesmo defeito do FROM, com sintoma pior: a contagem errada faria
     * o PreparedStatement aceitar um setString em índice que o servidor não
     * tem, e o erro sairia como BIND no meio da execução.
     */
    @Test
    void placeholderDentroDeLiteralNaoConta() {
        assertEquals(0, placeholders("SELECT * FROM t WHERE s = '?'"));
        assertEquals(0, placeholders("SELECT \"a?b\" FROM t"));
        assertEquals(1, placeholders("SELECT * FROM t WHERE s = 'a''?''b' AND x = ?"));
    }

    /**
     * Literal sem fechamento não faz o scanner quebrar nem inventar.
     *
     * <p>O scanner é um EXTRATOR DE PALPITE, nunca um validador: quem recusa
     * SQL inválido é o servidor, com posição de byte. Um scanner que lançasse
     * aqui trocaria a mensagem boa do servidor por uma do cliente.
     */
    @Test
    void literalSemFechamentoNaoQuebraOScanner() {
        assertEquals("t", dataset("SELECT * FROM t WHERE s = 'abc"));
        assertEquals(0, placeholders("SELECT * FROM t WHERE s = 'abc ?"));
        assertNull(dataset("SELECT * FROM \"abc"));
    }

    /** Espaço em branco entre FROM e o nome inclui quebra de linha e tab. */
    @Test
    void oNomeDoDatasetPodeVirDepoisDeQuebraDeLinha() {
        assertEquals("t", dataset("SELECT *\n  FROM\n\tt\nWHERE a = 1"));
        assertEquals("t", dataset("SELECT * FROM t;"));
    }

    /**
     * FROMULA não é FROM.
     *
     * <p>Casar por prefixo faria o scanner tomar o resto do identificador
     * como dataset — e o palpite errado só apareceria como erro de bind.
     */
    @Test
    void identificadorQueComecaComFromNaoEhAClausula() {
        assertEquals("t", dataset("SELECT fromage FROM t"));
        assertNull(dataset("SELECT * FROMAGE t"));
    }

    /**
     * Nome qualificado no FROM ABANDONA o palpite.
     *
     * <p>O ponto não é parte de identificador para o scanner, então
     * {@code FROM _meta.fields} lhe entregaria {@code "_meta"} — um dataset que
     * não existe, e o servidor responderia 404. Devolver null faz o driver usar
     * o dataset da conexão, que é justamente o que a tabela de metadados
     * descreve.
     *
     * <p>Esta é a metade do trabalho que quebraria EM SILÊNCIO: o driver
     * continuaria compilando e os testes continuariam verdes, e a falha
     * apareceria só na primeira consulta ao metadado.
     */
    @Test
    void nomeQualificadoNoFromAbandonaOPalpite() {
        assertNull(dataset("SELECT name FROM _meta.fields"));
        assertNull(dataset("SELECT name FROM _meta.fields WHERE auto = true"));
        assertNull(dataset("select a from ns.tabela"));
    }

    /** O ponto abandona o palpite sem parar a contagem de placeholders. */
    @Test
    void nomeQualificadoNaoZeraAContagemDePlaceholders() {
        assertEquals(2, placeholders("SELECT name FROM _meta.fields WHERE type = ? AND auto = ?"));
    }

    /**
     * Abandonar o palpite não pode reabrir a caça: sem uma marca própria de
     * "já vi o FROM", o campo nulo faria um FROM posterior no texto voltar a
     * capturar um nome.
     */
    @Test
    void depoisDeAbandonarNaoCapturaOutroFrom() {
        assertNull(dataset("SELECT a FROM ns.t WHERE x = 'from outra'"));
    }

    /**
     * Nome qualificado com a PRIMEIRA parte entre aspas também abandona o
     * palpite.
     *
     * <p>É o SQL que a ferramenta gráfica monta ao abrir uma tabela pela
     * árvore: ela cita cada parte com o {@code getIdentifierQuoteString}, e
     * sai {@code "_meta"."counters"}. As aspas do SQL padrão envolvem UM
     * identificador, então a parte citada é só o namespace — capturá-la pediria
     * ao servidor um dataset {@code _meta}, que não existe, e o duplo clique em
     * qualquer view do metadado morreria com "dataset não está registrado".
     *
     * <p>A assimetria entre os dois ramos foi o defeito: o identificador nu já
     * abandonava, o citado capturava sempre.
     */
    @Test
    void nomeQualificadoComAPrimeiraParteCitadaAbandonaOPalpite() {
        assertNull(dataset("SELECT * FROM \"_meta\".counters"));
        assertNull(dataset("SELECT * FROM \"telecom\".\"service_view\""));
        assertNull(dataset("SELECT * FROM telecom.\"service_view\""));
        // O ponto pode vir separado por branco, como em qualquer fronteira.
        assertNull(dataset("SELECT * FROM \"_meta\"\n . counters"));
    }

    /**
     * Nome qualificado citado INTEIRO é UM identificador, e continua sendo
     * palpite.
     *
     * <p>{@code FROM "_meta.counters"} nomeia uma tabela cujo nome contém um
     * ponto. O servidor recusa esse dataset com a mensagem que ensina a citar
     * cada parte — e é ele quem deve recusar: um palpite abandonado aqui
     * trocaria essa mensagem pela de um dataset que o usuário não escreveu.
     */
    @Test
    void nomeQualificadoCitadoInteiroContinuaSendoUmIdentificadorSo() {
        assertEquals("_meta.counters", dataset("SELECT * FROM \"_meta.counters\""));
        assertEquals("telecom.service_view", dataset("SELECT * FROM \"telecom.service_view\""));
    }

    /** Nome citado SEM ponto depois não é qualificado: o palpite vale. */
    @Test
    void nomeCitadoSemPontoDepoisContinuaSendoPalpiteValido() {
        assertEquals("eventos", dataset("SELECT * FROM \"eventos\""));
        assertEquals("eventos", dataset("SELECT * FROM \"eventos\" WHERE a = 1"));
        assertEquals("eventos", dataset("SELECT * FROM \"eventos\" ORDER BY a.b"));
    }

    /**
     * A contagem de '?' não muda em nenhuma das formas de escrever o nome.
     *
     * <p>É a metade que quebraria em silêncio: um return antecipado no ramo das
     * aspas zeraria a contagem e o PreparedStatement recusaria o setString do
     * índice 1 — com a query certa e o dataset certo.
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

    /** Só o PRIMEIRO FROM conta também quando o nome vem entre aspas. */
    @Test
    void apenasOPrimeiroFromContaTambemComNomeCitado() {
        assertEquals("a", dataset("SELECT * FROM \"a\" FROM b"));
        assertNull(dataset("SELECT * FROM \"ns\".t FROM b"));
    }
}
