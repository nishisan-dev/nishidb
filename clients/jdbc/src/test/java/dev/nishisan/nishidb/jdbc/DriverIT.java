package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * O driver contra o BINÁRIO REAL, servindo um dataset com um campo de cada
 * tipo.
 *
 * <p>Os testes unitários provam que o driver faz o que quem o escreveu ACHA que
 * o servidor emite — eles falam com um servidor HTTP de mentira, com respostas
 * escritas à mão. Estes fecham a outra metade: se o fio suposto divergir do fio
 * real, é aqui que aparece.
 *
 * <p>O ambiente vem de {@code scripts/jdbc-it.sh}. A condição de ambiente
 * existe para que um {@code mvnw test} avulso continue funcionando sem
 * servidor; o que impede o SKIP de virar um verde mentiroso é
 * {@link IntegracaoObrigatoriaTest}, que NÃO é condicionado.
 */
@EnabledIfEnvironmentVariable(named = "NISHIDB_IT_URL", matches = ".+")
class DriverIT {

    private static Connection conecta() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", IntegracaoObrigatoriaTest.SENHA);
        return DriverManager.getConnection(IntegracaoObrigatoriaTest.URL, p);
    }

    /**
     * A matriz de conversão sobre os BYTES QUE O SERVIDOR DE FATO EMITE.
     *
     * <p>A linha de borda foi ingerida com os valores mais incômodos de cada
     * tipo: int64 no limite, double na grafia 'g' do Go, endereço no pior caso,
     * prefixo COM host bits (o encoder não mascara), json aninhado e os dois
     * formatos temporais do schema.
     */
    @Test
    void aMatrizDeConversaoSobreOFioReal() throws SQLException {
        try (Connection c = conecta();
                PreparedStatement st = c.prepareStatement(
                        "SELECT id, texto, inteiro, real, booleano, quando, v4, v6, rede4, rede6, "
                                + "cru, doc, momento, momento_iso, hora, hora_iso "
                                + "FROM todos_os_tipos WHERE id = ?")) {
            st.setString(1, "borda-1");
            try (ResultSet rs = st.executeQuery()) {
                assertTrue(rs.next(), "a linha de borda não voltou do servidor");

                assertEquals("linha com \"aspas\" e \\barra", rs.getString("texto"));
                assertEquals(Long.MAX_VALUE, rs.getLong("inteiro"));
                assertEquals(Double.doubleToLongBits(1e-07),
                        Double.doubleToLongBits(rs.getDouble("real")));
                // A grafia 'g' do Go, verbatim: é o que torna getString lossless.
                assertEquals("1e-07", rs.getString("real"));
                assertTrue(rs.getBoolean("booleano"));

                assertEquals(1787570000123L, rs.getTimestamp("quando").getTime());
                assertEquals(Instant.ofEpochMilli(1787570000123L),
                        rs.getObject("quando", Instant.class));

                assertEquals("255.255.255.255", rs.getString("v4"));
                assertEquals("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", rs.getString("v6"));
                // SEM Masked(): o host bit sobrevive ao round-trip.
                assertEquals("10.0.0.1/24", rs.getString("rede4"));
                assertEquals("2001:db8::1/32", rs.getString("rede6"));

                assertArrayEqualsHello(rs.getBytes("cru"));
                // json sai INLINE e verbatim, não como string escapada.
                assertEquals("{\"a\":[1,2,{\"b\":null}]}", rs.getString("doc"));

                // Os dois formatos temporais resolvem para o MESMO instante:
                // datetime é wall clock resolvido no fuso do host do servidor.
                assertEquals(rs.getTimestamp("momento").getTime(),
                        rs.getTimestamp("momento_iso").getTime());
                // A precisão da hora é do FORMATO declarado no schema, não do
                // tipo: `hora` é "hh:mm:ss a" e não tem onde guardar a fração,
                // enquanto `hora_iso` é "HH:mm:ss.SSS" e tem. Os dois campos
                // receberam o mesmo instante na ingestão, e o que os separa é o
                // formato — não o driver.
                assertEquals(LocalTime.of(23, 59, 59), rs.getObject("hora", LocalTime.class));
                assertEquals(LocalTime.of(23, 59, 59, 999_000_000),
                        rs.getObject("hora_iso", LocalTime.class));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void projectionAliasesAndSuffixesReachMetadataAndLookup() throws SQLException {
        for (String limit : new String[] {"", " LIMIT 0"}) {
            try (Connection c = conecta();
                    PreparedStatement st = c.prepareStatement(
                            "SELECT id, inteiro AS id, texto AS \"FROM\", inteiro amount "
                                    + "FROM todos_os_tipos WHERE id = ? ORDER BY id" + limit)) {
                st.setString(1, "borda-1");
                try (ResultSet rs = st.executeQuery()) {
                    ResultSetMetaData md = rs.getMetaData();
                    String[] names = {"id_1", "id", "FROM", "amount"};
                    int[] types = {Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.BIGINT};
                    assertEquals(4, md.getColumnCount());
                    for (int i = 0; i < names.length; i++) {
                        assertEquals(names[i], md.getColumnLabel(i + 1));
                        assertEquals(names[i], md.getColumnName(i + 1));
                        assertEquals(types[i], md.getColumnType(i + 1));
                        assertEquals(i + 1, rs.findColumn(names[i]));
                    }
                    assertEquals(ResultSetMetaData.columnNoNulls, md.isNullable(1));
                    assertEquals(ResultSetMetaData.columnNullable, md.isNullable(2));
                    if (limit.isEmpty()) {
                        assertTrue(rs.next());
                        assertEquals("borda-1", rs.getString("id_1"));
                        assertEquals(Long.MAX_VALUE, rs.getLong("id"));
                        assertEquals(Long.MAX_VALUE, rs.getLong("amount"));
                        assertEquals("linha com \"aspas\" e \\barra", rs.getString("FROM"));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    private static void assertArrayEqualsHello(byte[] b) {
        assertEquals("Hello", new String(b, java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * O ResultSetMetaData vem do servidor, com os números que o descritor único
     * calcula — o driver não os recalcula.
     */
    @Test
    void osNumerosDoMetadataVemDoServidor() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT id, inteiro, quando, hora, v6, cru FROM todos_os_tipos LIMIT 1")) {
            ResultSetMetaData m = rs.getMetaData();
            assertEquals(6, m.getColumnCount());
            assertEquals(Types.VARCHAR, m.getColumnType(1));
            // id é a chave do dataset: é a única coluna que o produto garante
            // não-nula, e o servidor a publica assim.
            assertEquals(ResultSetMetaData.columnNoNulls, m.isNullable(1));

            assertEquals(Types.BIGINT, m.getColumnType(2));
            assertEquals(19, m.getPrecision(2));
            assertEquals(20, m.getColumnDisplaySize(2));

            assertEquals(Types.TIMESTAMP, m.getColumnType(3));
            assertEquals(3, m.getScale(3), "a fração de segundo é sempre de três dígitos");

            assertEquals(Types.TIME, m.getColumnType(4));
            assertEquals(12, m.getColumnDisplaySize(4));

            // 39, e não os 45 do manual: o encoder nunca usa a forma mista
            // longa, e este número foi MEDIDO contra ele.
            assertEquals(39, m.getPrecision(5));

            assertEquals(Types.VARBINARY, m.getColumnType(6));
        }
    }

    /**
     * A sequência que uma FERRAMENTA faz ao abrir a conexão, na ordem em que
     * ela faz.
     *
     * <p>Não substitui o DBeaver — ele tem suposições de interface que só
     * aparecem na tela —, mas faz a maioria das surpresas aparecer aqui, onde o
     * diagnóstico custa um stack trace em vez de uma sessão gráfica.
     */
    @Test
    void aSequenciaQueUmaFerramentaFazAoAbrirAConexao() throws SQLException {
        try (Connection c = conecta()) {
            DatabaseMetaData m = c.getMetaData();
            assertEquals("NishiDB", m.getDatabaseProductName());
            // A versão vem do /v1/stats, e o que se afirma aqui é a FORMA do
            // buildinfo — uma constante no driver não teria "commit". Afirmar
            // que começa com "v" testaria o AMBIENTE DE BUILD e não o driver:
            // o checkout do CI é clone raso sem tags, então o
            // `git describe --tags --always` do Makefile cai no SHA puro.
            String versao = m.getDatabaseProductVersion();
            assertFalse(versao.isBlank(), "getDatabaseProductVersion veio vazia");
            assertTrue(versao.contains("commit "),
                    "a versão devia ter a forma do buildinfo do servidor: " + versao);
            // E major/minor DEGRADAM para zero quando a versão não é
            // parseável, sem lançar. No CI esse é o caso real — é o único
            // ambiente em que este caminho é exercitado.
            assertTrue(m.getDatabaseMajorVersion() >= 0);
            assertTrue(m.getDatabaseMinorVersion() >= 0);
            assertEquals("\"", m.getIdentifierQuoteString());
            assertFalse(m.supportsTransactions());

            // Catálogo: vazio, mas com a forma certa. O produto não tem
            // catálogo — o endereço qualificado tem dois níveis.
            try (ResultSet rs = m.getCatalogs()) {
                assertEquals(1, rs.getMetaData().getColumnCount());
            }
            // Schemas: EXATAMENTE o da conexão e o `_meta`, do servidor REAL. O
            // da conexão é o `schemaName` do YAML, que a URL nomeia; o `_meta` é
            // onde as tabelas que descrevem o motor moram. Um schema a mais seria
            // a árvore mostrando o que a conexão não abre.
            List<String> schemas = new ArrayList<>();
            try (ResultSet rs = m.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                }
            }
            assertEquals(List.of("_meta", "telecom"), schemas);
            assertEquals("telecom", c.getSchema());

            List<String> tipos = new ArrayList<>();
            try (ResultSet rs = m.getTableTypes()) {
                while (rs.next()) {
                    tipos.add(rs.getString(1));
                }
            }
            assertEquals(List.of("TABLE", "VIEW"), tipos);

            // A árvore: a base como TABLE no schema do dataset, e as tabelas
            // do _meta como VIEW no schema _meta.
            List<String> tabelas = new ArrayList<>();
            List<String> views = new ArrayList<>();
            // O endereço qualificado sai DA ÁRVORE, e não escrito à mão: é ele
            // que vai virar o FROM logo abaixo, e escrever o schema no teste
            // faria a asserção provar a string do teste, não a do driver.
            String enderecoDaBase = null;
            try (ResultSet rs = m.getTables(null, null, null, null)) {
                while (rs.next()) {
                    String qualificado = rs.getString("TABLE_SCHEM") + "."
                            + rs.getString("TABLE_NAME");
                    if ("VIEW".equals(rs.getString("TABLE_TYPE"))) {
                        views.add(qualificado);
                    } else {
                        tabelas.add(qualificado);
                        if ("todos_os_tipos".equals(rs.getString("TABLE_NAME"))) {
                            enderecoDaBase = qualificado;
                        }
                    }
                }
            }
            assertTrue(tabelas.contains("telecom.todos_os_tipos"),
                    "getTables não listou a base no schema do dataset: " + tabelas);
            assertTrue(views.contains("_meta.tables"),
                    "getTables não listou as tabelas do _meta como view: " + views);

            // O filtro por tipo é como a ferramenta abre a pasta de views.
            try (ResultSet rs = m.getTables(null, null, null, new String[] {"VIEW"})) {
                while (rs.next()) {
                    assertEquals("VIEW", rs.getString("TABLE_TYPE"));
                }
            }
            // E o de schema restringe à pasta aberta.
            try (ResultSet rs = m.getTables(null, "_meta", null, null)) {
                assertTrue(rs.next(), "getTables com o padrão de schema _meta não trouxe linha");
                do {
                    assertEquals("_meta", rs.getString("TABLE_SCHEM"));
                } while (rs.next());
            }

            // O PAGAMENTO: o SQL que a ferramenta monta a partir do (schema,
            // nome) desta árvore tem de EXECUTAR no servidor. É o que o driver
            // promete ao preencher o TABLE_SCHEM — e por isso o FROM é MONTADO
            // com o endereço que a árvore devolveu.
            assertNotNull(enderecoDaBase, "a base não apareceu na árvore: " + tabelas);
            String sql = "SELECT id FROM " + enderecoDaBase + " LIMIT 1";
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "o SELECT montado da árvore não devolveu linha: " + sql);
            }

            // getColumns publica o nome do CAMPO; o alias vai no REMARKS. O
            // dataset tem um campo com alias justamente para provar isso.
            boolean viuAlias = false;
            int colunas = 0;
            try (ResultSet rs = m.getColumns(null, null, "todos_os_tipos", null)) {
                while (rs.next()) {
                    colunas++;
                    if ("texto".equals(rs.getString("COLUMN_NAME"))) {
                        viuAlias = rs.getString("REMARKS").contains("alias=t");
                    }
                }
            }
            assertTrue(colunas > 10, "getColumns devolveu " + colunas + " colunas");
            assertTrue(viuAlias, "o alias do campo não apareceu no REMARKS de getColumns");

            // A chave e o índice descrevem a MESMA tabela que a árvore
            // publica: o TABLE_SCHEM das duas listas é o schema do dataset, e
            // é por ele que a ferramenta liga a chave à tabela da árvore.
            try (ResultSet rs = m.getPrimaryKeys(null, null, "todos_os_tipos")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals(enderecoDaBase,
                        rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"),
                        "a chave primária não caiu na tabela da árvore");
            }
            try (ResultSet rs = m.getIndexInfo(null, null, "todos_os_tipos", false, true)) {
                assertTrue(rs.next(), "o dataset declara índice em dois campos");
                assertEquals(enderecoDaBase,
                        rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"),
                        "o índice não caiu na tabela da árvore");
            }
            try (ResultSet rs = m.getTypeInfo()) {
                assertTrue(rs.next(), "getTypeInfo saiu vazio");
            }

            // E então a consulta, com o grid lendo célula a célula por getObject
            // e conferindo nulo — que é o padrão da ferramenta.
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM todos_os_tipos LIMIT 5")) {
                ResultSetMetaData rm = rs.getMetaData();
                int lidas = 0;
                while (rs.next()) {
                    lidas++;
                    for (int i = 1; i <= rm.getColumnCount(); i++) {
                        rs.getObject(i);
                        rs.wasNull();
                        rs.getString(i);
                    }
                }
                assertEquals(5, lidas);
            }
        }
    }

    /**
     * O SQL COM CADA PARTE CITADA executa — é o que o duplo clique na árvore
     * gera.
     *
     * <p>A ferramenta gráfica não monta {@code schema.tabela} cru: ela cita
     * cada parte com o {@link DatabaseMetaData#getIdentifierQuoteString()} e
     * manda {@code "telecom"."todos_os_tipos"}. Enquanto o driver tirava o
     * dataset do FROM, a parte citada virava palpite de dataset e abrir
     * qualquer view do metadado morria com "dataset não está registrado", com a
     * árvore montada e nada abrindo. Com a conexão por schema o driver manda o
     * schema da URL e o servidor resolve o nome inteiro — este teste é o que
     * impede o defeito de voltar por outro caminho.
     *
     * <p>O endereço vem DA ÁRVORE e as aspas vêm do metadata: escrever os dois
     * à mão faria o teste provar a string do teste, e não o par que o driver
     * publica.
     */
    @Test
    void oSqlComCadaParteCitadaDaArvoreExecuta() throws SQLException {
        try (Connection c = conecta()) {
            DatabaseMetaData m = c.getMetaData();
            String aspa = m.getIdentifierQuoteString();
            String base = null;
            String view = null;
            try (ResultSet rs = m.getTables(null, null, null, null)) {
                while (rs.next()) {
                    String endereco = aspa + rs.getString("TABLE_SCHEM") + aspa + "."
                            + aspa + rs.getString("TABLE_NAME") + aspa;
                    if ("VIEW".equals(rs.getString("TABLE_TYPE"))) {
                        // `_meta.tables` nomeadamente: é a view que sempre tem
                        // linha (ela se lista), e um teste que pegasse a
                        // primeira view qualquer poderia cair numa vazia e
                        // confundir "não abriu" com "abriu e não tem dado".
                        if ("_meta".equals(rs.getString("TABLE_SCHEM"))
                                && "tables".equals(rs.getString("TABLE_NAME"))) {
                            view = endereco;
                        }
                    } else if ("todos_os_tipos".equals(rs.getString("TABLE_NAME"))) {
                        base = endereco;
                    }
                }
            }
            assertNotNull(base, "a base não apareceu na árvore");
            assertNotNull(view, "a view _meta.tables não apareceu na árvore");

            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM " + base + " LIMIT 1")) {
                assertTrue(rs.next(), "o SELECT citado parte a parte não devolveu linha: " + base);
            }
            // O caso que quebrava: a view do _meta, que é o motivo de a árvore
            // existir, aberta pelo atalho sobre o dataset da conexão.
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM " + view + " LIMIT 1")) {
                assertTrue(rs.next(), "o SELECT na view do _meta não devolveu linha: " + view);
            }
            // E com a primeira parte citada e a segunda nua, que é a outra
            // forma que a ferramenta emite ao completar nome à mão.
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT * FROM " + aspa + "_meta" + aspa + ".tables LIMIT 1")) {
                assertTrue(rs.next(), "o SELECT em \"_meta\".tables não devolveu linha");
            }
        }
    }

    /**
     * A URL da forma anterior — que nomeava o DATASET — falha no connect
     * ENSINANDO o schema, contra o servidor real.
     *
     * <p>É a quebra de contrato da conexão por schema: sem fallback, mas sem
     * deixar o operador com um "não existe" que não diz o que trocar. A
     * mensagem traz o schema que o servidor conhece para aquele dataset e a
     * forma da URL que só o driver conhece.
     */
    @Test
    void aUrlQueNomeiaODatasetEnsinaOSchema() {
        String velha = IntegracaoObrigatoriaTest.URL.replace("/telecom?", "/todos_os_tipos?");
        assertFalse(velha.equals(IntegracaoObrigatoriaTest.URL),
                "a URL do IT não tem a forma esperada: " + IntegracaoObrigatoriaTest.URL);
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", IntegracaoObrigatoriaTest.SENHA);
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(velha, p));
        assertEquals("3F000", e.getSQLState(), e.getMessage());
        assertTrue(e.getMessage().contains("use \"telecom\""), e.getMessage());
        assertTrue(e.getMessage().contains("host[:porta]/schema"), e.getMessage());
    }

    /**
     * O nome de saída do ResultSetMetaData é o ALIAS; o do getColumns é o do
     * campo.
     *
     * <p>É a divergência que a execução da Fase E revelou, e este é o único
     * lugar em que ela pode ser provada contra o servidor de verdade.
     */
    @Test
    void oNomeDeSaidaEhOAliasEODoCatalogoEhODoCampo() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM todos_os_tipos LIMIT 1")) {
            List<String> saida = new ArrayList<>();
            ResultSetMetaData m = rs.getMetaData();
            for (int i = 1; i <= m.getColumnCount(); i++) {
                saida.add(m.getColumnName(i));
            }
            assertTrue(saida.contains("t"), "SELECT * devia publicar o alias 't': " + saida);
            assertFalse(saida.contains("texto"), "o nome do campo não devia sair na projeção: " + saida);
        }
    }

    /**
     * O erro do servidor chega com o SQLState certo, e não como falha genérica.
     *
     * <p>A cláusula recusada aqui é o GROUP BY, e não o ORDER BY como era até
     * a v1 ganhar ordenação: o ORDER BY passou a ser suportado (três modos, em
     * internal/query/executor/sort.go) e saiu de `unsupportedClauses`,
     * mas este teste ficou para trás e cobrava dele um erro que o servidor
     * deixou de emitir. O GROUP BY continua recusado e mantém a intenção
     * original — provar que UNSUPPORTED_CLAUSE chega traduzido —, e o
     * ORDER BY ganhou asserção PRÓPRIA, do outro lado, em
     * {@link #oOrderByEhSuportado()}.
     */
    @Test
    void oErroDoServidorChegaTraduzido() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement()) {
            SQLException clausula = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM todos_os_tipos GROUP BY id"));
            assertEquals("0A000", clausula.getSQLState(), clausula.getMessage());
            assertTrue(clausula.getMessage().contains("GROUP BY"), clausula.getMessage());

            SQLException sintaxe = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM"));
            assertEquals("42601", sintaxe.getSQLState(), sintaxe.getMessage());

            // Na conexão por schema o nome simples é tabela DO SCHEMA: a que
            // não existe é recusada pelo bind, nomeando o schema da conexão.
            SQLException tabela = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM nao_existe"));
            assertEquals("42000", tabela.getSQLState(), tabela.getMessage());
            assertTrue(tabela.getMessage().contains("telecom"), tabela.getMessage());

            // O schema que não é o da conexão é recusado, e não resolvido: a
            // árvore desta conexão não o mostra.
            SQLException alheio = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT id FROM fault.events"));
            assertEquals("42000", alheio.getSQLState(), alheio.getMessage());
            assertTrue(alheio.getMessage().contains("outside this connection"), alheio.getMessage());
        }
    }

    /**
     * O ORDER BY atravessa o driver, inclusive por coluna que NÃO está na
     * projeção — que é o que o getMaxColumnsInOrderBy e o
     * supportsOrderByUnrelated do DatabaseMetaData prometem.
     *
     * <p>Ele existe porque a asserção que este arquivo tinha era a negativa:
     * cobrava do servidor um erro que ele deixou de emitir, e passou a falhar
     * quando a ordenação entrou. Uma negativa obsoleta some ao ser corrigida;
     * sem esta positiva no lugar, o driver ficaria sem nenhuma prova de que
     * ordena.
     */
    @Test
    void oOrderByEhSuportado() throws SQLException {
        try (Connection c = conecta(); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM todos_os_tipos ORDER BY id LIMIT 5")) {
                String anterior = null;
                while (rs.next()) {
                    String atual = rs.getString("id");
                    if (anterior != null) {
                        assertTrue(anterior.compareTo(atual) <= 0,
                                "fora de ordem: " + anterior + " veio antes de " + atual);
                    }
                    anterior = atual;
                }
                assertNotNull(anterior, "o ORDER BY não devolveu linha nenhuma");
            }
            // Coluna de ordenação FORA da projeção: é o caso que o
            // supportsOrderByUnrelated declara, e o que uma asserção só sobre
            // `ORDER BY id` não distinguiria.
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM todos_os_tipos ORDER BY inteiro LIMIT 1")) {
                assertTrue(rs.next(), "ORDER BY por coluna não projetada não devolveu linha");
            }
        }
    }

    /**
     * SEM a CA, a conexão FALHA — a prova de que não sobrou nenhum caminho que
     * ignore a verificação de certificado.
     *
     * <p>É o teste que não pode passar por acidente: se um dia alguém
     * acrescentar uma propriedade "insecure", ou o driver cair para um
     * TrustManager permissivo, este teste vira verde e é o único que acusa.
     */
    @Test
    void semACAAConexaoFalha() {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", IntegracaoObrigatoriaTest.SENHA);
        SQLException e = assertThrows(SQLException.class,
                () -> DriverManager.getConnection(IntegracaoObrigatoriaTest.URL_SEM_CA, p));
        assertEquals("08001", e.getSQLState(), e.getMessage());
    }

    /** Credencial errada é 28000, e não um erro de conexão genérico. */
    @Test
    void credencialErradaEh28000() {
        Properties p = new Properties();
        p.setProperty("user", IntegracaoObrigatoriaTest.USUARIO);
        p.setProperty("password", "senha-errada");
        SQLException e = assertThrows(SQLException.class,
                () -> DriverManager.getConnection(IntegracaoObrigatoriaTest.URL, p));
        assertEquals("28000", e.getSQLState(), e.getMessage());
    }

    /**
     * O §128 no cliente, contra o servidor real: o resultado inteiro atravessa
     * o driver com heap FIXO.
     *
     * <p>O surefire roda com {@code -Xmx64m}. Um driver que materializasse o
     * resultado estouraria o heap muito antes do fim — e é por isso que este
     * teste percorre TODAS as linhas em vez de amostrar.
     */
    @Test
    void oResultadoInteiroAtravessaODriverComHeapFixo() throws SQLException {
        long esperadas = Long.parseLong(System.getenv("NISHIDB_IT_LINHAS")) + 2;
        try (Connection c = conecta(); Statement st = c.createStatement()) {
            // maxRows zero e teto do dataset alto: o objetivo é trazer tudo.
            st.setMaxRows(0);
            long lidas = 0;
            long soma = 0;
            try (ResultSet rs = st.executeQuery("SELECT id, inteiro, texto FROM todos_os_tipos")) {
                while (rs.next()) {
                    lidas++;
                    soma += rs.getLong("inteiro");
                }
            }
            assertEquals(esperadas, lidas,
                    "o driver não entregou todas as linhas ingeridas (soma lida: " + soma + ")");
        }
    }
}
