package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class NishiDatabaseMetaDataTest {

    private static String coluna(String nome, String alias, String tipo, String jdbc, boolean nulavel,
            int precision, int scale, String indice, boolean auto) {
        return "{\"name\":\"" + nome + "\",\"alias\":" + (alias == null ? "null" : "\"" + alias + "\"")
                + ",\"type\":\"" + tipo + "\",\"jdbc\":\"" + jdbc + "\",\"nullable\":" + nulavel
                + ",\"precision\":" + precision + ",\"scale\":" + scale + ",\"displaySize\":0"
                + ",\"index\":\"" + indice + "\",\"auto\":" + auto + "}";
    }

    private static String tabela(String nome, String schema, String namespace, String tipo,
            String colunas) {
        return "{\"name\":\"" + nome + "\",\"schema\":\"" + schema + "\",\"namespace\":\""
                + namespace + "\",\"kind\":\"" + tipo + "\",\"description\":\"\",\"columns\":["
                + colunas + "]}";
    }

    private static final String COLUNA_ID =
            coluna("id", null, "string", "VARCHAR", false, 0, 0, "none", false);

    private static final String COLUNAS_EVENTOS =
            coluna("service_id", null, "string", "VARCHAR", false, 0, 0, "none", false) + ","
            + coluna("status", "estado", "string", "VARCHAR", true, 0, 0, "bitmap", false) + ","
            + coluna("payload", null, "json", "VARCHAR", true, 0, 0, "none", true) + ","
            + coluna("quando", null, "timestamp", "TIMESTAMP", true, 24, 3, "none", false);

    /**
     * As tabelas do {@code _meta}, que TODO dataset publica.
     *
     * <p>Elas aparecem uma vez em cada elemento de {@code datasets} porque o
     * atalho {@code _meta.x} significa "no dataset da requisição" — é o
     * catálogo do servidor que é assim, e não o fixture inventando repetição.
     * O driver tem de mostrá-las UMA vez na árvore.
     */
    private static final String TABELAS_DE_META =
            tabela("fields", "_meta", "_meta", "VIEW",
                    coluna("name", null, "string", "VARCHAR", false, 0, 0, "none", false)) + ","
            + tabela("tables", "_meta", "_meta", "VIEW",
                    coluna("name", null, "string", "VARCHAR", false, 0, 0, "none", false));

    /**
     * O {@code /v1/metadata?schema=telecom} como o servidor o responde: os
     * datasets do schema da conexão — o base e a irmã do histórico, nessa
     * ordem —, cada um com o seu catálogo. Nenhum dataset de outro schema: o
     * recorte é do servidor, e o driver só o apresenta.
     */
    private static final String META = "{\"datasets\":["
            + "{\"name\":\"eventos\",\"schema\":\"telecom\",\"key\":\"service_id\","
            + "\"maxRows\":1000,\"timeoutMs\":30000,"
            + "\"columns\":[" + COLUNAS_EVENTOS + "],\"tables\":["
            + tabela("eventos", "telecom", "", "TABLE", COLUNAS_EVENTOS) + ","
            + tabela("por_olt", "telecom", "_counters", "VIEW",
                    coluna("olt", null, "string", "VARCHAR", false, 0, 0, "none", false) + ","
                    + coluna("total", null, "int64", "BIGINT", false, 19, 0, "none", false)) + ","
            + TABELAS_DE_META + "]},"
            + "{\"name\":\"eventos_history\",\"schema\":\"telecom\",\"key\":\"id\",\"maxRows\":10,"
            + "\"timeoutMs\":100,\"columns\":[" + COLUNA_ID + "],\"tables\":["
            + tabela("eventos_history", "telecom", "", "TABLE", COLUNA_ID) + "," + TABELAS_DE_META + "]}]}";

    private static final String STATS = "{\"process\":{\"version\":\"v1.4.2-3-gabc\",\"query\":{}},"
            + "\"datasets\":[]}";

    private String stats = STATS;
    private String metadata = META;

    /** As query strings que o driver mandou em GET /v1/metadata. */
    private final List<String> metadataQueries = new ArrayList<>();

    private void comMetadata(Consumer<DatabaseMetaData> corpo) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", (HttpExchange ex) -> {
            String c = ex.getRequestURI().getPath();
            if ("/v1/metadata".equals(c)) {
                metadataQueries.add(ex.getRequestURI().getQuery());
            }
            responde(ex, "/v1/stats".equals(c) ? stats : metadata);
        });
        srv.start();
        try {
            String url = "jdbc:nishidb://127.0.0.1:" + srv.getAddress().getPort() + "/telecom";
            try (Connection c = DriverManager.getConnection(url, new Properties())) {
                corpo.accept(c.getMetaData());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        } finally {
            srv.stop(0);
        }
    }

    private static void responde(HttpExchange ex, String corpo) {
        try {
            byte[] b = corpo.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> coluna1(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        while (rs.next()) {
            out.add(rs.getString(1));
        }
        return out;
    }

    /**
     * A árvore inteira, na ordem do contrato e com o TABLE_SCHEM preenchido.
     *
     * <p>A base sai como TABLE no schema do dataset; o contador e as tabelas
     * do {@code _meta} saem como VIEW — o contador no schema do dataset, as de
     * meta no schema {@code _meta}. A ordem é TABLE_TYPE, TABLE_CAT,
     * TABLE_SCHEM, TABLE_NAME; o TABLE_CAT é nulo em toda linha, então o que
     * se vê aqui é tipo, schema e nome.
     *
     * <p>É o TABLE_SCHEM que faz a ferramenta gerar {@code FROM telecom.x} —
     * medido no DBeaver antes desta implementação existir. Devolvê-lo nulo faz
     * o SQL gerado cair no dataset da conexão, que é outra tabela.
     *
     * <p>As duas linhas do {@code _meta} saem UMA vez, e não uma por dataset:
     * o documento as repete em cada elemento de {@code datasets} (o base e a
     * irmã) porque o atalho é relativo ao dataset da requisição.
     *
     * <p>E a árvore é a do SCHEMA DA CONEXÃO: o driver pede o documento
     * recortado pelo schema da URL, e toda chamada ao metadata leva o recorte.
     */
    @Test
    void listaBaseEViewsComSchema() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getTables(null, null, null, null)) {
                assertEquals(10, rs.getMetaData().getColumnCount());
                assertEquals(List.of(
                        "TABLE|telecom|eventos",
                        "TABLE|telecom|eventos_history",
                        "VIEW|_meta|fields",
                        "VIEW|_meta|tables",
                        "VIEW|telecom|por_olt"), arvore(rs));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        assertFalse(metadataQueries.isEmpty());
        for (String q : metadataQueries) {
            assertEquals("schema=telecom", q);
        }
    }

    /** Tipo, schema e nome de cada linha, na ordem em que o driver as devolveu. */
    private static List<String> arvore(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        while (rs.next()) {
            assertNull(rs.getString("TABLE_CAT"), "o NishiDB não tem catálogo");
            out.add(rs.getString("TABLE_TYPE") + "|" + rs.getString("TABLE_SCHEM") + "|"
                    + rs.getString("TABLE_NAME"));
        }
        return out;
    }

    /**
     * O filtro por tipo, que é como a ferramenta abre a pasta de views.
     *
     * <p>{@code null} traz tudo — é o contrato, e é a primeira chamada que a
     * ferramenta faz. Uma lista traz o que ela enumera, e a lista VAZIA não
     * traz nada: ela enumera zero tipos.
     */
    @Test
    void filtraPorTipoDeTabela() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(List.of("TABLE|telecom|eventos", "TABLE|telecom|eventos_history"),
                        arvoreDe(m.getTables(null, null, null, new String[] {"TABLE"})));
                assertEquals(List.of("VIEW|_meta|fields", "VIEW|_meta|tables",
                        "VIEW|telecom|por_olt"),
                        arvoreDe(m.getTables(null, null, null, new String[] {"VIEW"})));
                assertEquals(5, arvoreDe(m.getTables(null, null, null,
                        new String[] {"TABLE", "VIEW"})).size());
                assertEquals(5, arvoreDe(m.getTables(null, null, null, null)).size());
                assertEquals(List.of(), arvoreDe(m.getTables(null, null, null, new String[] {})));
                assertEquals(List.of(),
                        arvoreDe(m.getTables(null, null, null, new String[] {"SYSTEM TABLE"})));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O padrão de SCHEMA, que até aqui era ignorado.
     *
     * <p>{@code null} e {@code %} são "tudo", e o nome exato restringe à
     * pasta que a ferramenta abriu.
     */
    @Test
    void filtraPeloPadraoDeSchema() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(List.of("TABLE|telecom|eventos", "TABLE|telecom|eventos_history",
                        "VIEW|telecom|por_olt"),
                        arvoreDe(m.getTables(null, "telecom", null, null)));
                assertEquals(List.of("VIEW|_meta|fields", "VIEW|_meta|tables"),
                        arvoreDe(m.getTables(null, "_meta", null, null)));
                assertEquals(5, arvoreDe(m.getTables(null, "%", null, null)).size());
                assertEquals(5, arvoreDe(m.getTables(null, null, null, null)).size());
                assertEquals(List.of(), arvoreDe(m.getTables(null, "nada", null, null)));
                // Schema E nome juntos: os dois filtros valem na mesma chamada.
                assertEquals(List.of("VIEW|telecom|por_olt"),
                        arvoreDe(m.getTables(null, "telecom", "por%", null)));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static List<String> arvoreDe(ResultSet rs) throws SQLException {
        try (rs) {
            return arvore(rs);
        }
    }

    /**
     * Os schemas saem DISTINTOS e ordenados, e o padrão filtra: o {@code _meta}
     * e o schema da conexão, e nenhum outro.
     *
     * <p>É deles que a ferramenta monta as pastas da árvore. O {@code _meta}
     * chega repetido em cada dataset do documento e tem de sair uma vez só.
     */
    @Test
    void osSchemasSaemDistintosEOrdenados() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(List.of("_meta", "telecom"), coluna1De(m.getSchemas()));
                assertEquals(List.of("telecom"), coluna1De(m.getSchemas(null, "tele%")));
                assertEquals(List.of("_meta", "telecom"), coluna1De(m.getSchemas(null, null)));
                assertEquals(List.of(), coluna1De(m.getSchemas(null, "nada")));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static List<String> coluna1De(ResultSet rs) throws SQLException {
        try (rs) {
            return coluna1(rs);
        }
    }

    /**
     * getColumns cobre a VIEW, pelo mesmo documento e com o mesmo tipo JDBC.
     *
     * <p>O código do tipo vem calculado do servidor, no campo {@code jdbc} —
     * o driver não rededuz tipo a partir do nome.
     */
    @Test
    void colunasDaViewSaemComTipoJdbc() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getColumns(null, "telecom", "por_olt", null)) {
                assertTrue(rs.next());
                assertEquals("telecom", rs.getString("TABLE_SCHEM"));
                assertEquals("por_olt", rs.getString("TABLE_NAME"));
                assertEquals("olt", rs.getString("COLUMN_NAME"));
                assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
                assertEquals(1, rs.getInt("ORDINAL_POSITION"));

                assertTrue(rs.next());
                assertEquals("total", rs.getString("COLUMN_NAME"));
                assertEquals(Types.BIGINT, rs.getInt("DATA_TYPE"));
                // A posição é dentro da TABELA, e não dentro do dataset: a
                // view é a segunda tabela do `eventos` e a coluna continua
                // sendo a segunda dela.
                assertEquals(2, rs.getInt("ORDINAL_POSITION"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O ORDINAL_POSITION reinicia em CADA tabela e conta a coluna descartada.
     *
     * <p>São as duas propriedades que o contrato pede, e nenhuma das duas
     * aparece numa chamada que filtra uma tabela só e não filtra coluna: ali o
     * laço externo roda uma vez e nada é descartado. Por isso esta chamada
     * pede o SCHEMA inteiro — {@code telecom} tem a base e a view — e a de
     * baixo passa um padrão de coluna.
     *
     * <p>Contar a coluna descartada é o que faz a posição ser a do SCHEMA da
     * tabela, e não a desta resposta: quem pedir só o campo {@code status}
     * recebe a posição que ele ocupa na tabela (2), que é a mesma que uma
     * chamada sem filtro devolveria.
     */
    @Test
    void oOrdinalPositionEhPorTabelaEContaAColunaDescartada() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getColumns(null, "telecom", null, null)) {
                assertEquals(List.of(
                        "telecom|eventos|service_id|1",
                        "telecom|eventos|status|2",
                        "telecom|eventos|payload|3",
                        "telecom|eventos|quando|4",
                        "telecom|eventos_history|id|1",
                        "telecom|por_olt|olt|1",
                        "telecom|por_olt|total|2"), posicoes(rs));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        comMetadata(m -> {
            try (ResultSet rs = m.getColumns(null, "telecom", "eventos", "status")) {
                assertEquals(List.of("telecom|eventos|status|2"), posicoes(rs));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        comMetadata(m -> {
            // O padrão que descarta a PRIMEIRA coluna: sem contar a
            // descartada, `quando` sairia em 1.
            try (ResultSet rs = m.getColumns(null, "telecom", "eventos", "q%")) {
                assertEquals(List.of("telecom|eventos|quando|4"), posicoes(rs));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Schema, tabela, coluna e a posição de cada linha, na ordem devolvida. */
    private static List<String> posicoes(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        while (rs.next()) {
            out.add(rs.getString("TABLE_SCHEM") + "|" + rs.getString("TABLE_NAME") + "|"
                    + rs.getString("COLUMN_NAME") + "|" + rs.getInt("ORDINAL_POSITION"));
        }
        return out;
    }

    /**
     * O padrão LIKE é implementado no driver, e {@code null} casa TUDO.
     *
     * <p>Confundir null com "nenhum" faria a árvore da ferramenta aparecer
     * vazia justamente na primeira chamada, que é sem filtro.
     */
    @Test
    void oPadraoLikeCasaComoOJdbcManda() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(List.of("eventos", "eventos_history"),
                        nomesDe(m.getTables(null, null, "even%", null)));
                assertEquals(List.of("eventos"), nomesDe(m.getTables(null, null, "_ventos", null)));
                assertEquals(List.of(), nomesDe(m.getTables(null, null, "nada%", null)));
                assertEquals(5, nomesDe(m.getTables(null, null, "%", null)).size());
                // Um tipo que o catálogo não usa não devolve nada, e não
                // devolve erro.
                assertEquals(List.of(),
                        nomesDe(m.getTables(null, null, null, new String[] {"SYNONYM"})));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static List<String> nomesDe(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        try (rs) {
            while (rs.next()) {
                out.add(rs.getString("TABLE_NAME"));
            }
        }
        return out;
    }

    /**
     * getColumns publica o nome do CAMPO, e o alias vai no REMARKS.
     *
     * <p>É a divergência que quem ligar as duas rotas precisa conhecer: para o
     * campo com alias, aqui sai {@code status} e o ResultSetMetaData de um
     * SELECT * sai {@code estado}. Sem o alias no REMARKS, ligar um ao outro
     * seria impossível.
     */
    @Test
    void getColumnsPublicaONomeDoCampoEOAliasVaiNoRemarks() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getColumns(null, null, "eventos", null)) {
                assertEquals(24, rs.getMetaData().getColumnCount());

                assertTrue(rs.next());
                assertEquals("service_id", rs.getString("COLUMN_NAME"));
                assertEquals(1, rs.getInt("ORDINAL_POSITION"));
                assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
                assertEquals("NO", rs.getString("IS_NULLABLE"));

                assertTrue(rs.next());
                assertEquals("status", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("REMARKS").contains("alias=estado"), rs.getString("REMARKS"));
                assertTrue(rs.getString("REMARKS").contains("index=bitmap"), rs.getString("REMARKS"));
                assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));

                assertTrue(rs.next());
                assertEquals("payload", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("REMARKS").contains("auto=true"), rs.getString("REMARKS"));

                assertTrue(rs.next());
                assertEquals("quando", rs.getString("COLUMN_NAME"));
                assertEquals(Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
                assertEquals("timestamp", rs.getString("TYPE_NAME"));
                assertEquals(24, rs.getInt("COLUMN_SIZE"));
                assertEquals(3, rs.getInt("DECIMAL_DIGITS"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * A chave do dataset é ganho real: é ela que a ferramenta desenha.
     *
     * <p>E ela sai NO SCHEMA do dataset: quem cruza esta lista com a do
     * {@link DatabaseMetaData#getTables} pelo par (schema, nome) — que é como
     * uma ferramenta liga a chave à tabela da árvore — não acha a linha se
     * aqui o TABLE_SCHEM sair nulo.
     */
    @Test
    void aChaveDoDatasetSaiComoChavePrimariaNoSchemaDoDataset() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getPrimaryKeys(null, null, "eventos")) {
                assertTrue(rs.next());
                assertEquals("telecom", rs.getString("TABLE_SCHEM"));
                assertEquals("eventos", rs.getString("TABLE_NAME"));
                assertEquals("service_id", rs.getString("COLUMN_NAME"));
                assertEquals(1, rs.getInt("KEY_SEQ"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O índice sai com a ressalva de que é DECLARADO, não plano escolhido — e
     * no schema do dataset, pela razão da chave primária.
     */
    @Test
    void oIndiceSaiComARessalvaDeQueEhDeclaradoENoSchemaDoDataset() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getIndexInfo(null, null, "eventos", false, true)) {
                assertTrue(rs.next());
                assertEquals("telecom", rs.getString("TABLE_SCHEM"));
                assertEquals("status", rs.getString("COLUMN_NAME"));
                assertTrue(rs.getString("INDEX_NAME").contains("bitmap"), rs.getString("INDEX_NAME"));
                assertTrue(rs.getString("INDEX_NAME").contains("declarado"), rs.getString("INDEX_NAME"));
                assertFalse(rs.next());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * As duas listas HONRAM o schema que receberam.
     *
     * <p>Elas passaram a publicar o TABLE_SCHEM, e um método que publica o
     * campo sem filtrar por ele afirma um schema que não conferiu: pedir a
     * chave de {@code fault.eventos} devolveria a de {@code telecom.eventos}.
     * {@code null} continua sendo "não restrinja", que é o que a ferramenta
     * manda quando já sabe a tabela.
     */
    @Test
    void aChaveEOIndiceHonramOSchemaRecebido() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals(1, linhas(m.getPrimaryKeys(null, "telecom", "eventos")));
                assertEquals(0, linhas(m.getPrimaryKeys(null, "fault", "eventos")));
                assertEquals(0, linhas(m.getPrimaryKeys(null, "", "eventos")));
                assertEquals(1, linhas(m.getPrimaryKeys(null, null, "eventos")));

                assertEquals(1, linhas(m.getIndexInfo(null, "telecom", "eventos", false, true)));
                assertEquals(0, linhas(m.getIndexInfo(null, "fault", "eventos", false, true)));
                assertEquals(0, linhas(m.getIndexInfo(null, "", "eventos", false, true)));
                assertEquals(1, linhas(m.getIndexInfo(null, null, "eventos", false, true)));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static int linhas(ResultSet rs) throws SQLException {
        int n = 0;
        try (rs) {
            while (rs.next()) {
                n++;
            }
        }
        return n;
    }

    /**
     * getTypeInfo sai do fio, e o SEARCHABLE é honesto.
     *
     * <p>json e bytes não são pesquisáveis porque o binder recusa comparação
     * sobre eles: anunciá-los pesquisáveis faria a ferramenta oferecer um
     * filtro que o servidor recusa.
     */
    @Test
    void getTypeInfoSaiDoFioEODizSeEhPesquisavel() throws Exception {
        comMetadata(m -> {
            try (ResultSet rs = m.getTypeInfo()) {
                boolean viuJson = false;
                boolean viuString = false;
                while (rs.next()) {
                    if ("json".equals(rs.getString("TYPE_NAME"))) {
                        viuJson = true;
                        assertEquals(DatabaseMetaData.typePredNone, rs.getInt("SEARCHABLE"));
                    }
                    if ("string".equals(rs.getString("TYPE_NAME"))) {
                        viuString = true;
                        assertEquals(DatabaseMetaData.typeSearchable, rs.getInt("SEARCHABLE"));
                    }
                }
                assertTrue(viuJson && viuString, "os tipos do fio não apareceram no getTypeInfo");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O catálogo sai VAZIO COM A FORMA CERTA — a errata D3.
     *
     * <p>O produto não tem catálogo: o endereço qualificado tem dois níveis,
     * e o de cima é o schema. Lançar aqui faria a ferramenta mostrar erro ao
     * abrir a árvore de navegação, antes da primeira consulta.
     *
     * <p>Os TIPOS são TABLE e VIEW, e nunca SYSTEM TABLE: o DBeaver esconde
     * objeto de sistema por default, e as tabelas do {@code _meta} existem
     * justamente para serem descobertas.
     */
    @Test
    void oCatalogoSaiVazioEOsTiposSaoTabelaEView() throws Exception {
        comMetadata(m -> {
            try (ResultSet cat = m.getCatalogs(); ResultSet sch = m.getSchemas();
                    ResultSet tipos = m.getTableTypes()) {
                assertEquals(1, cat.getMetaData().getColumnCount());
                assertEquals("TABLE_CAT", cat.getMetaData().getColumnName(1));
                assertEquals(List.of(), coluna1(cat));

                assertEquals(2, sch.getMetaData().getColumnCount());
                assertEquals("TABLE_SCHEM", sch.getMetaData().getColumnName(1));

                assertEquals(List.of("TABLE", "VIEW"), coluna1(tipos));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** Todo getter de catálogo que não existe no produto devolve VAZIO, nunca exceção. */
    @Test
    void osGettersSemConteudoDevolvemVazioENaoExcecao() throws Exception {
        comMetadata(m -> {
            try {
                for (ResultSet rs : List.of(m.getProcedures(null, null, null),
                        m.getProcedureColumns(null, null, null, null),
                        m.getFunctions(null, null, null),
                        m.getFunctionColumns(null, null, null, null),
                        m.getImportedKeys(null, null, "eventos"),
                        m.getExportedKeys(null, null, "eventos"),
                        m.getCrossReference(null, null, "a", null, null, "b"),
                        m.getTablePrivileges(null, null, null),
                        m.getColumnPrivileges(null, null, "eventos", null),
                        m.getBestRowIdentifier(null, null, "eventos", 0, false),
                        m.getVersionColumns(null, null, "eventos"),
                        m.getUDTs(null, null, null, null),
                        m.getSuperTypes(null, null, null),
                        m.getSuperTables(null, null, null),
                        m.getAttributes(null, null, null, null),
                        m.getClientInfoProperties(),
                        m.getPseudoColumns(null, null, null, null))) {
                    ResultSetMetaData meta = rs.getMetaData();
                    assertTrue(meta.getColumnCount() > 0, "ResultSet vazio precisa ter as colunas certas");
                    assertFalse(rs.next());
                    rs.close();
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** As capacidades que o produto não tem respondem FALSE, e não lançam. */
    @Test
    void asCapacidadesAusentesRespondemFalse() throws Exception {
        comMetadata(m -> {
            try {
                assertFalse(m.supportsGroupBy());
                assertFalse(m.supportsOuterJoins());
                assertFalse(m.supportsSubqueriesInComparisons());
                assertFalse(m.supportsUnion());
                assertFalse(m.supportsTransactions());
                assertFalse(m.supportsStoredProcedures());
                assertFalse(m.supportsBatchUpdates());
                assertFalse(m.supportsColumnAliasing());
                assertFalse(m.supportsANSI92EntryLevelSQL());
                assertFalse(m.supportsSavepoints());
                assertFalse(m.supportsPositionedUpdate());
                assertFalse(m.supportsPositionedDelete());
                assertEquals(Connection.TRANSACTION_NONE, m.getDefaultTransactionIsolation());

                // O schema é a exceção entre os cinco supportsSchemasIn*: os
                // outros quatro são DDL, procedimento e privilégio, que o
                // produto não tem; a consulta ACEITA o qualificador.
                assertTrue(m.supportsSchemasInDataManipulation());
                assertFalse(m.supportsSchemasInTableDefinitions());
                assertFalse(m.supportsCatalogsInDataManipulation());
                assertEquals("schema", m.getSchemaTerm());

                assertTrue(m.allTablesAreSelectable());
                assertTrue(m.supportsMixedCaseIdentifiers());
                assertTrue(m.isReadOnly());
                assertTrue(m.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
                assertFalse(m.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
                assertEquals("\"", m.getIdentifierQuoteString());
                assertEquals(java.sql.RowIdLifetime.ROWID_UNSUPPORTED, m.getRowIdLifetime());
                assertEquals(DatabaseMetaData.sqlStateSQL, m.getSQLStateType());
                assertEquals(1, m.getMaxTablesInSelect());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * O ORDER BY existe, e o metadado precisa dizer as três coisas certas
     * sobre ele — nem uma a menos, nem uma a mais.
     *
     * <p>As três foram MEDIDAS contra o servidor, e não deduzidas da spec:
     * ordenar por coluna fora da projeção funciona, ordenar por posição
     * ({@code ORDER BY 1}) e por expressão ({@code ORDER BY a + 1}) são
     * recusados na sintaxe, e o parser cobra teto de 16 chaves.
     *
     * <p>Enquanto {@code supportsOrderByUnrelated} respondia false, um
     * construtor de query gráfico desabilitava a ordenação por coluna não
     * projetada — o usuário concluía que o banco não faz o que ele faz. E
     * enquanto {@code getMaxColumnsInOrderBy} respondia zero ("sem limite"),
     * quem monta ORDER BY por programa descobriria o 16 por erro de sintaxe
     * em produção.
     */
    @Test
    void oMetadadoDescreveOOrderByComoEleE() throws Exception {
        comMetadata(m -> {
            try {
                assertTrue(m.supportsOrderByUnrelated(),
                        "o servidor ordena por coluna fora da projeção");
                assertFalse(m.supportsExpressionsInOrderBy(),
                        "expressão no ORDER BY é erro de sintaxe no servidor");
                assertEquals(16, m.getMaxColumnsInOrderBy(),
                        "o teto é o MaxOrderKeys do parser, não 'sem limite'");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** A versão do produto sai do /v1/stats, e o número não parseável vira ZERO. */
    @Test
    void aVersaoSaiDoStatsESemNumeroViraZero() throws Exception {
        comMetadata(m -> {
            try {
                assertEquals("v1.4.2-3-gabc", m.getDatabaseProductVersion());
                assertEquals(1, m.getDatabaseMajorVersion());
                assertEquals(4, m.getDatabaseMinorVersion());
                assertEquals("NishiDB", m.getDatabaseProductName());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });

        stats = "{\"process\":{\"version\":\"desenvolvimento\"},\"datasets\":[]}";
        comMetadata(m -> {
            try {
                assertEquals(0, m.getDatabaseMajorVersion());
                assertEquals(0, m.getDatabaseMinorVersion());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
