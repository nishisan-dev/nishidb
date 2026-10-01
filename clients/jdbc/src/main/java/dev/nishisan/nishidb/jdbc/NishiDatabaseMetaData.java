package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonValue;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.TiposJdbc;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * O {@code DatabaseMetaData} do §71, com a errata registrada como decisão D3.
 *
 * <p><b>Aqui NÃO se lança por falta de recurso.</b> A regra do §71 —
 * {@code SQLFeatureNotSupportedException} para o que o produto não tem — vale
 * para capacidades de EXECUÇÃO: update, batch, transação, cursor rolável. A
 * introspecção responde "não" no vocabulário que o próprio JDBC criou para
 * isso: {@code supportsX()} devolve {@code false}, e um getter de catálogo que
 * não tem nada a mostrar devolve um {@code ResultSet} VAZIO com as colunas
 * certas. Uma ferramenta chama dezenas destes métodos ao abrir a conexão, e
 * exceção onde o contrato já prevê um "não" faz o produto parecer quebrado
 * antes da primeira consulta.
 *
 * <p><b>Os dados saem do fio a cada chamada</b>, e não de um cache. Um dataset
 * com campo {@code auto} muda de schema com a ingestão: uma coluna que o
 * servidor ainda não tinha observado aparece depois, e um cache faria a
 * ferramenta mostrar para sempre a foto do momento da conexão. Estas chamadas
 * não estão no caminho quente.
 *
 * <p><b>O nome da coluna aqui é o do CAMPO, e o do ResultSetMetaData é o de
 * SAÍDA.</b> Para um campo com alias, {@code getColumns} diz {@code status} e o
 * {@code ResultSetMetaData} de um {@code SELECT *} diz {@code estado} — porque
 * um descreve o schema e o outro a projeção, e é o alias que vira chave do
 * objeto JSON de cada linha. Casar os dois por igualdade de nome não funciona,
 * e o alias vai no REMARKS para que a ligação seja possível.
 */
public final class NishiDatabaseMetaData implements DatabaseMetaData {

    private final NishiConnection conexao;

    NishiDatabaseMetaData(NishiConnection conexao) {
        this.conexao = conexao;
    }

    // ------------------------------------------------------------ identidade

    @Override
    public String getDatabaseProductName() {
        return "NishiDB";
    }

    /** O texto de buildinfo do processo, tal como o /v1/stats o publica. */
    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return versaoDoProcesso();
    }

    /**
     * O número extraído de {@code vX.Y}, ou ZERO — nunca exceção.
     *
     * <p>A versão do processo é um texto livre de buildinfo, e pode ser um
     * commit curto num build de desenvolvimento. Lançar aqui derrubaria a
     * abertura da conexão por causa de uma string cosmética.
     */
    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return numeroDaVersao(0);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return numeroDaVersao(1);
    }

    private int numeroDaVersao(int qual) throws SQLException {
        String v = versaoDoProcesso();
        int i = 0;
        while (i < v.length() && (v.charAt(i) == 'v' || v.charAt(i) == 'V')) {
            i++;
        }
        int achados = 0;
        int valor = 0;
        boolean leu = false;
        for (; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c >= '0' && c <= '9') {
                valor = valor * 10 + (c - '0');
                leu = true;
                continue;
            }
            if (leu && achados == qual) {
                return valor;
            }
            if (leu) {
                achados++;
                valor = 0;
                leu = false;
            }
            if (c != '.') {
                return achados == qual ? 0 : 0;
            }
        }
        return leu && achados == qual ? valor : 0;
    }

    private String versaoDoProcesso() throws SQLException {
        JsonValue stats = conexao.transporte().documento("/v1/stats", conexao.networkTimeout());
        JsonValue processo = stats.campo("process");
        return processo == null ? "" : processo.textoOu("version", "");
    }

    @Override
    public String getDriverName() {
        return "NishiDB JDBC";
    }

    @Override
    public String getDriverVersion() {
        return DriverVersion.VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverVersion.MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverVersion.MINOR;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 3;
    }

    @Override
    public String getURL() {
        return conexao.configuracao().url();
    }

    @Override
    public String getUserName() {
        String u = conexao.configuracao().usuario();
        return u == null ? "" : u;
    }

    @Override
    public Connection getConnection() {
        return conexao;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    // -------------------------------------------------------------- catálogo

    /**
     * Uma tabela consultável, já resolvida para o que os getters de catálogo
     * publicam: onde ela mora, como se chama, o que ela é, o comentário e as
     * colunas.
     *
     * <p>{@code schema} é NULO quando o servidor não disse em qual schema a
     * tabela mora — é o "sem schema" do JDBC, e é o que este driver publicava
     * em toda linha antes de o campo existir no fio.
     */
    private record CatalogTable(String schema, String name, String type, String remarks,
            List<JsonValue> columns) {}

    /**
     * TODA tabela que a conexão consulta, sem repetição, na ordem do fio: as do
     * schema da conexão e as do {@code _meta}. O recorte é do servidor
     * ({@code /v1/metadata?schema=}), e é o mesmo escopo em que ele resolve o
     * {@code FROM} — a árvore não mostra o que a conexão não abre, nem esconde
     * o que ela abre.
     *
     * <p>Cada elemento de {@code datasets} publica em {@code tables} o
     * catálogo inteiro do dataset: a base como {@code TABLE} no schema
     * declarado, e o contador e as tabelas do {@code _meta} como {@code VIEW}
     * — o contador no schema do dataset, as de meta no schema {@code _meta}.
     * A ordem dentro de cada dataset é contrato do servidor.
     *
     * <p><b>A repetição que some aqui.</b> As linhas do {@code _meta} chegam
     * uma vez por dataset, porque o atalho {@code _meta.x} significa "no
     * dataset da requisição" e o catálogo de cada dataset o inclui. Elas
     * descrevem a mesma tabela — o nome, o schema e as colunas são os mesmos
     * em todos os datasets —, então a primeira ocorrência de cada par
     * (schema, nome) é a que sai. Sem isto a ferramenta mostraria o
     * {@code _meta} com uma cópia de cada tabela por dataset do schema — o base
     * e a irmã do histórico publicam as duas. O base vem primeiro no fio, e é
     * a cópia dele que sai.
     *
     * <p><b>De onde vem o direito de cortar por (schema, nome).</b> A chave
     * vale para TODA linha, e não só para as do {@code _meta}: duas tabelas
     * homônimas no mesmo schema fariam a segunda sumir da árvore em silêncio.
     * Quem garante que elas não chegam é o SERVIDOR, e na subida — o
     * {@code ValidateSchemaUniqueness} ({@code internal/config/config.go})
     * recusa dois datasets com o mesmo {@code schemaName}, e o
     * {@code validateCounters} ({@code internal/schema/counter.go}) recusa,
     * dentro de um dataset, o contador que se chame como o próprio dataset ou
     * como a tabela irmã do histórico. As três recusas juntas são o que faz o
     * par endereçar uma tabela e só uma no schema; a repetição do
     * {@code _meta} é a única que sobra para cortar aqui.
     *
     * <p>Não há leitura de servidor sem o campo {@code tables}: o recorte por
     * {@code ?schema=} e o campo saem do mesmo servidor, e um binário anterior
     * aos dois recusa a conexão antes de a árvore ser pedida.
     */
    private List<CatalogTable> catalogTables() throws SQLException {
        List<CatalogTable> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (JsonValue d : datasets()) {
            for (JsonValue t : d.itensDe("tables")) {
                String schema = textOrNull(t, "schema");
                String name = t.textoOu("name", "");
                // A chave é o endereço qualificado: nem schema nem nome
                // de tabela contêm ponto, então o par não pode colidir com
                // outro par por causa da concatenação.
                if (!seen.add(schema + "." + name)) {
                    continue;
                }
                // O tipo ausente cai em TABLE, e não em nulo: o filtro de tipo
                // e a ordenação comparam esta string, e um nulo aqui viraria
                // uma linha que nenhum filtro alcança.
                String type = t.textoOu("kind", "TABLE");
                String description = t.textoOu("description", "");
                String remarks = description;
                if (description.isEmpty()) {
                    // A descrição sai vazia em toda linha do servidor de hoje.
                    // Na linha da BASE o driver continua publicando o texto do
                    // dataset, que é o que ele sempre publicou ali.
                    remarks = "TABLE".equals(type) ? datasetRemarks(d) : null;
                }
                out.add(new CatalogTable(schema, name, type, remarks, t.itensDe("columns")));
            }
        }
        return out;
    }

    /** O texto do campo, ou {@code null} quando ele está ausente OU vazio. */
    private static String textOrNull(JsonValue v, String campo) {
        String s = v.textoOu(campo, "");
        return s.isEmpty() ? null : s;
    }

    /** Os tetos do dataset, que o JDBC não modela e a ferramenta mostra. */
    private static String datasetRemarks(JsonValue d) {
        return "chave: " + d.textoOu("key", "") + "; maxRows: " + d.inteiroOu("maxRows", 0)
                + "; timeoutMs: " + d.inteiroOu("timeoutMs", 0);
    }

    /**
     * A ordem do contrato de getTables: TABLE_TYPE, TABLE_CAT, TABLE_SCHEM,
     * TABLE_NAME.
     *
     * <p>O TABLE_CAT não entra porque é NULO em toda linha — o produto não tem
     * catálogo —, e comparar um campo constante não ordena nada. Os outros
     * três variam desde que a base e as views passaram a sair juntas, e uma
     * ferramenta que confia na ordem para agrupar mostraria a árvore
     * embaralhada sem isto.
     *
     * <p>O TABLE_SCHEM aceita nulo — é a linha de um servidor anterior ao
     * campo {@code tables} —, e nulo vem PRIMEIRO. Onde ele aparece, aparece
     * em TODA linha (aquele documento não diz schema nenhum), então a escolha
     * não reordena nada: ela existe para que o comparador não estoure ao
     * encontrá-lo.
     */
    private static final Comparator<Object[]> ORDEM_DE_TABLES =
            Comparator.<Object[], String>comparing(l -> (String) l[3])
                    .thenComparing(l -> (String) l[1], Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(l -> (String) l[2]);

    /** As 10 colunas canônicas de getTables. */
    private static final List<Molduras.Coluna> COLUNAS_TABLES = List.of(
            texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"), texto("TABLE_TYPE"),
            texto("REMARKS"), texto("TYPE_CAT"), texto("TYPE_SCHEM"), texto("TYPE_NAME"),
            texto("SELF_REFERENCING_COL_NAME"), texto("REF_GENERATION"));

    /**
     * A árvore que a ferramenta desenha: a base, os contadores e as tabelas do
     * {@code _meta}, cada uma no schema em que mora.
     *
     * <p>O TABLE_SCHEM é o que faz a ferramenta gerar {@code FROM
     * <schema>.<tabela>} ao abrir os dados de uma tabela — e é o endereço que
     * o servidor resolve. Publicá-lo nulo faria o SQL gerado cair no dataset
     * da conexão, que para toda linha de outro schema é outra tabela.
     *
     * <p>O {@code catalogo} é ignorado: o {@link #getCatalogs()} devolve vazio
     * de propósito, então não há valor que pudesse restringir coisa alguma.
     */
    @Override
    public ResultSet getTables(String catalogo, String padraoDeSchema, String padraoDeTabela,
            String[] tipos) throws SQLException {
        List<Object[]> linhas = new ArrayList<>();
        for (CatalogTable t : catalogTables()) {
            if (!acceptsType(tipos, t.type()) || !matchesSchema(padraoDeSchema, t.schema())
                    || !casa(padraoDeTabela, t.name())) {
                continue;
            }
            linhas.add(new Object[] {null, t.schema(), t.name(), t.type(), t.remarks(),
                null, null, null, null, null});
        }
        linhas.sort(ORDEM_DE_TABLES);
        return resultado(COLUNAS_TABLES, linhas);
    }

    /**
     * O filtro de tipo do contrato de {@code getTables}.
     *
     * <p>{@code null} é "todos os tipos" — é a primeira chamada que uma
     * ferramenta faz, e tratá-lo como "nenhum" deixaria a árvore vazia. Uma
     * lista traz o que ela ENUMERA, e a lista vazia não traz nada porque
     * enumera zero tipos. É por aqui que a pasta de views se abre: o DBeaver
     * pede {@code {"VIEW"}}.
     */
    private static boolean acceptsType(String[] tipos, String tipo) {
        if (tipos == null) {
            return true;
        }
        for (String t : tipos) {
            if (tipo.equals(t)) {
                return true;
            }
        }
        return false;
    }

    /**
     * O padrão de schema do contrato, com o "sem schema" tratado.
     *
     * <p>{@code null} casa tudo, como no padrão de tabela. A linha SEM schema
     * — a que um servidor anterior ao campo {@code tables} produz — casa
     * contra a string VAZIA: é assim que {@code %} continua trazendo tudo e
     * que {@code ""} traz justamente as que não têm schema, como o JDBC manda.
     */
    private static boolean matchesSchema(String padrao, String schema) {
        return casa(padrao, schema == null ? "" : schema);
    }

    /**
     * O SCHEMA de {@code getPrimaryKeys} e {@code getIndexInfo}, que NÃO é
     * padrão: nesses dois o contrato pede o nome do schema, e não um LIKE.
     *
     * <p>{@code null} é "não restrinja por schema" — é o que a ferramenta
     * manda quando já sabe qual tabela quer —, e a string VAZIA pede as linhas
     * SEM schema, que são as de um servidor anterior ao campo. Sem este filtro
     * os dois métodos AFIRMARIAM um TABLE_SCHEM que não conferiram: desde que
     * as duas listas publicam o campo, responder a chave de
     * {@code telecom.eventos} a quem pediu o schema {@code zoo} seria descrever
     * outra tabela. O servidor não produz o par ambíguo — dois datasets com o
     * mesmo {@code schemaName} são recusados na subida —, então o filtro só
     * muda a resposta quando o CHAMADOR pede um schema que não é o da tabela;
     * o que ele fecha é a afirmação, e não um documento observado.
     */
    private static boolean matchesSchemaName(String schema, String actual) {
        return schema == null || schema.equals(actual == null ? "" : actual);
    }

    /** As 24 colunas canônicas de getColumns. */
    private static final List<Molduras.Coluna> COLUNAS_COLUMNS = List.of(
            texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"), texto("COLUMN_NAME"),
            inteiro("DATA_TYPE"), texto("TYPE_NAME"), inteiro("COLUMN_SIZE"), inteiro("BUFFER_LENGTH"),
            inteiro("DECIMAL_DIGITS"), inteiro("NUM_PREC_RADIX"), inteiro("NULLABLE"), texto("REMARKS"),
            texto("COLUMN_DEF"), inteiro("SQL_DATA_TYPE"), inteiro("SQL_DATETIME_SUB"),
            inteiro("CHAR_OCTET_LENGTH"), inteiro("ORDINAL_POSITION"), texto("IS_NULLABLE"),
            texto("SCOPE_CATALOG"), texto("SCOPE_SCHEMA"), texto("SCOPE_TABLE"),
            inteiro("SOURCE_DATA_TYPE"), texto("IS_AUTOINCREMENT"), texto("IS_GENERATEDCOLUMN"));

    /**
     * A ordem do contrato de getColumns: TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
     * ORDINAL_POSITION. O TABLE_CAT fica de fora pela mesma razão do
     * {@link #ORDEM_DE_TABLES} — é nulo em toda linha.
     */
    private static final Comparator<Object[]> ORDEM_DE_COLUMNS =
            Comparator.<Object[], String>comparing(l -> (String) l[1],
                            Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(l -> (String) l[2])
                    .thenComparing(l -> (Long) l[16]);

    /**
     * As colunas de TODA tabela consultável — a base e as views —, pelo mesmo
     * documento de onde a árvore sai.
     *
     * <p>O código do tipo vem do campo {@code jdbc}, calculado pelo SERVIDOR.
     * Rededuzi-lo aqui a partir do nome do tipo criaria uma segunda tabela de
     * tipos no driver, que divergiria da do servidor no primeiro tipo novo.
     *
     * <p>O ORDINAL_POSITION é a posição dentro da TABELA, e conta também a
     * coluna que o padrão descartou: é a posição no schema que o contrato
     * pede, não a posição nesta resposta.
     */
    @Override
    public ResultSet getColumns(String catalogo, String padraoDeSchema, String padraoDeTabela,
            String padraoDeColuna) throws SQLException {
        List<Object[]> linhas = new ArrayList<>();
        for (CatalogTable t : catalogTables()) {
            if (!matchesSchema(padraoDeSchema, t.schema()) || !casa(padraoDeTabela, t.name())) {
                continue;
            }
            int posicao = 0;
            for (JsonValue c : t.columns()) {
                posicao++;
                String nome = c.textoOu("name", "");
                if (!casa(padraoDeColuna, nome)) {
                    continue;
                }
                boolean nulavel = c.booleanoOu("nullable", true);
                linhas.add(new Object[] {null, t.schema(), t.name(), nome,
                    (long) TiposJdbc.codigoDe(c.textoOu("jdbc", "OTHER")),
                    c.textoOu("type", "unknown"),
                    c.inteiroOu("precision", 0), null, c.inteiroOu("scale", 0), 10L,
                    nulavel ? (long) columnNullable : (long) columnNoNulls,
                    // O REMARKS carrega o que o fio tem e o JDBC não modela. O
                    // alias em especial: sem ele, ligar esta linha à coluna que
                    // um SELECT * devolve seria impossível, porque lá o nome é
                    // o de saída.
                    observacoes(c),
                    null, null, null, null, (long) posicao, nulavel ? "YES" : "NO",
                    null, null, null, null, "NO", "NO"});
            }
        }
        linhas.sort(ORDEM_DE_COLUMNS);
        return resultado(COLUNAS_COLUMNS, linhas);
    }

    private static String observacoes(JsonValue c) {
        StringBuilder sb = new StringBuilder();
        JsonValue alias = c.campo("alias");
        if (alias instanceof JsonValue.Texto t) {
            sb.append("alias=").append(t.valor()).append("; ");
        }
        sb.append("index=").append(c.textoOu("index", "none"));
        if (c.booleanoOu("auto", false)) {
            sb.append("; auto=true");
        }
        return sb.toString();
    }

    /**
     * A chave do dataset, que é ganho real: é ela que a ferramenta desenha
     * como chave primária na árvore e usa para montar a edição de uma linha.
     *
     * <p>A chave é publicada POR DATASET no documento, e não por tabela: a
     * única linha que ela descreve é a da base, e por isso esta lista continua
     * saindo dos datasets. O TABLE_SCHEM é o do dataset, e vai preenchido para
     * que a linha descreva a MESMA tabela que o {@link #getTables} publica —
     * uma ferramenta que case as duas por (schema, nome) perderia a chave se
     * aqui ele saísse nulo.
     *
     * <p>O {@code schema} do argumento é honrado, pelo
     * {@link #matchesSchemaName}: publicar o campo e ignorar o filtro seria
     * afirmar um schema que o método não conferiu.
     */
    @Override
    public ResultSet getPrimaryKeys(String catalogo, String schema, String tabela) throws SQLException {
        List<Molduras.Coluna> colunas = List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"),
                texto("TABLE_NAME"), texto("COLUMN_NAME"), inteiro("KEY_SEQ"), texto("PK_NAME"));
        List<Object[]> linhas = new ArrayList<>();
        for (JsonValue d : datasets()) {
            String nome = d.textoOu("name", "");
            String datasetSchema = textOrNull(d, "schema");
            if (!matchesSchemaName(schema, datasetSchema) || !casa(tabela, nome)) {
                continue;
            }
            String chave = d.textoOu("key", "");
            if (!chave.isEmpty()) {
                linhas.add(new Object[] {null, datasetSchema, nome, chave, 1L, "chave"});
            }
        }
        return resultado(colunas, linhas);
    }

    /**
     * O índice DECLARADO no YAML, e o REMARKS diz que é isso.
     *
     * <p>Não é promessa de plano: o índice hash é aceito na configuração desde
     * o M1 e o índice físico ainda não existe, então uma consulta sobre o campo
     * continua caindo em varredura. Publicar sem a ressalva faria alguém
     * concluir, olhando a ferramenta, que a consulta lenta está usando índice.
     *
     * <p>O índice viaja na COLUNA, e as colunas da base são as do campo
     * {@code columns} do dataset — é de lá que esta lista sai, e por isso ela
     * continua percorrendo os datasets. O TABLE_SCHEM vai preenchido pela
     * razão do {@link #getPrimaryKeys}, e o {@code schema} do argumento é
     * honrado pela mesma razão.
     */
    @Override
    public ResultSet getIndexInfo(String catalogo, String schema, String tabela, boolean unico,
            boolean aproximado) throws SQLException {
        List<Molduras.Coluna> colunas = List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"),
                texto("TABLE_NAME"), inteiro("NON_UNIQUE"), texto("INDEX_QUALIFIER"), texto("INDEX_NAME"),
                inteiro("TYPE"), inteiro("ORDINAL_POSITION"), texto("COLUMN_NAME"), texto("ASC_OR_DESC"),
                inteiro("CARDINALITY"), inteiro("PAGES"), texto("FILTER_CONDITION"));
        List<Object[]> linhas = new ArrayList<>();
        for (JsonValue d : datasets()) {
            String nome = d.textoOu("name", "");
            String datasetSchema = textOrNull(d, "schema");
            if (!matchesSchemaName(schema, datasetSchema) || !casa(tabela, nome)) {
                continue;
            }
            for (JsonValue c : d.itensDe("columns")) {
                String indice = c.textoOu("index", "none");
                if ("none".equals(indice)) {
                    continue;
                }
                if (unico) {
                    continue; // nenhum índice do NishiDB é único
                }
                linhas.add(new Object[] {null, datasetSchema, nome, 1L, null,
                    indice + " (declarado no YAML, não é garantia de plano)",
                    (long) tableIndexOther, 1L, c.textoOu("name", ""), null, 0L, 0L, null});
            }
        }
        return resultado(colunas, linhas);
    }

    /**
     * Os tipos que ESTE servidor de fato publica, derivados de
     * {@code /v1/metadata}.
     *
     * <p>A alternativa seria uma segunda tabela de tipos escrita à mão no
     * driver, e ela divergiria da do servidor no primeiro tipo novo — que é
     * exatamente o defeito que a tabela única do lado Go existe para evitar. O
     * preço, declarado: um tipo que nenhum dataset usa não aparece aqui. Como o
     * NishiDB não faz DDL, ninguém escolhe um tipo por esta lista.
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Molduras.Coluna> colunas = List.of(texto("TYPE_NAME"), inteiro("DATA_TYPE"),
                inteiro("PRECISION"), texto("LITERAL_PREFIX"), texto("LITERAL_SUFFIX"),
                texto("CREATE_PARAMS"), inteiro("NULLABLE"), texto("CASE_SENSITIVE"),
                inteiro("SEARCHABLE"), texto("UNSIGNED_ATTRIBUTE"), texto("FIXED_PREC_SCALE"),
                texto("AUTO_INCREMENT"), texto("LOCAL_TYPE_NAME"), inteiro("MINIMUM_SCALE"),
                inteiro("MAXIMUM_SCALE"), inteiro("SQL_DATA_TYPE"), inteiro("SQL_DATETIME_SUB"),
                inteiro("NUM_PREC_RADIX"));
        Map<String, Object[]> porTipo = new LinkedHashMap<>();
        for (JsonValue d : datasets()) {
            for (JsonValue c : d.itensDe("columns")) {
                String tipo = c.textoOu("type", "unknown");
                String jdbc = c.textoOu("jdbc", "OTHER");
                boolean texto = "VARCHAR".equals(jdbc);
                porTipo.putIfAbsent(tipo, new Object[] {tipo,
                    (long) TiposJdbc.codigoDe(jdbc), c.inteiroOu("precision", 0),
                    texto ? "'" : null, texto ? "'" : null, null,
                    (long) typeNullable, Boolean.TRUE.toString(),
                    // bytes e json NÃO são pesquisáveis: o binder recusa
                    // comparação sobre os dois, e anunciá-los pesquisáveis
                    // faria a ferramenta montar um WHERE que o servidor recusa.
                    TiposJdbc.pesquisavel(tipo) ? (long) typeSearchable : (long) typePredNone,
                    Boolean.toString(!TiposJdbc.assinado(jdbc)), "false", "false", tipo,
                    0L, c.inteiroOu("scale", 0), null, null, 10L});
            }
        }
        return resultado(colunas, new ArrayList<>(porTipo.values()));
    }

    /**
     * VAZIO com a forma certa — a errata D3 em ação.
     *
     * <p>O NishiDB não tem catálogo: o endereço qualificado que o servidor
     * resolve tem DOIS níveis, e o de cima é o schema. Lançar aqui faria a
     * ferramenta mostrar erro ao abrir a árvore de navegação; um ResultSet
     * vazio com a coluna certa é o "não" que o próprio JDBC criou para este
     * caso.
     *
     * <p>É por isso que o TABLE_CAT sai nulo em toda linha dos outros getters:
     * não existe valor que ele pudesse carregar.
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return resultado(List.of(texto("TABLE_CAT")), List.of());
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    /**
     * Os schemas que a conexão enxerga, DISTINTOS e ordenados: o {@code _meta}
     * e o schema da conexão, e nenhum outro.
     *
     * <p>São as pastas da árvore da ferramenta, e é sobre elas que ela pede as
     * tabelas de cada uma. Saem da MESMA montagem de catálogo que o
     * {@link #getTables} usa, e não de uma leitura própria do documento: duas
     * derivações da mesma lista divergiriam, e a divergência apareceria como
     * uma pasta que abre vazia.
     *
     * <p>O {@code catalogo} é ignorado, pela razão do {@link #getCatalogs()}.
     */
    @Override
    public ResultSet getSchemas(String catalogo, String padrao) throws SQLException {
        Set<String> nomes = new TreeSet<>();
        for (CatalogTable t : catalogTables()) {
            if (t.schema() != null && casa(padrao, t.schema())) {
                nomes.add(t.schema());
            }
        }
        List<Object[]> linhas = new ArrayList<>(nomes.size());
        for (String nome : nomes) {
            linhas.add(new Object[] {nome, null});
        }
        return resultado(List.of(texto("TABLE_SCHEM"), texto("TABLE_CATALOG")), linhas);
    }

    /**
     * TABLE e VIEW — os dois tipos do catálogo do servidor, e nunca
     * {@code SYSTEM TABLE}.
     *
     * <p>A base é TABLE; o contador e as tabelas do {@code _meta} são VIEW. O
     * terceiro tipo seria o natural para as de meta, e é justamente o que não
     * se pode usar: o DBeaver esconde objeto de sistema por default, e a opção
     * que o revela não está onde alguém a procuraria. Tabela que existe para
     * ser descoberta não pode nascer invisível.
     *
     * <p>A lista é FIXA, e não derivada do documento: ela diz o que este
     * driver sabe publicar, e é respondida antes de a ferramenta perguntar
     * quais tabelas existem. Contra um servidor anterior ao campo
     * {@code tables}, o {@link #getTables} simplesmente não devolve linha
     * nenhuma de tipo VIEW.
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return resultado(List.of(texto("TABLE_TYPE")),
                List.<Object[]>of(new Object[] {"TABLE"}, new Object[] {"VIEW"}));
    }

    // Os demais getters de catálogo: vazios COM A FORMA CERTA, pela mesma
    // razão. Chave estrangeira, procedimento, tipo definido pelo usuário,
    // privilégio e super-tabela não existem no produto.

    @Override
    public ResultSet getProcedures(String c, String s, String p) throws SQLException {
        return resultado(List.of(texto("PROCEDURE_CAT"), texto("PROCEDURE_SCHEM"), texto("PROCEDURE_NAME"),
                texto("RESERVED1"), texto("RESERVED2"), texto("RESERVED3"), texto("REMARKS"),
                inteiro("PROCEDURE_TYPE"), texto("SPECIFIC_NAME")), List.of());
    }

    @Override
    public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException {
        return resultado(List.of(texto("PROCEDURE_CAT"), texto("PROCEDURE_SCHEM"), texto("PROCEDURE_NAME"),
                texto("COLUMN_NAME"), inteiro("COLUMN_TYPE"), inteiro("DATA_TYPE"), texto("TYPE_NAME"),
                inteiro("PRECISION"), inteiro("LENGTH"), inteiro("SCALE"), inteiro("RADIX"),
                inteiro("NULLABLE"), texto("REMARKS")), List.of());
    }

    @Override
    public ResultSet getFunctions(String c, String s, String p) throws SQLException {
        return resultado(List.of(texto("FUNCTION_CAT"), texto("FUNCTION_SCHEM"), texto("FUNCTION_NAME"),
                texto("REMARKS"), inteiro("FUNCTION_TYPE"), texto("SPECIFIC_NAME")), List.of());
    }

    @Override
    public ResultSet getFunctionColumns(String c, String s, String p, String col) throws SQLException {
        return resultado(List.of(texto("FUNCTION_CAT"), texto("FUNCTION_SCHEM"), texto("FUNCTION_NAME"),
                texto("COLUMN_NAME"), inteiro("COLUMN_TYPE"), inteiro("DATA_TYPE"), texto("TYPE_NAME"),
                inteiro("PRECISION"), inteiro("LENGTH"), inteiro("SCALE"), inteiro("RADIX"),
                inteiro("NULLABLE"), texto("REMARKS")), List.of());
    }

    private static final List<Molduras.Coluna> COLUNAS_CHAVE_ESTRANGEIRA = List.of(
            texto("PKTABLE_CAT"), texto("PKTABLE_SCHEM"), texto("PKTABLE_NAME"), texto("PKCOLUMN_NAME"),
            texto("FKTABLE_CAT"), texto("FKTABLE_SCHEM"), texto("FKTABLE_NAME"), texto("FKCOLUMN_NAME"),
            inteiro("KEY_SEQ"), inteiro("UPDATE_RULE"), inteiro("DELETE_RULE"), texto("FK_NAME"),
            texto("PK_NAME"), inteiro("DEFERRABILITY"));

    @Override
    public ResultSet getImportedKeys(String c, String s, String t) throws SQLException {
        return resultado(COLUNAS_CHAVE_ESTRANGEIRA, List.of());
    }

    @Override
    public ResultSet getExportedKeys(String c, String s, String t) throws SQLException {
        return resultado(COLUNAS_CHAVE_ESTRANGEIRA, List.of());
    }

    @Override
    public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft)
            throws SQLException {
        return resultado(COLUNAS_CHAVE_ESTRANGEIRA, List.of());
    }

    @Override
    public ResultSet getTablePrivileges(String c, String s, String p) throws SQLException {
        return resultado(List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"),
                texto("GRANTOR"), texto("GRANTEE"), texto("PRIVILEGE"), texto("IS_GRANTABLE")), List.of());
    }

    @Override
    public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException {
        return resultado(List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"),
                texto("COLUMN_NAME"), texto("GRANTOR"), texto("GRANTEE"), texto("PRIVILEGE"),
                texto("IS_GRANTABLE")), List.of());
    }

    @Override
    public ResultSet getBestRowIdentifier(String c, String s, String t, int escopo, boolean nulavel)
            throws SQLException {
        return resultado(List.of(inteiro("SCOPE"), texto("COLUMN_NAME"), inteiro("DATA_TYPE"),
                texto("TYPE_NAME"), inteiro("COLUMN_SIZE"), inteiro("BUFFER_LENGTH"),
                inteiro("DECIMAL_DIGITS"), inteiro("PSEUDO_COLUMN")), List.of());
    }

    @Override
    public ResultSet getVersionColumns(String c, String s, String t) throws SQLException {
        return resultado(List.of(inteiro("SCOPE"), texto("COLUMN_NAME"), inteiro("DATA_TYPE"),
                texto("TYPE_NAME"), inteiro("COLUMN_SIZE"), inteiro("BUFFER_LENGTH"),
                inteiro("DECIMAL_DIGITS"), inteiro("PSEUDO_COLUMN")), List.of());
    }

    @Override
    public ResultSet getUDTs(String c, String s, String p, int[] tipos) throws SQLException {
        return resultado(List.of(texto("TYPE_CAT"), texto("TYPE_SCHEM"), texto("TYPE_NAME"),
                texto("CLASS_NAME"), inteiro("DATA_TYPE"), texto("REMARKS"), inteiro("BASE_TYPE")),
                List.of());
    }

    @Override
    public ResultSet getSuperTypes(String c, String s, String p) throws SQLException {
        return resultado(List.of(texto("TYPE_CAT"), texto("TYPE_SCHEM"), texto("TYPE_NAME"),
                texto("SUPERTYPE_CAT"), texto("SUPERTYPE_SCHEM"), texto("SUPERTYPE_NAME")), List.of());
    }

    @Override
    public ResultSet getSuperTables(String c, String s, String p) throws SQLException {
        return resultado(List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"),
                texto("SUPERTABLE_NAME")), List.of());
    }

    @Override
    public ResultSet getAttributes(String c, String s, String p, String a) throws SQLException {
        return resultado(List.of(texto("TYPE_CAT"), texto("TYPE_SCHEM"), texto("TYPE_NAME"),
                texto("ATTR_NAME"), inteiro("DATA_TYPE"), texto("ATTR_TYPE_NAME"), inteiro("ATTR_SIZE"),
                inteiro("DECIMAL_DIGITS"), inteiro("NUM_PREC_RADIX"), inteiro("NULLABLE"),
                texto("REMARKS")), List.of());
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return resultado(List.of(texto("NAME"), inteiro("MAX_LEN"), texto("DEFAULT_VALUE"),
                texto("DESCRIPTION")), List.of());
    }

    @Override
    public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException {
        return resultado(List.of(texto("TABLE_CAT"), texto("TABLE_SCHEM"), texto("TABLE_NAME"),
                texto("COLUMN_NAME"), inteiro("DATA_TYPE"), inteiro("COLUMN_SIZE"),
                inteiro("DECIMAL_DIGITS"), inteiro("NUM_PREC_RADIX"), texto("COLUMN_USAGE"),
                texto("REMARKS"), inteiro("CHAR_OCTET_LENGTH"), texto("IS_NULLABLE")), List.of());
    }

    // ------------------------------------------------------------- o que há

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    /** Identificadores distinguem caixa, e o binder resolve nome EXATO. */
    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    /**
     * Não há procedimento a chamar, então a pergunta é vazia — e "true" seria
     * a resposta literal ("todos os zero são chamáveis"), que faria uma
     * ferramenta abrir a pasta de procedimentos.
     */
    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean supportsResultSetType(int tipo) {
        return tipo == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int tipo, int concorrencia) {
        return tipo == ResultSet.TYPE_FORWARD_ONLY && concorrencia == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public String getSQLKeywords() {
        // As palavras que o servidor reconhece ALEM do SQL padrao. Vazio e a
        // resposta certa, e a razao mudou: nao e que o lexer nao tenha
        // keywords, e que todas as que ele tem (SELECT, FROM, WHERE, LIMIT,
        // AND, OR, IS, NOT, NULL, TRUE, FALSE) ja sao SQL padrao. LIKE tambem
        // e, e por isso nao entra aqui.
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return "";
    }

    /**
     * "schema" — o termo do produto, e é ele que a ferramenta escreve na
     * árvore. Vazio era a resposta de quando não havia o que nomear.
     */
    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogSeparator() {
        return "";
    }

    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() {
        // Não há AS na v1: o alias existe no SCHEMA do dataset, não na query.
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    /**
     * O servidor ORDENA por coluna que não está na projeção.
     *
     * <p>Medido, e não deduzido: {@code SELECT identifier FROM events ORDER BY
     * severity} devolve as linhas ordenadas. O executor coleta a chave de
     * ordenação junto da projeção e a descarta na serialização, então a coluna
     * não precisa aparecer no SELECT.
     *
     * <p>Declarar {@code false} aqui não é conservador — é errado, e custa
     * caro no lugar onde este método é lido: um construtor de query gráfico
     * (DBeaver, JasperReports) desabilita a ordenação por coluna não
     * projetada, e o usuário conclui que o banco não faz o que ele faz.
     */
    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        // LIKE existe desde o ciclo M6, e a cláusula ESCAPE nao. Este `false`
        // deixou de ser vacuidade e passou a ser afirmacao verdadeira: sem
        // ESCAPE, procurar um `%` ou um `_` LITERAL e impossivel no servidor.
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int de, int para) {
        return false;
    }

    /**
     * TRUE: o {@code FROM <schema>.<tabela>} é resolvido pelo servidor.
     *
     * <p>É a única das cinco que responde sim. As outras quatro cobrem DDL,
     * procedimento e privilégio, que o produto não tem — aqui a pergunta é
     * sobre a consulta, e a consulta aceita o qualificador. Responder não
     * faria um construtor de query desqualificar o nome, e o nome sem
     * qualificador significa "no dataset da conexão": para toda tabela de
     * outro schema, outra tabela.
     */
    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int nivel) {
        return nivel == Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsSharding() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    // Cursores só para frente não veem mudança de ninguém, e não há escrita.
    @Override
    public boolean ownUpdatesAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int tipo) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int tipo) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int tipo) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int tipo) {
        return false;
    }

    // ------------------------------------------------------------- limites
    //
    // Zero é a convenção do JDBC para "sem limite conhecido", e é a resposta
    // certa aqui: os tetos reais do produto são por dataset (maxRows,
    // timeoutMs) e saem por getTables, não por estas constantes globais.
    //
    // A exceção é o ORDER BY, que tem teto global de verdade — ver
    // getMaxColumnsInOrderBy.

    /**
     * Teto de chaves do ORDER BY, espelhando {@code MaxOrderKeys} do parser
     * do servidor (internal/query/parser/parser.go).
     *
     * <p>O número é duplicado aqui porque o driver não fala com o servidor
     * para responder a {@link java.sql.DatabaseMetaData}: ele responde antes
     * de qualquer query. Duplicar um número que muda junto do formato de fio
     * é o custo aceito; o que não seria aceitável é responder "sem limite"
     * para um limite que existe.
     */
    private static final int MAX_CHAVES_DE_ORDENACAO = 16;

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    /**
     * Dezesseis, e não zero: o ORDER BY tem teto REAL.
     *
     * <p>É a exceção da regra do bloco acima. Os outros limites são "sem
     * limite conhecido" porque os tetos do produto são por dataset; este é uma
     * constante do parser ({@code MaxOrderKeys}, internal/query/parser),
     * global e conferida em toda query. Responder zero seria dizer que não há
     * teto para um limite que existe — e quem monta ORDER BY por programa
     * descobriria o número por erro de sintaxe em produção.
     */
    @Override
    public int getMaxColumnsInOrderBy() {
        return MAX_CHAVES_DE_ORDENACAO;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    /** O teto REAL do corpo de uma consulta, e não um zero de conveniência. */
    @Override
    public int getMaxStatementLength() {
        return 1 << 20;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        // Uma: a v1 não tem JOIN. É um limite REAL, e publicá-lo evita que uma
        // ferramenta ofereça o construtor visual de junção.
        return 1;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    // -------------------------------------------------------------- interno

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("este DatabaseMetaData não embrulha " + alvo.getName(),
                SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    /**
     * Os datasets do SCHEMA DA CONEXÃO — o base e, quando há, a irmã do
     * histórico. É o recorte que faz a árvore mostrar só o schema da conexão e
     * o {@code _meta}, que é o que o servidor abre para ela.
     */
    private List<JsonValue> datasets() throws SQLException {
        return conexao.transporte()
                .documento(NishiConnection.metadataPath(conexao.schema()), conexao.networkTimeout())
                .itensDe("datasets");
    }

    private ResultSet resultado(List<Molduras.Coluna> colunas, List<Object[]> linhas) {
        return new NishiResultSet(new ArrayCursor(colunas, linhas), null);
    }

    private static Molduras.Coluna texto(String nome) {
        return new Molduras.Coluna(nome, "string", "VARCHAR", true, 0, 0, 0);
    }

    private static Molduras.Coluna inteiro(String nome) {
        return new Molduras.Coluna(nome, "int64", "BIGINT", true, 19, 0, 20);
    }

    /**
     * O casamento LIKE do JDBC, implementado aqui.
     *
     * <p>{@code null} casa tudo — é o contrato, e não "nenhum". Confundir os
     * dois faria a árvore da ferramenta aparecer vazia numa chamada sem filtro,
     * que é justamente a primeira que ela faz.
     */
    static boolean casa(String padrao, String nome) {
        if (padrao == null) {
            return true;
        }
        return regexDe(padrao).matcher(nome).matches();
    }

    private static java.util.regex.Pattern regexDe(String padrao) {
        StringBuilder sb = new StringBuilder(padrao.length() + 8);
        for (int i = 0; i < padrao.length(); i++) {
            char c = padrao.charAt(i);
            if (c == '\\' && i + 1 < padrao.length()) {
                sb.append(java.util.regex.Pattern.quote(String.valueOf(padrao.charAt(++i))));
            } else if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else {
                sb.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        return java.util.regex.Pattern.compile(sb.toString(), java.util.regex.Pattern.DOTALL);
    }
}
