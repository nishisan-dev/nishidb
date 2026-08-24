package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.json.JsonTipo;
import dev.nishisan.nishidb.jdbc.wire.Molduras;
import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import dev.nishisan.nishidb.jdbc.wire.Temporais;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * O ResultSet do driver — UM só, sobre a SPI {@link RowCursor}.
 *
 * <p>Um só de propósito: as duas fontes de linha do driver (o stream NDJSON de
 * uma consulta e as tabelas sintéticas do DatabaseMetaData) entram pela mesma
 * porta, e é isso que impede uma correção de conversão de valer só numa delas.
 *
 * <h2>A regra da conversão</h2>
 *
 * <p><b>{@code getString} devolve o TEXTO DO FIO</b>, para qualquer tipo. É
 * lossless, é grátis — o texto já está no buffer — e resolve o problema de fuso
 * sem mentir: a aplicação vê exatamente o que o servidor escreveu. Em especial,
 * o double NÃO é recomposto por {@code Double.toString}: as duas grafias
 * decodificam para o mesmo binary64, mas só uma é a que o servidor emitiu, e
 * quem exporta CSV pelo driver e pelo CLI espera os mesmos bytes.
 *
 * <p><b>O resto é dirigido pelo TOKEN, e o tipo declarado só entra onde o token
 * é ambíguo.</b> Um texto que é instante, um que é hora do dia e um que é
 * base64 têm a mesma cara no JSON, então quem os distingue é a gramática (para
 * os dois primeiros) e o tipo declarado (para o binário). Fora daí, o driver
 * não adivinha.
 *
 * <h2>Nulos que o fio produz e não são NULL</h2>
 *
 * <p>Quatro situações fazem o encoder emitir {@code null} sem que a coluna
 * esteja nula: double não-finito (NaN e infinitos não existem em JSON),
 * {@code time} fora do dia, documento {@code json} vazio e valor fisicamente
 * malformado. Nenhuma é distinguível de um NULL de verdade DAQUI, e nenhuma
 * deve ser "consertada" no driver: as quatro são inalcançáveis pelo caminho
 * normal de ingestão, e inventar um sinal para elas exigiria mudar o fio.
 * Ficam registradas para que quem investigar um null inexplicável saiba onde
 * procurar — no dado, não neste arquivo.
 */
public final class NishiResultSet implements ResultSet {

    private final RowCursor cursor;
    private final Statement dono;
    private final NishiResultSetMetaData meta;

    private boolean fechado;
    private boolean emLinha;
    private int linha;
    private boolean ultimoFoiNulo;
    private SQLWarning avisos;
    private boolean avisoDoCursorAbsorvido;

    public NishiResultSet(RowCursor cursor, Statement dono) {
        this.cursor = cursor;
        this.dono = dono;
        this.meta = new NishiResultSetMetaData(cursor.colunas(), cursor.dataset());
    }

    // ------------------------------------------------------------- navegação

    @Override
    public boolean next() throws SQLException {
        exigeAberto();
        boolean tem = cursor.proxima();
        absorveAviso();
        emLinha = tem;
        if (tem) {
            linha++;
        }
        return tem;
    }

    @Override
    public void close() throws SQLException {
        if (fechado) {
            return;
        }
        fechado = true;
        emLinha = false;
        // Fechar o cursor devolve a conexão HTTP: um stream não consumido nem
        // fechado nunca volta ao pool, e é assim que a ferramenta "trava depois
        // de vinte consultas".
        cursor.close();
    }

    @Override
    public boolean isClosed() {
        return fechado;
    }

    @Override
    public int getRow() throws SQLException {
        exigeAberto();
        return emLinha ? linha : 0;
    }

    @Override
    public boolean wasNull() throws SQLException {
        exigeAberto();
        return ultimoFoiNulo;
    }

    // --------------------------------------------------------------- getters

    @Override
    public String getString(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        return cursor.texto(i);
    }

    @Override
    public boolean getBoolean(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return false;
        }
        switch (cursor.tipo(i)) {
            case BOOLEANO:
                return cursor.booleano(i);
            case NUMERO:
                return cursor.numero(i) != 0;
            case TEXTO:
                String t = cursor.texto(i);
                if (t.equalsIgnoreCase("true") || t.equals("1")) {
                    return true;
                }
                if (t.equalsIgnoreCase("false") || t.equals("0")) {
                    return false;
                }
                throw naoConverte(i, t, "boolean");
            default:
                throw naoConverte(i, cursor.texto(i), "boolean");
        }
    }

    @Override
    public byte getByte(int coluna) throws SQLException {
        return (byte) inteiroEntre(coluna, Byte.MIN_VALUE, Byte.MAX_VALUE, "byte");
    }

    @Override
    public short getShort(int coluna) throws SQLException {
        return (short) inteiroEntre(coluna, Short.MIN_VALUE, Short.MAX_VALUE, "short");
    }

    @Override
    public int getInt(int coluna) throws SQLException {
        return (int) inteiroEntre(coluna, Integer.MIN_VALUE, Integer.MAX_VALUE, "int");
    }

    @Override
    public long getLong(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return 0L;
        }
        switch (cursor.tipo(i)) {
            case NUMERO:
                if (cursor.integral(i)) {
                    return cursor.inteiro(i);
                }
                return semArredondar(i, cursor.numero(i));
            case TEXTO:
                return cursor.inteiro(i);
            case BOOLEANO:
                return cursor.booleano(i) ? 1L : 0L;
            default:
                throw naoConverte(i, cursor.texto(i), "long");
        }
    }

    @Override
    public float getFloat(int coluna) throws SQLException {
        return (float) getDouble(coluna);
    }

    @Override
    public double getDouble(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return 0d;
        }
        switch (cursor.tipo(i)) {
            case NUMERO:
            case TEXTO:
                return cursor.numero(i);
            case BOOLEANO:
                return cursor.booleano(i) ? 1d : 0d;
            default:
                throw naoConverte(i, cursor.texto(i), "double");
        }
    }

    /**
     * Vem do TEXTO do fio, e nunca de um double já convertido.
     *
     * <p>{@code new BigDecimal(getDouble())} sobre {@code 0.1} produziria
     * {@code 0.1000000000000000055511151231257827…} — o valor exato do binary64
     * mais próximo, que não é o número que o servidor escreveu.
     */
    @Override
    public BigDecimal getBigDecimal(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        JsonTipo t = cursor.tipo(i);
        if (t == JsonTipo.BOOLEANO) {
            return cursor.booleano(i) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        if (t != JsonTipo.NUMERO && t != JsonTipo.TEXTO) {
            throw naoConverte(i, cursor.texto(i), "BigDecimal");
        }
        String texto = cursor.texto(i);
        try {
            return new BigDecimal(texto);
        } catch (NumberFormatException e) {
            throw naoConverte(i, texto, "BigDecimal");
        }
    }

    /**
     * Só a coluna binária, e é decisão contra a convenção do JDBC.
     *
     * <p>Devolver os bytes UTF-8 de uma coluna de texto seria o costume, mas
     * aqui ele é ambíguo de um jeito perigoso: uma coluna {@code bytes} que o
     * dataset ainda não fixou chega anunciada OTHER, e o mesmo método
     * devolveria ora o binário decodificado, ora os caracteres do base64 — sem
     * nada acusar. Falhar nomeando o método a usar é a única resposta que não
     * engana.
     */
    @Override
    public byte[] getBytes(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        if (!"VARBINARY".equals(cursor.colunas().get(i).jdbc())) {
            throw new SQLDataException("coluna " + nome(i) + " nao e binaria: getBytes decodificaria "
                    + "base64 de um texto que talvez nao seja base64; use getString",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        String texto = cursor.texto(i);
        try {
            // Decoder padrão: alfabeto std COM padding, espelhando o
            // base64.StdEncoding do encoder. O de URL falharia só nas linhas
            // cujo blob contém '+' ou '/', que é falha dependente de dado.
            return Base64.getDecoder().decode(texto);
        } catch (IllegalArgumentException e) {
            throw new SQLDataException("coluna " + nome(i) + ": base64 invalido (" + e.getMessage() + ")",
                    SqlStates.CONVERSAO_INVALIDA);
        }
    }

    // -------------------------------------------------------------- temporais

    @Override
    public Timestamp getTimestamp(int coluna) throws SQLException {
        Long millis = instanteDe(coluna);
        return millis == null ? null : Temporais.timestampDe(millis);
    }

    /**
     * O Calendar é IGNORADO, e a divergência é deliberada.
     *
     * <p>O contrato do Calendar é situar um relógio de parede que não carrega
     * fuso. Um timestamp do NishiDB já é um instante absoluto: não há relógio
     * de parede a situar, e aplicar o offset deslocaria o valor — a mesma linha
     * passaria a valer coisas diferentes em duas máquinas. Quem quer o
     * lossless usa {@code getObject(col, Instant.class)}.
     */
    @Override
    public Timestamp getTimestamp(int coluna, Calendar cal) throws SQLException {
        return getTimestamp(coluna);
    }

    @Override
    public Time getTime(int coluna) throws SQLException {
        return getTime(coluna, null);
    }

    /**
     * Aqui o Calendar SIM tem função — é o único getter em que ele muda a
     * resposta.
     *
     * <p>A hora do dia é um relógio de parede sem fuso, e o Calendar diz em que
     * fuso situá-lo. Sem Calendar, o fuso é o da JVM, que é o default do JDBC.
     */
    @Override
    public Time getTime(int coluna, Calendar cal) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        ZoneId zona = cal == null ? ZoneId.systemDefault() : cal.getTimeZone().toZoneId();
        return Temporais.timeDe(horaDoDiaDe(i), zona);
    }

    @Override
    public Date getDate(int coluna) throws SQLException {
        return getDate(coluna, null);
    }

    /**
     * Sem Calendar, a divisão em dias é feita em UTC — e não no fuso da JVM.
     *
     * <p>É a mesma doutrina do encoder, que se recusa a renderizar em hora
     * local: com o fuso da JVM, o MESMO instante cairia em dias diferentes
     * conforme a máquina que rodou o relatório. Com Calendar, o calendário
     * pedido manda, que é o uso legítimo do parâmetro.
     */
    @Override
    public Date getDate(int coluna, Calendar cal) throws SQLException {
        Long millis = instanteDe(coluna);
        if (millis == null) {
            return null;
        }
        ZoneId zona = cal == null ? ZoneOffset.UTC : cal.getTimeZone().toZoneId();
        return Temporais.dateDe(millis, zona);
    }

    /** O instante da coluna, ou null. Texto passa pela gramática do fio. */
    private Long instanteDe(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        switch (cursor.tipo(i)) {
            case TEXTO:
                return comColuna(i, () -> Temporais.instante(cursor.texto(i)));
            case NUMERO:
                if (cursor.integral(i)) {
                    return cursor.inteiro(i);
                }
                throw naoConverte(i, cursor.texto(i), "instante");
            default:
                throw naoConverte(i, cursor.texto(i), "instante");
        }
    }

    private long horaDoDiaDe(int i) throws SQLException {
        if (cursor.tipo(i) == JsonTipo.TEXTO) {
            return comColuna(i, () -> Temporais.horaDoDia(cursor.texto(i)));
        }
        if (cursor.tipo(i) == JsonTipo.NUMERO && cursor.integral(i)) {
            long millis = cursor.inteiro(i);
            if (millis < 0 || millis >= 86_400_000L) {
                throw new SQLDataException("coluna " + nome(i) + ": " + millis
                        + " esta fora do dia", SqlStates.FORMATO_TEMPORAL_INVALIDO);
            }
            return millis;
        }
        throw naoConverte(i, cursor.texto(i), "hora do dia");
    }

    // --------------------------------------------------------------- objetos

    /**
     * O TIPO DECLARADO manda, e OTHER cai no token.
     *
     * <p>Deixar o token decidir sempre seria mais simples e estaria errado: um
     * {@code double} de valor inteiro sai do encoder como {@code 1}, sem ponto,
     * e a coluna inteira apareceria como Long numa ferramenta. Quem sabe que
     * aquilo é um double é o schema.
     *
     * <p>A exceção é a coluna OTHER — um campo auto que nenhum evento fixou.
     * Ela é a única cujo metadado pode discordar do dado, porque a ingestão é
     * concorrente: pode ser anunciada OTHER no bind e trazer um inteiro na
     * linha seguinte. Nela, e só nela, responde o token efetivo.
     */
    @Override
    public Object getObject(int coluna) throws SQLException {
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        switch (cursor.colunas().get(i).jdbc()) {
            case "BIGINT":
                return getLong(coluna);
            case "DOUBLE":
                return getDouble(coluna);
            case "BOOLEAN":
                return getBoolean(coluna);
            case "TIMESTAMP":
                return getTimestamp(coluna);
            case "TIME":
                return getTime(coluna);
            case "VARBINARY":
                return getBytes(coluna);
            case "VARCHAR":
                return getString(coluna);
            default:
                return porToken(i, coluna);
        }
    }

    private Object porToken(int i, int coluna) throws SQLException {
        switch (cursor.tipo(i)) {
            case NUMERO:
                return cursor.integral(i) ? (Object) cursor.inteiro(i) : (Object) cursor.numero(i);
            case BOOLEANO:
                return cursor.booleano(i);
            default:
                return getString(coluna);
        }
    }

    /**
     * As conversões explícitas — e é por aqui que o usuário escapa das duas
     * divergências deliberadas do driver.
     *
     * <p>{@code Instant.class} devolve o instante sem passar por
     * {@code java.sql.Timestamp}, e {@code LocalTime.class} devolve a hora do
     * dia sem passar por {@code java.sql.Time} — os dois tipos legados cuja
     * conversão depende do fuso da JVM.
     *
     * <p>{@code LocalDate}, {@code LocalDateTime} e {@code OffsetDateTime} saem
     * em UTC, que é o fuso do fio: um default de fuso da JVM aqui faria a mesma
     * linha valer dias diferentes em duas máquinas.
     */
    @Override
    public <T> T getObject(int coluna, Class<T> alvo) throws SQLException {
        if (alvo == null) {
            throw new SQLException("getObject sem classe alvo", SqlStates.CONVERSAO_INVALIDA);
        }
        int i = indice(coluna);
        if (nulo(i)) {
            return null;
        }
        Object v = converte(coluna, i, alvo);
        if (v == null) {
            throw new SQLFeatureNotSupportedException("este driver nao converte a coluna " + nome(i)
                    + " para " + alvo.getName(), SqlStates.NAO_SUPORTADO);
        }
        if (alvo.isPrimitive()) {
            // Class.cast recusa o valor JÁ EMBRULHADO contra um Class
            // primitivo, então o cast tem de ser o não-checado — que é seguro
            // aqui porque quem escolheu o alvo foi o chamador.
            @SuppressWarnings("unchecked")
            T primitivo = (T) v;
            return primitivo;
        }
        return alvo.cast(v);
    }

    private Object converte(int coluna, int i, Class<?> alvo) throws SQLException {
        if (alvo == String.class) {
            return getString(coluna);
        }
        if (alvo == Boolean.class || alvo == boolean.class) {
            return getBoolean(coluna);
        }
        if (alvo == Byte.class || alvo == byte.class) {
            return getByte(coluna);
        }
        if (alvo == Short.class || alvo == short.class) {
            return getShort(coluna);
        }
        if (alvo == Integer.class || alvo == int.class) {
            return getInt(coluna);
        }
        if (alvo == Long.class || alvo == long.class) {
            return getLong(coluna);
        }
        if (alvo == Float.class || alvo == float.class) {
            return getFloat(coluna);
        }
        if (alvo == Double.class || alvo == double.class) {
            return getDouble(coluna);
        }
        if (alvo == BigDecimal.class) {
            return getBigDecimal(coluna);
        }
        if (alvo == byte[].class) {
            return getBytes(coluna);
        }
        if (alvo == Timestamp.class) {
            return getTimestamp(coluna);
        }
        if (alvo == Time.class) {
            return getTime(coluna);
        }
        if (alvo == Date.class) {
            return getDate(coluna);
        }
        if (alvo == Instant.class) {
            Long millis = instanteDe(coluna);
            return millis == null ? null : Instant.ofEpochMilli(millis);
        }
        if (alvo == LocalTime.class) {
            // Numa coluna time é a hora do dia; num instante é a hora EM UTC,
            // dita aqui e não adivinhada.
            if (cursor.tipo(i) == JsonTipo.TEXTO && ehHoraDoDia(cursor.texto(i))) {
                return Temporais.localTimeDe(horaDoDiaDe(i));
            }
            return emUtc(coluna).toLocalTime();
        }
        if (alvo == LocalDate.class) {
            return emUtc(coluna).toLocalDate();
        }
        if (alvo == LocalDateTime.class) {
            return emUtc(coluna);
        }
        if (alvo == OffsetDateTime.class) {
            return emUtc(coluna).atOffset(ZoneOffset.UTC);
        }
        if (alvo == Object.class) {
            return getObject(coluna);
        }
        return null;
    }

    private LocalDateTime emUtc(int coluna) throws SQLException {
        Long millis = instanteDe(coluna);
        return millis == null ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    private static boolean ehHoraDoDia(String texto) {
        return texto.length() >= 8 && texto.charAt(2) == ':' && texto.charAt(5) == ':';
    }

    // ---------------------------------------------------------- por nome

    @Override
    public String getString(String coluna) throws SQLException {
        return getString(findColumn(coluna));
    }

    @Override
    public boolean getBoolean(String coluna) throws SQLException {
        return getBoolean(findColumn(coluna));
    }

    @Override
    public byte getByte(String coluna) throws SQLException {
        return getByte(findColumn(coluna));
    }

    @Override
    public short getShort(String coluna) throws SQLException {
        return getShort(findColumn(coluna));
    }

    @Override
    public int getInt(String coluna) throws SQLException {
        return getInt(findColumn(coluna));
    }

    @Override
    public long getLong(String coluna) throws SQLException {
        return getLong(findColumn(coluna));
    }

    @Override
    public float getFloat(String coluna) throws SQLException {
        return getFloat(findColumn(coluna));
    }

    @Override
    public double getDouble(String coluna) throws SQLException {
        return getDouble(findColumn(coluna));
    }

    @Override
    public BigDecimal getBigDecimal(String coluna) throws SQLException {
        return getBigDecimal(findColumn(coluna));
    }

    @Override
    public byte[] getBytes(String coluna) throws SQLException {
        return getBytes(findColumn(coluna));
    }

    @Override
    public Date getDate(String coluna) throws SQLException {
        return getDate(findColumn(coluna));
    }

    @Override
    public Date getDate(String coluna, Calendar cal) throws SQLException {
        return getDate(findColumn(coluna), cal);
    }

    @Override
    public Time getTime(String coluna) throws SQLException {
        return getTime(findColumn(coluna));
    }

    @Override
    public Time getTime(String coluna, Calendar cal) throws SQLException {
        return getTime(findColumn(coluna), cal);
    }

    @Override
    public Timestamp getTimestamp(String coluna) throws SQLException {
        return getTimestamp(findColumn(coluna));
    }

    @Override
    public Timestamp getTimestamp(String coluna, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(coluna), cal);
    }

    @Override
    public Object getObject(String coluna) throws SQLException {
        return getObject(findColumn(coluna));
    }

    @Override
    public <T> T getObject(String coluna, Class<T> alvo) throws SQLException {
        return getObject(findColumn(coluna), alvo);
    }

    @Override
    public Reader getCharacterStream(String coluna) throws SQLException {
        return getCharacterStream(findColumn(coluna));
    }

    @Override
    public Reader getNCharacterStream(String coluna) throws SQLException {
        return getCharacterStream(findColumn(coluna));
    }

    @Override
    public String getNString(String coluna) throws SQLException {
        return getString(findColumn(coluna));
    }

    @Override
    public InputStream getBinaryStream(String coluna) throws SQLException {
        return getBinaryStream(findColumn(coluna));
    }

    /**
     * Casa EXATO primeiro, e só depois sem caixa.
     *
     * <p>Os identificadores do NishiDB são case-sensitive e o JDBC promete
     * casamento sem caixa; a ordem resolve o conflito sem abrir mão de nenhum
     * dos dois. Duas colunas casando sem caixa é AMBIGUIDADE, e devolver "a
     * primeira" entregaria a coluna errada em silêncio — enquanto no casamento
     * exato a primeira É a resposta certa, porque é o que o contrato do JDBC
     * manda para {@code SELECT a, a}.
     *
     * <p>A comparação usa {@code equalsIgnoreCase}, e não
     * {@code toLowerCase()}: o segundo aplica o Locale default, e em turco o
     * 'I' não vira 'i' — o driver quebraria só naquelas máquinas.
     */
    @Override
    public int findColumn(String nome) throws SQLException {
        exigeAberto();
        List<Molduras.Coluna> colunas = cursor.colunas();
        for (int i = 0; i < colunas.size(); i++) {
            if (colunas.get(i).name().equals(nome)) {
                return i + 1;
            }
        }
        int achada = -1;
        for (int i = 0; i < colunas.size(); i++) {
            if (colunas.get(i).name().equalsIgnoreCase(nome)) {
                if (achada >= 0 && !colunas.get(achada).name().equals(colunas.get(i).name())) {
                    throw new SQLException("o nome \"" + nome + "\" e ambiguo sem considerar a caixa: "
                            + "casa \"" + colunas.get(achada).name() + "\" e \"" + colunas.get(i).name()
                            + "\", e os identificadores do NishiDB distinguem caixa",
                            SqlStates.CONVERSAO_INVALIDA);
                }
                if (achada < 0) {
                    achada = i;
                }
            }
        }
        if (achada >= 0) {
            return achada + 1;
        }
        throw new SQLException("nao ha coluna \"" + nome + "\" neste resultado",
                SqlStates.CONVERSAO_INVALIDA);
    }

    // ---------------------------------------------------------------- fluxos

    @Override
    public Reader getCharacterStream(int coluna) throws SQLException {
        String texto = getString(coluna);
        return texto == null ? null : new StringReader(texto);
    }

    @Override
    public Reader getNCharacterStream(int coluna) throws SQLException {
        return getCharacterStream(coluna);
    }

    @Override
    public String getNString(int coluna) throws SQLException {
        return getString(coluna);
    }

    @Override
    public InputStream getBinaryStream(int coluna) throws SQLException {
        byte[] b = getBytes(coluna);
        return b == null ? null : new ByteArrayInputStream(b);
    }

    /**
     * Não implementado, e não por preguiça: um "stream de ASCII" sobre texto
     * UTF-8 perde todo caractere fora do ASCII, em silêncio. Quem quer os
     * caracteres usa {@link #getCharacterStream}.
     */
    @Override
    public InputStream getAsciiStream(int coluna) throws SQLException {
        throw naoSuportado("getAsciiStream perderia os caracteres nao-ASCII; use getCharacterStream");
    }

    @Override
    public InputStream getAsciiStream(String coluna) throws SQLException {
        return getAsciiStream(findColumn(coluna));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int coluna) throws SQLException {
        throw naoSuportado("getUnicodeStream esta obsoleto desde o JDBC 2.0; use getCharacterStream");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String coluna) throws SQLException {
        return getUnicodeStream(findColumn(coluna));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int coluna, int escala) throws SQLException {
        throw naoSuportado("getBigDecimal com escala esta obsoleto e arredondaria em silencio; "
                + "use getBigDecimal(int)");
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String coluna, int escala) throws SQLException {
        return getBigDecimal(findColumn(coluna), escala);
    }

    // -------------------------------------------------------------- metadados

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        exigeAberto();
        return meta;
    }

    @Override
    public Statement getStatement() {
        return dono;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        exigeAberto();
        return avisos;
    }

    @Override
    public void clearWarnings() throws SQLException {
        exigeAberto();
        avisos = null;
    }

    @Override
    public int getType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Aceito em SILÊNCIO, e é a única exceção legítima à regra de nunca ignorar.
     *
     * <p>O contrato do JDBC diz que é uma DICA, e o stream do NishiDB entrega as
     * linhas no ritmo em que o servidor as escreve — não há lote a dimensionar.
     * Lançar aqui quebraria toda ferramenta que chama isto por hábito antes de
     * executar, e o comentário existe para que ninguém "conserte" o silêncio.
     */
    @Override
    public void setFetchSize(int linhas) throws SQLException {
        exigeAberto();
        if (linhas < 0) {
            throw new SQLException("fetchSize negativo: " + linhas, SqlStates.VALOR_FORA_DE_FAIXA);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        exigeAberto();
        return 0;
    }

    @Override
    public void setFetchDirection(int direcao) throws SQLException {
        exigeAberto();
        if (direcao != FETCH_FORWARD) {
            throw new SQLException("este ResultSet e TYPE_FORWARD_ONLY: so FETCH_FORWARD",
                    SqlStates.NAO_SUPORTADO);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        exigeAberto();
        return FETCH_FORWARD;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw naoSuportado("este driver nao nomeia cursores");
    }

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("este ResultSet nao embrulha " + alvo.getName(), SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    // ------------------------------------------------- o que um cursor só para
    // frente não pode responder

    /**
     * As quatro perguntas de posição exigem saber o que vem DEPOIS, e um cursor
     * só para frente sobre um stream não sabe.
     *
     * <p>Responder "false" seria pior que recusar: {@code isBeforeFirst()} é
     * usado para testar resultado vazio, e um false mentiroso faria a aplicação
     * concluir que há linhas.
     */
    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw naoSuportado("isBeforeFirst exigiria olhar adiante num cursor so para frente");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw naoSuportado("isAfterLast exigiria olhar adiante num cursor so para frente");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw naoSuportado("isFirst exigiria olhar adiante num cursor so para frente");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw naoSuportado("isLast exigiria olhar adiante num cursor so para frente");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public void afterLast() throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public boolean first() throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public boolean last() throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public boolean absolute(int linha) throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public boolean relative(int linhas) throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public boolean previous() throws SQLException {
        throw naoSuportado("o cursor deste driver e so para frente");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw naoSuportado("o resultado e um stream ja consumido; nao ha o que reler");
    }

    // ---------------------------------------------- o que um resultado somente
    // leitura não faz

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void insertRow() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateRow() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    private static final String SO_LEITURA =
            "o NishiDB nao aceita escrita por SQL: os dados entram por ingestao";

    @Override
    public void updateNull(int coluna) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBoolean(int coluna, boolean v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateByte(int coluna, byte v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateShort(int coluna, short v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateInt(int coluna, int v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateLong(int coluna, long v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateFloat(int coluna, float v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateDouble(int coluna, double v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBigDecimal(int coluna, BigDecimal v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateString(int coluna, String v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBytes(int coluna, byte[] v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateDate(int coluna, Date v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateTime(int coluna, Time v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateTimestamp(int coluna, Timestamp v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(int coluna, InputStream v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(int coluna, InputStream v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(int coluna, Reader v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(int coluna, Object v, int escala) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(int coluna, Object v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNull(String coluna) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBoolean(String coluna, boolean v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateByte(String coluna, byte v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateShort(String coluna, short v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateInt(String coluna, int v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateLong(String coluna, long v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateFloat(String coluna, float v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateDouble(String coluna, double v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBigDecimal(String coluna, BigDecimal v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateString(String coluna, String v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBytes(String coluna, byte[] v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateDate(String coluna, Date v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateTime(String coluna, Time v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateTimestamp(String coluna, Timestamp v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(String coluna, InputStream v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(String coluna, InputStream v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(String coluna, Reader v, int tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(String coluna, Object v, int escala) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(String coluna, Object v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateRef(int coluna, Ref v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateRef(String coluna, Ref v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(int coluna, Blob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(String coluna, Blob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(int coluna, Clob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(String coluna, Clob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateArray(int coluna, Array v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateArray(String coluna, Array v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateRowId(int coluna, RowId v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateRowId(String coluna, RowId v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNString(int coluna, String v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNString(String coluna, String v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(int coluna, NClob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(String coluna, NClob v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateSQLXML(int coluna, SQLXML v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateSQLXML(String coluna, SQLXML v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNCharacterStream(int coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNCharacterStream(String coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(int coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(int coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(int coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(String coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(String coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(String coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(int coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(String coluna, InputStream v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(int coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(String coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(int coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(String coluna, Reader v, long tamanho) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNCharacterStream(int coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNCharacterStream(String coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(int coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(int coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(int coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateAsciiStream(String coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBinaryStream(String coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateCharacterStream(String coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(int coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateBlob(String coluna, InputStream v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(int coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateClob(String coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(int coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateNClob(String coluna, Reader v) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(int coluna, Object v, SQLType tipo, int escala) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(String coluna, Object v, SQLType tipo, int escala) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(int coluna, Object v, SQLType tipo) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    @Override
    public void updateObject(String coluna, Object v, SQLType tipo) throws SQLException {
        throw naoSuportado(SO_LEITURA);
    }

    // ------------------------------------------------ tipos que não existem no
    // NishiDB

    private static final String SEM_TIPO =
            "o NishiDB nao tem este tipo; as colunas sao escalares, texto, binario e json";

    @Override
    public Object getObject(int coluna, Map<String, Class<?>> mapa) throws SQLException {
        throw naoSuportado("este driver nao usa mapa de tipos; use getObject(int, Class)");
    }

    @Override
    public Object getObject(String coluna, Map<String, Class<?>> mapa) throws SQLException {
        return getObject(findColumn(coluna), mapa);
    }

    @Override
    public Ref getRef(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public Ref getRef(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public Blob getBlob(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getBytes na coluna bytes");
    }

    @Override
    public Blob getBlob(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getBytes na coluna bytes");
    }

    @Override
    public Clob getClob(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getString");
    }

    @Override
    public Clob getClob(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getString");
    }

    @Override
    public NClob getNClob(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getString");
    }

    @Override
    public NClob getNClob(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; use getString");
    }

    @Override
    public Array getArray(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; um array vem dentro da coluna json");
    }

    @Override
    public Array getArray(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO + "; um array vem dentro da coluna json");
    }

    @Override
    public URL getURL(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public URL getURL(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public RowId getRowId(int coluna) throws SQLException {
        throw naoSuportado("o NishiDB nao tem rowid estavel; a identidade da linha e a chave do dataset");
    }

    @Override
    public RowId getRowId(String coluna) throws SQLException {
        return getRowId(findColumn(coluna));
    }

    @Override
    public SQLXML getSQLXML(int coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    @Override
    public SQLXML getSQLXML(String coluna) throws SQLException {
        throw naoSuportado(SEM_TIPO);
    }

    // -------------------------------------------------------------- interno

    /** Uma conversão que pode falhar e cujo erro precisa nomear a coluna. */
    private interface Conversao {
        long faz() throws SQLException;
    }

    private long comColuna(int i, Conversao c) throws SQLException {
        try {
            return c.faz();
        } catch (SQLException e) {
            throw new SQLDataException("coluna " + nome(i) + ": " + e.getMessage(), e.getSQLState());
        }
    }

    private long inteiroEntre(int coluna, long minimo, long maximo, String tipo) throws SQLException {
        long v = getLong(coluna);
        if (v < minimo || v > maximo) {
            throw new SQLDataException("coluna " + nome(indice(coluna)) + ": " + v
                    + " nao cabe em " + tipo, SqlStates.VALOR_FORA_DE_FAIXA);
        }
        return v;
    }

    /**
     * O double vira long só quando não há nada a perder.
     *
     * <p>É o espelho da recusa do servidor a um parâmetro int64 com valor 3.5:
     * quem pediu um inteiro e recebeu 3 não teria como saber que perdeu meio.
     */
    private long semArredondar(int i, double d) throws SQLException {
        if (!Double.isFinite(d) || d != Math.rint(d)
                || d < -9.223372036854776E18 || d >= 9.223372036854776E18) {
            throw new SQLDataException("coluna " + nome(i) + ": " + d
                    + " nao e um inteiro representavel, e este driver nao arredonda em silencio",
                    SqlStates.VALOR_FORA_DE_FAIXA);
        }
        return (long) d;
    }

    /**
     * Grava a flag de nulo ANTES de qualquer conversão que possa lançar.
     *
     * <p>Se a flag fosse escrita depois, uma conversão que falhasse deixaria
     * {@code wasNull()} respondendo sobre a coluna anterior — e o valor
     * seguinte, legítimo, seria lido como nulo.
     */
    private boolean nulo(int i) throws SQLException {
        ultimoFoiNulo = cursor.tipo(i) == JsonTipo.NULO;
        return ultimoFoiNulo;
    }

    private String nome(int i) {
        return cursor.colunas().get(i).name();
    }

    /** Traduz o índice 1-based do JDBC para o 0-based do cursor. */
    private int indice(int coluna) throws SQLException {
        exigeAberto();
        if (!emLinha) {
            throw new SQLException("nao ha linha corrente: chame next() antes de ler",
                    SqlStates.CURSOR_INVALIDO);
        }
        int n = cursor.colunas().size();
        if (coluna < 1 || coluna > n) {
            throw new SQLException("coluna " + coluna + " fora de [1," + n + "]",
                    SqlStates.CONVERSAO_INVALIDA);
        }
        return coluna - 1;
    }

    private void exigeAberto() throws SQLException {
        if (fechado) {
            throw new SQLException("este ResultSet ja foi fechado", SqlStates.CURSOR_INVALIDO);
        }
    }

    /** Traz para o ResultSet o aviso que o cursor acumulou, uma vez só. */
    private void absorveAviso() {
        if (avisoDoCursorAbsorvido) {
            return;
        }
        SQLWarning w = cursor.aviso();
        if (w != null) {
            avisoDoCursorAbsorvido = true;
            avisos = w;
        }
    }

    private SQLDataException naoConverte(int i, String valor, String alvo) {
        return new SQLDataException("coluna " + nome(i) + ": \"" + valor + "\" nao vira " + alvo,
                SqlStates.CONVERSAO_INVALIDA);
    }

    private static SQLFeatureNotSupportedException naoSuportado(String porque) {
        return new SQLFeatureNotSupportedException(porque, SqlStates.NAO_SUPORTADO);
    }
}
