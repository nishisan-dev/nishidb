package dev.nishisan.nishidb.jdbc.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonScannerTest {

    /**
     * Uma barra invertida, montada em runtime.
     *
     * <p>Não é preciosismo: o pré-processador do Java resolve sequências de
     * escape unicode ANTES do compilador, inclusive dentro de literais de
     * String e de comentários. Escrever a sequência literal aqui faria o
     * compilador convertê-la no CARACTERE, e o teste passaria a exercitar um
     * texto já desescapado — verificando o oposto do que afirma, sem que nada
     * acusasse. Concatenar mantém a barra e o 'u' separados até o runtime.
     */
    private static final String B = String.valueOf((char) 92);


    /** Percorre um objeto de topo e devolve "chave=tipo:valor" por campo. */
    private static List<String> campos(String json) throws JsonException {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        JsonScanner s = new JsonScanner(b, 0, b.length);
        s.abreObjeto();
        List<String> out = new ArrayList<>();
        while (s.proximoCampo()) {
            String chave = new String(b, s.chaveIni(), s.chaveFim() - s.chaveIni(), StandardCharsets.UTF_8);
            String valor = new String(b, s.valorIni(), s.valorFim() - s.valorIni(), StandardCharsets.UTF_8);
            out.add(chave + "=" + s.tipo() + ":" + valor);
        }
        s.exigeFimDaEntrada();
        return out;
    }

    @Test
    void objetoPlanoComOsQuatroTiposEscalares() throws JsonException {
        assertEquals(
                List.of("a=NUMERO:1", "b=NULO:null", "c=TEXTO:x", "d=BOOLEANO:true"),
                campos("{\"a\":1,\"b\":null,\"c\":\"x\",\"d\":true}"));
    }

    /**
     * NULL vem EXPLÍCITO, e o número de chaves da linha é igual ao de colunas
     * do schema.
     *
     * <p>É contrato do servidor (TestNullExplicito, do lado Go), e é o que
     * permite ao cursor casar linha e schema por POSIÇÃO. Um servidor que
     * passasse a omitir campos nulos quebraria esse casamento, e este teste é o
     * lembrete de que a invariante é lida daqui.
     */
    @Test
    void nuloEhTokenExplicitoENaoAusencia() throws JsonException {
        assertEquals(List.of("a=NULO:null", "b=NULO:null", "c=TEXTO:x"),
                campos("{\"a\":null,\"b\":null,\"c\":\"x\"}"));
    }

    /**
     * A coluna json sai INLINE e VERBATIM, não como string.
     *
     * <p>Um parser que só esperasse escalares dessincronizaria a linha inteira
     * no primeiro documento — leria o '{' aninhado como fim do objeto de topo e
     * todas as colunas seguintes sairiam erradas, sem erro nenhum.
     */
    @Test
    void colunaJsonVemInlineEEntregueCrua() throws JsonException {
        assertEquals(
                List.of("doc=BRUTO:{\"a\":[1,2],\"b\":\"txt\"}", "z=NUMERO:9"),
                campos("{\"doc\":{\"a\":[1,2],\"b\":\"txt\"},\"z\":9}"));
    }

    @Test
    void arrayAninhadoTambemEhBruto() throws JsonException {
        assertEquals(List.of("xs=BRUTO:[1,[2,3],{\"k\":null}]"), campos("{\"xs\":[1,[2,3],{\"k\":null}]}"));
    }

    /**
     * CHAVE DUPLICADA É ACEITA — o único ponto em que o parser é
     * deliberadamente mais frouxo que a RFC.
     *
     * <p>{@code SELECT a, a} é uma query VÁLIDA e produz a chave duas vezes na
     * linha; o encoder do servidor documenta isso. Recusar aqui quebraria uma
     * consulta legítima, e é por isso que o casamento com o schema é posicional
     * e nunca por mapa de nome.
     */
    @Test
    void chaveDuplicadaEhAceitaPorqueSelectAAEhValido() throws JsonException {
        assertEquals(List.of("a=NUMERO:1", "a=NUMERO:1"), campos("{\"a\":1,\"a\":1}"));
    }

    @Test
    void objetoVazio() throws JsonException {
        assertEquals(List.of(), campos("{}"));
    }

    @Test
    void espacosEntreOsTokensSaoTolerados() throws JsonException {
        assertEquals(List.of("a=NUMERO:1", "b=TEXTO:x"), campos("  { \"a\" : 1 , \"b\" : \"x\" }  "));
    }

    // ------------------------------------------------------------- recusas

    /**
     * O que a RFC não tem, o scanner recusa — e recusa NOMEANDO O BYTE.
     *
     * <p>Aceitar qualquer um destes seria pior que falhar: NaN e Infinity não
     * têm representação JSON e chegariam como texto que o Double.parseDouble
     * do JDK aceita em silêncio; um zero à esquerda muda o valor em quem
     * interpreta como octal; e uma string não fechada faria o scanner engolir o
     * resto da linha como conteúdo.
     */
    @Test
    void oQueNaoEhJsonEhRecusado() {
        String[] casos = {
            "{\"a\":NaN}", "{\"a\":Infinity}", "{\"a\":-Infinity}",
            "{\"a\":+1}", "{\"a\":007}", "{\"a\":.5}", "{\"a\":5.}", "{\"a\":1e}", "{\"a\":0x10}",
            "{'a':1}",                    // aspas simples
            "{a:1}",                      // chave sem aspas
            "{\"a\":1,}",                 // vírgula sobrando
            "{\"a\":1} // comentário",    // comentário
            "{\"a\":\"sem fim}",          // string não fechada
            "{\"a\":\"" + B + "q\"}",     // escape desconhecido
            "{\"a\":\"" + B + "u12\"}",   // escape unicode truncado
            "{\"a\":1}{\"b\":2}",         // conteúdo depois do valor de topo
            "[1,2]",                      // valor de topo que não é objeto
            "{\"a\":}",                   // valor ausente
            "{\"a\" 1}",                  // dois-pontos ausente
        };
        for (String caso : casos) {
            assertThrows(JsonException.class, () -> campos(caso), () -> "aceitou: " + caso);
        }
    }

    /**
     * Byte de controle CRU dentro de string é recusado.
     *
     * <p>A RFC os proíbe, e o encoder do servidor os escapa — então um controle
     * cru significa que a resposta não veio de onde deveria, ou veio corrompida.
     */
    @Test
    void controleCruDentroDeStringEhRecusado() {
        assertThrows(JsonException.class, () -> campos("{\"a\":\"x\u0001y\"}"));
    }

    /**
     * Profundidade excessiva vira JsonException, e NUNCA StackOverflowError.
     *
     * <p>A diferença importa: um StackOverflowError é um Error, não é
     * capturado por quem trata SQLException, e derruba a thread da aplicação
     * que só queria ler um ResultSet.
     */
    @Test
    void profundidadeExcessivaFalhaEmVezDeEstourarAPilha() {
        StringBuilder sb = new StringBuilder("{\"a\":");
        sb.append("[".repeat(500));
        sb.append("]".repeat(500));
        sb.append('}');
        assertThrows(JsonException.class, () -> campos(sb.toString()));
    }

    // -------------------------------------------------------------- texto

    @Test
    void desescapaOsEscapesDaRfc() throws JsonException {
        String escapado = "a" + B + "\"b" + B + B + "c" + B + "/d" + B + "b" + B + "u0435"
                + B + "f" + B + "n" + B + "r" + B + "t" + "f";
        assertEquals("a\"b" + B + "c/d\b\u0435\f\n\r\tf", texto(escapado));
    }

    @Test
    void desescapaParDeSurrogates() throws JsonException {
        // U+1F600, que só cabe em UTF-16 como par de surrogates.
        assertEquals("\uD83D\uDE00", texto(B + "uD83D" + B + "uDE00"));
    }

    @Test
    void textoSemEscapeNaoPassaPorConversaoAlguma() throws JsonException {
        assertEquals("Acct-Status-Type", texto("Acct-Status-Type"));
        // UTF-8 multibyte cru, que é como o servidor emite acentuação.
        assertEquals("ação", texto("ação"));
    }

    private static String texto(String conteudo) throws JsonException {
        byte[] b = conteudo.getBytes(StandardCharsets.UTF_8);
        return JsonScanner.texto(b, 0, b.length);
    }

    // ------------------------------------------------------------- números

    /**
     * O inteiro é parseado dos DÍGITOS, e não convertido para double no
     * caminho.
     *
     * <p>É o espelho exato de por que o campo "value" do parâmetro é
     * json.RawMessage e não any no lado Go: passar por double faria
     * 9223372036854775807 chegar como ...5808, errado e SEM ERRO.
     */
    @Test
    void inteiroNoLimiteDeLongSaiExato() throws JsonException {
        assertEquals(Long.MAX_VALUE, inteiro("9223372036854775807"));
        assertEquals(Long.MIN_VALUE, inteiro("-9223372036854775808"));
        assertEquals(0L, inteiro("0"));
        assertEquals(-1L, inteiro("-1"));
    }

    @Test
    void inteiroAcimaDoLimiteFalhaEmVezDeArredondar() {
        assertThrows(JsonException.class, () -> inteiro("9223372036854775808"));
        assertThrows(JsonException.class, () -> inteiro("-9223372036854775809"));
        assertThrows(JsonException.class, () -> inteiro("99999999999999999999999"));
    }

    /**
     * A grafia 'g' do Go faz round-trip.
     *
     * <p>O servidor emite doubles com strconv.AppendFloat(_, 'g', -1, 64), que
     * produz formas como 1e-07 e 1.5e+21 — JSON válido, e diferentes do que o
     * encoding/json escreveria. A regra do driver é: nós validamos a gramática,
     * o JDK arredonda. E doubles NUNCA se comparam como texto.
     */
    @Test
    void doubleNaGrafiaDoGoFazRoundTripDeBITS() throws JsonException {
        double[] valores = {1e-7, 1.5e21, -1.7976931348623157e308, 4.9e-324, 0.1, -0.0};
        String[] grafias = {"1e-07", "1.5e+21", "-1.7976931348623157e+308", "5e-324", "0.1", "-0"};
        for (int i = 0; i < valores.length; i++) {
            assertEquals(Double.doubleToLongBits(valores[i]), Double.doubleToLongBits(numero(grafias[i])),
                    "grafia " + grafias[i]);
        }
    }

    /**
     * O JDK aceita coisas que JSON não tem, e por isso a validação vem ANTES
     * dele.
     *
     * <p>Double.parseDouble engole "NaN", "Infinity", "0x1p3" e o sufixo 'd' —
     * todos ilegais em JSON. Sem a validação prévia, um servidor comprometido
     * (ou um proxy que reescreve o corpo) injetaria valores que o driver
     * entregaria à aplicação como número legítimo.
     */
    @Test
    void oQueOJdkAceitaMasJsonNaoTemEhRecusadoAntes() {
        for (String grafia : new String[] {"NaN", "Infinity", "-Infinity", "0x1p3", "1d", "1f", "1.0D"}) {
            assertThrows(JsonException.class, () -> numero(grafia), () -> "aceitou " + grafia);
        }
    }

    @Test
    void inteiroComGrafiaDeFracionarioNaoEhInteiro() {
        assertThrows(JsonException.class, () -> inteiro("1.0"));
        assertThrows(JsonException.class, () -> inteiro("1e3"));
    }

    private static long inteiro(String s) throws JsonException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return JsonScanner.inteiro(b, 0, b.length);
    }

    private static double numero(String s) throws JsonException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return JsonScanner.numero(b, 0, b.length);
    }

    /** O offset acompanha a falha: é o que separa diagnóstico de adivinhação. */
    @Test
    void aFalhaNomeiaOByte() {
        // {"a":1,"b":q}
        //  0123456789012 — o 'q' está no byte 11.
        JsonException e = assertThrows(JsonException.class, () -> campos("{\"a\":1,\"b\":q}"));
        assertEquals(11, e.offset());
        assertTrue(e.getMessage().contains("byte 11"), e.getMessage());
    }
}
