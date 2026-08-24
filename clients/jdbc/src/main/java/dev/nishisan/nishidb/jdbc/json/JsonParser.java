package dev.nishisan.nishidb.jdbc.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Materializa um documento JSON PEQUENO em {@link JsonValue}.
 *
 * <p>Pequeno é a palavra: a linha de schema, /v1/metadata, /v1/stats,
 * /v1/health e o corpo de erro. Todos são lidos uma vez por conexão ou por
 * consulta, e nenhum cresce com o resultado. As LINHAS DE DADOS não passam por
 * aqui — elas usam o {@link JsonScanner} com spans, porque materializá-las
 * seria o buffering proporcional ao resultado que o §128 proíbe.
 *
 * <p>A gramática é a do scanner: este parser delimita cada valor por
 * {@link JsonScanner#valorEm} e só decide o que construir. O que seria
 * arriscado duplicar — escapes de string, controle cru, zero à esquerda,
 * expoente — mora num lugar só.
 */
public final class JsonParser {

    /**
     * Teto do corpo de um documento de metadados.
     *
     * <p>16 MiB é folgado para o /v1/metadata de um processo com muitos
     * datasets, e impede que uma resposta inesperadamente enorme — ou um proxy
     * devolvendo outra coisa no lugar — vire OutOfMemoryError dentro do driver.
     */
    public static final int TETO_DE_DOCUMENTO = 16 * 1024 * 1024;

    /** Teto de aninhamento do documento, pelo mesmo motivo do scanner. */
    private static final int PROFUNDIDADE_MAXIMA = 100;

    private final byte[] buf;
    private final JsonScanner scanner;

    private JsonParser(byte[] buf) {
        this.buf = buf;
        this.scanner = new JsonScanner(buf, 0, buf.length);
    }

    /** Lê o corpo inteiro e o materializa. */
    public static JsonValue le(InputStream in) throws JsonException, IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] pedaco = new byte[8192];
        int n;
        while ((n = in.read(pedaco)) > 0) {
            if (bos.size() + n > TETO_DE_DOCUMENTO) {
                throw new JsonException(bos.size(), "documento acima de " + TETO_DE_DOCUMENTO + " bytes");
            }
            bos.write(pedaco, 0, n);
        }
        return de(bos.toByteArray());
    }

    public static JsonValue de(byte[] bruto) throws JsonException {
        JsonParser p = new JsonParser(bruto);
        int fim = p.scanner.valorEm(0);
        JsonValue v = p.constroi(0);
        // Nada depois do valor de topo: sem esta checagem, um corpo com dois
        // documentos colados passaria como um só e a segunda metade sumiria.
        for (int i = fim; i < bruto.length; i++) {
            byte b = bruto[i];
            if (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
                throw new JsonException(i, "conteúdo depois do fim do documento");
            }
        }
        return v;
    }

    public static JsonValue de(String texto) throws JsonException {
        return de(texto.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Constrói o JsonValue do span que o scanner acabou de delimitar.
     *
     * <p>O span é copiado para locais ANTES de qualquer recursão: as chamadas
     * de dentro reescrevem o estado do scanner, e ler os campos depois delas
     * daria o span do último valor aninhado.
     */
    private JsonValue constroi(int profundidade) throws JsonException {
        if (profundidade > PROFUNDIDADE_MAXIMA) {
            throw new JsonException(scanner.valorIni(), "aninhamento acima de " + PROFUNDIDADE_MAXIMA + " níveis");
        }
        JsonTipo tipo = scanner.tipo();
        int ini = scanner.valorIni();
        int fim = scanner.valorFim();

        return switch (tipo) {
            case TEXTO -> new JsonValue.Texto(JsonScanner.texto(buf, ini, fim));
            case NUMERO -> new JsonValue.Numero(new String(buf, ini, fim - ini, StandardCharsets.US_ASCII));
            case BOOLEANO -> new JsonValue.Booleano(buf[ini] == 't');
            case NULO -> new JsonValue.Nulo();
            case BRUTO -> buf[ini] == '{' ? objeto(ini, fim, profundidade) : arranjo(ini, fim, profundidade);
        };
    }

    private JsonValue objeto(int ini, int fim, int profundidade) throws JsonException {
        JsonScanner s = new JsonScanner(buf, ini, fim);
        s.abreObjeto();
        Map<String, JsonValue> campos = new LinkedHashMap<>();
        while (s.proximoCampo()) {
            // Chave e span do valor copiados ANTES da recursão, que reescreve
            // o estado de s.
            String chave = JsonScanner.texto(buf, s.chaveIni(), s.chaveFim());
            JsonTipo tipo = s.tipo();
            int vi = s.valorIni();
            int vf = s.valorFim();
            campos.put(chave, valorDoSpan(tipo, vi, vf, profundidade));
        }
        return new JsonValue.Objeto(Collections.unmodifiableMap(campos));
    }

    private JsonValue arranjo(int ini, int fim, int profundidade) throws JsonException {
        List<JsonValue> itens = new ArrayList<>();
        JsonScanner s = new JsonScanner(buf, ini, fim);
        int p = ini + 1; // depois do '['
        p = pulaEspacos(p, fim);
        if (p < fim && buf[p] == ']') {
            return new JsonValue.Arranjo(List.of());
        }
        while (true) {
            int depois = s.valorEm(p);
            itens.add(valorDoSpan(s.tipo(), s.valorIni(), s.valorFim(), profundidade));
            p = pulaEspacos(depois, fim);
            if (p >= fim) {
                throw new JsonException(p, "o array terminou sem ']'");
            }
            if (buf[p] == ']') {
                return new JsonValue.Arranjo(Collections.unmodifiableList(itens));
            }
            if (buf[p] != ',') {
                throw new JsonException(p, "esperado ',' ou ']' no array");
            }
            p = pulaEspacos(p + 1, fim);
        }
    }

    /** Reconstrói a partir de um span já delimitado, sem depender do estado do scanner. */
    private JsonValue valorDoSpan(JsonTipo tipo, int ini, int fim, int profundidade) throws JsonException {
        if (profundidade > PROFUNDIDADE_MAXIMA) {
            throw new JsonException(ini, "aninhamento acima de " + PROFUNDIDADE_MAXIMA + " níveis");
        }
        return switch (tipo) {
            case TEXTO -> new JsonValue.Texto(JsonScanner.texto(buf, ini, fim));
            case NUMERO -> new JsonValue.Numero(new String(buf, ini, fim - ini, StandardCharsets.US_ASCII));
            case BOOLEANO -> new JsonValue.Booleano(buf[ini] == 't');
            case NULO -> new JsonValue.Nulo();
            case BRUTO -> buf[ini] == '{'
                    ? objeto(ini, fim, profundidade + 1)
                    : arranjo(ini, fim, profundidade + 1);
        };
    }

    private int pulaEspacos(int p, int fim) {
        while (p < fim) {
            byte b = buf[p];
            if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
                p++;
            } else {
                return p;
            }
        }
        return fim;
    }
}
