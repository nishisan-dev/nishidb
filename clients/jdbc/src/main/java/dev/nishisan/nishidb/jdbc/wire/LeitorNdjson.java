package dev.nishisan.nishidb.jdbc.wire;

import dev.nishisan.nishidb.jdbc.json.JsonException;
import dev.nishisan.nishidb.jdbc.json.JsonParser;
import dev.nishisan.nishidb.jdbc.json.JsonValue;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;

/**
 * Percorre a resposta de {@code POST /v1/query}. É a porta do
 * {@code protocolo.Leitor} do lado Go, com o mesmo contrato.
 *
 * <p>A primeira linha é o schema, as do meio são dados, e a última é SEMPRE um
 * fecho. Um stream que acaba sem fecho é FALHA — é assim que uma conexão
 * cortada no meio deixa de ser indistinguível de um resultado que terminou.
 *
 * <p>Os três desfechos ficam separados, e isso é o coração do porte:
 * {@link #fim()} é "a consulta terminou bem", {@link #erro()} é "o servidor
 * disse que a consulta quebrou", e uma exceção de transporte é "o servidor
 * sumiu no meio da frase". Colapsá-los faria a aplicação tratar um servidor que
 * caiu como um resultado vazio.
 *
 * <p>As linhas de dados são entregues CRUAS, como spans sobre um buffer
 * reusado: decodificá-las aqui seria materializar o resultado, que é o que o
 * §128 proíbe.
 */
public final class LeitorNdjson implements AutoCloseable {

    /**
     * Teto de uma linha. Uma row do NishiDB não passa de 4 MiB (o teto de
     * record do storage), então 64 MiB é folga larga — e ainda assim impede
     * que um stream que nunca manda LF vire OutOfMemoryError.
     */
    private static final int TETO_DE_LINHA = 64 * 1024 * 1024;

    private static final byte[] PREFIXO_MOLDURA =
            ("{\"" + Molduras.CHAVE + "\":").getBytes(StandardCharsets.UTF_8);

    private final InputStream in;

    /** Buffer de leitura do stream, e a janela ainda não consumida dele. */
    private byte[] entrada = new byte[64 * 1024];
    private int entradaIni;
    private int entradaFim;

    /** A linha corrente, montada quando ela cruza a fronteira do buffer. */
    private byte[] linha = new byte[8 * 1024];
    private int linhaIni;
    private int linhaFim;

    private Molduras.LinhaSchema schema;
    private Molduras.LinhaFim fim;
    private Molduras.LinhaErro erro;
    private boolean acabou;

    public LeitorNdjson(InputStream in) {
        this.in = in;
    }

    /** O schema, disponível depois da primeira chamada a {@link #proxima}. */
    public Molduras.LinhaSchema schema() {
        return schema;
    }

    /** O fecho de sucesso, ou null se a consulta terminou em erro. */
    public Molduras.LinhaFim fim() {
        return fim;
    }

    /** O fecho de falha, ou null se a consulta terminou bem. */
    public Molduras.LinhaErro erro() {
        return erro;
    }

    /** O buffer da linha de dados corrente. Válido até a próxima chamada. */
    public byte[] buffer() {
        return linha;
    }

    public int inicio() {
        return linhaIni;
    }

    public int fimDaLinha() {
        return linhaFim;
    }

    /**
     * Avança para a próxima linha de DADOS. {@code false} quando o stream
     * fechou.
     *
     * <p>Depois de um {@code false}, exatamente um entre {@link #fim()} e
     * {@link #erro()} está preenchido — ou uma exceção já foi lançada.
     */
    public boolean proxima() throws SQLException {
        while (true) {
            if (acabou) {
                return false;
            }
            if (!leLinha()) {
                acabou = true;
                // EOF sem fecho. Sem esta checagem o driver apresentaria um
                // resultado parcial como completo, que é o defeito que a
                // moldura inteira existe para impedir.
                throw transporte("o stream terminou sem a linha de fecho");
            }
            if (linhaFim == linhaIni) {
                continue; // linha em branco
            }
            if (!comecaComMoldura()) {
                if (schema == null) {
                    acabou = true;
                    throw transporte("a primeira linha da resposta nao e o schema");
                }
                return true;
            }
            if (!consomeMoldura()) {
                return false;
            }
        }
    }

    /**
     * Lança o desfecho da consulta, se ele for uma falha.
     *
     * <p>O erro chega DEPOIS de N linhas já entregues à aplicação, e a
     * mensagem diz quantas: "falhou" sozinho deixaria quem lê sem saber se o
     * que já processou vale.
     */
    public void exigeDesfechoFeliz() throws SQLException {
        if (erro != null) {
            throw SqlStates.excecaoDe(erro.code(),
                    erro.message() + " [depois de " + erro.rows() + " linha(s) entregue(s)]");
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            in.close();
        } catch (IOException e) {
            throw transporte("fechando a resposta: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ moldura

    private boolean comecaComMoldura() {
        int n = linhaFim - linhaIni;
        if (n < PREFIXO_MOLDURA.length) {
            return false;
        }
        for (int i = 0; i < PREFIXO_MOLDURA.length; i++) {
            if (linha[linhaIni + i] != PREFIXO_MOLDURA[i]) {
                return false;
            }
        }
        return true;
    }

    /** Consome uma linha de moldura. {@code true} = continue lendo (era o schema). */
    private boolean consomeMoldura() throws SQLException {
        JsonValue v;
        try {
            byte[] copia = new byte[linhaFim - linhaIni];
            System.arraycopy(linha, linhaIni, copia, 0, copia.length);
            v = JsonParser.de(copia);
        } catch (JsonException e) {
            acabou = true;
            throw transporte("moldura ilegivel: " + e.getMessage());
        }
        String qual = v.textoOu(Molduras.CHAVE, "");
        switch (qual) {
            case Molduras.SCHEMA -> {
                if (schema != null) {
                    acabou = true;
                    throw transporte("segunda linha de schema no mesmo stream");
                }
                schema = new Molduras.LinhaSchema(v.textoOu("dataset", ""), colunasDe(v));
                return true;
            }
            case Molduras.FIM -> fim = new Molduras.LinhaFim(
                    v.inteiroOu("rows", 0), v.booleanoOu("truncated", false));
            case Molduras.ERRO -> erro = new Molduras.LinhaErro(
                    v.textoOu("code", Codigos.INTERNAL), v.inteiroOu("rows", 0), v.textoOu("message", ""));
            default -> {
                acabou = true;
                throw transporte("moldura desconhecida: " + qual);
            }
        }
        acabou = true;
        return false;
    }

    private static List<Molduras.Coluna> colunasDe(JsonValue schema) {
        List<JsonValue> brutas = schema.itensDe("columns");
        List<Molduras.Coluna> out = new ArrayList<>(brutas.size());
        for (JsonValue c : brutas) {
            out.add(new Molduras.Coluna(
                    c.textoOu("name", ""),
                    c.textoOu("type", ""),
                    // OTHER como padrão, e nunca VARCHAR: um servidor que não
                    // mandasse o campo faria o driver descrever como String uma
                    // coluna que pode ser qualquer coisa.
                    c.textoOu("jdbc", "OTHER"),
                    c.booleanoOu("nullable", true),
                    (int) c.inteiroOu("precision", 0),
                    (int) c.inteiroOu("scale", 0),
                    (int) c.inteiroOu("displaySize", 0)));
        }
        return out;
    }

    // ------------------------------------------------------------- leitura

    /** Lê uma linha para linha[linhaIni..linhaFim). {@code false} no EOF limpo. */
    private boolean leLinha() throws SQLException {
        linhaIni = 0;
        linhaFim = 0;
        boolean algumByte = false;
        while (true) {
            if (entradaIni >= entradaFim && !enche()) {
                // EOF: uma linha sem LF final é resposta truncada, não linha.
                if (algumByte) {
                    throw transporte("ultima linha sem terminador");
                }
                return false;
            }
            for (int i = entradaIni; i < entradaFim; i++) {
                if (entrada[i] == '\n') {
                    acrescenta(entradaIni, i);
                    entradaIni = i + 1;
                    aparaCr();
                    return true;
                }
            }
            acrescenta(entradaIni, entradaFim);
            algumByte = algumByte || entradaFim > entradaIni;
            entradaIni = entradaFim;
        }
    }

    private void aparaCr() {
        if (linhaFim > linhaIni && linha[linhaFim - 1] == '\r') {
            linhaFim--;
        }
    }

    private void acrescenta(int de, int ate) throws SQLException {
        int n = ate - de;
        if (n <= 0) {
            return;
        }
        if (linhaFim + n > TETO_DE_LINHA) {
            throw transporte("linha acima de " + TETO_DE_LINHA + " bytes");
        }
        if (linhaFim + n > linha.length) {
            int novo = Math.max(linha.length * 2, linhaFim + n);
            byte[] maior = new byte[Math.min(novo, TETO_DE_LINHA)];
            System.arraycopy(linha, 0, maior, 0, linhaFim);
            linha = maior;
        }
        System.arraycopy(entrada, de, linha, linhaFim, n);
        linhaFim += n;
    }

    private boolean enche() throws SQLException {
        try {
            int n = in.read(entrada, 0, entrada.length);
            entradaIni = 0;
            entradaFim = Math.max(n, 0);
            return n > 0;
        } catch (ResponseTimeoutException e) {
            // O prazo da requisição estourou no meio do corpo: a exceção é a
            // que a política do prazo montou (SQLTimeoutException do
            // queryTimeout, ou a conexão fechada do prazo de rede), e não a
            // queda de conexão genérica abaixo.
            acabou = true;
            throw e.failure();
        } catch (IOException e) {
            acabou = true;
            throw transporte("lendo a resposta: " + e.getMessage());
        }
    }

    /**
     * Falha de TRANSPORTE — e não da consulta.
     *
     * <p>SQLRecoverableException e 08006: a conexão morreu, mas a aplicação
     * pode abrir outra e repetir. É a classe que separa "o servidor sumiu" de
     * "o servidor respondeu que a consulta está errada".
     */
    private static SQLException transporte(String mensagem) {
        return new SQLRecoverableException(mensagem, SqlStates.CONEXAO_INTERROMPIDA);
    }
}
