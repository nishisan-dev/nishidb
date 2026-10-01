package dev.nishisan.nishidb.jdbc.wire;

import java.sql.SQLException;

/**
 * O prazo de espera de UMA requisição pelo servidor, e a tradução do estouro.
 *
 * <p><b>O prazo limita a ESPERA, não a duração.</b> Ele vale para a espera
 * pelos cabeçalhos da resposta (o {@code HttpRequest.timeout} do JDK, que só
 * cobre até eles) e para cada espera por bytes do corpo. O tempo que a
 * aplicação gasta processando as linhas entre uma leitura e outra não conta,
 * e um stream que continua mandando bytes nunca estoura: um prazo de duração
 * total mataria a consulta longa que estivesse indo bem. A exceção são os
 * documentos pequenos ({@link Transporte#documento}), em que o prazo é da
 * requisição inteira, cabeçalhos e corpo somados — é o que faz
 * {@code isValid(s)} voltar em cerca de {@code s}.
 *
 * <p><b>Quem cria o prazo decide o que o estouro significa</b>, e por isso o
 * prazo carrega a tradução. O mesmo silêncio do servidor é um
 * {@code SQLTimeoutException} quando o prazo veio do {@code queryTimeout} da
 * consulta (a conexão continua utilizável), um {@code false} no
 * {@code isValid}, e uma conexão fechada quando o prazo é o de rede — que é o
 * contrato do {@code Connection.setNetworkTimeout}. O transporte só detecta;
 * não conhece nenhuma dessas políticas.
 *
 * @param millis o prazo em ms; zero é "sem prazo", o comportamento de antes
 *               deste parâmetro existir
 * @param onExpiry a tradução do estouro, chamada uma vez, na thread que
 *                 esperava
 */
public record RequestTimeout(long millis, ExpiryHandler onExpiry) {

    /** Sem prazo: a requisição espera o servidor indefinidamente. */
    public static final RequestTimeout NONE = new RequestTimeout(0, detail -> new SQLException(
            "estouro de prazo numa requisicao sem prazo: " + detail, SqlStates.INTERNO));

    /** A tradução do estouro em exceção, com os efeitos que a política exigir. */
    @FunctionalInterface
    public interface ExpiryHandler {

        /**
         * @param detail o que esperou e por quanto tempo, já em texto
         * @return a exceção que a operação que esperava vai lançar
         */
        SQLException onExpiry(String detail);
    }

    public RequestTimeout {
        if (millis < 0) {
            throw new IllegalArgumentException("prazo negativo: " + millis);
        }
        if (onExpiry == null) {
            throw new IllegalArgumentException("prazo sem traducao do estouro");
        }
    }

    /** Se há prazo. Sem ele, nada é agendado e o corpo não é embrulhado. */
    public boolean isSet() {
        return millis > 0;
    }
}
