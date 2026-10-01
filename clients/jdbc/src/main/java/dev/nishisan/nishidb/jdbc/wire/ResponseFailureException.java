package dev.nishisan.nishidb.jdbc.wire;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Uma falha na leitura do corpo que JÁ SABE qual {@link SQLException} a
 * operação tem de lançar.
 *
 * <p>Nasce em dois lugares: no estouro do prazo ({@link TimedInputStream}, com
 * a exceção que a política do {@link RequestTimeout} montou) e na leitura que
 * falhou porque a conexão foi derrubada de propósito ({@code abort} ou estouro
 * do prazo de rede, ver {@link Transporte#abandon()}). Viaja como
 * {@link IOException} porque é por {@code InputStream.read} que a falha chega
 * a quem lê o corpo; quem a captura ({@link LeitorNdjson}, {@link Transporte})
 * lança a {@link #failure()} em vez de traduzir o {@code IOException} como
 * queda genérica de conexão — que daria a mensagem e o SQLSTATE errados.
 */
final class ResponseFailureException extends IOException {

    private static final long serialVersionUID = 1L;

    private final transient SQLException failure;

    ResponseFailureException(SQLException failure) {
        super(failure.getMessage());
        this.failure = failure;
    }

    /** A exceção que a operação que lia tem de lançar. */
    SQLException failure() {
        return failure;
    }
}
