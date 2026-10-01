package dev.nishisan.nishidb.jdbc.wire;

import java.io.IOException;
import java.sql.SQLException;

/**
 * O estouro do prazo durante a leitura do corpo, carregando a exceção que a
 * política do prazo já montou.
 *
 * <p>Viaja como {@link IOException} porque é por {@code InputStream.read} que
 * o estouro chega a quem lê o corpo; quem a captura ({@link LeitorNdjson},
 * {@link Transporte}) lança a {@link #failure()} em vez de traduzir o
 * {@code IOException} como queda de conexão.
 */
final class ResponseTimeoutException extends IOException {

    private static final long serialVersionUID = 1L;

    private final transient SQLException failure;

    ResponseTimeoutException(SQLException failure) {
        super(failure.getMessage());
        this.failure = failure;
    }

    /** A exceção que a operação que esperava tem de lançar. */
    SQLException failure() {
        return failure;
    }
}
