package dev.nishisan.nishidb.jdbc;

/**
 * Os SQLStates que o driver publica.
 *
 * <p>Por enquanto só os que a URL e a configuração precisam; a tabela completa
 * de código do protocolo para SQLState entra junto do transporte.
 */
public final class SqlStates {

    /** 08001 — o cliente não conseguiu estabelecer a conexão. */
    public static final String URL_INVALIDA = "08001";

    private SqlStates() {
    }
}
