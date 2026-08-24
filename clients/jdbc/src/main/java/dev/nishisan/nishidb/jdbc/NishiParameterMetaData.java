package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * O metadado dos parâmetros — e ele ADMITE que não sabe o tipo.
 *
 * <p>Quem decide o tipo de um parâmetro é a COLUNA contra a qual ele é
 * comparado, e essa decisão acontece no servidor, no bind: o mesmo {@code ?}
 * vale um inteiro contra uma coluna int64 e uma string contra uma ipv4.
 * Inventar um tipo aqui faria a ferramenta oferecer o editor errado e depois
 * culpar o servidor pela recusa.
 *
 * <p>O que o driver SABE é a contagem, e ela vem do mesmo scanner que extrai o
 * dataset do FROM.
 */
public final class NishiParameterMetaData implements ParameterMetaData {

    private final int quantos;

    NishiParameterMetaData(int quantos) {
        this.quantos = quantos;
    }

    @Override
    public int getParameterCount() {
        return quantos;
    }

    @Override
    public int isNullable(int parametro) throws SQLException {
        exige(parametro);
        // Não é "pode ser nulo": é "não há como saber", e é a verdade — o
        // protocolo não tem parâmetro nulo, e a nulabilidade da COLUNA depende
        // de qual coluna o bind escolher.
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int parametro) throws SQLException {
        exige(parametro);
        return false;
    }

    @Override
    public int getPrecision(int parametro) throws SQLException {
        exige(parametro);
        return 0;
    }

    @Override
    public int getScale(int parametro) throws SQLException {
        exige(parametro);
        return 0;
    }

    @Override
    public int getParameterType(int parametro) throws SQLException {
        exige(parametro);
        return Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int parametro) throws SQLException {
        exige(parametro);
        return "unknown";
    }

    @Override
    public String getParameterClassName(int parametro) throws SQLException {
        exige(parametro);
        return "java.lang.Object";
    }

    /** Só entrada: o protocolo não devolve valor por parâmetro. */
    @Override
    public int getParameterMode(int parametro) throws SQLException {
        exige(parametro);
        return parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> alvo) throws SQLException {
        if (alvo.isInstance(this)) {
            return alvo.cast(this);
        }
        throw new SQLException("este ParameterMetaData não embrulha " + alvo.getName(),
                SqlStates.NAO_SUPORTADO);
    }

    @Override
    public boolean isWrapperFor(Class<?> alvo) {
        return alvo.isInstance(this);
    }

    private void exige(int parametro) throws SQLException {
        if (parametro < 1 || parametro > quantos) {
            throw new SQLException("parâmetro " + parametro + " fora de [1," + quantos + "]",
                    SqlStates.CONVERSAO_INVALIDA);
        }
    }
}
