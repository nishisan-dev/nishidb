package dev.nishisan.nishidb.jdbc;

import dev.nishisan.nishidb.jdbc.wire.SqlStates;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * O ponto de entrada do §70: {@code jdbc:nishidb://host[:porta]/dataset[?k=v]}.
 *
 * <p>Registra-se no {@link DriverManager} de duas formas, e as duas são
 * necessárias: pelo bloco estático, para quem faz {@code Class.forName}, e pelo
 * arquivo {@code META-INF/services/java.sql.Driver}, que é o único caminho que
 * uma ferramenta gráfica tem — ela põe o jar no classpath e não chama classe
 * nenhuma pelo nome.
 */
public final class NishiDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new NishiDriver());
        } catch (SQLException e) {
            // Sem lugar para onde propagar num inicializador estático, e
            // engolir seria pior: quem tenta conectar veria "no suitable
            // driver" sem nenhuma pista da causa.
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * NUNCA lança, nem para {@code null}.
     *
     * <p>O DriverManager percorre TODOS os drivers registrados perguntando a
     * cada um; um que lance aborta a busca inteira, e o sintoma aparece na
     * conexão que o usuário estava abrindo com outro banco.
     */
    @Override
    public boolean acceptsURL(String url) {
        return NishiUrl.aceita(url);
    }

    /** Devolve {@code null} — e não erro — quando a URL é de outro banco. */
    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        return new NishiConnection(ConnectionConfig.de(url, props));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) {
        return ConnectionConfig.propriedades(props);
    }

    @Override
    public int getMajorVersion() {
        return DriverVersion.MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return DriverVersion.MINOR;
    }

    /**
     * FALSO, e dizer o contrário seria mentira verificável.
     *
     * <p>O contrato exige suporte a SQL-92 Entry Level. O NishiDB não tem JOIN,
     * não tem GROUP BY e não tem transação — e uma ferramenta que confiasse no
     * "true" montaria consultas que o servidor recusa.
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Não há logger: o driver não escreve log próprio.
     *
     * <p>Um driver que loga por conta acaba escrevendo credencial e SQL no log
     * da aplicação sem que ela tenha pedido. O que o driver tem a dizer sai
     * pela mensagem da SQLException, que é onde quem chamou está olhando.
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("este driver não usa java.util.logging",
                SqlStates.NAO_SUPORTADO);
    }
}
