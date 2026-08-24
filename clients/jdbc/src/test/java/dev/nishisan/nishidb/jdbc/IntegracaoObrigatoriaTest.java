package dev.nishisan.nishidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * A guarda contra o SKIP silencioso — o pecado do débito 53 traduzido para o
 * JUnit.
 *
 * <p>As classes {@code *IT} são condicionadas à presença de
 * {@code NISHIDB_IT_URL}, para que um {@code mvnw test} avulso continue
 * funcionando sem servidor. Só que essa condição, sozinha, torna possível o
 * pior desfecho: o harness quebrar — porta errada, servidor que não subiu,
 * variável com nome trocado — e a suíte terminar VERDE, com zero testes de
 * integração executados e ninguém percebendo.
 *
 * <p>Esta classe NÃO é condicionada. Ela roda sempre, e quando
 * {@code NISHIDB_IT_REQUIRED=1} está no ambiente — que é o que o
 * {@code scripts/jdbc-it.sh} seta — ela exige que a URL esteja lá. Harness
 * quebrado reprova, em vez de passar em silêncio.
 */
class IntegracaoObrigatoriaTest {

    static final String URL = System.getenv("NISHIDB_IT_URL");
    static final String USUARIO = System.getenv("NISHIDB_IT_USER");
    static final String SENHA = System.getenv("NISHIDB_IT_PASSWORD");
    static final String URL_SEM_CA = System.getenv("NISHIDB_IT_URL_SEM_CA");

    @Test
    void quandoAIntegracaoEhExigidaOHarnessTemDeTerEntregadoOAmbiente() {
        if (!"1".equals(System.getenv("NISHIDB_IT_REQUIRED"))) {
            // Sem a exigência, este teste não tem o que afirmar: é o modo
            // `mvnw test` avulso, em que os IT ficam de fora por decisão.
            return;
        }
        if (URL == null || URL.isBlank()) {
            fail("NISHIDB_IT_REQUIRED=1 mas NISHIDB_IT_URL está vazia: o harness de integração "
                    + "não entregou o servidor, e as classes *IT teriam sido puladas em silêncio");
        }
        assertNotNull(USUARIO, "NISHIDB_IT_USER ausente");
        assertNotNull(SENHA, "NISHIDB_IT_PASSWORD ausente");
        assertNotNull(URL_SEM_CA, "NISHIDB_IT_URL_SEM_CA ausente");
    }
}
