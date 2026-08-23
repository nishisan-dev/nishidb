package cli

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
)

// A tradução código→exit code é o CONTRATO que os três comandos de leitura do
// §96 herdaram do `query --server`: é ela que permite a um script trocar
// `--config` por `--server`, ou `query` por `status`, sem reescrever o
// tratamento de $?.
//
// Dois ramos da tabela não são alcançáveis por teste de fora, e ficaram sem
// oráculo nenhum no repositório: DATASET_CORRUPT exigiria avariar o dataset sob
// o servidor no ar, e TIMEOUT exigiria uma consulta que estoure o query.timeout
// — prazo que o teste teria de encurtar para segundos e depois torcer para ele
// ser alcançado, que é receita de teste intermitente. As duas rotas de LEITURA
// não os produzem: /v1/stats e /v1/metadata só recusam com UNKNOWN_DATASET e
// 405.
//
// Daí o teste interno, e daí ele cobrir a tabela INTEIRA: mutar qualquer um dos
// dois ramos deixava `go test ./...` verde, e o operador só descobriria pelo
// script que decidiu errado em produção.
func TestCodigoDoProtocoloTraduzATabelaInteira(t *testing.T) {
	casos := []struct {
		codigo string
		quer   int
		porque string
	}{
		{protocolo.CodSintaxe, ExitQuery, "a query está errada e nenhuma releitura do servidor a conserta"},
		{protocolo.CodBind, ExitQuery, "coluna ou dataset que não casa com o schema é erro da query"},
		{protocolo.CodBadParameter, ExitQuery, "parâmetro fora do contrato é erro de quem escreveu a consulta"},
		{protocolo.CodMaxRows, ExitTruncado, "as linhas entregues são válidas; o que falta é o resto"},
		{protocolo.CodTimeout, ExitInterromp, "a consulta foi cortada por prazo, e repetir mais tarde é a ação"},
		{protocolo.CodDatasetCorrompido, ExitDataset, "o problema é o dataset, não o comando: reabrir antes de insistir"},
		{protocolo.CodBadRequest, ExitUso, "o vala-comum manda olhar o comando ou o servidor"},
		{protocolo.CodDatasetDesconhecido, ExitUso, "dataset que não existe neste servidor é erro de linha de comando"},
		{protocolo.CodMuitasQueries, ExitUso, "o teto de concorrência é do servidor, e a ação é tentar de novo"},
		{protocolo.CodInterno, ExitUso, "falha interna não tem código próprio no contrato do §96"},
		{protocolo.CodUnauthenticated, ExitUso, "credencial recusada (§74-77) é erro de quem chamou o comando, não do servidor"},
		{protocolo.CodForbidden, ExitUso, "papel insuficiente (§74-77) é erro de quem chamou o comando, não do servidor"},
		{"CODIGO_QUE_NAO_EXISTE", ExitUso, "código desconhecido cai no vala-comum, nunca em zero"},
	}
	for _, c := range casos {
		if got := codigoDoProtocolo(c.codigo); got != c.quer {
			t.Errorf("%s → exit %d, queria %d: %s", c.codigo, got, c.quer, c.porque)
		}
	}
}
