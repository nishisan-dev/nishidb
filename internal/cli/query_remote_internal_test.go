package cli

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
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
		{protocol.CodeSyntax, ExitQuery, "a query está errada e nenhuma releitura do servidor a conserta"},
		{protocol.CodeBind, ExitQuery, "coluna ou dataset que não casa com o schema é erro da query"},
		{protocol.CodeBadParameter, ExitQuery, "parâmetro fora do contrato é erro de quem escreveu a consulta"},
		{protocol.CodeUnsupportedClause, ExitQuery, "cláusula ausente da v1 se conserta reescrevendo o SQL, e não olhando o servidor: o default deste switch daria o conselho errado"},
		{protocol.CodeMaxRows, ExitResultTruncated, "as linhas entregues são válidas; o que falta é o resto"},
		{protocol.CodeTimeout, ExitInterrupted, "a consulta foi cortada por prazo, e repetir mais tarde é a ação"},
		{protocol.CodeDatasetCorrupt, ExitDataset, "o problema é o dataset, não o comando: reabrir antes de insistir"},
		{protocol.CodeBadRequest, ExitUsageError, "o vala-comum manda olhar o comando ou o servidor"},
		{protocol.CodeUnknownDataset, ExitUsageError, "dataset que não existe neste servidor é erro de linha de comando"},
		{protocol.CodeTooManyQueries, ExitUsageError, "o teto de concorrência é do servidor, e a ação é tentar de novo"},
		{protocol.CodeInternal, ExitUsageError, "falha interna não tem código próprio no contrato do §96"},
		{protocol.CodeUnauthenticated, ExitUsageError, "credencial recusada (§74-77) é erro de quem chamou o comando, não do servidor"},
		{protocol.CodeForbidden, ExitUsageError, "papel insuficiente (§74-77) é erro de quem chamou o comando, não do servidor"},
		{protocol.CodeWriterFailed, ExitWriter, "o writer está em modo falho sticky (Fase 11, B5): o MESMO 4 do caminho local"},
		{protocol.CodeCompactionBusy, ExitUsageError, "passada em voo (Fase 11, B5) é retentável, mas cai no MESMO vala-comum de TOO_MANY_QUERIES"},
		{"CODIGO_QUE_NAO_EXISTE", ExitUsageError, "código desconhecido cai no vala-comum, nunca em zero"},
	}
	for _, c := range casos {
		if got := codigoOfProtocolo(c.codigo); got != c.quer {
			t.Errorf("%s → exit %d, queria %d: %s", c.codigo, got, c.quer, c.porque)
		}
	}
}
