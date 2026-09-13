package server

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// unknownSchema é a recusa do schema da conexão que nenhum dataset declara, e é
// a MESMA redação no /v1/query e no /v1/metadata?schema=.
//
// Ela ensina em vez de só recusar, porque o caso mais provável não é digitação:
// é o endereço que nomeava o DATASET antes da conexão por schema
// (`jdbc:nishidb://host:9142/service_view`). Quando o nome recebido é o de um
// dataset registrado — base ou irmã do histórico, que herda o schema —, a
// mensagem diz qual schema usar; senão, lista os schemas que existem.
func (s *Server) unknownSchema(name string) protocol.ErrorDetail {
	if d, ok := s.byNome[name]; ok {
		return protocol.ErrorDetail{
			Code: protocol.CodeUnknownSchema,
			Message: fmt.Sprintf("schema %q is not registered on this server: %q is a dataset, "+
				"and the connection names its schema; use %q", name, name, d.sch.SchemaName),
		}
	}
	schemas := make([]string, 0, len(s.bySchema))
	for n := range s.bySchema {
		schemas = append(schemas, n)
	}
	sort.Strings(schemas)
	return protocol.ErrorDetail{
		Code: protocol.CodeUnknownSchema,
		Message: fmt.Sprintf("schema %q is not registered on this server; registered schemas: %s",
			name, strings.Join(schemas, ", ")),
	}
}
