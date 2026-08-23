package server

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// TestErroDeMutacaoTraduzATabelaInteira trava a tabela de tradução que as
// duas rotas mutantes compartilham — inclusive o caso em que uma falha de
// Store.Checkpoint chamada POR DENTRO de CompactaUmaVez (aposentaSegmento
// chama Checkpoint) chega como ErrWriterFailed: as DUAS rotas passam pelo
// MESMO writer e podem encontrar o MESMO modo falho sticky, então o
// mapeamento não pode ser exclusivo de /v1/checkpoint.
func TestErroDeMutacaoTraduzATabelaInteira(t *testing.T) {
	casos := []struct {
		nome       string
		err        error
		querStatus int
		querCodigo string
	}{
		{
			"compaction busy", storage.ErrCompactionBusy,
			http.StatusConflict, protocolo.CodCompactionBusy,
		},
		{
			"writer failed", fmt.Errorf("checkpoint: %w", storage.ErrWriterFailed),
			http.StatusInternalServerError, protocolo.CodWriterFailed,
		},
		{
			"dataset corrupt", fmt.Errorf("varredura: %w", storage.ErrDatasetCorrupt),
			http.StatusInternalServerError, protocolo.CodDatasetCorrompido,
		},
		{
			"erro desconhecido cai no vala-comum", errors.New("falha qualquer de I/O"),
			http.StatusInternalServerError, protocolo.CodInterno,
		},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			status, falha := erroDeMutacao(c.err)
			if status != c.querStatus {
				t.Errorf("status = %d, quer %d", status, c.querStatus)
			}
			if falha.Code != c.querCodigo {
				t.Errorf("código = %q, quer %q", falha.Code, c.querCodigo)
			}
			if falha.Message == "" {
				t.Error("mensagem vazia: o cliente precisa de algum texto para agir")
			}
		})
	}
}
