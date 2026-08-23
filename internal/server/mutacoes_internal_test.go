package server

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
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

// TestProcessaDatasetsParaNoPrimeiroErroENomeiaOsJaProcessados é o Important
// 3 da revisão da B5: "todos os datasets" para no primeiro erro (política
// mantida), mas o erro tem de NOMEAR o dataset que falhou e os que já
// tinham tido sucesso ANTES dele — o efeito colateral já aconteceu para
// esses, e escondê-los do operador seria mentir sobre o que já rodou.
//
// O dublê (não um *dataset real) prova a ORDEM: "a" processa, "b" falha,
// "c" NUNCA é tentado — stop-no-primeiro-erro continua valendo.
func TestProcessaDatasetsParaNoPrimeiroErroENomeiaOsJaProcessados(t *testing.T) {
	alvos := []*dataset{{nome: "a"}, {nome: "b"}, {nome: "c"}}
	erroFalso := errors.New("falha simulada no dataset b")
	var tentados []string

	processados, parcial := processaDatasets(alvos, func(d *dataset) (string, error) {
		tentados = append(tentados, d.nome)
		if d.nome == "b" {
			return "", erroFalso
		}
		return "ok:" + d.nome, nil
	})

	if parcial == nil {
		t.Fatal("quer ErroParcial não-nil: o dublê falhou no dataset b")
	}
	if parcial.Dataset != "b" {
		t.Errorf("Dataset = %q, quer %q", parcial.Dataset, "b")
	}
	if !errors.Is(parcial.Err, erroFalso) {
		t.Errorf("Err = %v, quer %v", parcial.Err, erroFalso)
	}
	if len(parcial.Processados) != 1 || parcial.Processados[0] != "a" {
		t.Errorf("Processados = %v, quer [a] (só o que rodou COM SUCESSO antes do que falhou)", parcial.Processados)
	}
	if len(processados) != 1 || processados[0] != "ok:a" {
		t.Errorf("resultado parcial = %v, quer [ok:a]", processados)
	}
	if len(tentados) != 2 || tentados[0] != "a" || tentados[1] != "b" {
		t.Fatalf("fn foi chamada para %v, quer exatamente [a b]: 'c' não pode ser tentado depois do erro em 'b'", tentados)
	}
}

// TestProcessaDatasetsSemErroDevolveTodosNaOrdem é o caminho feliz: nenhum
// erro, todos os resultados voltam na MESMA ordem de alvos.
func TestProcessaDatasetsSemErroDevolveTodosNaOrdem(t *testing.T) {
	alvos := []*dataset{{nome: "x"}, {nome: "y"}}
	processados, parcial := processaDatasets(alvos, func(d *dataset) (string, error) {
		return d.nome, nil
	})
	if parcial != nil {
		t.Fatalf("parcial = %+v, quer nil", parcial)
	}
	if len(processados) != 2 || processados[0] != "x" || processados[1] != "y" {
		t.Fatalf("processados = %v, quer [x y]", processados)
	}
}

// TestMensagemParcialNomeiaNenhumQuandoFalhaNoPrimeiro evita uma lista vazia
// silenciosa: "processados: " sem nada ao lado seria ambíguo entre "não sei"
// e "nenhum mesmo" — a mensagem sempre nomeia "nenhum" explicitamente.
func TestMensagemParcialNomeiaNenhumQuandoFalhaNoPrimeiro(t *testing.T) {
	p := &ErroParcial{Dataset: "a", Processados: nil, Err: errors.New("x")}
	msg := mensagemParcial(p, "base")
	if !strings.Contains(msg, "nenhum") {
		t.Errorf("mensagem = %q, quer conter %q", msg, "nenhum")
	}
	if !strings.Contains(msg, `"a"`) {
		t.Errorf("mensagem = %q, quer nomear o dataset a", msg)
	}
}
