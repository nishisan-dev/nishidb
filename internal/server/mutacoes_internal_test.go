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

// TestProcessaDatasetsFalhaNoPrimeiroItemDevolveProcessadosVazio é o gap de
// cobertura 2 da re-revisão da B5: o teste do meio (acima) já prova a
// ORDEM; falta o EXTREMO em que não há absolutamente nada processado antes
// da falha — Processados tem de vir VAZIO (não nil-mas-tratado-como-vazio
// por acidente; a asserção confere len==0 de verdade), e é este caso que
// mensagemParcial usa para decidir entre nomear os antecessores e dizer
// "nenhum". Substitui o teste anterior, que montava um *ErroParcial à mão
// em vez de produzi-lo pelo caminho real.
func TestProcessaDatasetsFalhaNoPrimeiroItemDevolveProcessadosVazio(t *testing.T) {
	alvos := []*dataset{{nome: "a"}, {nome: "b"}, {nome: "c"}}
	erroFalso := errors.New("falha simulada no dataset a")
	var tentados []string

	processados, parcial := processaDatasets(alvos, func(d *dataset) (string, error) {
		tentados = append(tentados, d.nome)
		if d.nome == "a" {
			return "", erroFalso
		}
		return "ok:" + d.nome, nil
	})

	if parcial == nil {
		t.Fatal("quer ErroParcial não-nil: o dublê falhou no primeiro dataset")
	}
	if parcial.Dataset != "a" {
		t.Errorf("Dataset = %q, quer %q", parcial.Dataset, "a")
	}
	if !errors.Is(parcial.Err, erroFalso) {
		t.Errorf("Err = %v, quer %v", parcial.Err, erroFalso)
	}
	if len(parcial.Processados) != 0 {
		t.Errorf("Processados = %v, quer vazio: nada rodou com sucesso antes do primeiro item falhar", parcial.Processados)
	}
	if len(processados) != 0 {
		t.Errorf("resultado parcial = %v, quer vazio", processados)
	}
	if len(tentados) != 1 || tentados[0] != "a" {
		t.Fatalf("fn foi chamada para %v, quer exatamente [a]: 'b' e 'c' não podem ser tentados", tentados)
	}

	msg := mensagemParcial(parcial, "base")
	if !strings.Contains(msg, "nenhum") {
		t.Errorf("mensagem = %q, quer conter %q (Processados vazio)", msg, "nenhum")
	}
	if !strings.Contains(msg, `"a"`) {
		t.Errorf("mensagem = %q, quer nomear o dataset a", msg)
	}
}

// TestProcessaDatasetsFalhaNoUltimoItemNomeiaTodosOsAnteriores é o outro
// extremo do gap de cobertura 2: quando o ÚLTIMO item da lista falha, TODOS
// os anteriores já tinham sido processados com sucesso — Processados tem
// de nomeá-los todos, na ordem, e o resultado parcial devolvido também.
func TestProcessaDatasetsFalhaNoUltimoItemNomeiaTodosOsAnteriores(t *testing.T) {
	alvos := []*dataset{{nome: "a"}, {nome: "b"}, {nome: "c"}}
	erroFalso := errors.New("falha simulada no dataset c")
	var tentados []string

	processados, parcial := processaDatasets(alvos, func(d *dataset) (string, error) {
		tentados = append(tentados, d.nome)
		if d.nome == "c" {
			return "", erroFalso
		}
		return "ok:" + d.nome, nil
	})

	if parcial == nil {
		t.Fatal("quer ErroParcial não-nil: o dublê falhou no último dataset")
	}
	if parcial.Dataset != "c" {
		t.Errorf("Dataset = %q, quer %q", parcial.Dataset, "c")
	}
	if !errors.Is(parcial.Err, erroFalso) {
		t.Errorf("Err = %v, quer %v", parcial.Err, erroFalso)
	}
	if len(parcial.Processados) != 2 || parcial.Processados[0] != "a" || parcial.Processados[1] != "b" {
		t.Errorf("Processados = %v, quer [a b] (os DOIS anteriores, na ordem)", parcial.Processados)
	}
	if len(processados) != 2 || processados[0] != "ok:a" || processados[1] != "ok:b" {
		t.Errorf("resultado parcial = %v, quer [ok:a ok:b]", processados)
	}
	if len(tentados) != 3 || tentados[0] != "a" || tentados[1] != "b" || tentados[2] != "c" {
		t.Fatalf("fn foi chamada para %v, quer exatamente [a b c]", tentados)
	}

	msg := mensagemParcial(parcial, "base")
	if !strings.Contains(msg, "a, b") {
		t.Errorf("mensagem = %q, quer nomear os dois processados antes (\"a, b\")", msg)
	}
}
