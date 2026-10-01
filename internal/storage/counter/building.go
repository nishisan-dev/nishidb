package counter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// buildingMarkerName é o marcador de CONSTRUÇÃO EM CURSO no diretório do
// contador. É um nome em disco, e por isso contrato.
//
// # Por que existe
//
// A abertura decide entre adotar e reconstruir olhando o manifest (os
// parâmetros) e o vetor (Born e Covers). Nenhuma das duas perguntas sabe se o
// CONTEÚDO do vetor é o de uma construção que terminou:
//
//   - a reconstrução em operação (Store.RebuildCounters) zera o vetor IN
//     PLACE (rowindex.Dense32.Clear) e o mmap é MAP_SHARED: o kernel pode levar
//     as páginas zeradas ao disco a qualquer momento, e um processo que morra
//     antes do checkpoint deixa um vetor zerado com os mesmos parâmetros no
//     manifest, Born falso e Covers verdadeiro;
//   - a construção na abertura cria (ou zera) o vetor e só o completa no
//     ApplyOverlay do checkpoint do fim do Open, que percorre o overlay em
//     ordem arbitrária. Um crash no meio deixa o vetor zerado ou parcial — e,
//     com o manifest anterior descrevendo os mesmos parâmetros, adotado.
//
// Nos dois casos a reabertura adotaria contagem baixa em silêncio: sem órfã,
// sem log, sem divergência que a dobra perceba. O marcador é o fato que
// faltava: ele é gravado (com fsync do arquivo e do diretório) ANTES de o vetor
// ser zerado ou construído, e removido só DEPOIS que o checkpoint que adota o
// vetor completo retorna com sucesso. Marcador presente na abertura força a
// reconstrução.
//
// Renomear os arquivos sozinho não resolveria: o manifest com os mesmos
// parâmetros adotaria o vetor NOVO, parcial, do mesmo jeito.
const buildingMarkerName = "building"

// BuildingMarked diz se o diretório do contador tem o marcador de construção
// em curso. Diretório inexistente é "não": o contador ainda não existe.
func BuildingMarked(dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, buildingMarkerName))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("counter: inspect the building marker in %s: %w", dir, err)
	}
}

// markBuilding grava o marcador de forma durável: o arquivo e a entrada de
// diretório. Idempotente — remarcar uma construção que já estava marcada não
// muda nada.
func markBuilding(dir string) error {
	path := filepath.Join(dir, buildingMarkerName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("counter: create the building marker %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("counter: fsync the building marker %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("counter: close the building marker %s: %w", path, err)
	}
	return atomicfile.SyncDir(dir)
}

// MarkBuilding grava o marcador de construção em curso. Quem vai zerar ou
// construir o vetor chama ANTES de mexer nele — Store.RebuildCounters e o
// ramo da abertura que zera um vetor que não cobre as linhas. A abertura com
// recreate já o grava sozinha (Open).
func (c *Counter) MarkBuilding() error { return markBuilding(c.dir) }

// ClearBuildingMark remove o marcador e torna a remoção durável. Só depois do
// checkpoint que adota a construção: removê-lo antes reabriria a janela que ele
// fecha. Marcador ausente não é erro.
func (c *Counter) ClearBuildingMark() error {
	path := filepath.Join(c.dir, buildingMarkerName)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("counter: remove the building marker %s: %w", path, err)
	}
	return atomicfile.SyncDir(c.dir)
}
