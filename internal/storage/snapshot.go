package storage

import "github.com/nishisan-dev/nishidb/internal/types"

// AbreSnapshot fixa o universo da consulta com três leituras atômicas e
// nenhuma alocação.
//
// Não há par Fecha: em Snapshot não há recurso a devolver — ele é um valor, e
// o Store não guarda referência a ele.
func (s *Store) AbreSnapshot() types.Snapshot {
	return types.Snapshot{
		Instante:    s.agora(),
		LimiteRowID: types.RowID(s.nextRowID.Load()),
		BaseRowID:   types.RowID(s.rowIDBase.Load()),
	}
}
