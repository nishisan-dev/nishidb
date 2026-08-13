// Package executor executa uma query ligada e planejada sobre o storage, em
// STREAMING: nada do resultado é materializado (spec §66). A memória do
// executor é O(batch), nunca O(resultado) — o que separa este projeto de um
// que carrega a resposta em RAM antes de devolvê-la.
//
// A regra que organiza o pacote inteiro (spec §44): o plano escolhe apenas uma
// FONTE DE CANDIDATOS, e o executor REVALIDA o WHERE completo contra o estado
// corrente de cada candidato — inclusive no point lookup, onde a tentação de
// "achei pela key, devolvo" é maior. Índice devolve superset por construção
// (posting órfão é estado tolerado no storage), então confiar nele produziria
// linhas que não satisfazem o filtro.
package executor

import "github.com/nishisan-dev/nishidb/internal/types"

// Store é o recorte do storage consumido pelo executor. *storage.Store e
// *fakestorage.Store satisfazem.
type Store interface {
	// GetByRowID devolve o record da row. ok=false significa row sem location
	// ou tombstoned — as duas formas de "morta".
	//
	// O []byte devolvido é o buffer POSSIVELMENTE CRESCIDO e tem de ser
	// reatribuído pelo caller em TODOS os caminhos, inclusive ok=false e
	// err!=nil: perder o retorno faz o executor realocar por row e deixa
	// rec.Payload apontando para um buffer que ele não conhece mais.
	GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error)
	GetByKey(key, buf []byte) (types.Record, []byte, bool, error)
	ScanLive() types.RowIDIterator
	LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error)
}

// HashLookuper é a capability do índice hash (Fase 8, M4). O *storage.Store do
// M1 NÃO a implementa; o dublê de teste implementa. A detecção é por type
// assertion, feita uma vez em Open — e o planner já degradou o plano quando
// ela falta, então o executor nunca falha em runtime por falta de índice.
type HashLookuper interface {
	LookupHash(field types.FieldID, v types.Value) (types.RowIDIterator, error)
}
