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
	GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error)
	GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error)
	// OpenSnapshot fixa o universo desta consulta. É chamado UMA vez por
	// Open, antes de qualquer fonte ser aberta: um snapshot por fonte daria
	// a cada plano um universo próprio, que é exatamente o defeito que ele
	// existe para fechar.
	OpenSnapshot() types.Snapshot
	// CloseSnapshot devolve o que o snapshot segurava, e o Close do iterador
	// o chama em TODOS os caminhos de saída — inclusive erro e cancelamento.
	// Uma consulta que não fecha o seu deixa o writer guardando versões que
	// ninguém vai ler e a compaction sem poder aposentar os segmentos onde
	// elas moram, pela vida do processo.
	CloseSnapshot(snap types.Snapshot)
	ScanLive(snap types.Snapshot) types.RowIDIterator
	// Lookup abre o iterador de candidatos de um termo `campo = valor`.
	//
	// É UM método na interface, e não mais o LookupBitmap somado à capability
	// OPCIONAL HashLookuper, detectada por type assertion: existe um motor de
	// índice só (spec §3.1), e a opcional obrigava todo caminho de abertura a
	// carregar o ramo "e se o store não a implementar" para um erro que o
	// planner já tinha o dever de não deixar chegar aqui.
	Lookup(field types.FieldID, v types.Value) (types.RowIDIterator, error)
}
