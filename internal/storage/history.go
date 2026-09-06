package storage

// ParComHistorico embrulha o dataset base e a tabela irmã para os
// consumidores que precisam checkpointar os dois — hoje o writer do Kafka e o
// laço do `nishidb ingest`.
//
// O embutido é o BASE, então SetSourceOffsets, SourceOffsets e RowCount saem
// dele sem intermediário: a posição da fonte é do dataset base, e é o
// manifest dele que a torna durável.
type ParComHistorico struct {
	*Store
	// Hist é a tabela irmã, ou nil.
	Hist *Store
	// AoFalharOHistorico recebe a causa quando o checkpoint da tabela irmã
	// falha. Opcional; sem ele a falha é apenas engolida.
	AoFalharOHistorico func(error)
}

// Checkpoint durabiliza a tabela irmã ANTES do dataset base.
//
// A ordem é a garantia, e é a mesma do §48 aplicada a mais um arquivo: o
// checkpoint do base é o que torna a posição da fonte durável, e o commit do
// offset no broker só acontece depois dele. Com o histórico primeiro, um
// crash entre os dois deixa o offset ATRÁS dos dois datasets — o replay
// reprocessa a janela e o histórico ganha DUPLICATAS, nunca buracos. Na ordem
// inversa, os eventos entre os dois pontos nunca chegariam à tabela irmã.
//
// A falha do histórico NÃO impede o checkpoint do base, e é deliberado:
// bloqueá-lo travaria o avanço do offset, e a ingestão de produção pararia
// por causa da tabela de debug — exatamente o que a política de degradação
// existe para impedir. O preço fica escrito: os eventos que o histórico não
// gravou saem da janela de replay quando o offset avança, e não voltam.
func (p ParComHistorico) Checkpoint() error {
	if p.Hist != nil {
		if err := p.Hist.Checkpoint(); err != nil && p.AoFalharOHistorico != nil {
			p.AoFalharOHistorico(err)
		}
	}
	return p.Store.Checkpoint()
}
