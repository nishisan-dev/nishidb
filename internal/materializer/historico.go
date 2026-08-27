package materializer

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// EscritorDeHistorico é o que o materializer precisa da tabela irmã.
//
// Declarada aqui, e não importada do storage, pela mesma razão que a Store
// deste pacote: manter o materializer testável com um dublê sem arrastar a
// implementação real — e, neste caso, sem arrastar também a possibilidade de
// alguém chamar Put no histórico por engano.
type EscritorDeHistorico interface {
	Apenda(payload []byte) (types.RowID, error)
}

// LigaHistorico conecta a tabela irmã a este materializer.
//
// campoRaw é a coluna `_raw`, ou nil quando `rawPayload` está desligado. Ela
// é o que permite gravar também o evento que NÃO parseia: sem ela não há o
// que guardar de um JSON inválido, e o evento continua só contado.
func (m *Materializer) LigaHistorico(h EscritorDeHistorico, campoRaw *schema.CompiledField) {
	m.hist = h
	m.histRaw = campoRaw
}

// HistoricoDegradado devolve a causa da degradação, ou nil.
func (m *Materializer) HistoricoDegradado() error { return m.histCausa }

// apendaNoHistorico grava a linha do evento e NUNCA propaga erro.
//
// O histórico é ferramenta; o produto é o dataset base. Uma falha aqui conta,
// degrada e segue — derrubar a ingestão de produção porque o disco do debug
// encheu seria trocar o que importa pelo que ajuda a entender o que importa.
//
// Roda logo DEPOIS do Parse e ANTES do stale check, e as três consequências
// disso são o desenho:
//
//  1. patch.entries É o evento cru mapeado, com as colunas derivadas de
//     `extract` já resolvidas e o `_writtenAt` já injetado. Nenhum decode a
//     mais, nenhuma segunda passada sobre o JSON.
//  2. o evento que a `sequence` descartaria também é gravado — que é
//     exatamente o que se quer ver ao depurar um descarte por stale.
//  3. o `_expiresIn` ainda não foi emitido, então ele não entra na linha sem
//     precisar de filtro: o prazo por entidade não significa nada aqui.
func (m *Materializer) apendaNoHistorico(evento []byte) {
	if m.hist == nil {
		return
	}
	if m.histCausa != nil {
		m.metrics.histDescartadas.Add(1)
		return
	}
	payload, err := encodePatchHistorico(m.patch.entries, evento, m.histRaw, m.histBuf[:0])
	m.histBuf = payload
	if err == nil {
		_, err = m.hist.Apenda(payload)
	}
	if err != nil {
		m.degradaHistorico(err)
		return
	}
	m.metrics.histLinhas.Add(1)
}

// apendaEventoRuimNoHistorico grava a linha do evento que o schema não
// conseguiu mapear.
//
// Só existe com `rawPayload: true`, e é a única razão de peso para ligá-lo:
// sem a coluna `_raw` não há o que guardar de um JSON inválido, e o evento
// que nunca virou linha continua sendo apenas um número num contador — que é
// o ponto cego que este histórico existe para fechar.
//
// A linha sai com `_writtenAt` e `_raw`, e mais nada. Ela não tem identifier,
// porque a falha pode ter sido justamente ali.
func (m *Materializer) apendaEventoRuimNoHistorico(evento []byte) {
	if m.hist == nil || m.histRaw == nil {
		return
	}
	if m.histCausa != nil {
		m.metrics.histDescartadas.Add(1)
		return
	}
	payload, err := encodeEventoRuim(m.schema.Escrita, m.histRaw, m.agora().UnixMilli(), evento, m.histBuf[:0])
	m.histBuf = payload
	if err == nil {
		_, err = m.hist.Apenda(payload)
	}
	if err != nil {
		m.degradaHistorico(err)
		return
	}
	m.metrics.histLinhas.Add(1)
}

// degradaHistorico entra no modo falho STICKY.
//
// Sticky pela mesma razão do writer do storage: o erro que chega aqui é disco
// cheio, dataset fechado ou writer morto, e a chamada seguinte reencontraria
// o mesmo estado. Sem a marca, cada evento tentaria de novo e o log viraria
// uma linha por evento sobre uma condição que não muda sozinha.
//
// A consulta ao histórico degradado continua respondendo, sobre o que já foi
// gravado. Um histórico que some quando quebra é pior que um truncado com a
// marca de que truncou.
func (m *Materializer) degradaHistorico(causa error) {
	m.histCausa = causa
	m.metrics.histDescartadas.Add(1)
	m.metrics.histDegradado.Store(true)
}

// encodePatchHistorico serializa as entries do evento para a tabela irmã.
//
// Difere do encodePatch em duas coisas, e as duas são o que separa "o que a
// origem mandou" de "o que o merge decidiu":
//
// As congeladas SAEM. A captura emite a entry com o valor CORRENTE da origem,
// e é o merge que preserva o primeiro. No histórico, que não tem merge, uma
// `originalSeverity` seria igual a `severity` em toda linha — duplicação com
// cara de informação.
//
// O `_raw` ENTRA, quando declarado, e entra na POSIÇÃO do FieldID dele: o
// payload exige ordem estritamente crescente, e o id de `_raw` não é
// necessariamente o maior — ele é atribuído quando o histórico compila, e
// toda coluna descoberta depois disso ganha um id maior.
func encodePatchHistorico(patch []patchEntry, evento []byte, raw *schema.CompiledField, out []byte) ([]byte, error) {
	var (
		lastID     types.FieldID
		err        error
		rawEscrito bool
	)
	for _, e := range patch {
		id := e.field.ID
		if id <= lastID {
			return out, fmt.Errorf("%w: patch fora de ordem: campo %d após %d", ErrInternalState, id, lastID)
		}
		lastID = id
		if raw != nil && !rawEscrito && raw.ID < id {
			if out, err = encoding.AppendField(out, raw.ID, valorBruto(evento)); err != nil {
				return out, err
			}
			rawEscrito = true
		}
		// isNull é REMOÇÃO de campo, que só faz sentido sobre estado anterior.
		// Aqui ela é o fato de a origem ter mandado null, e o jeito de
		// registrar isso é o mesmo do resto do produto: a ausência da coluna.
		if e.isNull || e.congelada {
			continue
		}
		if out, err = encoding.AppendField(out, id, e.val); err != nil {
			return out, err
		}
	}
	if raw != nil && !rawEscrito {
		if out, err = encoding.AppendField(out, raw.ID, valorBruto(evento)); err != nil {
			return out, err
		}
	}
	return out, nil
}

// encodeEventoRuim monta a linha mínima do evento que não parseou.
func encodeEventoRuim(escrita, raw *schema.CompiledField, agoraMs int64, evento []byte, out []byte) ([]byte, error) {
	var err error
	// A ordem por FieldID crescente vale aqui como em qualquer payload, e os
	// dois campos podem estar em qualquer ordem relativa: `_writtenAt` é
	// registrado pelo dataset base e `_raw` pelo histórico, mas uma recriação
	// de um dos dois muda quem veio antes.
	if escrita != nil && escrita.ID > raw.ID {
		if out, err = encoding.AppendField(out, raw.ID, valorBruto(evento)); err != nil {
			return out, err
		}
		return encoding.AppendField(out, escrita.ID, types.Value{Type: types.TypeTimestamp, I64: agoraMs})
	}
	if escrita != nil {
		if out, err = encoding.AppendField(out, escrita.ID, types.Value{Type: types.TypeTimestamp, I64: agoraMs}); err != nil {
			return out, err
		}
	}
	return encoding.AppendField(out, raw.ID, valorBruto(evento))
}

// valorBruto embrulha os bytes do evento como documento json.
//
// json e não bytes: o valor É um documento, e declarar o tipo certo é o que
// permite `_raw.chave` na projeção do dia em que alguém precisar disso — sem
// mudar o que está gravado.
func valorBruto(evento []byte) types.Value {
	return types.Value{Type: types.TypeJSON, Bytes: evento}
}
