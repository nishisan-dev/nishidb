package segindex

import "github.com/nishisan-dev/nishidb/internal/types"

// Seal converte a forma WIP do segmento na forma selada, e desliga os .widx.
//
// A ORDEM é a garantia: o Writer grava o .sidx por .tmp + fsync + rename +
// fsync do diretório (writer.go), e SÓ ENTÃO os .widx são removidos. Na
// ordem inversa, um crash no meio deixaria o segmento sem índice nenhum, e
// ele responderia por enumeração até a reconstrução alcançá-lo — correto, mas
// trabalho que não precisava existir.
//
// Idempotente em dois estados, e eles são DIFERENTES. Um segmento sem .widx é
// no-op, presente ou não o .sidx — é o estado que uma selagem COMPLETA deixa, e
// o teste da idempotência confere que o arquivo não muda byte nenhum na segunda
// chamada. Já o crash entre o rename do .sidx e o unlink dos .widx deixa OS
// DOIS em disco (o texto anterior afirmava o estado oposto): um Seal reinvocado
// ali releria os .widx e regravaria os MESMOS bytes (golden §6.3) antes de
// completar o unlink — mas essa reinvocação não existe em processo (o Swap do
// aposAppend garante uma selagem por segmento), e através de um restart o Open
// descarta os .widx antes de qualquer Seal (§2.5). O estado é alcançável; o
// caminho que o atravessaria, não.
func Seal(dir string, seg uint32) error {
	fields, err := ListWIP(dir, seg)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	e := NewWriter()
	for _, field := range fields {
		// slotsMin == 0: abre o .widx já gravado sem redimensionar. Criar um
		// arquivo aqui faria o segmento passar por indexado sem posting
		// nenhuma (contrato de OpenWIP em wip.go).
		w, err := OpenWIP(dir, seg, field, 0)
		if err != nil {
			return err
		}
		// ForEach não entrega nada ordenado, de propósito (wip.go) — o
		// Writer é quem ordena e deduplica. Reordenar aqui seria a mesma
		// ordenação em dois lugares.
		err = w.ForEach(func(h uint64, row types.RowID) {
			e.Add(field, h, row)
		})
		if ferr := w.Close(); err == nil {
			err = ferr
		}
		if err != nil {
			return err
		}
	}
	if err := e.WriteSealed(SealedPath(dir, seg), seg); err != nil {
		return err
	}
	return RemoveWIPs(dir, seg)
}

// RemoveWIPs remove os .widx do segmento, depois que o .sidx já está gravado e
// durável. É o segundo tempo da selagem, separado de Seal para que a Tarefa 5
// (rotação) possa reagir ao meio-termo — .sidx presente, .widx ainda de pé —
// sem precisar reabrir o Writer.
func RemoveWIPs(dir string, seg uint32) error {
	fields, err := ListWIP(dir, seg)
	if err != nil {
		return err
	}
	for _, field := range fields {
		// slotsMin == 0: mesmo motivo de Seal — só abre o que já está lá.
		w, err := OpenWIP(dir, seg, field, 0)
		if err != nil {
			return err
		}
		// Remove direto, sem Close antes: o arquivo está indo embora, e Remove
		// já desmonta o mapeamento sem gravar o header (wip.go) — gravá-lo
		// aqui sujaria uma página que o arquivo removido em seguida nunca
		// precisou ter.
		if err := w.Remove(); err != nil {
			return err
		}
	}
	return nil
}
