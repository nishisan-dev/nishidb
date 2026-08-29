package segindex

import "github.com/nishisan-dev/nishidb/internal/types"

// Sela converte a forma WIP do segmento na forma selada, e desliga os .widx.
//
// A ORDEM é a garantia: o Escritor grava o .sidx por .tmp + fsync + rename +
// fsync do diretório (escritor.go), e SÓ ENTÃO os .widx são removidos. Na
// ordem inversa, um crash no meio deixaria o segmento sem índice nenhum, e
// ele responderia por enumeração até a reconstrução alcançá-lo — correto, mas
// trabalho que não precisava existir.
//
// Idempotente: um segmento sem .widx é no-op, presente ou não o .sidx. É
// exatamente o estado que um crash entre o rename do .sidx e o unlink dos
// .widx deixa, e Sela precisa atravessá-lo sem reescrever nada — o teste que
// cobre isso confere que o arquivo não muda byte nenhum na segunda chamada.
func Sela(dir string, seg uint32) error {
	campos, err := ListaWIP(dir, seg)
	if err != nil {
		return err
	}
	if len(campos) == 0 {
		return nil
	}
	e := NovoEscritor()
	for _, campo := range campos {
		// slotsMin == 0: abre o .widx já gravado sem redimensionar. Criar um
		// arquivo aqui faria o segmento passar por indexado sem posting
		// nenhuma (contrato de AbreWIP em wip.go).
		w, err := AbreWIP(dir, seg, campo, 0)
		if err != nil {
			return err
		}
		// ParaCada não entrega nada ordenado, de propósito (wip.go) — o
		// Escritor é quem ordena e deduplica. Reordenar aqui seria a mesma
		// ordenação em dois lugares.
		err = w.ParaCada(func(h uint64, row types.RowID) {
			e.Adiciona(campo, h, row)
		})
		if ferr := w.Fecha(); err == nil {
			err = ferr
		}
		if err != nil {
			return err
		}
	}
	if err := e.Escreve(CaminhoSelado(dir, seg), seg); err != nil {
		return err
	}
	return ApagaWIPs(dir, seg)
}

// ApagaWIPs remove os .widx do segmento, depois que o .sidx já está gravado e
// durável. É o segundo tempo da selagem, separado de Sela para que a Tarefa 5
// (rotação) possa reagir ao meio-termo — .sidx presente, .widx ainda de pé —
// sem precisar reabrir o Escritor.
func ApagaWIPs(dir string, seg uint32) error {
	campos, err := ListaWIP(dir, seg)
	if err != nil {
		return err
	}
	for _, campo := range campos {
		// slotsMin == 0: mesmo motivo de Sela — só abre o que já está lá.
		w, err := AbreWIP(dir, seg, campo, 0)
		if err != nil {
			return err
		}
		// Apaga direto, sem Fecha antes: o arquivo está indo embora, e Apaga
		// já desmonta o mapeamento sem gravar o header (wip.go) — gravá-lo
		// aqui sujaria uma página que o arquivo removido em seguida nunca
		// precisou ter.
		if err := w.Apaga(); err != nil {
			return err
		}
	}
	return nil
}
