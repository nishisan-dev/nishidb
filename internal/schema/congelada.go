package schema

// OrigemRelogio é a origem especial que entrega o instante da escrita em vez
// do valor de outra coluna. É o que faz um `createdAt` sair da mesma máquina
// que o `originalSeverity`, sem caso especial em lugar nenhum.
const OrigemRelogio = "@writeTime"

// validaCongeladas confere as regras de frozenFrom que dependem de OUTROS
// campos, e por isso não cabem no laço por campo: cadeia, autorreferência e
// origem inexistente só são decidíveis com o mapa inteiro na mão.
func validaCongeladas(fields map[string]FieldSpec, keyField, modo string) error {
	for nome, f := range fields {
		if f.FrozenFrom == "" {
			continue
		}
		if modo == "replace" {
			// No replace o estado anterior é descartado inteiro e encodePatch
			// nem o lê. Uma coluna que sobrevivesse ao replace contradiria o
			// que replace significa, e sustentá-la custaria uma leitura de
			// record por evento no modo cuja razão de existir é não pagá-la.
			return specErrf("campo %q: frozenFrom não se aplica a mode: replace — "+
				"no replace o estado anterior é descartado inteiro", nome)
		}
		if f.Type != "" {
			return specErrf("campo %q: frozenFrom e type juntos — o tipo é herdado da origem %q",
				nome, f.FrozenFrom)
		}
		if f.Format != "" {
			return specErrf("campo %q: format não se aplica a frozenFrom — "+
				"o formato é do parsing da origem", nome)
		}
		if f.FrozenFrom == OrigemRelogio {
			// O relógio entrega timestamp, e nenhum dos dois motores de índice
			// aceita esse tipo — a checagem sai aqui em vez de deixar o
			// `index:` decodificar em silêncio e não fazer nada.
			if err := validaIndice(nome, f.Index, "timestamp"); err != nil {
				return err
			}
			continue
		}
		if f.FrozenFrom == nome {
			return specErrf("campo %q: frozenFrom aponta para si mesmo", nome)
		}
		if f.FrozenFrom == keyField {
			return specErrf("campo %q: frozenFrom não aceita a key %q — "+
				"a key identifica a entidade e já é constante", nome, keyField)
		}
		origem, existe := fields[f.FrozenFrom]
		if !existe {
			return specErrf("campo %q: frozenFrom nomeia %q, que não está declarado em schema.fields",
				nome, f.FrozenFrom)
		}
		// A indexabilidade se decide pelo tipo da ORIGEM: é o valor dela que
		// vai para a posting list.
		if err := validaIndice(nome, f.Index, origem.Type); err != nil {
			return err
		}
		if origem.FrozenFrom != "" {
			// A cadeia faria a ordem de avaliação virar contrato implícito, e
			// o valor dependeria de qual das duas foi capturada primeiro.
			return specErrf("campo %q: frozenFrom aponta para %q, que também é congelada — "+
				"cadeia de congelamento não é suportada", nome, f.FrozenFrom)
		}
	}
	return nil
}

// validaIndice confere a compatibilidade entre o motor de índice pedido e o
// tipo da coluna. Vive numa função porque a congelada faz a MESMA pergunta
// sobre o tipo de outro campo, e duas cópias divergiriam no dia em que um
// motor passasse a aceitar mais um tipo.
func validaIndice(nome, index, tipo string) error {
	switch index {
	case "":
		return nil
	case "bitmap":
		if !bitmapIndexable(tipo) {
			return specErrf("campo %q: tipo %q não suporta índice bitmap", nome, tipo)
		}
	case "hash":
		if !hashIndexable(tipo) {
			return specErrf("campo %q: tipo %q não suporta índice hash", nome, tipo)
		}
	default:
		return specErrf("campo %q: index %q inválido (bitmap|hash)", nome, index)
	}
	return nil
}
