package materializer

import (
	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// countTipo nomeia o contador que uma contagem adiada representa.
type countTipo uint8

const (
	countOcorrencia countTipo = iota
	countCoercao
	countDivergencia
	countCongelada
)

// count é um incremento de contador ADIADO até a aplicação.
//
// POR QUE ADIAR o que é só um atomic.Add. Não é concorrência: os contadores já
// são atômicos e somas comutam, então N decodificadores incrementando em
// paralelo dariam o mesmo total. É o REPROCESSO. O decodificador somente-
// leitura para no primeiro metadado que teria de criar, e o writer refaz o
// evento inteiro pelo caminho completo — se o decode abortado já tivesse
// contado as chaves que percorreu antes de parar, elas contariam DUAS vezes.
//
// E não é contador interno: `_meta.fields` publica ocorrências, divergências e
// coerções por coluna, e a leitura que o operador faz com eles é uma RAZÃO
// ("2.315 de 2.383"). Um numerador inflado pelo reprocesso apontaria para a
// coluna errada exatamente na decisão que esses números existem para tomar —
// quais chaves ganham tipo declarado. É também o que o golden da Fase 4 compara
// entre `decodeWorkers: 1` e `decodeWorkers: 8`.
//
// O custo é um append por chave num slice reusado, e uma passada no fim: zero
// alocação em regime, travado pelos gates de alocação do pacote.
type count struct {
	column *schema.CompiledField
	// veio é o tipo que a origem mandou, e só a divergência o usa.
	veio types.FieldType
	tipo countTipo
}

// count registra um incremento para a aplicação executar.
func (b *PatchBuffer) count(tipo countTipo, column *schema.CompiledField, veio types.FieldType) {
	b.contagens = append(b.contagens, count{column: column, veio: veio, tipo: tipo})
}

// countSeDivergiu registra a divergência de tipo SÓ quando houve divergência de
// verdade.
//
// O caso que ele exclui é o `null` EXPLÍCITO no documento. Convert devolve
// Value{Type: TypeNull} SEM erro para KindNull (typeconv.go), então a mesma
// condição que pega "o valor não coube no tipo declarado" pegava também "a
// origem mandou null" — e as duas seguiam para o mesmo contador. Não são a
// mesma coisa: null é a origem dizendo que não há valor, e a coluna ficar
// ausente é a resposta CERTA, não uma perda por tipo.
//
// A REMOÇÃO continua sendo emitida nos dois casos, e é o que importa para a
// corretude: sem ela, no modo merge, a coluna seguiria mostrando o valor de um
// evento anterior enquanto o documento da mesma linha diz null. O que muda aqui
// é só a CONTAGEM.
//
// Por que isso importa além da estética: o contador de valor incompatível é o
// número que decide quais chaves ganham tipo declarado. Inflado por null, ele
// aponta para colunas cujo problema não é o tipo — e a decisão sai errada
// justamente onde ela deveria sair de medição.
func (b *PatchBuffer) countSeDivergiu(column *schema.CompiledField, f jsonp.Field) {
	veio := InferType(f)
	if veio == types.TypeNull {
		return
	}
	b.count(countDivergencia, column, veio)
}

// countSeCoergiu registra que o valor chegou noutra forma JSON e ainda assim
// entrou na coluna — número numa coluna de texto, string numérica numa coluna
// int64 ou double.
//
// POR QUE O CONTADOR EXISTE. A coerção resolve uma perda e cria uma cegueira. O
// número numa coluna `string` era recusado, a coluna ficava nula e o
// typeMismatches subia — o operador via, pelo contador, que a origem estava
// inconsistente. Convertendo, o valor é preservado e aquele contador vai a ZERO
// no mesmo cenário: a inconsistência da origem some do painel sem ter sumido do
// dado. Sem este terceiro contador, a mudança trocaria uma perda visível por uma
// inconsistência invisível, e a ressalva da documentação — `"07"` e `"7"`
// continuam sendo duas strings — não teria como ser MEDIDA em dataset nenhum.
//
// ONDE ELE É CHAMADO, e por que só aqui. Convert é usado em três sítios: os dois
// da segunda passada e o campo de TOPO em patch.go. O topo fica de fora porque
// os contadores por coluna são publicados só para as DERIVADAS — no topo o
// `_meta.fields` os emite NULL, e incrementar um número que ninguém lê seria
// custo no caminho quente sem leitor. Os dois sítios daqui não se sobrepõem: a
// descoberta pula toda chave já DECLARADA (ver `declarado`), então nenhuma
// ocorrência é contada duas vezes.
//
// A COMPARAÇÃO É f.Kind CONTRA A FORMA NATIVA DO ALVO, e não InferType contra o
// alvo. A segunda seria mais curta e estaria errada: InferType de uma string
// RFC3339 é TypeString, que difere de TypeTimestamp, e toda coluna de timestamp
// marcaria 100% de coerção. O JSON não tem timestamp — ver kindNativo, que é
// onde essa tabela mora e onde ela precisa ser mantida junto do Convert.
//
// Só é chamado no caminho de SUCESSO. Ocorrência que falhou é divergência e já
// tem contador; ocorrência que veio `null` não é nem uma coisa nem outra.
func (b *PatchBuffer) countSeCoergiu(column *schema.CompiledField, f jsonp.Field, target types.FieldType) {
	nativo, hasNativo := kindNativo(target)
	if !hasNativo || f.Kind == nativo {
		return
	}
	b.count(countCoercao, column, types.TypeNull)
}

// aplicaContagens executa os incrementos adiados e esvazia a lista.
//
// Roda na aplicação, uma vez por evento, INCLUSIVE quando o evento é ruim: o
// contador por coluna sempre contou o que o Parse alcançou antes de falhar, e
// mudar isso mexeria no denominador de `_meta.fields` sem que ninguém tivesse
// pedido.
//
// met pode ser nil: o PatchBuffer é usado direto em testes, onde não há métrica
// de dataset para alimentar.
func (b *PatchBuffer) aplicaContagens(met *Metrics) {
	for i := range b.contagens {
		k := &b.contagens[i]
		switch k.tipo {
		case countOcorrencia:
			k.column.CountOccurrence()
		case countCoercao:
			k.column.CountCoercion()
		case countDivergencia:
			b.compilado.CountIncompatibleValue(k.column, k.veio)
		case countCongelada:
			if met != nil {
				met.congeladasCapturadas.Add(1)
			}
		}
	}
	b.contagens = b.contagens[:0]
}
