package materializer

import (
	"errors"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// paiVisto guarda a ocorrência de um campo json que declara extrações.
//
// O Value é uma janela sobre o buffer do EVENTO, e não sobre o arena: o
// evento é estável durante todo o Parse, enquanto o arena pode ser
// reapontado a cada conversão.
type paiVisto struct {
	campo *schema.CompiledField
	valor []byte
	kind  jsonp.Kind
}

// extraiDerivadas roda a segunda passada sobre os campos json que declaram
// `extract`, acrescentando uma entry por coluna derivada encontrada.
//
// REGRA DURA: esta passada NUNCA propaga erro de DADO. O pai pode legitimamente
// ser um array ou um null — o conversor de json aceita os dois —, o documento
// pode trazer o caminho com outro tipo, e nada disso é culpa do evento como um
// todo. Propagar qualquer uma dessas falhas faria o evento inteiro cair como
// INVALID_JSON, e a política log-and-skip o descartaria: as dezenas de colunas
// de TOPO iriam junto por causa de uma chave interna. A coluna derivada fica
// AUSENTE ou é REMOVIDA, conforme haja ou não valor anterior a contradizer.
//
// O QUE ELA PROPAGA, e a distinção é a mesma que o caminho de topo já faz em
// patch.go: erro de INFRA (falha de I/O ao gravar o dicionário, disco cheio,
// EIO) e a POLÍTICA DECLARADA de teto de campos. Antes, um `continue` seco
// tratava os três iguais — e o efeito era concreto: disco cheio virava coluna
// ausente sem erro, sem log e sem métrica, e a política `stop` do
// `schema.onMaxCols` era silenciosamente rebaixada a `ignore` neste caminho
// enquanto o de topo a honrava. Bug e infra nunca entram no balde de dado ruim.
func (b *PatchBuffer) extraiDerivadas() error {
	if len(b.pais) == 0 {
		return nil
	}
	// De trás para frente, pulando o que já foi resolvido: vale a ÚLTIMA
	// ocorrência do pai, que é a mesma regra que a coluna json dele segue.
	// Divergir faria a derivada contradizer o documento do mesmo record.
	b.paisFeitos = b.paisFeitos[:0]
	for i := len(b.pais) - 1; i >= 0; i-- {
		p := b.pais[i]
		if b.jaResolvido(p.campo.ID) {
			continue
		}
		b.paisFeitos = append(b.paisFeitos, p.campo.ID)
		if p.kind != jsonp.KindObject {
			b.removeAsColunasDoPai(p)
			continue
		}
		if p.campo.DescobreAninhados {
			if err := b.descobreAninhados(p); err != nil {
				return err
			}
		}
		for _, d := range p.campo.Derivadas {
			f, ok := b.buscaCaminho(p.valor, d.Path, 0)
			if !ok {
				continue
			}
			val, arena, err := Convert(f, d.EffectiveType(), d.Layout, b.arena)
			if err != nil || val.Type == types.TypeNull {
				// REMOÇÃO explícita, e não `continue`: no modo merge, campo
				// ausente do patch PRESERVA o valor anterior, então sair calado
				// deixaria a coluna com o valor de um evento passado enquanto o
				// documento da MESMA linha traz o novo. A query devolveria valor
				// velho com cara de corrente — linha errada, não linha a menos.
				b.entries = append(b.entries, patchEntry{field: d, isNull: true})
				// Tipo incompatível com o declarado: a coluna é removida, o
				// evento entra, e a divergência é CONTADA — sem contador
				// ninguém descobre que a coluna deixou de responder.
				//
				// O arena pode ter crescido mesmo no erro, então ele é
				// reatribuído aqui como em qualquer outro caminho.
				b.arena = arena
				contaSeDivergiu(b.compilado, d, f)
				continue
			}
			b.arena = arena
			b.entries = append(b.entries, patchEntry{field: d, val: val})
		}
	}
	return nil
}

// removeAsColunasDoPai emite remoção explícita para TODA coluna derivada de um
// pai que chegou com forma divergente — array, null ou escalar.
//
// Sair calado daqui era a mesma família do defeito que o valor de tipo
// divergente teve, e ficou aberta quando aquele foi corrigido: no modo merge,
// campo ausente do patch PRESERVA o valor anterior, então o `continue` seco
// deixava a coluna respondendo pelo documento de um evento PASSADO enquanto o
// documento da mesma linha é um array ou um null. Coluna e documento se
// contradiziam dentro do mesmo record, e a query devolvia valor velho com cara
// de corrente — linha errada, não linha a menos.
//
// ENUMERA DECLARADAS E DESCOBERTAS, e essa é a parte que não pode ser
// esquecida: `pai.Derivadas` só tem as declaradas, e um dataset de descoberta
// pura (o do piloto: `unknownFields: include` sem nenhum `extract`) tem essa
// lista VAZIA. Percorrer só as Derivadas aqui seria um conserto que não
// consertaria nada exatamente onde o defeito foi medido.
//
// POR QUE O CUSTO É ACEITÁVEL, e por que ele NÃO abre a porta para o caso
// vizinho. Um documento de ~1.650 colunas produz ~1.650 entries nesta passagem,
// o que seria proibitivo no caminho quente — e é por isso que a chave que SOME
// de um documento que CONTINUA sendo objeto segue o merge, deliberadamente e
// travada por TestChaveQueSomeDoDocumentoSegueOMerge. A diferença é a
// frequência: aquele caso ocorre em TODO evento (11 chaves de 1.650 colunas),
// este só quando a origem manda a forma errada. Medido no piloto em 25/08/2026:
// 3.000 de 3.000 documentos são objeto. Pagar 1.650 entries num evento que
// quase não acontece é barato; pagá-las em todos não seria.
//
// A DIVERGÊNCIA É CONTADA UMA VEZ, no pai, e não uma por coluna removida. É
// afirmação diferente e mais honesta: o que divergiu foi UM documento, e as
// colunas nem estavam nele — não havia objeto onde procurá-las. Contar 1.650
// afogaria o sinal por coluna que o contador existe para dar.
func (b *PatchBuffer) removeAsColunasDoPai(p paiVisto) {
	b.filhosBuf = b.compilado.AnexaFilhos(p.campo, b.filhosBuf[:0])
	if len(b.filhosBuf) == 0 {
		return
	}
	for _, f := range b.filhosBuf {
		b.entries = append(b.entries, patchEntry{field: f, isNull: true})
	}
	b.compilado.ContaValorIncompativel(p.campo, InferType(jsonp.Field{Kind: p.kind}))
}

func (b *PatchBuffer) jaResolvido(id types.FieldID) bool {
	for _, feito := range b.paisFeitos {
		if feito == id {
			return true
		}
	}
	return false
}

// buscaCaminho encontra o campo endereçado por path dentro do documento.
//
// O caminho é percorrido segmento a segmento, sem alocar: strings.IndexByte
// sobre a própria string do path, e um parser por NÍVEL, reusado entre
// eventos. Erro de sintaxe em qualquer nível devolve (_, false), nunca erro —
// ver a regra dura em extraiDerivadas.
func (b *PatchBuffer) buscaCaminho(doc []byte, path string, nivel int) (jsonp.Field, bool) {
	seg, resto := path, ""
	if i := strings.IndexByte(path, '.'); i >= 0 {
		seg, resto = path[:i], path[i+1:]
	}
	for len(b.aninhados) <= nivel {
		b.aninhados = append(b.aninhados, &jsonp.Parser{})
	}
	p := b.aninhados[nivel]
	p.Reset(doc)
	var achado jsonp.Field
	var achou bool
	for {
		f, ok, err := p.Next()
		if err != nil || !ok {
			// Documento malformado num nível interno: a coluna fica ausente.
			break
		}
		if string(f.Key) != seg {
			continue
		}
		// Last-wins também DENTRO do documento, pela mesma razão de fora: é a
		// regra que o resto do produto já segue para chave duplicada.
		//
		// Reter o Field entre iterações exige COPIAR o Raw: o godoc do jsonp
		// avisa que ele vale só até o Next seguinte, porque para string com
		// escape ele aponta para o scratch do parser. Objeto e array são
		// janela sobre o input e não precisariam da cópia, mas distinguir
		// aqui trocaria uma cópia curta por uma regra fácil de violar depois.
		b.folha = append(b.folha[:0], f.Raw...)
		f.Raw = b.folha
		achado, achou = f, true
	}
	if !achou {
		return jsonp.Field{}, false
	}
	if resto == "" {
		return achado, true
	}
	if achado.Kind != jsonp.KindObject {
		return jsonp.Field{}, false
	}
	return b.buscaCaminho(achado.Raw, resto, nivel+1)
}

// descobreAninhados promove a coluna toda chave do documento que não esteja
// DECLARADA.
//
// A motivação do campo json é preservar tudo para estudo e debug depois, e o
// documento já faz isso — ele é gravado verbatim. O que a descoberta
// acrescenta é poder ESCREVER a pergunta: sem coluna, `WHERE doc.algo = 'x'`
// não é sequer expressável, e o operador precisa trazer o documento inteiro e
// filtrar fora.
//
// É de UM nível. Objeto aninhado vira coluna json, inteiro, e quem precisa da
// chave lá dentro declara o caminho. Sem esse corte o vocabulário cresceria
// por profundidade além de crescer por largura, e o dataset não controla nem
// uma nem outra — quem produz os eventos é a integração.
//
// Como o resto da segunda passada, não propaga erro de DADO: chave que não vira
// coluna some da projeção e continua no documento. Infra e política declarada
// sobem — ver o godoc de extraiDerivadas.
func (b *PatchBuffer) descobreAninhados(p paiVisto) error {
	for len(b.aninhados) <= 0 {
		b.aninhados = append(b.aninhados, &jsonp.Parser{})
	}
	// Parser próprio para a varredura de descoberta: o do nível 0 é usado pela
	// busca por caminho das colunas DECLARADAS, que roda logo depois.
	if b.descobridor == nil {
		b.descobridor = &jsonp.Parser{}
	}
	b.descobridor.Reset(p.valor)
	for {
		f, ok, err := b.descobridor.Next()
		if err != nil || !ok {
			// Fim do documento, ou documento malformado num nível interno. O
			// segundo é inalcançável por construção — o Raw do pai veio do
			// skipComposite, que já validou a estrutura —, e por isso a
			// descoberta para em silêncio em vez de propagar: se um dia for
			// alcançável, é bug do parser e não dado ruim, e o lugar de
			// descobrir isso é o fuzz do jsonp, não um evento em produção.
			return nil
		}
		if b.declarado(p.campo, f.Key) {
			continue // declaração vence: o nome e o tipo já foram escolhidos
		}
		// "pai.chave" num buffer reusado — um nome novo por chave por evento
		// seria alocação no hot path.
		b.nomeBuf = append(append(append(b.nomeBuf[:0], p.campo.Name...), '.'), f.Key...)
		cf, err := b.compilado.ResolveAninhado(p.campo, b.nomeBuf, f.Key)
		if err != nil {
			// Teto sob política `stop` (schema.ErrTetoDeCampos, que o classify
			// mapeia para FIELDS_EXHAUSTED e que PARA a ingestão) ou falha do
			// dicionário (infra). Os dois sobem: o primeiro é a política que o
			// operador declarou, o segundo é dataset avariado. Sob `ignore` não
			// se chega aqui — o ResolveAninhado devolve (nil, nil).
			return err
		}
		if cf == nil {
			// Teto sob `ignore`, ou nome tomado por uma coluna de topo com
			// ponto no nome. A coluna não existe e nunca existiu, então não há
			// valor anterior que ela pudesse contradizer — o documento segue
			// guardando o valor. A perda do teto está no camposDescartados.
			continue
		}
		alvo := cf.EffectiveType()
		if alvo == types.TypeNull {
			t := InferType(f)
			if t == types.TypeNull {
				continue // null JSON não fixa tipo
			}
			if err := b.compilado.FixAutoType(cf, t); err != nil {
				if !errors.Is(err, fielddict.ErrTypeHintConflict) {
					// Falha de I/O ao persistir o hint: disco cheio, EIO. É do
					// DATASET, não do evento. O `continue` que estava aqui a
					// transformava em coluna ausente — sem erro, sem log, sem
					// métrica — e no modo merge a coluna ainda congelava o valor
					// anterior. É a mesma doutrina que patch.go aplica ao campo
					// de topo: bug e infra nunca entram no balde de dado ruim.
					return err // STORAGE_ERROR
				}
				// CONFLITO de tipo no dicionário: o campo mudou de formato na
				// origem, e isso é dado. Segue o mesmo desfecho da conversão que
				// não cabe — a coluna é removida e a divergência é contada —, e
				// NÃO derruba o evento, que é a regra desta passada.
				b.entries = append(b.entries, patchEntry{field: cf, isNull: true})
				b.compilado.ContaValorIncompativel(cf, t)
				continue
			}
			alvo = t
		}
		val, arena, err := Convert(f, alvo, cf.Layout, b.arena)
		b.arena = arena
		if err != nil || val.Type == types.TypeNull {
			// A chave ESTÁ no documento, com um valor que a coluna não
			// representa. A coluna é removida — com entry de remoção, não com
			// `continue` —, porque no modo merge o silêncio preserva o valor
			// do evento anterior: a coluna mostraria um valor velho enquanto o
			// documento da mesma linha traz o novo, e a query devolveria linha
			// ERRADA em vez de linha a menos.
			//
			// Derrubar o evento por causa de uma chave interna é o que esta
			// passada existe para não fazer; deixar a divergência invisível
			// também não faz parte do acordo. Aqui o tipo foi decidido por qual
			// evento chegou primeiro, não por quem escreveu o schema, então
			// divergir é esperado e precisa ser contável.
			b.entries = append(b.entries, patchEntry{field: cf, isNull: true})
			contaSeDivergiu(b.compilado, cf, f)
			continue
		}
		b.entries = append(b.entries, patchEntry{field: cf, val: val})
	}
}

// contaSeDivergiu incrementa o contador de valor incompatível SÓ quando houve
// divergência de tipo de verdade.
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
func contaSeDivergiu(c *schema.Compiled, coluna *schema.CompiledField, f jsonp.Field) {
	veio := InferType(f)
	if veio == types.TypeNull {
		return
	}
	c.ContaValorIncompativel(coluna, veio)
}

// declarado informa se o caminho de um nível já é coberto por um extract.
func (b *PatchBuffer) declarado(pai *schema.CompiledField, chave []byte) bool {
	for _, d := range pai.Derivadas {
		if d.Path == string(chave) {
			return true
		}
	}
	return false
}
