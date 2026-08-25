package materializer

import (
	"strings"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
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
// REGRA DURA: esta passada NUNCA devolve erro. O pai pode legitimamente ser um
// array ou um null — o conversor de json aceita os dois —, o documento pode
// trazer o caminho com outro tipo, e nada disso é culpa do evento como um
// todo. Propagar qualquer uma dessas falhas faria o evento inteiro cair como
// INVALID_JSON, e a política log-and-skip o descartaria: as dezenas de colunas
// de TOPO iriam junto por causa de uma chave interna. A coluna derivada fica
// AUSENTE, que é a resposta correta e a mesma que o caminho não encontrado já
// tem.
func (b *PatchBuffer) extraiDerivadas() {
	if len(b.pais) == 0 {
		return
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
			// Array, null, escalar: não há objeto a percorrer. Todas as
			// derivadas ficam ausentes.
			continue
		}
		if p.campo.DescobreAninhados {
			b.descobreAninhados(p)
		}
		for _, d := range p.campo.Derivadas {
			f, ok := b.buscaCaminho(p.valor, d.Path, 0)
			if !ok {
				continue
			}
			val, arena, err := Convert(f, d.EffectiveType(), d.Layout, b.arena)
			if err != nil || val.Type == types.TypeNull {
				// Tipo incompatível com o declarado: a coluna fica ausente, o
				// evento entra, e a divergência é CONTADA — a coluna passa a
				// devolver menos linhas do que existem, e sem contador
				// ninguém descobre.
				//
				// O arena pode ter crescido mesmo no erro, então ele é
				// reatribuído aqui como em qualquer outro caminho.
				b.arena = arena
				b.compilado.ContaValorIncompativel(d, InferType(f))
				continue
			}
			b.arena = arena
			b.entries = append(b.entries, patchEntry{field: d, val: val})
		}
	}
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
// Como o resto da segunda passada, NUNCA propaga erro: chave que não vira
// coluna some da projeção e continua no documento.
func (b *PatchBuffer) descobreAninhados(p paiVisto) {
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
			return
		}
		if b.declarado(p.campo, f.Key) {
			continue // declaração vence: o nome e o tipo já foram escolhidos
		}
		// "pai.chave" num buffer reusado — um nome novo por chave por evento
		// seria alocação no hot path.
		b.nomeBuf = append(append(append(b.nomeBuf[:0], p.campo.Name...), '.'), f.Key...)
		cf, err := b.compilado.ResolveAninhado(p.campo, b.nomeBuf, f.Key)
		if err != nil || cf == nil {
			// Teto alcançado, nome tomado, ou política de descarte: a coluna
			// não existe, e o documento continua guardando o valor.
			continue
		}
		alvo := cf.EffectiveType()
		if alvo == types.TypeNull {
			t := InferType(f)
			if t == types.TypeNull {
				continue // null JSON não fixa tipo
			}
			if err := b.compilado.FixAutoType(cf, t); err != nil {
				continue
			}
			alvo = t
		}
		val, arena, err := Convert(f, alvo, cf.Layout, b.arena)
		b.arena = arena
		if err != nil || val.Type == types.TypeNull {
			// Tipo divergente do fixado: a coluna fica ausente NESTA linha, e
			// o documento preserva o valor. Derrubar o evento por causa de uma
			// chave interna é o que esta passada existe para não fazer — mas
			// deixar a divergência INVISÍVEL não faz parte do acordo: aqui o
			// tipo foi decidido por qual evento chegou primeiro, não por quem
			// escreveu o schema, então divergir é esperado e precisa ser
			// contável.
			b.compilado.ContaValorIncompativel(cf, InferType(f))
			continue
		}
		b.entries = append(b.entries, patchEntry{field: cf, val: val})
	}
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
