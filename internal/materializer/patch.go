package materializer

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Categorias de erro de ingestão (spec §101). INVALID_JSON vem de
// jsonp.ErrInvalidJSON, SCHEMA_VALIDATION de schema.ErrUnknownField e
// TYPE_CONVERSION de ErrTypeConversion.
var (
	ErrInvalidKey      = errors.New("INVALID_KEY")
	ErrInvalidSequence = errors.New("INVALID_SEQUENCE")
)

// patchEntry é um campo do evento, resolvido e tipado. val.Bytes aponta
// para o arena do PatchBuffer.
type patchEntry struct {
	field  *schema.CompiledField
	isNull bool
	// congelada faz o merge PRESERVAR o valor anterior em vez de sobrescrever,
	// e acumula o faz SOMAR o novo ao anterior. Com "sobrescreve" (default) e
	// "remove" (isNull), são as quatro políticas de escrita — e todas elas
	// viajam na entry, para que o merge seja o único lugar que as conhece.
	congelada bool
	acumula   bool
	val       types.Value
}

// PatchBuffer converte um evento JSON em entries tipadas ordenadas por
// FieldID, prontas para o merge/replace. Todos os buffers são reutilizados
// entre eventos (owned pela goroutine do writer; não é thread-safe).
type PatchBuffer struct {
	entries []patchEntry
	arena   []byte
	key     []byte // janela do arena
	seq     int64
	hasSeq  bool
	parser  jsonp.Parser

	// Buffers da segunda passada (extração de campos aninhados). Reusados
	// entre eventos como todo o resto: a passada não pode alocar por evento.
	pais        []paiVisto
	paisFeitos  []types.FieldID
	aninhados   []*jsonp.Parser
	folha       []byte
	descobridor *jsonp.Parser
	nomeBuf     []byte
	// filhosBuf serve ao caminho do pai com forma divergente (array, null,
	// escalar), que precisa enumerar TODAS as colunas daquele documento para
	// removê-las. É buffer e não slice novo pela mesma razão dos vizinhos,
	// ainda que este caminho seja raro: um caminho raro que aloca é uma
	// alocação que só aparece em produção.
	filhosBuf []*schema.CompiledField
	// compilado é o schema do Parse corrente. A descoberta precisa dele para
	// registrar coluna nova, e passá-lo por parâmetro em toda a cadeia da
	// segunda passada só o faria viajar de graça.
	compilado *schema.Compiled

	// contagens são os incrementos de contador ADIADOS até a aplicação. Ver o
	// godoc do tipo contagem para o porquê — em uma linha: o evento que o
	// writer reprocessa passa pelo Parse duas vezes, e contar dentro dele
	// dobraria os números que o `_meta.fields` publica.
	contagens []contagem

	// soLeitura proíbe este Parse de CRIAR metadado: coluna nova e tipo fixado
	// ficam de fora, e o Parse para com errPrecisaDoWriter no ponto exato em
	// que precisaria de um dos dois.
	//
	// É o que permite decodificar fora da goroutine do writer. Deixar N
	// decodificadores descobrirem campos não é questão de desempenho, é de
	// CORREÇÃO: LookupOrCreate atribui o FieldID na ordem de chamada, FieldID é
	// formato de disco e a posição da coluna no `SELECT *` é contrato do driver
	// JDBC. Com workers, essa ordem viraria a de quem ganha o mutex — e "o tipo
	// é fixado na primeira observação" viraria "na primeira a chegar no lock",
	// não "no menor offset". A violação já foi vista em produção; ver
	// internal/cli/determinism_test.go.
	soLeitura bool
}

// errPrecisaDoWriter interrompe um Parse somente-leitura que alcançou um
// metadado a criar.
//
// É sentinela INTERNA: nunca chega ao classify e nunca vira categoria do §101.
// Quem a recebe é o Decodifica, que a traduz em `precisaDoWriter` e devolve o
// evento intacto para o writer refazer pelo caminho completo.
var errPrecisaDoWriter = errors.New("materializer: decode somente-leitura alcançou metadado a criar")

// resolveDesconhecido resolve o campo de topo que o FieldByName não conhece,
// honrando o modo somente-leitura.
func (b *PatchBuffer) resolveDesconhecido(c *schema.Compiled, nome []byte) (*schema.CompiledField, error) {
	if !b.soLeitura {
		return c.ResolveUnknown(nome)
	}
	cf, precisa, err := c.ResolveUnknownReadOnly(nome)
	if err != nil {
		return nil, err
	}
	if precisa {
		return nil, errPrecisaDoWriter
	}
	return cf, nil
}

// resolveAninhado resolve a coluna de uma chave descoberta dentro de um
// documento, honrando o modo somente-leitura.
func (b *PatchBuffer) resolveAninhado(pai *schema.CompiledField, nomeQualificado, caminho []byte) (*schema.CompiledField, error) {
	if !b.soLeitura {
		return b.compilado.ResolveNested(pai, nomeQualificado, caminho)
	}
	cf, precisa := b.compilado.ResolveNestedReadOnly(pai, nomeQualificado)
	if precisa {
		return nil, errPrecisaDoWriter
	}
	return cf, nil
}

// fixaTipo fixa o tipo de um campo auto na primeira observação, honrando o modo
// somente-leitura.
//
// A fixação é escrita DURÁVEL no dicionário e publicação em memória, e mais: ela
// se propaga ao Compiled da tabela irmã. Nada disso pode sair da goroutine do
// writer, e é por isso que o modo somente-leitura devolve a pendência em vez de
// tentar.
func (b *PatchBuffer) fixaTipo(c *schema.Compiled, cf *schema.CompiledField, t types.FieldType) error {
	if b.soLeitura {
		return errPrecisaDoWriter
	}
	return c.FixAutoType(cf, t)
}

// Parse processa um evento completo em um único passo: tokeniza, resolve
// campos contra o schema (include/exclude/unknown policy), converte tipos
// para o arena, extrai key e sequence, ordena por FieldID e aplica
// last-wins em chaves JSON duplicadas.
//
// Exceção ao last-wins: key e sequence são capturadas DURANTE a conversão,
// antes do dedup, então a primeira ocorrência inválida aborta o evento mesmo
// havendo uma válida à frente — {"id":null,"id":"A"} é INVALID_KEY, não "A".
// O mesmo vale para erro de conversão de qualquer campo. Adiar o abort para
// depois do dedup exigiria adiar a conversão do evento inteiro: preço alto
// no hot path para socorrer evento malformado.
//
// Após erro, os buffers ficam com estado PARCIAL (entries, arena, key e seq
// do ponto em que parou). O caller deve descartar o evento; nada além do
// erro é observável até o próximo Parse, que reseta tudo.
// agora é o instante da escrita, injetado no campo de sistema quando o
// dataset declara expireAfterWrite. Vem por PARÂMETRO, e não de um time.Now()
// aqui dentro, porque um teste de expiração precisa controlar o relógio sem
// dormir — e porque todo campo de um mesmo evento tem de carimbar o MESMO
// instante, que uma chamada por campo não garantiria.
func (b *PatchBuffer) Parse(event []byte, c *schema.Compiled, agora time.Time) error {
	b.entries = b.entries[:0]
	b.arena = b.arena[:0]
	b.key = nil
	b.seq = 0
	b.hasSeq = false
	b.pais = b.pais[:0]
	b.contagens = b.contagens[:0]
	b.compilado = c

	b.parser.Reset(event)
	for {
		f, ok, err := b.parser.Next()
		if err != nil {
			return err // jsonp.ErrInvalidJSON
		}
		if !ok {
			break
		}
		cf, found := c.FieldByName(f.Key)
		if found && (cf.IsSystem || cf.FrozenFrom != nil || cf.FromClock) {
			// O evento não pode escrever nem o relógio do writer nem uma
			// coluna congelada. No primeiro caso o produtor controlaria quando
			// a entidade expira; no segundo, decidiria qual é o valor
			// "original" — e a coluna passaria a mentir sem erro nenhum.
			//
			// É diferente da coluna derivada, ignorada em silêncio logo
			// abaixo: lá o nome de topo pode existir legitimamente no evento
			// com outro significado, aqui o nome foi inventado pelo operador
			// para esta finalidade.
			return fmt.Errorf("%w: campo %q é reservado ao writer e não pode vir no evento",
				schema.ErrUnknownField, f.Key) // SCHEMA_VALIDATION
		}
		// Coluna DERIVADA não aceita escrita de fora: ela é preenchida pela
		// segunda passada, a partir do documento do pai. Sem esta guarda, um
		// evento que trouxesse a chave no topo controlaria uma coluna que o
		// operador declarou como sendo de dentro do documento — e o valor
		// dependeria da ordem em que as duas apareceram.
		if found && cf.Parent != nil {
			continue
		}
		if !found {
			// Campo excluído não está compilado: cai aqui e o
			// ResolveUnknown o descarta — uma comparação a menos por campo
			// por evento no hot path.
			cf, err = b.resolveDesconhecido(c, f.Key)
			if err != nil {
				// A pendência do decode somente-leitura sobe CRUA: ela não é
				// erro do evento, e embrulhá-la aqui a faria chegar ao
				// Decodifica vestida de SCHEMA_VALIDATION.
				if errors.Is(err, errPrecisaDoWriter) {
					return err
				}
				return fmt.Errorf("campo %q: %w", f.Key, err) // SCHEMA_VALIDATION
			}
			if cf == nil {
				continue // excluído, ignorado ou universo fechado
			}
		}

		// R11: o tipo de um campo sem declaração é decidido na PRIMEIRA
		// observação e persistido; as seguintes convertem contra ele, não
		// contra o shape do evento. Inferir por evento faria o mesmo campo ter
		// significados diferentes conforme a ordem de chegada — e nenhuma
		// query sobre ele seria confiável.
		target := cf.EffectiveType()
		if cf.IsAuto && target == types.TypeNull {
			// null JSON não fixa tipo: gravaria "sem tipo" e travaria o campo.
			if t := InferType(f); t != types.TypeNull {
				if err := b.fixaTipo(c, cf, t); err != nil {
					if errors.Is(err, errPrecisaDoWriter) {
						return err
					}
					// Só o CONFLITO é evento ruim: o campo mudou de formato na
					// origem. Ele precisa carregar ErrTypeConversion, senão o
					// classify cai no default e PARA a ingestão por causa de um
					// evento.
					//
					// Qualquer outro erro daqui é do DATASET — falha de I/O ao
					// gravar o hint (disco cheio, EIO) ou bug de programação.
					// Embrulhá-lo em ErrTypeConversion o faria virar "evento
					// ruim": o pipeline pularia em silêncio, e para sempre, todo
					// evento que tocasse um campo auto ainda sem tipo, com
					// Failed=0 no painel. Bug e infra nunca entram no balde de
					// dado ruim.
					if errors.Is(err, fielddict.ErrTypeHintConflict) {
						return fmt.Errorf("campo %q: %w: %w", f.Key, ErrTypeConversion, err)
					}
					return fmt.Errorf("campo %q: %w", f.Key, err) // STORAGE_ERROR
				}
				target = t
			}
		}
		val, arena, err := Convert(f, target, cf.Layout, b.arena)
		if err != nil {
			return fmt.Errorf("campo %q: %w", f.Key, err) // TYPE_CONVERSION
		}
		b.arena = arena

		isNull := val.Type == types.TypeNull
		switch {
		case cf.IsKey:
			switch {
			case isNull:
				return fmt.Errorf("%w: campo %q nulo", ErrInvalidKey, cf.Name)
			case len(val.Bytes) == 0:
				// Key vazia é rejeitada pelo Store real (ErrInvalidArgument);
				// barrar aqui evita o evento morrer só na escrita.
				return fmt.Errorf("%w: campo %q vazio", ErrInvalidKey, cf.Name)
			}
			b.key = val.Bytes
		case cf.IsSequence:
			if isNull {
				return fmt.Errorf("%w: campo %q nulo", ErrInvalidSequence, cf.Name)
			}
			b.seq = val.I64
			b.hasSeq = true
		}
		// O pai entra na segunda passada se tem extração DECLARADA ou se
		// descobre. Testar só as declaradas fazia a descoberta pura — sem
		// nenhum `extract` no YAML, que é o caso do piloto — nunca rodar.
		if len(cf.DerivedColumns) > 0 || cf.DiscoverNested {
			// Guarda a ocorrência para a segunda passada. Para objeto, o Raw é
			// janela sobre o EVENTO (skipComposite devolve p.data[start:pos]),
			// que é estável durante todo o Parse — ao contrário do arena, que
			// é reapontado a cada conversão. Os outros Kinds nem são
			// percorridos, então o valor deles não é guardado.
			pv := paiVisto{campo: cf, kind: f.Kind}
			if f.Kind == jsonp.KindObject {
				pv.valor = f.Raw
			}
			b.pais = append(b.pais, pv)
		}
		b.entries = append(b.entries, patchEntry{
			field: cf, isNull: isNull, acumula: cf.Accumulates, val: val,
		})
	}

	// A extração roda DEPOIS do laço, porque precisa da última ocorrência de
	// cada pai, e ANTES da ordenação, para que as derivadas participem do
	// sort e do dedup como qualquer outra entry.
	if err := b.extraiDerivadas(); err != nil {
		return err
	}

	// As congeladas entram depois das derivadas e antes da ordenação: podem
	// ter origem numa coluna extraída, e precisam participar do sort e do
	// dedup como qualquer outra entry.
	if len(c.FrozenColumns) > 0 {
		if err := b.capturaCongeladas(c, agora); err != nil {
			return err
		}
	}

	// O campo de sistema entra ANTES da ordenação, como uma entry qualquer:
	// assim o merge e o replace o incluem pelo mesmo caminho, sem que nenhum
	// dos dois precise saber que ele existe. É também o que garante que toda
	// escrita o renove — é disso que o idle timeout depende.
	if c.WriteInstantField != nil {
		b.entries = append(b.entries, patchEntry{
			field: c.WriteInstantField,
			val:   types.Value{Type: types.TypeTimestamp, I64: agora.UnixMilli()},
		})
	}

	// Ordena por FieldID; sort ESTÁVEL preserva a ordem do evento entre
	// duplicatas, permitindo o last-wins no compact abaixo.
	slices.SortStableFunc(b.entries, func(a, e patchEntry) int {
		switch {
		case a.field.ID < e.field.ID:
			return -1
		case a.field.ID > e.field.ID:
			return 1
		}
		return 0
	})
	b.entries = dedupLastWins(b.entries)

	if b.key == nil {
		return fmt.Errorf("%w: campo %q ausente no evento", ErrInvalidKey, c.Key.Name)
	}
	if c.Seq != nil && !b.hasSeq {
		return fmt.Errorf("%w: campo %q ausente no evento", ErrInvalidSequence, c.Seq.Name)
	}
	return nil
}

// dedupLastWins remove duplicatas de FieldID mantendo a ÚLTIMA ocorrência
// (ordem do evento, preservada pelo sort estável). In-place, O(n).
func dedupLastWins(entries []patchEntry) []patchEntry {
	if len(entries) < 2 {
		return entries
	}
	out := entries[:0]
	for i := 0; i < len(entries); i++ {
		if i+1 < len(entries) && entries[i+1].field.ID == entries[i].field.ID {
			continue // há ocorrência mais recente à frente
		}
		out = append(out, entries[i])
	}
	return out
}
