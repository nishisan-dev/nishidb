package schema

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/units"
)

// DefaultHistorySuffix é o sufixo do nome da tabela irmã. O default é o nome
// por extenso, e não uma abreviação: quem lê `events_history` num FROM sabe o
// que está consultando sem abrir o YAML.
const DefaultHistorySuffix = "_history"

// ErrInvalidIndexWindow marca as recusas do bloco `history.index`.
//
// Tem sentinela PRÓPRIA pela mesma razão de ErrObsoleteIndexSpelling: a subida
// precisa sair com ExitDataset (3), e quem decide isso é o errors.Is do
// errorCode do cli. Sem ela, uma janela recusada cairia no default (ExitUso,
// 1) — o único código que manda o operador olhar a linha de comando em vez do
// YAML do dataset.
var ErrInvalidIndexWindow = errors.New("history.index inválido")

// windowErrf é o specErrf da janela: mesma forma, sentinela diferente.
func windowErrf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidIndexWindow, fmt.Sprintf(format, args...))
}

// RawFieldName guarda os bytes originais do evento, quando `rawPayload: true`.
//
// Ele NÃO é campo de sistema. Os de sistema (`_writtenAt`, `_expiresIn`) são
// escondidos do `SELECT *` porque são mecanismo; o `_raw` é o conteúdo que a
// feature existe para entregar, e esconder o que se pediu para guardar seria
// entregar pela metade.
const RawFieldName = "_raw"

// minSegmentsForCeiling é quantos segmentos o maxBytes precisa comportar.
//
// O número sai da D6: o segmento CORRENTE nunca é candidato à purga. Com um
// teto de dois segmentos, o histórico oscilaria entre "acima do teto" e "não
// há o que aposentar" a cada rotação. Quatro dá folga para o motor trabalhar
// sem que o teto seja atingido pelo próprio arquivo em que se está escrevendo.
const minSegmentsForCeiling = 4

// segmentCeiling espelha segment.MaxSegmentSize, que este pacote não pode
// importar sem inverter a dependência (o storage é quem lê o schema).
//
// O número é consequência do formato, não uma escolha: o offset dentro da
// location empacotada tem 30 bits. Duplicá-lo aqui é aceitável porque ele não
// pode mudar sem mudar o formato em disco — e o dia em que mudar, o
// TestSegmentSizeAcimaDoTetoEhRecusado fica vermelho junto do resto.
const segmentCeiling = 1 << 30

// HistorySpec é o bloco `history` validado.
//
// Ele descreve uma SEGUNDA tabela do mesmo dataset: as mesmas colunas tipadas,
// uma linha por evento, sem deduplicação e sem merge. É ferramenta de debug, e
// por isso tem retenção própria — a do dataset base governa entidades, e aqui
// não há entidade, só fluxo.
type HistorySpec struct {
	// Suffix compõe o nome da tabela irmã: <name><Suffix>.
	Suffix string
	// ExpireAfterWrite é o prazo de uma linha contado da escrita. Zero
	// significa SEM política de tempo — distinto do `never` das regras de TTL,
	// que é uma decisão de imortalidade tomada por uma regra que casou.
	ExpireAfterWrite time.Duration
	// MaxBytes é o teto dos SEGMENTOS DE DADOS, que é o que a purga sabe
	// devolver. Índices e locations.idx ficam de fora e são publicados em
	// separado — um teto que a purga não consegue perseguir seria um teto que
	// mente. Zero significa sem política de tamanho.
	MaxBytes int64
	// SegmentSize é a granularidade da purga por tamanho. Zero herda o
	// storage.segmentSize global, e nesse caso a conferência contra MaxBytes
	// só é possível na abertura — ver ValidateAgainstSegment.
	SegmentSize int64
	// RawPayload acrescenta a coluna _raw com os bytes originais do evento.
	RawPayload bool
	// Indices são os overrides de índice do histórico, por nome de coluna.
	// Eles NÃO são herdados do base de propósito: o bitmap custa
	// (distintos × rows / 8), e o histórico tem muito mais rows.
	Indices map[string]string
	// Window é o bloco `history.index`. NIL é a janela INFINITA — indexa tudo
	// que a retenção mantém —, que é o default e o comportamento de sempre.
	//
	// Ela vive no bloco `history` e só nele porque é política EXCLUSIVA da
	// tabela histórica (D1): no dataset materializado a linha vive no segmento
	// corrente independente da idade dela, e uma janela ali não teria contra o
	// que cortar.
	Window *IndexWindowSpec
}

// IndexWindowSpec é o bloco `history.index` validado: quanto do histórico
// vale indexar.
//
// A simetria com a retenção é deliberada. `history.expireAfterWrite` +
// `history.maxBytes` limitam o DADO; `history.index.window` +
// `history.index.maxBytes` limitam o ÍNDICE — e nos dois casos o teto de bytes
// é a política forte, que engole o prazo declarado. Quem já entendeu uma
// entende a outra sem documentação nova.
//
// Fora da janela a consulta continua respondendo: o segmento sem índice é
// varrido por ENUMERAÇÃO, que é correta e mais lenta. A janela decide custo, e
// nunca resultado.
type IndexWindowSpec struct {
	// Duration é a intenção em tempo. Zero significa SEM fronteira por tempo, e
	// nunca "janela de duração zero": `window: 0` é recusado no load
	// justamente porque as duas leituras são opostas.
	Duration time.Duration
	// MaxBytes é o teto do ÍNDICE — dos .sidx —, e não do dado indexado. Zero
	// significa sem teto, e `maxBytes: 0` é recusado pela mesma razão que o
	// `window: 0`.
	MaxBytes int64
}

// WindowExceedsRetention reporta a redundância que a subida ANUNCIA sem
// recusar: uma janela maior que o prazo de retenção não indexa nada a mais,
// porque a retenção aposenta o segmento antes de a janela o alcançar.
//
// Não é erro, é inócuo — e recusar transformaria redundância em queda de
// serviço. O WARN nomeado é o meio-termo que a doutrina pede: a configuração
// nunca é ignorada em silêncio, mas só é recusada quando estar errada tem
// consequência.
//
// Sem `expireAfterWrite` declarado não há prazo contra o qual comparar: o
// `maxBytes` da retenção limita disco, não idade, e avisar ali seria ruído
// sobre uma configuração que não tem nada de redundante.
func (h *HistorySpec) WindowExceedsRetention() bool {
	if h == nil || h.Window == nil {
		return false
	}
	return h.Window.Duration > 0 && h.ExpireAfterWrite > 0 && h.Window.Duration > h.ExpireAfterWrite
}

// TableName é o nome do dataset irmão.
func (h *HistorySpec) TableName(base string) string { return base + h.Suffix }

// ValidateAgainstSegment confere o teto contra o tamanho de segmento EFETIVO.
//
// Existe porque o `segmentSize` do bloco é opcional: quem o omite herda o
// global do nishidb.yaml, que a spec não conhece. A conferência então acontece
// duas vezes — no load quando o valor é declarado, e na abertura sempre.
func (h *HistorySpec) ValidateAgainstSegment(effective int64) error {
	if h == nil || h.MaxBytes == 0 {
		return nil
	}
	if minBytes := effective * minSegmentsForCeiling; h.MaxBytes < minBytes {
		return specErrf(
			"history.maxBytes (%d) precisa comportar ao menos %d segmentos de %d bytes (%d): "+
				"o segmento corrente nunca é purgado, e um teto menor que isso nunca seria alcançado; "+
				"suba o maxBytes ou baixe o segmentSize",
			h.MaxBytes, minSegmentsForCeiling, effective, minBytes)
	}
	return nil
}

// RequiresWriteInstant reporta se este dataset precisa gravar _writtenAt em
// toda escrita.
//
// São duas razões independentes e o campo é o mesmo: as regras de TTL do base,
// e a retenção por tempo do histórico. Sem esta pergunta num lugar só, ligar
// `history.expireAfterWrite` num dataset sem TTL produziria um histórico cujo
// motor de retenção não tem contra o que comparar — e ele não expiraria nada,
// sem erro e sem log.
func (s *DatasetSpec) RequiresWriteInstant() bool {
	if s == nil {
		return false
	}
	for _, c := range s.Counters {
		if len(c.Measures) > 0 {
			return true
		}
	}
	if len(s.Materialization.TTLRules) > 0 {
		return true
	}
	if s.IsHistory {
		return true
	}
	return s.History != nil && s.History.ExpireAfterWrite > 0
}

// validateHistory valida o bloco `history`. Bloco ausente devolve nil, que é
// o dataset sem tabela irmã.
//
// Roda DEPOIS de schema.fields estar resolvido porque toda a validação de
// `history.fields` é cruzada: a coluna precisa existir, o tipo dela decide a
// indexabilidade, e a congelada precisa ser reconhecida como tal.
func validateHistory(n *yaml.Node, baseName string, fields map[string]FieldSpec) (*HistorySpec, error) {
	if absent(n) {
		return nil, nil
	}
	m, err := block(n, "history",
		"suffix", "expireAfterWrite", "maxBytes", "segmentSize", "rawPayload", "fields", "index")
	if err != nil {
		return nil, err
	}
	h := &HistorySpec{Suffix: DefaultHistorySuffix}

	if sn, ok := m["suffix"]; ok {
		if err := decodeInto(sn, "history.suffix", &h.Suffix); err != nil {
			return nil, err
		}
		h.Suffix = strings.TrimSpace(h.Suffix)
	}
	if h.Suffix == "" {
		return nil, specErrf("history.suffix vazio: a tabela irmã teria o mesmo nome do dataset base")
	}
	siblingName := h.TableName(baseName)
	if !isIdentName(siblingName) {
		return nil, specErrf("history.suffix %q produz o nome de tabela %q, inválido (esperado [a-z0-9_]+)",
			h.Suffix, siblingName)
	}
	// ReservedAlarmsTable é a mesma colisão de nome de dataset/contador, pelo
	// caminho do sufixo: a irmã HERDA o schema do base, e uma tabela irmã
	// chamada `alarms` disputaria a mesma entrada no catálogo com a view de
	// alarmes publicada sob CountersNamespace.
	if siblingName == ReservedAlarmsTable {
		return nil, specErrf(
			"history.suffix %q produces table name %q, which is reserved for the alarms view published under %s.%s",
			h.Suffix, siblingName, CountersNamespace, ReservedAlarmsTable)
	}

	if err := historyExpiry(m, h); err != nil {
		return nil, err
	}
	if tn, ok := m["maxBytes"]; ok {
		var raw string
		if err := decodeInto(tn, "history.maxBytes", &raw); err != nil {
			return nil, err
		}
		if h.MaxBytes, err = units.Size(raw, 0); err != nil {
			return nil, specErrf("history.maxBytes: %s", withoutSpecPrefix(err))
		}
	}
	if sn, ok := m["segmentSize"]; ok {
		var raw string
		if err := decodeInto(sn, "history.segmentSize", &raw); err != nil {
			return nil, err
		}
		if h.SegmentSize, err = units.Size(raw, 0); err != nil {
			return nil, specErrf("history.segmentSize: %s", withoutSpecPrefix(err))
		}
		if h.SegmentSize > segmentCeiling {
			return nil, specErrf("history.segmentSize %d excede o máximo de 1 GiB "+
				"(o offset da location tem 30 bits)", h.SegmentSize)
		}
	}
	// A conferência cruzada só vale com o segmentSize DECLARADO. Quem o omite
	// herda o global, que só é conhecido na abertura — e lá o
	// ValidateAgainstSegment roda com o valor efetivo.
	if h.SegmentSize > 0 {
		if err := h.ValidateAgainstSegment(h.SegmentSize); err != nil {
			return nil, err
		}
	}

	if h.ExpireAfterWrite == 0 && h.MaxBytes == 0 {
		return nil, specErrf("history: declare expireAfterWrite, maxBytes ou os dois — " +
			"um histórico sem retenção é disco que cresce para sempre")
	}

	if rn, ok := m["rawPayload"]; ok {
		if err := decodeInto(rn, "history.rawPayload", &h.RawPayload); err != nil {
			return nil, err
		}
	}
	if h.RawPayload {
		if _, taken := fields[RawFieldName]; taken {
			return nil, specErrf("history.rawPayload exige a coluna %q, e o schema já declara um campo com esse nome",
				RawFieldName)
		}
	}

	if h.Indices, err = historyIndices(m["fields"], fields); err != nil {
		return nil, err
	}
	if h.Window, err = validateWindow(m["index"]); err != nil {
		return nil, err
	}
	return h, nil
}

// validateWindow lê `history.index`. Bloco ausente devolve nil, que é a janela
// INFINITA: indexa tudo que a retenção mantém, o default e o comportamento de
// sempre.
//
// As recusas aqui saem com ErrInvalidIndexWindow, e não com o specErrf do
// resto do arquivo, por uma razão de CONTRATO e não de estilo: recusa de
// configuração de dataset tem de sair com ExitDataset (3), e é o errors.Is do
// errorCode que decide isso. Sem sentinela própria elas cairiam no default
// (ExitUso, 1) e mandariam o operador conferir a linha de comando quando o
// problema está no YAML.
func validateWindow(n *yaml.Node) (*IndexWindowSpec, error) {
	if absent(n) {
		return nil, nil
	}
	m, err := block(n, "history.index", "window", "maxBytes")
	if err != nil {
		return nil, err
	}
	j := &IndexWindowSpec{}
	if wn, ok := m["window"]; ok {
		var raw string
		if err := decodeInto(wn, "history.index.window", &raw); err != nil {
			return nil, err
		}
		// O `units` já recusa zero e negativo, e é ele que fecha o caso
		// `window: 0` da tabela de admissão: zero é AMBÍGUO entre "não indexe
		// nada" e "indexe tudo", e as duas leituras têm consequências opostas —
		// uma tira o índice do dataset inteiro, a outra é o default. Não há
		// grafia que escolha por si, e escolher no código faria metade dos
		// operadores receber o oposto do que quis.
		//
		// A saída fica no fim da mensagem, e ela é a mesma para QUALQUER valor
		// recusado — zero, negativo ou typo: o operador que não quer janela por
		// tempo omite o bloco. Dizê-la sempre custa uma oração e poupa uma ida
		// à documentação; dizê-la só no caso do zero exigiria distinguir os
		// modos de falha do units pela mensagem dele, que é acoplamento pior
		// que a oração a mais.
		d, err := units.DurationWithDays(raw, 0)
		if err != nil {
			return nil, windowErrf("history.index.window: %s — a janela não tem grafia para \"nada\": "+
				"omita o bloco index: para indexar tudo que a retenção mantém", withoutSpecPrefix(err))
		}
		j.Duration = d
	}
	if tn, ok := m["maxBytes"]; ok {
		var raw string
		if err := decodeInto(tn, "history.index.maxBytes", &raw); err != nil {
			return nil, err
		}
		// Mesma ambiguidade do window, e a mesma saída: um teto de zero bytes
		// tanto pode significar "nenhum índice" quanto "sem teto".
		b, err := units.Size(raw, 0)
		if err != nil {
			return nil, windowErrf("history.index.maxBytes: %s — omita a chave para não limitar o índice por bytes",
				withoutSpecPrefix(err))
		}
		j.MaxBytes = b
	}
	// Bloco declarado e vazio é a mesma recusa que `history:` sem política
	// nenhuma: o operador escreveu uma intenção que o motor não tem como
	// executar, e aceitá-la seria a configuração decorativa.
	if j.Duration == 0 && j.MaxBytes == 0 {
		return nil, windowErrf("history.index: declare window, maxBytes ou os dois — " +
			"um bloco vazio não diz o que indexar; omita-o para indexar tudo que a retenção mantém")
	}
	return j, nil
}

// refuseWindowOutsideHistory recusa uma chave `index:` no TOPO do documento.
//
// A janela é `history.index` e só isso: ela é política da tabela histórica, e o
// dataset materializado tem janela infinita por construção (D1). Declarada no
// topo, a chave seria IGNORADA em silêncio — o rawSpec não usa
// yaml.KnownFields —, e o operador ficaria com o comportamento default achando
// que declarou uma janela.
func refuseWindowOutsideHistory(n *yaml.Node) error {
	if absent(n) {
		return nil
	}
	return windowErrf("index: não é chave de topo — a janela de indexação é history.index e é " +
		"exclusiva da tabela histórica, porque no dataset materializado a linha vive no segmento " +
		"corrente independente da idade dela")
}

// historyExpiry lê expireAfterWrite. O `never` das regras de TTL não vale
// aqui: lá ele é a decisão de uma regra que casou; aqui, não querer expiração
// por tempo se diz OMITINDO a chave — e o maxBytes continua de pé.
func historyExpiry(m map[string]*yaml.Node, h *HistorySpec) error {
	pn, ok := m["expireAfterWrite"]
	if !ok {
		return nil
	}
	var raw string
	if err := decodeInto(pn, "history.expireAfterWrite", &raw); err != nil {
		return err
	}
	if raw == DurationNever {
		return specErrf("history.expireAfterWrite %q não existe no histórico: "+
			"omita a chave para não expirar por tempo", DurationNever)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return specErrf("history.expireAfterWrite: %s", withoutSpecPrefix(err))
	}
	if d < ExpiryResolution {
		return specErrf("history.expireAfterWrite %s é menor que a resolução do coletor (%s)",
			raw, ExpiryResolution)
	}
	h.ExpireAfterWrite = d
	return nil
}

// historyIndices valida `history.fields` e devolve o mapa coluna→motor.
//
// A iteração é em ordem alfabética das chaves do documento: o mapa do yaml.v3
// não tem ordem, e sem ordenar a mensagem de um YAML com dois defeitos mudaria
// entre execuções.
func historyIndices(n *yaml.Node, fields map[string]FieldSpec) (map[string]string, error) {
	if absent(n) {
		return nil, nil
	}
	columns, err := block(n, "history.fields", columnNames(fields)...)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make(map[string]string, len(names))
	for _, name := range names {
		keyPath := "history.fields." + name
		keys, err := block(columns[name], keyPath, "index")
		if err != nil {
			return nil, err
		}
		idx, err := text(keys, keyPath, "index")
		if err != nil {
			return nil, err
		}
		if idx == "" {
			return nil, specErrf("%s: index é a única chave declarável aqui, e é obrigatória — "+
				"o histórico espelha o schema base, não o redefine", keyPath)
		}
		typ, err := historyIndexableType(name, fields)
		if err != nil {
			return nil, err
		}
		if err := validateIndex(keyPath, idx, typ); err != nil {
			return nil, err
		}
		out[name] = idx
	}
	return out, nil
}

// columnNames é a whitelist que o block() usa para recusar coluna
// inexistente. Passar a lista das que EXISTEM faz a mensagem de erro nomear as
// alternativas, em vez de só dizer que a chave é desconhecida.
func columnNames(fields map[string]FieldSpec) []string {
	out := make([]string, 0, len(fields))
	for name := range fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// historyIndexableType devolve o tipo contra o qual a indexabilidade se
// decide, recusando as duas famílias que não têm um.
func historyIndexableType(name string, fields map[string]FieldSpec) (string, error) {
	f := fields[name]
	if f.FrozenFrom != "" {
		return "", specErrf("history.fields.%s: %q é uma coluna congelada, e congelada é produto do merge — "+
			"o histórico não tem merge, então a coluna não existe lá", name, name)
	}
	if f.Type == "" || f.Type == "auto" {
		return "", specErrf("history.fields.%s: o campo é auto e o tipo só é conhecido em runtime; "+
			"declare o tipo em schema.fields para poder indexá-lo", name)
	}
	return f.Type, nil
}

// DeriveHistory monta a spec do dataset irmão a partir da spec base.
//
// Não devolve erro porque não decide nada: tudo o que poderia ser recusado já
// foi, no validate. O que ela faz é ESPELHAR — e as quatro diferenças em
// relação ao base são as quatro coisas que o histórico não tem.
func (s *DatasetSpec) DeriveHistory() *DatasetSpec {
	if s == nil || s.History == nil {
		return nil
	}
	// A spec derivada carrega o bloco (ver abaixo), então derivar de novo a
	// partir dela produziria uma irmã da irmã. O par tem exatamente dois
	// lados, e é o IsHistory que fecha a recursão.
	if s.IsHistory {
		return nil
	}
	h := &DatasetSpec{
		Version: 1,
		Name:    s.History.TableName(s.Name),
		// O schema é HERDADO, e não declarado: a histórica é outra tabela do
		// MESMO dataset, como os contadores e as de meta, e o desenho é que
		// tudo que um dataset publica vive num schema só. Ela também não vem
		// de arquivo nenhum, então não haveria onde declarar.
		SchemaName: s.SchemaName,
		IsHistory:  true,
		Query:      s.Query,
		// O bloco viaja para a spec derivada porque é a IRMÃ quem roda a
		// retenção: o motor vive no Store dela, e quem sobe a goroutine
		// percorre a lista de datasets perguntando a cada um pela própria
		// configuração. Sem isto ela subiria sem política nenhuma, e o disco
		// cresceria para sempre com o YAML declarando um teto — a configuração
		// aceita e sem efeito que este bloco existe para não ser.
		History: s.History,
	}
	// Sem `source`: o histórico não tem consumer próprio. Quem escreve nele é
	// o materializer do base, no mesmo evento.
	//
	// Sem key, sem sequence e sem regras de TTL: os três descrevem estado
	// corrente. A ausência da key é o que faz o Compiled sair com Key nil, e é
	// isso que impede o planner de escolher o point lookup sobre um Store que
	// não tem primary (planner.go:94).
	h.Materialization.Mode = "replace"

	h.Schema = SchemaSpec{
		UnknownFields: s.Schema.UnknownFields,
		MaxCols:       s.Schema.MaxCols,
		OnMaxCols:     s.Schema.OnMaxCols,
		MaxNestedCols: s.Schema.MaxNestedCols,
		Fields:        make(map[string]FieldSpec, len(s.Schema.Fields)+1),
	}
	for name, f := range s.Schema.Fields {
		if f.FrozenFrom != "" {
			continue
		}
		// Merge é política de escrita e o histórico não escreve por cima de
		// nada: a coluna `merge: sum` guarda a PARCELA do evento, que é o que
		// a origem mandou — e é o que se quer ver ao depurar um total.
		f.Merge = ""
		// O índice NÃO é herdado; ele vem só de history.fields.
		f.Index = s.History.Indices[name]
		f.Extract = cloneExtracts(f.Extract)
		h.Schema.Fields[name] = f
	}
	if s.History.RawPayload {
		h.Schema.Fields[RawFieldName] = FieldSpec{Type: "json"}
	}
	h.Schema.Include = historyInclude(s, h.Schema.Fields)
	// As congeladas viajam no `exclude` da spec derivada, e não só na ausência
	// delas em Fields: com o dicionário compartilhado, a reidratação do
	// Compile as adotaria de volta como campo auto sem tipo.
	h.Schema.Exclude = frozenNames(s.Schema.Fields)
	return h
}

// frozenNames devolve, em ordem, as colunas com frozenFrom.
func frozenNames(fields map[string]FieldSpec) []string {
	var out []string
	for name, f := range fields {
		if f.FrozenFrom != "" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// namesOutsideUniverse são as colunas que este dataset NÃO pode adotar do
// dicionário compartilhado, além do que o `exclude` do YAML já diz.
//
// São as colunas que pertencem ao OUTRO lado do par: o `_raw` é da tabela
// irmã, e o `_expiresIn` é do dataset base. Uma coluna adotada assim não teria
// valor nenhum gravado deste lado — ela existiria só para responder nulo, e
// para fazer o SELECT * das duas tabelas divergir do que a documentação
// promete.
func namesOutsideUniverse(s *DatasetSpec) []string {
	if s == nil {
		return nil
	}
	if s.IsHistory {
		// O prazo por linha é do dataset base: aqui a retenção é do dataset
		// inteiro, e é uma constante.
		return []string{TTLFieldName}
	}
	if s.History != nil && s.History.RawPayload {
		return []string{RawFieldName}
	}
	return nil
}

// cloneExtracts copia as extrações do campo pai. A cópia é necessária
// porque o slice é compartilhado com a spec do base, e o Compile do histórico
// não pode enxergar — nem escrever — no que o base compilou.
func cloneExtracts(orig []ExtractSpec) []ExtractSpec {
	if len(orig) == 0 {
		return nil
	}
	out := make([]ExtractSpec, len(orig))
	copy(out, orig)
	return out
}

// historyInclude devolve o conjunto visível do histórico.
//
// O `include` do base é reescrito em vez de copiado por uma razão só: ele pode
// nomear uma congelada, que aqui não existe, e um include que nomeia campo
// inexistente é recusado no Compile. Ordenar torna o FieldID determinístico
// pelo mesmo caminho que o base usa.
func historyInclude(s *DatasetSpec, fields map[string]FieldSpec) []string {
	if len(s.Schema.Include) == 1 && s.Schema.Include[0] == "*" {
		return []string{"*"}
	}
	out := make([]string, 0, len(fields))
	for name := range fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// DescribeHistory rende o bloco num texto de UMA linha, para o log de
// subida — a mesma disciplina de DescribeRules: o operador precisa poder
// conferir que o YAML que escreveu é o que o processo entendeu.
func DescribeHistory(name string, h *HistorySpec) string {
	if h == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(name)
	if h.ExpireAfterWrite > 0 {
		fmt.Fprintf(&sb, " expireAfterWrite=%s", h.ExpireAfterWrite)
	}
	if h.MaxBytes > 0 {
		fmt.Fprintf(&sb, " maxBytes=%d", h.MaxBytes)
	}
	if h.RawPayload {
		sb.WriteString(" raw")
	}
	// A janela entra na mesma linha, e a razão é a desta função inteira: o
	// operador precisa poder conferir que o YAML que escreveu é o que o
	// processo entendeu. Sem ela, `history.index` seria a única política do
	// bloco invisível na subida.
	if h.Window != nil {
		if h.Window.Duration > 0 {
			fmt.Fprintf(&sb, " indexWindow=%s", h.Window.Duration)
		}
		if h.Window.MaxBytes > 0 {
			fmt.Fprintf(&sb, " indexMaxBytes=%d", h.Window.MaxBytes)
		}
	}
	if len(h.Indices) > 0 {
		names := make([]string, 0, len(h.Indices))
		for n := range h.Indices {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(&sb, " %s:%s", n, h.Indices[n])
		}
	}
	return sb.String()
}

// AddMirror registra m para receber, em runtime, toda coluna que c descobrir.
//
// É chamado UMA vez por par, na subida, antes de a ingestão começar. O
// registro é aditivo e idempotente por identidade: espelhar duas vezes o mesmo
// Compiled não duplica a propagação.
//
// A rota que ele cobre é estreita e específica: o dataset de universo ABERTO
// (`include: ["*"]` com `unknownFields: include`) promove chaves a coluna
// conforme elas aparecem. Depois de um restart isso não precisaria de nada — a
// reidratação pelo dicionário compartilhado traz a coluna para os dois lados —,
// mas DENTRO da sessão em que a chave apareceu só o base a conhece.
func (c *Compiled) AddMirror(m *Compiled) {
	if c == nil || m == nil || m == c {
		return
	}
	current := c.mirrorList()
	if slices.Contains(current, m) {
		return
	}
	updated := make([]*Compiled, len(current), len(current)+1)
	copy(updated, current)
	updated = append(updated, m)
	c.mirrors.Store(&updated)
}

// mirrorList devolve o slice imutável corrente, ou nil.
func (c *Compiled) mirrorList() []*Compiled {
	if c == nil {
		return nil
	}
	if p := c.mirrors.Load(); p != nil {
		return *p
	}
	return nil
}

// propagateToMirrors replica a coluna recém-descoberta. Chamado pelo writer
// com c.mu já travado na escrita — por isso ele não pode tocar c.mu, e por
// isso a lista vive num ponteiro atômico.
//
// A ordem de travamento é sempre base→espelho, e como o espelho não repassa a
// propagação, não há cadeia em que ela pudesse se inverter.
func (c *Compiled) propagateToMirrors(cf *CompiledField) {
	for _, m := range c.mirrorList() {
		m.adoptDiscovered(cf)
	}
}

// adoptDiscovered cria a coluna equivalente à do original. O receptor aqui é
// sempre o ESPELHO, e o parâmetro é o campo do Compiled que a descobriu.
//
// O CompiledField é NOVO, e não o ponteiro do original, porque `Index` mora
// nele e é justamente o que difere entre as duas tabelas: compartilhar o
// ponteiro faria o índice declarado em `history.fields` valer para o dataset
// base também.
func (c *Compiled) adoptDiscovered(orig *CompiledField) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, taken := c.byName[orig.Name]; taken {
		return
	}
	if _, taken := c.byID[orig.ID]; taken {
		return
	}
	cf := &CompiledField{
		ID:     orig.ID,
		Name:   orig.Name,
		IsAuto: true,
		Path:   orig.Path,
	}
	if orig.Parent != nil {
		// O pai é reatado POR ID a este Compiled, e nunca copiado do original:
		// o `pai.*` compara ponteiro de pai, e apontar para o campo do outro
		// Compiled faria a expansão não achar as filhas — e, pior, a coluna
		// derivada apareceria no SELECT * daqui, porque é a ligação com o pai
		// que a mantém fora dele.
		parent, ok := c.byID[orig.Parent.ID]
		if !ok {
			// Pai ausente neste espelho: adotar a filha a deixaria órfã e
			// visível no SELECT *, que é pior do que não a ter.
			return
		}
		cf.Parent = parent
	}
	cf.autoType.Store(orig.autoType.Load())
	c.byName[cf.Name] = cf
	c.byID[cf.ID] = cf
}

// fixMirroredType replica o tipo que o original acabou de fixar. Como em
// adoptDiscovered, o receptor é o ESPELHO.
//
// Não passa por FixAutoType de propósito: aquele PERSISTE no dicionário, e o
// dicionário é o mesmo — gravar de novo seria uma segunda escrita durável para
// o mesmo fato, com a única consequência possível sendo um conflito espúrio se
// as duas chegarem fora de ordem.
func (c *Compiled) fixMirroredType(id types.FieldID, t types.FieldType) {
	c.mu.RLock()
	f, ok := c.byID[id]
	c.mu.RUnlock()
	if ok && f.IsAuto {
		f.autoType.Store(uint32(t))
	}
}
