package schema

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/units"
)

// SufixoHistoricoPadrao é o sufixo do nome da tabela irmã. O default é o nome
// por extenso, e não uma abreviação: quem lê `events_history` num FROM sabe o
// que está consultando sem abrir o YAML.
const SufixoHistoricoPadrao = "_history"

// CampoRaw guarda os bytes originais do evento, quando `rawPayload: true`.
//
// Ele NÃO é campo de sistema. Os de sistema (`_writtenAt`, `_expiresIn`) são
// escondidos do `SELECT *` porque são mecanismo; o `_raw` é o conteúdo que a
// feature existe para entregar, e esconder o que se pediu para guardar seria
// entregar pela metade.
const CampoRaw = "_raw"

// segmentosMinimosDoTeto é quantos segmentos o maxBytes precisa comportar.
//
// O número sai da D6: o segmento CORRENTE nunca é candidato à purga. Com um
// teto de dois segmentos, o histórico oscilaria entre "acima do teto" e "não
// há o que aposentar" a cada rotação. Quatro dá folga para o motor trabalhar
// sem que o teto seja atingido pelo próprio arquivo em que se está escrevendo.
const segmentosMinimosDoTeto = 4

// tetoDeSegmento espelha segment.MaxSegmentSize, que este pacote não pode
// importar sem inverter a dependência (o storage é quem lê o schema).
//
// O número é consequência do formato, não uma escolha: o offset dentro da
// location empacotada tem 30 bits. Duplicá-lo aqui é aceitável porque ele não
// pode mudar sem mudar o formato em disco — e o dia em que mudar, o
// TestSegmentSizeAcimaDoTetoEhRecusado fica vermelho junto do resto.
const tetoDeSegmento = 1 << 30

// HistoricoSpec é o bloco `history` validado.
//
// Ele descreve uma SEGUNDA tabela do mesmo dataset: as mesmas colunas tipadas,
// uma linha por evento, sem deduplicação e sem merge. É ferramenta de debug, e
// por isso tem retenção própria — a do dataset base governa entidades, e aqui
// não há entidade, só fluxo.
type HistoricoSpec struct {
	// Sufixo compõe o nome da tabela irmã: <name><Sufixo>.
	Sufixo string
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
	// só é possível na abertura — ver ValidaContraSegmento.
	SegmentSize int64
	// RawPayload acrescenta a coluna _raw com os bytes originais do evento.
	RawPayload bool
	// Indices são os overrides de índice do histórico, por nome de coluna.
	// Eles NÃO são herdados do base de propósito: o bitmap custa
	// (distintos × rows / 8), e o histórico tem muito mais rows.
	Indices map[string]string
}

// NomeDaTabela é o nome do dataset irmão.
func (h *HistoricoSpec) NomeDaTabela(base string) string { return base + h.Sufixo }

// ValidaContraSegmento confere o teto contra o tamanho de segmento EFETIVO.
//
// Existe porque o `segmentSize` do bloco é opcional: quem o omite herda o
// global do nishidb.yaml, que a spec não conhece. A conferência então acontece
// duas vezes — no load quando o valor é declarado, e na abertura sempre.
func (h *HistoricoSpec) ValidaContraSegmento(efetivo int64) error {
	if h == nil || h.MaxBytes == 0 {
		return nil
	}
	if minimo := efetivo * segmentosMinimosDoTeto; h.MaxBytes < minimo {
		return specErrf(
			"history.maxBytes (%d) precisa comportar ao menos %d segmentos de %d bytes (%d): "+
				"o segmento corrente nunca é purgado, e um teto menor que isso nunca seria alcançado; "+
				"suba o maxBytes ou baixe o segmentSize",
			h.MaxBytes, segmentosMinimosDoTeto, efetivo, minimo)
	}
	return nil
}

// ExigeInstanteDeEscrita reporta se este dataset precisa gravar _writtenAt em
// toda escrita.
//
// São duas razões independentes e o campo é o mesmo: as regras de TTL do base,
// e a retenção por tempo do histórico. Sem esta pergunta num lugar só, ligar
// `history.expireAfterWrite` num dataset sem TTL produziria um histórico cujo
// motor de retenção não tem contra o que comparar — e ele não expiraria nada,
// sem erro e sem log.
func (s *DatasetSpec) ExigeInstanteDeEscrita() bool {
	if s == nil {
		return false
	}
	if len(s.Materialization.RegrasTTL) > 0 {
		return true
	}
	if s.EhHistorico {
		return true
	}
	return s.Historico != nil && s.Historico.ExpireAfterWrite > 0
}

// validaHistorico valida o bloco `history`. Bloco ausente devolve nil, que é
// o dataset sem tabela irmã.
//
// Roda DEPOIS de schema.fields estar resolvido porque toda a validação de
// `history.fields` é cruzada: a coluna precisa existir, o tipo dela decide a
// indexabilidade, e a congelada precisa ser reconhecida como tal.
func validaHistorico(n *yaml.Node, nomeBase string, fields map[string]FieldSpec) (*HistoricoSpec, error) {
	if ausente(n) {
		return nil, nil
	}
	m, err := bloco(n, "history",
		"suffix", "expireAfterWrite", "maxBytes", "segmentSize", "rawPayload", "fields")
	if err != nil {
		return nil, err
	}
	h := &HistoricoSpec{Sufixo: SufixoHistoricoPadrao}

	if sn, ok := m["suffix"]; ok {
		if err := decodeEm(sn, "history.suffix", &h.Sufixo); err != nil {
			return nil, err
		}
		h.Sufixo = strings.TrimSpace(h.Sufixo)
	}
	if h.Sufixo == "" {
		return nil, specErrf("history.suffix vazio: a tabela irmã teria o mesmo nome do dataset base")
	}
	if nome := h.NomeDaTabela(nomeBase); !isIdentName(nome) {
		return nil, specErrf("history.suffix %q produz o nome de tabela %q, inválido (esperado [a-z0-9_]+)",
			h.Sufixo, nome)
	}

	if err := prazoDoHistorico(m, h); err != nil {
		return nil, err
	}
	if tn, ok := m["maxBytes"]; ok {
		var bruto string
		if err := decodeEm(tn, "history.maxBytes", &bruto); err != nil {
			return nil, err
		}
		if h.MaxBytes, err = units.Tamanho(bruto, 0); err != nil {
			return nil, specErrf("history.maxBytes: %s", semPrefixoDeSpec(err))
		}
	}
	if sn, ok := m["segmentSize"]; ok {
		var bruto string
		if err := decodeEm(sn, "history.segmentSize", &bruto); err != nil {
			return nil, err
		}
		if h.SegmentSize, err = units.Tamanho(bruto, 0); err != nil {
			return nil, specErrf("history.segmentSize: %s", semPrefixoDeSpec(err))
		}
		if h.SegmentSize > tetoDeSegmento {
			return nil, specErrf("history.segmentSize %d excede o máximo de 1 GiB "+
				"(o offset da location tem 30 bits)", h.SegmentSize)
		}
	}
	// A conferência cruzada só vale com o segmentSize DECLARADO. Quem o omite
	// herda o global, que só é conhecido na abertura — e lá o
	// ValidaContraSegmento roda com o valor efetivo.
	if h.SegmentSize > 0 {
		if err := h.ValidaContraSegmento(h.SegmentSize); err != nil {
			return nil, err
		}
	}

	if h.ExpireAfterWrite == 0 && h.MaxBytes == 0 {
		return nil, specErrf("history: declare expireAfterWrite, maxBytes ou os dois — " +
			"um histórico sem retenção é disco que cresce para sempre")
	}

	if rn, ok := m["rawPayload"]; ok {
		if err := decodeEm(rn, "history.rawPayload", &h.RawPayload); err != nil {
			return nil, err
		}
	}
	if h.RawPayload {
		if _, colide := fields[CampoRaw]; colide {
			return nil, specErrf("history.rawPayload exige a coluna %q, e o schema já declara um campo com esse nome",
				CampoRaw)
		}
	}

	if h.Indices, err = indicesDoHistorico(m["fields"], fields); err != nil {
		return nil, err
	}
	return h, nil
}

// prazoDoHistorico lê expireAfterWrite. O `never` das regras de TTL não vale
// aqui: lá ele é a decisão de uma regra que casou; aqui, não querer expiração
// por tempo se diz OMITINDO a chave — e o maxBytes continua de pé.
func prazoDoHistorico(m map[string]*yaml.Node, h *HistoricoSpec) error {
	pn, ok := m["expireAfterWrite"]
	if !ok {
		return nil
	}
	var bruto string
	if err := decodeEm(pn, "history.expireAfterWrite", &bruto); err != nil {
		return err
	}
	if bruto == DuracaoNunca {
		return specErrf("history.expireAfterWrite %q não existe no histórico: "+
			"omita a chave para não expirar por tempo", DuracaoNunca)
	}
	d, err := units.DuracaoComDias(bruto, 0)
	if err != nil {
		return specErrf("history.expireAfterWrite: %s", semPrefixoDeSpec(err))
	}
	if d < ResolucaoDaExpiracao {
		return specErrf("history.expireAfterWrite %s é menor que a resolução do coletor (%s)",
			bruto, ResolucaoDaExpiracao)
	}
	h.ExpireAfterWrite = d
	return nil
}

// indicesDoHistorico valida `history.fields` e devolve o mapa coluna→motor.
//
// A iteração é em ordem alfabética das chaves do documento: o mapa do yaml.v3
// não tem ordem, e sem ordenar a mensagem de um YAML com dois defeitos mudaria
// entre execuções.
func indicesDoHistorico(n *yaml.Node, fields map[string]FieldSpec) (map[string]string, error) {
	if ausente(n) {
		return nil, nil
	}
	colunas, err := bloco(n, "history.fields", nomesDeColuna(fields)...)
	if err != nil {
		return nil, err
	}
	nomes := make([]string, 0, len(colunas))
	for nome := range colunas {
		nomes = append(nomes, nome)
	}
	sort.Strings(nomes)

	out := make(map[string]string, len(nomes))
	for _, nome := range nomes {
		caminho := "history.fields." + nome
		chaves, err := bloco(colunas[nome], caminho, "index")
		if err != nil {
			return nil, err
		}
		idx, err := texto(chaves, caminho, "index")
		if err != nil {
			return nil, err
		}
		if idx == "" {
			return nil, specErrf("%s: index é a única chave declarável aqui, e é obrigatória — "+
				"o histórico espelha o schema base, não o redefine", caminho)
		}
		tipo, err := tipoIndexavelDoHistorico(nome, fields)
		if err != nil {
			return nil, err
		}
		if err := validaIndice(caminho, idx, tipo); err != nil {
			return nil, err
		}
		out[nome] = idx
	}
	return out, nil
}

// nomesDeColuna é a whitelist que o bloco() usa para recusar coluna
// inexistente. Passar a lista das que EXISTEM faz a mensagem de erro nomear as
// alternativas, em vez de só dizer que a chave é desconhecida.
func nomesDeColuna(fields map[string]FieldSpec) []string {
	out := make([]string, 0, len(fields))
	for nome := range fields {
		out = append(out, nome)
	}
	sort.Strings(out)
	return out
}

// tipoIndexavelDoHistorico devolve o tipo contra o qual a indexabilidade se
// decide, recusando as duas famílias que não têm um.
func tipoIndexavelDoHistorico(nome string, fields map[string]FieldSpec) (string, error) {
	f := fields[nome]
	if f.FrozenFrom != "" {
		return "", specErrf("history.fields.%s: %q é uma coluna congelada, e congelada é produto do merge — "+
			"o histórico não tem merge, então a coluna não existe lá", nome, nome)
	}
	if f.Type == "" || f.Type == "auto" {
		return "", specErrf("history.fields.%s: o campo é auto e o tipo só é conhecido em runtime; "+
			"declare o tipo em schema.fields para poder indexá-lo", nome)
	}
	return f.Type, nil
}

// DerivaHistorico monta a spec do dataset irmão a partir da spec base.
//
// Não devolve erro porque não decide nada: tudo o que poderia ser recusado já
// foi, no validate. O que ela faz é ESPELHAR — e as quatro diferenças em
// relação ao base são as quatro coisas que o histórico não tem.
func (s *DatasetSpec) DerivaHistorico() *DatasetSpec {
	if s == nil || s.Historico == nil {
		return nil
	}
	h := &DatasetSpec{
		Version:     1,
		Name:        s.Historico.NomeDaTabela(s.Name),
		EhHistorico: true,
		Query:       s.Query,
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
	for nome, f := range s.Schema.Fields {
		if f.FrozenFrom != "" {
			continue
		}
		// Merge é política de escrita e o histórico não escreve por cima de
		// nada: a coluna `merge: sum` guarda a PARCELA do evento, que é o que
		// a origem mandou — e é o que se quer ver ao depurar um total.
		f.Merge = ""
		// O índice NÃO é herdado; ele vem só de history.fields.
		f.Index = s.Historico.Indices[nome]
		f.Extract = espelhaExtracoes(f.Extract)
		h.Schema.Fields[nome] = f
	}
	if s.Historico.RawPayload {
		h.Schema.Fields[CampoRaw] = FieldSpec{Type: "json"}
	}
	h.Schema.Include = includeDoHistorico(s, h.Schema.Fields)
	return h
}

// espelhaExtracoes copia as extrações do campo pai. A cópia é necessária
// porque o slice é compartilhado com a spec do base, e o Compile do histórico
// não pode enxergar — nem escrever — no que o base compilou.
func espelhaExtracoes(orig []ExtractSpec) []ExtractSpec {
	if len(orig) == 0 {
		return nil
	}
	out := make([]ExtractSpec, len(orig))
	copy(out, orig)
	return out
}

// includeDoHistorico devolve o conjunto visível do histórico.
//
// O `include` do base é reescrito em vez de copiado por uma razão só: ele pode
// nomear uma congelada, que aqui não existe, e um include que nomeia campo
// inexistente é recusado no Compile. Ordenar torna o FieldID determinístico
// pelo mesmo caminho que o base usa.
func includeDoHistorico(s *DatasetSpec, fields map[string]FieldSpec) []string {
	if len(s.Schema.Include) == 1 && s.Schema.Include[0] == "*" {
		return []string{"*"}
	}
	out := make([]string, 0, len(fields))
	for nome := range fields {
		out = append(out, nome)
	}
	sort.Strings(out)
	return out
}

// DescreveHistorico rende o bloco num texto de UMA linha, para o log de
// subida — a mesma disciplina de DescreveRegras: o operador precisa poder
// conferir que o YAML que escreveu é o que o processo entendeu.
func DescreveHistorico(nome string, h *HistoricoSpec) string {
	if h == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(nome)
	if h.ExpireAfterWrite > 0 {
		fmt.Fprintf(&sb, " expireAfterWrite=%s", h.ExpireAfterWrite)
	}
	if h.MaxBytes > 0 {
		fmt.Fprintf(&sb, " maxBytes=%d", h.MaxBytes)
	}
	if h.RawPayload {
		sb.WriteString(" raw")
	}
	if len(h.Indices) > 0 {
		nomes := make([]string, 0, len(h.Indices))
		for n := range h.Indices {
			nomes = append(nomes, n)
		}
		sort.Strings(nomes)
		for _, n := range nomes {
			fmt.Fprintf(&sb, " %s:%s", n, h.Indices[n])
		}
	}
	return sb.String()
}

// Espelha registra m para receber, em runtime, toda coluna que c descobrir.
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
func (c *Compiled) Espelha(m *Compiled) {
	if c == nil || m == nil || m == c {
		return
	}
	atual := c.listaDeEspelhos()
	if slices.Contains(atual, m) {
		return
	}
	nova := make([]*Compiled, len(atual), len(atual)+1)
	copy(nova, atual)
	nova = append(nova, m)
	c.espelhos.Store(&nova)
}

// listaDeEspelhos devolve o slice imutável corrente, ou nil.
func (c *Compiled) listaDeEspelhos() []*Compiled {
	if c == nil {
		return nil
	}
	if p := c.espelhos.Load(); p != nil {
		return *p
	}
	return nil
}

// propagaAosEspelhos replica a coluna recém-descoberta. Chamado pelo writer
// com c.mu já travado na escrita — por isso ele não pode tocar c.mu, e por
// isso a lista vive num ponteiro atômico.
//
// A ordem de travamento é sempre base→espelho, e como o espelho não repassa a
// propagação, não há cadeia em que ela pudesse se inverter.
func (c *Compiled) propagaAosEspelhos(cf *CompiledField) {
	for _, m := range c.listaDeEspelhos() {
		m.adotaDescoberta(cf)
	}
}

// adotaDescoberta cria a coluna equivalente à do original. O receptor aqui é
// sempre o ESPELHO, e o parâmetro é o campo do Compiled que a descobriu.
//
// O CompiledField é NOVO, e não o ponteiro do original, porque `Index` mora
// nele e é justamente o que difere entre as duas tabelas: compartilhar o
// ponteiro faria o índice declarado em `history.fields` valer para o dataset
// base também.
func (c *Compiled) adotaDescoberta(orig *CompiledField) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, tomado := c.byName[orig.Name]; tomado {
		return
	}
	if _, tomado := c.byID[orig.ID]; tomado {
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
		pai, ok := c.byID[orig.Parent.ID]
		if !ok {
			// Pai ausente neste espelho: adotar a filha a deixaria órfã e
			// visível no SELECT *, que é pior do que não a ter.
			return
		}
		cf.Parent = pai
	}
	cf.autoType.Store(orig.autoType.Load())
	c.byName[cf.Name] = cf
	c.byID[cf.ID] = cf
}

// fixaTipoEspelhado replica o tipo que o original acabou de fixar. Como em
// adotaDescoberta, o receptor é o ESPELHO.
//
// Não passa por FixAutoType de propósito: aquele PERSISTE no dicionário, e o
// dicionário é o mesmo — gravar de novo seria uma segunda escrita durável para
// o mesmo fato, com a única consequência possível sendo um conflito espúrio se
// as duas chegarem fora de ordem.
func (c *Compiled) fixaTipoEspelhado(id types.FieldID, t types.FieldType) {
	c.mu.RLock()
	f, ok := c.byID[id]
	c.mu.RUnlock()
	if ok && f.IsAuto {
		f.autoType.Store(uint32(t))
	}
}
