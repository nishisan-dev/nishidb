package schema

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// yamlWithHistory monta uma spec de dataset base com o bloco `history` dado.
// O schema tem um campo de cada família que a validação do bloco consulta —
// indexável por hash, indexável por bitmap, não indexável, e uma congelada.
func yamlWithHistory(history string) string {
	return "version: 1\nname: events\n" +
		"materialization:\n  key:\n    field: identifier\n" +
		history +
		"schema:\n  include: [\"*\"]\n  fields:\n" +
		"    identifier:\n      type: string\n" +
		"    node:\n      type: string\n      index: true\n" +
		"    severity:\n      type: int64\n" +
		"    latencia:\n      type: double\n" +
		"    doc:\n      type: json\n" +
		"    originalSeverity:\n      frozenFrom: severity\n"
}

func specWithHistory(t *testing.T, history string) *DatasetSpec {
	t.Helper()
	s, err := Parse([]byte(yamlWithHistory(history)), BuiltinDefaults)
	if err != nil {
		t.Fatalf("a spec foi recusada: %v", err)
	}
	return s
}

func refusesHistory(t *testing.T, history string, snippets ...string) {
	t.Helper()
	_, err := Parse([]byte(yamlWithHistory(history)), BuiltinDefaults)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, snippet := range snippets {
		if !strings.Contains(err.Error(), snippet) {
			t.Errorf("a mensagem precisa nomear %q: %v", snippet, err)
		}
	}
}

func TestSemBlocoHistoricoNaoHaTabelaIrma(t *testing.T) {
	if h := specWithHistory(t, "").History; h != nil {
		t.Fatalf("Historico = %+v, quer nil", h)
	}
}

// Um histórico sem nenhuma política de retenção é disco que cresce para
// sempre. Aceitar em silêncio é a configuração que mente.
func TestHistoricoSemRetencaoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  suffix: _hist\n", "expireAfterWrite", "maxBytes")
}

func TestHistoricoRecusaChaveDesconhecida(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  maxByte: 1GiB\n",
		"history.maxByte", "chave desconhecida")
}

func TestSufixoPadraoEhHistory(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n")
	if got := s.History.TableName(s.Name); got != "events_history" {
		t.Fatalf("TableName = %q, quer %q", got, "events_history")
	}
}

func TestSufixoDeclaradoVence(t *testing.T) {
	s := specWithHistory(t, "history:\n  suffix: _bruto\n  expireAfterWrite: 7d\n")
	if got := s.History.TableName(s.Name); got != "events_bruto" {
		t.Fatalf("TableName = %q, quer %q", got, "events_bruto")
	}
}

// O nome derivado é nome de dataset e passa pelas mesmas regras: sem ela, o
// sufixo produziria uma tabela que o FROM não consegue nomear.
func TestSufixoQueProduzNomeInvalidoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  suffix: \"-Hist\"\n  expireAfterWrite: 7d\n", "events-Hist")
}

func TestSufixoVazioEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  suffix: \"\"\n  expireAfterWrite: 7d\n", "suffix")
}

func TestExpireAfterWriteAbaixoDaResolucaoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 30s\n", "resolução")
}

// `never` existe para dizer que a ENTIDADE não expira. No histórico, não
// querer expiração por tempo se diz omitindo a chave — e sobra o maxBytes.
func TestExpireAfterWriteNeverEhRecusadoNoHistorico(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: never\n  maxBytes: 1GiB\n", "omita")
}

func TestMaxBytesAceitaUnidadeBinaria(t *testing.T) {
	s := specWithHistory(t, "history:\n  maxBytes: 2GiB\n")
	if got := s.History.MaxBytes; got != 2<<30 {
		t.Fatalf("MaxBytes = %d, quer %d", got, 2<<30)
	}
}

func TestMaxBytesDecimalEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  maxBytes: 512MB\n", "history.maxBytes")
}

// Com o teto abaixo de quatro segmentos a purga nunca fica abaixo dele sem
// tocar o segmento corrente, que a D6 proíbe.
func TestMaxBytesMenorQueQuatroSegmentosEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  maxBytes: 100MiB\n  segmentSize: 64MiB\n", "segmentSize")
}

func TestSegmentSizeAcimaDoTetoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  maxBytes: 100GiB\n  segmentSize: 2GiB\n", "history.segmentSize")
}

// Com o segmentSize omitido o teto efetivo só é conhecido na abertura, e é lá
// que a conferência acontece.
func TestValidaContraSegmentoEfetivoConfereODeclaradoNaAbertura(t *testing.T) {
	h := specWithHistory(t, "history:\n  maxBytes: 100MiB\n").History
	if err := h.ValidateAgainstSegment(512 << 20); err == nil {
		t.Fatal("100MiB de teto com segmento de 512MiB foi aceito")
	}
	if err := h.ValidateAgainstSegment(16 << 20); err != nil {
		t.Fatalf("100MiB de teto com segmento de 16MiB foi recusado: %v", err)
	}
}

func TestIndiceDoHistoricoSobreCampoInexistenteEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    inexistente:\n      index: true\n",
		"inexistente")
}

// O histórico ESPELHA o schema base; ele não redefine tipo, format nem
// política de escrita. Só `index` é declarável ali.
func TestBlocoDeCampoDoHistoricoSoAceitaIndex(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    node:\n      type: string\n",
		"chave desconhecida", "index")
}

func TestIndiceInvalidoNoHistoricoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    node:\n      index: arvore\n",
		"arvore")
}

func TestTipoNaoIndexavelNoHistoricoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    latencia:\n      index: true\n",
		"não suporta")
}

// A congelada é produto do merge, e o histórico não tem merge: a coluna não
// existe lá, então indexá-la é indexar o que não há.
func TestCongeladaNaoPodeSerIndexadaNoHistorico(t *testing.T) {
	refusesHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    originalSeverity:\n      index: true\n",
		"originalSeverity", "congelada")
}

// No base a key é recusada porque a primary já a indexa. No histórico não há
// primary, e ela é a coluna de consulta mais provável de todas.
func TestAKeyDoBasePodeSerIndexadaNoHistorico(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    identifier:\n      index: true\n")
	if got := s.History.Indices["identifier"]; got != "true" {
		t.Fatalf("Indices[identifier] = %q, quer %q", got, "true")
	}
}

func TestRawPayloadColideComCampoRawDoBase(t *testing.T) {
	yaml := "version: 1\nname: events\n" +
		"materialization:\n  key:\n    field: identifier\n" +
		"history:\n  expireAfterWrite: 7d\n  rawPayload: true\n" +
		"schema:\n  include: [\"*\"]\n  fields:\n" +
		"    identifier:\n      type: string\n" +
		"    _raw:\n      type: string\n"
	if _, err := Parse([]byte(yaml), BuiltinDefaults); err == nil {
		t.Fatal("a spec foi aceita")
	} else if !strings.Contains(err.Error(), RawFieldName) {
		t.Fatalf("a mensagem precisa nomear %q: %v", RawFieldName, err)
	}
}

func TestDerivaHistoricoNomeiaATabelaIrma(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n").DeriveHistory()
	if h.Name != "events_history" {
		t.Fatalf("Name = %q, quer %q", h.Name, "events_history")
	}
	if !h.IsHistory {
		t.Fatal("EhHistorico = false")
	}
}

func TestSemBlocoHistoricoDerivaDevolveNil(t *testing.T) {
	if h := specWithHistory(t, "").DeriveHistory(); h != nil {
		t.Fatalf("DeriveHistory = %+v, quer nil", h)
	}
}

// Sem key não há primary, e sem primary não há dedup — que é a definição da
// tabela. A sequence some pela mesma razão: não há estado a sobrescrever.
func TestDerivaHistoricoNaoTemKeyNemSequence(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n").DeriveHistory()
	if h.Materialization.Key.Field != "" {
		t.Errorf("Key.Field = %q, quer vazio", h.Materialization.Key.Field)
	}
	if h.Materialization.Sequence != nil {
		t.Errorf("Sequence = %+v, quer nil", h.Materialization.Sequence)
	}
	if len(h.Materialization.TTLRules) != 0 {
		t.Errorf("TTLRules = %+v, quer vazio", h.Materialization.TTLRules)
	}
}

func TestDerivaHistoricoNaoTemColunasCongeladas(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n").DeriveHistory()
	if _, has := h.Schema.Fields["originalSeverity"]; has {
		t.Fatal("a congelada sobreviveu à derivação")
	}
	if _, tem := h.Schema.Fields["severity"]; !tem {
		t.Fatal("a ORIGEM da congelada foi removida junto")
	}
}

// O bitmap custa (distintos × rows / 8), e o histórico tem muito mais rows que
// o base. Herdar o índice do base multiplicaria esse custo sem ninguém pedir.
func TestDerivaHistoricoNaoHerdaIndiceDoBase(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n").DeriveHistory()
	if idx := h.Schema.Fields["node"].Index; idx != "" {
		t.Fatalf("node.Index = %q no histórico, quer vazio", idx)
	}
}

func TestDerivaHistoricoAplicaOsIndicesDeclarados(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n  fields:\n"+
		"    identifier:\n      index: true\n    severity:\n      index: true\n").DeriveHistory()
	if idx := h.Schema.Fields["identifier"].Index; idx != "true" {
		t.Errorf("identifier.Index = %q, quer true", idx)
	}
	if idx := h.Schema.Fields["severity"].Index; idx != "true" {
		t.Errorf("severity.Index = %q, quer true", idx)
	}
}

func TestDerivaHistoricoAcrescentaRawQuandoPedido(t *testing.T) {
	withoutRaw := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n").DeriveHistory()
	if _, has := withoutRaw.Schema.Fields[RawFieldName]; has {
		t.Errorf("%s apareceu sem rawPayload", RawFieldName)
	}
	withRaw := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n  rawPayload: true\n").DeriveHistory()
	f, tem := withRaw.Schema.Fields[RawFieldName]
	if !tem {
		t.Fatalf("%s não foi acrescentado", RawFieldName)
	}
	if f.Type != "json" {
		t.Fatalf("%s.Type = %q, quer json", RawFieldName, f.Type)
	}
}

// A retenção por tempo lê o _writtenAt de cada linha; sem ele o motor não tem
// contra o que comparar.
func TestHistoricoComPrazoExigeOInstanteDeEscrita(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n")
	if !s.RequiresWriteInstant() {
		t.Fatal("RequiresWriteInstant = false com expireAfterWrite declarado")
	}
	if !s.DeriveHistory().RequiresWriteInstant() {
		t.Fatal("a spec derivada não exige o instante de escrita")
	}
}

func TestExpireAfterWriteDoHistoricoEhParseado(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 3d\n")
	if got := s.History.ExpireAfterWrite; got != 72*time.Hour {
		t.Fatalf("ExpireAfterWrite = %s, quer 72h", got)
	}
}

// compilePair compila o base e o histórico contra o MESMO dicionário, que é
// como o servidor os abre. É o dicionário compartilhado que faz os FieldIDs
// coincidirem — sem ele o payload escrito pelo patch do base não decodificaria
// do outro lado.
func compilePair(t *testing.T, history string) (base, hist *Compiled) {
	t.Helper()
	s := specWithHistory(t, history)
	d := newFakeDict()
	base, err := s.Compile(d)
	if err != nil {
		t.Fatalf("compilando o base: %v", err)
	}
	hist, err = s.DeriveHistory().Compile(d)
	if err != nil {
		t.Fatalf("compilando o histórico: %v", err)
	}
	return base, hist
}

func TestOHistoricoCompartilhaOsFieldIDsDoBase(t *testing.T) {
	base, hist := compilePair(t, "history:\n  expireAfterWrite: 7d\n")
	for _, name := range []string{"identifier", "node", "severity", "latencia", "doc", WriteInstantFieldName} {
		bf, ok := base.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("o base não tem %q", name)
		}
		hf, ok := hist.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("o histórico não tem %q", name)
		}
		if bf.ID != hf.ID {
			t.Errorf("%s: FieldID %d no base e %d no histórico", name, bf.ID, hf.ID)
		}
		if bf.Type != hf.Type {
			t.Errorf("%s: tipo %v no base e %v no histórico", name, bf.Type, hf.Type)
		}
	}
}

// Sem key não há primary, e é o Key nil que impede o planner de eleger o point
// lookup sobre um Store que não tem contra o que resolvê-lo.
func TestOCompiladoDoHistoricoNaoTemKeyNemSequence(t *testing.T) {
	_, hist := compilePair(t, "history:\n  expireAfterWrite: 7d\n")
	if hist.Key != nil {
		t.Errorf("Key = %+v, quer nil", hist.Key)
	}
	if hist.Seq != nil {
		t.Errorf("Seq = %+v, quer nil", hist.Seq)
	}
	// A coluna continua lá — o que sumiu foi o papel dela.
	f, ok := hist.FieldByName([]byte("identifier"))
	if !ok {
		t.Fatal("identifier sumiu do histórico")
	}
	if f.IsKey {
		t.Error("identifier ainda é IsKey no histórico")
	}
}

// O instante de escrita é o que a retenção por tempo lê. O prazo por linha não
// existe aqui: o prazo é do dataset inteiro, e é uma constante.
func TestOHistoricoTemInstanteDeEscritaESemPrazo(t *testing.T) {
	_, hist := compilePair(t, "history:\n  expireAfterWrite: 7d\n")
	if hist.WriteInstantField == nil {
		t.Error("o histórico não compilou o instante de escrita")
	}
	if hist.TTLField != nil {
		t.Error("o histórico compilou o campo de prazo por linha")
	}
}

// Ligar o histórico com prazo num dataset SEM regras de TTL tem de acrescentar
// o _writtenAt ao BASE também: é ele quem grava o campo, no patch.
func TestBaseSemTTLGanhaOInstanteDeEscritaQuandoOHistoricoExpira(t *testing.T) {
	base, _ := compilePair(t, "history:\n  expireAfterWrite: 7d\n")
	if base.WriteInstantField == nil {
		t.Fatal("o base não compilou o instante de escrita")
	}
	if base.TTLField != nil {
		t.Error("o base compilou o campo de prazo sem declarar regras de TTL")
	}
}

// Histórico só por tamanho não precisa do instante de escrita, e acrescentá-lo
// custaria 8 bytes por versão de record para nada.
func TestHistoricoSoPorTamanhoNaoExigeOInstanteDeEscrita(t *testing.T) {
	s := specWithHistory(t, "history:\n  maxBytes: 4GiB\n")
	if s.RequiresWriteInstant() {
		t.Fatal("RequiresWriteInstant = true sem expireAfterWrite")
	}
}

func TestOHistoricoNaoCompilaAsCongeladas(t *testing.T) {
	base, hist := compilePair(t, "history:\n  expireAfterWrite: 7d\n")
	if _, ok := base.FieldByName([]byte("originalSeverity")); !ok {
		t.Fatal("a congelada não está no base — a fixture perdeu o caso")
	}
	if _, ok := hist.FieldByName([]byte("originalSeverity")); ok {
		t.Fatal("a congelada foi compilada no histórico")
	}
}

func TestOsIndicesDoHistoricoChegamAoCompilado(t *testing.T) {
	base, hist := compilePair(t, "history:\n  expireAfterWrite: 7d\n  fields:\n"+
		"    identifier:\n      index: true\n")
	hf, _ := hist.FieldByName([]byte("identifier"))
	if !hf.Indexed {
		t.Error("identifier não saiu indexado no histórico")
	}
	// E o node, que é indexado no BASE, não herda nada.
	bn, _ := base.FieldByName([]byte("node"))
	if !bn.Indexed {
		t.Fatal("a fixture perdeu o índice do base")
	}
	hn, _ := hist.FieldByName([]byte("node"))
	if hn.Indexed {
		t.Error("node saiu indexado no histórico, quer sem índice")
	}
}

// pairWithDiscovery compila base e histórico contra o mesmo dicionário e já
// registra o espelho, que é como o servidor os abre.
func pairWithDiscovery(t *testing.T, extraSchema string) (base, hist *Compiled) {
	t.Helper()
	yaml := "version: 1\nname: events\n" +
		"materialization:\n  key:\n    field: identifier\n" +
		"history:\n  expireAfterWrite: 7d\n" +
		"schema:\n  include: [\"*\"]\n  unknownFields: include\n" + extraSchema + "  fields:\n" +
		"    identifier:\n      type: string\n" +
		"    doc:\n      type: json\n      unknownFields: include\n"
	s, err := Parse([]byte(yaml), BuiltinDefaults)
	if err != nil {
		t.Fatalf("a spec foi recusada: %v", err)
	}
	d := newFakeDict()
	if base, err = s.Compile(d); err != nil {
		t.Fatalf("compilando o base: %v", err)
	}
	if hist, err = s.DeriveHistory().Compile(d); err != nil {
		t.Fatalf("compilando o histórico: %v", err)
	}
	base.AddMirror(hist)
	return base, hist
}

func TestCampoDescobertoEmRuntimeApareceNasDuasTabelas(t *testing.T) {
	base, hist := pairWithDiscovery(t, "")
	cf, err := base.ResolveUnknown([]byte("novo"))
	if err != nil || cf == nil {
		t.Fatalf("ResolveUnknown = (%v, %v)", cf, err)
	}
	hf, ok := hist.FieldByName([]byte("novo"))
	if !ok {
		t.Fatal("a coluna descoberta não chegou ao histórico")
	}
	if hf.ID != cf.ID {
		t.Errorf("FieldID %d no base e %d no histórico", cf.ID, hf.ID)
	}
	if !hf.IsAuto {
		t.Error("a coluna espelhada não é auto")
	}
	// Ponteiros distintos: o Index mora no CompiledField, e é ele que difere
	// entre as duas tabelas.
	if hf == cf {
		t.Error("o histórico compartilhou o ponteiro do CompiledField do base")
	}
}

// A derivada é mantida fora do SELECT * pela ligação com o pai. Reatá-la ao
// pai DESTE Compiled é o que faz `doc.*` achá-la aqui.
func TestColunaAninhadaDescobertaReataAoPaiDoHistorico(t *testing.T) {
	base, hist := pairWithDiscovery(t, "")
	baseParent, ok := base.FieldByName([]byte("doc"))
	if !ok {
		t.Fatal("o base não tem doc")
	}
	cf, err := base.ResolveNested(baseParent, []byte("doc.ti"), []byte("ti"))
	if err != nil || cf == nil {
		t.Fatalf("ResolveNested = (%v, %v)", cf, err)
	}
	hf, ok := hist.FieldByName([]byte("doc.ti"))
	if !ok {
		t.Fatal("a coluna aninhada não chegou ao histórico")
	}
	histParent, _ := hist.FieldByName([]byte("doc"))
	if hf.Parent != histParent {
		t.Fatalf("Parent = %p, quer o doc do histórico (%p)", hf.Parent, histParent)
	}
	if hf.Parent == baseParent {
		t.Fatal("o histórico reatou a filha ao pai do BASE")
	}
	if hf.Path != "ti" {
		t.Errorf("Path = %q, quer %q", hf.Path, "ti")
	}
	// E ela continua fora do SELECT * dos dois lados.
	for _, f := range hist.FieldsByID() {
		if f.Name == "doc.ti" {
			t.Fatal("a derivada entrou no SELECT * do histórico")
		}
	}
}

func TestFixAutoTypeChegaAoEspelho(t *testing.T) {
	base, hist := pairWithDiscovery(t, "")
	cf, err := base.ResolveUnknown([]byte("contador"))
	if err != nil || cf == nil {
		t.Fatalf("ResolveUnknown = (%v, %v)", cf, err)
	}
	hf, _ := hist.FieldByName([]byte("contador"))
	if hf.EffectiveType() != types.TypeNull {
		t.Fatalf("o tipo já estava fixado antes da observação: %v", hf.EffectiveType())
	}
	if err := base.FixAutoType(cf, types.TypeInt64); err != nil {
		t.Fatalf("FixAutoType: %v", err)
	}
	if got := hf.EffectiveType(); got != types.TypeInt64 {
		t.Fatalf("tipo no histórico = %v, quer int64", got)
	}
}

// O teto governa o crescimento do dicionário, e o espelho não pode contorná-lo:
// a coluna que o base recusou não existe em lugar nenhum.
func TestColunaRecusadaPeloTetoNaoChegaAoEspelho(t *testing.T) {
	base, hist := pairWithDiscovery(t, "  maxCols: 3\n")
	before := len(hist.FieldsByIDIncludingSystem())
	cf, err := base.ResolveUnknown([]byte("excedente"))
	if err != nil {
		t.Fatalf("ResolveUnknown: %v", err)
	}
	if cf != nil {
		t.Fatal("a fixture não alcançou o teto — o caso se perdeu")
	}
	if _, ok := hist.FieldByName([]byte("excedente")); ok {
		t.Fatal("a coluna recusada pelo teto apareceu no histórico")
	}
	if after := len(hist.FieldsByIDIncludingSystem()); after != before {
		t.Fatalf("o histórico foi de %d para %d colunas", before, after)
	}
}

func TestEspelharDuasVezesNaoDuplicaAPropagacao(t *testing.T) {
	base, hist := pairWithDiscovery(t, "")
	base.AddMirror(hist)
	base.AddMirror(hist)
	if _, err := base.ResolveUnknown([]byte("novo")); err != nil {
		t.Fatalf("ResolveUnknown: %v", err)
	}
	if _, ok := hist.FieldByName([]byte("novo")); !ok {
		t.Fatal("a coluna não chegou ao histórico")
	}
}

func TestEspelharASiMesmoEhIgnorado(t *testing.T) {
	base, _ := pairWithDiscovery(t, "")
	base.AddMirror(base)
	if n := len(base.mirrorList()); n != 1 {
		t.Fatalf("espelhos = %d, quer 1 (só o histórico)", n)
	}
}

// pairSpecWithRaw é o par completo: universo aberto (que é o que liga a
// reidratação pelo dicionário), congelada, TTL e rawPayload.
const pairSpecWithRaw = `version: 1
name: events
materialization:
  key:
    field: identifier
  ttl:
    - expireAfterWrite: 90d
history:
  expireAfterWrite: 7d
  rawPayload: true
schema:
  include: ["*"]
  unknownFields: include
  fields:
    identifier:
      type: string
    severity:
      type: int64
    originalSeverity:
      frozenFrom: severity
`

// Com o dicionário compartilhado, a reidratação do Compile adota tudo o que
// estiver nele. Sem os nomes fora do universo, cada lado do par adotaria as
// colunas do outro como campo auto SEM TIPO — `IS NULL` eterno e recusa de
// comparação por "sem tipo conhecido", que é pior do que a coluna não existir.
func TestNenhumLadoDoParAdotaAsColunasDoOutro(t *testing.T) {
	spec, err := Parse([]byte(pairSpecWithRaw), BuiltinDefaults)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	d := newFakeDict()
	if _, err := spec.Compile(d); err != nil {
		t.Fatalf("Compile do base: %v", err)
	}
	if _, err := spec.DeriveHistory().Compile(d); err != nil {
		t.Fatalf("Compile do histórico: %v", err)
	}
	// Recompila os DOIS contra o dicionário já povoado: é o que acontece no
	// restart, e é aí que a reidratação tem o que adotar.
	base, err := spec.Compile(d)
	if err != nil {
		t.Fatalf("recompilando o base: %v", err)
	}
	hist, err := spec.DeriveHistory().Compile(d)
	if err != nil {
		t.Fatalf("recompilando o histórico: %v", err)
	}

	// O `_raw` é da irmã.
	if _, has := base.FieldByName([]byte(RawFieldName)); has {
		t.Errorf("o dataset base adotou %s do dicionário", RawFieldName)
	}
	// O prazo por linha e a congelada são do base.
	if _, tem := hist.FieldByName([]byte(TTLFieldName)); tem {
		t.Errorf("o histórico adotou %s do dicionário", TTLFieldName)
	}
	if _, tem := hist.FieldByName([]byte("originalSeverity")); tem {
		t.Error("o histórico adotou a congelada do dicionário")
	}
	// E o que é dos dois continua nos dois.
	for _, name := range []string{"identifier", "severity", WriteInstantFieldName} {
		if _, tem := base.FieldByName([]byte(name)); !tem {
			t.Errorf("o base perdeu %q", name)
		}
		if _, tem := hist.FieldByName([]byte(name)); !tem {
			t.Errorf("o histórico perdeu %q", name)
		}
	}
}

// A exclusão precisa valer para o SELECT *, que é o que o operador vê.
func TestOSelectEstrelaDeCadaLadoNaoTemAColunaDoOutro(t *testing.T) {
	spec, err := Parse([]byte(pairSpecWithRaw), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	d := newFakeDict()
	if _, err := spec.Compile(d); err != nil {
		t.Fatal(err)
	}
	if _, err := spec.DeriveHistory().Compile(d); err != nil {
		t.Fatal(err)
	}
	base, _ := spec.Compile(d)
	hist, _ := spec.DeriveHistory().Compile(d)

	names := func(c *Compiled) []string {
		var out []string
		for _, f := range c.FieldsByID() {
			out = append(out, f.Name)
		}
		return out
	}
	baseNames, histNames := names(base), names(hist)
	if slices.Contains(baseNames, RawFieldName) {
		t.Errorf("%s no SELECT * do base: %v", RawFieldName, baseNames)
	}
	if slices.Contains(histNames, "originalSeverity") {
		t.Errorf("congelada no SELECT * do histórico: %v", histNames)
	}
	if slices.Contains(histNames, TTLFieldName) {
		t.Errorf("%s no SELECT * do histórico: %v", TTLFieldName, histNames)
	}
	if !slices.Contains(histNames, RawFieldName) {
		t.Errorf("%s fora do SELECT * do histórico: %v", RawFieldName, histNames)
	}
}

// Quem roda a retenção é a IRMÃ: o motor vive no Store dela, e quem sobe a
// goroutine percorre a lista de datasets perguntando a cada um pela própria
// configuração. Sem o bloco na spec derivada ela subia sem política nenhuma, e
// o disco crescia para sempre com o YAML declarando um teto.
func TestASpecDerivadaCarregaOBlocoDeRetencao(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n  maxBytes: 4GiB\n")
	h := s.DeriveHistory()
	if h.History == nil {
		t.Fatal("a spec derivada não carrega o bloco history")
	}
	if h.History.ExpireAfterWrite != 7*24*time.Hour {
		t.Errorf("ExpireAfterWrite = %s", h.History.ExpireAfterWrite)
	}
	if h.History.MaxBytes != 4<<30 {
		t.Errorf("MaxBytes = %d", h.History.MaxBytes)
	}
}

// O par tem exatamente dois lados: derivar a partir da irmã produziria uma
// irmã da irmã, e é o EhHistorico que fecha a recursão.
func TestAIrmaNaoDerivaOutraIrma(t *testing.T) {
	s := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n")
	if grandchild := s.DeriveHistory().DeriveHistory(); grandchild != nil {
		t.Fatalf("a irmã derivou %q", grandchild.Name)
	}
}

// TestAdmissaoDaJanela é a tabela de admissão da §4.1 inteira, uma linha por
// caso.
//
// Ela existe como tabela e não como cinco funções porque as cinco respostas só
// fazem sentido lidas juntas: o que separa "recusa" de "aceita com WARN" é uma
// decisão de produto — configuração ERRADA para a subida, configuração INÓCUA
// não —, e essa fronteira é mais fácil de conferir quando os casos estão um
// embaixo do outro.
func TestAdmissaoDaJanela(t *testing.T) {
	cases := []struct {
		name, history string
		refused       bool
		snippets      []string
		// infiniteWindow cobra que o caso aceito NÃO produziu bloco `index`.
		// Sem ele a linha do default seria a única das cinco cuja RESPOSTA
		// ninguém confere — o teste diria só que o YAML parseia, e "bloco
		// ausente é janela infinita" é justamente uma afirmação sobre o
		// comportamento, não sobre a sintaxe.
		infiniteWindow bool
	}{
		{
			name:           "bloco ausente é janela infinita",
			history:        "history:\n  maxBytes: 100MiB\n",
			infiniteWindow: true,
		},
		{
			name:     "index fora do bloco history",
			history:  "index:\n  window: 6h\n",
			refused:  true,
			snippets: []string{"history.index", "exclusiva da tabela histórica"},
		},
		{
			name:     "window zero é ambíguo",
			history:  "history:\n  maxBytes: 100MiB\n  index:\n    window: 0\n",
			refused:  true,
			snippets: []string{"history.index.window", "omita"},
		},
		{
			name:    "maxBytes sem window é válido",
			history: "history:\n  maxBytes: 100MiB\n  index:\n    maxBytes: 1GiB\n",
		},
		{
			name:    "window maior que a retenção passa",
			history: "history:\n  expireAfterWrite: 1h\n  index:\n    window: 7d\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.refused {
				refusesHistory(t, c.history, c.snippets...)
				return
			}
			h := specWithHistory(t, c.history).History
			if c.infiniteWindow && h.Window != nil {
				t.Fatalf("Janela = %+v, quer nil: sem o bloco index: a janela é INFINITA, "+
					"e indexa tudo que a retenção mantém", h.Window)
			}
		})
	}
}

// A recusa da janela sai com ExitDataset (3), e não com o exit de uso: o
// problema está no YAML do dataset, não na linha de comando. O sentinela é o
// que o codigoDoErro do cli consulta — sem ele a recusa cairia no default, e o
// operador seria mandado conferir a flag que digitou.
func TestRecusaDaJanelaTemSentinelaPropria(t *testing.T) {
	cases := map[string]string{
		"fora do history": "index:\n  window: 6h\n",
		"window zero":     "history:\n  maxBytes: 100MiB\n  index:\n    window: 0\n",
		"bloco vazio":     "history:\n  maxBytes: 100MiB\n  index: {}\n",
	}
	for name, history := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(yamlWithHistory(history)), BuiltinDefaults)
			if !errors.Is(err, ErrInvalidIndexWindow) {
				t.Fatalf("erro = %v, quer ErrInvalidIndexWindow", err)
			}
		})
	}
}

// Janela maior que a retenção não está ERRADA, é INÓCUA — a retenção já limita
// antes —, e recusá-la transformaria redundância em queda de serviço. O que ela
// ganha é um WARN nomeado, e é este predicado que a subida consulta para
// emiti-lo.
func TestJanelaMaiorQueARetencaoEhAnunciada(t *testing.T) {
	h := specWithHistory(t, "history:\n  expireAfterWrite: 1h\n  index:\n    window: 7d\n").History
	if h.Window == nil {
		t.Fatal("o bloco index: não foi lido")
	}
	if !h.WindowExceedsRetention() {
		t.Fatal("window: 7d sobre expireAfterWrite: 1h precisa anunciar que a janela é inócua")
	}
	// E o contrário não anuncia nada: janela smaller que a retenção é o caso
	// para o qual a chave existe.
	smaller := specWithHistory(t, "history:\n  expireAfterWrite: 7d\n  index:\n    window: 6h\n").History
	if smaller.WindowExceedsRetention() {
		t.Fatal("window: 6h sobre expireAfterWrite: 7d é o caso normal e não pode avisar nada")
	}
	// Sem prazo declarado não há contra o que comparar: o maxBytes da retenção
	// não é um instante, e avisar aqui seria ruído sobre uma configuração que
	// não tem nada de redundante.
	withoutExpiry := specWithHistory(t, "history:\n  maxBytes: 100MiB\n  index:\n    window: 6h\n").History
	if withoutExpiry.WindowExceedsRetention() {
		t.Fatal("sem expireAfterWrite não há prazo contra o qual a janela pudesse ser inócua")
	}
}

// O bloco lido é o bloco declarado: uma janela que o processo entendeu pela
// metade é a configuração decorativa que a §4.1 existe para não ser.
func TestJanelaDeclaradaChegaNaSpec(t *testing.T) {
	h := specWithHistory(t,
		"history:\n  expireAfterWrite: 7d\n  index:\n    window: 6h\n    maxBytes: 2GiB\n").History
	if h.Window == nil {
		t.Fatal("o bloco index: não foi lido")
	}
	if h.Window.Duration != 6*time.Hour {
		t.Fatalf("Duracao = %s, quer 6h", h.Window.Duration)
	}
	if h.Window.MaxBytes != 2<<30 {
		t.Fatalf("MaxBytes = %d, quer %d", h.Window.MaxBytes, 2<<30)
	}
}

func TestJanelaRecusaChaveDesconhecida(t *testing.T) {
	refusesHistory(t, "history:\n  maxBytes: 100MiB\n  index:\n    windows: 6h\n",
		"history.index.windows", "chave desconhecida")
}

// maxBytes: 0 é recusado pela MESMA razão que window: 0 — um teto de zero bytes
// é ambíguo entre "nenhum índice" e "sem teto", e as duas leituras têm
// consequências opostas. Desligar o teto se diz OMITINDO a chave.
func TestTetoDeIndiceZeradoEhRecusado(t *testing.T) {
	refusesHistory(t, "history:\n  maxBytes: 100MiB\n  index:\n    maxBytes: 0\n",
		"history.index.maxBytes", "omita")
}
