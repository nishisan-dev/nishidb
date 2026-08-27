package schema

import (
	"strings"
	"testing"
	"time"
)

// yamlComHistorico monta uma spec de dataset base com o bloco `history` dado.
// O schema tem um campo de cada família que a validação do bloco consulta —
// indexável por hash, indexável por bitmap, não indexável, e uma congelada.
func yamlComHistorico(historico string) string {
	return "version: 1\nname: events\n" +
		"materialization:\n  key:\n    field: identifier\n" +
		historico +
		"schema:\n  include: [\"*\"]\n  fields:\n" +
		"    identifier:\n      type: string\n" +
		"    node:\n      type: string\n      index: hash\n" +
		"    severity:\n      type: int64\n" +
		"    latencia:\n      type: double\n" +
		"    doc:\n      type: json\n" +
		"    originalSeverity:\n      frozenFrom: severity\n"
}

func specComHistorico(t *testing.T, historico string) *DatasetSpec {
	t.Helper()
	s, err := Parse([]byte(yamlComHistorico(historico)), DefaultsPadrao)
	if err != nil {
		t.Fatalf("a spec foi recusada: %v", err)
	}
	return s
}

func recusaHistorico(t *testing.T, historico string, trechos ...string) {
	t.Helper()
	_, err := Parse([]byte(yamlComHistorico(historico)), DefaultsPadrao)
	if err == nil {
		t.Fatal("a spec foi aceita")
	}
	for _, trecho := range trechos {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("a mensagem precisa nomear %q: %v", trecho, err)
		}
	}
}

func TestSemBlocoHistoricoNaoHaTabelaIrma(t *testing.T) {
	if h := specComHistorico(t, "").Historico; h != nil {
		t.Fatalf("Historico = %+v, quer nil", h)
	}
}

// Um histórico sem nenhuma política de retenção é disco que cresce para
// sempre. Aceitar em silêncio é a configuração que mente.
func TestHistoricoSemRetencaoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  suffix: _hist\n", "expireAfterWrite", "maxBytes")
}

func TestHistoricoRecusaChaveDesconhecida(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  maxByte: 1GiB\n",
		"history.maxByte", "chave desconhecida")
}

func TestSufixoPadraoEhHistory(t *testing.T) {
	s := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n")
	if got := s.Historico.NomeDaTabela(s.Name); got != "events_history" {
		t.Fatalf("NomeDaTabela = %q, quer %q", got, "events_history")
	}
}

func TestSufixoDeclaradoVence(t *testing.T) {
	s := specComHistorico(t, "history:\n  suffix: _bruto\n  expireAfterWrite: 7d\n")
	if got := s.Historico.NomeDaTabela(s.Name); got != "events_bruto" {
		t.Fatalf("NomeDaTabela = %q, quer %q", got, "events_bruto")
	}
}

// O nome derivado é nome de dataset e passa pelas mesmas regras: sem ela, o
// sufixo produziria uma tabela que o FROM não consegue nomear.
func TestSufixoQueProduzNomeInvalidoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  suffix: \"-Hist\"\n  expireAfterWrite: 7d\n", "events-Hist")
}

func TestSufixoVazioEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  suffix: \"\"\n  expireAfterWrite: 7d\n", "suffix")
}

func TestExpireAfterWriteAbaixoDaResolucaoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 30s\n", "resolução")
}

// `never` existe para dizer que a ENTIDADE não expira. No histórico, não
// querer expiração por tempo se diz omitindo a chave — e sobra o maxBytes.
func TestExpireAfterWriteNeverEhRecusadoNoHistorico(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: never\n  maxBytes: 1GiB\n", "omita")
}

func TestMaxBytesAceitaUnidadeBinaria(t *testing.T) {
	s := specComHistorico(t, "history:\n  maxBytes: 2GiB\n")
	if got := s.Historico.MaxBytes; got != 2<<30 {
		t.Fatalf("MaxBytes = %d, quer %d", got, 2<<30)
	}
}

func TestMaxBytesDecimalEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  maxBytes: 512MB\n", "history.maxBytes")
}

// Com o teto abaixo de quatro segmentos a purga nunca fica abaixo dele sem
// tocar o segmento corrente, que a D6 proíbe.
func TestMaxBytesMenorQueQuatroSegmentosEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  maxBytes: 100MiB\n  segmentSize: 64MiB\n", "segmentSize")
}

func TestSegmentSizeAcimaDoTetoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  maxBytes: 100GiB\n  segmentSize: 2GiB\n", "history.segmentSize")
}

// Com o segmentSize omitido o teto efetivo só é conhecido na abertura, e é lá
// que a conferência acontece.
func TestValidaContraSegmentoEfetivoConfereODeclaradoNaAbertura(t *testing.T) {
	h := specComHistorico(t, "history:\n  maxBytes: 100MiB\n").Historico
	if err := h.ValidaContraSegmento(512 << 20); err == nil {
		t.Fatal("100MiB de teto com segmento de 512MiB foi aceito")
	}
	if err := h.ValidaContraSegmento(16 << 20); err != nil {
		t.Fatalf("100MiB de teto com segmento de 16MiB foi recusado: %v", err)
	}
}

func TestIndiceDoHistoricoSobreCampoInexistenteEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    inexistente:\n      index: hash\n",
		"inexistente")
}

// O histórico ESPELHA o schema base; ele não redefine tipo, format nem
// política de escrita. Só `index` é declarável ali.
func TestBlocoDeCampoDoHistoricoSoAceitaIndex(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    node:\n      type: string\n",
		"chave desconhecida", "index")
}

func TestIndiceInvalidoNoHistoricoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    node:\n      index: arvore\n",
		"arvore")
}

func TestTipoNaoIndexavelNoHistoricoEhRecusado(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    latencia:\n      index: bitmap\n",
		"não suporta")
}

// A congelada é produto do merge, e o histórico não tem merge: a coluna não
// existe lá, então indexá-la é indexar o que não há.
func TestCongeladaNaoPodeSerIndexadaNoHistorico(t *testing.T) {
	recusaHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    originalSeverity:\n      index: bitmap\n",
		"originalSeverity", "congelada")
}

// No base a key é recusada porque a primary já a indexa. No histórico não há
// primary, e ela é a coluna de consulta mais provável de todas.
func TestAKeyDoBasePodeSerIndexadaNoHistorico(t *testing.T) {
	s := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n    identifier:\n      index: hash\n")
	if got := s.Historico.Indices["identifier"]; got != "hash" {
		t.Fatalf("Indices[identifier] = %q, quer %q", got, "hash")
	}
}

func TestRawPayloadColideComCampoRawDoBase(t *testing.T) {
	yaml := "version: 1\nname: events\n" +
		"materialization:\n  key:\n    field: identifier\n" +
		"history:\n  expireAfterWrite: 7d\n  rawPayload: true\n" +
		"schema:\n  include: [\"*\"]\n  fields:\n" +
		"    identifier:\n      type: string\n" +
		"    _raw:\n      type: string\n"
	if _, err := Parse([]byte(yaml), DefaultsPadrao); err == nil {
		t.Fatal("a spec foi aceita")
	} else if !strings.Contains(err.Error(), CampoRaw) {
		t.Fatalf("a mensagem precisa nomear %q: %v", CampoRaw, err)
	}
}

func TestDerivaHistoricoNomeiaATabelaIrma(t *testing.T) {
	h := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n").DerivaHistorico()
	if h.Name != "events_history" {
		t.Fatalf("Name = %q, quer %q", h.Name, "events_history")
	}
	if !h.EhHistorico {
		t.Fatal("EhHistorico = false")
	}
}

func TestSemBlocoHistoricoDerivaDevolveNil(t *testing.T) {
	if h := specComHistorico(t, "").DerivaHistorico(); h != nil {
		t.Fatalf("DerivaHistorico = %+v, quer nil", h)
	}
}

// Sem key não há primary, e sem primary não há dedup — que é a definição da
// tabela. A sequence some pela mesma razão: não há estado a sobrescrever.
func TestDerivaHistoricoNaoTemKeyNemSequence(t *testing.T) {
	h := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n").DerivaHistorico()
	if h.Materialization.Key.Field != "" {
		t.Errorf("Key.Field = %q, quer vazio", h.Materialization.Key.Field)
	}
	if h.Materialization.Sequence != nil {
		t.Errorf("Sequence = %+v, quer nil", h.Materialization.Sequence)
	}
	if len(h.Materialization.RegrasTTL) != 0 {
		t.Errorf("RegrasTTL = %+v, quer vazio", h.Materialization.RegrasTTL)
	}
}

func TestDerivaHistoricoNaoTemColunasCongeladas(t *testing.T) {
	h := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n").DerivaHistorico()
	if _, tem := h.Schema.Fields["originalSeverity"]; tem {
		t.Fatal("a congelada sobreviveu à derivação")
	}
	if _, tem := h.Schema.Fields["severity"]; !tem {
		t.Fatal("a ORIGEM da congelada foi removida junto")
	}
}

// O bitmap custa (distintos × rows / 8), e o histórico tem muito mais rows que
// o base. Herdar o índice do base multiplicaria esse custo sem ninguém pedir.
func TestDerivaHistoricoNaoHerdaIndiceDoBase(t *testing.T) {
	h := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n").DerivaHistorico()
	if idx := h.Schema.Fields["node"].Index; idx != "" {
		t.Fatalf("node.Index = %q no histórico, quer vazio", idx)
	}
}

func TestDerivaHistoricoAplicaOsIndicesDeclarados(t *testing.T) {
	h := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n  fields:\n"+
		"    identifier:\n      index: hash\n    severity:\n      index: bitmap\n").DerivaHistorico()
	if idx := h.Schema.Fields["identifier"].Index; idx != "hash" {
		t.Errorf("identifier.Index = %q, quer hash", idx)
	}
	if idx := h.Schema.Fields["severity"].Index; idx != "bitmap" {
		t.Errorf("severity.Index = %q, quer bitmap", idx)
	}
}

func TestDerivaHistoricoAcrescentaRawQuandoPedido(t *testing.T) {
	sem := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n").DerivaHistorico()
	if _, tem := sem.Schema.Fields[CampoRaw]; tem {
		t.Errorf("%s apareceu sem rawPayload", CampoRaw)
	}
	com := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n  rawPayload: true\n").DerivaHistorico()
	f, tem := com.Schema.Fields[CampoRaw]
	if !tem {
		t.Fatalf("%s não foi acrescentado", CampoRaw)
	}
	if f.Type != "json" {
		t.Fatalf("%s.Type = %q, quer json", CampoRaw, f.Type)
	}
}

// A retenção por tempo lê o _writtenAt de cada linha; sem ele o motor não tem
// contra o que comparar.
func TestHistoricoComPrazoExigeOInstanteDeEscrita(t *testing.T) {
	s := specComHistorico(t, "history:\n  expireAfterWrite: 7d\n")
	if !s.ExigeInstanteDeEscrita() {
		t.Fatal("ExigeInstanteDeEscrita = false com expireAfterWrite declarado")
	}
	if !s.DerivaHistorico().ExigeInstanteDeEscrita() {
		t.Fatal("a spec derivada não exige o instante de escrita")
	}
}

func TestExpireAfterWriteDoHistoricoEhParseado(t *testing.T) {
	s := specComHistorico(t, "history:\n  expireAfterWrite: 3d\n")
	if got := s.Historico.ExpireAfterWrite; got != 72*time.Hour {
		t.Fatalf("ExpireAfterWrite = %s, quer 72h", got)
	}
}

// compilaPar compila o base e o histórico contra o MESMO dicionário, que é
// como o servidor os abre. É o dicionário compartilhado que faz os FieldIDs
// coincidirem — sem ele o payload escrito pelo patch do base não decodificaria
// do outro lado.
func compilaPar(t *testing.T, historico string) (base, hist *Compiled) {
	t.Helper()
	s := specComHistorico(t, historico)
	d := newFakeDict()
	base, err := s.Compile(d)
	if err != nil {
		t.Fatalf("compilando o base: %v", err)
	}
	hist, err = s.DerivaHistorico().Compile(d)
	if err != nil {
		t.Fatalf("compilando o histórico: %v", err)
	}
	return base, hist
}

func TestOHistoricoCompartilhaOsFieldIDsDoBase(t *testing.T) {
	base, hist := compilaPar(t, "history:\n  expireAfterWrite: 7d\n")
	for _, nome := range []string{"identifier", "node", "severity", "latencia", "doc", CampoInstanteDeEscrita} {
		bf, ok := base.FieldByName([]byte(nome))
		if !ok {
			t.Fatalf("o base não tem %q", nome)
		}
		hf, ok := hist.FieldByName([]byte(nome))
		if !ok {
			t.Fatalf("o histórico não tem %q", nome)
		}
		if bf.ID != hf.ID {
			t.Errorf("%s: FieldID %d no base e %d no histórico", nome, bf.ID, hf.ID)
		}
		if bf.Type != hf.Type {
			t.Errorf("%s: tipo %v no base e %v no histórico", nome, bf.Type, hf.Type)
		}
	}
}

// Sem key não há primary, e é o Key nil que impede o planner de eleger o point
// lookup sobre um Store que não tem contra o que resolvê-lo.
func TestOCompiladoDoHistoricoNaoTemKeyNemSequence(t *testing.T) {
	_, hist := compilaPar(t, "history:\n  expireAfterWrite: 7d\n")
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
	_, hist := compilaPar(t, "history:\n  expireAfterWrite: 7d\n")
	if hist.Escrita == nil {
		t.Error("o histórico não compilou o instante de escrita")
	}
	if hist.Prazo != nil {
		t.Error("o histórico compilou o campo de prazo por linha")
	}
}

// Ligar o histórico com prazo num dataset SEM regras de TTL tem de acrescentar
// o _writtenAt ao BASE também: é ele quem grava o campo, no patch.
func TestBaseSemTTLGanhaOInstanteDeEscritaQuandoOHistoricoExpira(t *testing.T) {
	base, _ := compilaPar(t, "history:\n  expireAfterWrite: 7d\n")
	if base.Escrita == nil {
		t.Fatal("o base não compilou o instante de escrita")
	}
	if base.Prazo != nil {
		t.Error("o base compilou o campo de prazo sem declarar regras de TTL")
	}
}

// Histórico só por tamanho não precisa do instante de escrita, e acrescentá-lo
// custaria 8 bytes por versão de record para nada.
func TestHistoricoSoPorTamanhoNaoExigeOInstanteDeEscrita(t *testing.T) {
	s := specComHistorico(t, "history:\n  maxBytes: 4GiB\n")
	if s.ExigeInstanteDeEscrita() {
		t.Fatal("ExigeInstanteDeEscrita = true sem expireAfterWrite")
	}
}

func TestOHistoricoNaoCompilaAsCongeladas(t *testing.T) {
	base, hist := compilaPar(t, "history:\n  expireAfterWrite: 7d\n")
	if _, ok := base.FieldByName([]byte("originalSeverity")); !ok {
		t.Fatal("a congelada não está no base — a fixture perdeu o caso")
	}
	if _, ok := hist.FieldByName([]byte("originalSeverity")); ok {
		t.Fatal("a congelada foi compilada no histórico")
	}
}

func TestOsIndicesDoHistoricoChegamAoCompilado(t *testing.T) {
	base, hist := compilaPar(t, "history:\n  expireAfterWrite: 7d\n  fields:\n"+
		"    identifier:\n      index: hash\n")
	hf, _ := hist.FieldByName([]byte("identifier"))
	if hf.Index != IndexHash {
		t.Errorf("identifier.Index = %v no histórico, quer hash", hf.Index)
	}
	// E o node, que é hash no BASE, não herda nada.
	bn, _ := base.FieldByName([]byte("node"))
	if bn.Index != IndexHash {
		t.Fatal("a fixture perdeu o índice do base")
	}
	hn, _ := hist.FieldByName([]byte("node"))
	if hn.Index != IndexNone {
		t.Errorf("node.Index = %v no histórico, quer nenhum", hn.Index)
	}
}
