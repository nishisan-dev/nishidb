package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const extractYAML = `version: 1
name: eventos
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
        - path: frontAlarmId
          type: string
`

func compileExtract(t *testing.T, yml string) *Compiled {
	t.Helper()
	s := parseOK(t, yml)
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return c
}

// Chave de dentro do json vira COLUNA DE VERDADE: com FieldID próprio, tipo
// declarado e comparável. É isso que a torna consultável — coluna json não é
// comparável, e o binder recusa.
func TestExtractViraColunaDeVerdade(t *testing.T) {
	c := compileExtract(t, extractYAML)
	f, ok := c.FieldByName([]byte("probe_ip"))
	if !ok {
		t.Fatal("probe_ip não foi compilado")
	}
	if f.Type != types.TypeString {
		t.Errorf("probe_ip.Type = %v, quer string", f.Type)
	}
	if f.ID == 0 {
		t.Error("probe_ip sem FieldID próprio")
	}
	if f.Parent == nil || f.Parent.Name != "doc" {
		t.Errorf("probe_ip.Parent = %v, quer doc", f.Parent)
	}
	if f.Path != "probeIpNode" {
		t.Errorf("probe_ip.Path = %q, quer probeIpNode", f.Path)
	}
}

// `as` omitido vale o próprio path, que é o caso comum e não merece
// cerimônia. Só o path com ponto exige o nome explícito — ver a recusa abaixo.
func TestAsOmitidoValeOPath(t *testing.T) {
	c := compileExtract(t, extractYAML)
	if _, ok := c.FieldByName([]byte("frontAlarmId")); !ok {
		t.Fatal("frontAlarmId não foi compilado com o nome do path")
	}
}

// O documento json ORIGINAL continua inteiro na coluna dele. Extrair não é
// mover: nada se perde, e a aplicação que já lia o documento continua lendo.
func TestOPaiContinuaSendoColuna(t *testing.T) {
	c := compileExtract(t, extractYAML)
	f, ok := c.FieldByName([]byte("doc"))
	if !ok {
		t.Fatal("o campo json pai sumiu")
	}
	if f.Type != types.TypeJSON {
		t.Errorf("doc.Type = %v, quer json", f.Type)
	}
	if f.Parent != nil {
		t.Error("o pai não pode ter pai")
	}
}

// SELECT * devolve as colunas de topo e NÃO as derivadas. Sem isto o dataset
// do piloto nasceria com centenas de colunas por linha, e o custo de fio
// passaria a ser proporcional ao dicionário em vez de ao que foi pedido.
func TestDerivadaNaoEntraNoSelectEstrela(t *testing.T) {
	c := compileExtract(t, extractYAML)
	for _, f := range c.FieldsByID() {
		if f.Parent != nil {
			t.Errorf("coluna derivada %q apareceu no SELECT *", f.Name)
		}
	}
	// ...mas ela existe, e é projetável pelo nome.
	if _, ok := c.FieldByName([]byte("probe_ip")); !ok {
		t.Error("a derivada sumiu do universo de campos")
	}
}

// O metadata é SUPERSET do SELECT *: esconder a derivada ali faria o DBeaver
// não mostrá-la, e ninguém descobriria que ela existe.
func TestDerivadaApareceNoMetadata(t *testing.T) {
	c := compileExtract(t, extractYAML)
	var found bool
	for _, f := range c.FieldsForMetadata() {
		if f.Name == "probe_ip" {
			found = true
		}
	}
	if !found {
		t.Error("probe_ip não apareceu no conjunto de metadata")
	}
}

// FieldsByParent é o que o `SELECT doc.*` vai expandir.
func TestFieldsByParentDevolveAsDerivadas(t *testing.T) {
	c := compileExtract(t, extractYAML)
	got := c.FieldsByParent("doc")
	if len(got) != 2 {
		t.Fatalf("FieldsByParent(doc) = %d colunas, quer 2", len(got))
	}
	if got[0].ID > got[1].ID {
		t.Error("as derivadas precisam sair em ordem de FieldID, como o SELECT *")
	}
	if n := len(c.FieldsByParent("id")); n != 0 {
		t.Errorf("FieldsByParent de campo sem derivadas = %d, quer 0", n)
	}
}

func TestExtractRecusas(t *testing.T) {
	cases := []struct {
		name string
		yml  string
		want string
	}{
		{
			"extract sobre campo que nao e json",
			strings.Replace(extractYAML, "    doc:\n      type: json", "    doc:\n      type: string", 1),
			"json",
		},
		{
			"path com ponto sem as",
			strings.Replace(extractYAML, "        - path: frontAlarmId\n          type: string\n",
				"        - path: a.b\n          type: string\n", 1),
			"as",
		},
		{
			"as com ponto",
			strings.Replace(extractYAML, "          as: probe_ip", "          as: doc.probe", 1),
			"ponto",
		},
		{
			"as colide com campo de topo",
			strings.Replace(extractYAML, "          as: probe_ip", "          as: id", 1),
			"id",
		},
		{
			"as duplicado entre extracoes",
			strings.Replace(extractYAML, "        - path: frontAlarmId\n          type: string\n",
				"        - path: outro\n          as: probe_ip\n          type: string\n", 1),
			"probe_ip",
		},
		{
			"path vazio",
			strings.Replace(extractYAML, "        - path: probeIpNode", "        - path: \"\"", 1),
			"path",
		},
		{
			"tipo invalido na derivada",
			strings.Replace(extractYAML, "          as: probe_ip\n          type: string", "          as: probe_ip\n          type: float", 1),
			"float",
		},
		{
			// ipv4 SAIU deste caso: com um índice só (D5), ipv4 é indexável —
			// era o hashIndexable que já o aceitava, e a união com bitmapIndexable
			// não perde nada que qualquer um dos dois já tinha. double continua
			// fora dos dois, então é ele que prova a recusa por tipo.
			"index em tipo que nao aceita",
			strings.Replace(extractYAML, "          as: probe_ip\n          type: string",
				"          as: probe_ip\n          type: double\n          index: true", 1),
			"não suporta",
		},
		{
			"pai excluido",
			strings.Replace(extractYAML, "  unknownFields: include", "  exclude:\n    - doc\n  unknownFields: include", 1),
			"exclude",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parseErr(t, tc.yml, tc.want)
		})
	}
}

// A derivada não pode adotar o histórico de outra coluna.
//
// O caso perigoso é um campo de topo AUTODESCOBERTO com o mesmo nome, já
// observado numa execução anterior: o LookupOrCreate devolveria o ID
// existente, e a coluna passaria a ler valores que nunca foram dela. O que
// torna isso detectável é o TIPO fixado — se o dicionário guarda um tipo e a
// declaração pede outro, houve adoção.
//
// Quando não há tipo fixado, também não há valor materializado, e adotar o ID
// é inofensivo. É por isso que a checagem é sobre o hint e não sobre a mera
// presença do nome: esta última recusaria todo REOPEN de um dataset criado
// com extract, onde a derivada está no dicionário por direito.
func TestDerivadaComTipoDivergenteDoFixadoERecusada(t *testing.T) {
	d := newFakeDict()
	id, err := d.LookupOrCreate([]byte("probe_ip"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
		t.Fatal(err)
	}
	s := parseOK(t, extractYAML)
	_, err = s.Compile(d)
	if !errors.Is(err, ErrDerivedColumn) {
		t.Fatalf("Compile = %v, quer ErrDerivedColumn", err)
	}
	if !strings.Contains(err.Error(), "int64") {
		t.Errorf("erro = %q, quer nomear o tipo que o dataset já fixou", err)
	}
}

// O reopen de um dataset criado COM extract encontra a derivada já no
// dicionário, e isso é o caso normal — não uma colisão. Ela precisa voltar
// como derivada, com o pai no lugar, e não ressuscitar como campo de topo
// autodescoberto pela rota de carregamento do dicionário.
func TestDerivadaSobreviveAoReopenSemVirarCampoDeTopo(t *testing.T) {
	d := newFakeDict()
	if _, err := parseOK(t, extractYAML).Compile(d); err != nil {
		t.Fatal(err)
	}
	c, err := parseOK(t, extractYAML).Compile(d)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	f, ok := c.FieldByName([]byte("probe_ip"))
	if !ok {
		t.Fatal("probe_ip sumiu no reopen")
	}
	if f.Parent == nil {
		t.Error("probe_ip voltou como campo de topo; ela adotaria o SELECT * junto")
	}
	if f.IsAuto {
		t.Error("probe_ip voltou como auto; o tipo dela é declarado")
	}
	for _, g := range c.FieldsByID() {
		if g.Name == "probe_ip" {
			t.Error("a derivada apareceu no SELECT * depois do reopen")
		}
	}
}

// IndexedFieldNames é o ÚNICO lugar que decide quais campos o Store abre com
// índice. Existia como um laço copiado em quatro sítios — servidor, duas rotas
// do CLI e o benchmark —, e nenhum deles conhecia a coluna derivada: o
// servidor criaria o dataset com o índice, e o `nishidb query` seguinte
// recusaria abrir por divergência com o manifest.
//
// A função chamava-se CamposComBitmap, e tinha uma gêmea, CamposComHash, com um
// arquivo de teste próprio — campos_hash_test.go, com TestCamposComHashInclui-
// ADerivada, TestCamposComHashVazioSemIndice e TestCamposComBitmapEComHash-
// DevolvemOMesmoConjunto. As duas liam a MESMA chave do YAML desde a D5 e
// colapsaram numa só quando storage.Options unificou o par de campos por motor;
// o arquivo saiu junto, porque os três casos dele são os dois daqui campo por
// campo — a derivada indexada, a lista vazia e a ordem alfabética entre dois
// campos de topo. O NOME destes dois testes fica: ele diz de qual das duas
// gêmeas esta cobertura veio.
func TestCamposComBitmapIncluiADerivada(t *testing.T) {
	yml := strings.Replace(extractYAML,
		"          as: probe_ip\n          type: string",
		"          as: probe_ip\n          type: string\n          index: true", 1)
	yml = strings.Replace(yml, "    id:\n      type: string",
		"    id:\n      type: string\n    status:\n      type: string\n      index: true", 1)
	s := parseOK(t, yml)

	got := IndexedFieldNames(s)
	want := []string{"probe_ip", "status"} // ordenado
	if len(got) != len(want) {
		t.Fatalf("IndexedFieldNames = %v, quer %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IndexedFieldNames = %v, quer %v (ordem é contrato: ela decide o FieldID de um dataset novo)", got, want)
		}
	}
}

func TestCamposComBitmapVazioSemIndice(t *testing.T) {
	if got := IndexedFieldNames(parseOK(t, extractYAML)); len(got) != 0 {
		t.Errorf("IndexedFieldNames = %v, quer vazio", got)
	}
}

const discoverYAML = `version: 1
name: eventos
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      unknownFields: include
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
`

// `unknownFields: include` no CAMPO descobre as chaves de dentro do documento
// como colunas, do mesmo jeito que a chave homônima do bloco `schema` descobre
// campos de topo. É deliberadamente a mesma palavra: o operador já sabe o que
// ela significa um nível acima.
func TestUnknownFieldsNoCampoLigaADescoberta(t *testing.T) {
	c := compileExtract(t, discoverYAML)
	doc, ok := c.FieldByName([]byte("doc"))
	if !ok {
		t.Fatal("doc não compilou")
	}
	if !doc.DiscoverNested {
		t.Error("doc.DiscoverNested = false; a chave no campo não ligou a descoberta")
	}
}

// Sem a chave, o campo json continua opaco — o comportamento de antes.
func TestSemAChaveOCampoJsonContinuaOpaco(t *testing.T) {
	c := compileExtract(t, extractYAML)
	doc, _ := c.FieldByName([]byte("doc"))
	if doc.DiscoverNested {
		t.Error("descoberta ligada sem a chave ter sido declarada")
	}
}

func TestRecusasDaDescoberta(t *testing.T) {
	cases := []struct{ name, yml, want string }{
		{
			"em campo que nao e json",
			strings.Replace(discoverYAML, "    doc:\n      type: json", "    doc:\n      type: string", 1),
			"json",
		},
		{
			"valor invalido",
			strings.Replace(discoverYAML, "      unknownFields: include", "      unknownFields: talvez", 1),
			"unknownFields",
		},
		{
			"reject nao faz sentido dentro do documento",
			strings.Replace(discoverYAML, "      unknownFields: include", "      unknownFields: reject", 1),
			"reject",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { parseErr(t, tc.yml, tc.want) })
	}
}

// A coluna DESCOBERTA precisa voltar como derivada depois do restart.
//
// Sem isto ela ressuscita pela rota de carregamento do dicionário como campo
// de TOPO autodescoberto — e aí volta para o `SELECT *`, que é exatamente o
// que a decisão de mantê-la fora existe para evitar: o dataset do piloto
// nasceria com centenas de colunas por linha na primeira reabertura.
//
// O que identifica a coluna é o próprio NOME: ele é qualificado por
// construção, e o prefixo antes do ponto é um campo json com descoberta
// ligada. Não é preciso estado durável novo — o nome já é o registro.
func TestColunaDescobertaVoltaComoDerivadaNoReopen(t *testing.T) {
	d := newFakeDict()
	if _, err := parseOK(t, discoverYAML).Compile(d); err != nil {
		t.Fatal(err)
	}
	// Simula o que a ingestão faria: registra colunas descobertas.
	for _, name := range []string{"doc.tlCause", "doc.alarme"} {
		if _, err := d.LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}

	c, err := parseOK(t, discoverYAML).Compile(d)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	f, ok := c.FieldByName([]byte("doc.tlCause"))
	if !ok {
		t.Fatal("doc.tlCause sumiu no reopen")
	}
	if f.Parent == nil || f.Parent.Name != "doc" {
		t.Fatalf("doc.tlCause.Parent = %v, quer doc", f.Parent)
	}
	if f.Path != "tlCause" {
		t.Errorf("doc.tlCause.Path = %q, quer tlCause", f.Path)
	}
	for _, g := range c.FieldsByID() {
		if g.Parent == nil && g.Name != "id" && g.Name != "doc" {
			t.Errorf("coluna descoberta %q voltou para o SELECT * no reopen", g.Name)
		}
	}
	if n := len(c.FieldsByParent("doc")); n != 3 { // as duas descobertas + probe_ip declarada
		t.Errorf("FieldsByParent(doc) = %d, quer 3", n)
	}
}

// O ponto no nome só significa "aninhada" quando o prefixo é um pai que
// DESCOBRE. Num dataset sem descoberta ligada, um campo de topo cujo nome
// tenha ponto continua sendo de topo.
func TestNomeComPontoSemPaiQueDescobreContinuaDeTopo(t *testing.T) {
	d := newFakeDict()
	if _, err := parseOK(t, extractYAML).Compile(d); err != nil {
		t.Fatal(err)
	}
	if _, err := d.LookupOrCreate([]byte("doc.tlCause")); err != nil {
		t.Fatal(err)
	}
	c, err := parseOK(t, extractYAML).Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := c.FieldByName([]byte("doc.tlCause"))
	if f != nil && f.Parent != nil {
		t.Error("campo virou derivado num dataset que não declara descoberta")
	}
}

// O descarte por teto precisa aparecer no LOG, e não só no contador: quem
// opera descobre pelo painel que o número subiu, mas o log é o que diz QUAL
// campo foi o primeiro a não caber — o resto se investiga a partir dele.
//
// Uma vez por dataset, e não por evento: sob a política `ignore` o descarte
// passa a ser o estado normal, e uma linha por evento afogaria o log num
// dataset que ingere a 500 ev/s.
func TestDescarteAvisaUmaVezPorDataset(t *testing.T) {
	s := parseOK(t, ceilingYAML(2, "ignore"))
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	var warnings []string
	c.OnFieldDiscarded(func(field string) { warnings = append(warnings, field) })

	if _, err := c.ResolveUnknown([]byte("cabe")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"primeiroExcedente", "segundo", "terceiro"} {
		if _, err := c.ResolveUnknown([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	if len(warnings) != 1 {
		t.Fatalf("avisos = %v, quer exatamente um", warnings)
	}
	if warnings[0] != "primeiroExcedente" {
		t.Errorf("aviso = %q, quer o PRIMEIRO campo que não coube", warnings[0])
	}
	if c.DiscardedFieldCount() != 3 {
		t.Errorf("contador = %d, quer 3 — o log é uma vez, a contagem é toda", c.DiscardedFieldCount())
	}
}
