package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const extractYAML = `version: 1
name: eventos
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

func compilaExtract(t *testing.T, yml string) *Compiled {
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
	c := compilaExtract(t, extractYAML)
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
	c := compilaExtract(t, extractYAML)
	if _, ok := c.FieldByName([]byte("frontAlarmId")); !ok {
		t.Fatal("frontAlarmId não foi compilado com o nome do path")
	}
}

// O documento json ORIGINAL continua inteiro na coluna dele. Extrair não é
// mover: nada se perde, e a aplicação que já lia o documento continua lendo.
func TestOPaiContinuaSendoColuna(t *testing.T) {
	c := compilaExtract(t, extractYAML)
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
	c := compilaExtract(t, extractYAML)
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
	c := compilaExtract(t, extractYAML)
	var achou bool
	for _, f := range c.FieldsParaMetadata() {
		if f.Name == "probe_ip" {
			achou = true
		}
	}
	if !achou {
		t.Error("probe_ip não apareceu no conjunto de metadata")
	}
}

// FieldsByParent é o que o `SELECT doc.*` vai expandir.
func TestFieldsByParentDevolveAsDerivadas(t *testing.T) {
	c := compilaExtract(t, extractYAML)
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
	casos := []struct {
		nome string
		yml  string
		quer string
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
			"bitmap em tipo que nao aceita",
			strings.Replace(extractYAML, "          as: probe_ip\n          type: string",
				"          as: probe_ip\n          type: ipv4\n          index: bitmap", 1),
			"bitmap",
		},
		{
			"pai excluido",
			strings.Replace(extractYAML, "  unknownFields: include", "  exclude:\n    - doc\n  unknownFields: include", 1),
			"exclude",
		},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			parseErr(t, tc.yml, tc.quer)
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
	if !errors.Is(err, ErrColunaDerivada) {
		t.Fatalf("Compile = %v, quer ErrColunaDerivada", err)
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

// Declarar extract num dataset que JÁ TEM linhas é recusado.
//
// As linhas antigas não têm a coluna derivada — a extração roda na ingestão, e
// elas já passaram por ela. `WHERE derivada = 'x'` omitiria todas elas sem
// erro, sem log e sem métrica: resultado incompleto com cara de completo, que
// é o defeito que o produto já recusa na abertura para o caso do índice.
//
// A consequência é que extract vira decisão de CRIAÇÃO de dataset, na mesma
// doutrina que já vale para mudança de tipo.
func TestExtractEmDatasetComLinhasERecusado(t *testing.T) {
	s := parseOK(t, extractYAML)
	if err := s.ValidaContraDatasetExistente(0); err != nil {
		t.Errorf("dataset vazio devia aceitar: %v", err)
	}
	err := s.ValidaContraDatasetExistente(1)
	if !errors.Is(err, ErrColunaDerivada) {
		t.Fatalf("dataset com linhas = %v, quer ErrColunaDerivada", err)
	}
	for _, quer := range []string{"probe_ip", "recrie"} {
		if !strings.Contains(err.Error(), quer) {
			t.Errorf("erro = %q, quer conter %q", err, quer)
		}
	}
}

// Sem extract declarado, o dataset com linhas abre normalmente.
func TestSemExtractODatasetComLinhasAbre(t *testing.T) {
	s := parseOK(t, baseYAML)
	if err := s.ValidaContraDatasetExistente(1_000_000); err != nil {
		t.Errorf("dataset sem extract devia abrir: %v", err)
	}
}

// CamposComBitmap é o ÚNICO lugar que decide quais campos o Store abre com
// índice. Existia como um laço copiado em quatro sítios — servidor, duas rotas
// do CLI e o benchmark —, e nenhum deles conhecia a coluna derivada: o
// servidor criaria o dataset com o índice, e o `nishidb query` seguinte
// recusaria abrir por divergência com o manifest.
func TestCamposComBitmapIncluiADerivada(t *testing.T) {
	yml := strings.Replace(extractYAML,
		"          as: probe_ip\n          type: string",
		"          as: probe_ip\n          type: string\n          index: bitmap", 1)
	yml = strings.Replace(yml, "    id:\n      type: string",
		"    id:\n      type: string\n    status:\n      type: string\n      index: bitmap", 1)
	s := parseOK(t, yml)

	got := CamposComBitmap(s)
	quer := []string{"probe_ip", "status"} // ordenado
	if len(got) != len(quer) {
		t.Fatalf("CamposComBitmap = %v, quer %v", got, quer)
	}
	for i := range quer {
		if got[i] != quer[i] {
			t.Fatalf("CamposComBitmap = %v, quer %v (ordem é contrato: ela decide o FieldID de um dataset novo)", got, quer)
		}
	}
}

func TestCamposComBitmapVazioSemIndice(t *testing.T) {
	if got := CamposComBitmap(parseOK(t, extractYAML)); len(got) != 0 {
		t.Errorf("CamposComBitmap = %v, quer vazio", got)
	}
}
