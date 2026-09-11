package materializer

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// Evento RADIUS representativo (~30 campos): Extended JSON em três shapes,
// o campo excluído do YAML, desconhecidos que viram auto no warm-up
// (wildcard + unknownFields: include) e escapes REAIS espalhados pelo meio
// do evento — aspas, \n e solidus em Class/NAS-Port-Id, e o trecho
// interpretado abaixo com \u e surrogate pair em User-Name.
//
// Os escapes são o que faz o gate valer alguma coisa: sem eles o tokenizer
// nunca entra no caminho do scratch, que é justamente de onde uma alocação
// escaparia sem ser notada (o Convert copia dali para o arena).
var benchRadiusEvent = []byte(`{"id":"CMP-V0001PPCLP-013","_id":"68992d1a77b6f2618d867d43","_SEQ":{"$numberLong":"7455409733"},"Acct-Authentic":"RADIUS","Acct-Session-Id":"D0D94381DB27B076","Acct-Status-Type":"Interim-Update","Calling-Station-Id":"B4:0F:3B:4E:8F:AA","Class":"srv=hsi&prof=200M \"tag\"\nfim","Connect-Info":1000000,"Event-Timestamp":{"$date":"2026-08-11T22:28:10.000Z"},"EventExpireDate":{"$date":{"$numberLong":"1786399690000"}},"Filter-Id":"filter-hsi-200","Framed-IP-Address":"10.65.223.104","Framed-Protocol":"PPP","Idle-Timeout":300,"NAS-IP-Address":"189.40.10.7","NAS-Identifier":"i-br-sp-cmp-cmp-hl4-01","NAS-Port":1063256064,"NAS-Port-Id":"ethernet1\/1\/1:100.200","NAS-Port-Type":"Ethernet","Service-Type":"Framed-User","Session-Timeout":86400,` +
	"\"User-Name\":\"jo\\u00e3o@isp.net.br \\ud83d\\ude00\"," +
	`"Acct-Input-Gigawords":2,"Acct-Input-Octets":66952,"Acct-Input-Packets":48211,"Acct-Output-Gigawords":11,"Acct-Output-Octets":998231,"Acct-Output-Packets":881237,"Acct-Session-Time":73210,"roaming":false,"debug":null}`)

// Gate da Q8: 0 allocs/op no Parse de evento RADIUS em steady state
// (todos os campos já resolvidos no schema/cache).
func BenchmarkPatchParseRadiusEvent(b *testing.B) {
	s, err := schema.Parse([]byte(patchYAML), schema.BuiltinDefaults)
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		b.Fatal(err)
	}
	var pb PatchBuffer
	// Warm-up: registra os campos desconhecidos do evento (_id, Class,
	// roaming e companhia) no dicionário, para medir o steady state.
	if err := pb.Parse(benchRadiusEvent, c, time.Now()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusEvent)))
	for b.Loop() {
		if err := pb.Parse(benchRadiusEvent, c, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

// benchDescobertaYAML tem um campo json com descoberta ligada e NENHUM extract
// declarado — a forma exata do dataset do piloto, onde `persistent` é
// `type: json` + `unknownFields: include` e mais nada.
//
// Isso é o ponto desta bancada. O YAML dos benchmarks existentes
// (testdata/radius.materialization.yaml) não tem um único campo `type: json`,
// então nenhuma medição de vazão do projeto exercitava a segunda passada:
// nem a busca por caminho, nem a descoberta, nem a resolução de tipo. O
// caminho mais quente do dataset real era o único sem instrumento.
const benchDescobertaYAML = `version: 1
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
    persistent:
      type: json
      unknownFields: include
`

// Documento de 11 chaves, que é a ordem de grandeza medida no piloto, com os
// três tipos que aparecem lá (texto, inteiro, booleano) e um objeto aninhado —
// que a descoberta de UM nível transforma em coluna json inteira, e é um custo
// diferente de copiar um escalar.
var benchEventWithDocumento = []byte(`{"id":"CMP-V0001PPCLP-013","severity":3,"node":"i-br-sp-cmp-cmp-hl4-01",` +
	`"persistent":{"dataFalha":"2026-08-11 22:28:10","dataClear":"2026-08-11 23:01:44","curtoTally":4,` +
	`"curtoTTL":300,"longoTally":11,"longoTTL":86400,"medioTally":7,"medioTTL":3600,` +
	`"slot":"1/1/1","frontPersistence":"on","frontPodName":"pod-alarme-07",` +
	`"origem":{"host":"nms-03","porta":9142}}}`)

// BenchmarkParseComDescoberta mede a segunda passada em REGIME — com todas as
// colunas do documento já registradas no dicionário e com o tipo já fixado.
//
// É a linha de base contra a qual o contador por coluna do bloco seguinte
// precisa ser medido: ele incrementaria uma vez por coluna derivada por evento,
// no caminho mais quente do produto, e a promessa de que o custo "sai de
// medição, não de raciocínio" não tinha como ser cumprida sem esta bancada.
//
// O gate de alocação do pacote cobre alocação e NÃO cobre CPU; esta cobre CPU e
// reporta alocação junto, para que uma regressão em qualquer das duas apareça.
func BenchmarkParseComDescoberta(b *testing.B) {
	s, err := schema.Parse([]byte(benchDescobertaYAML), schema.BuiltinDefaults)
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		b.Fatal(err)
	}
	var pb PatchBuffer
	// Warm-up: registra as 11 colunas derivadas e fixa o tipo de cada uma.
	// Sem ele a bancada mediria a DESCOBERTA (uma vez na vida do dataset) em
	// vez do regime (todo evento), que é o custo que interessa.
	if err := pb.Parse(benchEventWithDocumento, c, time.Now()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchEventWithDocumento)))
	for b.Loop() {
		if err := pb.Parse(benchEventWithDocumento, c, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseSemDescoberta é o MESMO evento contra um schema em que
// `persistent` é json opaco — sem descoberta, sem extract.
//
// Existe para que o custo da segunda passada seja legível por SUBTRAÇÃO. Sem
// ele, o número da bancada acima é um absoluto que não diz quanto do tempo é o
// documento sendo percorrido e quanto é o resto do evento.
func BenchmarkParseSemDescoberta(b *testing.B) {
	yaml := strings.Replace(benchDescobertaYAML,
		"    persistent:\n      type: json\n      unknownFields: include\n",
		"    persistent:\n      type: json\n", 1)
	s, err := schema.Parse([]byte(yaml), schema.BuiltinDefaults)
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		b.Fatal(err)
	}
	var pb PatchBuffer
	if err := pb.Parse(benchEventWithDocumento, c, time.Now()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchEventWithDocumento)))
	for b.Loop() {
		if err := pb.Parse(benchEventWithDocumento, c, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
