// Package radiusgen gera eventos de accounting RADIUS sintéticos e
// DETERMINÍSTICOS, no formato da amostra real de produção
// (testdata/radius_event_sample.json) e compatíveis com o YAML da spec §12.
//
// Existe para alimentar os harnesses de benchmark (a rodada de 8M entidades,
// §108-111) e os micro-benchmarks, sem depender de um dump real. Duas
// propriedades tornam a medição comparável entre rodadas:
//
//   - mesma Config produz o mesmo corpus byte a byte — uma regressão medida
//     entre duas rodadas é da engine, não do corpus;
//   - Next não aloca em regime — alocação do gerador entraria na conta de
//     allocs/evento do harness e falsearia justamente o número que a spec
//     §111 cobra.
//
// O gerador NÃO controla taxa: eventos/s é do harness (rate limiter), não
// daqui.
package radiusgen

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
)

// Config parametriza o corpus.
//
// Os RATIOS valem exatamente o que está escrito: zero é zero, não "use o
// default". Sem isso o harness não conseguiria isolar uma dimensão de cada
// vez — "medir sem Extended JSON" é um pedido legítimo, e um default
// silencioso o transformaria em "medir com 10%". Para o workload de
// referência use DefaultConfig().
//
// Os campos de CARDINALIDADE (Entities, StatusValues, NASCount,
// TerminateCauses) tratam zero como default, porque zero ali não descreve
// nenhum corpus: seria um gerador sem entidades ou sem vocabulário.
type Config struct {
	Seed     uint64 // semente do PCG; 0 é uma semente válida
	Entities int    // entidades distintas (0 → 10_000)

	PatchRatio        float64 // eventos que são patch parcial (referência 0.85)
	ExtendedJSONRatio float64 // eventos com wrappers Mongo (referência 0.10)
	UnknownFieldRatio float64 // eventos com campo fora do schema (referência 0.01)
	NullFieldRatio    float64 // patches com um campo null explícito (referência 0.05)

	StatusValues    int // valores distintos de Acct-Status-Type (0 → 8)
	NASCount        int // NAS distintos (0 → 300)
	TerminateCauses int // valores de Acct-Terminate-Cause (0 → 10)

	// Sequential percorre as entidades em ordem em vez de sortear.
	//
	// Existe para a fase LOAD do harness: com sorteio uniforme, N eventos
	// tocam só ~63% das N entidades, e criar 8M rows exigiria ~13M eventos —
	// 60% a mais de tempo e de disco na rodada mais cara do projeto. Com
	// Sequential, N eventos criam exatamente N entidades.
	//
	// NÃO use para medir carga de regime: a ordem perfeita elimina a
	// localidade irregular que o índice primary enfrenta na vida real.
	Sequential bool
}

// DefaultConfig é o workload de referência: a carga de accounting real, com
// 85% de interim-update parcial. Ajuste Seed e Entities e passe adiante.
func DefaultConfig() Config {
	return Config{
		Entities:          defaultEntities,
		PatchRatio:        defaultPatchRatio,
		ExtendedJSONRatio: defaultExtendedRatio,
		UnknownFieldRatio: defaultUnknownRatio,
		NullFieldRatio:    defaultNullRatio,
		StatusValues:      defaultStatusValues,
		NASCount:          defaultNASCount,
		TerminateCauses:   defaultTerminateCauses,
	}
}

// Defaults do corpus de referência.
const (
	defaultEntities        = 10_000
	defaultPatchRatio      = 0.85
	defaultExtendedRatio   = 0.10
	defaultUnknownRatio    = 0.01
	defaultNullRatio       = 0.05
	defaultStatusValues    = 8
	defaultNASCount        = 300
	defaultTerminateCauses = 10
)

// seqBase põe a sequence na ordem de grandeza da real (~7,4 bi): o corpus
// precisa exercitar int64 de verdade, não um contador que caberia em int32.
const seqBase int64 = 7_400_000_000

// baseMillis é o instante de referência dos timestamps: 2026-08-12T21:00:00Z,
// o mesmo dia da amostra real. Fixo por construção — usar time.Now() aqui
// quebraria o determinismo e tornaria duas rodadas incomparáveis.
const baseMillis int64 = 1_786_568_400_000

// statusValues, terminateCauses e statusWeights descrevem o vocabulário de
// accounting. Interim-Update domina, como na realidade.
var (
	statusValues = [...]string{
		"Interim-Update", "Alive", "Start", "Stop",
		"Accounting-On", "Accounting-Off", "Failed", "Modify",
	}
	// statusWeights[i] é o peso acumulado de statusValues[i] em 1000.
	statusWeights = [...]int{700, 850, 900, 960, 972, 984, 992, 1000}

	terminateCauses = [...]string{
		"Session-Timeout", "User-Request", "Lost-Carrier", "Idle-Timeout",
		"NAS-Reboot", "Admin-Reset", "Port-Error", "Lost-Service",
		"Admin-Reboot", "NAS-Error",
	}
)

// Generator produz os eventos. NÃO é thread-safe: o rng e os contadores são
// mutados a cada Next (o harness usa um Generator por goroutine produtora).
type Generator struct {
	cfg Config
	rng *rand.Rand

	// seqs[i] é quantos eventos a entidade i já recebeu. uint32 basta para
	// qualquer rodada plausível e custa 4 B por entidade — 32 MiB em 8M, no
	// processo do GERADOR, não na engine. É o preço da monotonicidade por
	// entidade, e está documentado porque aparece no RSS do harness.
	seqs []uint32

	// next é o cursor do modo Sequential.
	next int
}

// New valida a Config, aplica os defaults e devolve o gerador.
//
// Devolve erro em vez de normalizar em silêncio: pedir 150% de Extended JSON
// e receber 30% produziria uma medição que não corresponde ao que o operador
// pediu — e ninguém confere um corpus de 8M eventos a olho.
func New(cfg Config) (*Generator, error) {
	if cfg.Entities < 0 {
		return nil, fmt.Errorf("radiusgen: Entities = %d, deve ser >= 0", cfg.Entities)
	}
	for _, r := range []struct {
		nome string
		v    float64
	}{
		{"PatchRatio", cfg.PatchRatio},
		{"ExtendedJSONRatio", cfg.ExtendedJSONRatio},
		{"UnknownFieldRatio", cfg.UnknownFieldRatio},
		{"NullFieldRatio", cfg.NullFieldRatio},
	} {
		if r.v < 0 || r.v > 1 {
			return nil, fmt.Errorf("radiusgen: %s = %g, deve estar em [0,1]", r.nome, r.v)
		}
	}
	for _, n := range []struct {
		nome string
		v    int
	}{
		{"StatusValues", cfg.StatusValues},
		{"NASCount", cfg.NASCount},
		{"TerminateCauses", cfg.TerminateCauses},
	} {
		if n.v < 0 {
			return nil, fmt.Errorf("radiusgen: %s = %d, deve ser >= 0", n.nome, n.v)
		}
	}
	if cfg.StatusValues > len(statusValues) {
		return nil, fmt.Errorf("radiusgen: StatusValues = %d, o vocabulário tem %d valores",
			cfg.StatusValues, len(statusValues))
	}
	if cfg.TerminateCauses > len(terminateCauses) {
		return nil, fmt.Errorf("radiusgen: TerminateCauses = %d, o vocabulário tem %d valores",
			cfg.TerminateCauses, len(terminateCauses))
	}

	applyDefaults(&cfg)
	return &Generator{
		cfg: cfg,
		// O segundo stream do PCG deriva da seed por golden ratio: seeds
		// vizinhas (1, 2, 3…) produzem sequências descorrelacionadas.
		rng:  rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		seqs: make([]uint32, cfg.Entities),
	}, nil
}

// MustNew é o New para chamador que trata Config inválida como bug de
// programação — testes e benchmarks, onde não há a quem devolver o erro.
func MustNew(cfg Config) *Generator {
	g, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return g
}

// applyDefaults preenche apenas a cardinalidade — os ratios são respeitados
// como vieram (ver o doc de Config).
func applyDefaults(cfg *Config) {
	if cfg.Entities == 0 {
		cfg.Entities = defaultEntities
	}
	if cfg.StatusValues == 0 {
		cfg.StatusValues = defaultStatusValues
	}
	if cfg.NASCount == 0 {
		cfg.NASCount = defaultNASCount
	}
	if cfg.TerminateCauses == 0 {
		cfg.TerminateCauses = defaultTerminateCauses
	}
}

// GenerateCorpus materializa n eventos, cada um em seu próprio slice — para
// micro-benchmarks, onde o custo de gerar precisa ficar FORA do laço medido.
func GenerateCorpus(cfg Config, n int) ([][]byte, error) {
	g, err := New(cfg)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, n)
	for i := range out {
		out[i] = g.Next(nil)
	}
	return out, nil
}

// Next appenda UM evento a dst e devolve o slice resultante. Não aloca em
// regime (o crescimento de dst é do caller, que o reutiliza).
//
// A ordem de consumo do rng é fixa e independente dos ratios — todo sorteio
// acontece sempre, mesmo quando o resultado não é usado. É isso que mantém o
// corpus estável: mudar um ratio muda quais eventos variam, não a sequência
// inteira a partir dali.
func (g *Generator) Next(dst []byte) []byte {
	// O sorteio da entidade acontece SEMPRE, mesmo no modo Sequential, para
	// que os dois modos consumam o rng na mesma ordem — trocar o modo não
	// pode mudar o resto do evento.
	idx := g.rng.IntN(g.cfg.Entities)
	if g.cfg.Sequential {
		idx = g.next % g.cfg.Entities
		g.next++
	}
	rPatch := g.rng.Float64()
	rExt := g.rng.Float64()
	rUnknown := g.rng.Float64()
	rNull := g.rng.Float64()
	rStatus := g.rng.IntN(1000)
	nas := g.pickNAS()

	seq := g.seqs[idx]
	g.seqs[idx]++

	e := event{
		idx:      idx,
		seq:      seq,
		nas:      nas,
		status:   g.pickStatus(rStatus),
		cause:    terminateCauses[int(seq)%g.cfg.TerminateCauses],
		extended: rExt < g.cfg.ExtendedJSONRatio,
		unknown:  rUnknown < g.cfg.UnknownFieldRatio,
	}
	if rPatch < g.cfg.PatchRatio {
		e.null = rNull < g.cfg.NullFieldRatio
		return e.appendPatch(dst)
	}
	return e.appendFull(dst)
}

// pickStatus resolve o valor de Acct-Status-Type pelos pesos acumulados,
// respeitando o teto de StatusValues (os pesos são reescalados para o
// subconjunto pedido).
func (g *Generator) pickStatus(r int) string {
	n := g.cfg.StatusValues
	teto := statusWeights[n-1]
	alvo := r * teto / 1000
	for i := 0; i < n; i++ {
		if alvo < statusWeights[i] {
			return statusValues[i]
		}
	}
	return statusValues[n-1]
}

// pickNAS concentra as sessões em poucos NAS: uma exponencial truncada em
// que ~20% dos NAS levam ~80% do tráfego, como na topologia real. Uniforme
// aqui faria o índice sobre NAS parecer melhor do que é.
func (g *Generator) pickNAS() int {
	n := g.cfg.NASCount
	if n == 1 {
		return 0
	}
	v := int(g.rng.ExpFloat64() * float64(n) / 8)
	if v >= n {
		v = n - 1
	}
	return v
}

// event carrega o que varia entre eventos; os métodos append escrevem o JSON
// sem nenhum fmt (strconv.Append* e literais).
type event struct {
	idx      int
	seq      uint32
	nas      int
	status   string
	cause    string
	extended bool
	unknown  bool
	null     bool
}

// millis do evento: base + 30 s por evento da entidade (cadência de
// interim-update).
func (e event) millis() int64 { return baseMillis + int64(e.seq)*30_000 }

func (e event) appendFull(dst []byte) []byte {
	ms := e.millis()
	dst = append(dst, '{')
	dst = e.appendID(append(dst, `"_id":`...))
	dst = append(dst, `,"Acct-Delay-Time":0,"Acct-Multi-Session-Id":"i-br-pa`...)
	dst = strconv.AppendInt(dst, int64(e.idx), 10)
	dst = append(dst, `AAAaaH","Acct-Session-Id":"i-br-pa`...)
	dst = strconv.AppendInt(dst, int64(e.idx)*31+int64(e.seq), 16)
	dst = append(dst, `AAAO6W","Acct-Session-Time":`...)
	dst = strconv.AppendInt(dst, int64(e.seq)*30, 10)
	dst = append(dst, `,"Acct-Status-Type":"`...)
	dst = append(dst, e.status...)
	dst = append(dst, `","Class":"#S:Abie71#T:`...)
	dst = e.appendIDBody(dst)
	dst = append(dst, `#C:117673879#L:361#M:68:d4:0c:6a:f5:f1#I:","Event-Timestamp":`...)
	dst = e.appendDate(dst, ms)
	dst = append(dst, `,"EventExpireDate":`...)
	dst = e.appendDate(dst, ms+7*24*3600*1000)
	dst = append(dst, `,"Framed-IP-Address":"`...)
	dst = appendIPv4(dst, 0x0A000000+uint32(e.idx))
	dst = append(dst, `","Framed-IPv6-Prefix":"2804:1b2:d102:`...)
	dst = strconv.AppendInt(dst, int64(e.idx&0xFFFF), 16)
	dst = append(dst, `::/64","Framed-Interface-Id":"0x`...)
	dst = strconv.AppendInt(dst, int64(e.idx)*2654435761&0xFFFFFFFF, 16)
	dst = append(dst, `","Framed-Protocol":"PPP","LastSessionStartedDate":`...)
	dst = e.appendDate(dst, ms-int64(e.seq)*30_000)
	dst = append(dst, `,"NAS-IP-Address":"`...)
	dst = appendIPv4(dst, 0xBB64AA00+uint32(e.nas))
	dst = append(dst, `","NAS-Identifier":"i-br-pa-blm-ccp-hl4-`...)
	dst = appendPadded(dst, int64(e.nas), 3)
	dst = append(dst, `","NAS-Port":`...)
	dst = strconv.AppendInt(dst, int64(e.idx%1_000_000), 10)
	dst = append(dst, `,"NAS-Port-Id":"slot=0;subslot=0;port=0;vlanid=`...)
	dst = strconv.AppendInt(dst, int64(e.idx%4096), 10)
	dst = append(dst, `;vlanid2=123;","NAS-Port-Type":"Ethernet","Service-Type":"Framed-User","User-Name":"cliente`...)
	dst = strconv.AppendInt(dst, int64(e.idx), 10)
	dst = append(dst, `@isp.net.br","_insertedDate":`...)
	dst = e.appendDate(dst, baseMillis-90*24*3600*1000)
	dst = append(dst, `,"_lastUpdate":`...)
	dst = e.appendDate(dst, ms+5_000)
	dst = append(dst, `,"id":`...)
	dst = e.appendID(dst)
	dst = e.appendCounters(dst)
	dst = append(dst, `,"Delegated-IPv6-Prefix":"2804:1b2:d080:`...)
	dst = strconv.AppendInt(dst, int64(e.idx&0xFFFF), 16)
	dst = append(dst, `::/64","LastSessionAliveDate":`...)
	dst = e.appendDate(dst, ms)
	dst = append(dst, `,"Acct-Output-Gigawords":`...)
	dst = strconv.AppendInt(dst, int64(e.seq)/1000, 10)
	dst = append(dst, `,"Acct-Terminate-Cause":"`...)
	dst = append(dst, e.cause...)
	dst = append(dst, `","LastSessionStoppedDate":`...)
	dst = e.appendDate(dst, ms-3600_000)
	dst = append(dst, `,"RemoteServerAddr":"192.168.201.`...)
	dst = strconv.AppendInt(dst, int64(e.nas%254+1), 10)
	dst = append(dst, `","PSA-SERVER":"192.168.201.`...)
	dst = strconv.AppendInt(dst, int64(e.nas%254+1), 10)
	dst = append(dst, `","Acct-Input-Gigawords":`...)
	dst = strconv.AppendInt(dst, int64(e.seq)/1500, 10)
	dst = append(dst, `,"_SEQ":`...)
	dst = e.appendSeq(dst)
	// Connect-Info chega como STRING num campo declarado int64 — é assim na
	// amostra real, e é o caso de coerção C1 que a ingestão precisa aguentar.
	dst = append(dst, `,"Acct-Authentic":"RADIUS","Calling-Station-Id":"i-br-pa-blm-ccp-hl4-`...)
	dst = appendPadded(dst, int64(e.nas), 3)
	dst = append(dst, `#0/0#123:272","Connect-Info":"1000000000","Filter-Id":"hsi","Idle-Timeout":0,"Session-Timeout":766800`...)
	dst = e.appendUnknown(dst)
	return append(dst, '}')
}

func (e event) appendPatch(dst []byte) []byte {
	dst = append(dst, `{"id":`...)
	dst = e.appendID(dst)
	dst = append(dst, `,"_SEQ":`...)
	dst = e.appendSeq(dst)
	dst = append(dst, `,"Acct-Status-Type":"`...)
	dst = append(dst, e.status...)
	dst = append(dst, `","Acct-Session-Time":`...)
	dst = strconv.AppendInt(dst, int64(e.seq)*30, 10)
	dst = e.appendCounters(dst)
	dst = append(dst, `,"LastSessionAliveDate":`...)
	dst = e.appendDate(dst, e.millis())
	if e.null {
		// null explícito: a semântica D2 (remover o campo) precisa aparecer no
		// corpus, senão o caminho nunca é exercitado em escala.
		dst = append(dst, `,"Acct-Terminate-Cause":null`...)
	}
	dst = e.appendUnknown(dst)
	return append(dst, '}')
}

// appendCounters escreve os quatro contadores de octetos/pacotes, que são o
// que de fato muda a cada interim-update.
func (e event) appendCounters(dst []byte) []byte {
	base := int64(e.idx)*1000 + int64(e.seq)*104_729
	dst = append(dst, `,"Acct-Input-Octets":`...)
	dst = strconv.AppendInt(dst, base*3, 10)
	dst = append(dst, `,"Acct-Input-Packets":`...)
	dst = strconv.AppendInt(dst, base/97, 10)
	dst = append(dst, `,"Acct-Output-Octets":`...)
	// Acima de MaxInt32 de propósito: o campo é int64 e o parser precisa
	// provar isso no corpus, não só no teste unitário.
	dst = strconv.AppendInt(dst, 2_147_483_648+base*7, 10)
	dst = append(dst, `,"Acct-Output-Packets":`...)
	return strconv.AppendInt(dst, base/31, 10)
}

// appendID escreve a key entre aspas: "CMP-Vxxxx PPCLP-yyy", 1:1 com a
// entidade e estável entre eventos.
func (e event) appendID(dst []byte) []byte {
	dst = append(dst, '"')
	dst = e.appendIDBody(dst)
	return append(dst, '"')
}

func (e event) appendIDBody(dst []byte) []byte {
	dst = append(dst, `CMP-V`...)
	dst = appendPadded(dst, int64(e.idx/1000), 4)
	dst = append(dst, `PPCLP-`...)
	return appendPadded(dst, int64(e.idx%1000), 3)
}

// appendSeq escreve o _SEQ, cru ou no wrapper $numberLong do Extended JSON.
func (e event) appendSeq(dst []byte) []byte {
	seq := seqBase + int64(e.idx) + int64(e.seq)
	if e.extended {
		dst = append(dst, `{"$numberLong":"`...)
		dst = strconv.AppendInt(dst, seq, 10)
		return append(dst, `"}`...)
	}
	return strconv.AppendInt(dst, seq, 10)
}

// appendDate escreve o timestamp. Fora do Extended JSON sai como string
// RFC3339 com offset numérico (+00:00, como a amostra real — não "Z"); com
// Extended JSON, alterna entre o shape relaxed e o canonical, porque o
// tokenizer trata os dois por caminhos diferentes.
func (e event) appendDate(dst []byte, ms int64) []byte {
	if !e.extended {
		return appendRFC3339(dst, ms)
	}
	if e.idx%10 == 0 {
		dst = append(dst, `{"$date":{"$numberLong":"`...)
		dst = strconv.AppendInt(dst, ms, 10)
		return append(dst, `"}}`...)
	}
	dst = append(dst, `{"$date":`...)
	dst = appendRFC3339(dst, ms)
	return append(dst, '}')
}

// appendUnknown adiciona um campo que o schema não declara — o caminho da
// policy unknownFields, que em produção aparece quando o vendor acrescenta um
// atributo.
func (e event) appendUnknown(dst []byte) []byte {
	if !e.unknown {
		return dst
	}
	dst = append(dst, `,"X-Vendor-Attr-`...)
	dst = strconv.AppendInt(dst, int64(e.idx%64), 10)
	dst = append(dst, `":"v`...)
	dst = strconv.AppendInt(dst, int64(e.seq), 10)
	return append(dst, '"')
}

// appendRFC3339 escreve "2026-08-12T21:00:08.000+00:00" entre aspas.
func appendRFC3339(dst []byte, ms int64) []byte {
	dst = append(dst, '"')
	dst = time.UnixMilli(ms).UTC().AppendFormat(dst, "2006-01-02T15:04:05.000-07:00")
	return append(dst, '"')
}

// appendIPv4 escreve o endereço em notação decimal pontuada.
func appendIPv4(dst []byte, addr uint32) []byte {
	dst = strconv.AppendUint(dst, uint64(addr>>24), 10)
	dst = append(dst, '.')
	dst = strconv.AppendUint(dst, uint64(addr>>16&0xFF), 10)
	dst = append(dst, '.')
	dst = strconv.AppendUint(dst, uint64(addr>>8&0xFF), 10)
	dst = append(dst, '.')
	return strconv.AppendUint(dst, uint64(addr&0xFF), 10)
}

// appendPadded escreve v com zeros à esquerda até width dígitos (valores
// acima do width saem inteiros, sem truncar — id truncado colidiria entre
// entidades).
func appendPadded(dst []byte, v int64, width int) []byte {
	digitos := 1
	for n := v; n >= 10; n /= 10 {
		digitos++
	}
	for i := digitos; i < width; i++ {
		dst = append(dst, '0')
	}
	return strconv.AppendInt(dst, v, 10)
}
