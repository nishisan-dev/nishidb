package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"strings"
	"time"
)

// AlarmStateFileName é o estado de sustentação dos alarmes de um dataset.
//
// # Por que ele mora aqui e não no manifest
//
// O estado que o applyRecord mantém tem de ser reconstruível a partir do
// payload dos records, nunca do relógio — e "desde quando esta condição vale" é
// exatamente relógio. Ele não é estado do dataset: é o cronômetro de um
// observador, e some sem consequência para os dados.
//
// # Por que ele NÃO entra na ordem do passo 8
//
// A ordem manifest.bin → kafka.offsets é invariante de código porque os dois
// descrevem o MESMO fato por dois caminhos — o quanto foi aplicado e o quanto
// foi consumido —, e invertê-los perde evento (§48). Este arquivo não descreve
// nem dado materializado nem posição de consumo. Escrevê-lo antes ou depois de
// qualquer um dos dois só muda QUANDO um alarme dispara depois de um reinício,
// nunca a integridade de nada. Por isso ele é escrito pela goroutine de alarme,
// nas transições, fora do Checkpoint.
const AlarmStateFileName = "alarms.state"

var (
	// ErrInAlarmState indica ausência do arquivo: dataset que nunca teve
	// alarme ativo, ou primeira subida depois de declarar o bloco. É o estado
	// NORMAL, e não uma anomalia.
	ErrInAlarmState = errors.New("checkpoint: estado de alarme ausente")

	// ErrCorruptAlarmState indica magic, version, CRC ou tamanho
	// inconsistente.
	//
	// Como o ErrCorruptOffsets, NÃO é dataset corrompido: os dados continuam
	// íntegros e as consultas têm de subir (§51). O que se perde são os
	// cronômetros, e a consequência é que cada alarme ativo recomeça a contar
	// o prazo — degradação visível no log, nunca recusa de abertura.
	ErrCorruptAlarmState = errors.New("checkpoint: estado de alarme corrompido")
)

var alarmStateMagic = [4]byte{'N', 'D', 'B', 'A'}

// alarmStateVersion é a versão do layout.
//
// # Por que a v2 não tem leitor da v1
//
// A v2 acrescenta a condição de qualificação e o fingerprint das métricas à
// assinatura da regra. Uma entrada v1 não tem nenhum dos dois, e lê-la exigiria
// inventá-los: vazio afirmaria "sem qualificação e sem métrica citada", o que
// preservaria o cronômetro de um alarme cujo YAML ganhou `qualifyWhen` — o
// prazo antigo assombrando a condição nova, justamente o que a assinatura
// existe para impedir.
//
// Por isso qualquer versão diferente desta, a 1 inclusive, é
// ErrCorruptAlarmState. O custo é conhecido e foi aceito: a subida que troca a
// v0.8.x por esta versão reinicia os cronômetros UMA vez, com o WARN
// `alarm.state_unreadable` que a degradação já emite, e os alarmes em curso
// voltam a contar o prazo do zero.
const alarmStateVersion = 2

// MaxAlarmStateText é o maior texto que um prefixo u16 do alarms.state
// representa.
//
// É exportado porque a escrita não é o lugar certo de descobrir o limite: o
// compilador do alarme (internal/alarm) recusa na SUBIDA o `when`, o
// `qualifyWhen` e o fingerprint das métricas que passem dele, com esta MESMA
// constante — o número não existe em dois lugares. A recusa da escrita continua
// como rede de segurança.
//
// A chave canônica do grupo não precisa de recusa na subida: o dicionário do
// contador já recusa, ao internar, chave acima de u16 (Dict.Intern, em
// internal/storage/counter/dict.go), então nenhum grupo com chave maior chega a
// existir para ser observado.
const MaxAlarmStateText = 1<<16 - 1

// Tamanho mínimo de cada registro, usado como sanidade ANTES de alocar: sem
// isso um alarmCount adulterado para 4 bilhões faria o decodificador pedir a
// memória do processo antes de chegar ao CRC.
const (
	minBytesPerAlarm = 2 + 2 + 2 + 2 + 2 + 8 + 4 // cinco prefixos de texto, prazo, contagem
	minBytesPerGroup = 2 + 8                     // prefixo da chave e instante
)

// AlarmState é o conteúdo do checkpoint/alarms.state.
type AlarmState struct {
	// SavedAtUnixMs é quando o arquivo foi escrito. É o que permite ao evento
	// de um alarme restaurado dizer quanto tempo o processo ficou fora —
	// sem ele, um disparo sobre um período que ninguém observou seria
	// indistinguível de um observado de ponta a ponta.
	SavedAtUnixMs int64
	// Alarms são os alarmes com pelo menos um grupo em observação.
	Alarms []AlarmEntry
}

// AlarmEntry é o estado de UM alarme, com os grupos que o sustentam.
//
// When, QualifyWhen, MetricsFingerprint e SustainedFor são a ASSINATURA da
// regra, e viajam com a entrada para que o estado lido possa ser INVALIDADO
// quando o YAML muda: um prazo antigo aplicado a uma condição nova faria o
// alarme disparar por uma sustentação que ninguém mediu. Ficam por alarme, e
// não por grupo, porque num contador por PON são milhares de grupos repetindo o
// mesmo texto de condição.
type AlarmEntry struct {
	Counter string
	Alarm   string
	When    string
	// QualifyWhen é a condição de qualificação normalizada; vazio quando o
	// alarme não a declara.
	QualifyWhen string
	// MetricsFingerprint é o texto canônico das métricas de janela citadas por
	// When ou QualifyWhen. É o que faz uma janela alterada no YAML invalidar o
	// cronômetro mesmo com o texto das condições intacto: `auth_24h_max > 10`
	// sobre uma janela de 24h e sobre uma de 1h são regras diferentes com o
	// mesmo texto.
	MetricsFingerprint string
	SustainedFor       time.Duration
	Groups             []GroupSince
}

// GroupSince é um grupo em observação e desde quando.
type GroupSince struct {
	// Group é a chave CANÔNICA do grupo em bytes (a composta do valuekey), não
	// o texto do groupKey. O texto é derivado do keyFormat, que pode mudar sem
	// reconstruir o contador — e usá-lo como identidade faria uma troca de
	// molde descartar, em silêncio, o estado de todos os alarmes ativos.
	Group       []byte
	SinceUnixMs int64
}

// Layout do alarms.state (little-endian):
//
//	0   4  magic "NDBA"
//	4   1  version = 2
//	5   3  reservado
//	8   8  savedAt i64 (unix ms)
//	16  4  alarmCount u32
//	       por alarme:
//	         counterLen u16     | counter
//	         alarmLen u16       | alarm
//	         whenLen u16        | when
//	         qualifyWhenLen u16 | qualifyWhen (0 quando não declarada)
//	         fingerprintLen u16 | metricsFingerprint (0 sem métrica citada)
//	              8             | sustainedFor i64 (ns)
//	              4             | groupCount u32
//	         por grupo:
//	           groupLen u16 | chave canônica
//	                8       | since i64 (unix ms)
//	    4  crc32c sobre tudo acima
//
// Texto que não cabe no prefixo u16 é ERRO na escrita, e não truncamento: uma
// assinatura truncada seria comparada contra o YAML inteiro e descartaria o
// cronômetro a cada subida, sem nunca dizer por quê.
func (s AlarmState) encode() ([]byte, error) {
	alarms := s.canonical()
	b := make([]byte, 0, 24+len(alarms)*64)
	b = append(b, alarmStateMagic[:]...)
	b = append(b, alarmStateVersion, 0, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, uint64(s.SavedAtUnixMs))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(alarms)))
	for _, a := range alarms {
		for _, field := range [...]struct{ name, text string }{
			{"counter", a.Counter},
			{"alarm", a.Alarm},
			{"when", a.When},
			{"qualifyWhen", a.QualifyWhen},
			{"metricsFingerprint", a.MetricsFingerprint},
		} {
			if len(field.text) > MaxAlarmStateText {
				return nil, fmt.Errorf("checkpoint: alarm %q: %s has %d bytes, above the %d the alarm state can hold",
					a.Alarm, field.name, len(field.text), MaxAlarmStateText)
			}
			b = appendText(b, field.text)
		}
		b = binary.LittleEndian.AppendUint64(b, uint64(a.SustainedFor))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(a.Groups)))
		for _, g := range a.Groups {
			if len(g.Group) > MaxAlarmStateText {
				return nil, fmt.Errorf("checkpoint: alarm %q: group key has %d bytes, above the %d the alarm state can hold",
					a.Alarm, len(g.Group), MaxAlarmStateText)
			}
			b = binary.LittleEndian.AppendUint16(b, uint16(len(g.Group)))
			b = append(b, g.Group...)
			b = binary.LittleEndian.AppendUint64(b, uint64(g.SinceUnixMs))
		}
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli)), nil
}

// canonical devolve os alarmes em ordem determinística, com os grupos de cada
// um também ordenados, SEM mexer nos slices do chamador.
//
// A ordem é canônica pelo mesmo motivo da ordem crescente de partição no
// kafka.offsets: dois estados iguais produzem arquivos idênticos byte a byte, e
// um diff de dois arquivos é ferramenta de diagnóstico. Com a ordem da memória
// — um mapa iterado — o mesmo estado produziria um arquivo diferente a cada
// escrita, e nenhuma comparação entre duas gerações diria nada.
func (s AlarmState) canonical() []AlarmEntry {
	out := make([]AlarmEntry, len(s.Alarms))
	for i, a := range s.Alarms {
		a.Groups = slices.Clone(a.Groups)
		slices.SortFunc(a.Groups, func(x, y GroupSince) int { return slices.Compare(x.Group, y.Group) })
		out[i] = a
	}
	slices.SortFunc(out, func(x, y AlarmEntry) int {
		if c := strings.Compare(x.Counter, y.Counter); c != 0 {
			return c
		}
		return strings.Compare(x.Alarm, y.Alarm)
	})
	return out
}

func decodeAlarmState(raw []byte) (AlarmState, error) {
	var s AlarmState
	if len(raw) < 8 || [4]byte(raw[0:4]) != alarmStateMagic {
		return s, fmt.Errorf("%w: magic inválida", ErrCorruptAlarmState)
	}
	// A v1 cai aqui como qualquer versão desconhecida (ver alarmStateVersion).
	if raw[4] != alarmStateVersion {
		return s, fmt.Errorf("%w: version desconhecida %d", ErrCorruptAlarmState, raw[4])
	}
	if len(raw) < 24 {
		return s, fmt.Errorf("%w: truncado", ErrCorruptAlarmState)
	}
	body, tail := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return s, fmt.Errorf("%w: CRC32C não confere", ErrCorruptAlarmState)
	}

	r := &reader{b: body, pos: 8}
	s.SavedAtUnixMs = int64(r.u64())
	n := r.u32()
	if int(n) > len(body)/minBytesPerAlarm {
		return s, fmt.Errorf("%w: alarmCount %d impossível", ErrCorruptAlarmState, n)
	}
	s.Alarms = make([]AlarmEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		var a AlarmEntry
		a.Counter = r.text()
		a.Alarm = r.text()
		a.When = r.text()
		a.QualifyWhen = r.text()
		a.MetricsFingerprint = r.text()
		a.SustainedFor = time.Duration(r.u64())
		g := r.u32()
		if r.bad || int(g) > len(body)/minBytesPerGroup {
			return s, fmt.Errorf("%w: groupCount %d impossível em %q", ErrCorruptAlarmState, g, a.Alarm)
		}
		a.Groups = make([]GroupSince, 0, g)
		for j := uint32(0); j < g; j++ {
			key := []byte(r.text())
			a.Groups = append(a.Groups, GroupSince{Group: key, SinceUnixMs: int64(r.u64())})
		}
		s.Alarms = append(s.Alarms, a)
	}
	// Byte a mais no fim também é corrupção: sem esta checagem, um arquivo
	// truncado e completado com lixo passaria pelo CRC de um prefixo válido.
	if r.bad || r.pos != len(body) {
		return s, fmt.Errorf("%w: tamanho inconsistente (pos=%d body=%d)", ErrCorruptAlarmState, r.pos, len(body))
	}
	return s, nil
}

// WriteAlarmState grava o alarms.state atomicamente, pelo mesmo caminho de
// durabilidade do manifest e do kafka.offsets.
//
// Estado VAZIO é gravável e significativo: sem isso o arquivo da passada
// anterior continuaria afirmando que há alarmes ativos depois de todos
// resolverem, e a subida seguinte reanunciaria disparos que já passaram.
func WriteAlarmState(path string, s AlarmState) error {
	raw, err := s.encode()
	if err != nil {
		return err
	}
	return writeAtomic(path, raw)
}

// LoadAlarmState lê o alarms.state. Ausência devolve ErrInAlarmState, que é
// distinto de corrupção: "nunca existiu" é a primeira subida, e "existia e não
// dá para ler" é degradação a registrar. Tratar os dois igual faria a segunda
// passar calada.
func LoadAlarmState(path string) (AlarmState, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return AlarmState{}, fmt.Errorf("%w: %s", ErrInAlarmState, path)
	}
	if err != nil {
		return AlarmState{}, err
	}
	return decodeAlarmState(raw)
}
