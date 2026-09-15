package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleAlarmState() AlarmState {
	return AlarmState{
		SavedAtUnixMs: 1757000000000,
		Alarms: []AlarmEntry{{
			Counter:            "autenticados_por_olt",
			Alarm:              "olt_down",
			When:               "total < 100",
			QualifyWhen:        "auth_24h_max > 10",
			MetricsFingerprint: "auth_24h:window:total:86400000",
			SustainedFor:       120_000_000_000,
			Groups: []GroupSince{
				{Group: []byte{0x01, 'A'}, SinceUnixMs: 1756999880000},
			},
		}},
	}
}

// mustEncode codifica o estado e falha o teste se a codificação recusar.
func mustEncode(t *testing.T, s AlarmState) []byte {
	t.Helper()
	raw, err := s.encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestEstadoDeAlarmeEhGoldenEmBytes fixa o formato em disco. Os bytes foram
// montados FORA do Go, a partir do layout documentado (um script à parte que
// escreve campo a campo e calcula o CRC32C à mão, conferido contra o golden da
// v1), porque um golden produzido pelo próprio encoder não prova formato nenhum
// — ele acompanharia qualquer mudança acidental em silêncio.
func TestEstadoDeAlarmeEhGoldenEmBytes(t *testing.T) {
	want := []byte{
		0x4e, 0x44, 0x42, 0x41, 0x02, 0x00, 0x00, 0x00, 0x00, 0x62, 0x5c, 0x15, 0x99, 0x01, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x61, 0x75, 0x74, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x64,
		0x6f, 0x73, 0x5f, 0x70, 0x6f, 0x72, 0x5f, 0x6f, 0x6c, 0x74, 0x08, 0x00, 0x6f, 0x6c, 0x74, 0x5f,
		0x64, 0x6f, 0x77, 0x6e, 0x0b, 0x00, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x20, 0x3c, 0x20, 0x31, 0x30,
		0x30, 0x11, 0x00, 0x61, 0x75, 0x74, 0x68, 0x5f, 0x32, 0x34, 0x68, 0x5f, 0x6d, 0x61, 0x78, 0x20,
		0x3e, 0x20, 0x31, 0x30, 0x1e, 0x00, 0x61, 0x75, 0x74, 0x68, 0x5f, 0x32, 0x34, 0x68, 0x3a, 0x77,
		0x69, 0x6e, 0x64, 0x6f, 0x77, 0x3a, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x3a, 0x38, 0x36, 0x34, 0x30,
		0x30, 0x30, 0x30, 0x30, 0x00, 0xb0, 0x8e, 0xf0, 0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x01, 0x41, 0x40, 0x8d, 0x5a, 0x15, 0x99, 0x01, 0x00, 0x00, 0x5d, 0xb2, 0x5d, 0x8e,
	}
	got := mustEncode(t, sampleAlarmState())
	if !bytes.Equal(got, want) {
		t.Fatalf("formato em disco mudou\ngot  (%d) %x\nquero(%d) %x", len(got), got, len(want), want)
	}
}

// TestAlarmStateV1FileIsRefusedAsUnknownVersion é a decisão de não ler a v1.
//
// Os bytes são um arquivo v1 DE VERDADE, com CRC válido — o golden que a v0.8.x
// grava. Uma entrada v1 não tem qualifyWhen nem fingerprint, e lê-la exigiria
// inventar os dois; a subida da versão nova descarta o arquivo e os cronômetros
// recomeçam uma vez. A recusa tem de ser a da VERSÃO: lido como v2, o mesmo
// arquivo também morreria por tamanho inconsistente, e um teste que só conferisse
// ErrCorruptAlarmState passaria com a checagem de versão removida.
func TestAlarmStateV1FileIsRefusedAsUnknownVersion(t *testing.T) {
	v1 := []byte{
		0x4e, 0x44, 0x42, 0x41, 0x01, 0x00, 0x00, 0x00, 0x00, 0x62, 0x5c, 0x15, 0x99, 0x01, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x61, 0x75, 0x74, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x64,
		0x6f, 0x73, 0x5f, 0x70, 0x6f, 0x72, 0x5f, 0x6f, 0x6c, 0x74, 0x08, 0x00, 0x6f, 0x6c, 0x74, 0x5f,
		0x64, 0x6f, 0x77, 0x6e, 0x0b, 0x00, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x20, 0x3c, 0x20, 0x31, 0x30,
		0x30, 0x00, 0xb0, 0x8e, 0xf0, 0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01,
		0x41, 0x40, 0x8d, 0x5a, 0x15, 0x99, 0x01, 0x00, 0x00, 0x4c, 0x44, 0xc4, 0x37,
	}
	if !bytes.Equal(v1[len(v1)-4:], crcOf(v1[:len(v1)-4])) {
		t.Fatal("o arquivo v1 do teste não tem CRC válido: o teste provaria a recusa do CRC, não a da versão")
	}
	path := filepath.Join(t.TempDir(), AlarmStateFileName)
	if err := os.WriteFile(path, v1, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadAlarmState(path)
	if !errors.Is(err, ErrCorruptAlarmState) {
		t.Fatalf("erro = %v, quero ErrCorruptAlarmState", err)
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("a recusa tem de ser a da versão, veio: %v", err)
	}
}

// TestEstadoDeAlarmeVoltaComoFoiEscrito é o round trip pelo arquivo de verdade,
// com a assinatura inteira da regra — é ela que decide, na subida, se o
// cronômetro ainda vale.
func TestEstadoDeAlarmeVoltaComoFoiEscrito(t *testing.T) {
	path := filepath.Join(t.TempDir(), AlarmStateFileName)
	want := sampleAlarmState()
	if err := WriteAlarmState(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.SavedAtUnixMs != want.SavedAtUnixMs || len(got.Alarms) != 1 {
		t.Fatalf("estado = %+v", got)
	}
	a, b := got.Alarms[0], want.Alarms[0]
	if a.Counter != b.Counter || a.Alarm != b.Alarm || a.When != b.When || a.SustainedFor != b.SustainedFor ||
		a.QualifyWhen != b.QualifyWhen || a.MetricsFingerprint != b.MetricsFingerprint {
		t.Errorf("alarme = %+v, quero %+v", a, b)
	}
	if len(a.Groups) != 1 || !bytes.Equal(a.Groups[0].Group, b.Groups[0].Group) ||
		a.Groups[0].SinceUnixMs != b.Groups[0].SinceUnixMs {
		t.Errorf("grupos = %+v, quero %+v", a.Groups, b.Groups)
	}
}

// TestAlarmStateWithoutQualificationRoundTripsEmptyTexts: alarme sem
// qualifyWhen e sem métrica citada grava os dois textos vazios, e eles têm de
// voltar vazios — é o que a restauração compara contra o YAML.
func TestAlarmStateWithoutQualificationRoundTripsEmptyTexts(t *testing.T) {
	path := filepath.Join(t.TempDir(), AlarmStateFileName)
	s := sampleAlarmState()
	s.Alarms[0].QualifyWhen, s.Alarms[0].MetricsFingerprint = "", ""
	if err := WriteAlarmState(path, s); err != nil {
		t.Fatal(err)
	}
	got, err := LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if a := got.Alarms[0]; a.QualifyWhen != "" || a.MetricsFingerprint != "" || a.When != "total < 100" {
		t.Errorf("alarme = %+v", a)
	}
}

// TestAlarmStateTextAboveU16IsRefusedOnWrite: texto que não cabe no prefixo é
// erro, nunca truncamento. Uma assinatura truncada seria comparada contra o YAML
// inteiro na subida e descartaria o cronômetro a cada reinício, sem dizer por
// quê. O limite exato (65.535) ainda é gravável, para que a recusa não pegue o
// que cabe.
func TestAlarmStateTextAboveU16IsRefusedOnWrite(t *testing.T) {
	fits := sampleAlarmState()
	fits.Alarms[0].QualifyWhen = strings.Repeat("x", MaxAlarmStateText)
	if err := WriteAlarmState(filepath.Join(t.TempDir(), AlarmStateFileName), fits); err != nil {
		t.Fatalf("texto de exatamente %d bytes foi recusado: %v", MaxAlarmStateText, err)
	}
	cases := map[string]func(*AlarmEntry){
		"when":               func(a *AlarmEntry) { a.When = strings.Repeat("x", MaxAlarmStateText+1) },
		"qualifyWhen":        func(a *AlarmEntry) { a.QualifyWhen = strings.Repeat("x", MaxAlarmStateText+1) },
		"metricsFingerprint": func(a *AlarmEntry) { a.MetricsFingerprint = strings.Repeat("x", MaxAlarmStateText+1) },
		"group key":          func(a *AlarmEntry) { a.Groups[0].Group = bytes.Repeat([]byte{1}, MaxAlarmStateText+1) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := sampleAlarmState()
			mutate(&s.Alarms[0])
			path := filepath.Join(t.TempDir(), AlarmStateFileName)
			err := WriteAlarmState(path, s)
			if err == nil {
				t.Fatal("texto acima do prefixo u16 foi aceito")
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("a recusa não nomeia o campo %q: %v", name, err)
			}
			if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
				t.Errorf("a recusa deixou arquivo no disco (stat: %v)", statErr)
			}
		})
	}
}

// TestEstadoDeAlarmeAusenteNaoEhCorrupcao: a distinção é a mesma do
// kafka.offsets e vale pelo mesmo motivo — "nunca existiu" é o estado normal da
// primeira subida, e tratá-lo como corrupção faria todo dataset novo gritar.
func TestEstadoDeAlarmeAusenteNaoEhCorrupcao(t *testing.T) {
	path := filepath.Join(t.TempDir(), AlarmStateFileName)
	_, err := LoadAlarmState(path)
	if !errors.Is(err, ErrInAlarmState) {
		t.Fatalf("erro = %v, quero ErrInAlarmState", err)
	}
	if errors.Is(err, ErrCorruptAlarmState) {
		t.Error("ausência não pode casar corrupção: os dois pedem decisões diferentes")
	}
}

// TestEstadoDeAlarmeCorrompidoEhRecusado cobre cada forma de adulteração que o
// decodificador tem de pegar. Sem a checagem de tamanho no fim, um arquivo
// truncado e completado com lixo passaria pelo CRC de um prefixo válido.
func TestEstadoDeAlarmeCorrompidoEhRecusado(t *testing.T) {
	valid := mustEncode(t, sampleAlarmState())
	cases := map[string]func([]byte) []byte{
		"magic trocada":        func(b []byte) []byte { c := clone(b); c[0] = 'X'; return c },
		"version desconhecida": func(b []byte) []byte { c := clone(b); c[4] = 9; return c },
		"crc adulterado":       func(b []byte) []byte { c := clone(b); c[len(c)-1] ^= 0xff; return c },
		"byte do meio virado":  func(b []byte) []byte { c := clone(b); c[30] ^= 0xff; return c },
		"truncado no header":   func(b []byte) []byte { return clone(b[:6]) },
		"truncado no corpo":    func(b []byte) []byte { return clone(b[:40]) },
		"byte a mais no fim": func(b []byte) []byte {
			// Prefixo válido com CRC válido e uma cauda de lixo: só a
			// conferência de posição final pega.
			c := append(clone(b[:len(b)-4]), 0xaa)
			return append(c, crcOf(c)...)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), AlarmStateFileName)
			if err := os.WriteFile(path, mutate(valid), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadAlarmState(path)
			if !errors.Is(err, ErrCorruptAlarmState) {
				t.Fatalf("erro = %v, quero ErrCorruptAlarmState", err)
			}
		})
	}
}

// TestEstadoVazioEhEstadoValido: nenhum alarme ativo é uma afirmação, e ela
// tem de ser gravável — senão o arquivo de uma passada anterior continuaria
// dizendo que há alarmes ativos depois de todos resolverem.
func TestEstadoVazioEhEstadoValido(t *testing.T) {
	path := filepath.Join(t.TempDir(), AlarmStateFileName)
	if err := WriteAlarmState(path, AlarmState{SavedAtUnixMs: 1757000000000}); err != nil {
		t.Fatal(err)
	}
	got, err := LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Alarms) != 0 {
		t.Fatalf("%d alarmes, quero nenhum", len(got.Alarms))
	}
	if got.SavedAtUnixMs != 1757000000000 {
		t.Errorf("savedAt = %d, e é ele que mede a lacuna do processo parado", got.SavedAtUnixMs)
	}
}

// TestOMesmoEstadoProduzOMesmoArquivo: a ordem de emissão é canônica, e não a
// ordem em que os alarmes apareceram na memória. É o mesmo argumento da ordem
// crescente de partição no kafka.offsets — um diff de dois arquivos é
// ferramenta de diagnóstico, e não seria se a ordem variasse entre execuções.
func TestOMesmoEstadoProduzOMesmoArquivo(t *testing.T) {
	g1 := GroupSince{Group: []byte{0x01, 'A'}, SinceUnixMs: 1}
	g2 := GroupSince{Group: []byte{0x01, 'B'}, SinceUnixMs: 2}
	a := AlarmEntry{Counter: "c1", Alarm: "a1", When: "total = 0", SustainedFor: 1, Groups: []GroupSince{g1, g2}}
	b := AlarmEntry{Counter: "c1", Alarm: "a2", When: "total = 0", SustainedFor: 1, Groups: []GroupSince{g1}}

	direct := AlarmState{SavedAtUnixMs: 7, Alarms: []AlarmEntry{a, b}}
	swapped := AlarmState{SavedAtUnixMs: 7, Alarms: []AlarmEntry{
		{Counter: "c1", Alarm: "a2", When: "total = 0", SustainedFor: 1, Groups: []GroupSince{g1}},
		{Counter: "c1", Alarm: "a1", When: "total = 0", SustainedFor: 1, Groups: []GroupSince{g2, g1}},
	}}
	if !bytes.Equal(mustEncode(t, direct), mustEncode(t, swapped)) {
		t.Fatal("a ordem da memória vazou para o arquivo: dois estados iguais produziram bytes diferentes")
	}
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }

// TestContagemAbsurdaComCrcCoerenteEhRecusadaAntesDeAlocar exercita a sanidade
// que o CRC não alcança.
//
// Um count adulterado num arquivo qualquer morre no CRC. Estes dois casos
// forjam o CRC junto, que é o que um arquivo escrito por outra versão — ou por
// quem tem acesso ao diretório — produziria: sem a guarda, o decodificador
// atenderia um pedido de capacidade de bilhões de registros ANTES de descobrir
// que o corpo não os contém, e o processo morre por memória em vez de recusar
// o arquivo.
func TestContagemAbsurdaComCrcCoerenteEhRecusadaAntesDeAlocar(t *testing.T) {
	valid := mustEncode(t, sampleAlarmState())
	// Deslocamentos contados sobre o estado de exemplo, campo a campo do
	// layout — um offset chutado cai no meio de outro campo e faz o subteste
	// passar por acidente, provando a guarda errada:
	//
	//	alarmCount = 4 magic + 4 version/reservado + 8 savedAt = 16
	//	groupCount = 20 + (2+20 counter) + (2+8 alarm) + (2+11 when)
	//	           + (2+17 qualifyWhen) + (2+30 fingerprint) + 8  = 124
	cases := map[string]int{
		"alarmCount": 16,
		"groupCount": 124,
	}
	if got := binary.LittleEndian.Uint32(valid[124:]); got != 1 {
		t.Fatalf("groupCount no offset 124 = %d, quero 1: o offset não aponta para o campo", got)
	}
	for name, off := range cases {
		t.Run(name, func(t *testing.T) {
			body := clone(valid[:len(valid)-4])
			binary.LittleEndian.PutUint32(body[off:], 0xffffffff)
			forged := append(body, crcOf(body)...)
			path := filepath.Join(t.TempDir(), AlarmStateFileName)
			if err := os.WriteFile(path, forged, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadAlarmState(path)
			if !errors.Is(err, ErrCorruptAlarmState) {
				t.Fatalf("erro = %v, quero ErrCorruptAlarmState", err)
			}
			if !strings.Contains(err.Error(), "impossível") {
				t.Errorf("a recusa tem de ser a da sanidade de contagem, veio: %v", err)
			}
		})
	}
}
