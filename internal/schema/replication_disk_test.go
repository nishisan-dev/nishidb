package schema

import (
	"strings"
	"testing"
	"time"
)

// diskKeyCases são as três chaves do feed em disco, com um valor válido cada.
// periodicSnapshot só é válido com targets; nos testes de recusa por
// papel/habilitação a guarda de "sem targets" nunca é a que dispara, porque a
// recusa por presença vem antes do valor.
var diskKeyCases = []struct{ key, line string }{
	{"maxDiskBytes", "maxDiskBytes: 1GiB"},
	{"publishHistory", "publishHistory: false"},
	{"periodicSnapshot", "periodicSnapshot: 24h"},
}

func TestReplicationMaxDiskBytesDefaultsToTwoGiB(t *testing.T) {
	// Literal 2147483648 de propósito: o oráculo não pode ser a constante do
	// código em teste.
	for name, yml := range map[string]string{
		"bloco ausente":     baseYAML,
		"master habilitado": replicationYAML("enabled: true"),
		"master desligado":  replicationYAML("enabled: false"),
	} {
		s := parseOK(t, yml)
		if s.Replication.MaxDiskBytes != 2147483648 {
			t.Fatalf("%s: MaxDiskBytes = %d, quero 2147483648", name, s.Replication.MaxDiskBytes)
		}
		if !s.Replication.PublishHistory {
			t.Fatalf("%s: PublishHistory ausente deveria valer true", name)
		}
		if s.Replication.PeriodicSnapshot != 0 {
			t.Fatalf("%s: PeriodicSnapshot = %v, quero 0 (desligado)", name, s.Replication.PeriodicSnapshot)
		}
	}
	s := parseOK(t, replicationYAML("enabled: true", "maxDiskBytes: 512MiB"))
	if s.Replication.MaxDiskBytes != 536870912 {
		t.Fatalf("MaxDiskBytes = %d, quero 536870912", s.Replication.MaxDiskBytes)
	}
}

func TestReplicationRejectsDiskKeysOutsideEnabledMaster(t *testing.T) {
	for _, k := range diskKeyCases {
		for _, tc := range []struct{ name, want string }{
			{"follower", `replication.` + k.key + ` is only valid for role master (role is "follower")`},
			{"disabled", "replication." + k.key + " requires replication.enabled: true"},
		} {
			var yml string
			if tc.name == "follower" {
				yml = replicationYAML("enabled: true", "role: follower", "masterURL: https://master.example", k.line)
			} else {
				yml = replicationYAML("enabled: false", k.line)
			}
			parseReplicationRefusal(t, yml, tc.want)
		}
		// Bloco sem `enabled` (padrão desligado) é o mesmo caso.
		parseReplicationRefusal(t, replicationYAML(k.line), "replication."+k.key+" requires replication.enabled: true")
		// E, ao contrário, a MESMA linha é aceita no master habilitado: a
		// recusa acima é do papel, não do valor.
		lines := []string{"enabled: true", k.line}
		if k.key == "periodicSnapshot" {
			lines = append(lines, "targets: [metrics]")
		}
		parseOK(t, replicationYAML(lines...))
	}
}

func TestReplicationRejectsMaxDiskBytesBelowFloor(t *testing.T) {
	// Piso absoluto: 8 MiB. Um byte abaixo é recusado, exatamente 8 MiB passa.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3MiB", "maxDiskBytes: 8388607B"),
		"replication.maxDiskBytes (8388607) must be at least 8388608 (8MiB)")
	s := parseOK(t, replicationYAML("enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3MiB", "maxDiskBytes: 8MiB"))
	if s.Replication.MaxDiskBytes != 8388608 {
		t.Fatalf("MaxDiskBytes = %d, quero 8388608", s.Replication.MaxDiskBytes)
	}

	// Piso relativo: 4 x maxBatchBytes (aqui 4 x 4 MiB = 16 MiB), acima do
	// absoluto, para o mutante que troca 4 por 2 não passar pelo piso de 8 MiB.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 4MiB", "maxBufferedBytes: 12MiB", "maxDiskBytes: 16777215B"),
		"replication.maxDiskBytes (16777215) must be at least four times replication.maxBatchBytes (4194304)")
	parseOK(t, replicationYAML("enabled: true", "maxBatchBytes: 4MiB", "maxBufferedBytes: 12MiB", "maxDiskBytes: 16MiB"))

	// O padrão de 2 GiB também é conferido: um lote de 600 MiB exigiria 2400 MiB.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 600MiB", "maxBufferedBytes: 1800MiB"),
		"replication.maxDiskBytes (2147483648) must be at least four times replication.maxBatchBytes (629145600)")

	// Valor que units.Size recusa e valor vazio explícito (que não pode virar
	// o padrão em silêncio).
	parseReplicationRefusal(t, replicationYAML("enabled: true", "maxDiskBytes: 1GB"), "replication.maxDiskBytes:")
	parseReplicationRefusal(t, replicationYAML("enabled: true", `maxDiskBytes: ""`), "replication.maxDiskBytes must not be empty")
}

func TestReplicationRejectsMaxDiskBytesAboveTheCeiling(t *testing.T) {
	// Teto: 256 GiB (um descritor aberto por segmento de até 64 MiB). Um
	// byte acima é recusado com a mensagem PRÓPRIA da guarda, e não com a de
	// outra (o units.Size aceita os dois valores); exatamente 256 GiB passa.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxDiskBytes: 274877906945B"),
		"replication.maxDiskBytes (274877906945) must be at most 274877906944 (256GiB): each feed segment keeps a file descriptor open")
	s := parseOK(t, replicationYAML("enabled: true", "maxDiskBytes: 256GiB"))
	if s.Replication.MaxDiskBytes != 274877906944 {
		t.Fatalf("MaxDiskBytes = %d, quero 274877906944", s.Replication.MaxDiskBytes)
	}
	// O caso que motivou o teto: 1 TiB seriam 16.384 descritores.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxDiskBytes: 1TiB"),
		"replication.maxDiskBytes (1099511627776) must be at most 274877906944 (256GiB)")
}

func TestReplicationBufferFloorIsThreeBatches(t *testing.T) {
	const want = "replication.maxBufferedBytes (3145727) must be at least three times replication.maxBatchBytes (1048576)"
	// Sem targets: o piso vale para todo master habilitado, não só para quem
	// publica em Kafka.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3145727B"), want)
	// Com targets, o mesmo piso (e não mais o dobro).
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3145727B", "targets: [metrics]"), want)
	// 2x, que era o piso antigo com targets, agora é recusado.
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 2MiB", "targets: [metrics]"),
		"replication.maxBufferedBytes (2097152) must be at least three times replication.maxBatchBytes (1048576)")
	// Exatamente o triplo é o piso aceito, com e sem targets.
	for _, lines := range [][]string{
		{"enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3MiB"},
		{"enabled: true", "maxBatchBytes: 1MiB", "maxBufferedBytes: 3MiB", "targets: [metrics]"},
	} {
		s := parseOK(t, replicationYAML(lines...))
		if s.Replication.MaxBufferedBytes != 3145728 || s.Replication.MaxBatchBytes != 1048576 {
			t.Fatalf("replication = %+v", s.Replication)
		}
	}
	// O piso é do master habilitado: um dataset com replicação desligada não
	// alimenta feed nenhum e segue só com a regra buffer >= lote.
	parseOK(t, replicationYAML("enabled: false", "maxBatchBytes: 1MiB", "maxBufferedBytes: 1MiB"))
	// Idem para o follower habilitado: o piso de 3x é do MASTER. O segundo
	// caso (lote de 600 MiB, que reprovaria o disco padrão de 2 GiB num
	// master) mostra que os pisos de disco também não se aplicam ao follower.
	for _, batch := range []string{"1MiB", "600MiB"} {
		s := parseOK(t, replicationYAML("enabled: true", "role: follower", "masterURL: https://master.example",
			"maxBatchBytes: "+batch, "maxBufferedBytes: "+batch))
		if s.Replication.Role != "follower" || s.Replication.MaxBufferedBytes != s.Replication.MaxBatchBytes {
			t.Fatalf("follower com lote %s: replication = %+v", batch, s.Replication)
		}
	}
}

func TestReplicationBufferFloorDoesNotOverflow(t *testing.T) {
	// 2^62 triplicado transborda int64; a comparação ingênua aprovaria o par.
	for _, extra := range [][]string{nil, {"targets: [metrics]"}} {
		lines := append([]string{
			"enabled: true",
			"maxBatchBytes: 4611686018427387904B", "maxBufferedBytes: 4611686018427387904B",
		}, extra...)
		parseReplicationRefusal(t, replicationYAML(lines...),
			"must be at least three times replication.maxBatchBytes (4611686018427387904)")
	}
}

func TestReplicationPublishHistoryDistinguishesAbsentFromFalse(t *testing.T) {
	absent := parseOK(t, replicationYAML("enabled: true"))
	if !absent.Replication.PublishHistory || absent.Replication.PublishHistoryDeclared {
		t.Fatalf("ausente: PublishHistory=%v Declared=%v, quero true/false",
			absent.Replication.PublishHistory, absent.Replication.PublishHistoryDeclared)
	}
	off := parseOK(t, replicationYAML("enabled: true", "publishHistory: false"))
	if off.Replication.PublishHistory || !off.Replication.PublishHistoryDeclared {
		t.Fatalf("false explícito: PublishHistory=%v Declared=%v, quero false/true",
			off.Replication.PublishHistory, off.Replication.PublishHistoryDeclared)
	}
	on := parseOK(t, replicationYAML("enabled: true", "publishHistory: true"))
	if !on.Replication.PublishHistory || !on.Replication.PublishHistoryDeclared {
		t.Fatalf("true explícito: PublishHistory=%v Declared=%v, quero true/true",
			on.Replication.PublishHistory, on.Replication.PublishHistoryDeclared)
	}
}

// historyReplicationYAML é o dataset base com um bloco `history:` mínimo, o mesmo dos
// testes de histórico.
func historyReplicationYAML(t *testing.T, replicationLines ...string) string {
	t.Helper()
	return baseYAML + "history:\n  expireAfterWrite: 7d\n" + "replication:\n  " + strings.Join(replicationLines, "\n  ") + "\n"
}

func TestReplicationWarnsPublishHistoryWithoutHistory(t *testing.T) {
	// Declarada (com qualquer valor) num dataset sem `history:`: avisa.
	for _, line := range []string{"publishHistory: true", "publishHistory: false"} {
		s := parseOK(t, replicationYAML("enabled: true", line))
		if !s.PublishHistoryWithoutHistory() {
			t.Fatalf("%q sem history: o aviso deveria se aplicar", line)
		}
	}
	// Ausente: nada a avisar, mesmo sem `history:`.
	if parseOK(t, replicationYAML("enabled: true")).PublishHistoryWithoutHistory() {
		t.Fatalf("publishHistory ausente não deveria avisar")
	}
	if parseOK(t, baseYAML).PublishHistoryWithoutHistory() {
		t.Fatalf("dataset sem bloco replication não deveria avisar")
	}
	// Com `history:` a chave tem efeito: sem aviso.
	withHistory := parseOK(t, historyReplicationYAML(t, "enabled: true", "publishHistory: false"))
	if withHistory.History == nil {
		t.Fatalf("o fixture deveria declarar history")
	}
	if withHistory.PublishHistoryWithoutHistory() {
		t.Fatalf("publishHistory com history: não deveria avisar")
	}
}

func TestReplicationPeriodicSnapshotRequiresTargetsAndRange(t *testing.T) {
	ok := func(value string) time.Duration {
		t.Helper()
		s := parseOK(t, replicationYAML("enabled: true", "targets: [metrics]", "periodicSnapshot: "+value))
		return s.Replication.PeriodicSnapshot
	}
	// Fronteiras inclusivas.
	if got := ok("1h"); got != time.Hour {
		t.Fatalf("1h = %v", got)
	}
	if got := ok("8760h"); got != 365*24*time.Hour {
		t.Fatalf("8760h = %v", got)
	}
	if got := ok("90m"); got != 90*time.Minute {
		t.Fatalf("90m = %v", got)
	}

	const invalid = "invalid (use whole seconds from 1h to 8760h"
	for _, value := range []string{
		"3599s",    // um segundo abaixo de 1h
		"8760h1s",  // um segundo acima de 365 d
		"0s",       // zero não é "desligado": desligar é omitir a chave
		"-2h",      // negativo
		"1h0m0.5s", // fração de segundo: recusa, não arredonda
		"3600",     // número sem unidade
		"1d",       // ParseDuration não conhece dia
		`""`,       // vazio explícito
		"soon",     // lixo
	} {
		parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: [metrics]", "periodicSnapshot: "+value), invalid)
	}

	// Sem targets, mesmo com valor válido.
	parseReplicationRefusal(t, replicationYAML("enabled: true", "periodicSnapshot: 24h"),
		"replication.periodicSnapshot requires replication.targets")
}
