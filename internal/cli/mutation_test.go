package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Os comandos MUTANTES do §96 (Fase 11, Task B5, decisão D3 do roadmap) —
// `compact` e `checkpoint` — nascem remotos pela MESMA razão dos três
// comandos de leitura (o flock do dataset é exclusivo por processo), e esta
// suíte afere a INTERFACE do CLI: endereço, dataset opcional, tradução de
// erro. O comportamento de fundo das duas rotas (papel ADMIN, 409
// concorrente, o que cada uma faz no storage) já está travado em
// internal/server; aqui o oráculo é "o comando fala com o servidor e traduz
// a resposta certo", não uma segunda cópia daquela suíte.

// configComServidorSemCompaction é o gêmeo de configComServidor
// (query_remota_test.go) com storage.compaction.enabled: false — o caso de
// uso central de `compact`: o operador desligou o laço automático porque,
// com ele de pé, TODO POST /v1/compact recebe 409 (compCancela nunca é nil
// enquanto o laço existe, mesmo dormindo entre passadas).
const configComServidorSemCompaction = `version: 1
storage:
  path: %s
  compaction:
    enabled: false
datasets:
  directory: %s
server:
  bind: 127.0.0.1
  port: %d
`

func novoAmbienteRemotoSemCompaction(t *testing.T) *ambienteRemoto {
	t.Helper()
	a := novoAmbiente(t)
	porta := portaLivre(t)
	corpo := fmt.Sprintf(configComServidorSemCompaction,
		filepath.Join(a.dir, "dados"), filepath.Join(a.dir, "datasets"), porta)
	if err := os.WriteFile(a.config, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return &ambienteRemoto{ambiente: a, porta: porta}
}

// checkpoint funciona INDEPENDENTE de storage.compaction: a rota não disputa
// o single-flight da compaction, então o servidor padrão (compaction ligada
// por default) já basta para provar a ponta a ponta.
func TestCheckpointRemotoFuncionaComOServidorNoAr(t *testing.T) {
	a := servidorComDadosNoAr(t, 5)

	if code := a.roda("checkpoint", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d, quer 0: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	if !strings.Contains(saida, "radius_sessions") {
		t.Errorf("a saída não nomeia o dataset:\n%s", saida)
	}
	if !strings.Contains(saida, "DURA") { // "DURAÇÃO" — sem acento no grep para não depender de normalização
		t.Errorf("a saída não traz a duração:\n%s", saida)
	}
}

// compact funciona com storage.compaction.enabled: false — o caso de uso
// central do modo manual. O dataset filtrado pelo argumento posicional (em
// vez de "todos") exercita o outro lado do contrato: ausente = todos,
// presente = só ele.
func TestCompactRemotoFuncionaComOServidorNoArEEnabledFalse(t *testing.T) {
	a := sobeComEntrada(t, novoAmbienteRemotoSemCompaction(t), corpus(t, 5))

	if code := a.roda("compact", "--server", a.endereco(), "radius_sessions"); code != 0 {
		t.Fatalf("exit = %d, quer 0: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	if !strings.Contains(saida, "radius_sessions") {
		t.Errorf("a saída não nomeia o dataset:\n%s", saida)
	}
	if !strings.Contains(saida, "SEGMENTOS") {
		t.Errorf("a saída não traz a tabela esperada:\n%s", saida)
	}
}

// TestCompactRemotoNaConfigDefaultEventualmenteSai0 é o Critical da revisão
// da B5, visto pelo CLI: o laço automático de pé (o default — storage.
// compaction ausente vale enabled: true) NÃO pode deixar `compact --server`
// morto para sempre — só sai 1 (COMPACTION_BUSY) enquanto uma passada está
// de fato em voo, e a janela ocupada num dataset sem garbage dura
// microssegundos. Tentativas imediatas, sem sleep de corrida: se a rota
// estivesse morta de verdade, nenhuma teria sucesso e o teste falharia alto
// no teto — a mesma técnica de
// TestCompactComAdminNaConfigDefaultFuncionaSemPassadaEmVoo (internal/server),
// agora ponta a ponta pelo binário do CLI.
func TestCompactRemotoNaConfigDefaultEventualmenteSai0(t *testing.T) {
	a := servidorComDadosNoAr(t, 5)

	const tentativas = 200
	var code int
	for i := 0; i < tentativas; i++ {
		code = a.roda("compact", "--server", a.endereco())
		if code == 0 {
			break
		}
	}
	if code != 0 {
		t.Fatalf("exit = %d após %d tentativas imediatas: a rota parece permanentemente ocupada na config default: %s",
			code, tentativas, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "radius_sessions") {
		t.Errorf("a saída não nomeia o dataset:\n%s", a.stdout.String())
	}
}

// Sem --server não há para onde mandar o POST — mesmo argumento dos três
// comandos de leitura (§96 + débito 54), agora para as duas rotas mutantes.
func TestCompactECheckpointExigemOServidor(t *testing.T) {
	a := novoAmbienteRemoto(t, 100000)

	for _, args := range [][]string{
		{"compact"},
		{"checkpoint"},
	} {
		if code := a.roda(args...); code != 1 {
			t.Errorf("%v: exit = %d, quer 1", args, code)
		}
		if !strings.Contains(a.stderr.String(), "--server") {
			t.Errorf("%v: a mensagem não ensina o caminho:\n%s", args, a.stderr.String())
		}
	}
}

// Mais de um dataset posicional é erro de USO, não uma tentativa de
// compactar/checkpointar os dois: o contrato é UM dataset por chamada, ou
// nenhum (todos).
func TestCompactECheckpointRecusamMaisDeUmDataset(t *testing.T) {
	a := novoAmbienteRemoto(t, 100000)

	for _, args := range [][]string{
		{"compact", "--server", a.endereco(), "d1", "d2"},
		{"checkpoint", "--server", a.endereco(), "d1", "d2"},
	} {
		if code := a.roda(args...); code != 1 {
			t.Errorf("%v: exit = %d, quer 1", args, code)
		}
	}
}

// O texto de uso é o único lugar em que o operador descobre que os dois
// comandos existem — mesma razão da suíte irmã (TestOUsoAnunciaOsTresComandosDeLeitura),
// que este teste NÃO toca: é aditivo, não substitui a asserção existente.
func TestOUsoAnunciaCompactECheckpoint(t *testing.T) {
	a := novoAmbiente(t)
	if code := a.roda("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, cmd := range []string{"nishidb compact", "nishidb checkpoint"} {
		if !strings.Contains(a.stdout.String(), cmd) {
			t.Errorf("o texto de uso não anuncia %q", cmd)
		}
	}
}
