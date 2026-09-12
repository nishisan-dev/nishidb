package schema

import (
	"strings"
	"testing"
	"time"
)

// TestContadorAceitaOBlocoDeEmissao: a chave `emit:` passa a ser conhecida.
func TestContadorAceitaOBlocoDeEmissao(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    emit:
      every: 30s
      file: autenticados.ndjson
`)), BuiltinDefaults)
	if err != nil {
		t.Fatalf("o bloco emit tem de ser aceito: %v", err)
	}
}

// TestEmissaoDeclaradaAplicaOsDefaultsDeRotacao: maxBytes e keep têm default
// porque um arquivo sem teto é disco que cresce para sempre — e não existe
// grafia para "nunca rotacionar", pela mesma razão que history: recusa um bloco
// sem política. O `file` também tem default, derivado do nome do contador, que
// é único no dataset e por isso não colide com o de outro.
func TestEmissaoDeclaradaAplicaOsDefaultsDeRotacao(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: com_default
    groupBy: [olt]
    emit:
      every: 30s
  - name: com_tudo
    groupBy: [slot]
    emit:
      every: 1m
      file: series_da_pon.ndjson
      maxBytes: 16MiB
      keep: 3
`)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	a := spec.Counters[0].Emit
	if a == nil {
		t.Fatal("emit declarado não chegou à spec")
	}
	if a.Every != 30*time.Second {
		t.Errorf("every = %v, quero 30s", a.Every)
	}
	if got, quero := a.File, "com_default"+DefaultEmitFileExt; got != quero {
		t.Errorf("file = %q, quero %q derivado do nome do contador", got, quero)
	}
	if a.MaxBytes != DefaultEmitMaxBytes || a.Keep != DefaultEmitKeep {
		t.Errorf("defaults de rotação não aplicados: maxBytes=%d keep=%d", a.MaxBytes, a.Keep)
	}
	b := spec.Counters[1].Emit
	if b.Every != time.Minute || b.File != "series_da_pon.ndjson" || b.MaxBytes != 16<<20 || b.Keep != 3 {
		t.Errorf("emit declarado por inteiro = %+v", b)
	}
}

// TestEmissaoAusenteDeixaOContadorSemEmissao.
func TestEmissaoAusenteDeixaOContadorSemEmissao(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
`)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Counters[0].Emit != nil {
		t.Fatalf("sem bloco, Emit tem de ser nil; veio %+v", spec.Counters[0].Emit)
	}
}

// TestEmissaoRecusaDoisContadoresNoMesmoArquivo: dois escritores no mesmo
// arquivo intercalariam séries de contadores diferentes no mesmo fluxo, e cada
// um rotacionaria por baixo do outro. A mensagem nomeia o OUTRO contador,
// porque é ele que o operador precisa achar.
func TestEmissaoRecusaDoisContadoresNoMesmoArquivo(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: primeiro
    groupBy: [olt]
    emit:
      every: 30s
      file: compartilhado.ndjson
  - name: segundo
    groupBy: [slot]
    emit:
      every: 30s
      file: compartilhado.ndjson
`)), BuiltinDefaults)
	if err == nil {
		t.Fatal("dois contadores no mesmo arquivo têm de ser recusados")
	}
	if !strings.Contains(err.Error(), "counters[1].emit.file") || !strings.Contains(err.Error(), `"primeiro"`) {
		t.Errorf("a mensagem tem de trazer o caminho e o outro contador: %v", err)
	}
}

// TestEmissaoRecusas cobre a tabela de admissão do bloco. Cada caso é uma
// recusa que, sem ela, deixaria o operador com configuração que não faz o que
// ele escreveu.
func TestEmissaoRecusas(t *testing.T) {
	cases := []struct {
		name, block, want string
	}{
		{"não é mapa", "emit:\n      - every: 30s\n", "deve ser um mapa"},
		{"chave desconhecida", "emit:\n      every: 30s\n      arquivo: x.ndjson\n", "chave desconhecida"},
		{"every ausente", "emit:\n      file: x.ndjson\n", "every obrigatório"},
		{"every zero", "emit:\n      every: 0\n", "mínimo"},
		{"every abaixo do mínimo", "emit:\n      every: 5s\n", "menor que o mínimo"},
		{"every inválido", "emit:\n      every: trinta\n", "every"},
		{"every negativo", "emit:\n      every: -30s\n", "every"},
		{"file com caminho", "emit:\n      every: 30s\n      file: series/x.ndjson\n", "é um caminho"},
		{"file com ponto inicial", "emit:\n      every: 30s\n      file: .oculto\n", "ponto"},
		{"file subindo diretório", "emit:\n      every: 30s\n      file: ..\n", "ponto"},
		{"file com alfabeto inválido", "emit:\n      every: 30s\n      file: \"x y.ndjson\"\n", "esperado"},
		{"maxBytes abaixo do mínimo", "emit:\n      every: 30s\n      maxBytes: 512KiB\n", "menor que o mínimo"},
		{"maxBytes inválido", "emit:\n      every: 30s\n      maxBytes: muito\n", "maxBytes"},
		{"keep zero", "emit:\n      every: 30s\n      keep: 0\n", "keep 0 fora"},
		{"keep negativo", "emit:\n      every: 30s\n      keep: -1\n", "fora de"},
		{"keep acima do teto", "emit:\n      every: 30s\n      keep: 101\n", "fora de"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			block := "counters:\n  - name: x\n    groupBy: [olt]\n    " + c.block
			_, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults)
			if err == nil {
				t.Fatalf("aceitou:\n%s", block)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("erro %q não menciona %q", err, c.want)
			}
		})
	}
}
