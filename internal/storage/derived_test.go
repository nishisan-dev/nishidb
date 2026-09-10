package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// põeUmaLinha grava um record e faz checkpoint, para que o manifest passe a
// declarar RowCount > 0 — que é a condição sob a qual a recusa vale.
func põeUmaLinha(t *testing.T, s *Store) {
	t.Helper()
	// O Put recusa fieldID fora do dicionário: registrar "id" aqui é o que lhe
	// dá o ID 1 que fldID nomeia.
	if _, err := s.FieldDict().LookupOrCreate([]byte("id")); err != nil {
		t.Fatalf("registrando id: %v", err)
	}
	payload := mustAppendField(t, nil, fldID, types.Value{Type: types.TypeString, Bytes: []byte("A")})
	if _, _, err := s.Put([]byte("A"), 1, 0, payload, nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

// Acrescentar uma coluna derivada a um dataset que JÁ TEM linhas é recusado.
//
// A extração roda na ingestão: as linhas gravadas antes de a coluna existir
// não a têm, e `WHERE derivada = 'x'` as omitiria sem erro, sem log e sem
// métrica — resultado incompleto com cara de completo. É a mesma recusa que
// já vale para índice bitmap declarado sobre dataset com linhas.
func TestColunaDerivadaNovaEmDatasetComLinhasERecusada(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	põeUmaLinha(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, DerivedColumnNames: []string{"probe_ip"}})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("Open = %v, quer ErrDatasetCorrupt", err)
	}
	for _, quer := range []string{"probe_ip", "recrie"} {
		if !strings.Contains(err.Error(), quer) {
			t.Errorf("erro = %q, quer conter %q", err, quer)
		}
	}
}

// O dataset CRIADO com a coluna derivada reabre normalmente depois de ter
// linhas — a coluna existe no dicionário desde a primeira subida.
//
// Sem esta metade a recusa acima tornaria a feature inutilizável: bastaria um
// evento para o dataset nunca mais abrir. Foi assim que a primeira versão
// desta guarda saiu, e foi a verificação com o binário que a pegou.
func TestDatasetCriadoComDerivadaReabreDepoisDeTerLinhas(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 1 << 20, DerivedColumnNames: []string{"probe_ip"}}

	s := openStore(t, opts)
	põeUmaLinha(t, s)
	// Registra a derivada como o Compile faria num dataset criado com extract.
	if _, err := s.FieldDict().LookupOrCreate([]byte("probe_ip")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reabrir o dataset criado com extract: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// Dataset VAZIO aceita a declaração: é exatamente o momento em que ela deve
// ser feita.
func TestColunaDerivadaEmDatasetVazioEAceita(t *testing.T) {
	openStore(t, Options{DerivedColumnNames: []string{"probe_ip"}})
}
