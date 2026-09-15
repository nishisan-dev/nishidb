package metricwindow

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
)

// Os testes deste arquivo precisam dos seams internos (writeTrace, openRegion),
// e por isso vivem no pacote. Nenhum deles é paralelo: os seams são variáveis
// de pacote.

type tracedWrite struct {
	off int64
	v   uint64
}

func traceWrites(t *testing.T) *[]tracedWrite {
	t.Helper()
	var writes []tracedWrite
	writeTrace = func(off int64, v uint64) { writes = append(writes, tracedWrite{off, v}) }
	t.Cleanup(func() { writeTrace = nil })
	return &writes
}

func openInternal(t *testing.T, path string) *File {
	t.Helper()
	f, _, err := Open(path, Header{BucketCount: 4, BucketWidthMs: 5_000, Source: 1}, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestWriteBucketZeroesWordZeroFirstAndWritesItLast trava a ordem das palavras
// dentro da entrada de balde, que é o que torna a escrita parcial num crash
// um prefixo inofensivo (ver WriteBucket).
func TestWriteBucketZeroesWordZeroFirstAndWritesItLast(t *testing.T) {
	f := openInternal(t, filepath.Join(t.TempDir(), "w.mwin"))
	const keyHash = 0xdeadbeefcafef00d
	f.SetIdentity(5, Identity{KeyHash: keyHash, ValidFromEpoch: 9})
	writes := traceWrites(t)

	f.WriteBucket(3, 5, Bucket{Epoch: 0x01020304, Owner: 0x5ab1, Count: 0x0708, Sum: 11, Min: -22, Max: 33})

	base := f.bucketOff(3, 5)
	want := []tracedWrite{
		{base, 0},
		{base + 8, 11},
		{base + 16, uint64(0xffffffffffffffea)}, // -22
		{base + 24, 33},
		{base, 0x01020304<<32 | 0x5ab1<<16 | 0x0708},
	}
	if len(*writes) != len(want) {
		t.Fatalf("WriteBucket escreveu %d palavras (%+v), quer %d", len(*writes), *writes, len(want))
	}
	for i := range want {
		if (*writes)[i] != want[i] {
			t.Errorf("escrita %d = {off %d, v %#x}, quer {off %d, v %#x}",
				i, (*writes)[i].off, (*writes)[i].v, want[i].off, want[i].v)
		}
	}
}

// TestSetIdentityWritesValidFromEpochBeforeKeyHash trava a ordem da identidade:
// o hash é o commit (ver SetIdentity).
func TestSetIdentityWritesValidFromEpochBeforeKeyHash(t *testing.T) {
	f := openInternal(t, filepath.Join(t.TempDir(), "w.mwin"))
	writes := traceWrites(t)

	f.SetIdentity(7, Identity{KeyHash: 0xaaaa, ValidFromEpoch: 0xbbbb})

	base := f.identityOff(7)
	want := []tracedWrite{{base + 8, 0xbbbb}, {base, 0xaaaa}}
	if len(*writes) != len(want) || (*writes)[0] != want[0] || (*writes)[1] != want[1] {
		t.Fatalf("SetIdentity escreveu %+v, quer %+v", *writes, want)
	}
}

// TestFailedReopenAfterGrowthClosesTheFileAndEnsureCapacityReturnsErrClosed
// reproduz o segundo desfecho de EnsureCapacity: o rename aconteceu e a
// reabertura falhou. O File tem de ficar fechado, e a chamada seguinte tem de
// voltar com ErrClosed em tempo finito — com capacidade zero, o laço de dobra
// nunca terminaria.
func TestFailedReopenAfterGrowthClosesTheFileAndEnsureCapacityReturnsErrClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.mwin")
	f := openInternal(t, path)
	f.SetIdentity(9, Identity{KeyHash: 1, ValidFromEpoch: 2})

	openRegion = func(p string, size int64) (*mmap.Region, error) {
		if p == path {
			return nil, errors.New("injected reopen failure")
		}
		return mmap.Open(p, size)
	}
	t.Cleanup(func() { openRegion = mmap.Open })

	err := f.EnsureCapacity(1024)
	if err == nil || !strings.Contains(err.Error(), "injected reopen failure") {
		t.Fatalf("EnsureCapacity = %v, quer o erro da reabertura", err)
	}
	if f.Capacity() != 0 || f.SizeBytes() != 0 {
		t.Fatalf("File depois da reabertura falha: capacidade %d, tamanho %d; quer fechado", f.Capacity(), f.SizeBytes())
	}

	done := make(chan error, 1)
	go func() { done <- f.EnsureCapacity(4096) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("EnsureCapacity no File fechado = %v, quer ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EnsureCapacity no File fechado não voltou em 5 s: laço de dobra partindo de capacidade zero")
	}
	if err := f.Msync(); !errors.Is(err, ErrClosed) {
		t.Errorf("Msync no File fechado = %v, quer ErrClosed", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("Close no File que o crescimento fechou = %v, quer nil", err)
	}
	func() {
		defer func() {
			if msg, _ := recover().(string); !strings.Contains(msg, "use of closed file") {
				t.Errorf("Identity no File fechado: pânico %q, quer mensagem de File fechado", msg)
			}
		}()
		f.Identity(9)
	}()

	// O arquivo em disco é o novo, e válido.
	openRegion = mmap.Open
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if ro.Capacity() != 2048 || ro.Identity(9) != (Identity{KeyHash: 1, ValidFromEpoch: 2}) {
		t.Errorf("arquivo em disco: capacidade %d, identidade %+v; quer o novo, com o id 9", ro.Capacity(), ro.Identity(9))
	}
}
