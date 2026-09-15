package metricwindow_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

// Os testes vivem no pacote externo de propósito: o golden de bytes não tem
// como reaproveitar a função de offset do pacote para gerar o esperado — o
// endereço sai da conta feita à mão, e um golden autocumprido não provaria nada.
// Os que precisam dos seams internos (ordem de escrita, reabertura falha) estão
// em metricwindow_internal_test.go.
//
// NÃO VERIFICADO aqui: a DURABILIDADE do msync do tmp e do fsync do diretório,
// e o desfecho de uma falha no fsync do diretório na CRIAÇÃO e no RESET do
// Open. O único fsync de diretório com falha real injetada é o do crescimento
// (diretório sem permissão de leitura), e nenhum teste simula a queda da
// máquina que decidiria se o rename e as páginas chegaram de fato ao disco.

var baseHeader = metricwindow.Header{BucketCount: 4, BucketWidthMs: 75_000, Source: 2}

func openFile(t *testing.T, path string, h metricwindow.Header, minCapacity, maxCapacity uint32) (*metricwindow.File, bool) {
	t.Helper()
	f, reset, err := metricwindow.Open(path, h, minCapacity, maxCapacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, reset
}

func closeFile(t *testing.T, f *metricwindow.File) {
	t.Helper()
	if err := f.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func identityFor(id uint32) metricwindow.Identity {
	return metricwindow.Identity{KeyHash: uint64(id) * 0x9e3779b97f4a7c15, ValidFromEpoch: uint64(id) + 1_000}
}

// bucketFor é um balde distinto por (slot, id), com os dois sinais e valores
// largos no meio, para que uma coluna copiada no lugar da outra, ou uma
// palavra trocada, nunca coincida com o esperado. Owner é a marca do dono dos
// dados, identityFor(id), que o chamador passa e WriteBucket grava como veio.
func bucketFor(slot, id uint32) metricwindow.Bucket {
	return metricwindow.Bucket{
		Epoch: slot*100_000 + id,
		Owner: metricwindow.OwnerTag(identityFor(id).KeyHash),
		Count: uint16(id + slot + 1),
		Sum:   -int64(id) * int64(slot+1),
		Min:   int64(slot) - int64(id)<<32,
		Max:   int64(id)<<20 | int64(slot),
	}
}

// writeOwned grava identidade e balde do id na ordem que o amostrador usa.
func writeOwned(f *metricwindow.File, slot, id uint32) {
	f.SetIdentity(id, identityFor(id))
	f.WriteBucket(slot, id, bucketFor(slot, id))
}

func assertNoTmp(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("sobrou o tmp %s no diretório", e.Name())
		}
	}
}

func TestFileRoundTripsBucketsAndIdentityAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.mwin")
	f, reset := openFile(t, path, baseHeader, 1, 4096)
	if reset {
		t.Fatal("arquivo novo não pode sair como reset")
	}
	if f.Header() != baseHeader {
		t.Fatalf("Header() = %+v, quer %+v", f.Header(), baseHeader)
	}
	if f.Capacity() != 1024 {
		t.Fatalf("capacidade = %d, quer o piso 1024", f.Capacity())
	}
	// 4096 + 1024*16 + 4*1024*32 = 4096 + 16384 + 131072.
	if f.SizeBytes() != 151_552 {
		t.Fatalf("SizeBytes = %d, quer 151552", f.SizeBytes())
	}

	identity5 := metricwindow.Identity{KeyHash: 0xdeadbeefcafef00d, ValidFromEpoch: 123}
	f.SetIdentity(5, identity5)
	// A marca é gravada como recebida, mesmo divergindo da identidade corrente
	// (OwnerTag(0xdeadbeefcafef00d) = 0x5ab1): quem decide a marca é quem sabe
	// de quem são os dados.
	edge := metricwindow.Bucket{Epoch: math.MaxUint32, Owner: 0x1234, Count: math.MaxUint16, Sum: -40, Min: math.MinInt64, Max: math.MaxInt64}
	f.WriteBucket(3, 5, edge)
	writeOwned(f, 0, 1023)
	closeFile(t, f)

	check := func(t *testing.T, g *metricwindow.File) {
		t.Helper()
		if got := g.Identity(5); got != identity5 {
			t.Errorf("identidade do id 5 = %+v", got)
		}
		if got := g.Bucket(3, 5); got != edge {
			t.Errorf("balde (3, 5) = %+v, quer %+v", got, edge)
		}
		if got := g.Bucket(0, 1023); got != bucketFor(0, 1023) {
			t.Errorf("balde (0, 1023) = %+v, quer %+v", got, bucketFor(0, 1023))
		}
		if got := g.Identity(1023); got != identityFor(1023) {
			t.Errorf("identidade do id 1023 = %+v", got)
		}
		// Vizinhos intactos: o id ao lado, o slot ao lado e o primeiro id da
		// coluna seguinte.
		for _, n := range [][2]uint32{{3, 4}, {3, 6}, {2, 5}, {1, 0}, {3, 1023}} {
			if got := g.Bucket(n[0], n[1]); got != (metricwindow.Bucket{}) {
				t.Errorf("balde vizinho (%d, %d) = %+v, quer zero", n[0], n[1], got)
			}
		}
	}

	g, reset := openFile(t, path, baseHeader, 1, 4096)
	if reset {
		t.Fatal("reabrir com o mesmo cabeçalho não pode resetar")
	}
	check(t, g)
	closeFile(t, g)

	ro, err := metricwindow.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if ro.Header() != baseHeader || ro.Capacity() != 1024 {
		t.Fatalf("somente-leitura: cabeçalho %+v, capacidade %d", ro.Header(), ro.Capacity())
	}
	check(t, ro)
	if err := ro.EnsureCapacity(2000); err == nil {
		t.Error("EnsureCapacity na abertura somente-leitura tem de recusar")
	}
}

// TestOwnerTagFoldsTheFourHalvesOfTheHash trava a dobra por valor, calculado
// fora do pacote. Os hashes com um único bit em cada metade provam que é
// dobra, e não truncamento: truncar devolveria zero para 1<<16, 1<<32 e 1<<48.
func TestOwnerTagFoldsTheFourHalvesOfTheHash(t *testing.T) {
	cases := []struct {
		keyHash uint64
		want    uint16
	}{
		{0xdeadbeefcafef00d, 0x5ab1},
		{0x1111222233334444, 0x4444},
		{1, 0x0001},
		{1 << 16, 0x0001},
		{1 << 32, 0x0001},
		{1 << 48, 0x0001},
		{math.MaxUint64, 0x0000},
	}
	for _, tc := range cases {
		if got := metricwindow.OwnerTag(tc.keyHash); got != tc.want {
			t.Errorf("OwnerTag(%#016x) = %#04x, quer %#04x", tc.keyHash, got, tc.want)
		}
	}
}

// TestBucketOwnerDisagreesWithAStaleIdentityOfAReusedID simula as duas
// combinações de página que uma queda pode deixar num id reaproveitado — a
// identidade e os baldes moram em páginas diferentes, e a ordem de escrita não
// protege nada entre elas.
func TestBucketOwnerDisagreesWithAStaleIdentityOfAReusedID(t *testing.T) {
	const k1, k2 = 0xdeadbeefcafef00d, 0x1111222233334444
	if metricwindow.OwnerTag(k1) == metricwindow.OwnerTag(k2) || metricwindow.OwnerTag(k1) == 0 || metricwindow.OwnerTag(k2) == 0 {
		t.Fatal("as chaves do teste precisam de marcas distintas e não nulas")
	}

	t.Run("bucket page of the new owner, identity page of the old", func(t *testing.T) {
		f, _ := openFile(t, filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: 10})
		f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 15, Owner: metricwindow.OwnerTag(k1), Count: 1, Sum: 1, Min: 1, Max: 1})
		// O id passa a K2, que grava um balde.
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k2, ValidFromEpoch: 20})
		f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 25, Owner: metricwindow.OwnerTag(k2), Count: 2, Sum: 2, Min: 2, Max: 2})
		// A queda leva a página de identidade de volta a K1.
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: 10})

		got := f.Bucket(3, 5)
		if got.Owner == metricwindow.OwnerTag(f.Identity(5).KeyHash) {
			t.Fatalf("balde de K2 passa como de K1: marca %#04x = OwnerTag(K1)", got.Owner)
		}
		if got.Owner != metricwindow.OwnerTag(k2) {
			t.Errorf("marca do balde = %#04x, quer OwnerTag(K2) = %#04x", got.Owner, metricwindow.OwnerTag(k2))
		}
	})

	t.Run("identity page of the new owner, bucket page of the old", func(t *testing.T) {
		f, _ := openFile(t, filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: 10})
		f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 15, Owner: metricwindow.OwnerTag(k1), Count: 1, Sum: 1, Min: 1, Max: 1})
		// A identidade de K2 chegou ao disco; o balde de K2 não.
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k2, ValidFromEpoch: 20})

		got, identity := f.Bucket(3, 5), f.Identity(5)
		if got.Owner == metricwindow.OwnerTag(identity.KeyHash) {
			t.Errorf("balde de K1 com a marca de K2: %#04x", got.Owner)
		}
		if uint64(got.Epoch) >= identity.ValidFromEpoch {
			t.Errorf("balde de K1 com época %d dentro do validFromEpoch %d de K2", got.Epoch, identity.ValidFromEpoch)
		}
	})
}

// TestWriteBucketHonorsTheOwnerTagItReceives: a marca gravada é a do dono dos
// DADOS, passada pelo chamador, e não a da identidade corrente. É o que faz o
// balde aberto do dono antigo, gravado depois da troca de dono, ser descartado
// na leitura em vez de atribuído ao novo em silêncio.
func TestWriteBucketHonorsTheOwnerTagItReceives(t *testing.T) {
	const k1, k2 = 0xdeadbeefcafef00d, 0x1111222233334444

	t.Run("open bucket of the old owner written after the identity changed", func(t *testing.T) {
		f, _ := openFile(t, filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: 10})
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k2, ValidFromEpoch: 20})
		f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 21, Owner: metricwindow.OwnerTag(k1), Count: 3, Sum: 3, Min: 1, Max: 1})

		got, identity := f.Bucket(3, 5), f.Identity(5)
		if uint64(got.Epoch) < identity.ValidFromEpoch {
			t.Fatal("o cenário exige época >= validFromEpoch: só a marca pode descartar este balde")
		}
		if got.Owner != metricwindow.OwnerTag(k1) {
			t.Errorf("marca gravada = %#04x, quer a recebida OwnerTag(K1) = %#04x", got.Owner, metricwindow.OwnerTag(k1))
		}
		if got.Owner == metricwindow.OwnerTag(identity.KeyHash) {
			t.Errorf("balde de K1 saiu com a marca da identidade corrente (K2): o leitor o atribuiria a K2")
		}
	})

	t.Run("rewriting a bucket read back preserves its tag", func(t *testing.T) {
		f, _ := openFile(t, filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: 10})
		f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 15, Owner: metricwindow.OwnerTag(k1), Count: 2, Sum: 2, Min: 1, Max: 1})
		f.SetIdentity(5, metricwindow.Identity{KeyHash: k2, ValidFromEpoch: 20})

		b := f.Bucket(3, 5)
		f.WriteBucket(3, 5, b)
		f.WriteBucket(1, 5, b)
		for _, slot := range []uint32{3, 1} {
			if got := f.Bucket(slot, 5); got != b || got.Owner != metricwindow.OwnerTag(k1) {
				t.Errorf("slot %d depois de regravar = %+v, quer %+v com a marca de K1", slot, got, b)
			}
		}
	})
}

// TestOpenOfAValidFileRemovesALeftoverTemporaryFileWithoutRewriting: a
// remoção do tmp residual é promessa do PRÓPRIO Open, e não só do build. O
// arquivo existe, é válido e já tem a capacidade pedida, então não há reset
// nem crescimento — e o mesmo inode antes e depois prova que nenhuma reescrita
// aconteceu.
func TestOpenOfAValidFileRemovesALeftoverTemporaryFileWithoutRewriting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.mwin")
	f, _ := openFile(t, path, baseHeader, 1, 4096)
	writeOwned(f, 2, 7)
	closeFile(t, f)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", []byte("resto de uma reescrita interrompida"), 0o644); err != nil {
		t.Fatal(err)
	}

	g, reset := openFile(t, path, baseHeader, 1, 4096)
	if reset || g.Capacity() != 1024 {
		t.Fatalf("abertura: reset=%v, capacidade %d; quer adoção sem reset na capacidade 1024", reset, g.Capacity())
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("o Open reescreveu o arquivo: o cenário precisa de uma abertura sem build")
	}
	if got := g.Bucket(2, 7); got != bucketFor(2, 7) {
		t.Errorf("balde (2, 7) = %+v, quer %+v", got, bucketFor(2, 7))
	}
	assertNoTmp(t, dir)
}

func TestOpenResetsTheFileWhenTheHeaderDiverges(t *testing.T) {
	cases := []struct {
		name string
		h    metricwindow.Header
	}{
		{"bucket count", metricwindow.Header{BucketCount: 5, BucketWidthMs: baseHeader.BucketWidthMs, Source: baseHeader.Source}},
		{"bucket width", metricwindow.Header{BucketCount: baseHeader.BucketCount, BucketWidthMs: 60_000, Source: baseHeader.Source}},
		{"source", metricwindow.Header{BucketCount: baseHeader.BucketCount, BucketWidthMs: baseHeader.BucketWidthMs, Source: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "w.mwin")
			f, _ := openFile(t, path, baseHeader, 1, 4096)
			writeOwned(f, 3, 5)
			closeFile(t, f)

			g, reset := openFile(t, path, tc.h, 1, 4096)
			if !reset {
				t.Fatalf("cabeçalho divergente em %s tem de devolver reset=true", tc.name)
			}
			if g.Header() != tc.h {
				t.Fatalf("Header() = %+v, quer %+v", g.Header(), tc.h)
			}
			if got := g.Identity(5); got != (metricwindow.Identity{}) {
				t.Errorf("identidade sobreviveu ao reset: %+v", got)
			}
			if got := g.Bucket(3, 5); got != (metricwindow.Bucket{}) {
				t.Errorf("balde sobreviveu ao reset: %+v", got)
			}
			closeFile(t, g)
			assertNoTmp(t, dir)

			// O reset tem de ter gravado o cabeçalho novo: a abertura seguinte
			// com a forma nova adota o arquivo.
			if _, reset := openFile(t, path, tc.h, 1, 4096); reset {
				t.Error("a abertura depois do reset voltou a resetar: o cabeçalho novo não foi gravado")
			}
		})
	}
}

func TestOpenResetsAndOpenReadOnlyRefusesAnUnreadableHeader(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		// reserved: coberto só pelo crc, nenhum campo comparado muda.
		{"reserved byte", func(t *testing.T, path string) { flipByte(t, path, 12) }},
		{"crc byte", func(t *testing.T, path string) { flipByte(t, path, 33) }},
		{"magic byte", func(t *testing.T, path string) { flipByte(t, path, 0) }},
		// Versão 2 com crc RECALCULADO: só a checagem de versão recusa.
		{"version 2 with a valid crc", func(t *testing.T, path string) { rewriteVersion(t, path, 2) }},
		{"truncated columns", func(t *testing.T, path string) { truncate(t, path, 4096+100) }},
		{"grown beyond the declared size", func(t *testing.T, path string) { appendGarbage(t, path, 4096) }},
		{"empty file", func(t *testing.T, path string) { truncate(t, path, 0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "w.mwin")
			f, _ := openFile(t, path, baseHeader, 1, 4096)
			writeOwned(f, 3, 5)
			closeFile(t, f)
			tc.damage(t, path)

			if ro, err := metricwindow.OpenReadOnly(path); !errors.Is(err, metricwindow.ErrCorruptHeader) {
				if ro != nil {
					_ = ro.Close()
				}
				t.Fatalf("OpenReadOnly = %v, quer ErrCorruptHeader", err)
			}

			g, reset := openFile(t, path, baseHeader, 1, 4096)
			if !reset {
				t.Fatal("cabeçalho ilegível tem de devolver reset=true")
			}
			if got := g.Bucket(3, 5); got != (metricwindow.Bucket{}) {
				t.Errorf("balde sobreviveu ao reset: %+v", got)
			}
			if got := g.Identity(5); got != (metricwindow.Identity{}) {
				t.Errorf("identidade sobreviveu ao reset: %+v", got)
			}
			if g.SizeBytes() != 151_552 {
				t.Errorf("tamanho depois do reset = %d, quer exatamente 151552", g.SizeBytes())
			}
		})
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fd.Close() }()
	b := make([]byte, 1)
	if _, err := fd.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := fd.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

// rewriteVersion troca a versão e recalcula o crc32c dos 32 bytes do cabeçalho
// com a biblioteca padrão, fora do pacote.
func rewriteVersion(t *testing.T, path string, version uint32) {
	t.Helper()
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fd.Close() }()
	hdr := make([]byte, 36)
	if _, err := fd.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(hdr[4:8], version)
	binary.LittleEndian.PutUint32(hdr[32:36], crc32.Checksum(hdr[:32], crc32.MakeTable(crc32.Castagnoli)))
	if _, err := fd.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
}

func truncate(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
}

func appendGarbage(t *testing.T, path string, n int) {
	t.Helper()
	fd, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fd.Close() }()
	if _, err := fd.Write(bytes.Repeat([]byte{0xab}, n)); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureCapacityPreservesIdentityAndEveryBucketSlotWhenGrowing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.mwin")
	f, _ := openFile(t, path, baseHeader, 1, 1<<20)
	const oldCapacity = 1024
	for id := uint32(1); id < oldCapacity; id++ {
		for slot := range baseHeader.BucketCount {
			writeOwned(f, slot, id)
		}
	}
	if err := f.EnsureCapacity(oldCapacity - 1); err != nil {
		t.Fatal(err)
	}
	if f.Capacity() != oldCapacity {
		t.Fatalf("id coberto não pode crescer: capacidade %d", f.Capacity())
	}

	if err := f.EnsureCapacity(oldCapacity); err != nil {
		t.Fatal(err)
	}
	if f.Capacity() != 2*oldCapacity {
		t.Fatalf("capacidade depois de cruzar = %d, quer %d", f.Capacity(), 2*oldCapacity)
	}
	// 4096 + 2048*16 + 4*2048*32 = 4096 + 32768 + 262144.
	if f.SizeBytes() != 299_008 {
		t.Fatalf("SizeBytes = %d, quer 299008", f.SizeBytes())
	}
	assertNoTmp(t, dir)

	checkOld(t, f, oldCapacity)
	checkZero(t, f, oldCapacity, 2*oldCapacity)
	writeOwned(f, 3, 2*oldCapacity-1)
	closeFile(t, f)

	// O crescimento é durável: a reabertura adota a capacidade nova, sem reset.
	g, reset := openFile(t, path, baseHeader, 1, 1<<20)
	if reset {
		t.Fatal("reabrir depois do crescimento não pode resetar")
	}
	if g.Capacity() != 2*oldCapacity {
		t.Fatalf("capacidade reaberta = %d, quer %d", g.Capacity(), 2*oldCapacity)
	}
	checkOld(t, g, oldCapacity)
	if got := g.Bucket(3, 2*oldCapacity-1); got != bucketFor(3, 2*oldCapacity-1) {
		t.Errorf("último id da capacidade nova = %+v", got)
	}

	// Um id muito além dobra quantas vezes for preciso, numa reescrita só.
	if err := g.EnsureCapacity(5_000); err != nil {
		t.Fatal(err)
	}
	if g.Capacity() != 8192 {
		t.Fatalf("capacidade para o id 5000 = %d, quer 8192", g.Capacity())
	}
	checkOld(t, g, oldCapacity)
	assertNoTmp(t, dir)
}

// checkOld confere identidade e todos os slots dos ids [1, limit).
func checkOld(t *testing.T, g *metricwindow.File, limit uint32) {
	t.Helper()
	bad := 0
	for id := uint32(1); id < limit; id++ {
		if got := g.Identity(id); got != identityFor(id) {
			bad++
			if bad <= 5 {
				t.Errorf("identidade do id %d = %+v, quer %+v", id, got, identityFor(id))
			}
		}
		for slot := range baseHeader.BucketCount {
			if got := g.Bucket(slot, id); got != bucketFor(slot, id) {
				bad++
				if bad <= 5 {
					t.Errorf("balde (%d, %d) = %+v, quer %+v", slot, id, got, bucketFor(slot, id))
				}
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d entradas antigas perdidas no crescimento", bad)
	}
}

// checkZero confere que identidade e todos os slots dos ids [from, to) são
// zero.
func checkZero(t *testing.T, g *metricwindow.File, from, to uint32) {
	t.Helper()
	bad := 0
	for id := from; id < to; id++ {
		if got := g.Identity(id); got != (metricwindow.Identity{}) {
			bad++
		}
		for slot := range baseHeader.BucketCount {
			if got := g.Bucket(slot, id); got != (metricwindow.Bucket{}) {
				bad++
				if bad <= 5 {
					t.Errorf("id novo (%d, %d) nasceu sujo: %+v", slot, id, got)
				}
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d entradas de ids novos nasceram sujas", bad)
	}
}

func TestEnsureCapacityRefusesAnIDAtOrAboveTheMaximum(t *testing.T) {
	dir := t.TempDir()
	f, _ := openFile(t, filepath.Join(dir, "w.mwin"), baseHeader, 1, 1500)
	if err := f.EnsureCapacity(1499); err != nil {
		t.Fatalf("id abaixo do teto: %v", err)
	}
	if f.Capacity() != 2048 {
		t.Fatalf("capacidade = %d, quer 2048", f.Capacity())
	}
	for _, id := range []uint32{1500, 2000, 5000, math.MaxUint32} {
		if err := f.EnsureCapacity(id); !errors.Is(err, metricwindow.ErrCapacityExceeded) {
			t.Errorf("EnsureCapacity(%d) = %v, quer ErrCapacityExceeded", id, err)
		}
	}
	if f.Capacity() != 2048 {
		t.Errorf("a recusa mudou a capacidade para %d", f.Capacity())
	}
	assertNoTmp(t, dir)
}

func TestOpenGrowsAnExistingFileToTheRequestedMinimumAndNeverShrinks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.mwin")
	f, _ := openFile(t, path, baseHeader, 1, 1<<20)
	writeOwned(f, 2, 7)
	closeFile(t, f)

	g, reset := openFile(t, path, baseHeader, 3_000, 1<<20)
	if reset {
		t.Fatal("crescer na abertura não é reset")
	}
	if g.Capacity() != 4096 {
		t.Fatalf("capacidade = %d, quer 4096", g.Capacity())
	}
	if g.Identity(7) != identityFor(7) || g.Bucket(2, 7) != bucketFor(2, 7) {
		t.Errorf("o crescimento na abertura perdeu o id 7: %+v %+v", g.Identity(7), g.Bucket(2, 7))
	}
	closeFile(t, g)
	assertNoTmp(t, dir)

	h, _ := openFile(t, path, baseHeader, 1, 1<<20)
	if h.Capacity() != 4096 {
		t.Errorf("abrir pedindo menos encolheu o arquivo para %d", h.Capacity())
	}
}

// writeDirtyTmp deixa em path+".tmp" a sobra de uma reescrita interrompida:
// 8 MiB de lixo, maior que qualquer arquivo destes testes, para que um tmp
// reaproveitado suje os ids novos e o tamanho.
func writeDirtyTmp(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", bytes.Repeat([]byte{0xab}, 8<<20), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestALeftoverDirtyTemporaryFileNeverLeaksIntoTheRewrittenFile(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "w.mwin")
		writeDirtyTmp(t, path)
		f, _ := openFile(t, path, baseHeader, 1, 1<<20)
		if f.SizeBytes() != 151_552 {
			t.Errorf("tamanho = %d, quer exatamente 151552", f.SizeBytes())
		}
		checkZero(t, f, 0, f.Capacity())
		assertNoTmp(t, dir)
	})

	t.Run("growth", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "w.mwin")
		f, _ := openFile(t, path, baseHeader, 1, 1<<20)
		for id := uint32(1); id < 1024; id++ {
			writeOwned(f, id%baseHeader.BucketCount, id)
		}
		// A sobra aparece DEPOIS da abertura, como se um crescimento anterior
		// tivesse caído no meio sem que ninguém reabrisse.
		writeDirtyTmp(t, path)
		if err := f.EnsureCapacity(1024); err != nil {
			t.Fatal(err)
		}
		if f.SizeBytes() != 299_008 {
			t.Errorf("tamanho depois do crescimento = %d, quer exatamente 299008", f.SizeBytes())
		}
		checkZero(t, f, 1024, 2048)
		for id := uint32(1); id < 1024; id++ {
			if got := f.Bucket(id%baseHeader.BucketCount, id); got != bucketFor(id%baseHeader.BucketCount, id) {
				t.Fatalf("id antigo %d perdido: %+v", id, got)
			}
		}
		assertNoTmp(t, dir)
	})
}

// TestGrowthWhoseDirectorySyncFailsKeepsTheFileOnTheNewInode é o terceiro
// desfecho de EnsureCapacity, com a mesma sonda do revisor: diretório sem
// permissão de leitura (0300) deixa criar e renomear, mas o fsync do diretório
// falha ao abri-lo. O File tem de estar no arquivo novo — a escrita feita
// depois do erro sobrevive à reabertura.
func TestGrowthWhoseDirectorySyncFailsKeepsTheFileOnTheNewInode(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignora a permissão do diretório: a sonda de fsync do diretório não falha")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "w.mwin")
	f, _ := openFile(t, path, baseHeader, 1, 1<<20)
	writeOwned(f, 1, 9)

	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if err := f.EnsureCapacity(1024); err == nil {
		t.Fatal("EnsureCapacity com o fsync do diretório falhando tem de devolver erro")
	}
	if f.Capacity() != 2048 {
		t.Fatalf("capacidade depois do erro no fsync = %d, quer 2048 (o File tem de estar no arquivo novo)", f.Capacity())
	}
	writeOwned(f, 2, 10)
	writeOwned(f, 3, 2047)
	closeFile(t, f)

	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	g, reset := openFile(t, path, baseHeader, 1, 1<<20)
	if reset || g.Capacity() != 2048 {
		t.Fatalf("reabertura: reset=%v, capacidade %d; quer o arquivo novo", reset, g.Capacity())
	}
	for _, e := range [][2]uint32{{1, 9}, {2, 10}, {3, 2047}} {
		if got := g.Bucket(e[0], e[1]); got != bucketFor(e[0], e[1]) {
			t.Errorf("balde (%d, %d) = %+v, quer %+v: escrita feita no inode antigo se perdeu", e[0], e[1], got, bucketFor(e[0], e[1]))
		}
	}
}

func TestCloseIsIdempotentAndAClosedFileRefusesUse(t *testing.T) {
	f, _, err := metricwindow.Open(filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("segundo Close = %v, quer nil", err)
	}
	if err := f.EnsureCapacity(2000); !errors.Is(err, metricwindow.ErrClosed) {
		t.Errorf("EnsureCapacity depois do Close = %v, quer ErrClosed", err)
	}
	if err := f.Msync(); !errors.Is(err, metricwindow.ErrClosed) {
		t.Errorf("Msync depois do Close = %v, quer ErrClosed", err)
	}
	ops := map[string]func(){
		"Identity":    func() { f.Identity(1) },
		"SetIdentity": func() { f.SetIdentity(1, metricwindow.Identity{}) },
		"Bucket":      func() { f.Bucket(0, 1) },
		"WriteBucket": func() { f.WriteBucket(0, 1, metricwindow.Bucket{}) },
	}
	for name, op := range ops {
		func() {
			defer func() {
				if msg, _ := recover().(string); !strings.Contains(msg, "use of closed file") {
					t.Errorf("%s depois do Close: pânico %q, quer mensagem de File fechado", name, msg)
				}
			}()
			op()
		}()
	}
}

func TestOpenRefusesAnInvalidShapeOrCapacityRange(t *testing.T) {
	cases := []struct {
		name     string
		h        metricwindow.Header
		min, max uint32
	}{
		{"zero bucket count", metricwindow.Header{BucketCount: 0, BucketWidthMs: 1000}, 1, 4096},
		{"zero bucket width", metricwindow.Header{BucketCount: 60, BucketWidthMs: 0}, 1, 4096},
		{"negative bucket width", metricwindow.Header{BucketCount: 60, BucketWidthMs: -1}, 1, 4096},
		{"zero maximum", baseHeader, 0, 0},
		{"minimum above maximum", baseHeader, 5000, 4096},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "w.mwin")
			f, _, err := metricwindow.Open(path, tc.h, tc.min, tc.max)
			if err == nil {
				_ = f.Close()
				t.Fatal("Open aceitou uma forma sem leitura possível")
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Errorf("a recusa deixou o arquivo criado (stat: %v)", statErr)
			}
		})
	}
}

func TestAccessOutsideTheShapePanicsNamingTheArgument(t *testing.T) {
	f, _ := openFile(t, filepath.Join(t.TempDir(), "w.mwin"), baseHeader, 1, 4096)
	cases := []struct {
		name string
		want string
		op   func()
	}{
		{"identity id", "id 1024 outside capacity 1024", func() { f.Identity(1024) }},
		{"set identity id", "id 1024 outside capacity 1024", func() { f.SetIdentity(1024, metricwindow.Identity{}) }},
		{"bucket id", "id 1024 outside capacity 1024", func() { f.Bucket(0, 1024) }},
		{"bucket slot", "slot 4 outside bucket count 4", func() { f.Bucket(4, 1) }},
		{"write bucket slot", "slot 4 outside bucket count 4", func() { f.WriteBucket(4, 1, metricwindow.Bucket{}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				msg, _ := r.(string)
				if !strings.Contains(msg, tc.want) {
					t.Errorf("pânico = %v, quer mensagem com %q", r, tc.want)
				}
			}()
			tc.op()
		})
	}
}

// TestGoldenBytesOfHeaderAndEntry trava o formato em disco byte a byte.
//
// Os offsets são contados à mão, não pela função do pacote:
//
//	identidade do id 5:      4096 + 5*16                        = 4176
//	coluna do slot 3:        4096 + 1024*16 + 3*1024*32          = 118784
//	balde (slot 3, id 5):    118784 + 5*32                       = 118944
//	tamanho do arquivo:      4096 + 1024*16 + 60*1024*32         = 1986560
//
// O crc do cabeçalho (0x6d52a01c) é o crc32c dos 32 bytes literais abaixo, e a
// marca de dono 0x5ab1, passada pelo chamador como literal, é a dobra de
// 0xdeadbeefcafef00d (0xf00d ^ 0xcafe ^ 0xbeef ^ 0xdead); os dois foram
// calculados fora do pacote e estão escritos como literal para que uma mudança
// de campo, de ordem ou de tabela de crc quebre aqui, e não passe calada.
// Qualquer byte fora das três regiões tem de ser zero.
func TestGoldenBytesOfHeaderAndEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.mwin")
	h := metricwindow.Header{BucketCount: 60, BucketWidthMs: 5_000, Source: 7}
	f, _ := openFile(t, path, h, 1024, 1024)
	f.SetIdentity(5, metricwindow.Identity{KeyHash: 0xdeadbeefcafef00d, ValidFromEpoch: 0x4142434445464748})
	f.WriteBucket(3, 5, metricwindow.Bucket{Epoch: 0x01020304, Owner: 0x5ab1, Count: 0x0708, Sum: 0x1112131415161718, Min: -2, Max: 0x2122232425262728})
	closeFile(t, f)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	headerBytes := []byte{
		'N', 'D', 'B', 'T', // magic
		0x01, 0x00, 0x00, 0x00, // version 1
		0x3c, 0x00, 0x00, 0x00, // bucketCount 60
		0x00, 0x00, 0x00, 0x00, // reserved
		0x88, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // bucketWidthMs 5000
		0x07, 0x00, 0x00, 0x00, // source 7
		0x00, 0x04, 0x00, 0x00, // capacity 1024
		0x1c, 0xa0, 0x52, 0x6d, // crc32c 0x6d52a01c
	}
	identityBytes := []byte{
		0x0d, 0xf0, 0xfe, 0xca, 0xef, 0xbe, 0xad, 0xde, // keyHash
		0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, // validFromEpoch
	}
	bucketBytes := []byte{
		0x08, 0x07, // count 0x0708 (bits 15..0)
		0xb1, 0x5a, // owner 0x5ab1 (bits 31..16)
		0x04, 0x03, 0x02, 0x01, // epoch 0x01020304 (bits 63..32)
		0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // sum
		0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // min -2
		0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, // max
	}
	want := make([]byte, 1_986_560)
	copy(want[0:], headerBytes)
	copy(want[4176:], identityBytes)
	copy(want[118_944:], bucketBytes)

	if len(raw) != len(want) {
		t.Fatalf("arquivo tem %d bytes, golden %d", len(raw), len(want))
	}
	if !bytes.Equal(raw, want) {
		for i := range raw {
			if raw[i] != want[i] {
				t.Fatalf("primeiro byte divergente no offset %d: % x, golden % x",
					i, raw[i:min(i+16, len(raw))], want[i:min(i+16, len(want))])
			}
		}
	}
}
