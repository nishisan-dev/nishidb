// Package primary implementa o índice primário do NishiDB (spec §35-36):
// hash table open-addressing disk-backed via mmap mapeando serviceKey→rowId.
package primary

// HashKey calcula o hash de 64 bits de uma serviceKey: FNV-1a 64 com
// finalizer fmix64 (murmur3) para dispersão dos bits baixos sob máscara de
// potência de 2. Determinístico entre restarts e plataformas (o arquivo do
// índice é persistente). Nunca devolve 0 ou 1 — reservados para os estados
// de slot EMPTY e DELETED.
func HashKey(key []byte) uint64 {
	return remapReserved(fmix64(fnv1a64(key)))
}

const (
	fnvOffset64 = 0xcbf29ce484222325
	fnvPrime64  = 0x100000001b3
)

func fnv1a64(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// fmix64 é o finalizer do murmur3: avalanche forte em todos os bits.
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// remapReserved afasta os valores reservados de slot (0=EMPTY, 1=DELETED).
// O eventual falso positivo de hash é resolvido pela validação da key real.
func remapReserved(h uint64) uint64 {
	if h < 2 {
		return h + 2
	}
	return h
}
