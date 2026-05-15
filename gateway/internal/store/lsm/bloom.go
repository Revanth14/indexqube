package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// Bloom is a space-efficient probabilistic membership filter.
//
// # Optimal parameters
//
// Given n expected insertions and a target false-positive rate p ∈ (0, 1):
//
//	m = ⌈ −n · ln p / (ln 2)² ⌉   — bit-array size
//	k = ⌈ (m/n) · ln 2 ⌉           — number of hash functions
//
// # Double hashing (Kirsch-Mitzenmacher)
//
// Instead of k independent hash functions we generate:
//
//	g_i(x) = ( h1(x) + i · h2(x) ) mod m,  i = 0 … k-1
//
// using only two FNV-1a-64 evaluations per key. h2 is always forced odd so
// the probe sequence visits every bit position when m is a power of two.
type Bloom struct {
	bits []uint64 // bit array, word-aligned
	m    uint64   // total number of bits
	k    int      // number of hash probes per key
}

// NewBloom constructs a Bloom filter for n expected insertions at false-positive
// probability p. Both n and p must be positive; p must be < 1.
func NewBloom(n int, p float64) *Bloom {
	if n < 1 {
		n = 1
	}
	m := bloomM(n, p)
	k := bloomK(m, n)
	return &Bloom{
		bits: make([]uint64, (m+63)/64),
		m:    m,
		k:    k,
	}
}

// Add inserts key into the filter.
func (b *Bloom) Add(key []byte) {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(b.k); i++ {
		bit := (h1 + i*h2) % b.m
		b.bits[bit>>6] |= 1 << (bit & 63)
	}
}

// MayContain reports whether key is possibly in the set.
// A false return is a definitive miss. A true return may be a false positive.
func (b *Bloom) MayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(b.k); i++ {
		bit := (h1 + i*h2) % b.m
		if b.bits[bit>>6]&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// Encode serialises the filter for storage inside an SSTable.
//
// Wire format: k:4 | m:8 | bits…  (little-endian, 12-byte header)
func (b *Bloom) Encode() []byte {
	out := make([]byte, 12+len(b.bits)*8)
	binary.LittleEndian.PutUint32(out[0:], uint32(b.k))
	binary.LittleEndian.PutUint64(out[4:], b.m)
	for i, w := range b.bits {
		binary.LittleEndian.PutUint64(out[12+i*8:], w)
	}
	return out
}

// DecodeBloom deserialises a filter produced by Encode.
func DecodeBloom(data []byte) (*Bloom, error) {
	const hdr = 12
	if len(data) < hdr {
		return nil, fmt.Errorf("lsm/bloom: data too short (%d bytes)", len(data))
	}
	k := int(binary.LittleEndian.Uint32(data[0:]))
	m := binary.LittleEndian.Uint64(data[4:])
	words := (m + 63) / 64
	need := uint64(hdr) + words*8
	if uint64(len(data)) < need {
		return nil, fmt.Errorf("lsm/bloom: truncated bit array (have %d, need %d)", len(data), need)
	}
	bits := make([]uint64, words)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(data[hdr+i*8:])
	}
	return &Bloom{bits: bits, m: m, k: k}, nil
}

// bloomHash returns two independent 64-bit hashes of key.
// h2 is forced odd so gcd(h2, m) = 1 when m is a power of two.
func bloomHash(key []byte) (h1, h2 uint64) {
	a := fnv.New64a()
	a.Write(key)
	h1 = a.Sum64()

	// Mix in a fixed suffix to de-correlate h2 from h1.
	b := fnv.New64a()
	b.Write(key)
	b.Write([]byte{0xCA, 0xFE, 0xBA, 0xBE})
	h2 = b.Sum64() | 1
	return
}

// bloomM computes the optimal bit-array size.
func bloomM(n int, p float64) uint64 {
	return uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))
}

// bloomK computes the optimal number of hash functions.
func bloomK(m uint64, n int) int {
	k := int(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	return k
}
