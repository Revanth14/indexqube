package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync/atomic"
)

// Bloom is a probabilistic set membership filter sized for a target
// false-positive rate. It is concurrency-safe via atomic word operations.
//
// CURRENT STATUS: built but unwired. The filter exists for the upcoming
// Supabase L2 tier where it serves as a cheap negative-lookup guard
// (avoid a network round trip when we know the entry is definitely
// absent). Without an L2, the in-memory MemoryCache already returns
// hash-map lookups in nanoseconds; a Bloom filter in front of it would
// pure cost. Do not call it from the governor read path until L2 lands.
//
// Sizing: NewBloom(n, p) picks bit count m and hash count k by the
// canonical optimal formulas
//
//	m = -n * ln(p) / (ln 2)^2
//	k = (m / n) * ln 2
//
// Hashing strategy: Kirsch-Mitzenmacher double-hashing, with two
// independent FNV variants as the base hashes. The trick is
//
//	h_i(x) = (h1(x) + i*h2(x)) mod m
//
// which gives effectively-independent k hashes from only two primary
// hash computations.
type Bloom struct {
	bits []atomic.Uint64
	m    uint64 // bit count (always a multiple of 64)
	k    uint64 // hash count
}

// NewBloom returns a Bloom filter sized for expectedItems with the
// given target false-positive rate (e.g. 0.01 for 1%).
//
// Both arguments must be > 0; falsePositiveRate must be in (0, 1).
func NewBloom(expectedItems uint64, falsePositiveRate float64) *Bloom {
	if expectedItems == 0 {
		expectedItems = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}
	m := optimalM(expectedItems, falsePositiveRate)
	k := optimalK(m, expectedItems)
	// Round m up to a multiple of 64 so each word holds 64 bits exactly.
	if m%64 != 0 {
		m += 64 - (m % 64)
	}
	return &Bloom{
		bits: make([]atomic.Uint64, m/64),
		m:    m,
		k:    k,
	}
}

// Add inserts key into the filter. Subsequent Contains(key) is guaranteed
// to return true.
func (b *Bloom) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := uint64(0); i < b.k; i++ {
		bit := (h1 + i*h2) % b.m
		b.bits[bit/64].Or(uint64(1) << (bit % 64))
	}
}

// Contains reports whether key may be in the set. False is definitive
// (the key was never Added). True is probabilistic -- it may be a false
// positive at the configured rate.
func (b *Bloom) Contains(key []byte) bool {
	h1, h2 := hashes(key)
	for i := uint64(0); i < b.k; i++ {
		bit := (h1 + i*h2) % b.m
		if b.bits[bit/64].Load()&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// hashes returns two independent 64-bit hashes of key by splitting one
// SHA-256 digest in half. SHA-256 is overkill for raw speed but gives
// strong uniformity and bit-level independence -- the previous attempt
// (FNV-1 + FNV-1a) had structural correlation that pushed the empirical
// false-positive rate ~5x above the design target. The Bloom filter is
// on the slow path (network short-circuit before L2 lookups); SHA-256
// at ~1 GB/s is irrelevant here.
func hashes(key []byte) (uint64, uint64) {
	sum := sha256.Sum256(key)
	h1 := binary.BigEndian.Uint64(sum[0:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])
	return h1, h2
}

func optimalM(n uint64, p float64) uint64 {
	v := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	return uint64(math.Ceil(v))
}

func optimalK(m, n uint64) uint64 {
	v := math.Round(float64(m) / float64(n) * math.Ln2)
	if v < 1 {
		return 1
	}
	return uint64(v)
}
