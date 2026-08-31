package lsm_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/store/lsm"
)

// ── Bloom filter ──────────────────────────────────────────────────────────────

func TestBloom_NoFalseNegatives(t *testing.T) {
	b := lsm.NewBloom(1000, 0.01)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
		b.Add(keys[i])
	}
	for _, k := range keys {
		if !b.MayContain(k) {
			t.Errorf("false negative for key %q", k)
		}
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	const n = 10_000
	const wantFPR = 0.01
	b := lsm.NewBloom(n, wantFPR)
	for i := range n {
		b.Add([]byte(fmt.Sprintf("inserted-%d", i)))
	}
	fps := 0
	const probes = 100_000
	for i := range probes {
		// Keys that were never inserted.
		if b.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fps++
		}
	}
	gotFPR := float64(fps) / probes
	// Allow 3× the target rate as slack for the statistical nature of Bloom filters.
	if gotFPR > wantFPR*3 {
		t.Errorf("false positive rate %.4f exceeds 3× target %.4f", gotFPR, wantFPR)
	}
}

func TestBloom_EncodeDecode(t *testing.T) {
	b := lsm.NewBloom(500, 0.01)
	for i := range 500 {
		b.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	enc := b.Encode()
	b2, err := lsm.DecodeBloom(enc)
	if err != nil {
		t.Fatalf("DecodeBloom: %v", err)
	}
	for i := range 500 {
		if !b2.MayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Errorf("decoded bloom: false negative for k%d", i)
		}
	}
}

// ── MemTable ──────────────────────────────────────────────────────────────────

func TestMemTable_PutGet(t *testing.T) {
	m := lsm.NewMemTableForTest(4 * 1024 * 1024)
	m.Put([]byte("hello"), []byte("world"))
	v, found, tomb := m.Get([]byte("hello"))
	if !found || tomb || string(v) != "world" {
		t.Errorf("Get(hello) = %q, %v, %v; want world, true, false", v, found, tomb)
	}
}

func TestMemTable_UpdateInPlace(t *testing.T) {
	m := lsm.NewMemTableForTest(4 * 1024 * 1024)
	m.Put([]byte("k"), []byte("v1"))
	m.Put([]byte("k"), []byte("v2"))
	v, _, _ := m.Get([]byte("k"))
	if string(v) != "v2" {
		t.Errorf("after update: got %q; want v2", v)
	}
}

func TestMemTable_Delete(t *testing.T) {
	m := lsm.NewMemTableForTest(4 * 1024 * 1024)
	m.Put([]byte("x"), []byte("1"))
	m.Delete([]byte("x"))
	_, found, tomb := m.Get([]byte("x"))
	if !found || !tomb {
		t.Errorf("after Delete: found=%v tomb=%v; want true, true", found, tomb)
	}
}

func TestMemTable_Iterator_Order(t *testing.T) {
	m := lsm.NewMemTableForTest(4 * 1024 * 1024)
	keys := []string{"banana", "apple", "cherry", "apricot", "date"}
	for _, k := range keys {
		m.Put([]byte(k), []byte("v"))
	}
	sort.Strings(keys)
	it := m.Iterator()
	for _, want := range keys {
		if !it.Valid() {
			t.Fatalf("iterator ended early; want key %q", want)
		}
		if got := string(it.Key()); got != want {
			t.Errorf("key = %q; want %q", got, want)
		}
		it.Next()
	}
	if it.Valid() {
		t.Errorf("iterator has extra entries after all keys consumed")
	}
}

// ── SSTable builder + reader ──────────────────────────────────────────────────

func TestSSTable_BuildRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")
	opts := lsm.DefaultOptions()

	b, err := lsm.NewBuilderForTest(path, opts)
	if err != nil {
		t.Fatalf("newBuilder: %v", err)
	}
	entries := [][2]string{
		{"apple", "1"}, {"banana", "2"}, {"cherry", "3"},
		{"date", "4"}, {"elderberry", "5"},
	}
	for _, e := range entries {
		b.Add([]byte(e[0]), []byte(e[1]))
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	sst, err := lsm.OpenSSTableForTest(path)
	if err != nil {
		t.Fatalf("openSSTable: %v", err)
	}
	defer sst.Close()

	for _, e := range entries {
		v, found, err := sst.Get([]byte(e[0]))
		if err != nil {
			t.Fatalf("Get(%q): %v", e[0], err)
		}
		if !found {
			t.Errorf("Get(%q): not found", e[0])
			continue
		}
		if string(v) != e[1] {
			t.Errorf("Get(%q) = %q; want %q", e[0], v, e[1])
		}
	}

	// Absent key — Bloom should short-circuit.
	v, found, err := sst.Get([]byte("zzz"))
	if err != nil || found || v != nil {
		t.Errorf("Get(zzz) = %q, %v, %v; want nil, false, nil", v, found, err)
	}
}

func TestSSTable_Tombstone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tomb.sst")
	opts := lsm.DefaultOptions()

	b, _ := lsm.NewBuilderForTest(path, opts)
	b.Add([]byte("alive"), []byte("yes"))
	b.Add([]byte("dead"), nil) // tombstone
	b.Finish()

	sst, _ := lsm.OpenSSTableForTest(path)
	defer sst.Close()

	v, found, _ := sst.Get([]byte("alive"))
	if !found || string(v) != "yes" {
		t.Errorf("alive key: found=%v val=%q", found, v)
	}
	v, found, _ = sst.Get([]byte("dead"))
	if !found || v != nil {
		t.Errorf("dead key: found=%v val=%q; want found=true val=nil", found, v)
	}
}

func TestSSTable_Iterator(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "iter.sst")
	opts := lsm.DefaultOptions()

	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	b, _ := lsm.NewBuilderForTest(path, opts)
	for _, k := range keys {
		b.Add([]byte(k), []byte("v"))
	}
	b.Finish()

	sst, _ := lsm.OpenSSTableForTest(path)
	defer sst.Close()

	it := lsm.NewSSTIterForTest(sst)
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != len(keys) {
		t.Fatalf("iterated %d keys; want %d", len(got), len(keys))
	}
	for i, k := range keys {
		if got[i] != k {
			t.Errorf("key[%d] = %q; want %q", i, got[i], k)
		}
	}
}

// ── Engine: end-to-end ────────────────────────────────────────────────────────

func TestEngine_PutGet(t *testing.T) {
	e := openTestEngine(t)
	defer e.Close()

	if err := e.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := e.Get([]byte("foo"))
	if err != nil || !found || string(v) != "bar" {
		t.Errorf("Get(foo) = %q, %v, %v; want bar, true, nil", v, found, err)
	}
}

func TestEngine_Delete(t *testing.T) {
	e := openTestEngine(t)
	defer e.Close()

	e.Put([]byte("k"), []byte("v"))
	e.Delete([]byte("k"))
	_, found, _ := e.Get([]byte("k"))
	if found {
		t.Error("Get after Delete: found=true; want false")
	}
}

func TestEngine_Overwrite(t *testing.T) {
	e := openTestEngine(t)
	defer e.Close()

	e.Put([]byte("k"), []byte("v1"))
	e.Put([]byte("k"), []byte("v2"))
	v, _, _ := e.Get([]byte("k"))
	if string(v) != "v2" {
		t.Errorf("Get after overwrite = %q; want v2", v)
	}
}

func TestEngine_AbsentKey(t *testing.T) {
	e := openTestEngine(t)
	defer e.Close()

	_, found, err := e.Get([]byte("nosuchkey"))
	if err != nil || found {
		t.Errorf("Get(absent) = found=%v err=%v; want false, nil", found, err)
	}
}

func TestEngine_FlushAndRecover(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.DefaultOptions()
	opts.MemTableSize = 512 // tiny threshold to force flushes

	e, err := lsm.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 200
	for i := range n {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Allow background flush to complete.
	time.Sleep(50 * time.Millisecond)
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open and verify SSTable persistence.
	e2, err := lsm.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	// At least some keys should survive in L0 SSTables.
	hits := 0
	for i := range n {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := fmt.Sprintf("val-%04d", i)
		v, found, err := e2.Get(key)
		if err != nil {
			t.Fatalf("Get key-%04d: %v", i, err)
		}
		if found && string(v) == want {
			hits++
		}
	}
	if hits == 0 {
		t.Error("no keys survived after engine re-open")
	}
	t.Logf("re-open: %d/%d keys found in SSTables", hits, n)
}

// TestEngine_L0Compaction writes enough data to trigger L0→L1 compaction
// and verifies all keys remain readable.
func TestEngine_L0Compaction(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.DefaultOptions()
	opts.MemTableSize = 1024 // flush every ~1 KB
	opts.MaxL0Tables = 2     // compact L0 after 2 tables
	opts.BloomExpected = 32

	e, err := lsm.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	const n = 500
	for i := range n {
		key := []byte(fmt.Sprintf("ckey-%06d", i))
		val := []byte(fmt.Sprintf("cval-%06d", i))
		if err := e.Put(key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Give background compactor time to run.
	time.Sleep(100 * time.Millisecond)

	for i := range n {
		key := []byte(fmt.Sprintf("ckey-%06d", i))
		want := fmt.Sprintf("cval-%06d", i)
		v, found, err := e.Get(key)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if !found {
			t.Errorf("key-%06d: not found after compaction", i)
			continue
		}
		if string(v) != want {
			t.Errorf("key-%06d: got %q; want %q", i, v, want)
		}
	}
}

// TestEngine_DeleteAfterFlush verifies that a tombstone written after a key has
// been flushed to L0 correctly hides the value on Get.
func TestEngine_DeleteAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.DefaultOptions()
	opts.MemTableSize = 512

	e, _ := lsm.Open(dir, opts)
	defer e.Close()

	e.Put([]byte("tkey"), []byte("tval"))
	time.Sleep(30 * time.Millisecond) // allow flush to L0
	e.Delete([]byte("tkey"))

	_, found, _ := e.Get([]byte("tkey"))
	if found {
		t.Error("key visible after Delete; tombstone did not mask flushed value")
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkEngine_Put measures Put throughput with all I/O serialised.
func BenchmarkEngine_Put(b *testing.B) {
	e := openBenchEngine(b)
	defer e.Close()
	key := make([]byte, 16)
	val := make([]byte, 64)
	b.SetBytes(int64(len(key) + len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		writeUint64(key, uint64(i))
		e.Put(key, val) //nolint:errcheck
	}
}

// BenchmarkEngine_Get_MemTable measures Get on an in-memory-only dataset
// (no disk I/O, no Bloom filter cost).
func BenchmarkEngine_Get_MemTable(b *testing.B) {
	e := openBenchEngine(b)
	defer e.Close()

	const n = 10_000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%06d", i))
		e.Put(keys[i], []byte("bench-value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		e.Get(keys[i%n]) //nolint:errcheck
	}
}

// BenchmarkBloom_Add and BenchmarkBloom_MayContain measure the filter's hot path.
func BenchmarkBloom_Add(b *testing.B) {
	bl := lsm.NewBloom(b.N+1, 0.01)
	key := make([]byte, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		writeUint64(key, uint64(i))
		bl.Add(key)
	}
}

func BenchmarkBloom_MayContain(b *testing.B) {
	const n = 100_000
	bl := lsm.NewBloom(n, 0.01)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bkey-%d", i))
		bl.Add(keys[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		bl.MayContain(keys[i%n])
	}
}

// BenchmarkMemTable_Put measures skip-list insertion.
func BenchmarkMemTable_Put(b *testing.B) {
	m := lsm.NewMemTableForTest(1 << 30)
	key := make([]byte, 16)
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		writeUint64(key, uint64(i))
		m.Put(key, val)
	}
}

// BenchmarkMemTable_Get measures skip-list point-lookup.
func BenchmarkMemTable_Get(b *testing.B) {
	const n = 100_000
	m := lsm.NewMemTableForTest(1 << 30)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%d", rand.IntN(1<<24)))
		m.Put(keys[i], []byte("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		m.Get(keys[i%n])
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func openTestEngine(t *testing.T) *lsm.Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := lsm.Open(dir, lsm.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func openBenchEngine(b *testing.B) *lsm.Engine {
	b.Helper()
	dir, err := os.MkdirTemp("", "lsm-bench-*")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	e, err := lsm.Open(dir, lsm.DefaultOptions())
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	return e
}

func writeUint64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}
