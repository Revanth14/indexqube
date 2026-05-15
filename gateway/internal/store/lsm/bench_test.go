package lsm_test

// Benchmark suite: LSM-Tree vs SQLite B-tree
//
// Workload: 70% writes / 30% reads - mirrors IndexQube's actual token-cache ratio.
// Scales:   1K, 10K, 100K entries (1M omitted from CI; run manually with -benchtime=1x).
//
// How to read the output:
//
//	ns/op        - latency per operation
//	MB/s         - throughput (key+value payload, not wire bytes)
//	write_amp    - disk bytes written / logical bytes written (lower = better for LSM)
//	disk_bytes   - total on-disk footprint after the run
//
// Key insight this demonstrates:
//
//	LSM batches random writes into sequential I/O (MemTable -> SSTable flush).
//	SQLite's B-tree does in-place updates: every write is a random page seek.
//	At the 70/30 write-heavy workload IndexQube sees, LSM wins on throughput.
//	At read-heavy workloads the Bloom filters keep LSM competitive, but the
//	B-tree's O(log N) indexed lookup begins to close the gap.

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/store/lsm"
	_ "modernc.org/sqlite" // pure-Go SQLite, no CGO required
)

// Workload shape.
const (
	writeFraction = 0.70 // 70% Put, 30% Get
	keyBytes      = 32   // SHA-256 hash size - typical IndexQube chunk key
	valBytes      = 512  // representative chunk content excerpt
)

// benchScales lists the entry counts to benchmark at.
// 1M is excluded from the default run to keep CI under 60 s; add it locally.
var benchScales = []int{1_000, 10_000, 100_000}

// --- key / value generation ---------------------------------------------------

// makeKey produces a deterministic fixed-width key for entry i.
// Using a Fibonacci-scrambled index spreads keys across the key space
// (avoids pathological B-tree sequential-insert behaviour that would
//
//	flatter the B-tree unfairly).
func makeKey(i int) []byte {
	scrambled := uint64(i) * 0x9e3779b97f4a7c15 // Knuth multiplicative hash
	return []byte(fmt.Sprintf("%016x%016x", scrambled, uint64(i)))
}

// makeVal produces a deterministic value of exactly valBytes.
func makeVal(i int) []byte {
	v := make([]byte, valBytes)
	for j := range v {
		v[j] = byte((i*31 + j) & 0xff)
	}
	return v
}

// --- helpers ------------------------------------------------------------------

// dirSize returns the total byte count of all regular files under dir.
// Used as a write-amplification proxy: disk_bytes / logical_bytes_written.
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, fi os.FileInfo, _ error) error {
		if fi != nil && !fi.IsDir() {
			total += fi.Size()
		}
		return nil
	})
	return total
}

// reportWriteAmp computes and reports write amplification and disk footprint.
func reportWriteAmp(b *testing.B, dir string) {
	b.Helper()
	diskBytes := dirSize(dir)
	logicalWrites := int64(float64(b.N) * writeFraction * float64(keyBytes+valBytes))
	if logicalWrites > 0 {
		b.ReportMetric(float64(diskBytes)/float64(logicalWrites), "write_amp")
	}
	b.ReportMetric(float64(diskBytes)/1024/1024, "disk_MiB")
}

// --- LSM benchmarks -----------------------------------------------------------

// BenchmarkLSM_70w30r benchmarks the custom LSM-Tree engine at the 70/30 ratio.
func BenchmarkLSM_70w30r(b *testing.B) {
	for _, n := range benchScales {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			eng, err := lsm.Open(dir, lsm.Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()

			// Pre-populate n/2 keys so reads have a realistic hit rate.
			for i := range n / 2 {
				if err := eng.Put(makeKey(i), makeVal(i)); err != nil {
					b.Fatal(err)
				}
			}

			rng := rand.New(rand.NewPCG(42, 0))
			b.ResetTimer()
			b.SetBytes(int64(keyBytes + valBytes))

			for range b.N {
				idx := rng.IntN(n)
				if rng.Float64() < writeFraction {
					if err := eng.Put(makeKey(idx), makeVal(idx)); err != nil {
						b.Fatal(err)
					}
				} else {
					if _, _, err := eng.Get(makeKey(idx)); err != nil {
						b.Fatal(err)
					}
				}
			}

			b.StopTimer()
			reportWriteAmp(b, dir)
		})
	}
}

// BenchmarkLSM_ReadHeavy benchmarks a read-dominant workload (10w/90r) to show
// how Bloom filters keep LSM competitive even when reads dominate.
func BenchmarkLSM_ReadHeavy(b *testing.B) {
	for _, n := range benchScales {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			eng, err := lsm.Open(dir, lsm.Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()

			for i := range n {
				if err := eng.Put(makeKey(i), makeVal(i)); err != nil {
					b.Fatal(err)
				}
			}

			rng := rand.New(rand.NewPCG(99, 0))
			b.ResetTimer()
			b.SetBytes(int64(keyBytes + valBytes))

			for range b.N {
				idx := rng.IntN(n)
				if rng.Float64() < 0.10 {
					eng.Put(makeKey(idx), makeVal(idx)) //nolint:errcheck
				} else {
					eng.Get(makeKey(idx)) //nolint:errcheck
				}
			}

			b.StopTimer()
			reportWriteAmp(b, dir)
		})
	}
}

// --- SQLite B-tree benchmarks -------------------------------------------------

// openSQLiteKV opens a SQLite database with a simple KV table tuned to be a
// fair B-tree comparison: WAL mode (same durability model as LSM flush),
// WITHOUT ROWID (clustered primary-key index, minimises SQLite overhead),
// NORMAL synchronous (matching our LSM's lack of fsync per write).
func openSQLiteKV(b *testing.B, dir string) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1) // WAL mode still only allows one writer
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous  = NORMAL;
		PRAGMA cache_size   = -8192;
		CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID;
	`)
	if err != nil {
		b.Fatal(err)
	}
	return db
}

// BenchmarkSQLite_70w30r benchmarks SQLite's B-tree at the same 70/30 workload.
// This is the apples-to-apples comparison: same key/value sizes, same hit rate.
func BenchmarkSQLite_70w30r(b *testing.B) {
	for _, n := range benchScales {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			db := openSQLiteKV(b, dir)
			defer db.Close()

			// Pre-populate n/2 keys in a single transaction (same as LSM pre-load).
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			ins, err := tx.Prepare(`INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)`)
			if err != nil {
				b.Fatal(err)
			}
			for i := range n / 2 {
				if _, err := ins.Exec(makeKey(i), makeVal(i)); err != nil {
					b.Fatal(err)
				}
			}
			ins.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}

			putStmt, err := db.Prepare(`INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)`)
			if err != nil {
				b.Fatal(err)
			}
			getStmt, err := db.Prepare(`SELECT v FROM kv WHERE k = ?`)
			if err != nil {
				b.Fatal(err)
			}
			defer putStmt.Close()
			defer getStmt.Close()

			rng := rand.New(rand.NewPCG(42, 0))
			b.ResetTimer()
			b.SetBytes(int64(keyBytes + valBytes))

			for range b.N {
				idx := rng.IntN(n)
				if rng.Float64() < writeFraction {
					if _, err := putStmt.Exec(makeKey(idx), makeVal(idx)); err != nil {
						b.Fatal(err)
					}
				} else {
					row := getStmt.QueryRow(makeKey(idx))
					var val []byte
					_ = row.Scan(&val)
				}
			}

			b.StopTimer()
			reportWriteAmp(b, dir)
		})
	}
}

// BenchmarkSQLite_ReadHeavy is the SQLite counterpart to BenchmarkLSM_ReadHeavy.
func BenchmarkSQLite_ReadHeavy(b *testing.B) {
	for _, n := range benchScales {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			db := openSQLiteKV(b, dir)
			defer db.Close()

			tx, _ := db.Begin()
			ins, _ := tx.Prepare(`INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)`)
			for i := range n {
				ins.Exec(makeKey(i), makeVal(i)) //nolint:errcheck
			}
			ins.Close()
			tx.Commit() //nolint:errcheck

			putStmt, _ := db.Prepare(`INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)`)
			getStmt, _ := db.Prepare(`SELECT v FROM kv WHERE k = ?`)
			defer putStmt.Close()
			defer getStmt.Close()

			rng := rand.New(rand.NewPCG(99, 0))
			b.ResetTimer()
			b.SetBytes(int64(keyBytes + valBytes))

			for range b.N {
				idx := rng.IntN(n)
				if rng.Float64() < 0.10 {
					putStmt.Exec(makeKey(idx), makeVal(idx)) //nolint:errcheck
				} else {
					row := getStmt.QueryRow(makeKey(idx))
					var val []byte
					row.Scan(&val) //nolint:errcheck
				}
			}

			b.StopTimer()
			reportWriteAmp(b, dir)
		})
	}
}
