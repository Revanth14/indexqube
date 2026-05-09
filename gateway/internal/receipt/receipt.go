// Package receipt runs optimizer samples and returns human/demo friendly
// savings receipts.
package receipt

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/governor"
	"github.com/Revanth14/indexqube/gateway/internal/proxy"
)

// Sample is one prompt scenario. Turns are replayed in order against the same
// pruning session so diffs and unchanged-block markers can appear naturally.
type Sample struct {
	Name  string
	Turns []string
}

// Options controls one receipt run.
type Options struct {
	ProjectMemory     string
	MaxLines          int
	RepeatSingleTurns bool
}

// Report is the full optimizer receipt for all samples.
type Report struct {
	Samples []SampleReceipt `json:"samples"`
	Totals  Totals          `json:"totals"`
}

// SampleReceipt summarizes one sample scenario.
type SampleReceipt struct {
	Name   string        `json:"name"`
	Turns  []TurnReceipt `json:"turns"`
	Totals Totals        `json:"totals"`
}

// TurnReceipt summarizes one optimizer pass.
type TurnReceipt struct {
	Name              string         `json:"name"`
	Turn              int            `json:"turn"`
	RawBytes          int            `json:"raw_bytes"`
	BytesBefore       int            `json:"bytes_before"`
	BytesAfter        int            `json:"bytes_after"`
	BytesSaved        int            `json:"bytes_saved"`
	TokensBefore      int            `json:"estimated_tokens_before"`
	TokensAfter       int            `json:"estimated_tokens_after"`
	TokensSaved       int            `json:"estimated_tokens_saved"`
	ReductionRatio    float64        `json:"reduction_ratio"`
	BlocksSeen        int            `json:"blocks_seen"`
	BlocksPruned      int            `json:"blocks_pruned"`
	BlocksSkipped     int            `json:"blocks_skipped"`
	SkipReasons       map[string]int `json:"skip_reasons,omitempty"`
	DiffExact         int            `json:"diff_exact"`
	DiffFallback      int            `json:"diff_fallback"`
	Optimized         bool           `json:"optimized"`
	OutputPreview     string         `json:"output_preview,omitempty"`
	OutputPreviewSize int            `json:"output_preview_size,omitempty"`
}

// Totals aggregates receipt counters.
type Totals struct {
	Requests          int            `json:"requests"`
	OptimizedRequests int            `json:"optimized_requests"`
	BlocksSeen        int            `json:"blocks_seen"`
	BlocksPruned      int            `json:"blocks_pruned"`
	BlocksSkipped     int            `json:"blocks_skipped"`
	SkipReasons       map[string]int `json:"skip_reasons,omitempty"`
	BytesBefore       int            `json:"bytes_before"`
	BytesAfter        int            `json:"bytes_after"`
	BytesSaved        int            `json:"bytes_saved"`
	TokensBefore      int            `json:"estimated_tokens_before"`
	TokensAfter       int            `json:"estimated_tokens_after"`
	TokensSaved       int            `json:"estimated_tokens_saved"`
	ReductionRatio    float64        `json:"reduction_ratio"`
	DiffExact         int            `json:"diff_exact"`
	DiffFallback      int            `json:"diff_fallback"`
}

// Run replays samples through the real browser text normalization and governor
// pruning stack.
func Run(ctx context.Context, samples []Sample, opts Options) (Report, error) {
	gov, err := governor.New(
		governor.WithHistory(governor.NewMemoryHistory()),
		governor.WithPruning(true, opts.MaxLines),
		governor.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		return Report{}, fmt.Errorf("receipt: governor init: %w", err)
	}

	var report Report
	for _, sample := range samples {
		turns := sample.Turns
		if opts.RepeatSingleTurns && len(turns) == 1 {
			turns = []string{turns[0], turns[0]}
		}
		sr := SampleReceipt{Name: sample.Name}
		tenant := "receipt/" + sample.Name
		for i, raw := range turns {
			normalized := proxy.NormalizeRawOptimizeText(raw, "", "")
			msgs, stats, err := gov.Optimize(ctx, tenant, []domain.Message{{Role: "user", Content: normalized}}, opts.ProjectMemory)
			if err != nil {
				return Report{}, err
			}
			output := proxy.RenderOptimizedText(msgs)
			tr := turnReceipt(sample.Name, i+1, raw, output, stats)
			sr.Turns = append(sr.Turns, tr)
			addTurn(&sr.Totals, tr)
		}
		finishTotals(&sr.Totals)
		report.Samples = append(report.Samples, sr)
		addTotals(&report.Totals, sr.Totals)
	}
	finishTotals(&report.Totals)
	sort.Slice(report.Samples, func(i, j int) bool {
		return report.Samples[i].Name < report.Samples[j].Name
	})
	return report, nil
}

func turnReceipt(name string, turn int, raw, output string, stats domain.PruneStats) TurnReceipt {
	bytesSaved := positive(stats.BytesBefore - stats.BytesAfter)
	tokensSaved := positive(stats.TokensBefore - stats.TokensAfter)
	return TurnReceipt{
		Name:              name,
		Turn:              turn,
		RawBytes:          len(raw),
		BytesBefore:       stats.BytesBefore,
		BytesAfter:        stats.BytesAfter,
		BytesSaved:        bytesSaved,
		TokensBefore:      stats.TokensBefore,
		TokensAfter:       stats.TokensAfter,
		TokensSaved:       tokensSaved,
		ReductionRatio:    stats.ReductionRatio,
		BlocksSeen:        stats.BlocksSeen,
		BlocksPruned:      stats.BlocksPruned,
		BlocksSkipped:     stats.BlocksSkipped,
		SkipReasons:       cloneReasons(stats.SkipReasons),
		DiffExact:         stats.DiffExact,
		DiffFallback:      stats.DiffFallback,
		Optimized:         stats.BlocksPruned > 0 && bytesSaved > 0,
		OutputPreview:     preview(output, 220),
		OutputPreviewSize: len(output),
	}
}

func addTurn(t *Totals, r TurnReceipt) {
	t.Requests++
	if r.Optimized {
		t.OptimizedRequests++
	}
	t.BlocksSeen += r.BlocksSeen
	t.BlocksPruned += r.BlocksPruned
	t.BlocksSkipped += r.BlocksSkipped
	t.BytesBefore += r.BytesBefore
	t.BytesAfter += r.BytesAfter
	t.BytesSaved += r.BytesSaved
	t.TokensBefore += r.TokensBefore
	t.TokensAfter += r.TokensAfter
	t.TokensSaved += r.TokensSaved
	t.DiffExact += r.DiffExact
	t.DiffFallback += r.DiffFallback
	mergeReasons(&t.SkipReasons, r.SkipReasons)
}

func addTotals(dst *Totals, src Totals) {
	dst.Requests += src.Requests
	dst.OptimizedRequests += src.OptimizedRequests
	dst.BlocksSeen += src.BlocksSeen
	dst.BlocksPruned += src.BlocksPruned
	dst.BlocksSkipped += src.BlocksSkipped
	dst.BytesBefore += src.BytesBefore
	dst.BytesAfter += src.BytesAfter
	dst.BytesSaved += src.BytesSaved
	dst.TokensBefore += src.TokensBefore
	dst.TokensAfter += src.TokensAfter
	dst.TokensSaved += src.TokensSaved
	dst.DiffExact += src.DiffExact
	dst.DiffFallback += src.DiffFallback
	mergeReasons(&dst.SkipReasons, src.SkipReasons)
}

func finishTotals(t *Totals) {
	if t.BytesBefore <= 0 {
		t.ReductionRatio = 0
		return
	}
	ratio := float64(t.BytesBefore-t.BytesAfter) / float64(t.BytesBefore)
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 {
		ratio = 0
	}
	t.ReductionRatio = ratio
	t.BytesSaved = positive(t.BytesBefore - t.BytesAfter)
	t.TokensSaved = positive(t.TokensBefore - t.TokensAfter)
}

func mergeReasons(dst *map[string]int, src map[string]int) {
	for reason, n := range src {
		if reason == "" || n <= 0 {
			continue
		}
		if *dst == nil {
			*dst = make(map[string]int)
		}
		(*dst)[reason] += n
	}
}

func cloneReasons(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func preview(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

func positive(n int) int {
	if n < 0 {
		return 0
	}
	return n
}
