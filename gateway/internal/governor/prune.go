package governor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const defaultDiffContextLines = 3

// PruneMessages walks user/assistant messages, replaces large unchanged code
// fences with tiny markers, and replaces changed fences with unified diffs.
func PruneMessages(ctx context.Context, hist History, tenant string, msgs []domain.Message, maxLines int, logger *slog.Logger) ([]domain.Message, domain.PruneStats) {
	var st domain.PruneStats
	if hist == nil {
		return msgs, finishPruneStats(st)
	}
	out := cloneMessages(msgs)
	for i := range out {
		role := strings.ToLower(strings.TrimSpace(out[i].Role))
		if role != "user" && role != "assistant" {
			continue
		}
		before := len(out[i].Content)
		newBody, blockStats := pruneContent(ctx, hist, tenant, out[i].Content, maxLines)
		out[i].Content = newBody
		st.BytesBefore += before
		st.BytesAfter += len(newBody)
		mergePruneStats(&st, blockStats)
	}
	st = finishPruneStats(st)
	if logger != nil && st.BlocksPruned > 0 {
		logger.DebugContext(ctx, "pruned code blocks",
			slog.String("tenant", tenant),
			slog.Int("blocks_pruned", st.BlocksPruned),
			slog.Int("blocks_skipped", st.BlocksSkipped),
			slog.Int("bytes_before", st.BytesBefore),
			slog.Int("bytes_after", st.BytesAfter),
			slog.Float64("reduction_ratio", st.ReductionRatio),
		)
	}
	return out, st
}

func pruneContent(ctx context.Context, hist History, tenant, content string, maxLines int) (string, domain.PruneStats) {
	var st domain.PruneStats
	blocks := ExtractCodeBlocks(content)
	if len(blocks) == 0 {
		return content, st
	}
	st.BlocksSeen = len(blocks)

	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Start > blocks[j].Start })

	out := content
	for _, b := range blocks {
		inner := b.Inner
		currentHash := ContentHash(inner)
		prev, ok := hist.Get(ctx, tenant, b.Path)
		hist.Put(ctx, tenant, b.Path, inner)

		if !ok {
			continue
		}
		if prev.Hash == currentHash {
			short := fmt.Sprintf("[IndexQube] No changes since last turn for `%s`.\n", b.Path)
			if !replacementSavesBytes(b.RawOuter, short) {
				addSkipReason(&st, "not_smaller")
				continue
			}
			out = out[:b.Start] + short + out[b.End:]
			st.BlocksPruned++
			continue
		}

		diff := OptimizeLineDiff(b.Path, prev.Content, inner, LineOptimizerConfig{
			MaxLines:     maxLines,
			ContextLines: defaultDiffContextLines,
		})
		if diff.Diff == "" {
			addSkipReason(&st, diff.SkipReason)
			continue
		}
		if diff.Exact {
			st.DiffExact++
		} else {
			st.DiffFallback++
		}
		replacement := fmt.Sprintf(
			"[IndexQube] Incremental update for `%s` (prior version was cached in session history):\n\n```diff\n%s```\n",
			b.Path,
			diff.Diff,
		)
		if !replacementSavesBytes(b.RawOuter, replacement) {
			addSkipReason(&st, "not_smaller")
			continue
		}
		out = out[:b.Start] + replacement + out[b.End:]
		st.BlocksPruned++
	}
	return out, st
}

func replacementSavesBytes(original, replacement string) bool {
	return len(replacement) < len(original)
}

func cloneMessages(m []domain.Message) []domain.Message {
	out := make([]domain.Message, len(m))
	copy(out, m)
	return out
}

func mergePruneStats(dst *domain.PruneStats, src domain.PruneStats) {
	dst.BlocksSeen += src.BlocksSeen
	dst.BlocksPruned += src.BlocksPruned
	dst.BlocksSkipped += src.BlocksSkipped
	dst.DiffExact += src.DiffExact
	dst.DiffFallback += src.DiffFallback
	for reason, n := range src.SkipReasons {
		if dst.SkipReasons == nil {
			dst.SkipReasons = make(map[string]int)
		}
		dst.SkipReasons[reason] += n
	}
}

func addSkipReason(st *domain.PruneStats, reason string) {
	if reason == "" {
		reason = "unknown"
	}
	st.BlocksSkipped++
	if st.SkipReasons == nil {
		st.SkipReasons = make(map[string]int)
	}
	st.SkipReasons[reason]++
}

func finishPruneStats(st domain.PruneStats) domain.PruneStats {
	st.TokensBefore = estimateTokens(st.BytesBefore)
	st.TokensAfter = estimateTokens(st.BytesAfter)
	if st.BytesBefore <= 0 {
		st.ReductionRatio = 0
		return st
	}
	ratio := float64(st.BytesBefore-st.BytesAfter) / float64(st.BytesBefore)
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 {
		ratio = 0
	}
	st.ReductionRatio = ratio
	return st
}

func estimateTokens(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	return (bytes + 3) / 4
}
