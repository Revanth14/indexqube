package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/receipt"
)

func main() {
	var (
		fixtureDir       = flag.String("fixtures", "internal/proxy/testdata/browser_prompts", "directory of browser prompt .txt fixtures")
		format           = flag.String("format", "text", "output format: text or json")
		includeSynthetic = flag.Bool("synthetic", true, "include a generated 2,000-line one-edit sample")
		repeatFixtures   = flag.Bool("repeat-fixtures", true, "send single-turn fixtures twice to measure repeated-context savings")
		maxLines         = flag.Int("max-lines", 8000, "maximum lines per code block for exact optimizer work")
	)
	flag.Parse()

	samples, err := loadFixtureSamples(*fixtureDir)
	if err != nil {
		exitErr(err)
	}
	if *includeSynthetic {
		samples = append(samples, syntheticLargeEditSample(2000))
	}
	if len(samples) == 0 {
		exitErr(fmt.Errorf("no samples found"))
	}

	report, err := receipt.Run(context.Background(), samples, receipt.Options{
		MaxLines:          *maxLines,
		RepeatSingleTurns: *repeatFixtures,
	})
	if err != nil {
		exitErr(err)
	}

	switch strings.ToLower(strings.TrimSpace(*format)) {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			exitErr(err)
		}
	case "text", "":
		printTextReport(report)
	default:
		exitErr(fmt.Errorf("unknown -format %q; use text or json", *format))
	}
}

func loadFixtureSamples(dir string) ([]receipt.Sample, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.txt"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	samples := make([]receipt.Sample, 0, len(paths))
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read fixture %s: %w", path, err)
		}
		name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		samples = append(samples, receipt.Sample{
			Name:  name,
			Turns: []string{strings.TrimSpace(string(b))},
		})
	}
	return samples, nil
}

func syntheticLargeEditSample(lines int) receipt.Sample {
	if lines < 20 {
		lines = 20
	}
	return receipt.Sample{
		Name: "synthetic_large_one_line_edit",
		Turns: []string{
			largeGoPrompt("Can you review this large file?", lines, "return input + 1"),
			largeGoPrompt("I changed one line. What is different now?", lines, "return input + 2"),
		},
	}
}

func largeGoPrompt(question string, lines int, changedLine string) string {
	var b strings.Builder
	b.WriteString(question)
	b.WriteString("\n\n")
	b.WriteString("src/large.go\n")
	b.WriteString("package main\n\n")
	b.WriteString("func calculate(input int) int {\n")
	for i := 0; i < lines; i++ {
		if i == lines/2 {
			b.WriteByte('\t')
			b.WriteString(changedLine)
			b.WriteByte('\n')
			continue
		}
		fmt.Fprintf(&b, "\t_ = input + %d\n", i)
	}
	b.WriteString("\treturn input\n")
	b.WriteString("}\n")
	return b.String()
}

func printTextReport(report receipt.Report) {
	t := report.Totals
	fmt.Println("IndexQube Optimizer Receipt")
	fmt.Println()
	fmt.Printf("Requests: %d total, %d optimized\n", t.Requests, t.OptimizedRequests)
	fmt.Printf("Blocks:   %d seen, %d pruned, %d skipped\n", t.BlocksSeen, t.BlocksPruned, t.BlocksSkipped)
	fmt.Printf("Bytes:    %s -> %s, saved %s (%s)\n", commas(t.BytesBefore), commas(t.BytesAfter), commas(t.BytesSaved), percent(t.ReductionRatio))
	fmt.Printf("Tokens:   %s -> %s, saved %s estimated input tokens\n", commas(t.TokensBefore), commas(t.TokensAfter), commas(t.TokensSaved))
	fmt.Printf("Diffs:    %d exact, %d fallback\n", t.DiffExact, t.DiffFallback)
	if reasons := formatReasons(t.SkipReasons); reasons != "" {
		fmt.Printf("Skips:    %s\n", reasons)
	}
	fmt.Println()
	fmt.Println("Samples")
	for _, sample := range report.Samples {
		st := sample.Totals
		fmt.Printf("- %s: %s saved, %s token estimate saved, %s reduction\n",
			sample.Name,
			commas(st.BytesSaved),
			commas(st.TokensSaved),
			percent(st.ReductionRatio),
		)
		for _, turn := range sample.Turns {
			status := "warmup"
			if turn.Optimized {
				status = "optimized"
			} else if turn.BlocksSkipped > 0 {
				status = "skipped"
			} else if turn.BlocksSeen == 0 {
				status = "no_code"
			}
			details := ""
			if reasons := formatReasons(turn.SkipReasons); reasons != "" {
				details = " | skips: " + reasons
			}
			fmt.Printf("  turn %d: %-9s %s -> %s bytes, saved %s tokens, blocks %d/%d/%d%s\n",
				turn.Turn,
				status,
				commas(turn.BytesBefore),
				commas(turn.BytesAfter),
				commas(turn.TokensSaved),
				turn.BlocksSeen,
				turn.BlocksPruned,
				turn.BlocksSkipped,
				details,
			)
		}
	}
}

func formatReasons(reasons map[string]int) string {
	if len(reasons) == 0 {
		return ""
	}
	keys := make([]string, 0, len(reasons))
	for reason, n := range reasons {
		if reason != "" && n > 0 {
			keys = append(keys, reason)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, reason := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", reason, reasons[reason]))
	}
	return strings.Join(parts, ", ")
}

func percent(ratio float64) string {
	return fmt.Sprintf("%.1f%%", ratio*100)
}

func commas(n int) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return sign + s
	}
	var out []byte
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	out = append(out, s[:rem]...)
	for i := rem; i < len(s); i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}
	return sign + string(out)
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, "iqbench:", err)
	os.Exit(1)
}
