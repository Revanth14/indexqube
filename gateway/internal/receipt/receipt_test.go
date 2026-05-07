package receipt

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestRunLargeOneLineEditReceipt(t *testing.T) {
	t.Parallel()
	report, err := Run(context.Background(), []Sample{largeEditSampleForTest(500)}, Options{MaxLines: 8000})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if report.Totals.Requests != 2 {
		t.Fatalf("requests=%d want 2", report.Totals.Requests)
	}
	if report.Totals.OptimizedRequests != 1 {
		t.Fatalf("optimized requests=%d want 1; report=%+v", report.Totals.OptimizedRequests, report.Totals)
	}
	if report.Totals.TokensSaved <= 0 {
		t.Fatalf("tokens saved=%d want >0", report.Totals.TokensSaved)
	}
	if report.Totals.ReductionRatio < 0.40 {
		t.Fatalf("reduction ratio=%f want >=0.40", report.Totals.ReductionRatio)
	}
	if got := report.Samples[0].Turns[1]; got.BlocksPruned != 1 || got.DiffExact != 1 {
		t.Fatalf("second turn blocks_pruned=%d diff_exact=%d want 1/1", got.BlocksPruned, got.DiffExact)
	}
}

func TestRunRepeatsSingleTurnFixtures(t *testing.T) {
	t.Parallel()
	report, err := Run(context.Background(), []Sample{{
		Name: "repeat_fixture",
		Turns: []string{`src/demo.go
package main

func demo() {
	println("hello")
}`},
	}}, Options{RepeatSingleTurns: true})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := report.Totals.Requests; got != 2 {
		t.Fatalf("requests=%d want 2", got)
	}
	if got := report.Totals.BlocksSeen; got != 2 {
		t.Fatalf("blocks seen=%d want 2", got)
	}
}

func largeEditSampleForTest(lines int) Sample {
	return Sample{
		Name: "large_edit",
		Turns: []string{
			testLargePrompt(lines, "return input + 1"),
			testLargePrompt(lines, "return input + 2"),
		},
	}
}

func testLargePrompt(lines int, changedLine string) string {
	var b strings.Builder
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
