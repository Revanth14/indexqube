package guard

import "testing"

func TestBuildFingerprintStableForEquivalentShape(t *testing.T) {
	t.Parallel()
	a := BuildFingerprint(FingerprintInput{
		Route:             "/v1/messages",
		Model:             "claude-sonnet-4-6",
		MessageCount:      17,
		ContentBlockCount: 31,
		ToolResultCount:   5,
		AttemptedTokens:   75_000,
		BlocksAnalyzed:    120,
		BlocksPruned:      102,
		LatestUserText:    "Summarize this file",
		SystemText:        "You are an assistant",
	})
	b := BuildFingerprint(FingerprintInput{
		Route:             "/v1/messages",
		Model:             "claude-sonnet-4-6",
		MessageCount:      18, // same bucket
		ContentBlockCount: 33, // same bucket
		ToolResultCount:   6,  // same bucket
		AttemptedTokens:   80_000,
		BlocksAnalyzed:    131,
		BlocksPruned:      111,
		LatestUserText:    "Summarize this file",
		SystemText:        "You are an assistant",
	})
	if a != b {
		t.Fatalf("fingerprints differ for equivalent bucketed shape: %q != %q", a, b)
	}
}

func TestBuildFingerprintDiffersForDifferentShape(t *testing.T) {
	t.Parallel()
	a := BuildFingerprint(FingerprintInput{
		Route:             "/v1/messages",
		Model:             "claude-sonnet-4-6",
		MessageCount:      10,
		ContentBlockCount: 10,
		ToolResultCount:   0,
		AttemptedTokens:   30_000,
		BlocksAnalyzed:    10,
		BlocksPruned:      2,
		LatestUserText:    "A",
		SystemText:        "S",
	})
	b := BuildFingerprint(FingerprintInput{
		Route:             "/v1/messages",
		Model:             "claude-sonnet-4-6",
		MessageCount:      70,
		ContentBlockCount: 70,
		ToolResultCount:   20,
		AttemptedTokens:   210_000,
		BlocksAnalyzed:    120,
		BlocksPruned:      90,
		LatestUserText:    "B",
		SystemText:        "T",
	})
	if a == b {
		t.Fatalf("fingerprints unexpectedly match: %q", a)
	}
}
