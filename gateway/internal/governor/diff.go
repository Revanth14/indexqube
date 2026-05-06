package governor

// UnifiedLineDiff renders a compact unified diff for callers that use the
// original governor diff helper.
func UnifiedLineDiff(path string, oldLines, newLines []string, maxLines int) string {
	result := OptimizeLineSlices(path, oldLines, newLines, LineOptimizerConfig{
		MaxLines:     maxLines,
		ContextLines: defaultDiffContextLines,
	})
	return result.Diff
}

// UnifiedLineDiffWithContext renders a compact unified diff and returns a
// skip reason when the caller's explicit limits prevent diffing.
func UnifiedLineDiffWithContext(path string, oldLines, newLines []string, maxLines, contextLines int) (string, string) {
	result := OptimizeLineSlices(path, oldLines, newLines, LineOptimizerConfig{
		MaxLines:     maxLines,
		ContextLines: contextLines,
	})
	return result.Diff, result.SkipReason
}
