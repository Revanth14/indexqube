package contextopt

import "testing"

func TestIsProtectedContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sourcePath string
		text       string
		want       bool
	}{
		{
			name:       "cursor rules path",
			sourcePath: `/repo/.cursor/rules/backend.mdc`,
			want:       true,
		},
		{
			name: "instruction path in text",
			text: `Read /repo/CONTEXT.md before editing.`,
			want: true,
		},
		{
			name: "credential marker",
			text: `Authorization: Bearer my-api-key`,
			want: true,
		},
		{
			name:       "ordinary source",
			sourcePath: `/repo/src/main.go`,
			text:       `package main`,
			want:       false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsProtectedContent(tt.sourcePath, tt.text); got != tt.want {
				t.Fatalf("IsProtectedContent()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsProtectedInstructionPathNormalizesSeparators(t *testing.T) {
	t.Parallel()

	if !ContainsProtectedInstructionPath(`C:\repo\.cursor\rules\backend.mdc`) {
		t.Fatal("expected Windows-style cursor rules path to be protected")
	}
}
