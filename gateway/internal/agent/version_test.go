package agent

import "testing"

func TestCLIVersionPolicyFailsClosedOutsideSupportedRange(t *testing.T) {
	policy := CLIVersionPolicy{
		Product: "fixture", MinInclusive: SemanticVersion{Major: 2, Minor: 1},
		MaxExclusive: SemanticVersion{Major: 2, Minor: 2},
	}
	for _, test := range []struct {
		output string
		ok     bool
	}{
		{output: "2.1.0", ok: true},
		{output: "tool 2.1.252 (release)", ok: true},
		{output: "2.0.999"},
		{output: "2.2.0"},
		{output: "development build"},
	} {
		_, err := policy.Check(test.output)
		if (err == nil) != test.ok {
			t.Fatalf("output=%q ok=%v err=%v", test.output, test.ok, err)
		}
	}
}
