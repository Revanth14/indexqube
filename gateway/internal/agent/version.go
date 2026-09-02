package agent

import (
	"fmt"
	"regexp"
	"strconv"
)

var semanticVersionPattern = regexp.MustCompile(`(?:^|[^0-9])([0-9]+)\.([0-9]+)\.([0-9]+)(?:[^0-9]|$)`)

type SemanticVersion struct {
	Major int
	Minor int
	Patch int
}

func (v SemanticVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func (v SemanticVersion) compare(other SemanticVersion) int {
	if v.Major != other.Major {
		return v.Major - other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor - other.Minor
	}
	return v.Patch - other.Patch
}

type CLIVersionPolicy struct {
	Product      string
	MinInclusive SemanticVersion
	MaxExclusive SemanticVersion
}

func (p CLIVersionPolicy) Check(output string) (SemanticVersion, error) {
	version, err := ParseSemanticVersion(output)
	if err != nil {
		return SemanticVersion{}, fmt.Errorf("%s unsupported CLI version: %w; supported range is >=%s and <%s",
			p.Product, err, p.MinInclusive, p.MaxExclusive)
	}
	if version.compare(p.MinInclusive) < 0 || version.compare(p.MaxExclusive) >= 0 {
		return version, fmt.Errorf("%s unsupported CLI version %s; supported range is >=%s and <%s",
			p.Product, version, p.MinInclusive, p.MaxExclusive)
	}
	return version, nil
}

func ParseSemanticVersion(output string) (SemanticVersion, error) {
	match := semanticVersionPattern.FindStringSubmatch(output)
	if len(match) != 4 {
		return SemanticVersion{}, fmt.Errorf("no semantic version found")
	}
	parts := [3]int{}
	for index := range parts {
		value, err := strconv.Atoi(match[index+1])
		if err != nil {
			return SemanticVersion{}, fmt.Errorf("invalid semantic version: %w", err)
		}
		parts[index] = value
	}
	return SemanticVersion{Major: parts[0], Minor: parts[1], Patch: parts[2]}, nil
}
