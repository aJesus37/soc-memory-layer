// Package entity normalizes raw text values into canonical entity keys
// suitable for deduplicated storage in mem.entities.
//
// Normalize attempts detection in a fixed order — trim, technique, hash,
// IP, domain — which disambiguates overlapping shapes (e.g. an IPv4 address
// is also syntactically a domain name; the IP check runs first).
package entity

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

// Type is the entity discriminator. String values MUST match the
// ClickHouse Enum8 values of the mem.entities type column exactly.
type Type string

const (
	IocDomain Type = "ioc_domain"
	IocIP     Type = "ioc_ip"
	IocHash   Type = "ioc_hash"
	Technique Type = "technique"
)

// Normalized is a canonicalized entity: its Type and the canonical Key
// used for lookup-or-create.
type Normalized struct {
	Type Type
	Key  string
}

var (
	techniqueRe = regexp.MustCompile(`(?i)^T\d{4}(\.\d{3})?$`)
	hashLens    = map[int]struct{}{32: {}, 40: {}, 64: {}, 128: {}}
	labelRe     = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

// Normalize turns a raw string into a Normalized entity or returns a
// descriptive error if the input does not match any supported shape.
func Normalize(raw string) (Normalized, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Normalized{}, fmt.Errorf("entity: empty or whitespace-only input")
	}
	if k, ok := normalizeTechnique(s); ok {
		return Normalized{Type: Technique, Key: k}, nil
	}
	if k, ok := normalizeHash(s); ok {
		return Normalized{Type: IocHash, Key: k}, nil
	}
	if k, ok := normalizeIP(s); ok {
		return Normalized{Type: IocIP, Key: k}, nil
	}
	if k, ok := normalizeDomain(s); ok {
		return Normalized{Type: IocDomain, Key: k}, nil
	}
	return Normalized{}, fmt.Errorf("entity: unrecognized %q (not a technique, hash, IP, or domain)", s)
}

func normalizeTechnique(s string) (string, bool) {
	if !techniqueRe.MatchString(s) {
		return "", false
	}
	return strings.ToUpper(s), true
}

func normalizeHash(s string) (string, bool) {
	if _, ok := hashLens[len(s)]; !ok || !isHex(s) {
		return "", false
	}
	return strings.ToLower(s), true
}

func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}

func normalizeIP(s string) (string, bool) {
	ip := net.ParseIP(s)
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

func normalizeDomain(s string) (string, bool) {
	d := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(s), "."))
	labels := strings.Split(d, ".")
	if len(labels) < 2 {
		return "", false
	}
	for _, l := range labels {
		if !labelRe.MatchString(l) {
			return "", false
		}
	}
	// TLDs are never all-numeric; this also rejects invalid dotted quads
	// such as "999.1.1.1" that slip past net.ParseIP.
	last := labels[len(labels)-1]
	for _, r := range last {
		if r >= 'a' && r <= 'z' {
			return d, true
		}
	}
	return "", false
}
