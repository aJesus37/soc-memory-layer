package memory

import (
	"os"
	"strconv"
	"strings"
)

// Status mirrors mem.facts.status Enum8('proposed','active','retracted')
// exactly; these string values are written straight into ClickHouse.
type Status string

const (
	Proposed  Status = "proposed"
	Active    Status = "active"
	Retracted Status = "retracted"
)

const (
	// defaultTrustFloor is the confidence an agent fact needs before a
	// whitelisted predicate auto-activates (design doc §5).
	defaultTrustFloor float32 = 0.8
	// defaultTrustWhitelist is the predicate set that may auto-activate
	// agent facts at or above the floor: deterministic derivations and
	// machine-extracted IOCs, where LLM judgment adds nothing.
	defaultTrustWhitelist = "resolved_to"
	// actorHuman is the only actor type that asserts facts outright.
	actorHuman = "human"
	// actorAgent proposes facts subject to the trust rules.
	actorAgent = "agent"
)

// trustConfig holds the auto-activation policy for agent-written facts.
type trustConfig struct {
	floor     float32
	whitelist map[string]bool
}

// ApplyTrust decides the initial status of a fact.
//
// Humans assert (always active). Agents propose unless the predicate is
// whitelisted AND confidence >= floor — then it activates automatically.
// Unknown actor types fail CLOSED (proposed), never open.
//
// Pure and log-free: env-derived configuration lives in loadTrustConfig,
// so callers pass the whitelist explicitly.
func ApplyTrust(actorType, predicate string, confidence float32, whitelist map[string]bool) Status {
	switch actorType {
	case actorHuman:
		return Active
	case actorAgent:
		if whitelist[predicate] && confidence >= defaultTrustFloor {
			return Active
		}
	}
	return Proposed
}

// loadTrustConfig reads MEM_TRUST_FLOOR (float) and MEM_TRUST_WHITELIST
// (comma-separated predicates). It is pure and log-free: an unset
// MEM_TRUST_FLOOR, empty string, or unparseable value keeps the default
// floor (0.8) with no error; MEM_TRUST_WHITELIST defaults to {resolved_to}
// and any non-empty value replaces it wholesale (empty entries after
// splitting are dropped, so an explicit list can disable auto-activation).
func loadTrustConfig() trustConfig {
	cfg := trustConfig{
		floor:     defaultTrustFloor,
		whitelist: map[string]bool{defaultTrustWhitelist: true},
	}
	if v := strings.TrimSpace(os.Getenv("MEM_TRUST_FLOOR")); v != "" {
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.floor = float32(f)
		}
	}
	if v := os.Getenv("MEM_TRUST_WHITELIST"); v != "" {
		wl := make(map[string]bool)
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				wl[p] = true
			}
		}
		cfg.whitelist = wl
	}
	return cfg
}
