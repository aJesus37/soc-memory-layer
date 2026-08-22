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
// whitelisted AND confidence >= tc.floor — then it activates automatically.
// Unknown actor types fail CLOSED (proposed), never open; a nil whitelist
// fails closed too, since nothing can be whitelisted in it.
//
// Pure and log-free: env-derived configuration lives in loadTrustConfig;
// callers thread the whole trustConfig through so the effective floor
// cannot drift from what the operator configured.
func ApplyTrust(actorType, predicate string, confidence float32, tc trustConfig) Status {
	switch actorType {
	case actorHuman:
		return Active
	case actorAgent:
		if tc.whitelist[predicate] && confidence >= tc.floor {
			return Active
		}
	}
	return Proposed
}

// loadTrustConfig reads MEM_TRUST_FLOOR (float) and MEM_TRUST_WHITELIST
// (comma-separated predicates). It is pure and log-free: an unset
// MEM_TRUST_FLOOR, empty string, unparseable value, NaN, or value outside
// [0,1] keeps the default floor (0.8) with no error; MEM_TRUST_WHITELIST
// defaults to {resolved_to} and any non-empty value replaces it wholesale
// (empty entries after splitting are dropped, so an explicit list can
// disable auto-activation).
func loadTrustConfig() trustConfig {
	cfg := trustConfig{
		floor:     defaultTrustFloor,
		whitelist: map[string]bool{defaultTrustWhitelist: true},
	}
	if v := strings.TrimSpace(os.Getenv("MEM_TRUST_FLOOR")); v != "" {
		// NaN fails both range comparisons, so it falls through to default.
		if f, err := strconv.ParseFloat(v, 32); err == nil && f >= 0 && f <= 1 {
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
