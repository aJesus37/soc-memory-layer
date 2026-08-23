// Package ids centralizes identifier generation for every mem.* row so all
// minted ids share one policy.
//
// Identifiers are UUIDv7: their canonical TEXTUAL order is chronological by
// construction. That property is load-bearing because ClickHouse compares
// UUID columns in its INTERNAL byte order (halves swapped relative to the
// textual layout), while the graph projector's keyset cursors and ORDER BY
// tiebreakers deliberately sort on toString(id) (verified on ClickHouse
// 26.3). With v7, "toString(id) sorts later" means "was minted later",
// making every textual tiebreaker semantically meaningful.
package ids

import "github.com/google/uuid"

// New mints a UUIDv7 identifier. NewV7 fails only on catastrophic clock
// regression (never observed in practice); rather than threading an error
// through every write path, it falls back to a random v4.
//
// Mixed-population semantics (an ADR will formalize this): legacy v4 ids
// textually sort AFTER all v7 ids ('5...' > '01a0...'), so monotonic
// cursors remain correct across the population split — "oldest first"
// processing becomes "v7-era first, then the legacy tail". Caller-supplied
// ClientEventIDs are out of scope here: retries must reuse the caller's id
// regardless of its version.
func New() uuid.UUID {
	u, err := uuid.NewV7()
	if err != nil {
		return uuid.New() // clock failure fallback; see doc comment
	}
	return u
}
