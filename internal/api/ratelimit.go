package api

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// tokenBucket is one agent's allowance. Capacity equals burst (= rps), so an
// idle agent may fire a short burst before throttling begins.
type tokenBucket struct {
	tokens float64
	last   time.Time
}

// agentLimiter rate-limits WRITE operations per agent identity. Humans are
// exempt (they gate themselves); agents are programmatic and can flood.
type agentLimiter struct {
	mu      sync.Mutex
	rps     float64
	buckets map[string]*tokenBucket
}

// newAgentLimiter guards against non-positive rates: a zero-value Config
// would otherwise silently block every agent write forever.
func newAgentLimiter(rps float64) *agentLimiter {
	if math.IsNaN(rps) || rps <= 0 {
		rps = 5
	}
	return &agentLimiter{rps: rps, buckets: make(map[string]*tokenBucket)}
}

// allow reports whether actorID may write now, and if not, how long to wait.
func (l *agentLimiter) allow(actorID string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[actorID]
	if !ok {
		b = &tokenBucket{tokens: l.rps, last: now}
		l.buckets[actorID] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rps
	if b.tokens > l.rps {
		b.tokens = l.rps
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	deficit := (1 - b.tokens) / l.rps
	return false, time.Duration(deficit * float64(time.Second))
}

// writeLimit applies the per-agent budget to write endpoints. Identity
// middleware must run first (it populates the context).
func (s *Server) writeLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ctxString(r, ctxActorType) == "agent" {
			ok, retryAfter := s.limiter.allow(ctxString(r, ctxActorID), time.Now())
			if !ok {
				secs := int(math.Ceil(retryAfter.Seconds()))
				if secs < 1 {
					secs = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(secs))
				writeErr(w, http.StatusTooManyRequests, "rate_limited",
					"agent write budget exhausted; retry later")
				return
			}
		}
		next(w, r)
	}
}
