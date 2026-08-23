package api

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"socmem/internal/config"
)

func TestAgentLimiterBurst(t *testing.T) {
	l := newAgentLimiter(2)
	now := time.Now()
	for i := 0; i < 2; i++ {
		if ok, _ := l.allow("bot", now); !ok {
			t.Fatalf("burst request %d should pass", i+1)
		}
	}
	ok, retryAfter := l.allow("bot", now)
	if ok {
		t.Fatal("third immediate request should be limited")
	}
	if retryAfter <= 0 {
		t.Fatalf("retry-after should be positive, got %v", retryAfter)
	}

	// refill: half a second at 2 rps = 1 token
	ok, _ = l.allow("bot", now.Add(500*time.Millisecond))
	if !ok {
		t.Fatal("refilled request should pass")
	}

	// humans exempt is enforced by middleware, but buckets are per-id:
	if ok, _ := l.allow("other-bot", now); !ok {
		t.Fatal("separate id gets its own bucket")
	}
}

func TestAgentLimiterZeroValueGuard(t *testing.T) {
	l := newAgentLimiter(0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		if ok, _ := l.allow("bot", now); !ok {
			t.Fatalf("zero-value config must not block agents (request %d)", i+1)
		}
	}
	if l.rps != 5 {
		t.Fatalf("zero rps should default to 5, got %v", l.rps)
	}
	neg := newAgentLimiter(-1)
	if neg.rps != 5 {
		t.Fatalf("negative rps should default to 5, got %v", neg.rps)
	}
}

// TestAgentLimiterCap pins the map-growth bound: flooding distinct actor ids
// must not grow the bucket map beyond maxLimiterEntries, eviction must not
// disturb live traffic, and a brand-new id arriving at capacity is allowed
// (its own fresh budget), never rejected because the map is full.
func TestAgentLimiterCap(t *testing.T) {
	l := newAgentLimiter(1000) // high rate: every request passes
	now := time.Now()

	for i := 0; i < maxLimiterEntries+100; i++ {
		id := fmt.Sprintf("flood-%d", i)
		if ok, _ := l.allow(id, now.Add(time.Duration(i)*time.Millisecond)); !ok {
			t.Fatalf("fresh id %q must be allowed at/after capacity", id)
		}
		if len(l.buckets) > maxLimiterEntries {
			t.Fatalf("map grew to %d entries, cap is %d", len(l.buckets), maxLimiterEntries)
		}
	}
	if got := len(l.buckets); got != maxLimiterEntries {
		t.Fatalf("map size = %d, want exactly %d after overflow", got, maxLimiterEntries)
	}

	// Eviction targeted the least-recently-seen end: the earliest flood
	// ids are gone, the latest survive.
	if _, ok := l.buckets["flood-0"]; ok {
		t.Fatal("flood-0 still tracked; oldest entry was not evicted")
	}
	if _, ok := l.buckets["flood-"+strconv.Itoa(maxLimiterEntries+99)]; !ok {
		t.Fatal("newest flood id evicted; eviction did not target the oldest")
	}

	// A re-arriving evicted id gets a fresh full bucket (allowed), not a
	// rejection.
	if ok, _ := l.allow("flood-0", now.Add(time.Hour)); !ok {
		t.Fatal("re-arriving evicted id must be allowed with a fresh bucket")
	}
}

func TestWriteLimitOverHTTP(t *testing.T) {
	addr := os.Getenv("MEM_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set MEM_TEST_CH_ADDR to run")
	}
	svc, res, conn := buildService(t)
	cfg := config.Load()
	cfg.AgentRateRPS = 2 // tiny budget for the test
	srv := New(svc, conn, cfg)
	h := srv.Routes()

	agent := newIdentity(t, "agent")
	for i := 0; i < 2; i++ {
		rec := do(t, h, "POST", "/v1/observations", map[string]any{
			"kind": "alert", "content": "rate limit probe",
		}, agent, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("burst write %d got %d want 200", i+1, rec.Code)
		}
	}
	rec := do(t, h, "POST", "/v1/observations", map[string]any{
		"kind": "alert", "content": "rate limit probe over budget",
	}, agent, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-budget write got %d want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}

	// humans are exempt
	human := newIdentity(t, "human")
	for i := 0; i < 5; i++ {
		rec = do(t, h, "POST", "/v1/observations", map[string]any{
			"kind": "alert", "content": "human probe",
		}, human, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("human write %d got %d want 200 (humans exempt)", i+1, rec.Code)
		}
	}
	_ = res
}
