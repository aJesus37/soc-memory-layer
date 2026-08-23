package api

import (
	"net/http"
	"os"
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
