package api

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"socmem/internal/memory"
)

// TestMapServiceErrorStorageFault pins the 502 mapping: a wrapped ClickHouse
// failure must surface as storage_error with the generic message — never a
// 400, and never driver-level detail in the body.
func TestMapServiceErrorStorageFault(t *testing.T) {
	chErr := fmt.Errorf("memory: commit observation %s: %w",
		"obs-1", &clickhouse.Exception{Code: 1000, Message: "Connection reset by peer"})

	rec := httptest.NewRecorder()
	mapServiceError(rec, chErr)
	if rec.Code != 502 {
		t.Fatalf("storage fault mapped to %d, want 502", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "storage_error") || !strings.Contains(body, "storage temporarily unavailable") {
		t.Fatalf("body = %s, want generic storage_error envelope", body)
	}
	for _, leaked := range []string{"Connection reset", "clickhouse", "code: 1000"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("body leaks driver detail %q: %s", leaked, body)
		}
	}

	// Service sentinels keep their existing mappings.
	rec = httptest.NewRecorder()
	mapServiceError(rec, memory.ErrHumanGated)
	if rec.Code != 403 {
		t.Errorf("ErrHumanGated mapped to %d, want 403", rec.Code)
	}

	// Non-storage unknown errors stay 400.
	rec = httptest.NewRecorder()
	mapServiceError(rec, errors.New("some bad request"))
	if rec.Code != 400 {
		t.Errorf("plain error mapped to %d, want 400", rec.Code)
	}
}
