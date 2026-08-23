package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDecodeJSONTrailingData pins strict decoding: exactly one JSON value,
// no trailing garbage.
func TestDecodeJSONTrailingData(t *testing.T) {
	var dst struct {
		A string `json:"a"`
	}

	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"a":"b"}`))
	rec := httptest.NewRecorder()
	if !decodeJSON(rec, req, &dst) || dst.A != "b" {
		t.Fatalf("clean payload rejected: %d %s", rec.Code, rec.Body.String())
	}

	for _, body := range []string{
		`{"a":"b"} {"a":"c"}`,
		`{"a":"b"}garbage`,
		`{"a":"b"}
		{"a":"c"}`,
	} {
		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		rec := httptest.NewRecorder()
		if decodeJSON(rec, req, &dst) {
			t.Errorf("trailing data accepted: %q", body)
			continue
		}
		if rec.Code != 400 {
			t.Errorf("trailing data %q: got %d want 400", body, rec.Code)
		}
	}
}
