package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// headers may already be written; nothing sane left to do
		return
	}
}

// identity extracts caller identity from X-Actor-* / X-Scope headers,
// rejecting requests that would otherwise be unauditable. The values are
// injected into the request context; handlers merge them into service
// inputs. Body-borne actor/scope fields are ignored by design — identity
// never travels in payloads.
//
// Echoes X-Mem-Actor on every response for easy correlation in logs.
func (s *Server) identity(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actorType := strings.TrimSpace(r.Header.Get("X-Actor-Type"))
		actorID := strings.TrimSpace(r.Header.Get("X-Actor-ID"))
		scope := strings.TrimSpace(r.Header.Get("X-Scope"))
		onBehalfOf := strings.TrimSpace(r.Header.Get("X-On-Behalf-Of"))

		if actorType != "human" && actorType != "agent" {
			writeErr(w, http.StatusBadRequest, "invalid_identity",
				"X-Actor-Type must be human or agent")
			return
		}
		if actorID == "" {
			writeErr(w, http.StatusBadRequest, "invalid_identity",
				"X-Actor-ID required")
			return
		}
		if scope == "" {
			writeErr(w, http.StatusBadRequest, "invalid_identity",
				"X-Scope required")
			return
		}
		w.Header().Set("X-Mem-Actor", actorType+":"+actorID)

		ctx := r.Context()
		ctx = contextWith(ctx, ctxActorType, actorType)
		ctx = contextWith(ctx, ctxActorID, actorID)
		ctx = contextWith(ctx, ctxOnBehalfOf, onBehalfOf)
		ctx = contextWith(ctx, ctxScope, scope)
		next(w, r.WithContext(ctx))
	}
}

func contextWith(ctx context.Context, key ctxKey, val string) context.Context {
	return context.WithValue(ctx, key, val)
}

func ctxString(r *http.Request, key ctxKey) string {
	v, _ := r.Context().Value(key).(string)
	return v
}

// parseIntParam parses an integer query parameter, writing a 400 on failure.
// Returns (value, ok).
func parseIntParam(w http.ResponseWriter, raw, name string) (int, bool) {
	var n int
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || raw != strconv.Itoa(n) {
		writeErr(w, http.StatusBadRequest, "invalid_param",
			name+" must be an integer")
		return 0, false
	}
	return n, true
}
