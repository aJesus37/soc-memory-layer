package mcpserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
)

// # Remote-MCP authentication (Phase-4)
//
// HTTP mode authenticates each request with a static bearer token drawn
// from a user-managed tokens file (MEM_MCP_TOKENS_FILE). Each record maps
// one token to the Identity every tool handler expects in the request
// context: whoever holds a token IS that identity for every read (scoped)
// and write (attributed, with the usual trust-policy consequences).
//
// # Trust boundary — read before deploying
//
// TLS IS NOT TERMINATED HERE, by design. Bearer tokens and session data
// cross this listener in PLAINTEXT; a reverse proxy must terminate TLS in
// front of it. Exposing memmcp's HTTP port directly beyond loopback or an
// already-trusted network segment is a credential-disclosure bug waiting to
// happen — tokens replayed off the wire are valid until the file is edited.
//
// Tokens are long-lived static credentials: no expiry, no revocation list,
// no rotation machinery. Rotation means editing tokens.json and restarting;
// treat these credentials like API keys, not like sessions. The token file
// must be readable only by the service account (it is a credential store).
//
// Fail-closed posture: cmd/memmcp refuses to start HTTP mode without a
// loadable, non-empty, strictly-valid tokens file, and unauthenticated
// requests are answered 401 + WWW-Authenticate: Bearer by TokenAuth's
// middleware before any MCP handling sees them.

// TokenRecord maps one bearer token to the actor it stands for, as stored
// in the tokens file:
//
//	[
//	  {"token": "smem_<64hex>", "actor_type": "human", "actor_id": "analyst-j", "scope": "team-a"},
//	  {"token": "...", "actor_type": "agent", "actor_id": "triage-bot-v3", "scope": "team-b"}
//	]
//
// ActorType is human|agent (same vocabulary as MEM_MCP_ACTOR_TYPE); Scope
// confines every read; writes are attributed to ActorType/ActorID.
type TokenRecord struct {
	Token     string `json:"token"`
	ActorType string `json:"actor_type"`
	ActorID   string `json:"actor_id"`
	Scope     string `json:"scope"`
}

// tokenPattern pins the wire format of tokens so a truncated paste, a hex
// typo, or a weaker entropy source cannot silently become a credential.
var tokenPattern = regexp.MustCompile(`^smem_[0-9a-f]{64}$`)

// GenerateToken mints one tokens-file entry value: "smem_" plus 64 hex
// chars from 32 cryptographically random bytes. crypto/rand.Read cannot
// fail on Go ≥1.24 (it panics irrecoverably instead), so there is no error.
func GenerateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return "smem_" + hex.EncodeToString(b)
}

// LoadTokenFile parses and validates a tokens file. Strictness is the point
// — a mis-keyed record would otherwise mint an identity nobody can audit:
//
//   - unknown JSON fields are rejected (catches renamed/typo'd keys)
//   - empty token/actor_type/actor_id/scope rejected (whitespace-only too)
//   - actor_type must be human|agent
//   - token must match ^smem_[0-9a-f]{64}$
//   - duplicate tokens rejected
//   - the file must contain at least one record
func LoadTokenFile(path string) ([]TokenRecord, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("mcpserver: tokens file path is empty")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("mcpserver: read tokens file: %w", err)
	}
	var records []TokenRecord
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&records); err != nil {
		return nil, fmt.Errorf("mcpserver: parse tokens file %s: %w", path, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("mcpserver: tokens file %s defines no identities", path)
	}
	seen := make(map[string]bool, len(records))
	for i, r := range records {
		rec := r // avoid mutating the slice we range over
		rec.Token = strings.TrimSpace(rec.Token)
		rec.ActorType = strings.TrimSpace(rec.ActorType)
		rec.ActorID = strings.TrimSpace(rec.ActorID)
		rec.Scope = strings.TrimSpace(rec.Scope)
		switch {
		case !tokenPattern.MatchString(rec.Token):
			return nil, fmt.Errorf("mcpserver: tokens file %s record %d: token must match smem_+64 lowercase hex", path, i+1)
		case rec.ActorType != "human" && rec.ActorType != "agent":
			return nil, fmt.Errorf("mcpserver: tokens file %s record %d: actor_type %q invalid (want human|agent)", path, i+1, rec.ActorType)
		case rec.ActorID == "":
			return nil, fmt.Errorf("mcpserver: tokens file %s record %d: actor_id must not be empty", path, i+1)
		case rec.Scope == "":
			return nil, fmt.Errorf("mcpserver: tokens file %s record %d: scope must not be empty", path, i+1)
		case seen[rec.Token]:
			return nil, fmt.Errorf("mcpserver: tokens file %s record %d: duplicate token", path, i+1)
		}
		seen[rec.Token] = true
		records[i] = rec
	}
	return records, nil
}

// TokenAuth holds the loaded token→identity mapping for one process. The
// mapping is immutable after NewTokenAuth in today's deployments (rotation
// = edit file + restart) but access is RWMutex-guarded so a future hot
// reload needs no call-site changes.
type TokenAuth struct {
	mu      sync.RWMutex
	records []TokenRecord
}

// NewTokenAuth builds the authenticator from validated records. Records
// should come from LoadTokenFile (which enforces uniqueness and shape);
// given duplicates here, the first occurrence wins.
func NewTokenAuth(records []TokenRecord) *TokenAuth {
	cp := make([]TokenRecord, len(records))
	copy(cp, records)
	return &TokenAuth{records: cp}
}

// Authenticate resolves the identity behind an Authorization header value.
// It expects "Bearer <token>" (scheme case-insensitive per RFC 7235) and
// compares the presented token against EVERY record in constant time via
// crypto/subtle, never short-circuiting on an early match, so request cost
// does not reveal which (or whether) a prefix matched. Missing header,
// wrong scheme, malformed token, and unknown token all return false
// indistinguishably.
func (a *TokenAuth) Authenticate(headerValue string) (Identity, bool) {
	token, ok := bearerToken(headerValue)
	if !ok {
		return Identity{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	matchIdx := -1
	for i, r := range a.records {
		if subtle.ConstantTimeCompare([]byte(token), []byte(r.Token)) == 1 {
			matchIdx = i // remember, but keep scanning all records
		}
	}
	if matchIdx < 0 {
		return Identity{}, false
	}
	r := a.records[matchIdx]
	return Identity{ActorType: r.ActorType, ActorID: r.ActorID, Scope: r.Scope}, true
}

// bearerToken extracts the credential from a "Bearer <token>" header value.
func bearerToken(headerValue string) (string, bool) {
	scheme, rest, found := strings.Cut(headerValue, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(rest)
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", false
	}
	return token, true
}

// Middleware wraps next with the remote-MCP auth gate: requests presenting
// a valid bearer token proceed WITH their mapped identity already injected
// into the request context (WithIdentity), which mcp-go propagates into
// every tool-handler context. Everything else gets 401 +
// WWW-Authenticate: Bearer and never reaches MCP handling.
//
// cmd/memmcp additionally re-injects identity inside its HTTPContextFunc as
// defense in depth; both injections derive from the same immutable mapping,
// so they cannot disagree.
func (a *TokenAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := a.Authenticate(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="soc-memory-mcp"`)
			http.Error(w, "unauthorized: missing or invalid bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
	})
}

// HTTPContextInjector returns an mcp-go HTTPContextFunc that re-resolves
// the caller's identity straight from the Authorization header and injects
// it into the MCP handler context. Used as the second injection layer for
// Streamable HTTP (see WithHTTPContextFunc); unreachable-failure paths fall
// through unchanged because the middleware has already 401'd them.
func (a *TokenAuth) HTTPContextInjector() func(ctx context.Context, r *http.Request) context.Context {
	return func(ctx context.Context, r *http.Request) context.Context {
		if id, ok := a.Authenticate(r.Header.Get("Authorization")); ok {
			return WithIdentity(ctx, id)
		}
		return ctx
	}
}
