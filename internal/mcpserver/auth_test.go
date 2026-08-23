package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// validToken is a syntactically valid token for tests; never a real secret.
const validToken = "smem_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func writeTokens(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTokenFileValid(t *testing.T) {
	path := writeTokens(t, fmt.Sprintf(`[
		{"token": %q, "actor_type": "human", "actor_id": "analyst-j", "scope": "team-a"},
		{"token": %q, "actor_type": "agent", "actor_id": "triage-bot-v3", "scope": "team-b"}
	]`, validToken, GenerateToken()))

	recs, err := LoadTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].ActorType != "human" || recs[0].ActorID != "analyst-j" || recs[0].Scope != "team-a" {
		t.Fatalf("record 0 = %+v", recs[0])
	}
}

func TestLoadTokenFileRejects(t *testing.T) {
	badToken := "tok_" + validToken[5:] // right shape, wrong prefix
	cases := []struct {
		name      string
		content   string
		skipWrite bool
		wantErr   string
	}{
		{
			name:      "missing file",
			content:   "",
			skipWrite: true,
			wantErr:   "read tokens file",
		},
		{
			name:    "unparseable json",
			content: `[{"token": `,
			wantErr: "parse tokens file",
		},
		{
			name:    "empty array",
			content: `[]`,
			wantErr: "defines no identities",
		},
		{
			name:    "unknown field rejected",
			content: `[{"token": "` + validToken + `", "actor_type": "human", "actor_id": "a", "scope": "s", "admin": true}]`,
			wantErr: "unknown field",
		},
		{
			name:    "bad token prefix",
			content: `[{"token": "` + badToken + `", "actor_type": "human", "actor_id": "a", "scope": "s"}]`,
			wantErr: "smem_+64 lowercase hex",
		},
		{
			name:    "uppercase hex token",
			content: `[{"token": "` + "SMEM_" + validToken[5:] + `", "actor_type": "human", "actor_id": "a", "scope": "s"}]`,
			wantErr: "smem_+64 lowercase hex",
		},
		{
			name:    "empty actor_type",
			content: `[{"token": "` + validToken + `", "actor_type": "", "actor_id": "a", "scope": "s"}]`,
			wantErr: "actor_type",
		},
		{
			name:    "invalid actor_type",
			content: `[{"token": "` + validToken + `", "actor_type": "superuser", "actor_id": "a", "scope": "s"}]`,
			wantErr: `actor_type "superuser" invalid`,
		},
		{
			name:    "empty actor_id",
			content: `[{"token": "` + validToken + `", "actor_type": "human", "actor_id": "  ", "scope": "s"}]`,
			wantErr: "actor_id",
		},
		{
			name:    "empty scope",
			content: `[{"token": "` + validToken + `", "actor_type": "agent", "actor_id": "a", "scope": ""}]`,
			wantErr: "scope must not be empty",
		},
		{
			name: "duplicate tokens",
			content: `[{"token": "` + validToken + `", "actor_type": "human", "actor_id": "a", "scope": "s1"},` +
				`{"token": "` + validToken + `", "actor_type": "agent", "actor_id": "b", "scope": "s2"}]`,
			wantErr: "duplicate token",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var path string
			if !tc.skipWrite {
				path = writeTokens(t, tc.content)
			} else {
				path = filepath.Join(t.TempDir(), "does-not-exist.json")
			}
			if _, err := LoadTokenFile(path); err == nil {
				t.Fatalf("expected rejection, got nil error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestGenerateTokenShape(t *testing.T) {
	pat := regexp.MustCompile(`^smem_[0-9a-f]{64}$`)
	seen := map[string]bool{}
	for i := 0; i < 16; i++ {
		tok := GenerateToken()
		if !pat.MatchString(tok) {
			t.Fatalf("token %q does not match the documented shape", tok)
		}
		seen[tok] = true
	}
	if len(seen) != 16 {
		t.Fatalf("%d draws produced %d distinct tokens", 16, len(seen))
	}
}

func TestAuthenticate(t *testing.T) {
	tokenA := GenerateToken()
	tokenB := GenerateToken()
	auth := NewTokenAuth([]TokenRecord{
		{Token: tokenA, ActorType: "human", ActorID: "analyst-j", Scope: "team-a"},
		{Token: tokenB, ActorType: "agent", ActorID: "triage-bot-v3", Scope: "team-b"},
	})

	cases := []struct {
		name      string
		header    string
		wantOK    bool
		wantIdent Identity
	}{
		{
			name:      "correct token maps to its identity",
			header:    "Bearer " + tokenA,
			wantOK:    true,
			wantIdent: Identity{ActorType: "human", ActorID: "analyst-j", Scope: "team-a"},
		},
		{
			name:      "second token maps to the other identity",
			header:    "Bearer " + tokenB,
			wantOK:    true,
			wantIdent: Identity{ActorType: "agent", ActorID: "triage-bot-v3", Scope: "team-b"},
		},
		{name: "wrong token", header: "Bearer " + GenerateToken()},
		{name: "token with wrong prefix", header: "Bearer tok_" + tokenA[5:]},
		{name: "truncated token", header: "Bearer " + tokenA[:40]},
		{name: "garbage header", header: "Basic YW5hbHlzdDpwYXNz"},
		{name: "bearer without credential", header: "Bearer"},
		{name: "bearer with whitespace only", header: "Bearer    "},
		{name: "raw token missing scheme", header: tokenA},
		{name: "empty header", header: ""},
		{name: "lowercase scheme accepted", header: "bearer " + tokenB, wantOK: true,
			wantIdent: Identity{ActorType: "agent", ActorID: "triage-bot-v3", Scope: "team-b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := auth.Authenticate(tc.header)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (identity %+v)", ok, tc.wantOK, id)
			}
			if tc.wantOK && id != tc.wantIdent {
				t.Fatalf("identity = %+v, want %+v", id, tc.wantIdent)
			}
		})
	}
}

// The middleware is the fail-closed edge: unauthenticated requests get 401
// carrying WWW-Authenticate: Bearer and MUST NOT reach the wrapped handler;
// authenticated ones proceed with identity already in context.
func TestMiddleware(t *testing.T) {
	token := GenerateToken()
	auth := NewTokenAuth([]TokenRecord{
		{Token: token, ActorType: "human", ActorID: "analyst-j", Scope: "team-a"},
	})

	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		id, err := IdentityFrom(r.Context())
		if err != nil {
			t.Errorf("wrapped handler ran without injected identity: %v", err)
		}
		if id.Scope != "team-a" || id.ActorID != "analyst-j" {
			t.Errorf("injected identity = %+v", id)
		}
	})
	h := auth.Middleware(next)

	// No header → 401, WWW-Authenticate present, handler untouched.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/mcp", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no-token status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" || !regexp.MustCompile(`(?i)\bBearer\b`).MatchString(got) {
		t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", got)
	}
	if reached {
		t.Fatal("unauthenticated request reached MCP handling")
	}

	// Invalid token → same.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+GenerateToken())
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-token status = %d, want 401", rec.Code)
	}

	// Valid token → passes through once, with identity in ctx.
	reached = false
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !reached {
		t.Fatalf("valid token: status = %d reached = %v, want pass-through", rec.Code, reached)
	}
}

// The HTTPContextFunc layer re-injects identity from the raw request so
// mcp-go's own context derivation carries it even if middleware ordering
// ever changes.
func TestHTTPContextInjector(t *testing.T) {
	token := GenerateToken()
	auth := NewTokenAuth([]TokenRecord{
		{Token: token, ActorType: "agent", ActorID: "bot-9", Scope: "team-q"},
	})
	inject := auth.HTTPContextInjector()

	req, _ := http.NewRequestWithContext(context.Background(), "POST", "/mcp", nil)
	ctx := inject(context.Background(), req)
	if _, err := IdentityFrom(ctx); err == nil {
		t.Fatal("injector must not fabricate an identity for an anonymous request")
	}

	req.Header.Set("Authorization", "Bearer "+token)
	ctx = inject(context.Background(), req)
	id, err := IdentityFrom(ctx)
	if err != nil || id != (Identity{ActorType: "agent", ActorID: "bot-9", Scope: "team-q"}) {
		t.Fatalf("id=%+v err=%v, want bot-9 identity", id, err)
	}
}
