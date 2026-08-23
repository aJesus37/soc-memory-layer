package extract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// chatServer returns an httptest server that always replies with the
// given assistant content, and records the last request it received.
func chatServer(t *testing.T, content string) (*httptest.Server, *chatRequest) {
	t.Helper()
	var got chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			http.Error(w, "bad request json", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"role": "assistant", "content": content}},
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func chatClient(baseURL string) *Chat {
	return NewChat(Config{BaseURL: baseURL, Model: "qwen3-8b"})
}

const happyArray = `[
  {"subject":"203.0.113.7","predicate":"communicates_with","object_value":"evil.example.net","confidence":0.9},
  {"subject":"T1071","predicate":"uses","object_value":"beaconing over DNS","confidence":0.6},
  {"subject":"evil.example.net","predicate":"resolved_to","object_value":"198.51.100.9","confidence":0.8}
]`

func TestProposeHappyPath(t *testing.T) {
	srv, req := chatServer(t, happyArray)
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "some note")
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if len(props) != 3 {
		t.Fatalf("want 3 proposals, got %d: %+v", len(props), props)
	}
	wantSubjects := []string{"203.0.113.7", "T1071", "evil.example.net"}
	for i, w := range wantSubjects {
		if props[i].Subject != w {
			t.Errorf("order broken at %d: subject = %q, want %q", i, props[i].Subject, w)
		}
	}
	if props[0].Predicate != "communicates_with" || props[0].ObjectValue != "evil.example.net" {
		t.Errorf("unexpected first proposal: %+v", props[0])
	}
	if props[0].Confidence < 0.89 || props[0].Confidence > 0.91 {
		t.Errorf("confidence = %v, want ~0.9", props[0].Confidence)
	}

	if req.Model != "qwen3-8b" {
		t.Errorf("request model = %q, want qwen3-8b", req.Model)
	}
	if len(req.Messages) != 2 {
		t.Fatalf("messages = %d, want 2 (system,user)", len(req.Messages))
	}
	if req.Messages[0].Role != "system" || req.Messages[0].Content != SystemPrompt {
		t.Errorf("system message wrong; role=%q content-prefix=%q",
			req.Messages[0].Role, truncate(req.Messages[0].Content, 40))
	}
	if req.Messages[1].Role != "user" || req.Messages[1].Content != "some note" {
		t.Errorf("user message must carry note verbatim: %+v", req.Messages[1])
	}
	if req.Temperature != 0 {
		t.Errorf("temperature = %v, want 0", req.Temperature)
	}
	if req.MaxTokens != maxTokens {
		t.Errorf("max_tokens = %v, want %d", req.MaxTokens, maxTokens)
	}
}

func TestProposeFencedOutput(t *testing.T) {
	fenced := "```json" + happyArray + "```"
	srv, _ := chatServer(t, fenced)
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "note")
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if len(props) != 3 {
		t.Fatalf("want 3 proposals, got %d", len(props))
	}
}

func TestProposeFencedPlainOutput(t *testing.T) {
	fenced := "```\n" + happyArray + "\n```"
	srv, _ := chatServer(t, fenced)
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "note")
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if len(props) != 3 {
		t.Fatalf("want 3 proposals, got %d", len(props))
	}
}

func TestProposePreambleText(t *testing.T) {
	preamble := "Sure! Here are the facts I found:\n\n" + happyArray +
		"\n\nLet me know if you need anything else."
	srv, _ := chatServer(t, preamble)
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "note")
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if len(props) != 3 {
		t.Fatalf("want 3 proposals via bracket slicing, got %d", len(props))
	}
}

func TestProposeMalformedJSONAfterSlice(t *testing.T) {
	broken := `Found these: [{"subject":"203.0.113.7","predicate":}]`
	srv, _ := chatServer(t, broken)
	c := chatClient(srv.URL)

	_, err := c.Propose(context.Background(), "note")
	if err == nil {
		t.Fatal("want error for malformed JSON, got nil")
	}
	if !strings.Contains(err.Error(), "parse proposals") {
		t.Errorf("error should mention parse failure: %v", err)
	}
}

func TestProposeNoArrayAtAll(t *testing.T) {
	srv, _ := chatServer(t, "I cannot help with that.")
	c := chatClient(srv.URL)

	_, err := c.Propose(context.Background(), "note")
	if err == nil {
		t.Fatal("want error when output has no array, got nil")
	}
	if !strings.Contains(err.Error(), "no JSON array") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSanitizeMixedBatch(t *testing.T) {
	raws := []RawProposal{
		{Subject: "203.0.113.7", Predicate: "resolved_to", ObjectValue: "evil.example.net", Confidence: 0.95},                        // valid
		{Subject: "the whole internet", Predicate: "includes", ObjectValue: "everything", Confidence: 0.9},                           // bad subject
		{Subject: "evil.example.net", Predicate: "Communicates With!", ObjectValue: "c2.example.org", Confidence: 0.5},               // bad predicate
		{Subject: "44d88612fea8a8f36de82e1278abb02f", Predicate: "has_hash", ObjectValue: strings.Repeat("x", 501), Confidence: 0.5}, // oversized object
		{Subject: "  T1059  ", Predicate: "  uses_technique  ", ObjectValue: "  powershell  ", Confidence: -0.5},                     // trims + clamp to 0
		{Subject: "bad.com", Predicate: "ok_predicate", ObjectValue: "   ", Confidence: 0.5},                                         // empty object after trim
		{Subject: "10.0.0.1", Predicate: "communicates_with", ObjectValue: "evil.example.net", Confidence: 1.7},                      // clamp to 1
		{Subject: "", Predicate: "", ObjectValue: "", Confidence: 0.5},                                                               // all empty
	}

	got := Sanitize(raws)
	if len(got) != 3 {
		t.Fatalf("want 3 valid proposals, got %d: %+v", len(got), got)
	}

	first := got[0]
	if first.Subject != "203.0.113.7" || first.Predicate != "resolved_to" ||
		first.ObjectValue != "evil.example.net" {
		t.Errorf("first proposal mangled: %+v", first)
	}
	if first.Confidence < 0.94 || first.Confidence > 0.96 {
		t.Errorf("confidence = %v, want ~0.95", first.Confidence)
	}

	trimmed := got[1]
	if trimmed.Subject != "T1059" || trimmed.Predicate != "uses_technique" || trimmed.ObjectValue != "powershell" {
		t.Errorf("trim failed: %+v", trimmed)
	}
	if trimmed.Confidence != 0 {
		t.Errorf("confidence = %v, want clamped 0 for negative input", trimmed.Confidence)
	}

	clamped := got[2]
	if clamped.Confidence != 1 {
		t.Errorf("confidence = %v, want clamped 1", clamped.Confidence)
	}
}

func TestSanitizeNaNConfidence(t *testing.T) {
	got := Sanitize([]RawProposal{
		{Subject: "203.0.113.7", Predicate: "beacons_to", ObjectValue: "x.example.net", Confidence: float32(math.NaN())},
	})
	if len(got) != 1 {
		t.Fatalf("want 1 proposal, got %d", len(got))
	}
	if math.IsNaN(float64(got[0].Confidence)) || got[0].Confidence != 0 {
		t.Errorf("NaN confidence not mapped to 0: %v", got[0].Confidence)
	}
}

func TestSanitizeCapAt20(t *testing.T) {
	raws := make([]RawProposal, 35)
	for i := range raws {
		raws[i] = RawProposal{
			Subject:     fmt.Sprintf("10.0.0.%d", i%250+1),
			Predicate:   "relates_to",
			ObjectValue: fmt.Sprintf("host-%d.example.net", i),
			Confidence:  0.5,
		}
	}
	got := Sanitize(raws)
	if len(got) != maxProposals {
		t.Fatalf("want cap of %d, got %d", maxProposals, len(got))
	}
	if got[19].ObjectValue != "host-19.example.net" {
		t.Errorf("cap should keep the FIRST items in order: last=%+v", got[19])
	}
}

func TestProposeEmptyArray(t *testing.T) {
	srv, _ := chatServer(t, "[]")
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "nothing here")
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if len(props) != 0 {
		t.Fatalf("want zero proposals, got %+v", props)
	}
}

func TestProposeEmptyArrayWithPreamble(t *testing.T) {
	srv, _ := chatServer(t, "No explicit facts found.\n[]\nHope that helps!")
	c := chatClient(srv.URL)

	props, err := c.Propose(context.Background(), "nothing here")
	if err != nil || len(props) != 0 {
		t.Fatalf("Propose = (%+v, %v), want (empty, nil)", props, err)
	}
}

func TestProposeMissingChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{}})
	}))
	defer srv.Close()

	_, err := chatClient(srv.URL).Propose(context.Background(), "note")
	if err == nil {
		t.Fatal("want error for missing choices, got nil")
	}
	if !strings.Contains(err.Error(), "no choices") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProposeMissingContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"role": "assistant"}}},
		})
	}))
	defer srv.Close()

	_, err := chatClient(srv.URL).Propose(context.Background(), "note")
	if err == nil {
		t.Fatal("want error for empty model output, got nil")
	}
	if !strings.Contains(err.Error(), "no JSON array") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProposeStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"model exploded"}}`, http.StatusBadGateway)
	}))
	defer srv.Close()

	_, err := chatClient(srv.URL).Propose(context.Background(), "note")

	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("want *StatusError, got %T: %v", err, err)
	}
	if statusErr.Status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", statusErr.Status)
	}
	if !strings.Contains(statusErr.Body, "model exploded") {
		t.Errorf("body not captured: %q", statusErr.Body)
	}
	if !strings.Contains(statusErr.Error(), "status 502") {
		t.Errorf("Error() should include status: %q", statusErr.Error())
	}
}

func TestProposeCtxCancelHonored(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer func() { close(release); srv.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		cancel()
	}()
	_, err := chatClient(srv.URL).Propose(ctx, "note")
	if err == nil {
		t.Fatal("want error on cancelled ctx, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error should wrap context.Canceled: %v", err)
	}
}

func TestProposeUnconfiguredClient(t *testing.T) {
	c := NewChat(Config{BaseURL: "", Model: ""})
	if _, err := c.Propose(context.Background(), "note"); err == nil {
		t.Fatal("want error for unconfigured client, got nil")
	}
}

func TestParseArrayStripsThinkBlocks(t *testing.T) {
	in := "<think>\nLet me analyze the note. The IP is mentioned.\n</think>\n[{\"subject\":\"203.0.113.7\",\"predicate\":\"resolved_to\",\"object_value\":\"evil.net\",\"confidence\":0.9}]"
	proposals, err := parseArray(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(proposals) != 1 || proposals[0].Subject != "203.0.113.7" {
		t.Fatalf("got %+v", proposals)
	}
	// unterminated think block: model never answered
	if _, err := parseArray("<think>only reasoning so far and no close"); err == nil {
		t.Fatal("unterminated think block should error")
	}
}
