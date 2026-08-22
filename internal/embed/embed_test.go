package embed

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func writeEmbeddings(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Type", "application/json")
	data := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		data[i] = map[string]any{"index": i, "embedding": []float32{float32(i), float32(i)}}
	}
	json.NewEncoder(w).Encode(map[string]any{"data": data})
}

func TestFakeDeterministic(t *testing.T) {
	f := NewFake(8)
	ctx := context.Background()

	a1, err := f.Embed(ctx, "document", []string{"evil.example.com"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	a2, err := f.Embed(ctx, "document", []string{"evil.example.com"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(a1) != 1 || len(a2) != 1 {
		t.Fatalf("want 1 vector each, got %d and %d", len(a1), len(a2))
	}
	for i := range a1[0] {
		if a1[0][i] != a2[0][i] {
			t.Fatalf("same input produced different vectors at [%d]: %v vs %v", i, a1[0], a2[0])
		}
	}
}

func TestFakeDifferentTextsDiffer(t *testing.T) {
	f := NewFake(8)
	vecs, err := f.Embed(context.Background(), "document", []string{"alpha", "beta"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	same := true
	for i := range vecs[0] {
		if vecs[0][i] != vecs[1][i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different texts produced identical vectors")
	}
}

func TestFakeKindMatters(t *testing.T) {
	f := NewFake(8)
	doc, err := f.Embed(context.Background(), "document", []string{"x"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	qry, err := f.Embed(context.Background(), "query", []string{"x"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	for i := range doc[0] {
		if doc[0][i] != qry[0][i] {
			return
		}
	}
	t.Fatal("same text with different kinds produced identical vectors")
}

func TestFakeUnitNorm(t *testing.T) {
	f := NewFake(64)
	vecs, err := f.Embed(context.Background(), "document", []string{"norm check"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	var sum float64
	for _, v := range vecs[0] {
		sum += float64(v) * float64(v)
	}
	if math.Abs(math.Sqrt(sum)-1.0) > 1e-5 {
		t.Fatalf("L2 norm = %f, want ~1.0", math.Sqrt(sum))
	}
}

func TestFakeOrderPreserved(t *testing.T) {
	f := NewFake(8)
	in := []string{"one", "two", "three"}
	byText := map[string][]float32{}
	for _, s := range in {
		v, err := f.Embed(context.Background(), "document", []string{s})
		if err != nil {
			t.Fatalf("Embed: %v", err)
		}
		byText[s] = v[0]
	}
	got, err := f.Embed(context.Background(), "document", in)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	for i, s := range in {
		for j := range got[i] {
			if got[i][j] != byText[s][j] {
				t.Fatalf("order not preserved at %d (%s)", i, s)
			}
		}
	}
}

func TestOpenAIBodyAndPrefix(t *testing.T) {
	var body struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("path = %q, want /v1/embeddings", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeEmbeddings(w, len(body.Input))
	}))
	defer srv.Close()

	c := NewOpenAI(Config{BaseURL: srv.URL + "/v1", Model: "nomic-embed-text"})
	if _, err := c.Embed(context.Background(), "document", []string{"8.8.8.8", "bad host"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if body.Model != "nomic-embed-text" {
		t.Errorf("model = %q", body.Model)
	}
	want := []string{prefixDocument + "8.8.8.8", prefixDocument + "bad host"}
	if len(body.Input) != 2 || body.Input[0] != want[0] || body.Input[1] != want[1] {
		t.Errorf("input = %q, want %q", body.Input, want)
	}

	if _, err := c.Embed(context.Background(), "query", []string{"who did this"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if body.Input[0] != prefixQuery+"who did this" {
		t.Errorf("query input = %q, want prefix %q", body.Input[0], prefixQuery)
	}
}

func TestOpenAIRestoresOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return embeddings out of order: index 1 before index 0.
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 1, "embedding": []float32{4, 5, 6}},
				{"index": 0, "embedding": []float32{1, 2, 3}},
			},
		})
	}))
	defer srv.Close()

	c := NewOpenAI(Config{BaseURL: srv.URL, Model: "m"})
	vecs, err := c.Embed(context.Background(), "document", []string{"a", "b"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 2 {
		t.Fatalf("len(vecs) = %d, want 2", len(vecs))
	}
	if vecs[0][0] != 1 || vecs[1][0] != 4 {
		t.Fatalf("order not restored: %v %v", vecs[0], vecs[1])
	}
}

func TestOpenAIEmptyInput(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewOpenAI(Config{BaseURL: srv.URL, Model: "m"})
	vecs, err := c.Embed(context.Background(), "document", nil)
	if err != nil {
		t.Fatalf("Embed(nil): %v", err)
	}
	if vecs != nil {
		t.Errorf("vecs = %v, want nil", vecs)
	}
	vecs, err = c.Embed(context.Background(), "document", []string{})
	if err != nil {
		t.Fatalf("Embed(empty): %v", err)
	}
	if vecs != nil {
		t.Errorf("vecs = %v, want nil", vecs)
	}
	if called {
		t.Error("API was called for empty input")
	}
}

func TestOpenAIUnknownKind(t *testing.T) {
	c := NewOpenAI(Config{})
	if _, err := c.Embed(context.Background(), "bogus", []string{"x"}); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestOpenAINon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not loaded", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewOpenAI(Config{BaseURL: srv.URL, Model: "missing"})
	_, err := c.Embed(context.Background(), "document", []string{"x"})
	var serr *StatusError
	if !errors.As(err, &serr) {
		t.Fatalf("err = %v (%T), want *StatusError", err, err)
	}
	if serr.Status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", serr.Status)
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error %q missing status code", err)
	}
	if !strings.Contains(err.Error(), "model not loaded") {
		t.Errorf("error %q missing truncated body", err)
	}
}

func TestOpenAITimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	c := NewOpenAI(Config{
		BaseURL: srv.URL,
		Model:   "m",
		HTTP:    &http.Client{Timeout: 50 * time.Millisecond},
	})
	_, err := c.Embed(context.Background(), "document", []string{"x"})
	if err == nil {
		t.Fatal("timeout produced nil error")
	}
}
