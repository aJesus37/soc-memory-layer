package embed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestFakeUnknownKind(t *testing.T) {
	f := NewFake(8)
	if _, err := f.Embed(context.Background(), "bogus", []string{"x"}); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestOpenAIRestoresOrder(t *testing.T) {
	const n = 10
	const dim = 768
	inputs := make([]string, n)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("text-%02d", i)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if len(req.Input) != n {
			t.Errorf("len(input) = %d, want %d", len(req.Input), n)
		}
		want := make([]string, n)
		for i, s := range inputs {
			want[i] = prefixDocument + s
		}
		if !reflect.DeepEqual(req.Input, want) {
			t.Errorf("input = %q, want %q", req.Input, want)
		}

		// Respond with embeddings in shuffled index order.
		data := make([]map[string]any, n)
		for pos, idx := range []int{3, 7, 0, 9, 2, 5, 8, 1, 6, 4} {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = float32(idx+1) / float32(j+7)
			}
			data[pos] = map[string]any{"index": idx, "embedding": vec}
		}
		payload, err := json.Marshal(map[string]any{"data": data})
		if err != nil {
			t.Errorf("marshal payload: %v", err)
			return
		}
		// Guard the regression this test locks: n x dim fakes must exceed
		// the removed 64KiB read cap, else truncation would go unnoticed.
		if len(payload) <= 64<<10 {
			t.Errorf("response body = %d bytes, want > %d to exercise the removed cap", len(payload), 64<<10)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	}))
	defer srv.Close()

	c := NewOpenAI(Config{BaseURL: srv.URL, Model: "m"})
	vecs, err := c.Embed(context.Background(), "document", inputs)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != n {
		t.Fatalf("len(vecs) = %d, want %d", len(vecs), n)
	}
	for i := range inputs {
		if len(vecs[i]) != dim {
			t.Fatalf("len(vecs[%d]) = %d, want %d", i, len(vecs[i]), dim)
		}
		for j := 0; j < dim; j++ {
			if want := float32(i+1) / float32(j+7); vecs[i][j] != want {
				t.Fatalf("order not restored at [%d][%d]: got %v, want %v", i, j, vecs[i][j], want)
			}
		}
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

func TestOpenAIUnconfigured(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{BaseURL: "http://localhost:1234/v1"},
		{Model: "nomic-embed-text"},
	} {
		c := NewOpenAI(cfg)
		if _, err := c.Embed(context.Background(), "document", []string{"x"}); err == nil {
			t.Fatalf("cfg %+v: Embed succeeded on unconfigured client", cfg)
		} else if !strings.Contains(err.Error(), "not configured") {
			t.Fatalf("cfg %+v: error %q missing \"not configured\"", cfg, err)
		}
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
