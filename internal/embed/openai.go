package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxResponseBytes caps model responses; a hostile/broken endpoint must not
// stream unbounded into memory. 10 MiB dwarfs any legitimate embedding or
// chat payload.
const maxResponseBytes = 10 << 20

const (
	prefixDocument = "search_document: "
	prefixQuery    = "search_query: "
)

const defaultTimeout = 30 * time.Second

// Config configures the OpenAI-compatible embeddings client.
type Config struct {
	BaseURL string       // e.g. http://localhost:1234/v1
	Model   string       // e.g. nomic-embed-text-v1.5
	HTTP    *http.Client // optional override; default 30s timeout
}

// OpenAI is an Embedder backed by any OpenAI-compatible /embeddings
// endpoint (LM Studio, OpenAI, ...).
type OpenAI struct {
	cfg    Config
	client *http.Client
}

// NewOpenAI returns a client for cfg.BaseURL. BaseURL and Model must be
// non-empty; since the constructor cannot fail, unconfigured clients are
// rejected at Embed time instead.
func NewOpenAI(cfg Config) *OpenAI {
	httpClient := cfg.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &OpenAI{cfg: cfg, client: httpClient}
}

// StatusError is returned for non-200 responses.
type StatusError struct {
	Status int
	Body   string // truncated response body
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("embeddings request failed: status %d: %s", e.Status, e.Body)
}

type openAIRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type openAIResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed implements Embedder. Empty input returns nil without an API call.
func (c *OpenAI) Embed(ctx context.Context, kind string, texts []string) ([][]float32, error) {
	if c.cfg.BaseURL == "" || c.cfg.Model == "" {
		return nil, fmt.Errorf("embed: BaseURL/Model not configured")
	}

	switch kind {
	case "document":
		kind = prefixDocument
	case "query":
		kind = prefixQuery
	default:
		return nil, fmt.Errorf("embed: unknown kind %q (want \"document\" or \"query\")", kind)
	}

	if len(texts) == 0 {
		return nil, nil
	}

	input := make([]string, len(texts))
	for i, t := range texts {
		input[i] = kind + t
	}

	reqBody, err := json.Marshal(openAIRequest{Model: c.cfg.Model, Input: input})
	if err != nil {
		return nil, fmt.Errorf("embed: marshal request: %w", err)
	}

	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("embed: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("embed: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			Status: resp.StatusCode,
			Body:   truncate(string(body), 512),
		}
	}

	var parsed openAIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("embed: decode response: %w", err)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("embed: got %d embeddings, want %d", len(parsed.Data), len(texts))
	}

	out := make([][]float32, len(texts))
	for _, d := range parsed.Data {
		if d.Index < 0 || d.Index >= len(out) {
			return nil, fmt.Errorf("embed: embedding index %d out of range", d.Index)
		}
		out[d.Index] = d.Embedding
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("embed: missing embedding for index %d", i)
		}
	}
	return out, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
