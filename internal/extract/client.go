package extract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	"socmem/internal/entity"
)

// maxResponseBytes caps model responses; a hostile/broken endpoint must not
// stream unbounded into memory. 10 MiB dwarfs any legitimate embedding or
// chat payload.
const maxResponseBytes = 10 << 20

const defaultTimeout = 60 * time.Second

const (
	maxTokens      = 800
	maxProposals   = 20
	maxObjectValue = 500
)

// Proposal is a candidate fact the model claims is stated in a note.
// It has NOT been deduplicated or asserted; callers decide persistence.
type Proposal struct {
	Subject     string  `json:"subject"`      // entity key text, must Normalize() cleanly
	Predicate   string  `json:"predicate"`    // short_snake_case
	ObjectValue string  `json:"object_value"` // free-form value, non-empty, <=500 chars
	Confidence  float32 `json:"confidence"`   // clamped to [0,1]
}

// ChatClient asks the model for candidate facts stated in content.
type ChatClient interface {
	// Propose returns parsed proposals or an error. Never panics on
	// model output; malformed responses come back as errors or as
	// silently dropped items.
	Propose(ctx context.Context, content string) ([]Proposal, error)
}

// Config configures the OpenAI-compatible chat client.
type Config struct {
	BaseURL string       // e.g. http://localhost:1234/v1
	Model   string       // e.g. qwen3-8b
	APIKey  string       // bearer token; sent only when non-empty
	HTTP    *http.Client // optional override; default 60s timeout
}

// Chat is a ChatClient backed by any OpenAI-compatible /chat/completions
// endpoint (LM Studio, OpenAI, ...).
type Chat struct {
	cfg    Config
	client *http.Client
}

// NewChat returns a client for cfg.BaseURL. BaseURL and Model must be
// non-empty; since the constructor cannot fail, unconfigured clients are
// rejected at Propose time instead.
func NewChat(cfg Config) *Chat {
	httpClient := cfg.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Chat{cfg: cfg, client: httpClient}
}

// StatusError is returned for non-200 responses.
type StatusError struct {
	Status int
	Body   string // truncated response body
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("chat request failed: status %d: %s", e.Status, e.Body)
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// RawProposal is an unsanitized model output item. Exported so Sanitize
// can be reused outside the package (e.g. by tests or the worker).
type RawProposal struct {
	Subject     string  `json:"subject"`
	Predicate   string  `json:"predicate"`
	ObjectValue string  `json:"object_value"`
	Confidence  float32 `json:"confidence"`
}

var predicateRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// Propose implements ChatClient.
func (c *Chat) Propose(ctx context.Context, content string) ([]Proposal, error) {
	if c.cfg.BaseURL == "" || c.cfg.Model == "" {
		return nil, fmt.Errorf("extract: BaseURL/Model not configured")
	}

	userContent := "Extract facts from this investigation note. Return ONLY a JSON array per the system instructions — no prose, no explanation:\n\n" + content + "\n\nJSON array:"

	reqBody, err := json.Marshal(chatRequest{
		Model: c.cfg.Model,
		Messages: []chatMessage{
			{Role: "system", Content: SystemPrompt},
			{Role: "user", Content: userContent},
		},
		Temperature: 0,
		MaxTokens:   maxTokens,
	})
	if err != nil {
		return nil, fmt.Errorf("extract: marshal request: %w", err)
	}

	base := strings.TrimRight(c.cfg.BaseURL, "/")
	base = strings.TrimSuffix(base, "/chat/completions")
	base = strings.TrimSuffix(base, "/chat")
	url := base + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("extract: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("extract: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("extract: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			Status: resp.StatusCode,
			Body:   truncate(string(body), 512),
		}
	}

	var parsed chatResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("extract: decode response envelope: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return nil, fmt.Errorf("extract: response has no choices")
	}
	modelOut := parsed.Choices[0].Message.Content

	raws, err := parseArray(modelOut)
	if err != nil {
		if strings.Contains(err.Error(), "no JSON array") {
			return []Proposal{}, nil
		}
		return nil, fmt.Errorf("%w (model output snippet: %.300q)", err, truncate(modelOut, 300))
	}
	return Sanitize(raws), nil
}

// parseArray pulls a JSON array out of arbitrary model text: strips
// reasoning/think blocks and code fences, then slices from the first '[' to
// the last ']' so preamble and trailing commentary do not break decoding.
func parseArray(content string) ([]RawProposal, error) {
	s := stripThinkBlocks(stripFences(content))

	start := strings.Index(s, "[")
	end := strings.LastIndex(s, "]")
	if start < 0 || end < 0 || end < start {
		return nil, fmt.Errorf("extract: no JSON array in model output")
	}

	var raws []RawProposal
	if err := json.Unmarshal([]byte(s[start:end+1]), &raws); err != nil {
		return nil, fmt.Errorf("extract: parse proposals: %w", err)
	}
	return raws, nil
}

// stripThinkBlocks removes <think>…</think> reasoning blocks that thinking
// models (Qwen3, DeepSeek-R1, …) emit before their answer. Non-greedy so
// multiple blocks are each removed; unterminated think blocks swallow the
// remainder (the model never answered).
func stripThinkBlocks(s string) string {
	for {
		open := strings.Index(s, "<think>")
		if open < 0 {
			return s
		}
		close_ := strings.Index(s[open:], "</think>")
		if close_ < 0 {
			return strings.TrimSpace(s[:open])
		}
		s = s[:open] + s[open+close_+len("</think>"):]
	}
}

// stripFences removes a leading markdown code fence (``` or ```lang)
// and a trailing ``` fence when present. It never removes '[' or ']',
// so bracket slicing downstream stays correct even on odd inputs; a
// trailing fence is only cut if nothing but whitespace follows it.
func stripFences(s string) string {
	t := strings.TrimSpace(s)
	if !strings.HasPrefix(t, "```") {
		return s
	}
	body := t[3:]
	// Skip the optional fence language tag, e.g. "json". This tolerates
	// both "```json\n[...]" and degenerate inline forms like "```json[...]".
	for len(body) > 0 && isASCIILetterOrDigit(body[0]) {
		body = body[1:]
	}
	body = strings.TrimLeft(body, " \t\r\n")
	if i := strings.LastIndex(body, "```"); i >= 0 && strings.TrimSpace(body[i+3:]) == "" {
		body = body[:i]
	}
	return body
}

func isASCIILetterOrDigit(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// Sanitize turns raw model items into validated proposals. Items failing
// validation are dropped individually — one bad item never poisons the
// batch. Output is capped at maxProposals, order preserved.
func Sanitize(raws []RawProposal) []Proposal {
	out := make([]Proposal, 0, len(raws))
	for _, r := range raws {
		p, ok := sanitizeOne(r)
		if !ok {
			continue
		}
		out = append(out, p)
		if len(out) == maxProposals {
			break
		}
	}
	return out
}

func sanitizeOne(r RawProposal) (Proposal, bool) {
	subject := strings.TrimSpace(r.Subject)
	if _, err := entity.Normalize(subject); err != nil {
		return Proposal{}, false
	}
	predicate := strings.TrimSpace(r.Predicate)
	if !predicateRe.MatchString(predicate) {
		return Proposal{}, false
	}
	object := strings.TrimSpace(r.ObjectValue)
	if object == "" || len(object) > maxObjectValue {
		return Proposal{}, false
	}
	return Proposal{
		Subject:     subject,
		Predicate:   predicate,
		ObjectValue: object,
		Confidence:  clampConfidence(r.Confidence),
	}, true
}

func clampConfidence(c float32) float32 {
	switch {
	case math.IsNaN(float64(c)):
		return 0
	case c < 0:
		return 0
	case c > 1:
		return 1
	default:
		return c
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
