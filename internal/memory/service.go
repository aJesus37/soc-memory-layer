// Package memory implements the SOC memory write path: recording
// observations with entity linking, embedding, and audit trails.
package memory

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
)

// maxEntityCandidates bounds entity extraction per observation so a single
// huge document cannot fan out into an unbounded resolution round trip.
const maxEntityCandidates = 32

// Input is one observation to persist. Kind, ActorType and
// Confidentiality are validated against the schema's Enum8 value sets;
// Confidentiality defaults to internal when empty. Ts zero means now.
//
// ClientEventID makes retries idempotent at the ID level: empty means a
// fresh UUID is generated; non-empty must parse as a UUID and becomes the
// row's obs_id in canonical form (so equivalent spellings collide). There
// is NO storage-level dedup on obs_id — mem.observations is a plain
// MergeTree, so resubmitting the same key inserts a second physical row
// sharing the ID rather than overwriting the first. Treat ClientEventID as
// a best-effort correlation hint, not an upsert key.
type Input struct {
	Scope           string
	Kind            string // alert|triage_decision|investigation_note|hunt_finding|agent_action|human_statement
	ActorType       string // human|agent
	ActorID         string
	OnBehalfOf      string // optional; set when an agent acts for a human
	CaseID          string // optional uuid string, "" = NULL
	ClientEventID   string // optional uuid string; "" = generate, else used as obs_id
	Confidentiality string // internal|restricted, default internal
	Ts              time.Time
	Content         string
}

// Observation is the persisted result of a RecordObservation call.
type Observation struct {
	ID        string
	Scope     string
	Ts        time.Time
	Kind      string
	ActorType string
	ActorID   string
	EntityIDs []string
	Embedded  bool
}

var (
	validKinds = map[string]bool{
		"alert":              true,
		"triage_decision":    true,
		"investigation_note": true,
		"hunt_finding":       true,
		"agent_action":       true,
		"human_statement":    true,
	}
	validActorTypes = map[string]bool{"human": true, "agent": true}
	validConf       = map[string]bool{"internal": true, "restricted": true}
)

// Service records observations into mem.observations, linking entities via
// entity.Resolver and embedding content via embed.Embedder.
type Service struct {
	conn     driver.Conn
	resolver *entity.Resolver
	embedder embed.Embedder
	cfg      config.Config
	log      *slog.Logger
}

// New builds a Service over an open ClickHouse connection.
func New(conn driver.Conn, r *entity.Resolver, e embed.Embedder, cfg config.Config) *Service {
	return &Service{
		conn:     conn,
		resolver: r,
		embedder: e,
		cfg:      cfg,
		log:      slog.Default(),
	}
}

// RecordObservation validates input, links entities found in content,
// embeds the content (degrading to no vector if the embedder fails), and
// persists the row. The audit entry's payload_summary carries counts only
// — never content or IOC values.
//
// Audit is best-effort: a failed audit insert is logged and swallowed, so
// observation success is authoritative even if its audit trail row is
// missing.
//
// When Input.ClientEventID is set it becomes the observation ID (see Input),
// but storage does not collapse duplicates: a retry with the same key yields
// a second physical row with the same obs_id.
func (s *Service) RecordObservation(ctx context.Context, in Input) (Observation, error) {
	scope, kind, actorType, actorID, onBehalfOf, conf, caseUUID, clientEventID, err := validate(in)
	if err != nil {
		return Observation{}, err
	}

	candidates := extractEntityCandidates(in.Content)
	ents, err := s.resolver.ResolveBatch(ctx, scope, candidates)
	if err != nil {
		return Observation{}, fmt.Errorf("memory: resolve entities: %w", err)
	}
	entityIDs := make([]string, len(ents))
	refUUIDs := make([]uuid.UUID, len(ents))
	for i, e := range ents {
		entityIDs[i] = e.EntityID
		u, err := uuid.Parse(e.EntityID)
		if err != nil {
			return Observation{}, fmt.Errorf("memory: entity id %q: %w", e.EntityID, err)
		}
		refUUIDs[i] = u
	}

	vec, embedded := s.embed(ctx, in.Content)

	ts := in.Ts
	if ts.IsZero() {
		ts = time.Now()
	}
	ts = ts.UTC()

	obsID := clientEventID
	if obsID == "" {
		obsID = uuid.NewString()
	}
	b, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO mem.observations "+
			"(obs_id, scope, ts, kind, actor_type, actor_id, on_behalf_of, case_id, "+
			"confidentiality, content, content_vec, entity_refs)")
	if err != nil {
		return Observation{}, fmt.Errorf("memory: stage observation insert: %w", err)
	}
	var caseID any // nil interface -> NULL in Nullable(UUID)
	if caseUUID != nil {
		caseID = *caseUUID
	}
	if err := b.Append(obsID, scope, ts, kind, actorType, actorID, onBehalfOf,
		caseID, conf, in.Content, vec, refUUIDs); err != nil {
		return Observation{}, fmt.Errorf("memory: append observation row: %w", err)
	}
	if err := b.Send(); err != nil {
		return Observation{}, fmt.Errorf("memory: commit observation %s: %w", obsID, err)
	}

	// Audit summaries carry counts ONLY. Never content, never IOC values.
	// Audit is best-effort: the observation write already committed, so a
	// failed audit insert is logged and swallowed rather than failing the
	// call — observation success is authoritative.
	summary := fmt.Sprintf("kind=%s actors=1 entities=%d chars=%d",
		kind, len(entityIDs), len(in.Content))
	if err := s.conn.Exec(ctx,
		"INSERT INTO mem.audit "+
			"(actor_type, actor_id, operation, target_table, target_id, payload_summary) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		actorType, actorID, "record_observation", "observations", obsID, summary); err != nil {
		s.log.Warn("memory: audit insert failed; observation stands",
			"operation", "record_observation",
			"target_table", "observations",
			"target_id", obsID,
			"err", err)
	}

	return Observation{
		ID:        obsID,
		Scope:     scope,
		Ts:        ts,
		Kind:      kind,
		ActorType: actorType,
		ActorID:   actorID,
		EntityIDs: entityIDs,
		Embedded:  embedded,
	}, nil
}

// embed returns the content vector; ANY embedder failure degrades to an
// empty vector so the observation still persists.
func (s *Service) embed(ctx context.Context, content string) ([]float32, bool) {
	vecs, err := s.embedder.Embed(ctx, "document", []string{content})
	if err != nil {
		s.log.Warn("memory: embed failed, persisting without vector", "err", err)
		return []float32{}, false
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		s.log.Warn("memory: embed returned unexpected shape, persisting without vector",
			"vectors", len(vecs))
		return []float32{}, false
	}
	return vecs[0], true
}

func validate(in Input) (scope, kind, actorType, actorID, onBehalfOf, conf string, caseID *uuid.UUID, clientEventID string, err error) {
	scope = strings.TrimSpace(in.Scope)
	if scope == "" {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: scope required")
	}
	if !validKinds[in.Kind] {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: invalid kind %q", in.Kind)
	}
	if !validActorTypes[in.ActorType] {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: invalid actor type %q", in.ActorType)
	}
	actorID = strings.TrimSpace(in.ActorID)
	if actorID == "" {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: actor id required")
	}
	onBehalfOf = strings.TrimSpace(in.OnBehalfOf) // optional, never required

	conf = in.Confidentiality
	if conf == "" {
		conf = "internal"
	}
	if !validConf[conf] {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: invalid confidentiality %q", in.Confidentiality)
	}

	if strings.TrimSpace(in.Content) == "" {
		return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: content required")
	}

	if cid := strings.TrimSpace(in.CaseID); cid != "" {
		u, err := uuid.Parse(cid)
		if err != nil {
			return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: invalid case id %q", in.CaseID)
		}
		caseID = &u
	}

	clientEventID = strings.TrimSpace(in.ClientEventID)
	if clientEventID != "" {
		u, err := uuid.Parse(clientEventID)
		if err != nil {
			return "", "", "", "", "", "", nil, "", fmt.Errorf("memory: invalid client event id %q", in.ClientEventID)
		}
		clientEventID = u.String() // canonical form so equivalent spellings collide
	}
	return scope, in.Kind, in.ActorType, actorID, onBehalfOf, conf, caseID, clientEventID, nil
}

// extractEntityCandidates splits content on runes outside [A-Za-z0-9.:],
// post-processes host:port shaped tokens down to their host part (see
// stripHostPort), keeps unique candidates in order of first appearance,
// filters them through entity.Normalize and caps the result at
// maxEntityCandidates. Junk never consumes cap slots: the cap counts
// accepted candidates only, so filler text ahead of real indicators cannot
// crowd them out.
//
// Note that '[' and ']' are token delimiters, so bracketed IPv6 literals
// such as "[2001:db8::1]:8080" arrive already split into "2001:db8::1"
// (which normalizes) plus a ":8080" fragment (which does not, and is
// discarded) — no bracket handling is needed here.
func extractEntityCandidates(content string) []string {
	seen := make(map[string]bool)
	var cands []string
	cur := make([]rune, 0, 32)
	flush := func() {
		if len(cur) == 0 {
			return
		}
		tok := string(cur)
		cur = cur[:0]
		tok = stripHostPort(tok)
		if seen[tok] || len(cands) >= maxEntityCandidates {
			return
		}
		if _, err := entity.Normalize(tok); err != nil {
			return // not an entity-shaped token; not an error
		}
		seen[tok] = true
		cands = append(cands, tok)
	}
	for _, r := range content {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '.', r == ':':
			cur = append(cur, r)
		default:
			flush()
		}
	}
	flush()
	return cands
}

// stripHostPort reduces a "host:port" raw token to its host part when the
// token contains exactly one ':', everything after it is digits, and the
// prefix itself normalizes as an IP or domain ("8.8.8.8:53" → "8.8.8.8").
// Any other shape is returned unchanged: bare IPv6 ("2001:db8::1") has
// multiple colons, time-like pairs ("12:34") have a non-entity prefix, and
// a lone ":8080" fragment has an empty host.
func stripHostPort(tok string) string {
	if strings.Count(tok, ":") != 1 {
		return tok
	}
	host, port, _ := strings.Cut(tok, ":")
	if host == "" || port == "" {
		return tok
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return tok
		}
	}
	if _, err := entity.Normalize(host); err != nil {
		return tok
	}
	return host
}
