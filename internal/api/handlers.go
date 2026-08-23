package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"socmem/internal/entity"
	"socmem/internal/memory"
)

// decodeJSON decodes r.Body with a hard size cap and rejects unknown fields
// (typos in client payloads fail loudly instead of silently no-oping).
// Trailing data after the first JSON value is rejected too.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := decodeJSONBody(w, r, dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return false
	}
	return true
}

// decodeJSONBody performs the strict decode and returns the raw error so
// callers can special-case io.EOF (the retract endpoint tolerates empty and
// truncated bodies). After the first value, a second Decode must yield
// io.EOF — anything else is trailing garbage.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON value")
	}
	return nil
}

// --- POST /v1/observations -------------------------------------------------

type createObservationReq struct {
	Kind            string `json:"kind"`
	CaseID          string `json:"case_id,omitempty"`
	ClientEventID   string `json:"client_event_id,omitempty"`
	Confidentiality string `json:"confidentiality,omitempty"`
	Ts              string `json:"ts,omitempty"` // RFC3339; omitted = now
	Content         string `json:"content"`
}

type observationResp struct {
	ID        string    `json:"id"`
	Scope     string    `json:"scope"`
	Ts        time.Time `json:"ts"`
	Kind      string    `json:"kind"`
	ActorType string    `json:"actor_type"`
	ActorID   string    `json:"actor_id"`
	EntityIDs []string  `json:"entity_ids"`
	Embedded  bool      `json:"embedded"`
}

// CreateObservation handles POST /v1/observations.
//
//	@Summary		Record an observation
//	@Description	Store a new episodic observation; actor identity comes from X-Actor-* headers, never the body.
//	@Tags			write
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			request	body		createObservationReq	true	"Observation payload"
//	@Success		200		{object}	observationResp			"Created observation"
//	@Failure		400		{object}	errBody					"Invalid body, oversized content, or invalid ts"
//	@Failure		429		{object}	errBody					"Agent write budget exhausted (Retry-After set)"
//	@Failure		502		{object}	errBody					"Storage temporarily unavailable"
//	@Router			/v1/observations [post]
func (s *Server) handleCreateObservation(w http.ResponseWriter, r *http.Request) {
	var req createObservationReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Content) > maxObservationContent {
		writeErr(w, http.StatusBadRequest, "content_too_large",
			"content exceeds maximum length")
		return
	}
	ts := time.Time{}
	if req.Ts != "" {
		parsed, err := time.Parse(time.RFC3339, req.Ts)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_ts",
				"ts must be RFC3339")
			return
		}
		ts = parsed
	}
	o, err := s.svc.RecordObservation(r.Context(), memory.Input{
		Scope:           ctxString(r, ctxScope),
		Kind:            req.Kind,
		ActorType:       ctxString(r, ctxActorType),
		ActorID:         ctxString(r, ctxActorID),
		OnBehalfOf:      ctxString(r, ctxOnBehalfOf), // header only; never the body
		CaseID:          req.CaseID,
		ClientEventID:   req.ClientEventID,
		Confidentiality: req.Confidentiality,
		Ts:              ts,
		Content:         req.Content,
	})
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, observationResp{
		ID: o.ID, Scope: o.Scope, Ts: o.Ts, Kind: o.Kind,
		ActorType: o.ActorType, ActorID: o.ActorID,
		EntityIDs: o.EntityIDs, Embedded: o.Embedded,
	})
}

// --- POST /v1/facts --------------------------------------------------------

type assertFactReq struct {
	SubjectID     string  `json:"subject_id"`
	Predicate     string  `json:"predicate"`
	ObjectValue   string  `json:"object_value"`
	ObjectID      string  `json:"object_id,omitempty"`
	Confidence    float32 `json:"confidence"`
	SourceObs     string  `json:"source_obs,omitempty"`
	ClientEventID string  `json:"client_event_id,omitempty"`
}

type factResp struct {
	ID          string    `json:"id"`
	Scope       string    `json:"scope"`
	SubjectID   string    `json:"subject_id"`
	Predicate   string    `json:"predicate"`
	ObjectValue string    `json:"object_value"`
	Status      string    `json:"status"`
	Confidence  float32   `json:"confidence"`
	ValidFrom   time.Time `json:"valid_from"`
	WrittenBy   string    `json:"written_by"`
}

// AssertFact handles POST /v1/facts.
//
//	@Summary		Assert a fact
//	@Description	Assert a subject-predicate-object fact; agents propose, humans write active versions directly.
//	@Tags			write
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			request	body		assertFactReq	true	"Fact payload"
//	@Success		200		{object}	factResp		"Written fact (proposed or active per actor type)"
//	@Failure		400		{object}	errBody			"Invalid body or oversized predicate/value"
//	@Failure		429		{object}	errBody			"Agent write budget exhausted (Retry-After set)"
//	@Failure		502		{object}	errBody			"Storage temporarily unavailable"
//	@Router			/v1/facts [post]
func (s *Server) handleAssertFact(w http.ResponseWriter, r *http.Request) {
	var req assertFactReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.ObjectValue) > maxFactValue || len(req.Predicate) > maxFactValue {
		writeErr(w, http.StatusBadRequest, "value_too_large",
			"predicate/object_value exceed maximum length")
		return
	}
	f, err := s.svc.AssertFact(r.Context(), memory.FactInput{
		Scope:         ctxString(r, ctxScope),
		SubjectID:     req.SubjectID,
		Predicate:     req.Predicate,
		ObjectValue:   req.ObjectValue,
		ObjectID:      req.ObjectID,
		Confidence:    req.Confidence,
		SourceObs:     req.SourceObs,
		ActorType:     ctxString(r, ctxActorType),
		ActorID:       ctxString(r, ctxActorID),
		OnBehalfOf:    ctxString(r, ctxOnBehalfOf),
		ClientEventID: req.ClientEventID,
	})
	if err != nil {
		mapServiceError(w, err)
		return
	}
	factJSON(w, f)
}

func factJSON(w http.ResponseWriter, f memory.Fact) {
	writeJSON(w, factResp{
		ID: f.ID, Scope: f.Scope, SubjectID: f.SubjectID,
		Predicate: f.Predicate, ObjectValue: f.ObjectValue,
		Status: string(f.Status), Confidence: f.Confidence,
		ValidFrom: f.ValidFrom, WrittenBy: f.WrittenBy,
	})
}

// --- POST /v1/facts/{id}/promote|retract -----------------------------------

type transitionResp struct {
	Fact           factResp `json:"fact"`
	TransitionedBy string   `json:"transitioned_by"`
}

// retractReq mirrors the optional retract body for schema generation; the
// handler decodes the identical shape inline (empty/truncated bodies tolerated).
type retractReq struct {
	Reason string `json:"reason,omitempty"`
}

// PromoteFact handles POST /v1/facts/{id}/promote.
//
//	@Summary		Promote a fact
//	@Description	Transition a proposed fact to active; human-gated.
//	@Tags			write
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			id	path		string			true	"Fact ID (UUID)"
//	@Success		200	{object}	transitionResp	"Promoted fact with the current version ID"
//	@Failure		400	{object}	errBody			"Invalid fact ID"
//	@Failure		403	{object}	errBody			"Only human actors may promote (human_gated)"
//	@Failure		404	{object}	errBody			"Fact not found or ID consumed by an earlier transition"
//	@Failure		409	{object}	errBody			"Fact is not in proposed status"
//	@Failure		429	{object}	errBody			"Agent write budget exhausted (Retry-After set)"
//	@Failure		502	{object}	errBody			"Storage temporarily unavailable"
//	@Router			/v1/facts/{id}/promote [post]
func (s *Server) handlePromoteFact(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, "promote")
}

// RetractFact handles POST /v1/facts/{id}/retract.
//
//	@Summary		Retract a fact
//	@Description	Mark a fact retracted with an optional reason; human-gated. Empty request body allowed.
//	@Tags			write
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			id		path		string			true	"Fact ID (UUID)"
//	@Param			request	body		retractReq		false	"Optional reason"
//	@Success		200		{object}	transitionResp	"Retracted fact with the current version ID"
//	@Failure		400		{object}	errBody			"Invalid fact ID or malformed body"
//	@Failure		403		{object}	errBody			"Only human actors may retract (human_gated)"
//	@Failure		404		{object}	errBody			"Fact not found or ID consumed by an earlier transition"
//	@Failure		409		{object}	errBody			"Fact already retracted"
//	@Failure		429		{object}	errBody			"Agent write budget exhausted (Retry-After set)"
//	@Failure		502		{object}	errBody			"Storage temporarily unavailable"
//	@Router			/v1/facts/{id}/retract [post]
func (s *Server) handleRetractFact(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, "retract")
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request, op string) {
	id := r.PathValue("id")
	actorType := ctxString(r, ctxActorType)
	actorID := ctxString(r, ctxActorID)

	var reason string
	if op == "retract" {
		var req struct {
			Reason string `json:"reason,omitempty"`
		}
		// Retract tolerates an empty body. Chunked requests report
		// ContentLength -1 (unknown), so emptiness is judged by > 0 only;
		// a premature EOF on a length-declared body degrades to an empty
		// reason rather than a 400.
		if r.ContentLength > 0 {
			switch err := decodeJSONBody(w, r, &req); {
			case errors.Is(err, io.EOF):
				// premature EOF: treat as empty reason
			case err != nil:
				writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
				return
			}
		}
		reason = req.Reason
	}

	var (
		f   memory.Fact
		err error
	)
	switch op {
	case "promote":
		f, err = s.svc.PromoteFact(r.Context(), id, actorType, actorID)
	case "retract":
		f, err = s.svc.RetractFact(r.Context(), id, reason, actorType, actorID)
	default:
		panic("unreachable: unknown transition " + op)
	}
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, transitionResp{Fact: factRespOf(f), TransitionedBy: actorID})
}

func factRespOf(f memory.Fact) factResp {
	return factResp{
		ID: f.ID, Scope: f.Scope, SubjectID: f.SubjectID,
		Predicate: f.Predicate, ObjectValue: f.ObjectValue,
		Status: string(f.Status), Confidence: f.Confidence,
		ValidFrom: f.ValidFrom, WrittenBy: f.WrittenBy,
	}
}

// --- GET /v1/enrich?type=&key= ---------------------------------------------

type enrichResp struct {
	Found        bool           `json:"found"`
	Entity       entityJSON     `json:"entity"`
	Facts        []factViewJSON `json:"facts"`
	Observations []obsViewJSON  `json:"observations"`
	Neighbors    []neighborJSON `json:"neighbors"`
}

type entityJSON struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Key         string    `json:"key"`
	DisplayName string    `json:"display_name"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
}

type factViewJSON struct {
	ID          string    `json:"id"`
	Predicate   string    `json:"predicate"`
	ObjectValue string    `json:"object_value"`
	Status      string    `json:"status"`
	Confidence  float32   `json:"confidence"`
	ValidFrom   time.Time `json:"valid_from"`
	WrittenBy   string    `json:"written_by"`
	SourceObs   string    `json:"source_obs"`
}

type obsViewJSON struct {
	ID      string    `json:"id"`
	Ts      time.Time `json:"ts"`
	Kind    string    `json:"kind"`
	Excerpt string    `json:"excerpt"`
}

type neighborJSON struct {
	EntityID  string `json:"entity_id"`
	Relation  string `json:"relation"`
	Direction string `json:"direction"`
}

// Enrich handles GET /v1/enrich.
//
//	@Summary		Enrich an entity
//	@Description	Entity card for one key: profile, facts, observations, and graph neighbors.
//	@Tags			recall
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			type	query		string		true	"Entity type (e.g. ioc_ip, ioc_domain)"
//	@Param			key		query		string		true	"Entity lookup key"
//	@Success		200		{object}	enrichResp	"Entity card; found=false with empty lists when unknown"
//	@Failure		400		{object}	errBody		"Missing type/key or storage validation failure"
//	@Failure		502		{object}	errBody		"Storage temporarily unavailable"
//	@Router			/v1/enrich [get]
func (s *Server) handleEnrich(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := q.Get("key")
	typ := q.Get("type")
	if key == "" || typ == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "type and key are required")
		return
	}
	res, err := s.svc.Enrich(r.Context(), ctxString(r, ctxScope), key, entity.Type(typ))
	if err != nil {
		mapServiceError(w, err)
		return
	}
	out := enrichResp{Found: res.Found}
	if res.Found {
		out.Entity = entityJSON{
			ID: res.Entity.EntityID, Type: string(res.Entity.EntityType),
			Key: res.Entity.Key, DisplayName: res.Entity.DisplayName,
			FirstSeen: res.Entity.FirstSeen, LastSeen: res.Entity.LastSeen,
		}
	}
	for _, f := range res.Facts {
		out.Facts = append(out.Facts, factViewJSON{
			ID: f.ID, Predicate: f.Predicate, ObjectValue: f.ObjectValue,
			Status: string(f.Status), Confidence: f.Confidence,
			ValidFrom: f.ValidFrom, WrittenBy: f.WrittenBy, SourceObs: f.SourceObs,
		})
	}
	for _, o := range res.Observations {
		out.Observations = append(out.Observations, obsViewJSON{
			ID: o.ID, Ts: o.Ts, Kind: o.Kind, Excerpt: o.Excerpt,
		})
	}
	for _, n := range res.Neighbors {
		out.Neighbors = append(out.Neighbors, neighborJSON{
			EntityID: n.EntityID, Relation: n.Relation, Direction: n.Direction,
		})
	}
	writeJSON(w, out)
}

// --- GET /v1/similar?q=&k= -------------------------------------------------

type searchHitJSON struct {
	ObsID     string    `json:"obs_id"`
	Ts        time.Time `json:"ts"`
	Kind      string    `json:"kind"`
	Excerpt   string    `json:"excerpt"`
	Score     float64   `json:"score"`
	MatchedBy []string  `json:"matched_by"`
}

// Similar handles GET /v1/similar.
//
//	@Summary		Hybrid recall search
//	@Description	Semantic + keyword hybrid search over the scope's observations, ranked.
//	@Tags			recall
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			q	query		string			true	"Natural-language query text"
//	@Param			k	query		integer			false	"Maximum hits in [1,50] (default 10)"
//	@Success		200	{array}		searchHitJSON	"Ranked hits (possibly empty)"
//	@Failure		400	{object}	errBody			"Missing q or out-of-range/non-integer k"
//	@Failure		502	{object}	errBody			"Storage temporarily unavailable"
//	@Router			/v1/similar [get]
func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := q.Get("q")
	if strings.TrimSpace(query) == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "q is required")
		return
	}
	k := 10 // API default; Similar validates the [1,50] range itself
	if raw := q.Get("k"); raw != "" {
		parsed, ok := parseIntParam(w, raw, "k")
		if !ok {
			return
		}
		k = parsed
	}
	hits, err := s.svc.Similar(r.Context(), ctxString(r, ctxScope), query, k)
	if err != nil {
		mapServiceError(w, err)
		return
	}
	out := make([]searchHitJSON, 0, len(hits))
	for _, h := range hits {
		out = append(out, searchHitJSON{
			ObsID: h.ObsID, Ts: h.Ts, Kind: h.Kind, Excerpt: h.Excerpt,
			Score: h.Score, MatchedBy: h.MatchedBy,
		})
	}
	writeJSON(w, out)
}

// --- GET /v1/timeline?case_id=|entity_id=&limit=&offset= --------------------

type eventJSON struct {
	ID      string    `json:"id"`
	Ts      time.Time `json:"ts"`
	Source  string    `json:"source"`
	Kind    string    `json:"kind"`
	ActorID string    `json:"actor_id"`
	Text    string    `json:"text"`
}

// Timeline handles GET /v1/timeline.
//
//	@Summary		Case or entity timeline
//	@Description	Chronological events scoped to exactly one of case_id or entity_id.
//	@Tags			recall
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			case_id		query		string		false	"Case UUID (exactly one of case_id/entity_id)"
//	@Param			entity_id	query		string		false	"Entity ID (exactly one of case_id/entity_id)"
//	@Param			limit		query		integer		false	"Page size (default 50)"
//	@Param			offset		query		integer		false	"Offset into the result set (default 0)"
//	@Success		200			{array}		eventJSON	"Events, newest first"
//	@Failure		400			{object}	errBody		"Both/neither selector given or non-integer paging"
//	@Failure		502			{object}	errBody		"Storage temporarily unavailable"
//	@Router			/v1/timeline [get]
func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	caseID := q.Get("case_id")
	entityID := q.Get("entity_id")
	if (caseID == "") == (entityID == "") {
		writeErr(w, http.StatusBadRequest, "invalid_request",
			"exactly one of case_id or entity_id required")
		return
	}
	limit := 50 // API default
	if raw := q.Get("limit"); raw != "" {
		parsed, ok := parseIntParam(w, raw, "limit")
		if !ok {
			return
		}
		limit = parsed
	}
	offset := 0
	if raw := q.Get("offset"); raw != "" {
		parsed, ok := parseIntParam(w, raw, "offset")
		if !ok {
			return
		}
		offset = parsed
	}
	events, err := s.svc.Timeline(r.Context(), ctxString(r, ctxScope), caseID, entityID, limit, offset)
	if err != nil {
		mapServiceError(w, err)
		return
	}
	out := make([]eventJSON, 0, len(events))
	for _, e := range events {
		out = append(out, eventJSON{
			ID: e.ID, Ts: e.Ts, Source: e.Source, Kind: e.Kind,
			ActorID: e.ActorID, Text: e.Text,
		})
	}
	writeJSON(w, out)
}

// --- GET /v1/traverse?key=&type=&relation=&hops= ----------------------------

// pathJSON is one walked route: nodes[i+1] hangs off nodes[i] via
// relations[i]. The start entity is always nodes[0].
type pathJSON struct {
	Nodes     []entityJSON `json:"nodes"`
	Relations []string     `json:"relations"`
}

type traverseResp struct {
	Paths []pathJSON `json:"paths"`
}

// Traverse handles GET /v1/traverse.
//
//	@Summary		Graph traversal
//	@Description	Walk relations outward from an entity up to hops (1-3); degrades to at most 1 hop without Dgraph.
//	@Tags			recall
//	@Accept			json
//	@Produce		json
//	@Security		IdentityHeaders
//	@Param			key			query		string			true	"Start entity lookup key"
//	@Param			type		query		string			true	"Start entity type (e.g. ioc_domain)"
//	@Param			relation	query		string			false	"Restrict edges to one relation type"
//	@Param			hops		query		integer			false	"Hops in [1,3] (default 1)"
//	@Success		200			{object}	traverseResp	"Walked paths from the start entity"
//	@Failure		400			{object}	errBody			"Missing type/key, bad hops, or malformed relation"
//	@Failure		502			{object}	errBody			"Storage temporarily unavailable"
//	@Router			/v1/traverse [get]
func (s *Server) handleTraverse(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := q.Get("key")
	typ := q.Get("type")
	if key == "" || typ == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "type and key are required")
		return
	}
	hops := 1 // API default; Traverse validates the [1,3] range itself
	if raw := q.Get("hops"); raw != "" {
		parsed, ok := parseIntParam(w, raw, "hops")
		if !ok {
			return
		}
		hops = parsed
	}
	paths, err := s.svc.Traverse(r.Context(), ctxString(r, ctxScope), key,
		entity.Type(typ), q.Get("relation"), hops)
	if err != nil {
		mapServiceError(w, err) // ErrInvalidInput (bad hops/relation) → 400 here
		return
	}
	out := traverseResp{Paths: make([]pathJSON, 0, len(paths))}
	for _, p := range paths {
		pj := pathJSON{
			Nodes:     make([]entityJSON, 0, len(p.Nodes)),
			Relations: p.Relations,
		}
		for _, e := range p.Nodes {
			pj.Nodes = append(pj.Nodes, entityJSON{
				ID: e.EntityID, Type: string(e.EntityType),
				Key: e.Key, DisplayName: e.DisplayName,
				FirstSeen: e.FirstSeen, LastSeen: e.LastSeen,
			})
		}
		out.Paths = append(out.Paths, pj)
	}
	writeJSON(w, out)
}

// --- GET /healthz -----------------------------------------------------------

// healthResp mirrors the /healthz body shape for schema generation; the
// handler writes the same JSON via a string map.
type healthResp struct {
	Status string `json:"status"`
}

// Healthz handles GET /healthz.
//
//	@Summary		Liveness probe
//	@Description	Reports service health; pings ClickHouse with a short deadline.
//	@Tags			system
//	@Accept			json
//	@Produce		json
//	@Success		200	{object}	healthResp	"Service healthy"
//	@Failure		503	{object}	errBody		"Database unreachable"
//	@Router			/healthz [get]
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.conn.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "db_unavailable", err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}
