// Package api exposes the SOC memory service over HTTP. Identity arrives
// via X-Actor-* headers (OIDC integration is future work); the middleware
// injects it into the request context so handlers never trust body-borne
// actor/scope fields.
package api

import (
	"errors"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/config"
	"socmem/internal/memory"
)

// maxObservationContent bounds observation text at the HTTP edge; larger
// payloads are rejected with 400 before reaching the embedder or storage.
const maxObservationContent = 10_000

// maxFactValue bounds fact object values / predicates at the HTTP edge.
const maxFactValue = 2_000

type ctxKey int

const (
	ctxActorType ctxKey = iota
	ctxActorID
	ctxOnBehalfOf
	ctxScope
)

// Server wires a memory.Service to HTTP transport concerns.
type Server struct {
	svc     *memory.Service
	conn    driver.Conn
	cfg     config.Config
	limiter *agentLimiter
}

func New(svc *memory.Service, conn driver.Conn, cfg config.Config) *Server {
	return &Server{
		svc:     svc,
		conn:    conn,
		cfg:     cfg,
		limiter: newAgentLimiter(cfg.AgentRateRPS),
	}
}

// Routes builds the handler tree. Go 1.22 method+path patterns; no router dep.
// Write endpoints pass through writeLimit (per-agent budget; humans exempt).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.Handle("POST /v1/observations", s.identity(s.writeLimit(s.handleCreateObservation)))
	mux.Handle("POST /v1/facts", s.identity(s.writeLimit(s.handleAssertFact)))
	mux.Handle("POST /v1/facts/{id}/promote", s.identity(s.writeLimit(s.handlePromoteFact)))
	mux.Handle("POST /v1/facts/{id}/retract", s.identity(s.writeLimit(s.handleRetractFact)))
	mux.Handle("GET /v1/enrich", s.identity(s.handleEnrich))
	mux.Handle("GET /v1/similar", s.identity(s.handleSimilar))
	mux.Handle("GET /v1/timeline", s.identity(s.handleTimeline))
	return mux
}

// --- error envelope -------------------------------------------------------

type errBody struct {
	Error errInner `json:"error"`
}

type errInner struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeJSON(w, errBody{errInner{Code: code, Message: msg}})
}

// mapServiceError translates service-layer sentinels into HTTP semantics.
// Anything unrecognized is 400: the service validates its own inputs, so a
// non-sentinel error is by definition a bad request from this API's side.
func mapServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, memory.ErrHumanGated):
		writeErr(w, http.StatusForbidden, "human_gated",
			"only human actors may perform this transition")
	case errors.Is(err, memory.ErrFactNotFound):
		writeErr(w, http.StatusNotFound, "fact_not_found", err.Error())
	case errors.Is(err, memory.ErrConflict):
		writeErr(w, http.StatusConflict, "conflict", err.Error())
	default:
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
	}
}
