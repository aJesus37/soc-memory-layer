package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/extract"
)

// scriptRule maps the first observation content containing match (a plain
// substring) to its canned Propose outcome. Contents matching no rule
// yield zero proposals and no error — a model that found nothing.
type scriptRule struct {
	match     string
	proposals []extract.Proposal
	err       error
}

// scriptedFakeChat is to extract.ChatClient what failingEmbedder is to
// embed.Embedder: a deterministic in-process stand-in. It emulates the
// real client contract — garbage model output reaches the worker as ZERO
// proposals, never as an error, because Sanitize drops bad items silently;
// errors here mean transport/model failure and must abort the run.
// Safe for concurrent use so the loop tests can poll it from outside.
type scriptedFakeChat struct {
	mu    sync.Mutex
	rules []scriptRule
	calls int
	seen  []string // contents passed to Propose, in call order
}

func (f *scriptedFakeChat) Propose(_ context.Context, content string) ([]extract.Proposal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.seen = append(f.seen, content)
	for _, r := range f.rules {
		if strings.Contains(content, r.match) {
			if r.err != nil {
				return nil, r.err
			}
			return r.proposals, nil
		}
	}
	return []extract.Proposal{}, nil
}

func (f *scriptedFakeChat) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *scriptedFakeChat) seenContents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// panickingChat violates the ChatClient "never panics" contract on every
// call to prove the loop's per-tick panic recovery: without it, one tick
// kills the whole test process.
type panickingChat struct{ calls atomic.Int64 }

func (p *panickingChat) Propose(context.Context, string) ([]extract.Proposal, error) {
	p.calls.Add(1)
	panic("chat exploded")
}

// drainExtractionBacklog marks every pre-existing observation covered so
// scenario tests control exactly which rows the worker's GLOBAL ts-ordered
// anti-join sees: prior suite runs predate extract_log and would otherwise
// crowd their older-timestamped rows into every selection. Direct SQL
// backfill rather than worker passes — this is test setup, not behavior
// under test, and per-row inserts pay async-flush latency by the thousand.
func drainExtractionBacklog(t *testing.T, conn driver.Conn) {
	t.Helper()
	if err := conn.Exec(context.Background(),
		"INSERT INTO mem.extract_log (obs_id) "+
			"SELECT o.obs_id FROM mem.observations o "+
			"WHERE NOT EXISTS ("+
			"SELECT 1 FROM mem.extract_log x WHERE x.obs_id = o.obs_id)"); err != nil {
		t.Fatalf("drain backlog: %v", err)
	}
}

// extractionCovered reports whether mem.extract_log carries a row for obsID.
func extractionCovered(t *testing.T, conn driver.Conn, ctx context.Context, obsID string) bool {
	t.Helper()
	u, err := uuid.Parse(obsID)
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.extract_log WHERE obs_id = ?", u,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// uncoveredInScope counts the scope's observations still lacking extraction
// coverage — the precise per-scope form of the worker's global anti-join.
func uncoveredInScope(t *testing.T, conn driver.Conn, ctx context.Context, scope string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.observations o "+
			"WHERE o.scope = ? AND NOT EXISTS ("+
			"SELECT 1 FROM mem.extract_log x WHERE x.obs_id = o.obs_id)", scope,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRunExtractionOnce(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	// The shared instance may hold uncovered observations from earlier runs
	// with older timestamps; clear them so the scenario below controls
	// exactly which rows the global select sees.
	drainExtractionBacklog(t, conn)

	const (
		content1 = "Observed host 9.9.9.9 beaconing toward c2.evil.example.net every sixty seconds"
		content2 = "free-form triage scribbles with nothing machine-readable in here"
		content3 = "this note reliably crashes the extraction model"
		content4 = "follow-up pivot: fallback.evil.example.net resolved from 8.8.4.4"
	)

	// Distinct second-granularity timestamps pin the ts ASC processing
	// order; equal timestamps would leave the tie-break arbitrary.
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	rec := func(content string, offset time.Duration) Observation {
		o, err := s.RecordObservation(ctx, Input{
			Scope:     scope,
			Kind:      "investigation_note",
			ActorType: "human",
			ActorID:   "analyst-j",
			Ts:        base.Add(offset),
			Content:   content,
		})
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	obs1 := rec(content1, 0)             // → one valid proposal
	obs2 := rec(content2, time.Second)   // → garbage item sanitizes to zero proposals
	obs3 := rec(content3, 2*time.Second) // → LLM error mid-run
	obs4 := rec(content4, 3*time.Second) // → left for the retry run

	boom := errors.New("chat provider down")
	fake := &scriptedFakeChat{rules: []scriptRule{
		{
			match: "9.9.9.9",
			proposals: []extract.Proposal{{
				Subject:     "c2.evil.example.net",
				Predicate:   "beaconed_to", // non-whitelisted → stays proposed
				ObjectValue: "9.9.9.9",
				Confidence:  0.7,
			}},
		},
		{match: "nothing machine-readable", proposals: []extract.Proposal{}},
		{match: "crashes the extraction model", err: boom},
	}}

	// Run 1: batch of 3 processes obs1..3 in order and aborts on obs3.
	n1, err := s.RunExtractionOnce(ctx, fake, 3)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the chat error", err)
	}
	if n1 != 1 {
		t.Fatalf("asserted = %d, want 1 (obs2 yields none, obs3 errored)", n1)
	}
	if got := fake.seenContents(); !reflect.DeepEqual(got, []string{content1, content2, content3}) {
		t.Fatalf("fake saw %q, want exactly obs1..obs3 contents in ts order", got)
	}

	// Coverage: ALL attempted ids logged, including the errored obs3 —
	// but not the never-attempted obs4.
	for _, o := range []Observation{obs1, obs2, obs3} {
		if !extractionCovered(t, conn, ctx, o.ID) {
			t.Errorf("extract_log missing attempted obs %s", o.ID)
		}
	}
	if extractionCovered(t, conn, ctx, obs4.ID) {
		t.Error("obs4 was aborted before attempt but appears covered")
	}

	// The proposal landed as a PROPOSED fact linked to its observation,
	// stamped extractor-v1, visible on the authoritative FINAL read path.
	subj := mustResolveEntity(t, conn, scope, "c2.evil.example.net") // created by the worker's resolution
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	var (
		factID    string
		srcObs    uuid.UUID
		writtenBy string
		status    string
	)
	if err := conn.QueryRow(ctx,
		"SELECT fact_id, source_obs, written_by, status FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND valid_to > now64(3)",
		scope, subjUUID, "beaconed_to",
	).Scan(&factID, &srcObs, &writtenBy, &status); err != nil {
		t.Fatal(err)
	}
	if Status(status) != Proposed {
		t.Errorf("extraction fact status = %q, want proposed", status)
	}
	if srcObs.String() != obs1.ID {
		t.Errorf("source_obs = %s, want %s", srcObs, obs1.ID)
	}
	if writtenBy != "extractor-v1" {
		t.Errorf("written_by = %q, want extractor-v1", writtenBy)
	}
	if got := countOpenFacts(t, ctx, conn, scope, subjUUID, "beaconed_to"); got != 0 {
		t.Errorf("open ACTIVE facts for key = %d, want 0 (proposal is not active)", got)
	}

	// Audit trail: agent identity recorded, summary provably content-free.
	factUUID, _ := uuid.Parse(factID)
	var aOp, aType, aActor, aSummary string
	if err := conn.QueryRow(ctx,
		"SELECT operation, actor_type, actor_id, payload_summary "+
			"FROM mem.audit WHERE target_id = ?", factUUID,
	).Scan(&aOp, &aType, &aActor, &aSummary); err != nil {
		t.Fatal(err)
	}
	if aOp != "assert_fact" || aType != "agent" || aActor != "extractor-v1" {
		t.Errorf("audit shape wrong: op=%q actor_type=%q actor_id=%q", aOp, aType, aActor)
	}
	for _, secret := range []string{"9.9.9.9", "c2.evil.example.net", "beaconed_to"} {
		if strings.Contains(aSummary, secret) {
			t.Errorf("audit payload_summary leaks %q: %q", secret, aSummary)
		}
	}

	// Run 2 (retry next tick): the anti-join skips everything covered and
	// picks up exactly the leftover obs4.
	n2, err := s.RunExtractionOnce(ctx, fake, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("run 2 asserted %d proposals, want 0", n2)
	}
	if got := fake.seenContents()[3:]; !reflect.DeepEqual(got, []string{content4}) {
		t.Fatalf("run 2 processed %q, want exactly [obs4]", got)
	}

	// Run 3: nothing uncovered remains for this run to touch.
	n3, err := s.RunExtractionOnce(ctx, fake, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n3 != 0 {
		t.Fatalf("run 3 asserted %d proposals, want 0", n3)
	}
	if left := uncoveredInScope(t, conn, ctx, scope); left != 0 {
		t.Fatalf("%d observations of the scenario scope remain uncovered after run 3", left)
	}

	// Humans see the proposal through the FINAL open-fact read and promote
	// it via the normal path; promotion keeps provenance.
	promoted, err := s.PromoteFact(ctx, factID, "human", "analyst-j")
	if err != nil {
		t.Fatalf("promote proposal: %v", err)
	}
	if promoted.Status != Active {
		t.Fatalf("promoted status = %s, want active", promoted.Status)
	}
	var (
		pStatus    string
		pSourceObs uuid.UUID
		pWrittenBy string
	)
	if err := conn.QueryRow(ctx,
		"SELECT status, source_obs, written_by FROM mem.facts FINAL "+
			"WHERE fact_id = ?", promoted.ID,
	).Scan(&pStatus, &pSourceObs, &pWrittenBy); err != nil {
		t.Fatal(err)
	}
	if Status(pStatus) != Active || pWrittenBy != "analyst-j" || pSourceObs.String() != obs1.ID {
		t.Errorf("promoted row wrong: status=%q written_by=%q source_obs=%s",
			pStatus, pWrittenBy, pSourceObs)
	}
	if got := countOpenFacts(t, ctx, conn, scope, subjUUID, "beaconed_to"); got != 1 {
		t.Errorf("open ACTIVE facts after promote = %d, want 1", got)
	}
}

func TestExtractionDedupesActiveFacts(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	drainExtractionBacklog(t, conn)

	const key = "dedupe.example.com"
	subj := mustResolveEntity(t, conn, scope, key)

	// A human already asserted the business key as open-active.
	human, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "investigation_note",
		ActorType: "human",
		ActorID:   "analyst-j",
		Content:   "analyst note claiming " + key + " looks like benign parked infrastructure",
	})
	if err != nil {
		t.Fatal(err)
	}

	fake := &scriptedFakeChat{rules: []scriptRule{{
		match: key,
		proposals: []extract.Proposal{{
			Subject:     key, // same business key as the human's fact…
			Predicate:   "verdict_malicious",
			ObjectValue: "benign-parked", // …different value
			Confidence:  0.99,
		}},
	}}}

	n, err := s.RunExtractionOnce(ctx, fake, 5)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("asserted = %d, want 1", n)
	}

	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	// The agent proposal must NOT have superseded the human assertion:
	// proposals never close prior actives (Phase-1 supersede semantics).
	if got := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); got != 1 {
		t.Fatalf("open ACTIVE facts for the key = %d, want 1 (the human's)", got)
	}
	var vt time.Time
	if err := conn.QueryRow(ctx,
		"SELECT valid_to FROM mem.facts WHERE fact_id = ?", human.ID,
	).Scan(&vt); err != nil {
		t.Fatal(err)
	}
	if year := vt.Year(); year <= 2100 {
		t.Fatalf("proposal closed the human's active fact, valid_to=%v", vt)
	}

	// The proposal exists alongside it as PROPOSED, with provenance.
	var (
		val       string
		srcObs    uuid.UUID
		writtenBy string
	)
	if err := conn.QueryRow(ctx,
		"SELECT object_value, source_obs, written_by FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? AND status = 'proposed' "+
			"AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious",
	).Scan(&val, &srcObs, &writtenBy); err != nil {
		t.Fatal(err)
	}
	if val != "benign-parked" || srcObs.String() != o.ID || writtenBy != "extractor-v1" {
		t.Errorf("proposal row wrong: object_value=%q source_obs=%s written_by=%q",
			val, srcObs, writtenBy)
	}

	if !extractionCovered(t, conn, ctx, o.ID) {
		t.Error("observation not covered despite successful run")
	}
}

// shutdownRacingChat simulates a propose racing shutdown: the run context
// is canceled WHILE Propose is in flight — exactly what a SIGTERM handler
// does — and the transport surfaces it as an error wrapping
// context.Canceled, like net/http does for a request whose ctx died.
type shutdownRacingChat struct {
	cancel context.CancelFunc
	calls  atomic.Int64
}

func (c *shutdownRacingChat) Propose(context.Context, string) ([]extract.Proposal, error) {
	c.calls.Add(1)
	c.cancel() // the shutdown signal lands mid-flight
	return nil, fmt.Errorf("extract: POST http://model/v1/chat/completions: %w", context.Canceled)
}

// Regression: a cancellation mid-Propose must NOT permanently cover the
// observation — the coverage write is skipped so a shutdown race retries the
// observation on the next startup instead of burying it forever.
func TestExtractionCancellationLeavesObservationUncovered(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	drainExtractionBacklog(t, conn)

	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "investigation_note",
		ActorType: "human",
		ActorID:   "analyst-j",
		Content:   "shutdown raced this propose: evil.example.net resolved_to 198.51.100.23",
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc := &shutdownRacingChat{cancel: cancel}
	n, err := s.RunExtractionOnce(runCtx, cc, 5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
	if n != 0 {
		t.Errorf("asserted = %d, want 0", n)
	}
	if cc.calls.Load() != 1 {
		t.Errorf("propose called %d times, want 1", cc.calls.Load())
	}
	if extractionCovered(t, conn, ctx, o.ID) {
		t.Error("canceled propose covered the observation; a shutdown race would bury it forever")
	}
}

// timeoutChat emulates an http.Client whose Timeout fired on a slow model:
// the transport error unwraps to context.DeadlineExceeded even though the
// RUN context is perfectly alive. Classifying this as shutdown (the old
// behavior) left the observation uncovered on every tick — permanently.
type timeoutChat struct{ calls atomic.Int64 }

func (c *timeoutChat) Propose(context.Context, string) ([]extract.Proposal, error) {
	c.calls.Add(1)
	return nil, fmt.Errorf("extract: POST http://model/v1/chat/completions: %w", context.DeadlineExceeded)
}

// Regression: a client-side timeout with a live run context is an LLM
// failure, not a shutdown race — the observation MUST be covered before the
// run aborts so the queue keeps making progress across ticks.
func TestExtractionClientTimeoutCoversObservation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	drainExtractionBacklog(t, conn)

	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "investigation_note",
		ActorType: "human",
		ActorID:   "analyst-j",
		Content:   "the model times out on this one: slow.example.net resolved_to 198.51.100.24",
	})
	if err != nil {
		t.Fatal(err)
	}

	tc := &timeoutChat{}
	n, err := s.RunExtractionOnce(ctx, tc, 5)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if n != 0 {
		t.Errorf("asserted = %d, want 0", n)
	}
	if tc.calls.Load() != 1 {
		t.Errorf("propose called %d times, want 1", tc.calls.Load())
	}
	if !extractionCovered(t, conn, ctx, o.ID) {
		t.Error("client-timeout observation left uncovered; a slow model would wedge the queue head forever")
	}
}

func TestExtractionLoopCancels(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)

	t.Run("cancel stops the loop", func(t *testing.T) {
		scope := itestScope()
		drainExtractionBacklog(t, conn)
		base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
		for i := range 3 { // batch=1 below: exactly one Propose call per tick
			if _, err := s.RecordObservation(ctx, Input{
				Scope: scope, Kind: "investigation_note",
				ActorType: "human", ActorID: "analyst-j",
				Ts:      base.Add(time.Duration(i) * time.Second),
				Content: "loop fodder without indicators part " + string(rune('a'+i)),
			}); err != nil {
				t.Fatal(err)
			}
		}

		loopCtx, cancel := context.WithCancel(ctx)
		fake := &scriptedFakeChat{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.RunExtractionLoop(loopCtx, fake, 20*time.Millisecond, 1)
		}()

		waitForCalls(t, fake.callCount, 2, 3*time.Second, "loop stopped ticking early")
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after ctx cancel")
		}
	})

	t.Run("panic recovery keeps the loop alive", func(t *testing.T) {
		scope := itestScope()
		drainExtractionBacklog(t, conn)
		if _, err := s.RecordObservation(ctx, Input{
			Scope: scope, Kind: "investigation_note",
			ActorType: "human", ActorID: "analyst-j",
			Content: "the chat client will explode over this one",
		}); err != nil {
			t.Fatal(err)
		}

		loopCtx, cancel := context.WithCancel(ctx)
		pc := &panickingChat{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.RunExtractionLoop(loopCtx, pc, 15*time.Millisecond, 10)
		}()

		// Reaching three calls proves at least two panics were recovered:
		// an unrecovered one would have killed the whole process.
		waitForCalls(t, func() int { return int(pc.calls.Load()) }, 3, 3*time.Second,
			"loop died or stalled on panicking ticks")
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after cancel")
		}
	})

	t.Run("non-positive interval refuses to spin", func(t *testing.T) {
		fake := &scriptedFakeChat{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.RunExtractionLoop(ctx, fake, 0, 10)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop with invalid interval did not exit")
		}
		if fake.callCount() != 0 {
			t.Errorf("invalid-interval loop called Propose %d times, want 0", fake.callCount())
		}
	})

	t.Run("non-positive batch refuses to spin", func(t *testing.T) {
		fake := &scriptedFakeChat{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.RunExtractionLoop(ctx, fake, 20*time.Millisecond, 0)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop with invalid batch did not exit")
		}
		if fake.callCount() != 0 {
			t.Errorf("invalid-batch loop called Propose %d times, want 0", fake.callCount())
		}
	})
}

// waitForCalls polls until count() >= want or the deadline expires.
func waitForCalls(t *testing.T, count func() int, want int, within time.Duration, failMsg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if count() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s (calls=%d, want >= %d within %s)", failMsg, count(), want, within)
}
