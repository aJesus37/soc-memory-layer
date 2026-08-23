package memory

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTimelineByEntity interleaves three observations (recorded out of
// chronological order, at controlled ts) with one asserted fact and checks
// the union: 4 events, strictly newest-first, the fact rendered with
// Kind="fact:<predicate>" at its valid_from, observations carrying their
// excerpts. Also pins that RecordObservation honors Input.Ts — if it forced
// now(), the strict ordering below would collapse and fail.
func TestTimelineByEntity(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "timeline.target.example.com")

	type seed struct {
		text string
		age  time.Duration
	}
	// Insertion order deliberately NOT chronological (3h, 1h, 2h): the
	// timeline must order by ts, never by write order.
	seeds := []seed{
		{"oldest beacon note", 3 * time.Hour},
		{"newest beacon note", 1 * time.Hour},
		{"middle beacon note", 2 * time.Hour},
	}
	seededAt := time.Now().UTC()
	for _, sd := range seeds {
		if _, err := s.RecordObservation(ctx, Input{
			Scope:     scope,
			Kind:      "alert",
			ActorType: "agent",
			ActorID:   "sensor-7",
			Ts:        seededAt.Add(-sd.age),
			Content:   sd.text + " involving timeline.target.example.com",
		}); err != nil {
			t.Fatal(err)
		}
	}

	f, err := s.AssertFact(ctx, FactInput{
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

	events, err := s.Timeline(ctx, scope, "", subj.EntityID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("got %d events (%+v), want 4", len(events), events)
	}

	// Strictly newest-first: every event strictly newer than its successor.
	for i := 1; i < len(events); i++ {
		if !events[i-1].Ts.After(events[i].Ts) {
			t.Fatalf("not strictly newest-first at %d: %+v", i, events)
		}
	}

	// The fact (valid_from ~now) leads the timeline.
	fe := events[0]
	if fe.Source != "fact" || fe.Kind != "fact:verdict_malicious" || fe.ActorID != "analyst-j" {
		t.Fatalf("fact event wrong: %+v", fe)
	}
	if !strings.Contains(fe.Text, "verdict_malicious") || !strings.Contains(fe.Text, "c2") {
		t.Fatalf("fact Text %q must contain predicate and object value", fe.Text)
	}
	if d := fe.Ts.Sub(f.ValidFrom); d < -5*time.Second || d > 5*time.Second {
		t.Errorf("fact Ts %v, want ~valid_from %v", fe.Ts, f.ValidFrom)
	}

	// Then the observations newest-first, excerpts verbatim.
	want := []seed{
		{"newest beacon note", 1 * time.Hour},
		{"middle beacon note", 2 * time.Hour},
		{"oldest beacon note", 3 * time.Hour},
	}
	for i, w := range want {
		e := events[i+1]
		if e.Source != "observation" {
			t.Errorf("events[%d].Source = %q, want observation", i+1, e.Source)
		}
		if e.Kind != "alert" {
			t.Errorf("events[%d].Kind = %q, want alert", i+1, e.Kind)
		}
		if e.ActorID != "sensor-7" {
			t.Errorf("events[%d].ActorID = %q, want sensor-7", i+1, e.ActorID)
		}
		if excerpt := w.text + " involving timeline.target.example.com"; e.Text != excerpt {
			t.Errorf("events[%d].Text = %q, want %q", i+1, e.Text, excerpt)
		}
		wantTs := seededAt.Add(-w.age)
		if d := e.Ts.Sub(wantTs); d < -5*time.Second || d > 5*time.Second {
			t.Errorf("events[%d].Ts = %v, want ~%v (honored Input.Ts)", i+1, e.Ts, wantTs)
		}
	}
}

// TestTimelineByCase exercises the bridge: facts belong to ENTITIES, the
// case view reaches them by collecting entity refs from all case
// observations first. A case with linked entities yields observations plus
// the bridged fact; a case whose observations link nothing yields only
// observations; an unknown case yields an empty slice with nil error.
func TestTimelineByCase(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	caseID := uuid.NewString()
	seededAt := time.Now().UTC()
	o1, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "triage_decision",
		ActorType: "human",
		ActorID:   "analyst-j",
		CaseID:    caseID,
		Ts:        seededAt.Add(-40 * time.Minute),
		Content:   "escalating 10.9.9.1 into the case",
	})
	if err != nil {
		t.Fatal(err)
	}
	o2, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "investigation_note",
		ActorType: "human",
		ActorID:   "analyst-k",
		CaseID:    caseID,
		Ts:        seededAt.Add(-20 * time.Minute),
		Content:   "pivot to bridge.timeline.example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(o1.EntityIDs) == 0 || len(o2.EntityIDs) == 0 {
		t.Fatalf("seed observations must link entities: %v / %v", o1.EntityIDs, o2.EntityIDs)
	}
	if _, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   o1.EntityIDs[0],
		Predicate:   "verdict_malicious",
		ObjectValue: "c2-beacon",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	}); err != nil {
		t.Fatal(err)
	}

	events, err := s.Timeline(ctx, scope, caseID, "", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events (%+v), want 2 observations + 1 bridged fact", len(events), events)
	}
	var nObs, nFact int
	for _, e := range events {
		switch e.Source {
		case "observation":
			nObs++
			if e.Kind != "triage_decision" && e.Kind != "investigation_note" {
				t.Errorf("observation kind = %q, unexpected", e.Kind)
			}
		case "fact":
			nFact++
			if e.Kind != "fact:verdict_malicious" || !strings.Contains(e.Text, "c2-beacon") {
				t.Errorf("bridged fact event wrong: %+v", e)
			}
			if e.ActorID != "analyst-j" {
				t.Errorf("bridged fact ActorID = %q, want analyst-j", e.ActorID)
			}
		default:
			t.Errorf("event Source = %q, want observation|fact", e.Source)
		}
	}
	if nObs != 2 || nFact != 1 {
		t.Fatalf("sources = %d observations / %d facts, want 2 / 1", nObs, nFact)
	}

	// Newest-first across the mixed union.
	for i := 1; i < len(events); i++ {
		if !events[i-1].Ts.After(events[i].Ts) {
			t.Fatalf("not strictly newest-first at %d: %+v", i, events)
		}
	}

	// A case whose observations link NO entities: observations only, and
	// the facts leg must be skipped entirely (empty subject set).
	caseNoEnts := uuid.NewString()
	if _, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "hunt_finding",
		ActorType: "agent",
		ActorID:   "hunter-1",
		CaseID:    caseNoEnts,
		Content:   "plain prose without any indicators whatsoever here",
	}); err != nil {
		t.Fatal(err)
	}
	entsOnly, err := s.Timeline(ctx, scope, caseNoEnts, "", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entsOnly) != 1 || entsOnly[0].Source != "observation" {
		t.Fatalf("entity-less case events = %+v, want exactly 1 observation", entsOnly)
	}

	// Zero rows: empty slice, nil error.
	empty, err := s.Timeline(ctx, scope, uuid.NewString(), "", 50, 0)
	if err != nil {
		t.Fatalf("empty timeline must not error: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("unknown case events = %+v, want empty", empty)
	}

	// Scope isolation: the same case id viewed from another scope is empty.
	iso, err := s.Timeline(ctx, itestScope(), caseID, "", 50, 0)
	if err != nil {
		t.Fatalf("foreign-scope lookup must not error: %v", err)
	}
	if len(iso) != 0 {
		t.Fatalf("scope isolation broken: %+v", iso)
	}
}

// TestTimelinePagination walks five observations in limit=2 pages: exact
// continuation, no overlap, partial final page. Ordering must match the
// unpaginated reference exactly.
func TestTimelinePagination(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "pagination.target.example.com")

	seededAt := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		if _, err := s.RecordObservation(ctx, Input{
			Scope:     scope,
			Kind:      "alert",
			ActorType: "agent",
			ActorID:   "sensor-7",
			Ts:        seededAt.Add(-time.Duration(i) * time.Hour),
			Content:   fmt.Sprintf("pagination obs %d on pagination.target.example.com", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	ref, err := s.Timeline(ctx, scope, "", subj.EntityID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ref) != 5 {
		t.Fatalf("reference page = %d events, want 5", len(ref))
	}

	offset := 0
	var walked []string
	for {
		page, err := s.Timeline(ctx, scope, "", subj.EntityID, 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > 2 {
			t.Fatalf("limit=2 ignored: %+v", page)
		}
		for i, e := range page {
			if e.Text != ref[offset+i].Text {
				t.Fatalf("page(offset=%d)[%d].Text = %q, want %q",
					offset, i, e.Text, ref[offset+i].Text)
			}
			walked = append(walked, e.Text)
		}
		if len(page) < 2 {
			break
		}
		offset += 2
	}
	if len(walked) != 5 {
		t.Fatalf("walked %d events across pages, want 5 (final page must be partial)", len(walked))
	}
	seen := map[string]bool{}
	for i, txt := range walked {
		if seen[txt] {
			t.Errorf("overlap across pages: %q repeats at %d", txt, i)
		}
		seen[txt] = true
	}
	for i, txt := range walked {
		if txt != ref[i].Text {
			t.Errorf("walked[%d] = %q, want %q (pages must tile the reference)", i, txt, ref[i].Text)
		}
	}

	// Offset past the end: empty slice, nil error.
	tail, err := s.Timeline(ctx, scope, "", subj.EntityID, 2, 5)
	if err != nil {
		t.Fatalf("offset past end must not error: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("offset past end events = %+v, want empty", tail)
	}
}

// TestTimelineValidation pins the argument contract: exactly one of
// case/entity, parseable UUIDs when present, scope required, limit in
// [1,500], offset >= 0. Boundary limits (1 and 500) must be ACCEPTED.
func TestTimelineValidation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	goodEntity := mustResolveEntity(t, conn, scope, "validation.target.example.com")
	goodCase := uuid.NewString()

	bad := []struct {
		name  string
		scope string
		caseI string
		entI  string
		limit int
		off   int
	}{
		{"both ids set", scope, goodCase, goodEntity.EntityID, 50, 0},
		{"neither id set", scope, "", "", 50, 0},
		{"blank ids set", scope, "   ", "   ", 50, 0},
		{"bad entity uuid", scope, "", "not-a-uuid", 50, 0},
		{"bad case uuid", scope, "not-a-uuid", "", 50, 0},
		{"limit zero", scope, "", goodEntity.EntityID, 0, 0},
		{"limit negative", scope, "", goodEntity.EntityID, -3, 0},
		{"limit over max", scope, "", goodEntity.EntityID, 501, 0},
		{"negative offset", scope, "", goodEntity.EntityID, 50, -1},
		{"empty scope", "", "", goodEntity.EntityID, 50, 0},
		{"blank scope", "   ", "", goodEntity.EntityID, 50, 0},
	}
	for _, c := range bad {
		if _, err := s.Timeline(ctx, c.scope, c.caseI, c.entI, c.limit, c.off); err == nil {
			t.Errorf("%s: expected validation error, got nil", c.name)
		}
	}

	// Boundary limits accepted (fresh scope: no rows, but no error either).
	for _, lim := range []int{1, 500} {
		if _, err := s.Timeline(ctx, scope, "", goodEntity.EntityID, lim, 0); err != nil {
			t.Errorf("limit %d must be accepted, got: %v", lim, err)
		}
	}
	if _, err := s.Timeline(ctx, scope, goodCase, "", 50, 0); err != nil {
		t.Errorf("valid by-case arguments must be accepted, got: %v", err)
	}
}

func TestTimelineExcludesRetractedFacts(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "retracted.timeline.example.com")

	if _, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	}); err != nil {
		t.Fatal(err)
	}

	events, err := s.Timeline(ctx, scope, "", subj.EntityID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Source != "fact" {
		t.Fatalf("precondition: want exactly the fact event, got %+v", events)
	}

	if _, err := s.RetractFact(ctx, events[0].ID, "false positive", "human", "analyst-j"); err != nil {
		t.Fatal(err)
	}

	events, err = s.Timeline(ctx, scope, "", subj.EntityID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.Source == "fact" {
			t.Errorf("retracted fact still on timeline: %+v", e)
		}
	}
	if len(events) != 0 {
		t.Fatalf("timeline should be empty after retracting the only event, got %+v", events)
	}
}

func TestTimelineShowsSupersededPriors(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "superseded.timeline.example.com")

	f1, err := s.AssertFact(ctx, FactInput{
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
	time.Sleep(1100 * time.Millisecond) // distinct valid_from seconds + updated_at ms
	if _, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "benign-parked",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-k",
	}); err != nil {
		t.Fatal(err)
	}
	_ = f1

	events, err := s.Timeline(ctx, scope, "", subj.EntityID, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range events {
		if e.Source == "fact" {
			got = append(got, e.Text)
		}
	}
	// Timeline is newest-first: f2 (benign-parked) was asserted after f1 (c2),
	// and both versions appear at their own valid_from positions.
	want := []string{"verdict_malicious: benign-parked", "verdict_malicious: c2"}
	if len(got) != len(want) {
		t.Fatalf("want both fact versions on timeline (assertion history), got %v", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("event %d: got %q want %q", i, got[i], w)
		}
	}
}
