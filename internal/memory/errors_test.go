package memory

import (
	"context"
	"errors"
	"testing"
)

// mustErrOf runs fn and returns its error, failing t when nil.
func mustErrOf(t *testing.T, name string, fn func() error) error {
	t.Helper()
	err := fn()
	if err == nil {
		t.Fatalf("%s: expected an error, got nil", name)
	}
	return err
}

// TestValidationErrorsWrapErrInvalidInput pins the typed-classification
// contract programmatic callers rely on (memseed skip-vs-abort, API 400 vs
// 502): every input-validation failure across the service surface wraps
// ErrInvalidInput. Storage-free paths only — each case fails before any
// statement reaches ClickHouse, so a zero-value Service suffices.
func TestValidationErrorsWrapErrInvalidInput(t *testing.T) {
	var s Service // validation precedes all storage access
	ctx := context.Background()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"RecordObservation/scope", func() error {
			_, err := s.RecordObservation(ctx, Input{Kind: "alert", ActorType: "human", ActorID: "a", Content: "x"})
			return err
		}},
		{"RecordObservation/kind", func() error {
			_, err := s.RecordObservation(ctx, Input{Scope: "sc", Kind: "gossip", ActorType: "human", ActorID: "a", Content: "x"})
			return err
		}},
		{"RecordObservation/confidentiality", func() error {
			_, err := s.RecordObservation(ctx, Input{Scope: "sc", Kind: "alert", ActorType: "human", ActorID: "a", Confidentiality: "public", Content: "x"})
			return err
		}},
		{"AssertFact/subject", func() error {
			_, err := s.AssertFact(ctx, FactInput{Scope: "sc", SubjectID: "not-a-uuid", Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a"})
			return err
		}},
		{"AssertFact/object value", func() error {
			_, err := s.AssertFact(ctx, FactInput{Scope: "sc", SubjectID: "00000000-0000-0000-0000-000000000000", Predicate: "p", ObjectValue: " ", ActorType: "human", ActorID: "a"})
			return err
		}},
		{"PromoteFact/actor id", func() error {
			_, err := s.PromoteFact(ctx, "00000000-0000-0000-0000-000000000000", "human", "  ")
			return err
		}},
		{"RetractFact/actor id", func() error {
			_, err := s.RetractFact(ctx, "not-a-uuid", "", "human", "")
			return err
		}},
		{"Similar/k range", func() error {
			_, err := s.Similar(ctx, "sc", "query text", 99)
			return err
		}},
		{"Similar/query", func() error {
			_, err := s.Similar(ctx, "sc", "   ", 10)
			return err
		}},
		{"Timeline/exactly one id", func() error {
			_, err := s.Timeline(ctx, "sc", "", "", 10, 0)
			return err
		}},
		{"Timeline/limit range", func() error {
			_, err := s.Timeline(ctx, "sc", caseUUIDForTest(), "", 0, 0)
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := mustErrOf(t, c.name, c.fn)
			if !errors.Is(err, ErrInvalidInput) {
				t.Errorf("error does not wrap ErrInvalidInput: %v", err)
			}
		})
	}
}

// caseUUIDForTest returns any syntactically valid UUID for cases where the
// targeted validation error fires after the id parses.
func caseUUIDForTest() string {
	return "00000000-0000-0000-0000-000000000001"
}
