// Package embed turns text into vectors via an OpenAI-compatible API.
package embed

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand"
)

// Embedder produces embeddings for batches of text.
type Embedder interface {
	// Embed returns one vector per input text, in input order.
	// kind is "document" or "query" (models like nomic use task prefixes).
	Embed(ctx context.Context, kind string, texts []string) ([][]float32, error)
}

// Fake is a deterministic Embedder for tests.
type Fake struct {
	dim int
}

// NewFake returns an Embedder producing deterministic normalized vectors:
// FNV-1a hash of (kind + "\x00" + text) seeds dim values from math/rand,
// then L2-normalized. Same text+kind always yields identical vectors;
// different texts almost never collide.
func NewFake(dim int) *Fake {
	return &Fake{dim: dim}
}

func (f *Fake) Embed(_ context.Context, kind string, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		h := fnv.New64a()
		h.Write([]byte(kind))
		h.Write([]byte("\x00"))
		h.Write([]byte(text))
		rng := rand.New(rand.NewSource(int64(h.Sum64())))
		vec := make([]float32, f.dim)
		var sum float64
		for j := range vec {
			v := rng.NormFloat64()
			vec[j] = float32(v)
			sum += v * v
		}
		norm := math.Sqrt(sum)
		for j := range vec {
			vec[j] = float32(float64(vec[j]) / norm)
		}
		out[i] = vec
	}
	return out, nil
}
