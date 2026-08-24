package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ChAddr       string
	DgraphAddr   string
	ChUser       string
	ChPassword   string
	ListenAddr   string
	EmbedURL     string
	EmbedModel   string
	EmbedAPIKey  string // MEM_EMBED_API_KEY
	AgentRateRPS float64

	// Phase-2 background workers.
	ExtractEnabled         bool   // MEM_EXTRACT_ENABLED, default false
	ExtractModel           string // MEM_EXTRACT_MODEL
	ExtractBaseURL         string // MEM_EXTRACT_BASE_URL; empty → EmbedURL
	ExtractAPIKey          string // MEM_EXTRACT_API_KEY
	ExtractIntervalSeconds int    // MEM_EXTRACT_INTERVAL_SECONDS
	ProjectIntervalSeconds int    // MEM_PROJECT_INTERVAL_SECONDS
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBool reads a boolean env var: "1", "true" and "yes" (case-insensitive)
// enable it; anything else — unset, empty or unrecognized — stays false.
func envBool(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

// envInt reads an integer env var; an unset or unparseable value keeps def.
// Non-positive overrides pass through: the workers that consume intervals
// refuse them fast (see memory.RunExtractionLoop and the projection loop).
func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Load() Config {
	embedURL := envOr("MEM_EMBED_URL", "http://localhost:3000")
	extractBaseURL := os.Getenv("MEM_EXTRACT_BASE_URL")
	if strings.TrimSpace(extractBaseURL) == "" {
		extractBaseURL = embedURL
	}
	return Config{
		ChAddr:       envOr("MEM_CH_ADDR", "localhost:9000"),
		DgraphAddr:   envOr("MEM_DGRAPH_ADDR", "localhost:9080"),
		ChUser:       envOr("MEM_CH_USER", "mem"),
		ChPassword:   envOr("MEM_CH_PASSWORD", "memdev"),
		ListenAddr:   envOr("MEM_LISTEN_ADDR", ":8080"),
		EmbedURL:     embedURL,
		EmbedModel:   envOr("MEM_EMBED_MODEL", "nomic-ai/nomic-embed-text-v1.5"),
		EmbedAPIKey:  os.Getenv("MEM_EMBED_API_KEY"),
		AgentRateRPS: 5,

		ExtractEnabled:         envBool("MEM_EXTRACT_ENABLED"),
		ExtractModel:           envOr("MEM_EXTRACT_MODEL", "qwen/qwen3-8b"),
		ExtractBaseURL:         extractBaseURL,
		ExtractAPIKey:          os.Getenv("MEM_EXTRACT_API_KEY"),
		ExtractIntervalSeconds: envInt("MEM_EXTRACT_INTERVAL_SECONDS", 30),
		ProjectIntervalSeconds: envInt("MEM_PROJECT_INTERVAL_SECONDS", 5),
	}
}
