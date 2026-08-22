package config

import "os"

type Config struct {
	ChAddr       string
	ChUser       string
	ChPassword   string
	ListenAddr   string
	EmbedURL     string
	EmbedModel   string
	AgentRateRPS float64
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Load() Config {
	return Config{
		ChAddr:       envOr("MEM_CH_ADDR", "localhost:9000"),
		ChUser:       envOr("MEM_CH_USER", "mem"),
		ChPassword:   envOr("MEM_CH_PASSWORD", "memdev"),
		ListenAddr:   envOr("MEM_LISTEN_ADDR", ":8080"),
		EmbedURL:     envOr("MEM_EMBED_URL", "http://localhost:1234/v1"),
		EmbedModel:   envOr("MEM_EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5"),
		AgentRateRPS: 5,
	}
}
