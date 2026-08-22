package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "")
	t.Setenv("MEM_EMBED_URL", "")
	c := Load()
	if c.ChAddr != "localhost:9000" || c.ListenAddr != ":8080" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.EmbedURL != "http://localhost:1234/v1" || c.EmbedModel == "" {
		t.Fatalf("embed defaults wrong: %+v", c)
	}
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "db:9000")
	c := Load()
	if c.ChAddr != "db:9000" {
		t.Fatalf("override ignored: %+v", c)
	}
}
