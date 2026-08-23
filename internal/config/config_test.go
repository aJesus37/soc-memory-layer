package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "")
	t.Setenv("MEM_DGRAPH_ADDR", "")
	t.Setenv("MEM_EMBED_URL", "")
	t.Setenv("MEM_EXTRACT_ENABLED", "")
	t.Setenv("MEM_EXTRACT_MODEL", "")
	t.Setenv("MEM_EXTRACT_INTERVAL_SECONDS", "")
	t.Setenv("MEM_PROJECT_INTERVAL_SECONDS", "")
	c := Load()
	if c.ChAddr != "localhost:9000" || c.ListenAddr != ":8080" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.DgraphAddr != "localhost:9080" {
		t.Fatalf("dgraph default wrong: %+v", c)
	}
	if c.EmbedURL != "http://localhost:1234/v1" || c.EmbedModel == "" {
		t.Fatalf("embed defaults wrong: %+v", c)
	}
	if c.ExtractEnabled {
		t.Errorf("extraction enabled by default: %+v", c)
	}
	if c.ExtractModel != "qwen/qwen3-8b" {
		t.Errorf("extract model default = %q, want qwen/qwen3-8b", c.ExtractModel)
	}
	if c.ExtractIntervalSeconds != 30 {
		t.Errorf("extract interval default = %d, want 30", c.ExtractIntervalSeconds)
	}
	if c.ProjectIntervalSeconds != 5 {
		t.Errorf("project interval default = %d, want 5", c.ProjectIntervalSeconds)
	}
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "db:9000")
	t.Setenv("MEM_DGRAPH_ADDR", "graph:9080")
	t.Setenv("MEM_EXTRACT_ENABLED", "YES")
	t.Setenv("MEM_EXTRACT_MODEL", "llama-3-8b")
	t.Setenv("MEM_EXTRACT_INTERVAL_SECONDS", "60")
	t.Setenv("MEM_PROJECT_INTERVAL_SECONDS", "2")
	c := Load()
	if c.ChAddr != "db:9000" {
		t.Fatalf("override ignored: %+v", c)
	}
	if c.DgraphAddr != "graph:9080" {
		t.Fatalf("dgraph override ignored: %+v", c)
	}
	if !c.ExtractEnabled || c.ExtractModel != "llama-3-8b" ||
		c.ExtractIntervalSeconds != 60 || c.ProjectIntervalSeconds != 2 {
		t.Fatalf("worker overrides ignored: %+v", c)
	}
}

func TestEnvBool(t *testing.T) {
	cases := map[string]bool{
		"1":     true,
		"true":  true,
		"TRUE":  true,
		"Yes":   true,
		"yes":   true,
		"":      false,
		"0":     false,
		"false": false,
		"no":    false,
		"on":    false,
		"junk":  false,
		" true": false, // no trimming: exact tokens only
	}
	for in, want := range cases {
		t.Setenv("MEM_TEST_BOOL", in)
		if got := envBool("MEM_TEST_BOOL"); got != want {
			t.Errorf("envBool(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestEnvIntFallbacks(t *testing.T) {
	t.Setenv("MEM_TEST_INT", "")
	if got := envInt("MEM_TEST_INT", 7); got != 7 {
		t.Errorf("unset env kept def=%d, got %d", 7, got)
	}
	t.Setenv("MEM_TEST_INT", "junk")
	if got := envInt("MEM_TEST_INT", 7); got != 7 {
		t.Errorf("unparseable env kept def=7, got %d", got)
	}
	t.Setenv("MEM_TEST_INT", "-3")
	if got := envInt("MEM_TEST_INT", 7); got != -3 {
		t.Errorf("negative override must pass through for worker refusal, got %d", got)
	}
	t.Setenv("MEM_TEST_INT", " 12 ")
	if got := envInt("MEM_TEST_INT", 7); got != 12 {
		t.Errorf("trimmed override ignored: got %d", got)
	}
}
