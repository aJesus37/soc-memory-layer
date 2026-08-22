package memory

import "testing"

func TestApplyTrust(t *testing.T) {
	wl := map[string]bool{"resolved_to": true, "ioc_extraction": true}
	cases := []struct {
		name      string
		actorType string
		predicate string
		conf      float32
		want      Status
	}{
		{"human always active", "human", "attributed_to", 0.1, Active},
		{"human active even whitelisted", "human", "resolved_to", 0.99, Active},
		{"agent non-whitelisted proposed", "agent", "attributed_to", 0.99, Proposed},
		{"agent whitelisted high conf active", "agent", "resolved_to", 0.9, Active},
		{"agent whitelisted low conf proposed", "agent", "resolved_to", 0.5, Proposed},
		{"unknown actor proposed", "robot", "resolved_to", 0.9, Proposed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ApplyTrust(c.actorType, c.predicate, c.conf, wl); got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestTrustConfigFromEnv(t *testing.T) {
	t.Setenv("MEM_TRUST_FLOOR", "0.95")
	t.Setenv("MEM_TRUST_WHITELIST", "resolved_to,ioc_extraction")
	cfg := loadTrustConfig()
	if cfg.floor != 0.95 || !cfg.whitelist["resolved_to"] || !cfg.whitelist["ioc_extraction"] {
		t.Fatalf("bad: %+v", cfg)
	}
	// defaults
	t.Setenv("MEM_TRUST_FLOOR", "")
	t.Setenv("MEM_TRUST_WHITELIST", "")
	d := loadTrustConfig()
	if d.floor != 0.8 || len(d.whitelist) != 1 || !d.whitelist["resolved_to"] {
		t.Fatalf("defaults bad: %+v", d)
	}
}
