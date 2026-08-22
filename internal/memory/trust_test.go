package memory

import "testing"

func TestApplyTrust(t *testing.T) {
	tc := trustConfig{floor: 0.8, whitelist: map[string]bool{"resolved_to": true, "ioc_extraction": true}}
	def := loadTrustConfig() // one case exercises the default env config end-to-end
	cases := []struct {
		name      string
		actorType string
		predicate string
		conf      float32
		tc        trustConfig
		want      Status
	}{
		{"human always active", "human", "attributed_to", 0.1, tc, Active},
		{"human active even whitelisted", "human", "resolved_to", 0.99, tc, Active},
		{"agent non-whitelisted proposed", "agent", "attributed_to", 0.99, tc, Proposed},
		{"agent whitelisted high conf active", "agent", "resolved_to", 0.9, tc, Active},
		{"agent whitelisted low conf proposed", "agent", "resolved_to", 0.5, tc, Proposed},
		{"unknown actor proposed", "robot", "resolved_to", 0.9, tc, Proposed},
		{"default config above floor active", "agent", "resolved_to", 0.85, def, Active},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ApplyTrust(c.actorType, c.predicate, c.conf, c.tc); got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}

	t.Run("conf exactly at floor is active", func(t *testing.T) {
		half := trustConfig{floor: 0.5, whitelist: map[string]bool{"resolved_to": true}}
		if got := ApplyTrust(actorAgent, "resolved_to", 0.5, half); got != Active {
			t.Errorf("got %v want %v", got, Active)
		}
	})

	t.Run("nil whitelist fails closed", func(t *testing.T) {
		if got := ApplyTrust(actorAgent, "resolved_to", 0.9, trustConfig{}); got != Proposed {
			t.Errorf("got %v want %v", got, Proposed)
		}
	})
}

func TestTrustConfigFromEnv(t *testing.T) {
	t.Setenv("MEM_TRUST_FLOOR", "0.95")
	t.Setenv("MEM_TRUST_WHITELIST", "resolved_to,ioc_extraction")
	cfg := loadTrustConfig()
	if cfg.floor != 0.95 || !cfg.whitelist["resolved_to"] || !cfg.whitelist["ioc_extraction"] {
		t.Fatalf("bad: %+v", cfg)
	}

	// Replace semantics: a non-empty list must replace the default wholesale,
	// never merge — resolved_to must NOT survive alongside ioc_extraction.
	t.Setenv("MEM_TRUST_WHITELIST", "ioc_extraction")
	r := loadTrustConfig()
	if !r.whitelist["ioc_extraction"] || r.whitelist["resolved_to"] || len(r.whitelist) != 1 {
		t.Fatalf("whitelist replaced, not merged: %+v", r)
	}

	// Out-of-range floors fall back to the default.
	for _, v := range []string{"-0.5", "1.5", "NaN"} {
		t.Setenv("MEM_TRUST_FLOOR", v)
		if f := loadTrustConfig(); f.floor != defaultTrustFloor {
			t.Fatalf("floor %q must keep default %v, got %v", v, defaultTrustFloor, f.floor)
		}
	}

	// The boundaries of the accepted range are honored.
	t.Setenv("MEM_TRUST_FLOOR", "0")
	if f := loadTrustConfig(); f.floor != 0 {
		t.Fatalf("floor 0 rejected: got %v", f.floor)
	}
	t.Setenv("MEM_TRUST_FLOOR", "1")
	if f := loadTrustConfig(); f.floor != 1 {
		t.Fatalf("floor 1 rejected: got %v", f.floor)
	}

	// defaults
	t.Setenv("MEM_TRUST_FLOOR", "")
	t.Setenv("MEM_TRUST_WHITELIST", "")
	d := loadTrustConfig()
	if d.floor != 0.8 || len(d.whitelist) != 1 || !d.whitelist["resolved_to"] {
		t.Fatalf("defaults bad: %+v", d)
	}
}
