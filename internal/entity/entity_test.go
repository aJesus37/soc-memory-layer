package entity

import (
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := []struct {
		in  string
		typ Type
		key string
		ok  bool
	}{
		{"Example.COM", IocDomain, "example.com", true},
		{"  example.com. ", IocDomain, "example.com", true},
		{"sub.example.co.uk", IocDomain, "sub.example.co.uk", true},
		{"1.2.3.4", IocIP, "1.2.3.4", true},
		{"::1", IocIP, "::1", true},
		{"2001:db8::01", IocIP, "2001:db8::1", true},
		{"999.1.1.1", "", "", false},
		{"D41D8CD98F00B204E9800998ECF8427E", IocHash, "d41d8cd98f00b204e9800998ecf8427e", true},
		{"da39a3ee5e6b4b0d3255bfef95601890afd80709", IocHash, "da39a3ee5e6b4b0d3255bfef95601890afd80709", true},
		{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", IocHash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", true},
		{"notahash", "", "", false},
		{"T1566", Technique, "T1566", true},
		{"t1566.002", Technique, "T1566.002", true},
		{"T99999", "", "", false},
		{"hello world", "", "", false},
		{"", "", "", false},
		// sha512 length accepted
		{strings.Repeat("a", 128), IocHash, strings.Repeat("a", 128), true},
		// real-world contaminants must stay rejected (dedup keys depend on it)
		{"http://example.com", "", "", false},
		{"example.com:8080", "", "", false},
		{"fe80::1%eth0", "", "", false},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if c.ok {
			if err != nil {
				t.Errorf("%q: unexpected error %v", c.in, err)
				continue
			}
			if got.Type != c.typ || got.Key != c.key {
				t.Errorf("%q → %+v, want %s/%s", c.in, got, c.typ, c.key)
			}
		} else if err == nil {
			t.Errorf("%q should not normalize (got %+v)", c.in, got)
		}
	}
}
