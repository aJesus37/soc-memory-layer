package memory

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestExtractEntityCandidates is a pure unit test: it needs no ClickHouse
// and runs without MEM_TEST_CH_ADDR.
func TestExtractEntityCandidates(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "happy path ip and domain",
			content: "Saw 1.2.3.4 beaconing to evil.example.com",
			want:    []string{"1.2.3.4", "evil.example.com"},
		},
		{
			name:    "host:port collapses to host for ip",
			content: "C2 at 8.8.8.8:53 and bad.example.com:443 observed",
			want:    []string{"8.8.8.8", "bad.example.com"},
		},
		{
			name:    "bracketed ipv6 with port",
			content: "proxy hop [2001:db8::1]:8080 blocked",
			want:    []string{"2001:db8::1"},
		},
		{
			name:    "bare ipv6 loopback still extracted",
			content: "connection to ::1 refused",
			want:    []string{"::1"},
		},
		{
			name:    "duplicates deduped keeping first position",
			content: "hit 1.2.3.4 then 1.2.3.4 again, plus evil.example.com and evil.example.com once more",
			want:    []string{"1.2.3.4", "evil.example.com"},
		},
		{
			name:    "time-like colon pair dropped",
			content: "beacon every 12:34 sharp from 10.0.0.1",
			want:    []string{"10.0.0.1"},
		},
		{
			name:    "multi-colon non-ipv6 token dropped whole",
			content: "odd string a:b:c in notes next to 10.0.0.2",
			want:    []string{"10.0.0.2"},
		},
		{
			name:    "port suffix on unnormalizable prefix drops the token entirely",
			content: "weird foo:12345 marker plus 10.0.0.3",
			want:    []string{"10.0.0.3"},
		},
		{
			name:    "no candidates in plain prose",
			content: "plain prose without any indicators whatsoever here",
			want:    nil,
		},
		// TODO(fix): "report.docx" passes the domain label rules (letters in
		// the TLD) and is linked as ioc_domain. Separating filename
		// extensions from domain detection is intentionally OUT OF SCOPE for
		// this change; this case pins current behavior so any future fix
		// must be deliberate and update this assertion.
		{
			name:    "filename pinned as ioc_domain (known wart)",
			content: "attached report.docx for review",
			want:    []string{"report.docx"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractEntityCandidates(tt.content)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractEntityCandidates(%q)\n = %q\nwant %q", tt.content, got, tt.want)
			}
		})
	}
}

func TestExtractEntityCandidatesCap(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 40; i++ { // 40 distinct valid IPs > cap of 32
		fmt.Fprintf(&b, " scan from 10.%d.0.1", i)
	}
	got := extractEntityCandidates(b.String())
	if len(got) != maxEntityCandidates {
		t.Fatalf("len(candidates) = %d, want exactly %d", len(got), maxEntityCandidates)
	}
	for i, c := range got { // first-come order preserved, overflow dropped
		want := fmt.Sprintf("10.%d.0.1", i+1)
		if c != want {
			t.Fatalf("candidate[%d] = %q, want %q", i, c, want)
		}
	}
}

func TestExtractEntityCandidatesJunkDoesNotConsumeCap(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 40; i++ { // junk tokens before any valid candidate
		fmt.Fprintf(&b, " filler%d", i)
	}
	for i := 1; i <= 5; i++ {
		fmt.Fprintf(&b, " beacon 10.0.0.%d seen", i)
	}
	got := extractEntityCandidates(b.String())
	want := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("candidates = %q\nwant %q (junk must not consume cap slots)", got, want)
	}
}
