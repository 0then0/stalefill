package scenario

import (
	"encoding/json"
	"testing"
)

func TestBaselinePublicationConfig(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index int
		value string
		valid bool
	}{
		{"read", 1, "none", true}, {"verify", 4, "none", true}, {"default", 4, "", true},
		{"unknown", 4, "auto", false}, {"prepare", 0, "none", false}, {"write", 2, "none", false}, {"authoritative", 3, "none", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c Config
			if err := json.Unmarshal([]byte(Template), &c); err != nil {
				t.Fatal(err)
			}
			probes := []*Probe{&c.Prepare, &c.Read, &c.Write, &c.Authoritative, &c.Verify}
			probes[tc.index].CachePublication = tc.value
			if err := c.Validate(); (err == nil) != tc.valid {
				t.Fatal(err)
			}
		})
	}
}
