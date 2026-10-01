package scenario

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Assertion struct {
	Path   string          `json:"json_path"`
	Equals json.RawMessage `json:"equals"`
}
type Probe struct {
	CachePublication string            `json:"cache_publication,omitempty"`
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	Headers          map[string]string `json:"headers,omitempty"`
	JSON             json.RawMessage   `json:"json,omitempty"`
	Status           int               `json:"status"`
	Assert           *Assertion        `json:"assert,omitempty"`
}
type Config struct {
	Version int `json:"version"`
	Redis   struct {
		Listen      string `json:"listen"`
		Upstream    string `json:"upstream"`
		AllowRemote bool   `json:"allow_remote"`
	} `json:"redis"`
	Scenario struct {
		Type     string `json:"type"`
		Key      string `json:"key,omitempty"`
		KeyRegex string `json:"key_regex,omitempty"`
		Timeout  string `json:"timeout"`
	} `json:"scenario"`
	Prepare       Probe `json:"prepare"`
	Read          Probe `json:"read"`
	Write         Probe `json:"write"`
	Authoritative Probe `json:"authoritative"`
	Verify        Probe `json:"verify"`
}

func Load(path string) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e != nil {
		return c, errors.New("cannot read config file")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, errors.New("invalid config JSON or unknown field")
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, errors.New("config must contain one JSON object")
	}
	return c, c.Validate()
}

func LocalAddress(addr string) bool {
	host, _, e := net.SplitHostPort(addr)
	if e != nil {
		return false
	}
	// Literal loopback only: no DNS rebinding or ambiguous localhost resolution.
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func validAddress(addr string) bool {
	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return false
	}
	_, e = net.LookupPort("tcp", p)
	return e == nil
}

var pathRE = regexp.MustCompile(`^\$(\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])*$`)

func (c Config) Validate() error {
	if c.Version != 1 {
		return errors.New("config version must be 1")
	}
	if !validAddress(c.Redis.Listen) || !LocalAddress(c.Redis.Listen) {
		return errors.New("listen must use a literal loopback IP and port")
	}
	if !validAddress(c.Redis.Upstream) {
		return errors.New("invalid upstream address")
	}
	if !c.Redis.AllowRemote && !LocalAddress(c.Redis.Upstream) {
		return errors.New("remote upstream requires redis.allow_remote=true")
	}
	if c.Redis.Listen == c.Redis.Upstream {
		return errors.New("listen and upstream must differ")
	}
	if c.Scenario.Type != "stale_fill_after_invalidation" && c.Scenario.Type != "negative_cache_resurrection" {
		return errors.New("unsupported scenario type")
	}
	if (c.Scenario.Key == "") == (c.Scenario.KeyRegex == "") {
		return errors.New("specify exactly one key or key_regex")
	}
	if len(c.Scenario.KeyRegex) > 512 {
		return errors.New("key regex too long")
	}
	if c.Scenario.KeyRegex != "" {
		if !strings.HasPrefix(c.Scenario.KeyRegex, "^") || !strings.HasSuffix(c.Scenario.KeyRegex, "$") {
			return errors.New("key_regex must be anchored")
		}
		if _, e := regexp.Compile(c.Scenario.KeyRegex); e != nil {
			return errors.New("invalid key_regex")
		}
	}
	t, e := time.ParseDuration(c.Scenario.Timeout)
	if e != nil || t <= 0 || t > 5*time.Minute {
		return errors.New("scenario timeout must be >0 and <=5m")
	}
	for i, p := range []Probe{c.Prepare, c.Read, c.Write, c.Authoritative, c.Verify} {
		if p.CachePublication != "" && (p.CachePublication != "none" || (i != 1 && i != 4)) {
			return errors.New("cache_publication must be none and only applies to read or verify baseline probes")
		}
		u, e := url.Parse(p.URL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return errors.New("probe requires an HTTP(S) URL without userinfo")
		}
		if p.Method == "" || p.Status < 100 || p.Status > 599 {
			return errors.New("probe requires method and expected status")
		}
		if _, e = http.NewRequest(p.Method, p.URL, nil); e != nil {
			return errors.New("invalid probe method or URL")
		}
		if p.Assert != nil {
			if !pathRE.MatchString(p.Assert.Path) || len(p.Assert.Equals) == 0 || !json.Valid(p.Assert.Equals) {
				return errors.New("assert requires supported json_path and JSON equals")
			}
		}
	}
	if c.Read.Assert == nil || c.Verify.Assert == nil || c.Authoritative.Assert == nil {
		return errors.New("read, authoritative and verify assertions are required")
	}
	if c.Read.Assert.Path != c.Verify.Assert.Path || !equalJSON(c.Authoritative.Assert.Equals, c.Verify.Assert.Equals) {
		return errors.New("read and verify paths must match; authoritative and verify expected values must match")
	}
	if equalJSON(c.Read.Assert.Equals, c.Verify.Assert.Equals) {
		return errors.New("old and new observations must differ")
	}
	return nil
}

func equalJSON(a, b []byte) bool {
	var x, y any
	dx := json.NewDecoder(bytes.NewReader(a))
	dx.UseNumber()
	dy := json.NewDecoder(bytes.NewReader(b))
	dy.UseNumber()
	if dx.Decode(&x) != nil || dy.Decode(&y) != nil {
		return false
	}
	return equalValue(x, y)
}

func equalValue(x, y any) bool {
	switch a := x.(type) {
	case json.Number:
		b, ok := y.(json.Number)
		if !ok {
			return false
		}
		if len(a) > 128 || len(b) > 128 {
			return a == b
		}
		for _, n := range []json.Number{a, b} {
			if i := strings.IndexAny(string(n), "eE"); i >= 0 {
				ex, err := strconv.Atoi(string(n)[i+1:])
				if err != nil || ex > 1024 || ex < -1024 {
					return a == b
				}
			}
		}
		ra, oka := new(big.Rat).SetString(string(a))
		rb, okb := new(big.Rat).SetString(string(b))
		return oka && okb && ra.Cmp(rb) == 0
	case map[string]any:
		b, ok := y.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for k, v := range a {
			w, ok := b[k]
			if !ok || !equalValue(v, w) {
				return false
			}
		}
		return true
	case []any:
		b, ok := y.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if !equalValue(v, b[i]) {
				return false
			}
		}
		return true
	default:
		return x == y
	}
}

const Template = `{
  "version": 1,
  "redis": {"listen": "127.0.0.1:6380", "upstream": "127.0.0.1:6379", "allow_remote": false},
  "scenario": {"type": "stale_fill_after_invalidation", "key": "product:42", "timeout": "10s"},
  "prepare": {"method": "PUT", "url": "http://127.0.0.1:8080/items/42", "json": {"version": 1}, "status": 200},
  "read": {"method": "GET", "url": "http://127.0.0.1:8080/items/42", "status": 200, "assert": {"json_path": "$.version", "equals": 1}},
  "write": {"method": "PUT", "url": "http://127.0.0.1:8080/items/42", "json": {"version": 2}, "status": 200},
  "authoritative": {"method": "GET", "url": "http://127.0.0.1:8080/authoritative/42", "status": 200, "assert": {"json_path": "$.version", "equals": 2}},
  "verify": {"method": "GET", "url": "http://127.0.0.1:8080/items/42", "status": 200, "assert": {"json_path": "$.version", "equals": 2}}
}
`

func duration(c Config) time.Duration { t, _ := time.ParseDuration(c.Scenario.Timeout); return t }
