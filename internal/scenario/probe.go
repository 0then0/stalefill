package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type probeResult struct {
	value          json.RawMessage
	err            error
	mismatch       bool
	status         int
	statusMismatch bool
}

var pathTokenRE = regexpPathTokens()

// Only object fields and array indices: no filters, scripts or wildcard DSL.
func extract(body []byte, path string) (json.RawMessage, error) {
	var value any
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if e := d.Decode(&value); e != nil {
		return nil, errors.New("probe returned invalid JSON")
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, errors.New("probe returned multiple JSON values")
	}
	for _, token := range pathTokenRE.FindAllString(path[1:], -1) {
		if token[0] == '.' {
			m, ok := value.(map[string]any)
			if !ok {
				return nil, errors.New("JSON path is not an object")
			}
			value, ok = m[token[1:]]
			if !ok {
				return nil, errors.New("JSON path missing")
			}
		} else {
			a, ok := value.([]any)
			i, parseErr := strconv.Atoi(token[1 : len(token)-1])
			if parseErr != nil || !ok || i >= len(a) {
				return nil, errors.New("JSON index missing")
			}
			value = a[i]
		}
	}
	return json.Marshal(value)
}

func probe(ctx context.Context, p Probe) probeResult {
	req, e := http.NewRequestWithContext(ctx, p.Method, p.URL, bytes.NewReader(p.JSON))
	if e != nil {
		return probeResult{err: errors.New("invalid HTTP probe")}
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	if len(p.JSON) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	// Neither URL-containing transport errors nor response bodies enter reports.
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	res, e := client.Do(req)
	if e != nil {
		return probeResult{err: errors.New("HTTP probe connection failed or timed out")}
	}
	defer res.Body.Close()
	b, e := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
	if e != nil || len(b) > 1<<20 {
		return probeResult{err: errors.New("HTTP probe body unreadable or exceeds 1 MiB")}
	}
	result := probeResult{status: res.StatusCode, statusMismatch: res.StatusCode != p.Status}
	if p.Assert == nil {
		return result
	}
	v, e := extract(b, p.Assert.Path)
	if e != nil {
		return probeResult{err: e}
	}
	result.value = v
	result.mismatch = !equalJSON(v, p.Assert.Equals)
	return result
}
func baselineProbe(ctx context.Context, p Probe) error {
	r := probe(ctx, p)
	if r.err != nil {
		return r.err
	}
	if r.statusMismatch {
		return errors.New("HTTP probe status assertion failed")
	}
	if r.mismatch {
		return errors.New("baseline JSON assertion failed")
	}
	return nil
}

// Keep the parser definition near the supported path syntax.
func regexpPathTokens() *regexp.Regexp {
	return regexp.MustCompile(`\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\]`)
}

func findingMessage(id string) string {
	switch id {
	case "SF001":
		return "stale cache resurrection"
	case "SF002":
		return "negative cache resurrection"
	case "SF003":
		return "expected invalidation not observed"
	case "SF004":
		return "target read/fill/invalidation traffic absent; check proxy wiring and prepare probe"
	case "SF005":
		return "expected schedule did not complete"
	case "SF006":
		return "opaque command, multiple fills, multiple selected keys, same-connection invalidation or unsupported protocol mode"
	case "SF007":
		return "final observation differs from both configured old and new states"
	case "SF008":
		return "cache publication discarded, rejected or aborted"
	case "SF100":
		return "infrastructure or baseline error"
	}
	return strings.ToLower(id)
}
