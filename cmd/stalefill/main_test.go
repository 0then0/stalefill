package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/0then0/stalefill/internal/scenario"
)

func TestInitPreservesExistingFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	args := []string{"init", "--config", path}
	var out bytes.Buffer
	if run(context.Background(), args, &out, &out) != 0 {
		t.Fatal(out.String())
	}
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if run(context.Background(), args, &out, &out) != 3 {
		t.Fatal("overwrote existing config")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("existing config changed")
	}
	if _, e := scenario.Load(path); e != nil {
		t.Fatal(e)
	}
}
func TestMalformedConfigProducesInfrastructureReport(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	report := filepath.Join(dir, "trace.json")
	os.WriteFile(config, []byte(`{"secret":"SECRET"}`), 0600)
	var out, errOut bytes.Buffer
	if run(context.Background(), []string{"test", "--config", config, "--report", report, "--json"}, &out, &errOut) != 3 {
		t.Fatal("bad config did not return infrastructure exit code")
	}
	b, e := os.ReadFile(report)
	if e != nil {
		t.Fatal(e)
	}
	var r scenario.Report
	if json.Unmarshal(b, &r) != nil || r.Outcome != scenario.InfrastructureError {
		t.Fatal(string(b))
	}
	if bytes.Contains(b, []byte("SECRET")) || bytes.Contains(errOut.Bytes(), []byte("SECRET")) {
		t.Fatal("leaked malformed config")
	}
	info, _ := os.Stat(report)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}
func TestExitCodes(t *testing.T) {
	for i, s := range []string{scenario.Pass, scenario.Fail, scenario.Unresolved, scenario.InfrastructureError} {
		if scenario.ExitCode(s) != i {
			t.Fatal(s)
		}
	}
}

func TestReportCannotReplaceConfig(t *testing.T) {
	for _, name := range []string{"default", "explicit", "normalized", "symlink", "hardlink", "malformed"} {
		t.Run(name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			var requests atomic.Int32
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
			defer h.Close()
			var c scenario.Config
			if e := json.Unmarshal([]byte(scenario.Template), &c); e != nil {
				t.Fatal(e)
			}
			c.Prepare.URL = h.URL
			before, e := json.Marshal(c)
			if e != nil {
				t.Fatal(e)
			}
			if name == "malformed" {
				before = []byte(`{"invalid":true}`)
			}
			config := "stalefill-repro.json"
			if e = os.WriteFile(config, before, 0600); e != nil {
				t.Fatal(e)
			}
			args := []string{"test", "--config", config}
			switch name {
			case "explicit", "malformed":
				args = append(args, "--report", config)
			case "normalized":
				args = append(args, "--report", "./"+config)
			case "symlink":
				if e = os.Symlink(config, "alias.json"); e != nil {
					t.Skipf("symlinks unavailable: %v", e)
				}
				args = []string{"doctor", "--config", "alias.json", "--report", config}
			case "hardlink":
				if e = os.Link(config, "alias.json"); e != nil {
					t.Skipf("hardlinks unavailable: %v", e)
				}
				args = append(args, "--report", "alias.json")
			}
			var out bytes.Buffer
			if code := run(context.Background(), args, &out, &out); code != 3 || !bytes.Contains(out.Bytes(), []byte("report must not replace")) {
				t.Fatalf("collision not rejected: code=%d output=%s", code, &out)
			}
			after, e := os.ReadFile(config)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatalf("config changed: %v", e)
			}
			if requests.Load() != 0 {
				t.Fatal("probe executed before collision rejection")
			}
		})
	}
}
