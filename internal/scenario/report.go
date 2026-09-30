package scenario

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (r Report) Human(w io.Writer) {
	fmt.Fprintf(w, "Scenario: %s\n", r.Scenario)
	for _, e := range r.Events {
		fmt.Fprintf(w, "%02d %s\n", e.Seq, e.Event)
	}
	fmt.Fprintln(w, r.Outcome)
	for _, f := range r.Findings {
		fmt.Fprintf(w, "%s: %s\n", f.ID, f.Message)
	}
}

// WriteReport replaces a file atomically with mode 0600. No config, probe body,
// URL, header, Redis value or upstream error text is serialized.
func WriteReport(path string, r Report) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	f, e := os.CreateTemp(filepath.Dir(path), ".stalefill-report-*")
	if e != nil {
		return e
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(temp, path)
}
func ExitCode(outcome string) int {
	switch outcome {
	case Pass:
		return 0
	case Fail:
		return 1
	case Unresolved:
		return 2
	default:
		return 3
	}
}
