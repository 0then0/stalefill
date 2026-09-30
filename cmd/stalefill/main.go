package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/0then0/stalefill/internal/scenario"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, "usage: stalefill init|doctor|test|version [--config stalefill.json] [--report stalefill-repro.json] [--json]")
		return 3
	}
	if args[0] == "version" {
		fmt.Fprintln(out, scenario.Version)
		return 0
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(errOut)
	config := fs.String("config", "stalefill.json", "configuration file")
	reportPath := fs.String("report", "stalefill-repro.json", "JSON report and ordered trace")
	jsonOut := fs.Bool("json", false, "emit JSON to stdout")
	if e := fs.Parse(args[1:]); e != nil || fs.NArg() != 0 {
		return 3
	}
	if args[0] == "init" {
		f, e := os.OpenFile(*config, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			fmt.Fprintln(errOut, "cannot create config (existing files are never overwritten)")
			return 3
		}
		_, e = f.WriteString(scenario.Template)
		closeErr := f.Close()
		if e != nil || closeErr != nil {
			fmt.Fprintln(errOut, "cannot write config")
			return 3
		}
		fmt.Fprintln(out, "Created configuration")
		return 0
	}
	if args[0] != "test" && args[0] != "doctor" {
		fmt.Fprintln(errOut, "unknown command")
		return 3
	}
	if same, e := sameFile(*config, *reportPath); e != nil || same {
		fmt.Fprintln(errOut, "report must not replace the config file")
		return 3
	}
	c, e := scenario.Load(*config)
	var r scenario.Report
	if e != nil {
		r = scenario.InitialReport(c)
		r.Set(scenario.InfrastructureError, "SF100")
		fmt.Fprintln(errOut, e)
	} else {
		r = scenario.Run(ctx, c, args[0] == "doctor")
	}
	if e = scenario.WriteReport(*reportPath, r); e != nil {
		fmt.Fprintln(errOut, "cannot write report")
		return 3
	}
	if *jsonOut {
		if json.NewEncoder(out).Encode(r) != nil {
			return 3
		}
	} else {
		r.Human(out)
	}
	return scenario.ExitCode(r.Outcome)
}

func sameFile(a, b string) (bool, error) {
	a, e := filepath.Abs(a)
	if e != nil {
		return false, e
	}
	b, e = filepath.Abs(b)
	if e != nil {
		return false, e
	}
	if a == b {
		return true, nil
	}
	ai, ae := os.Stat(a)
	bi, be := os.Stat(b)
	return ae == nil && be == nil && os.SameFile(ai, bi), nil
}
