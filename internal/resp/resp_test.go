package resp

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

type fragmentReader struct{ b []byte }

func (r *fragmentReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	p[0] = r.b[0]
	r.b = r.b[1:]
	return 1, nil
}
func TestFrames(t *testing.T) {
	frames := []string{string(Encode("SET", "product:42", "a\x00b\r\nc")), "$-1\r\n", "*2\r\n+OK\r\n:2\r\n", "%1\r\n+proto\r\n:3\r\n", "_\r\n", "#t\r\n", "!3\r\nERR\r\n", "=5\r\ntxt:a\r\n", "~2\r\n:1\r\n:2\r\n"}
	r := bufio.NewReader(&fragmentReader{b: []byte(strings.Join(frames, ""))})
	for _, want := range frames {
		f, e := Read(r)
		if e != nil {
			t.Fatal(e)
		}
		if string(f.Raw) != want {
			t.Fatalf("framing differs")
		}
	}
	if _, e := Read(r); e != io.EOF {
		t.Fatal(e)
	}
}
func TestCommandBinary(t *testing.T) {
	f, e := Read(bufio.NewReader(bytes.NewReader(Encode("sEt", "a\x00b", "\xff\r\n"))))
	if e != nil {
		t.Fatal(e)
	}
	name, args, e := f.Command()
	if e != nil || name != "SET" || string(args[0]) != "a\x00b" || string(args[1]) != "\xff\r\n" {
		t.Fatal("not binary safe")
	}
}
func TestMalformedAndLimits(t *testing.T) {
	for _, s := range []string{"$-2\r\n", "*999999999\r\n", "$999999999\r\n", "$3\r\nabcXX", "+bad\n", ";3\r\nabc\r\n", "$?\r\n", "#x\r\n", "_x\r\n", strings.Repeat("*1\r\n", 40) + "+x\r\n"} {
		if _, e := Read(bufio.NewReader(strings.NewReader(s))); e == nil {
			t.Fatalf("accepted malformed frame %q", s[:min(len(s), 30)])
		}
	}
}
func FuzzRead(f *testing.F) {
	for _, s := range []string{string(Encode("GET", "x")), "$-1\r\n", "%1\r\n+a\r\n:1\r\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame {
			return
		}
		fr, e := Read(bufio.NewReader(bytes.NewReader(b)))
		if e == nil && !bytes.HasPrefix(b, fr.Raw) {
			t.Fatal("raw frame not preserved")
		}
	})
}
