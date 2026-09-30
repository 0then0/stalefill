// Package resp frames bounded RESP2 and non-streaming RESP3 messages without
// re-encoding them. Cache values remain opaque binary strings.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const MaxFrame = 16 << 20
const maxItems = 65536

type Frame struct {
	Raw   []byte
	Kind  byte
	Data  []byte
	Items []Frame
	Null  bool
}

func Read(r *bufio.Reader) (Frame, error) {
	var raw bytes.Buffer
	f, err := read(r, &raw, 0)
	f.Raw = raw.Bytes()
	return f, err
}

func read(r *bufio.Reader, raw *bytes.Buffer, depth int) (Frame, error) {
	var f Frame
	if depth > 32 {
		return f, errors.New("RESP nesting limit")
	}
	b, err := r.ReadByte()
	if err != nil {
		return f, err
	}
	f.Kind = b
	if raw.Len() >= MaxFrame {
		return f, errors.New("RESP frame limit")
	}
	raw.WriteByte(b)
	// ReadSlice bounds each allocation even for a malicious unterminated line.
	var line []byte
	for {
		part, e := r.ReadSlice('\n')
		if raw.Len()+len(part) > MaxFrame {
			return f, errors.New("RESP frame limit")
		}
		raw.Write(part)
		line = append(line, part...)
		if e == bufio.ErrBufferFull {
			continue
		}
		if e != nil {
			return f, e
		}
		break
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return f, errors.New("invalid RESP line")
	}
	line = line[:len(line)-2]
	switch b {
	case '+', '-', ':', ',', '(', '#', '_':
		f.Data = line
		f.Null = b == '_'
		if (b == '_' && len(line) != 0) || (b == '#' && string(line) != "t" && string(line) != "f") {
			return f, errors.New("invalid RESP scalar")
		}
	case '$', '!', '=':
		n, e := strconv.ParseInt(string(line), 10, 64)
		if e != nil || n < -1 || n > MaxFrame || (n == -1 && b != '$') {
			return f, errors.New("unsupported RESP bulk length")
		}
		if n == -1 {
			f.Null = true
			return f, nil
		}
		if n+2 > int64(MaxFrame-raw.Len()) {
			return f, errors.New("RESP frame limit")
		}
		data := make([]byte, int(n)+2)
		if _, e = io.ReadFull(r, data); e != nil {
			return f, e
		}
		if !bytes.Equal(data[n:], []byte("\r\n")) {
			return f, errors.New("invalid RESP bulk terminator")
		}
		raw.Write(data)
		f.Data = data[:n]
	case '*', '~', '%', '|', '>':
		n, e := strconv.ParseInt(string(line), 10, 64)
		if e != nil || n < -1 || n > maxItems || (n == -1 && b != '*') {
			return f, errors.New("unsupported RESP aggregate length")
		}
		if n == -1 {
			f.Null = true
			return f, nil
		}
		if b == '%' || b == '|' {
			n *= 2
		}
		for i := int64(0); i < n; i++ {
			child, e := read(r, raw, depth+1)
			if e != nil {
				return f, e
			}
			f.Items = append(f.Items, child)
		}
	default:
		return f, errors.New("unsupported RESP type (inline and streamed messages are unsupported)")
	}
	return f, nil
}

func (f Frame) Command() (string, [][]byte, error) {
	if f.Kind != '*' || f.Null || len(f.Items) == 0 {
		return "", nil, errors.New("expected command array")
	}
	args := make([][]byte, len(f.Items))
	for i, x := range f.Items {
		if x.Kind != '$' && x.Kind != '+' || x.Null {
			return "", nil, errors.New("expected string argument")
		}
		args[i] = x.Data
	}
	return strings.ToUpper(string(args[0])), args[1:], nil
}

func Encode(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, s := range args {
		fmt.Fprintf(&b, "$%d\r\n", len(s))
		b.WriteString(s)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

func (f Frame) IsError() bool { return f.Kind == '-' || f.Kind == '!' }
