package scenario

import (
	"bytes"
	"strconv"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
)

// FillPublication describes a publication boundary, not cache contents. EXEC
// publishes the entire supported transaction; individual QUEUED writes do not.
type FillPublication struct {
	Mode string
	Key  []byte
}

func isFill(name string) bool {
	return name == "SET" || name == "SETEX" || name == "PSETEX" || name == "HSET"
}
func isDelete(name string) bool { return name == "DEL" || name == "UNLINK" }
func isRead(name string) bool {
	return name == "GET" || name == "MGET" || name == "HGET" || name == "HMGET" || name == "HGETALL"
}
func isLua(name string) bool {
	return name == "EVAL" || name == "EVALSHA" || name == "EVAL_RO" || name == "EVALSHA_RO"
}

// Declared KEYS are the only Lua key evidence. Neither source nor ARGV is
// inspected. Malformed declarations cannot establish safe pass-through.
func luaKeys(c proxy.Command) ([][]byte, bool) {
	if len(c.Args) < 2 {
		return nil, false
	}
	count := string(c.Args[1])
	n, err := strconv.ParseInt(count, 10, 64)
	if err != nil || n < 0 || n > int64(len(c.Args)-2) || strconv.FormatInt(n, 10) != count {
		return nil, false
	}
	return c.Args[2 : 2+int(n)], true
}

func commandKeys(c proxy.Command) ([][]byte, bool) {
	if isLua(c.Name) {
		return luaKeys(c)
	}
	switch c.Name {
	case "GET", "SET", "SETEX", "PSETEX", "HGET", "HMGET", "HGETALL", "HSET", "HMSET",
		"HSETEX", "HGETDEL", "HGETEX", "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST",
		"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TTL", "PTTL", "EXISTS",
		"INCR", "INCRBY", "DECR", "DECRBY", "APPEND", "HDEL", "HINCRBY", "HINCRBYFLOAT", "HSETNX", "SETNX", "GETSET", "GETDEL", "GETEX":
		if len(c.Args) > 0 {
			return c.Args[:1], true
		}
		return nil, false
	case "MGET", "DEL", "UNLINK", "WATCH":
		return c.Args, true
	case "RENAME", "RENAMENX", "COPY":
		if len(c.Args) >= 2 {
			return c.Args[:2], true
		}
		return nil, false
	case "MSET", "MSETNX":
		if len(c.Args)%2 != 0 {
			return nil, false
		}
		var keys [][]byte
		for i := 0; i < len(c.Args); i += 2 {
			keys = append(keys, c.Args[i])
		}
		return keys, true
	}
	return nil, false
}

func (s *Scheduler) keys(c proxy.Command) [][]byte {
	keys, _ := commandKeys(c)
	return keys
}
func (s *Scheduler) target(c proxy.Command) bool {
	for _, k := range s.keys(c) {
		if s.match(k) {
			return true
		}
	}
	return false
}

// A deliberately small whitelist: [DEL|UNLINK] HSET+ [EXPIRE|PEXPIRE], all
// operating on one key. No operations on other keys or arbitrary programs.
func (s *Scheduler) publication(c proxy.Command) (FillPublication, bool) {
	if c.Name != "EXEC" && c.Name != "DISCARD" {
		if !c.InTransaction && isFill(c.Name) && s.target(c) {
			mode := "string"
			if c.Name == "HSET" {
				mode = "hash"
			}
			return FillPublication{Mode: mode, Key: c.Args[0]}, false
		}
		return FillPublication{}, false
	}
	if c.Transaction == nil {
		return FillPublication{}, false
	}
	tx := c.Transaction
	if tx.Truncated {
		return FillPublication{}, true
	}
	target := false
	for _, cmd := range tx.Commands {
		keys, known := commandKeys(cmd)
		if !known {
			return FillPublication{}, true
		}
		for _, k := range keys {
			target = target || s.match(k)
		}
	}
	if !target {
		return FillPublication{}, false
	}
	if c.Name == "DISCARD" {
		return FillPublication{}, true
	}
	var key []byte
	writes, ttl := 0, false
	for i, cmd := range tx.Commands {
		if len(cmd.Args) == 0 {
			return FillPublication{}, true
		}
		switch {
		case isDelete(cmd.Name):
			if i != 0 || len(cmd.Args) != 1 {
				return FillPublication{}, true
			}
		case cmd.Name == "HSET":
			if ttl || len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
				return FillPublication{}, true
			}
			writes++
		case cmd.Name == "EXPIRE" || cmd.Name == "PEXPIRE":
			if writes == 0 || ttl || len(cmd.Args) != 2 || i != len(tx.Commands)-1 {
				return FillPublication{}, true
			}
			n, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
			if err != nil || n <= 0 {
				return FillPublication{}, true
			}
			ttl = true
		default:
			return FillPublication{}, true
		}
		// Only whitelisted commands have a key in argument zero. In
		// particular, never compare script source/ARGV as if they were keys.
		if key == nil {
			key = cmd.Args[0]
		} else if !bytes.Equal(key, cmd.Args[0]) {
			return FillPublication{}, true
		}
	}
	if writes == 0 || !s.match(key) {
		return FillPublication{}, true
	}
	return FillPublication{Mode: "transactional_hash", Key: key}, false
}

func (s *Scheduler) readMiss(c proxy.Command, f resp.Frame) bool {
	switch c.Name {
	case "GET", "HGET":
		return (f.Kind == '$' || f.Kind == '_') && f.Null
	case "HGETALL":
		return (f.Kind == '*' || f.Kind == '%') && !f.Null && len(f.Items) == 0
	case "HMGET":
		if f.Kind != '*' || f.Null || len(f.Items) != len(c.Args)-1 || len(f.Items) == 0 {
			return false
		}
		for _, item := range f.Items {
			if !item.Null || (item.Kind != '$' && item.Kind != '_') {
				return false
			}
		}
		return true
	case "MGET":
		if f.Kind == '*' && !f.Null && len(f.Items) == len(c.Args) {
			for i, k := range c.Args {
				if s.match(k) && f.Items[i].Null {
					return true
				}
			}
		}
	}
	return false
}

func publicationSucceeded(c proxy.Command, f resp.Frame) bool {
	if c.Name != "EXEC" {
		return !f.IsError() // SET NX/XX refusal still requires the HTTP oracle.
	}
	if c.Transaction == nil || !c.Transaction.Accepted || f.Kind != '*' || f.Null || len(f.Items) != len(c.Transaction.Commands) {
		return false
	}
	for i, reply := range f.Items {
		if reply.IsError() || reply.Kind != ':' {
			return false
		}
		value, err := strconv.ParseInt(string(reply.Data), 10, 64)
		if err != nil || value < 0 {
			return false
		}
		name := c.Transaction.Commands[i].Name
		if (name == "EXPIRE" || name == "PEXPIRE") && value != 1 {
			return false
		}
	}
	return true
}
