package contracts

import (
	"fmt"
	"sort"
	"strconv"
	"unicode/utf8"
)

// Object is an ordered-by-sorting JSON object used for canonical encoding.
type Object map[string]any

// CanonicalJSON reproduces
// json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
// for the value shapes this service produces.
func CanonicalJSON(v any) []byte {
	return AppendCanonical(make([]byte, 0, 512), v)
}

// AppendCanonical appends the canonical encoding of v to b.
func AppendCanonical(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...)
	case string:
		return AppendPythonJSONString(b, x)
	case bool:
		if x {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case int:
		return strconv.AppendInt(b, int64(x), 10)
	case int64:
		return strconv.AppendInt(b, x, 10)
	case int32:
		return strconv.AppendInt(b, int64(x), 10)
	case Object:
		return appendObject(b, x)
	case map[string]any:
		return appendObject(b, x)
	case []string:
		b = append(b, '[')
		for i, item := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendPythonJSONString(b, item)
		}
		return append(b, ']')
	case []any:
		b = append(b, '[')
		for i, item := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendCanonical(b, item)
		}
		return append(b, ']')
	case []Object:
		b = append(b, '[')
		for i, item := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendObject(b, item)
		}
		return append(b, ']')
	default:
		panic(fmt.Sprintf("canonical json: unsupported type %T", v))
	}
}

func appendObject(b []byte, m map[string]any) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b = append(b, '{')
	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = AppendPythonJSONString(b, k)
		b = append(b, ':')
		b = AppendCanonical(b, m[k])
	}
	return append(b, '}')
}

const hexDigits = "0123456789abcdef"

// AppendPythonJSONString escapes like Python's json encoder with
// ensure_ascii=False: only quote, backslash and C0 controls are escaped.
func AppendPythonJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			if c < utf8.RuneSelf {
				i++
				continue
			}
			_, size := utf8.DecodeRuneInString(s[i:])
			i += size
			continue
		}
		b = append(b, s[start:i]...)
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		case '\b':
			b = append(b, '\\', 'b')
		case '\f':
			b = append(b, '\\', 'f')
		default:
			b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		i++
		start = i
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}
