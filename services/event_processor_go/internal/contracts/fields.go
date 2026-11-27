package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// fields is a strict JSON object reader with pydantic strict-mode semantics
// (no coercion, extra="forbid", every listed field required).
type fields struct {
	raw map[string]json.RawMessage
	err error
}

var errNotObject = errors.New("value must be a JSON object")

func decodeObject(data []byte) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errNotObject
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func newFields(data []byte, names ...string) *fields {
	m, err := decodeObject(data)
	f := &fields{raw: m, err: err}
	if err != nil {
		return f
	}
	if len(m) != len(names) {
		f.fail("unexpected or missing fields")
		return f
	}
	for _, name := range names {
		if _, ok := m[name]; !ok {
			f.fail("missing field " + name)
			return f
		}
	}
	return f
}

func (f *fields) fail(msg string) {
	if f.err == nil {
		f.err = errors.New(msg)
	}
}

func (f *fields) get(name string) json.RawMessage {
	if f.err != nil {
		return nil
	}
	v := f.raw[name]
	if len(v) == 0 {
		f.fail("missing field " + name)
	}
	return v
}

func kind(v json.RawMessage) byte {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func isNull(v json.RawMessage) bool { return string(v) == "null" }

func (f *fields) str(name string) string {
	v := f.get(name)
	if f.err != nil {
		return ""
	}
	if kind(v) != '"' {
		f.fail(name + " must be a string")
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		f.fail(name + " is not a valid string")
	}
	return s
}

func (f *fields) nonEmpty(name string) string {
	s := TrimPythonSpace(f.str(name))
	if f.err == nil && s == "" {
		f.fail(name + " must not be empty")
	}
	return s
}

func (f *fields) country(name string) string {
	s := f.str(name)
	if f.err == nil && !(len(s) == 2 && s[0] >= 'A' && s[0] <= 'Z' && s[1] >= 'A' && s[1] <= 'Z') {
		f.fail(name + " must be a two-letter uppercase country code")
	}
	return s
}

func (f *fields) enum(name string, allowed ...string) string {
	s := f.str(name)
	if f.err != nil {
		return ""
	}
	for _, a := range allowed {
		if s == a {
			return s
		}
	}
	f.fail(name + " is not a supported value")
	return ""
}

func (f *fields) uuid(name string) uuid.UUID {
	s := f.str(name)
	if f.err != nil {
		return uuid.Nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		f.fail(name + " must be a UUID")
	}
	return u
}

func (f *fields) nullableUUID(name string) *uuid.UUID {
	v := f.get(name)
	if f.err != nil || isNull(v) {
		return nil
	}
	u := f.uuid(name)
	return &u
}

func (f *fields) integer(name string, minimum int64) int64 {
	v := f.get(name)
	if f.err != nil {
		return 0
	}
	if bytes.ContainsAny(v, ".eE") || !(kind(v) == '-' || isDigit(kind(v))) {
		f.fail(name + " must be an integer")
		return 0
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		f.fail(name + " must be an integer")
		return 0
	}
	if n < minimum {
		f.fail(fmt.Sprintf("%s must be >= %d", name, minimum))
	}
	return n
}

func (f *fields) decimal(name string) Decimal {
	v := f.get(name)
	if f.err != nil {
		return Decimal{}
	}
	var text string
	switch {
	case kind(v) == '"':
		text = f.str(name)
	case kind(v) == '-' || isDigit(kind(v)):
		text = string(v)
	default:
		f.fail(name + " must be a decimal")
		return Decimal{}
	}
	if f.err != nil {
		return Decimal{}
	}
	d, err := ParseDecimal(text)
	if err != nil {
		f.fail(name + " must be a decimal")
	}
	return d
}

func (f *fields) money(name string, strictlyPositive bool) Decimal {
	d := f.decimal(name)
	if f.err != nil {
		return d
	}
	if d.Sign() < 0 || (strictlyPositive && d.Sign() == 0) {
		f.fail(name + " is out of range")
	}
	return d
}

func (f *fields) datetime(name string) time.Time {
	s := f.str(name)
	if f.err != nil {
		return time.Time{}
	}
	t, err := ParseAwareDateTime(s)
	if err != nil {
		f.fail(name + " must be a timezone-aware datetime")
	}
	return t
}

func (f *fields) ip(name string) string {
	s := f.str(name)
	if f.err != nil {
		return ""
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		f.fail(name + " must be an IP address")
		return ""
	}
	return addr.String()
}

func (f *fields) nonEmptyList(name string) []string {
	v := f.get(name)
	if f.err != nil {
		return nil
	}
	if kind(v) != '[' {
		f.fail(name + " must be a list")
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(v, &items); err != nil || len(items) == 0 {
		f.fail(name + " must be a non-empty list")
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if kind(item) != '"' {
			f.fail(name + " items must be strings")
			return nil
		}
		var s string
		if err := json.Unmarshal(item, &s); err != nil {
			f.fail(name + " items must be strings")
			return nil
		}
		s = TrimPythonSpace(s)
		if s == "" {
			f.fail(name + " items must not be empty")
			return nil
		}
		out = append(out, s)
	}
	return out
}
