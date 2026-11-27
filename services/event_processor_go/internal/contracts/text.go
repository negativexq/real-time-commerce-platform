package contracts

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// isPythonSpace mirrors str.isspace() for str.strip() without arguments.
func isPythonSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', 0x1c, 0x1d, 0x1e, 0x1f, ' ', 0x85, 0xa0,
		0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// TrimPythonSpace reproduces Python's str.strip().
func TrimPythonSpace(s string) string {
	return strings.TrimFunc(s, isPythonSpace)
}

var errDateTime = errors.New("invalid datetime")

// ParseAwareDateTime accepts the RFC 3339 / ISO 8601 subset pydantic's
// strict AwareDatetime accepts from JSON strings and normalizes to UTC with
// microsecond precision. Naive values are rejected.
func ParseAwareDateTime(s string) (time.Time, error) {
	if len(s) < 16 || !utf8.ValidString(s) {
		return time.Time{}, errDateTime
	}
	num := func(from, to int) (int, bool) {
		if to > len(s) {
			return 0, false
		}
		v := 0
		for i := from; i < to; i++ {
			if !isDigit(s[i]) {
				return 0, false
			}
			v = v*10 + int(s[i]-'0')
		}
		return v, true
	}
	year, ok1 := num(0, 4)
	month, ok2 := num(5, 7)
	day, ok3 := num(8, 10)
	if !ok1 || !ok2 || !ok3 || s[4] != '-' || s[7] != '-' {
		return time.Time{}, errDateTime
	}
	switch s[10] {
	case 'T', 't', ' ', '_':
	default:
		return time.Time{}, errDateTime
	}
	hour, ok4 := num(11, 13)
	minute, ok5 := num(14, 16)
	if !ok4 || !ok5 || s[13] != ':' {
		return time.Time{}, errDateTime
	}
	pos := 16
	second, micro := 0, 0
	if pos < len(s) && s[pos] == ':' {
		var ok bool
		second, ok = num(pos+1, pos+3)
		if !ok {
			return time.Time{}, errDateTime
		}
		pos += 3
		if pos < len(s) && (s[pos] == '.' || s[pos] == ',') {
			pos++
			start := pos
			for pos < len(s) && isDigit(s[pos]) {
				pos++
			}
			digits := s[start:pos]
			if digits == "" {
				return time.Time{}, errDateTime
			}
			if len(digits) > 6 {
				digits = digits[:6]
			}
			v, _ := strconv.Atoi(digits)
			for i := len(digits); i < 6; i++ {
				v *= 10
			}
			micro = v
		}
	}
	if pos >= len(s) {
		return time.Time{}, errDateTime // naive datetime
	}
	offset := 0
	switch s[pos] {
	case 'Z', 'z':
		pos++
	case '+', '-':
		sign := 1
		if s[pos] == '-' {
			sign = -1
		}
		pos++
		oh, ok := num(pos, pos+2)
		if !ok {
			return time.Time{}, errDateTime
		}
		pos += 2
		om := 0
		if pos < len(s) {
			if s[pos] == ':' {
				pos++
			}
			var ok bool
			om, ok = num(pos, pos+2)
			if !ok {
				return time.Time{}, errDateTime
			}
			pos += 2
		}
		if oh > 23 || om > 59 {
			return time.Time{}, errDateTime
		}
		offset = sign * (oh*3600 + om*60)
	default:
		return time.Time{}, errDateTime
	}
	if pos != len(s) {
		return time.Time{}, errDateTime
	}
	if year < 1 || month < 1 || month > 12 || day < 1 || day > daysIn(year, month) ||
		hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, errDateTime
	}
	loc := time.UTC
	if offset != 0 {
		loc = time.FixedZone("", offset)
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, loc).UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return time.Time{}, errDateTime
	}
	return t, nil
}

func daysIn(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// FormatUTCZ reproduces shared.commerce_common.clock.serialize_utc:
// isoformat() with an explicit Z, microseconds only when non-zero.
func FormatUTCZ(t time.Time) string {
	return formatISO(t.UTC(), "Z")
}

// FormatISOOffset reproduces datetime.isoformat() for an aware UTC value.
func FormatISOOffset(t time.Time) string {
	return formatISO(t.UTC(), "+00:00")
}

func formatISO(t time.Time, suffix string) string {
	b := make([]byte, 0, 32)
	b = t.AppendFormat(b, "2006-01-02T15:04:05")
	if micro := t.Nanosecond() / 1000; micro != 0 {
		b = append(b, '.')
		b = appendPadded(b, micro, 6)
	}
	return string(append(b, suffix...))
}

func appendPadded(b []byte, v, width int) []byte {
	s := strconv.Itoa(v)
	for i := len(s); i < width; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}
