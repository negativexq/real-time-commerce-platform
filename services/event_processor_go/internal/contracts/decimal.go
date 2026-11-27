package contracts

import (
	"errors"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// Decimal is an immutable arbitrary-precision decimal that reproduces the
// observable behaviour of Python's decimal.Decimal for the operations the
// processor needs: lossless parsing, str() formatting (which feeds the
// canonical event hash), exact add/sub/mul, numeric comparison, and division
// rounded to the default 28-digit ROUND_HALF_EVEN context.
//
// value = (-1)^neg * coef * 10^exp, coef >= 0. The coefficient is kept
// exactly as parsed (trailing zeros preserved) so String() matches Python.
type Decimal struct {
	neg  bool
	coef *big.Int
	exp  int32
}

var (
	bigTen      = big.NewInt(10)
	errDecimal  = errors.New("invalid decimal")
	decimalZero = Decimal{coef: new(big.Int)}
)

// DecimalFromInt returns Decimal(n).
func DecimalFromInt(n int64) Decimal {
	d := Decimal{coef: new(big.Int).SetInt64(n)}
	if n < 0 {
		d.neg = true
		d.coef.Neg(d.coef)
	}
	return d
}

// MustDecimal parses a literal known to be valid (configuration defaults).
func MustDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// ParseDecimal accepts the same finite syntax as Python's Decimal(str):
// surrounding whitespace, optional sign, digits with an optional fraction,
// optional exponent, and PEP 515 underscores between digits. Infinity and
// NaN are rejected (pydantic disallows them for Decimal fields).
func ParseDecimal(raw string) (Decimal, error) {
	s := TrimPythonSpace(raw)
	if s == "" {
		return Decimal{}, errDecimal
	}
	neg := false
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		neg = true
		s = s[1:]
	}
	mantissa, exponent := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mantissa, exponent = s[:i], s[i+1:]
	}
	intPart, fracPart := mantissa, ""
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		intPart, fracPart = mantissa[:i], mantissa[i+1:]
	}
	intDigits, ok := stripDigitGroups(intPart)
	if !ok {
		return Decimal{}, errDecimal
	}
	fracDigits, ok := stripDigitGroups(fracPart)
	if !ok {
		return Decimal{}, errDecimal
	}
	if intDigits == "" && fracDigits == "" {
		return Decimal{}, errDecimal
	}
	exp := int64(0)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		expDigits := exponent
		sign := int64(1)
		if expDigits != "" && (expDigits[0] == '+' || expDigits[0] == '-') {
			if expDigits[0] == '-' {
				sign = -1
			}
			expDigits = expDigits[1:]
		}
		clean, ok := stripDigitGroups(expDigits)
		if !ok || clean == "" {
			return Decimal{}, errDecimal
		}
		value, err := strconv.ParseInt(clean, 10, 32)
		if err != nil {
			return Decimal{}, errDecimal
		}
		exp = sign * value
	}
	exp -= int64(len(fracDigits))
	if exp < -1<<30 || exp > 1<<30 {
		return Decimal{}, errDecimal
	}
	coef, ok := new(big.Int).SetString(intDigits+fracDigits, 10)
	if !ok {
		return Decimal{}, errDecimal
	}
	return Decimal{neg: neg, coef: coef, exp: int32(exp)}, nil
}

// stripDigitGroups validates ASCII digits with single underscores strictly
// between digits and returns the digits alone.
func stripDigitGroups(s string) (string, bool) {
	if s == "" {
		return "", true
	}
	if strings.IndexByte(s, '_') < 0 {
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return "", false
			}
		}
		return s, true
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '_' && i > 0 && i < len(s)-1 && isDigit(s[i-1]) && isDigit(s[i+1]):
		default:
			return "", false
		}
	}
	return b.String(), true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// String reproduces Python's Decimal.__str__ (scientific when needed).
func (d Decimal) String() string {
	digits := d.coefficient().String()
	exp := int(d.exp)
	leftDigits := exp + len(digits)
	var dotPlace int
	if exp <= 0 && leftDigits > -6 {
		dotPlace = leftDigits
	} else {
		dotPlace = 1
	}
	var intPart, fracPart string
	switch {
	case dotPlace <= 0:
		intPart = "0"
		fracPart = "." + strings.Repeat("0", -dotPlace) + digits
	case dotPlace >= len(digits):
		intPart = digits + strings.Repeat("0", dotPlace-len(digits))
	default:
		intPart = digits[:dotPlace]
		fracPart = "." + digits[dotPlace:]
	}
	suffix := ""
	if leftDigits != dotPlace {
		suffix = "E" + strconv.FormatInt(int64(leftDigits-dotPlace), 10)
		if leftDigits-dotPlace >= 0 {
			suffix = "E+" + strconv.FormatInt(int64(leftDigits-dotPlace), 10)
		}
	}
	sign := ""
	if d.neg {
		sign = "-"
	}
	return sign + intPart + fracPart + suffix
}

func (d Decimal) coefficient() *big.Int {
	if d.coef == nil {
		return decimalZero.coef
	}
	return d.coef
}

func (d Decimal) signed() *big.Int {
	v := new(big.Int).Set(d.coefficient())
	if d.neg {
		v.Neg(v)
	}
	return v
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(bigTen, big.NewInt(int64(n)), nil)
}

// aligned returns both signed coefficients scaled to the smaller exponent.
func aligned(a, b Decimal) (*big.Int, *big.Int, int32) {
	x, y := a.signed(), b.signed()
	switch {
	case a.exp > b.exp:
		x.Mul(x, pow10(a.exp-b.exp))
		return x, y, b.exp
	case b.exp > a.exp:
		y.Mul(y, pow10(b.exp-a.exp))
		return x, y, a.exp
	}
	return x, y, a.exp
}

func fromSigned(v *big.Int, exp int32) Decimal {
	d := Decimal{coef: new(big.Int).Abs(v), exp: exp}
	d.neg = v.Sign() < 0
	return d
}

// Cmp compares numerically, like Python Decimal ordering (scale-insensitive).
func (d Decimal) Cmp(other Decimal) int {
	x, y, _ := aligned(d, other)
	return x.Cmp(y)
}

func (d Decimal) Equal(other Decimal) bool { return d.Cmp(other) == 0 }

func (d Decimal) Sign() int {
	if d.coefficient().Sign() == 0 {
		return 0
	}
	if d.neg {
		return -1
	}
	return 1
}

func (d Decimal) IsZero() bool { return d.Sign() == 0 }

func (d Decimal) Add(other Decimal) Decimal {
	x, y, exp := aligned(d, other)
	return fromSigned(x.Add(x, y), exp)
}

func (d Decimal) Sub(other Decimal) Decimal {
	x, y, exp := aligned(d, other)
	return fromSigned(x.Sub(x, y), exp)
}

func (d Decimal) Mul(other Decimal) Decimal {
	v := d.signed()
	v.Mul(v, other.signed())
	return fromSigned(v, d.exp+other.exp)
}

func (d Decimal) MulInt(n int64) Decimal { return d.Mul(DecimalFromInt(n)) }

const pythonContextPrecision = 28

// Div returns d / other rounded to Python's default context (28 significant
// digits, ROUND_HALF_EVEN). Only the numeric value is meaningful; the
// processor never formats a quotient.
func (d Decimal) Div(other Decimal) (Decimal, error) {
	if other.IsZero() {
		return Decimal{}, errors.New("decimal division by zero")
	}
	if d.IsZero() {
		return Decimal{coef: new(big.Int)}, nil
	}
	num := new(big.Int).Set(d.coefficient())
	den := new(big.Int).Set(other.coefficient())
	// value = num/den * 10^shift
	shift := d.exp - other.exp
	negative := d.neg != other.neg
	// Choose e so that 10^(p-1) <= num/den * 10^(shift-e) < 10^p.
	estimate := int32(len(num.String())-len(den.String())) + shift
	e := estimate - pythonContextPrecision
	lower := pow10(pythonContextPrecision - 1)
	upper := pow10(pythonContextPrecision)
	scaled := func(e int32) (*big.Int, *big.Int) {
		n := new(big.Int).Set(num)
		m := new(big.Int).Set(den)
		k := shift - e
		if k >= 0 {
			n.Mul(n, pow10(k))
		} else {
			m.Mul(m, pow10(-k))
		}
		return n, m
	}
	for i := 0; i < 4; i++ {
		n, m := scaled(e)
		q := new(big.Int).Quo(n, m)
		if q.Cmp(upper) >= 0 {
			e++
			continue
		}
		if q.Cmp(lower) < 0 {
			e--
			continue
		}
		break
	}
	n, m := scaled(e)
	q, r := new(big.Int).QuoRem(n, m, new(big.Int))
	twice := new(big.Int).Lsh(r, 1)
	switch twice.Cmp(m) {
	case 1:
		q.Add(q, big.NewInt(1))
	case 0:
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(1))
		}
	}
	if q.Cmp(upper) >= 0 {
		q.Quo(q, bigTen)
		e++
	}
	return Decimal{neg: negative, coef: q, exp: e}, nil
}

// DecimalFromNumeric converts a PostgreSQL NUMERIC scanned by pgx.
func DecimalFromNumeric(n pgtype.Numeric) (Decimal, error) {
	if !n.Valid || n.NaN || n.InfinityModifier != pgtype.Finite || n.Int == nil {
		return Decimal{}, errors.New("numeric is not a finite value")
	}
	return fromSigned(n.Int, n.Exp), nil
}

// NumericValue lets pgx encode a Decimal parameter losslessly.
func (d Decimal) NumericValue() (pgtype.Numeric, error) {
	return pgtype.Numeric{Int: d.signed(), Exp: d.exp, Valid: true}, nil
}
