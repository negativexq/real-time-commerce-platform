package contracts

import (
	"encoding/json"
	"os"
	"testing"
)

type goldenCase struct {
	Name      string `json:"name"`
	Input     string `json:"input"`
	OK        bool   `json:"ok"`
	Canonical string `json:"canonical"`
	SHA256    string `json:"sha256"`
	Key       string `json:"key"`
}

// TestGoldenParityWithPython replays inputs whose acceptance, canonical JSON
// and hash were recorded from the Python reference (testdata/golden.json).
func TestGoldenParityWithPython(t *testing.T) {
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			event, err := ParseEvent([]byte(tc.Input))
			if !tc.OK {
				if err == nil {
					t.Fatalf("Python rejected this input but Go accepted it: %s", event.CanonicalJSON())
				}
				return
			}
			if err != nil {
				t.Fatalf("Python accepted this input but Go rejected it: %v", err)
			}
			if got := string(event.CanonicalJSON()); got != tc.Canonical {
				t.Fatalf("canonical mismatch\n go: %s\n py: %s", got, tc.Canonical)
			}
			if got := event.PayloadSHA256(); got != tc.SHA256 {
				t.Fatalf("sha256 mismatch: %s != %s", got, tc.SHA256)
			}
			if got := string(event.MessageKey()); got != tc.Key {
				t.Fatalf("key mismatch: %s != %s", got, tc.Key)
			}
		})
	}
}

func TestDecimalStringMatchesPython(t *testing.T) {
	cases := map[string]string{
		"119.90":    "119.90",
		"007.50":    "7.50",
		"0.00":      "0.00",
		"-0":        "-0",
		"1E+3":      "1E+3",
		"1e-7":      "1E-7",
		"0.0000001": "1E-7",
		"0.000001":  "0.000001",
		"100":       "100",
		"1.23E+5":   "1.23E+5",
		" 5.0 ":     "5.0",
		"1_000.5":   "1000.5",
		".5":        "0.5",
		"5.":        "5",
	}
	for in, want := range cases {
		d, err := ParseDecimal(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got := d.String(); got != want {
			t.Errorf("%q: got %s want %s", in, got, want)
		}
	}
	for _, bad := range []string{"", "NaN", "Infinity", "1__0", "_1", "1e", "--1", "1.2.3", "abc"} {
		if _, err := ParseDecimal(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestDecimalArithmetic(t *testing.T) {
	d := func(s string) Decimal { return MustDecimal(s) }
	if got := d("10.00").Sub(d("1.5")).String(); got != "8.50" {
		t.Errorf("sub: %s", got)
	}
	if got := d("19.99").MulInt(3).String(); got != "59.97" {
		t.Errorf("mul: %s", got)
	}
	if !d("10.5").Equal(d("10.500000000000000000")) {
		t.Error("scale-insensitive equality failed")
	}
	q, err := d("10").Div(d("3"))
	if err != nil {
		t.Fatal(err)
	}
	if got := q.String(); got != "3.333333333333333333333333333" {
		t.Errorf("div: %s", got)
	}
	q, _ = d("2").Div(d("3"))
	if got := q.String(); got != "0.6666666666666666666666666667" {
		t.Errorf("div rounding: %s", got)
	}
	q, _ = d("300").Div(d("3"))
	if !q.Equal(d("100")) {
		t.Errorf("exact div: %s", q)
	}
}
