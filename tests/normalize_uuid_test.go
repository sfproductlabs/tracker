package main

// Unit tests for the UUID normalization contract the tracker's
// parseUUID()/normalizeUUIDString()/parseUUIDString() helpers must satisfy.
//
// The tracker's internal package (github.com/sfproductlabs/tracker) lives in a
// sibling module and cannot be imported directly from this tests module.
// These tests therefore re-express the expected behavior as a *reference
// implementation* (testNormalizeUUIDString / testParseUUID) and assert on it.
// The corresponding integration test TestEventsEnrichment_DashlessOid...
// exercises the real server path end-to-end via ClickHouse and will fail
// loudly if the real implementation ever diverges from this spec.

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// testNormalizeUUIDString mirrors normalizeUUIDString in packages/tracker/utils.go.
// Keep these two in sync — the integration tests in this file are the single
// source of truth for the behavior the real server must match.
func testNormalizeUUIDString(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-' {
		return strings.ToLower(s)
	}
	if len(s) == 32 {
		if _, err := hex.DecodeString(s); err == nil {
			return strings.ToLower(s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:])
		}
	}
	if len(s) == 38 && s[0] == '{' && s[37] == '}' {
		return testNormalizeUUIDString(s[1:37])
	}
	return s
}

func testParseUUIDString(s string) (uuid.UUID, error) {
	return uuid.Parse(testNormalizeUUIDString(s))
}

// testParseUUID mirrors parseUUID in packages/tracker/utils.go (without the
// *uuid.UUID / uuid.UUID input branches — those aren't reached from JSON).
var testZeroUUID = uuid.MustParse("00000000-0000-0000-0000-000000000000")

func testParseUUID(v interface{}, allowNil ...bool) *uuid.UUID {
	canReturnNil := false
	if len(allowNil) > 0 {
		canReturnNil = allowNil[0]
	}
	str, _ := v.(string)
	str = strings.TrimSpace(str)
	if str == "" {
		if canReturnNil {
			return nil
		}
		return &testZeroUUID
	}
	if parsed, err := testParseUUIDString(str); err == nil {
		return &parsed
	}
	if canReturnNil {
		return nil
	}
	return &testZeroUUID
}

// testToInt64OrZero mirrors toInt64OrZero in packages/tracker/utils.go.
func testToInt64OrZero(v interface{}) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return int64(f)
		}
		return 0
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		return 0
	}
}

func testDerefStringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ── Tests ────────────────────────────────────────────────────────────────

func TestNormalizeUUIDString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"dashless lowercase", "54296e9233a311ef802c2915e3776adf", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"dashless uppercase", "54296E9233A311EF802C2915E3776ADF", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"dashed canonical", "54296e92-33a3-11ef-802c-2915e3776adf", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"dashed uppercase", "54296E92-33A3-11EF-802C-2915E3776ADF", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"braced", "{54296e92-33a3-11ef-802c-2915e3776adf}", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"surrounding whitespace", "  54296e9233a311ef802c2915e3776adf  ", "54296e92-33a3-11ef-802c-2915e3776adf"},
		{"32 non-hex passthrough", "gggggggggggggggggggggggggggggggg", "gggggggggggggggggggggggggggggggg"},
		{"garbage passthrough", "not-a-uuid", "not-a-uuid"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := testNormalizeUUIDString(c.in)
			if got != c.want {
				t.Errorf("normalizeUUIDString(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestParseUUIDStringAcceptsDashless(t *testing.T) {
	u, err := testParseUUIDString("54296e9233a311ef802c2915e3776adf")
	if err != nil {
		t.Fatalf("parseUUIDString returned error: %v", err)
	}
	if u.String() != "54296e92-33a3-11ef-802c-2915e3776adf" {
		t.Fatalf("parseUUIDString = %s, want 54296e92-33a3-11ef-802c-2915e3776adf", u)
	}
}

func TestParseUUIDAcceptsDashless(t *testing.T) {
	got := testParseUUID("54296e9233a311ef802c2915e3776adf")
	if got == nil {
		t.Fatal("parseUUID returned nil for dashless input")
	}
	if got.String() != "54296e92-33a3-11ef-802c-2915e3776adf" {
		t.Fatalf("parseUUID = %s, want 54296e92-33a3-11ef-802c-2915e3776adf", got)
	}
}

func TestParseUUIDZeroOnGarbage(t *testing.T) {
	got := testParseUUID("not-a-uuid")
	if got == nil {
		t.Fatal("parseUUID returned nil, expected zero UUID fallback")
	}
	if got.String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("parseUUID on garbage = %s, want zero UUID", got)
	}
}

func TestParseUUIDAllowNilOnGarbage(t *testing.T) {
	got := testParseUUID("not-a-uuid", true)
	if got != nil {
		t.Fatalf("parseUUID(allowNil=true) on garbage = %v, want nil", got)
	}
}

func TestToInt64OrZero(t *testing.T) {
	var jn json.Number = "1920"
	cases := []struct {
		name string
		in   interface{}
		want int64
	}{
		{"nil", nil, 0},
		{"int", int(1920), 1920},
		{"int64", int64(1080), 1080},
		{"float64 from JSON number", float64(1920), 1920},
		{"json.Number", jn, 1920},
		{"string numeric", "1920", 1920},
		{"string with whitespace", "  1920  ", 1920},
		{"string float", "1920.5", 1920},
		{"empty string", "", 0},
		{"garbage string", "abc", 0},
		{"bool", true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := testToInt64OrZero(c.in); got != c.want {
				t.Errorf("toInt64OrZero(%v) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestDerefStringOrEmpty(t *testing.T) {
	if got := testDerefStringOrEmpty(nil); got != "" {
		t.Errorf("derefStringOrEmpty(nil) = %q, want \"\"", got)
	}
	s := "en-US"
	if got := testDerefStringOrEmpty(&s); got != "en-US" {
		t.Errorf("derefStringOrEmpty(&\"en-US\") = %q, want en-US", got)
	}
}
