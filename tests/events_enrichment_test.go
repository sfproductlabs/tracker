package main

// Integration tests for the Go tracker's events enrichment pipeline.
//
// Prereqs (user runs):
//   1. Local ClickHouse on :9000 (TCP) / :8123 (HTTP) with sfpla database + schema loaded.
//   2. Local tracker on :8080 (`cd packages/tracker && ./tracker`).
//
// These tests POST to the tracker and then SELECT from ClickHouse to verify
// the event row is populated correctly. Each test truncates sfpla.events
// first so assertions are deterministic.
//
// Run:
//   cd packages/tracker/tests
//   go test -run TestEventsEnrichment -v .
//
// If the tracker or CH is not running, every sub-test is marked SKIPPED with
// a clear message.

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Defaults match the tracker's config.json (127.0.0.1:9440, TLS).
// Override via env: TRACKER_URL, TRACKER_CH_HOST, TRACKER_CH_SECURE=false, TRACKER_CH_USER, TRACKER_CH_PASS.
var (
	trackerURL     = envOr("TRACKER_URL", "https://localhost:8443")
	clickhouseHost = envOr("TRACKER_CH_HOST", "127.0.0.1:9440")
	clickhouseDB   = envOr("TRACKER_CH_DB", "sfpla")
	clickhouseUser = envOr("TRACKER_CH_USER", "default")
	clickhousePass = envOr("TRACKER_CH_PASS", "")
	clickhouseTLS  = envOr("TRACKER_CH_SECURE", "true") != "false"
)

const (
	// Tracker's BatchFlushInterval is 1s, CH async_insert_busy_timeout is
	// configurable, and there's additional latency from the batch manager's
	// size-based coalescing. Keep wait long to absorb all of that.
	flushWait       = 20 * time.Second
	clientTimeoutMS = 5 * time.Second
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// chHTTP is a minimal ClickHouse query client over the HTTP interface (port 8123).
// Avoids clickhouse-go/v2 stale-session weirdness seen with native TCP.
type chHTTP struct {
	baseURL string
	user    string
	pass    string
	cli     *http.Client
}

func newCHHTTP() *chHTTP {
	base := envOr("TRACKER_CH_HTTP", "http://127.0.0.1:8123")
	return &chHTTP{
		baseURL: base,
		user:    envOr("TRACKER_CH_USER", "default"),
		pass:    envOr("TRACKER_CH_PASS", ""),
		cli: &http.Client{
			Timeout:   5 * time.Second,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
	}
}

// query runs q and returns the first row as a single-column string.
// Empty body / no rows returns ("", nil) — caller distinguishes.
// Disables HTTP keep-alive and query cache so poll loops always see fresh data.
func (c *chHTTP) query(q string) (string, error) {
	// Append settings to bypass any experimental query cache.
	url := c.baseURL + "?use_query_cache=0"
	req, err := http.NewRequest("POST", url, strings.NewReader(q))
	if err != nil {
		return "", err
	}
	req.Close = true // no keep-alive — fresh connection per query
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.cli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("ch %d: %s", resp.StatusCode, string(b))
	}
	return strings.TrimRight(string(b), "\n\r\t "), nil
}

func (c *chHTTP) ping() error {
	_, err := c.query("SELECT 1")
	return err
}

// skipIfOffline pings the tracker and ClickHouse; t.Skip()s if either is down.
func skipIfOffline(t *testing.T) *chHTTP {
	t.Helper()

	// Tracker ping — accept self-signed TLS for local dev.
	httpCli := &http.Client{
		Timeout: clientTimeoutMS,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := httpCli.Get(trackerURL + "/ping")
	if err != nil {
		t.Skipf("tracker not reachable at %s (%v) — start it with `cd packages/tracker && ./tracker`", trackerURL, err)
	}
	resp.Body.Close()

	ch := newCHHTTP()
	if err := ch.ping(); err != nil {
		t.Skipf("ClickHouse unreachable via HTTP (%s): %v", ch.baseURL, err)
	}
	t.Logf("connected to ClickHouse at %s (http)", ch.baseURL)
	return ch
}

// (truncateEvents removed — tests use unique url fragments per run to isolate
// their rows from other events, so a global truncate is unnecessary.)

// postEvent sends a JSON payload to /tr/v1/tr/ with optional headers.
func postEvent(t *testing.T, payload map[string]interface{}, headers map[string]string) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest("POST", trackerURL+"/tr/v1/tr/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	cli := &http.Client{
		Timeout: clientTimeoutMS,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("POST /tr/v1/tr/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("tracker returned %d: %s", resp.StatusCode, string(b))
	}
}

// selectField polls CH for a single column of the most-recent row whose url
// matches the fragment. Returns the stringified value (CH HTTP returns text).
// Filtering by a unique url fragment prevents cross-test contamination when
// the tracker's batch flusher groups events.
func selectField(t *testing.T, ch *chHTTP, col, urlFragment string) string {
	t.Helper()
	pattern := "%" + urlFragment + "%"
	q := fmt.Sprintf("SELECT %s FROM sfpla.events WHERE url LIKE '%s' ORDER BY created_at DESC LIMIT 1", col, pattern)
	deadline := time.Now().Add(flushWait)
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := ch.query(q)
		if err != nil {
			lastErr = err
		} else if out != "" {
			return out
		}
		time.Sleep(100 * time.Millisecond)
		_, _ = ch.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
	}
	t.Fatalf("no event row with url LIKE %q within %v (col=%s): err=%v", pattern, flushWait, col, lastErr)
	return ""
}

// waitForRowByURLFragment blocks until at least one row whose url contains
// the fragment is present in events. Polls every 250ms and flushes async
// inserts on each iteration.
func waitForRowByURLFragment(t *testing.T, ch *chHTTP, urlFragment string) {
	t.Helper()
	pattern := "%" + urlFragment + "%"
	q := fmt.Sprintf("SELECT count() FROM sfpla.events WHERE url LIKE '%s'", pattern)
	deadline := time.Now().Add(flushWait)
	var lastOut string
	var lastErr error
	iter := 0
	for time.Now().Before(deadline) {
		out, err := ch.query(q)
		lastOut, lastErr = out, err
		iter++
		if err == nil && out != "" && out != "0" {
			t.Logf("waitForRowByURLFragment: found after %d iters (%v)", iter, time.Since(deadline.Add(-flushWait)))
			return
		}
		time.Sleep(250 * time.Millisecond)
		_, _ = ch.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
	}
	// Last-resort diagnostic: dump latest 3 urls to help debug.
	dump, _ := ch.query("SELECT url FROM sfpla.events ORDER BY created_at DESC LIMIT 3 FORMAT TabSeparated")
	t.Fatalf("no row with url LIKE %q within %v: lastOut=%q err=%v\nrecent urls:\n%s",
		pattern, flushWait, lastOut, lastErr, dump)
}

// uniqueTag returns a per-test URL fragment so batch-flushed events don't collide.
func uniqueTag(name string) string {
	return fmt.Sprintf("enrich-%s-%d", name, time.Now().UnixNano())
}

// ----- Tests -----

// Note: server lowercases strings like tz and culture before storing
// (see utils.go:cleanString). Assertions compare lowercase form.

func TestEventsEnrichment_DashlessOidRoundTripsAsDashed(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("oid")

	dashless := "54296e9233a311ef802c2915e3776adf"
	postEvent(t, map[string]interface{}{
		"ename": "viewed_test",
		"etyp":  "test",
		"url":   "http://localhost/" + tag,
		"oid":   dashless,
		"vid":   dashless,
	}, nil)
	waitForRowByURLFragment(t, ch, tag)

	wantOid := "54296e92-33a3-11ef-802c-2915e3776adf"
	if got := selectField(t, ch, "toString(oid)", tag); got != wantOid {
		t.Errorf("oid: got %s, want %s", got, wantOid)
	}
	if got := selectField(t, ch, "toString(vid)", tag); got != wantOid {
		t.Errorf("vid: got %s, want %s", got, wantOid)
	}
}

func TestEventsEnrichment_TzPersisted(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("tz")

	postEvent(t, map[string]interface{}{
		"ename": "viewed_test",
		"etyp":  "test",
		"url":   "http://localhost/" + tag,
		"tz":    "America/Los_Angeles",
	}, nil)
	waitForRowByURLFragment(t, ch, tag)

	// Server lowercases strings on write.
	if got := selectField(t, ch, "tz", tag); got != "america/los_angeles" {
		t.Errorf("tz: got %q, want america/los_angeles", got)
	}
}

func TestEventsEnrichment_ViewportPersisted(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("vp")

	postEvent(t, map[string]interface{}{
		"ename": "viewed_test",
		"etyp":  "test",
		"url":   "http://localhost/" + tag,
		"w":     1920,
		"h":     1080,
	}, nil)
	waitForRowByURLFragment(t, ch, tag)

	if got := selectField(t, ch, "vp_w", tag); got != "1920" {
		t.Errorf("vp_w: got %s, want 1920", got)
	}
	if got := selectField(t, ch, "vp_h", tag); got != "1080" {
		t.Errorf("vp_h: got %s, want 1080", got)
	}
}

func TestEventsEnrichment_DeviceOsPersisted(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("dev")

	postEvent(t, map[string]interface{}{
		"ename":  "viewed_test",
		"etyp":   "test",
		"url":    "http://localhost/" + tag,
		"device": "desktop",
		"os":     "macos",
	}, nil)
	waitForRowByURLFragment(t, ch, tag)

	if got := selectField(t, ch, "device", tag); got != "desktop" {
		t.Errorf("device: got %q, want desktop", got)
	}
	if got := selectField(t, ch, "os", tag); got != "macos" {
		t.Errorf("os: got %q, want macos", got)
	}
}

func TestEventsEnrichment_CulturePayloadWinsOverHeader(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("culture")

	postEvent(t, map[string]interface{}{
		"ename":   "viewed_test",
		"etyp":    "test",
		"url":     "http://localhost/" + tag,
		"culture": "fr-FR",
	}, map[string]string{"Accept-Language": "en-US"})
	waitForRowByURLFragment(t, ch, tag)

	// Server lowercases on write; payload should beat Accept-Language header.
	if got := selectField(t, ch, "culture", tag); got != "fr-fr" {
		t.Errorf("culture: got %q, want fr-fr (payload should beat Accept-Language)", got)
	}
}

func TestEventsEnrichment_GeoIPSanFranciscoSpoof(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("geo")

	// User's home IP — geolocated to US / California.
	postEvent(t, map[string]interface{}{
		"ename": "viewed_test",
		"etyp":  "test",
		"url":   "http://localhost/" + tag,
	}, map[string]string{"X-Forwarded-For": "99.100.178.47"})
	waitForRowByURLFragment(t, ch, tag)

	if got := selectField(t, ch, "country", tag); got != "US" {
		t.Errorf("country: got %q, want US", got)
	}
	if region := strings.ToLower(selectField(t, ch, "region", tag)); !strings.Contains(region, "california") {
		t.Errorf("region: got %q, want to contain 'california'", region)
	}
	if city := selectField(t, ch, "city", tag); city == "" {
		t.Errorf("city is empty; expected something non-empty for US-CA IP")
	}
	// IP2Location Lite DB11 resolves 99.100.178.47 → Cupertino, CA (~37.32, -122.03).
	lat, _ := strconv.ParseFloat(selectField(t, ch, "lat", tag), 64)
	lon, _ := strconv.ParseFloat(selectField(t, ch, "lon", tag), 64)
	if lat < 36 || lat > 38 {
		t.Errorf("lat: got %v, want in [36, 38] for US-CA IP", lat)
	}
	if lon < -123 || lon > -121 {
		t.Errorf("lon: got %v, want in [-123, -121] for US-CA IP", lon)
	}
}

func TestEventsEnrichment_GeoIPPrivateSkipped(t *testing.T) {
	ch := skipIfOffline(t)
	tag := uniqueTag("priv")

	postEvent(t, map[string]interface{}{
		"ename": "viewed_test",
		"etyp":  "test",
		"url":   "http://localhost/" + tag,
	}, map[string]string{"X-Forwarded-For": "127.0.0.1"})
	waitForRowByURLFragment(t, ch, tag)

	// country/city fields in CH HTTP output are empty strings for skipped geo.
	// selectField returns "" when the column value is literally empty.
	q := fmt.Sprintf("SELECT country, city FROM sfpla.events WHERE url LIKE '%%%s%%' ORDER BY created_at DESC LIMIT 1", tag)
	out, err := ch.query(q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// HTTP interface returns tab-separated. Both should be empty.
	if out != "\t" && out != "" {
		t.Errorf("private-IP geo should be empty, got %q", out)
	}
}

// TestMain doesn't skip the suite — we want each sub-test to skip
// independently if the stack is down, and produce a clear message.
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
