package main

// TlsCertManager is a passive, READ-ONLY ClickHouse-backed store of:
//   - per-SNI TLS certificates       (mcerts object_type='tls')
//   - the approved-domain allowlist  (site_configs.root_domain, is_active=1)
//   - pending ACME HTTP-01 tokens    (mcerts object_type='acme_challenge')
//
// x (the platform) is the sole writer / ACME client. The tracker only reads,
// refreshing in the background on a ticker. If CH is briefly unreachable it
// keeps serving the last-known-good data (never drops to empty). See
// plans/PLAN-TRACKER-TLS.md.
import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// gTlsCertMgr is set in main once the cert manager has started (nil if CH-backed
// mode is off or failed to start). Used by the telemetry attribution gate.
var gTlsCertMgr *TlsCertManager

// SiteConfig is a single site/tenant row from `site_configs` (the source of
// truth managed by the x admin UI). Cached in-memory for ~5 minutes so the
// tracker never hardcodes tenant domains and never hits CH per request.
type SiteConfig struct {
	OID          string `json:"oid"`
	Org          string `json:"org"`
	SiteName     string `json:"site_name"`
	RootDomain   string `json:"root_domain"`
	TrackerHost  string `json:"tracker_host"`
	TrackerHhash string `json:"tracker_hhash"`
	DomainKey    string `json:"domain_key"`
	SiteURL      string `json:"site_url"`
	IsActive     bool   `json:"is_active"`
	// CMS config (from the config JSON column): the CDN base + the content route
	// prefix. Used by the tr-cluster content fallback (serves canonical from the
	// CDN + CH when x-1 is down).
	CdnCname    string `json:"cdn_cname"`
	RoutePrefix string `json:"route_prefix"`
}

type TlsCertManager struct {
	mu             sync.RWMutex
	conn           clickhouse.Conn
	db             string
	table          string
	certRefresh    time.Duration
	siteTable      string
	siteRefresh    time.Duration

	certs      map[string]*tls.Certificate // domain -> cert
	approved   map[string]struct{}         // approved root domains (lowercased)
	challenges map[string]string           // token -> keyAuthorization
	sites      map[string]SiteConfig       // root_domain (lowercased) -> config
	siteByTrk  map[string]SiteConfig       // tracker_host (lowercased) -> config
	loadedAt   time.Time
	siteLoaded time.Time
	lastErr    error
}

func NewTlsCertManager(db, table string, certRefresh time.Duration, siteTable string, siteRefresh time.Duration) *TlsCertManager {
	if db == "" {
		db = "sfpla"
	}
	if table == "" {
		table = "mcerts"
	}
	if siteTable == "" {
		siteTable = "site_configs"
	}
	if certRefresh <= 0 {
		certRefresh = 30 * time.Second
	}
	if siteRefresh <= 0 {
		siteRefresh = 5 * time.Minute
	}
	return &TlsCertManager{
		db:           db,
		table:        table,
		certRefresh:  certRefresh,
		siteTable:    siteTable,
		siteRefresh:  siteRefresh,
		certs:        map[string]*tls.Certificate{},
		approved:     map[string]struct{}{},
		challenges:   map[string]string{},
		sites:        map[string]SiteConfig{},
		siteByTrk:    map[string]SiteConfig{},
	}
}

// chServiceFromConfig returns the ClickHouse Notify service (or falls back to
// configuration.API, which the init loop sets to the CH service).
func chServiceFromConfig(cfg *Configuration) *Service {
	for i := range cfg.Notify {
		if cfg.Notify[i].Service == SERVICE_TYPE_CLICKHOUSE && !cfg.Notify[i].Skip {
			return &cfg.Notify[i]
		}
	}
	return &cfg.API
}

// connect opens a dedicated CH connection using the CH service credentials.
func (m *TlsCertManager) connect(cfg *Configuration) error {
	svc := chServiceFromConfig(cfg)
	opts := &clickhouse.Options{
		Addr: svc.Hosts,
		Auth: clickhouse.Auth{
			Database: m.db,
			Username: svc.Username,
			Password: svc.Password,
		},
		Settings:        clickhouse.Settings{"max_execution_time": 30},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	}
	if svc.Secure || svc.CACert != "" {
		opts.TLS = &tls.Config{InsecureSkipVerify: !svc.Secure}
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("open ch: %w", err)
	}
	m.conn = conn
	return m.refreshCerts(context.Background())
}

// Start connects and launches the background refreshers: certs (frequent, for
// ACME freshness) + the full site config (~5 min, the site/tenant source of truth).
func (m *TlsCertManager) Start(cfg *Configuration) error {
	if err := m.connect(cfg); err != nil {
		return err
	}
	// Populate before serving: retry a few times so a transient CH blip at
	// startup doesn't leave the cert map empty for a full refresh cycle
	// (which would make the TLS handshake fail -> "bad SSL cert").
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		sErr := m.refreshSites(ctx)
		cErr := m.refreshCerts(ctx)
		cancel()
		if sErr == nil && cErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	// Cert refresh (default 30s) — per-SNI certs + ACME challenges.
	go func() {
		ticker := time.NewTicker(m.certRefresh)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := m.refreshCerts(ctx); err != nil {
				m.mu.Lock()
				m.lastErr = err
				m.mu.Unlock()
			}
			cancel()
		}
	}()
	// Site config refresh (default 5 min) — the full site/tenant config + approved domains.
	go func() {
		ticker := time.NewTicker(m.siteRefresh)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := m.refreshSites(ctx); err != nil {
				m.mu.Lock()
				m.lastErr = err
				m.mu.Unlock()
			}
			cancel()
		}
	}()
	return nil
}

// refreshCerts reloads per-SNI certs + ACME challenges and swaps them in
// atomically (frequent, for ACME freshness).
func (m *TlsCertManager) refreshCerts(ctx context.Context) error {
	if m.conn == nil {
		return fmt.Errorf("not connected")
	}
	newCerts := map[string]*tls.Certificate{}
	newChallenges := map[string]string{}

	// A domain can have multiple rows (e.g. an old staging cert + a new prod
	// cert). Keep only the MOST RECENT (by updated_at) per domain.
	rows, err := m.conn.Query(ctx, "SELECT name, tpub, tpriv, updated_at FROM "+m.db+"."+m.table+" FINAL WHERE object_type='tls'")
	if err != nil {
		return fmt.Errorf("query certs: %w", err)
	}
	latestCertAt := map[string]time.Time{}
	for rows.Next() {
		var name, tpub, tpriv string
		var updatedAt time.Time
		if err := rows.Scan(&name, &tpub, &tpriv, &updatedAt); err != nil {
			continue
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || tpub == "" || tpriv == "" {
			continue
		}
		if cur, ok := latestCertAt[name]; ok && !updatedAt.After(cur) {
			continue // skip older rows for this domain
		}
		cert, cerr := tls.X509KeyPair([]byte(tpub), []byte(tpriv))
		if cerr != nil {
			continue
		}
		latestCertAt[name] = updatedAt
		newCerts[name] = &cert
	}
	rows.Close()

	cRows, err := m.conn.Query(ctx, "SELECT name, skey FROM "+m.db+"."+m.table+" FINAL WHERE object_type='acme_challenge'")
	if err != nil {
		return fmt.Errorf("query challenges: %w", err)
	}
	for cRows.Next() {
		var token, val string
		if err := cRows.Scan(&token, &val); err != nil {
			continue
		}
		if token != "" && val != "" {
			newChallenges[token] = val
		}
	}
	cRows.Close()

	// Don't clear a populated cert map on an empty result (likely a transient
	// CH flake on the distributed table); keep the last-known-good certs.
	if len(newCerts) == 0 {
		m.mu.RLock()
		hadCerts := len(m.certs) > 0
		m.mu.RUnlock()
		if hadCerts {
			return nil
		}
	}

	m.mu.Lock()
	m.certs = newCerts
	m.challenges = newChallenges
	m.loadedAt = time.Now()
	m.lastErr = nil
	m.mu.Unlock()
	return nil
}

// refreshSites reloads the FULL site/tenant config from `site_configs` (the
// source of truth managed by the x admin UI) and caches it in-memory (default
// ~5 min). It also derives the approved-domain allowlist used by the gate.
func (m *TlsCertManager) refreshSites(ctx context.Context) error {
	if m.conn == nil {
		return fmt.Errorf("not connected")
	}
	newSites := map[string]SiteConfig{}
	newByTrk := map[string]SiteConfig{}
	newApproved := map[string]struct{}{}

	rows, err := m.conn.Query(ctx,
		"SELECT toString(oid), org, site_name, root_domain, tracker_host, tracker_hhash, domain_key, site_url, is_active, toString(config) "+
			"FROM "+m.db+"."+m.siteTable+" FINAL WHERE is_active=1 AND root_domain != ''")
	if err != nil {
		fmt.Printf("[sites] refreshSites query error: %v\n", err)
		return fmt.Errorf("query sites: %w", err)
	}
	scanErrs := 0
	for rows.Next() {
		var s SiteConfig
		var active uint8
		var configRaw string
		if err := rows.Scan(&s.OID, &s.Org, &s.SiteName, &s.RootDomain, &s.TrackerHost, &s.TrackerHhash, &s.DomainKey, &s.SiteURL, &active, &configRaw); err != nil {
			if scanErrs == 0 {
				fmt.Printf("[sites] first scan error: %v\n", err)
			}
			scanErrs++
			continue
		}
		s.IsActive = active != 0
		// Parse the config JSON (cdn_cname, route_prefix, brand_tokens, …).
		if len(configRaw) > 0 {
			var cfg struct {
				CdnCname    string `json:"cdn_cname"`
				RoutePrefix string `json:"route_prefix"`
			}
			if err := json.Unmarshal([]byte(configRaw), &cfg); err == nil {
				s.CdnCname = cfg.CdnCname
				s.RoutePrefix = cfg.RoutePrefix
			}
		}
		rd := strings.ToLower(strings.TrimSpace(s.RootDomain))
		th := strings.ToLower(strings.TrimSpace(s.TrackerHost))
		s.RootDomain = rd
		if rd == "" {
			continue
		}
		newSites[rd] = s
		newApproved[rd] = struct{}{}
		if th != "" {
			newByTrk[th] = s
		}
	}
	rows.Close()
	if scanErrs > 0 {
		fmt.Printf("[sites] refreshSites: %d scan errors (sites=%d)\n", scanErrs, len(newSites))
	}

	// Don't drop a populated site/allowlist on an empty (likely transient) read.
	if len(newSites) == 0 {
		m.mu.RLock()
		hadSites := len(m.sites) > 0
		m.mu.RUnlock()
		if hadSites {
			return nil
		}
	}

	m.mu.Lock()
	m.sites = newSites
	m.siteByTrk = newByTrk
	m.approved = newApproved
	m.siteLoaded = time.Now()
	m.mu.Unlock()
	return nil
}

// GetCertificate returns the TLS cert for the SNI, or an error (no cert =>
// the handshake fails, which is the enforcement for unknown hosts).
func (m *TlsCertManager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sni := strings.ToLower(strings.TrimSuffix(hello.ServerName, "."))
	if sni == "" {
		return nil, fmt.Errorf("no SNI")
	}
	if c, ok := m.certs[sni]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("no cert for %q", sni)
}

// IsApproved reports whether d equals an approved root or is a subdomain of one
// (dot-boundary suffix match: evilpet.app approves www.evilpet.app but not not-evilpet.app).
func (m *TlsCertManager) IsApproved(d string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(d), "."))
	if d == "" {
		return false
	}
	for root := range m.approved {
		if d == root || strings.HasSuffix(d, "."+root) {
			return true
		}
	}
	return false
}

// ResolveHost resolves a request host to its cached site/tenant config — by
// tracker_host (tr.<domain>), root_domain, or any subdomain of the root. Returns
// ok=false when the host is unknown (no site config). Backed by the ~5-min cache
// so the tracker serves tenants purely from the DB (no hardcoded domains).
func (m *TlsCertManager) ResolveHost(host string) (SiteConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if h == "" {
		return SiteConfig{}, false
	}
	if s, ok := m.siteByTrk[h]; ok {
		return s, true
	}
	if s, ok := m.sites[h]; ok {
		return s, true
	}
	for root, s := range m.sites {
		if h == root || strings.HasSuffix(h, "."+root) {
			return s, true
		}
	}
	return SiteConfig{}, false
}

// SiteCount returns the number of cached sites (for diagnostics / health).
func (m *TlsCertManager) SiteCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sites)
}

// GetChallenge queries CH directly (fresh) for the ACME keyAuthorization for a
// token. Challenge serving is rare (only during provisioning), so a direct query
// avoids the refresh-cache lag that would cause cross-node 404s (each node would
// otherwise serve only what its own last refresh read).
func (m *TlsCertManager) GetChallenge(token string) (string, bool) {
	if m.conn == nil {
		return "", false
	}
	tok := strings.ReplaceAll(token, "'", "''")
	q := "SELECT skey FROM " + m.db + "." + m.table + " FINAL WHERE name='" + tok + "' AND object_type='acme_challenge' LIMIT 1"
	rows, err := m.conn.Query(context.Background(), q)
	if err != nil {
		return "", false
	}
	defer rows.Close()
	for rows.Next() {
		var skey string
		if err := rows.Scan(&skey); err != nil {
			return "", false
		}
		return skey, true
	}
	return "", false
}
