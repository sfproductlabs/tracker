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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// gTlsCertMgr is set in main once the cert manager has started (nil if CH-backed
// mode is off or failed to start). Used by the telemetry attribution gate.
var gTlsCertMgr *TlsCertManager

type TlsCertManager struct {
	mu           sync.RWMutex
	conn         clickhouse.Conn
	db           string
	table        string
	refreshEvery time.Duration

	certs      map[string]*tls.Certificate // domain -> cert
	approved   map[string]struct{}         // approved root domains (lowercased)
	challenges map[string]string           // token -> keyAuthorization
	loadedAt   time.Time
	lastErr    error
}

func NewTlsCertManager(db, table string, refresh time.Duration) *TlsCertManager {
	if db == "" {
		db = "sfpla"
	}
	if table == "" {
		table = "mcerts"
	}
	if refresh <= 0 {
		refresh = 30 * time.Second
	}
	return &TlsCertManager{
		db:           db,
		table:        table,
		refreshEvery: refresh,
		certs:        map[string]*tls.Certificate{},
		approved:     map[string]struct{}{},
		challenges:   map[string]string{},
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
	return m.refresh(context.Background())
}

// Start connects and launches the background refresher.
func (m *TlsCertManager) Start(cfg *Configuration) error {
	if err := m.connect(cfg); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(m.refreshEvery)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := m.refresh(ctx)
			cancel()
			if err != nil {
				m.mu.Lock()
				m.lastErr = err
				m.mu.Unlock()
			}
		}
	}()
	return nil
}

// refresh reloads certs + approved roots + challenges and swaps them in atomically.
func (m *TlsCertManager) refresh(ctx context.Context) error {
	if m.conn == nil {
		return fmt.Errorf("not connected")
	}
	newCerts := map[string]*tls.Certificate{}
	newApproved := map[string]struct{}{}
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

	aRows, err := m.conn.Query(ctx, "SELECT DISTINCT root_domain FROM "+m.db+".site_configs WHERE is_active=1 AND root_domain != ''")
	if err != nil {
		return fmt.Errorf("query approved: %w", err)
	}
	for aRows.Next() {
		var rd string
		if err := aRows.Scan(&rd); err != nil {
			continue
		}
		rd = strings.ToLower(strings.TrimSpace(rd))
		if rd != "" {
			newApproved[rd] = struct{}{}
		}
	}
	aRows.Close()

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

	m.mu.Lock()
	m.certs = newCerts
	m.approved = newApproved
	m.challenges = newChallenges
	m.loadedAt = time.Now()
	m.lastErr = nil
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
