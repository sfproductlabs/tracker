package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// TestService wraps ClickHouse connection
type TestService struct {
	Session clickhouse.Conn
	Config  *Service
	AppCfg  *Configuration
}

func (s *TestService) connect() error {
	opts := &clickhouse.Options{
		Addr: s.Config.Hosts,
		Auth: clickhouse.Auth{
			Database: s.Config.Context,
			Username: s.Config.Username,
			Password: s.Config.Password,
		},
		Settings: clickhouse.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   10000000,
			"async_insert_busy_timeout_ms": s.Config.BatchFlushInterval,
		},
		Debug:           s.AppCfg.Debug,
		DialTimeout:     time.Duration(s.Config.Timeout) * time.Second,
		MaxOpenConns:    s.Config.Connections,
		MaxIdleConns:    s.Config.Connections,
		ConnMaxLifetime: time.Hour,
	}

	if s.Config.EnableCompression {
		opts.Settings["enable_http_compression"] = 1
		opts.Settings["http_zlib_compression_level"] = 1
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return err
	}

	s.Session = conn
	return nil
}

// TestComprehensiveClickhouse tests ALL tables used in clickhouse.go
func TestComprehensiveClickhouse(t *testing.T) {
	// Change to parent directory to access config
	originalDir, _ := os.Getwd()
	err := os.Chdir("..")
	if err != nil {
		t.Fatalf("Failed to change to tracker directory: %v", err)
	}
	defer os.Chdir(originalDir)

	// Load configuration
	config, err := loadConfiguration()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Find ClickHouse service
	var clickhouseConfig *Service
	for i, service := range config.Notify {
		if service.Service == "clickhouse" {
			clickhouseConfig = &config.Notify[i]
			break
		}
	}

	if clickhouseConfig == nil {
		t.Skip("ClickHouse service not found in configuration - skipping integration test")
	}

	// Initialize test service
	testService := &TestService{
		Config: clickhouseConfig,
		AppCfg: config,
	}

	// Connect to ClickHouse
	err = testService.connect()
	if err != nil {
		t.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer testService.Session.Close()

	t.Log("✓ Connected to ClickHouse successfully")

	// Test shared identifiers
	testOID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	testUID := uuid.New()
	testVID := uuid.New()
	testSID := uuid.New()
	testTID := uuid.New()

	// Run comprehensive tests for all tables
	t.Run("Analytics", func(t *testing.T) {
		testAnalyticsTables(t, testService, testOID, testVID, testSID, testUID)
	})

	t.Run("Messaging", func(t *testing.T) {
		testMessagingTables(t, testService, testOID, testTID, testVID, testUID)
	})

	t.Run("Compliance", func(t *testing.T) {
		testComplianceTables(t, testService, testOID, testVID, testSID)
	})

	t.Run("Auth", func(t *testing.T) {
		testAuthTables(t, testService, testOID)
	})

	t.Run("Core", func(t *testing.T) {
		testCoreTables(t, testService, testOID, testVID, testSID)
	})

	t.Run("Users", func(t *testing.T) {
		testUsersTables(t, testService, testOID, testUID, testVID, testSID)
	})

	t.Run("VisitorInterests", func(t *testing.T) {
		testVisitorInterestsTables(t, testService, testOID, testVID)
	})

	t.Log("✓ All comprehensive tests completed")

	// Verification: Flush async inserts and verify data persisted
	t.Run("Verification", func(t *testing.T) {
		ctx := context.Background()

		// Wait a moment for async inserts to buffer
		time.Sleep(2 * time.Second)

		// Force flush of async insert queue
		t.Log("Flushing async insert queue...")
		err := testService.Session.Exec(ctx, "SYSTEM FLUSH ASYNC INSERT QUEUE")
		if err != nil {
			t.Errorf("Failed to flush async insert queue: %v", err)
		}

		// Wait for flush to complete
		time.Sleep(1 * time.Second)

		// Verify all tables have data (using FINAL for ReplicatedReplacingMergeTree)
		tables := []string{
			"events", "mthreads", "mstore", "mtriage", "redirects",
			"impression_daily", "agreements", "agreed", "payments", "visitor_interests",
		}

		t.Log("Verifying data persistence...")
		for _, table := range tables {
			var count uint64
			query := fmt.Sprintf("SELECT COUNT(*) FROM sfpla.%s FINAL", table)
			err := testService.Session.QueryRow(ctx, query).Scan(&count)
			if err != nil {
				t.Errorf("Failed to query %s: %v", table, err)
				continue
			}

			if count > 0 {
				t.Logf("✓ %s: %d rows", table, count)
			} else {
				t.Errorf("✗ %s: 0 rows (expected > 0)", table)
			}
		}
	})
}

// Analytics Tables: events, hits, referrers, browsers, usernames, emails, cells, reqs, ips
func testAnalyticsTables(t *testing.T, s *TestService, oid, vid, sid, uid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test events table
	t.Run("events", func(t *testing.T) {
		eid := uuid.New()
		err := s.Session.Exec(ctx, `INSERT INTO events (
			eid, vid, sid, oid, org, created_at, uid,
			url, ip, source, medium, campaign, country, region, city, lat, lon,
			app, ver, ename, params
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			eid, vid, sid, oid, "test_org", now, uid,
			"https://test.com/events", "192.168.1.1", "google", "cpc", "test_campaign",
			"US", "CA", "SF", 37.77, -122.41, "test_app", 1, "page_view",
			`{"test": "data"}`,
		)
		if err != nil {
			t.Errorf("events insert failed: %v", err)
		} else {
			t.Logf("✓ events: %s", eid.String()[:8])
		}
	})

	// Test hits table
	t.Run("hits", func(t *testing.T) {
		hhash := fmt.Sprintf("hash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO hits (hhash, url, total, date) VALUES (?, ?, ?, ?)`,
			hhash, "https://test.com/hit", 1, now)
		if err != nil {
			t.Errorf("hits insert failed: %v", err)
		} else {
			t.Logf("✓ hits: %s", hhash[:8])
		}
	})

	// Test ips table
	t.Run("ips", func(t *testing.T) {
		hhash := fmt.Sprintf("iphash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO ips (hhash, ip, total, date) VALUES (?, ?, ?, ?)`,
			hhash, "203.0.113.1", 1, now)
		if err != nil {
			t.Errorf("ips insert failed: %v", err)
		} else {
			t.Logf("✓ ips: %s", hhash[:8])
		}
	})

	// Test referrers table
	t.Run("referrers", func(t *testing.T) {
		hhash := fmt.Sprintf("refhash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO referrers (hhash, url, total, date) VALUES (?, ?, ?, ?)`,
			hhash, "https://google.com", 1, now)
		if err != nil {
			t.Errorf("referrers insert failed: %v", err)
		} else {
			t.Logf("✓ referrers: %s", hhash[:8])
		}
	})

	// Test browsers table
	t.Run("browsers", func(t *testing.T) {
		hhash := fmt.Sprintf("browserhash_%d", time.Now().UnixNano())
		bhash := fmt.Sprintf("bhash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO browsers (hhash, bhash, browser, total, date) VALUES (?, ?, ?, ?, ?)`,
			hhash, bhash, "Chrome/120.0", 1, now)
		if err != nil {
			t.Errorf("browsers insert failed: %v", err)
		} else {
			t.Logf("✓ browsers: %s", bhash[:8])
		}
	})

	// Test usernames table
	t.Run("usernames", func(t *testing.T) {
		hhash := fmt.Sprintf("uhash_%d", time.Now().UnixNano())
		uhash := fmt.Sprintf("username_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO usernames (hhash, uhash, vid, sid) VALUES (?, ?, ?, ?)`,
			hhash, uhash, vid, sid)
		if err != nil {
			t.Errorf("usernames insert failed: %v", err)
		} else {
			t.Logf("✓ usernames: %s", uhash[:8])
		}
	})

	// Test emails table
	t.Run("emails", func(t *testing.T) {
		hhash := fmt.Sprintf("ehash_%d", time.Now().UnixNano())
		ehash := fmt.Sprintf("email_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO emails (hhash, ehash, vid, sid) VALUES (?, ?, ?, ?)`,
			hhash, ehash, vid, sid)
		if err != nil {
			t.Errorf("emails insert failed: %v", err)
		} else {
			t.Logf("✓ emails: %s", ehash[:8])
		}
	})

	// Test cells table
	t.Run("cells", func(t *testing.T) {
		hhash := fmt.Sprintf("chash_%d", time.Now().UnixNano())
		chash := fmt.Sprintf("cell_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO cells (hhash, chash, vid, sid) VALUES (?, ?, ?, ?)`,
			hhash, chash, vid, sid)
		if err != nil {
			t.Errorf("cells insert failed: %v", err)
		} else {
			t.Logf("✓ cells: %s", chash[:8])
		}
	})

	// Test reqs table
	t.Run("reqs", func(t *testing.T) {
		hhash := fmt.Sprintf("reqhash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO reqs (hhash, vid, total, date) VALUES (?, ?, ?, ?)`,
			hhash, vid, 1, now)
		if err != nil {
			t.Errorf("reqs insert failed: %v", err)
		} else {
			t.Logf("✓ reqs: %s", hhash[:8])
		}
	})
}

// Messaging Tables: mthreads, mstore, mtriage, redirects, redirect_history, impression_daily
func testMessagingTables(t *testing.T, s *TestService, oid, tid, vid, uid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test mthreads table
	t.Run("mthreads", func(t *testing.T) {
		err := s.Session.Exec(ctx, `INSERT INTO mthreads (
			tid, oid, org, uid, vid, alias, name, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tid, oid, "test_org", uid, vid, "test_alias", "Test Thread", now, now)
		if err != nil {
			t.Errorf("mthreads insert failed: %v", err)
		} else {
			t.Logf("✓ mthreads: %s", tid.String()[:8])
		}
	})

	// Test mstore table
	t.Run("mstore", func(t *testing.T) {
		mid := uuid.New()
		err := s.Session.Exec(ctx, `INSERT INTO mstore (
			mid, tid, oid, org, uid, vid, subject, msg, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			mid, tid, oid, "test_org", uid, vid, "Test Subject",
			"Test message content", now)
		if err != nil {
			t.Errorf("mstore insert failed: %v", err)
		} else {
			t.Logf("✓ mstore: %s", mid.String()[:8])
		}
	})

	// Test mtriage table
	t.Run("mtriage", func(t *testing.T) {
		mid := uuid.New()
		err := s.Session.Exec(ctx, `INSERT INTO mtriage (
			mid, tid, oid, org, uid, vid, subject, msg, scheduled, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			mid, tid, oid, "test_org", uid, vid, "Test Triage", "Test triage message", now, now)
		if err != nil {
			t.Errorf("mtriage insert failed: %v", err)
		} else {
			t.Logf("✓ mtriage: %s", mid.String()[:8])
		}
	})

	// Test redirects table
	t.Run("redirects", func(t *testing.T) {
		hhash := fmt.Sprintf("redir_%d", time.Now().UnixNano())
		urlfrom := "example.com/from"
		err := s.Session.Exec(ctx, `INSERT INTO redirects (
			hhash, urlfrom, urlto, updated_at, updater, oid
		) VALUES (?, ?, ?, ?, ?, ?)`,
			hhash, urlfrom, "https://example.com/dest", now, uid, oid)
		if err != nil {
			t.Errorf("redirects insert failed: %v", err)
		} else {
			t.Logf("✓ redirects: %s", hhash[:8])
		}
	})

	// Test redirect_history table
	t.Run("redirect_history", func(t *testing.T) {
		urlfrom := "example.com/old"
		hostfrom := "example.com"
		slugfrom := "/old"
		err := s.Session.Exec(ctx, `INSERT INTO redirect_history (
			urlfrom, hostfrom, slugfrom, urlto, hostto, pathto, searchto, updater, oid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			urlfrom, hostfrom, slugfrom, "https://example.com/new",
			"example.com", "/new", "", uid, oid)
		if err != nil {
			t.Errorf("redirect_history insert failed: %v", err)
		} else {
			t.Logf("✓ redirect_history: %s", urlfrom[:10])
		}
	})

	// Test impression_daily table
	t.Run("impression_daily", func(t *testing.T) {
		campaignID := uuid.New()
		day := time.Now().UTC().Truncate(24 * time.Hour)
		err := s.Session.Exec(ctx, `INSERT INTO impression_daily (
			oid, org, tid, campaign_id, day, variant_id, total_impressions, anonymous_impressions,
			identified_impressions, unique_visitors, conversions, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			oid, "test_org", tid, campaignID, day, "all", 100, 60, 40, 80, 2, now)
		if err != nil {
			t.Errorf("impression_daily insert failed: %v", err)
		} else {
			t.Logf("✓ impression_daily: %s", campaignID.String()[:8])
		}
	})
}

// Compliance Tables: agreements, agreed
func testComplianceTables(t *testing.T, s *TestService, oid, vid, sid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test agreements table
	t.Run("agreements", func(t *testing.T) {
		uid := uuid.New()
		avid := uuid.New()
		refID := uuid.New()
		ownerID := uuid.New()
		hhash := "test_hhash"
		err := s.Session.Exec(ctx, `INSERT INTO agreements (
			vid, created_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid,
			country, region, culture, source, medium, campaign, term, ref, rcode, aff,
			browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			vid, now, 123, sid, uid, avid, hhash, "test_app", "1.0", "https://test.com", "192.168.1.1", "iphash123",
			"gaid123", "idfa123", "msid123", "fbid123", "US", "CA", "en-US", "google", "cpc", "test", "term", refID,
			"rcode", "aff", "Chrome", "bhash123", "desktop", "Mac", "UTC", 1920, 1080, 37.77, -122.41, "94102", ownerID, oid)
		if err != nil {
			t.Errorf("agreements insert failed: %v", err)
		} else {
			t.Logf("✓ agreements: %s", vid.String()[:8])
		}
	})

	// Test agreed table
	t.Run("agreed", func(t *testing.T) {
		uid := uuid.New()
		avid := uuid.New()
		refID := uuid.New()
		ownerID := uuid.New()
		hhash := "test_hhash"
		err := s.Session.Exec(ctx, `INSERT INTO agreed (
			vid, created_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid,
			country, region, culture, source, medium, campaign, term, ref, rcode, aff,
			browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			vid, now, 123, sid, uid, avid, hhash, "test_app", "1.0", "https://test.com", "192.168.1.1", "iphash123",
			"gaid123", "idfa123", "msid123", "fbid123", "US", "CA", "en-US", "google", "cpc", "test", "term", refID,
			"rcode", "aff", "Chrome", "bhash123", "desktop", "Mac", "UTC", 1920, 1080, 37.77, -122.41, "94102", ownerID, oid)
		if err != nil {
			t.Errorf("agreed insert failed: %v", err)
		} else {
			t.Logf("✓ agreed: %s", vid.String()[:8])
		}
	})
}

// Auth Tables: logs, updates, counters, dailies
func testAuthTables(t *testing.T, s *TestService, oid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test logs table
	t.Run("logs", func(t *testing.T) {
		logId := fmt.Sprintf("log_%d", time.Now().UnixNano())
		ldate := time.Now().Format("2006-01-02")
		ltime := time.Now().Format("15:04:05")
		err := s.Session.Exec(ctx, `INSERT INTO logs (
			id, ldate, created_at, ltime, topic, name, host, hostname, owner,
			ip, iphash, level, msg, params, oid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			logId, ldate, now, ltime, "test_topic", "test_name", "test_host",
			"test.example.com", "owner", "192.168.1.1", "iphash123",
			"INFO", "Test log message", `{}`, oid)
		if err != nil {
			t.Errorf("logs insert failed: %v", err)
		} else {
			t.Log("✓ logs")
		}
	})

	// Test updates table
	t.Run("updates", func(t *testing.T) {
		err := s.Session.Exec(ctx, `INSERT INTO updates (id, updated_at, msg) VALUES (?, ?, ?)`,
			fmt.Sprintf("update_%d", time.Now().UnixNano()), now, "Test update")
		if err != nil {
			t.Errorf("updates insert failed: %v", err)
		} else {
			t.Log("✓ updates")
		}
	})

	// Test counters table
	t.Run("counters", func(t *testing.T) {
		err := s.Session.Exec(ctx, `INSERT INTO counters (id, total, date) VALUES (?, ?, ?)`,
			fmt.Sprintf("counter_%d", time.Now().UnixNano()), 1, now)
		if err != nil {
			t.Errorf("counters insert failed: %v", err)
		} else {
			t.Log("✓ counters")
		}
	})

	// Test dailies table
	t.Run("dailies", func(t *testing.T) {
		err := s.Session.Exec(ctx, `INSERT INTO dailies (ip, day, total) VALUES (?, ?, ?)`,
			"192.168.1.200", now.Format("2006-01-02"), 1)
		if err != nil {
			t.Errorf("dailies insert failed: %v", err)
		} else {
			t.Log("✓ dailies")
		}
	})
}

// Core Tables: outcomes, hosts, payments
func testCoreTables(t *testing.T, s *TestService, oid, vid, sid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test outcomes table
	t.Run("outcomes", func(t *testing.T) {
		hhash := fmt.Sprintf("outhash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO outcomes (hhash, outcome, sink, created, url, total) VALUES (?, ?, ?, ?, ?, ?)`,
			hhash, "conversion", "webhook", now, "https://test.com/outcome", 1)
		if err != nil {
			t.Errorf("outcomes insert failed: %v", err)
		} else {
			t.Logf("✓ outcomes: %s", hhash[:8])
		}
	})

	// Test hosts table
	t.Run("hosts", func(t *testing.T) {
		hhash := fmt.Sprintf("hosthash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO hosts (hhash, hostname) VALUES (?, ?)`,
			hhash, "test.example.com")
		if err != nil {
			t.Errorf("hosts insert failed: %v", err)
		} else {
			t.Logf("✓ hosts: %s", hhash[:8])
		}
	})

	// Test payments table
	t.Run("payments", func(t *testing.T) {
		pid := uuid.New()
		prodID := uuid.New()
		uid := uuid.New()
		tid := uuid.New()
		invid := uuid.New()
		orid := uuid.New()

		err := s.Session.Exec(ctx, `INSERT INTO payments (
			id, oid, org, tid, uid, vid, sid, invid, orid, invoiced_at,
			product, product_id, pcat, man, model,
			qty, duration, starts, ends,
			price, discount, revenue, margin, cost,
			tax, tax_rate, commission, referral, fees,
			subtotal, total, payment,
			currency, country, rcode, region,
			campaign_id, paid_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			pid, oid, "test_org", tid, uid, vid, sid, invid, orid, now,
			"Test Product", prodID, "software", "TestCo", "Pro",
			1.0, int32(30), now, now.AddDate(0, 1, 0),
			99.99, 0.0, 99.99, 50.0, 49.99,
			8.0, 0.08, 5.0, 2.0, 1.0,
			99.99, 107.99, 107.99,
			"USD", "US", "CA", "California",
			uuid.Nil, now,
			now, now)
		if err != nil {
			t.Errorf("payments insert failed: %v", err)
		} else {
			t.Logf("✓ payments: %s", pid.String()[:8])
		}
	})
}

// Users Tables: userhosts, ltv
func testUsersTables(t *testing.T, s *TestService, oid, uid, vid, sid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test userhosts table
	t.Run("userhosts", func(t *testing.T) {
		hhash := fmt.Sprintf("uhosthash_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO userhosts (hhash, uid, vid, sid) VALUES (?, ?, ?, ?)`,
			hhash, uid, vid, sid)
		if err != nil {
			t.Errorf("userhosts insert failed: %v", err)
		} else {
			t.Logf("✓ userhosts: %s", hhash[:8])
		}
	})

	// Test ltv table
	t.Run("ltv", func(t *testing.T) {
		hhash := fmt.Sprintf("ltv_%d", time.Now().UnixNano())
		err := s.Session.Exec(ctx, `INSERT INTO ltv (
			hhash, id, id_type, paid, oid, org, updated_at, updater, created_at, owner
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			hhash, uid, "uid", 149.99, oid, "test_org", now, uid, now, "owner")
		if err != nil {
			t.Errorf("ltv insert failed: %v", err)
		} else {
			t.Logf("✓ ltv: %s", hhash[:8])
		}
	})
}

// Visitor Interests Tables
func testVisitorInterestsTables(t *testing.T, s *TestService, oid, vid uuid.UUID) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Test visitor_interests table
	t.Run("visitor_interests", func(t *testing.T) {
		err := s.Session.Exec(ctx, `INSERT INTO visitor_interests (
			oid, vid, audience_tags, content_tags, last_lat, last_lon,
			first_seen, last_updated, total_interactions, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			oid, vid,
			[]string{"ceo", "founder"}, []string{"sports", "tech"},
			37.7749, -122.4194,
			now, now, 1, now, now)
		if err != nil {
			t.Errorf("visitor_interests insert failed: %v", err)
		} else {
			t.Logf("✓ visitor_interests: %s", vid.String()[:8])
		}
	})
}

// Helper functions are in test_helpers.go
