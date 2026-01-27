package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

type WriteArgs struct {
	WriteType      int
	Values         *map[string]interface{}
	IsServer       bool
	SaveCookie     bool
	IP             string
	Browser        string
	Language       string
	URI            string
	Host           string
	EventID        uuid.UUID
	OrgID          *uuid.UUID
	CallingService *Service
}

// Constants
const (
	WRITE_EVENT = 1 << 3
	WRITE_LTV   = 1 << 4
)

// Test ClickHouse service
type TestClickhouseService struct {
	Configuration *Service
	Session       clickhouse.Conn
	AppConfig     *Configuration
	mu            sync.Mutex
	eventCount    int64
	ltvCount      int64
	startTime     time.Time
}

func (service *TestClickhouseService) connect() error {
	opts := &clickhouse.Options{
		Addr: service.Configuration.Hosts,
		Auth: clickhouse.Auth{
			Database: service.Configuration.Context,
			Username: service.Configuration.Username,
			Password: service.Configuration.Password,
		},
		Settings: clickhouse.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   10000000,
			"async_insert_busy_timeout_ms": service.Configuration.BatchFlushInterval,
		},
		Debug:           service.AppConfig.Debug,
		DialTimeout:     time.Duration(service.Configuration.Timeout) * time.Second,
		MaxOpenConns:    service.Configuration.Connections,
		MaxIdleConns:    service.Configuration.Connections,
		ConnMaxLifetime: time.Hour,
	}

	if service.Configuration.EnableCompression {
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

	service.Session = conn
	return nil
}

func (service *TestClickhouseService) writeEvent(writeArgs *WriteArgs) error {
	ctx := context.Background()
	now := time.Now().UTC()

	values := *writeArgs.Values

	// Extract core identifiers
	visitorID, _ := uuid.Parse(values["vid"].(string))
	sessionID, _ := uuid.Parse(values["sid"].(string))
	var userID uuid.UUID
	if uid, ok := values["uid"].(string); ok && uid != "" {
		userID, _ = uuid.Parse(uid)
	}

	// Insert into events table using actual schema (NO updated_at field)
	err := service.Session.Exec(ctx, `INSERT INTO events (
		eid, vid, sid, oid, org, created_at, uid,
		url, ip, source, medium, campaign, country, region, city, lat, lon,
		app, ver, ename, params
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		writeArgs.EventID, visitorID, sessionID, writeArgs.OrgID,
		getStringValue(values, "org", ""), now, userID,
		getStringValue(values, "url", ""),
		writeArgs.IP,
		getStringValue(values, "utm_source", ""),
		getStringValue(values, "utm_medium", ""),
		getStringValue(values, "utm_campaign", ""),
		getStringValue(values, "country", ""),
		getStringValue(values, "region", ""),
		getStringValue(values, "city", ""),
		getFloatValue(values, "lat", 0.0),
		getFloatValue(values, "lon", 0.0),
		getStringValue(values, "app", ""),
		getIntValue(values, "ver", 0),
		getStringValue(values, "event", ""),
		formatParamsAsJSON(values),
	)

	if err != nil {
		return fmt.Errorf("failed to insert event: %v", err)
	}

	service.mu.Lock()
	service.eventCount++
	service.mu.Unlock()

	return nil
}

func (service *TestClickhouseService) writeLTV(writeArgs *WriteArgs) error {
	ctx := context.Background()
	now := time.Now().UTC()

	values := *writeArgs.Values

	// Extract payment data
	paymentID, _ := uuid.Parse(values["invoice_id"].(string))
	productID := uuid.New() // Generate if not provided
	testOID := uuid.New()
	testUID := uuid.New()
	testVID := uuid.New()
	testSID := uuid.New()
	testTID := uuid.New()
	testInvID := uuid.New()
	testOrdID := uuid.New()

	// Insert into payments table using actual schema (matches users.1.sql)
	err := service.Session.Exec(ctx, `INSERT INTO payments (
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
		paymentID,
		testOID,
		getStringValue(values, "org", ""),
		testTID,
		testUID,
		testVID,
		testSID,
		testInvID,
		testOrdID,
		now,
		getStringValue(values, "product", ""),
		productID,
		getStringValue(values, "pcat", ""),
		getStringValue(values, "man", ""),
		getStringValue(values, "model", ""),
		getFloatValue(values, "qty", 1.0),
		int32(0), // duration
		now,      // starts
		now,      // ends
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "discount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "margin", 0.0),
		getFloatValue(values, "cost", 0.0),
		getFloatValue(values, "tax", 0.0),
		getFloatValue(values, "tax_rate", 0.0),
		getFloatValue(values, "commission", 0.0),
		getFloatValue(values, "referral", 0.0),
		getFloatValue(values, "fees", 0.0),
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getStringValue(values, "currency", "USD"),
		getStringValue(values, "country", ""),
		getStringValue(values, "rcode", ""),
		getStringValue(values, "region", ""),
		uuid.Nil, // campaign_id
		now,
		now,
		now,
	)

	if err != nil {
		return fmt.Errorf("failed to insert payment: %v", err)
	}

	service.mu.Lock()
	service.ltvCount++
	service.mu.Unlock()

	return nil
}

func (service *TestClickhouseService) write(writeArgs *WriteArgs) error {
	switch writeArgs.WriteType {
	case WRITE_EVENT:
		return service.writeEvent(writeArgs)
	case WRITE_LTV:
		return service.writeLTV(writeArgs)
	default:
		return fmt.Errorf("unsupported write type: %d", writeArgs.WriteType)
	}
}

// Test functions
func TestBatchWrite(t *testing.T) {
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
	testService := &TestClickhouseService{
		Configuration: clickhouseConfig,
		AppConfig:     config,
		startTime:     time.Now(),
	}

	// Connect to ClickHouse
	err = testService.connect()
	if err != nil {
		t.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer testService.Session.Close()

	t.Log("Connected to ClickHouse successfully")

	// Run subtests
	t.Run("SingleEvent", func(t *testing.T) {
		testSingleEventWrite(t, testService)
	})

	t.Run("SingleLTV", func(t *testing.T) {
		testSingleLTVWrite(t, testService)
	})

	t.Run("BatchEvents", func(t *testing.T) {
		testBatchEventWrite(t, testService, 25)
	})

	t.Run("MixedBatch", func(t *testing.T) {
		testMixedBatch(t, testService, 10)
	})

	// Performance summary
	duration := time.Since(testService.startTime)
	t.Logf("Performance Summary:")
	t.Logf("  Total duration: %v", duration)
	t.Logf("  Events written: %d", testService.eventCount)
	t.Logf("  LTV records written: %d", testService.ltvCount)
	if duration.Seconds() > 0 {
		t.Logf("  Events per second: %.2f", float64(testService.eventCount)/duration.Seconds())
	}
	t.Logf("  Total records: %d", testService.eventCount+testService.ltvCount)
}

func testSingleEventWrite(t *testing.T, service *TestClickhouseService) {
	eventID := uuid.New()
	visitorID := uuid.New()
	sessionID := uuid.New()
	userID := uuid.New()

	eventParams := map[string]interface{}{
		"vid":          visitorID.String(),
		"sid":          sessionID.String(),
		"uid":          userID.String(),
		"org":          "test_org",
		"url":          "https://test.example.com/single-event",
		"event":        "single_test_event",
		"utm_source":   "test",
		"utm_medium":   "batch_test",
		"utm_campaign": "single_event_test",
		"country":      "US",
		"region":       "CA",
		"city":         "San Francisco",
		"lat":          37.7749,
		"lon":          -122.4194,
		"app":          "test_app",
		"ver":          1,
	}

	testOrgID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	writeArgs := &WriteArgs{
		WriteType:      WRITE_EVENT,
		EventID:        eventID,
		OrgID:          &testOrgID,
		IP:             "203.0.113.10",
		Browser:        "Test-Browser/1.0",
		Language:       "en-US",
		URI:            "/single-event-test",
		Host:           "test.example.com",
		IsServer:       false,
		Values:         &eventParams,
		CallingService: service.Configuration,
	}

	err := service.write(writeArgs)
	if err != nil {
		t.Errorf("Failed to write single event: %v", err)
		return
	}

	t.Logf("Single event written: %s", eventID.String()[:8])
}

func testSingleLTVWrite(t *testing.T, service *TestClickhouseService) {
	eventID := uuid.New()
	paymentID := uuid.New()
	testOrgID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")

	ltvParams := map[string]interface{}{
		"invoice_id":     paymentID.String(),
		"product":        "Single Test Product",
		"amount":         99.99,
		"currency":       "USD",
		"status":         "completed",
		"payment_method": "test_card",
	}

	writeArgs := &WriteArgs{
		WriteType:      WRITE_LTV,
		EventID:        eventID,
		OrgID:          &testOrgID,
		IP:             "203.0.113.20",
		Browser:        "Test-Browser/1.0",
		Language:       "en-US",
		URI:            "/single-ltv-test",
		Host:           "billing.test.example.com",
		IsServer:       false,
		Values:         &ltvParams,
		CallingService: service.Configuration,
	}

	err := service.write(writeArgs)
	if err != nil {
		t.Errorf("Failed to write single LTV: %v", err)
		return
	}

	t.Logf("Single LTV written: %s", paymentID.String()[:8])
}

func testBatchEventWrite(t *testing.T, service *TestClickhouseService, batchSize int) {
	testOrgID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	startTime := time.Now()

	for i := 0; i < batchSize; i++ {
		eventID := uuid.New()
		visitorID := uuid.New()
		sessionID := uuid.New()
		userID := uuid.New()

		eventParams := map[string]interface{}{
			"vid":          visitorID.String(),
			"sid":          sessionID.String(),
			"uid":          userID.String(),
			"org":          "test_org",
			"url":          fmt.Sprintf("https://test.example.com/batch-event/%d", i),
			"event":        "batch_test_event",
			"batch_index":  i,
			"utm_source":   "batch_test",
			"utm_medium":   "automated",
			"utm_campaign": "batch_performance_test",
			"country":      "US",
			"region":       "CA",
			"city":         "San Francisco",
			"lat":          37.7749,
			"lon":          -122.4194,
			"app":          "batch_test_app",
			"ver":          2,
			"price":        float64(i * 10),
			"currency":     "USD",
		}

		writeArgs := &WriteArgs{
			WriteType:      WRITE_EVENT,
			EventID:        eventID,
			OrgID:          &testOrgID,
			IP:             fmt.Sprintf("203.0.113.%d", 50+(i%200)),
			Browser:        "Batch-Test-Browser/2.0",
			Language:       "en-US",
			URI:            fmt.Sprintf("/batch-test/%d", i),
			Host:           "batch.test.example.com",
			IsServer:       false,
			Values:         &eventParams,
			CallingService: service.Configuration,
		}

		err := service.write(writeArgs)
		if err != nil {
			t.Errorf("Batch write failed at index %d: %v", i, err)
			return
		}
	}

	duration := time.Since(startTime)
	t.Logf("Batch of %d events written in %v", batchSize, duration)
	if duration.Seconds() > 0 {
		t.Logf("Rate: %.2f events/second", float64(batchSize)/duration.Seconds())
	}
}

func testMixedBatch(t *testing.T, service *TestClickhouseService, count int) {
	testOrgID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	startTime := time.Now()

	for i := 0; i < count; i++ {
		// Write event
		eventID := uuid.New()
		visitorID := uuid.New()
		sessionID := uuid.New()
		userID := uuid.New()

		eventParams := map[string]interface{}{
			"vid":          visitorID.String(),
			"sid":          sessionID.String(),
			"uid":          userID.String(),
			"org":          "test_org",
			"url":          fmt.Sprintf("https://test.example.com/mixed-event/%d", i),
			"event":        "mixed_batch_event",
			"utm_campaign": "mixed_batch_test",
			"country":      "US",
			"app":          "mixed_test",
			"ver":          3,
		}

		eventWriteArgs := &WriteArgs{
			WriteType:      WRITE_EVENT,
			EventID:        eventID,
			OrgID:          &testOrgID,
			IP:             fmt.Sprintf("203.0.113.%d", 100+(i%150)),
			Browser:        "Mixed-Test-Browser/1.0",
			URI:            fmt.Sprintf("/mixed-event/%d", i),
			Host:           "mixed.test.example.com",
			Values:         &eventParams,
			CallingService: service.Configuration,
		}

		err := service.write(eventWriteArgs)
		if err != nil {
			t.Errorf("Mixed event write failed at index %d: %v", i, err)
			return
		}

		// Write LTV
		paymentID := uuid.New()
		ltvParams := map[string]interface{}{
			"payment_id": paymentID.String(),
			"product":    fmt.Sprintf("Mixed Test Product %d", i),
			"amount":     float64(100 + (i * 50)),
			"currency":   "USD",
		}

		ltvWriteArgs := &WriteArgs{
			WriteType:      WRITE_LTV,
			EventID:        uuid.New(),
			OrgID:          &testOrgID,
			IP:             fmt.Sprintf("203.0.113.%d", 100+(i%150)),
			Browser:        "Mixed-Test-Browser/1.0",
			URI:            fmt.Sprintf("/mixed-ltv/%d", i),
			Host:           "billing.mixed.test.example.com",
			Values:         &ltvParams,
			CallingService: service.Configuration,
		}

		err = service.write(ltvWriteArgs)
		if err != nil {
			t.Errorf("Mixed LTV write failed at index %d: %v", i, err)
			return
		}
	}

	duration := time.Since(startTime)
	t.Logf("Mixed batch of %d records written in %v", count*2, duration)
	if duration.Seconds() > 0 {
		t.Logf("Rate: %.2f records/second", float64(count*2)/duration.Seconds())
	}
}

// Helper functions are now in test_helpers.go
