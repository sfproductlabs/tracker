package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// Import the exact structures from tracker.go
type Configuration struct {
	Debug   bool      `json:"Debug"`
	Notify  []Service `json:"Notify"`
}

type Service struct {
	Service            string   `json:"Service"`
	Hosts              []string `json:"Hosts"`
	Context            string   `json:"Context"`
	Username           string   `json:"Username"`
	Password           string   `json:"Password"`
	Critical           bool     `json:"Critical"`
	Skip               bool     `json:"Skip"`
	Timeout            int      `json:"Timeout"`
	Retries            int      `json:"Retries"`
	Secure             bool     `json:"Secure"`
	Connections        int      `json:"Connections"`
	BatchingEnabled    bool     `json:"BatchingEnabled"`
	MaxBatchSize       int      `json:"MaxBatchSize"`
	BatchFlushInterval int      `json:"BatchFlushInterval"`
	EnableCompression  bool     `json:"EnableCompression"`
}

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
	CallingService *Service
}

// Constants from tracker.go
const (
	WRITE_EVENT = 1 << 3
	WRITE_LTV   = 1 << 4
)

// Simplified ClickHouse service for testing
type TestClickhouseService struct {
	Configuration *Service
	Session       clickhouse.Conn
	AppConfig     *Configuration
	mu            sync.Mutex
	eventCount    int64
	ltvCount      int64
	startTime     time.Time
}

func main() {
	fmt.Println("🚀 BATCH WRITE TEST - Local ClickHouse Database")
	fmt.Println("===============================================")
	fmt.Println("This test will ACTUALLY write data to the ClickHouse database!")
	
	// Change to parent directory to access config and tracker files
	originalDir, _ := os.Getwd()
	err := os.Chdir("..")
	if err != nil {
		log.Fatalf("❌ Failed to change to tracker directory: %v", err)
	}
	defer os.Chdir(originalDir)
	
	// Load real configuration
	config, err := loadConfiguration()
	if err != nil {
		log.Fatalf("❌ Failed to load configuration: %v", err)
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
		log.Fatalf("❌ ClickHouse service not found in configuration")
	}
	
	fmt.Printf("✅ Found ClickHouse config: %v (Database: %s)\n", clickhouseConfig.Hosts, clickhouseConfig.Context)
	
	// Initialize test service
	testService := &TestClickhouseService{
		Configuration: clickhouseConfig,
		AppConfig:     config,
		startTime:     time.Now(),
	}
	
	// Connect to ClickHouse
	err = testService.connect()
	if err != nil {
		log.Fatalf("❌ Failed to connect to ClickHouse: %v", err)
	}
	defer testService.Session.Close()
	
	fmt.Println("✅ Connected to ClickHouse successfully")
	
	// Check initial database state
	fmt.Println("\n🔍 Checking initial database state...")
	initialEvents, initialPayments := checkDatabaseCounts(testService)
	fmt.Printf("   📊 Initial events: %d\n", initialEvents)
	fmt.Printf("   💳 Initial payments: %d\n", initialPayments)
	
	// Test 1: Single event write
	fmt.Println("\n📝 Test 1: Writing single WRITE_EVENT...")
	err = testSingleEventWrite(testService)
	if err != nil {
		fmt.Printf("❌ Single event write failed: %v\n", err)
	} else {
		fmt.Println("✅ Single event write completed!")
	}
	
	// Test 2: Single LTV write
	fmt.Println("\n💰 Test 2: Writing single WRITE_LTV...")
	err = testSingleLTVWrite(testService)
	if err != nil {
		fmt.Printf("❌ Single LTV write failed: %v\n", err)
	} else {
		fmt.Println("✅ Single LTV write completed!")
	}
	
	// Test 3: Batch write test
	fmt.Println("\n🚀 Test 3: Batch writing multiple events...")
	batchSize := 50
	err = testBatchWrite(testService, batchSize)
	if err != nil {
		fmt.Printf("❌ Batch write failed: %v\n", err)
	} else {
		fmt.Println("✅ Batch write completed!")
	}
	
	// Test 4: Mixed event types batch
	fmt.Println("\n🔄 Test 4: Mixed event types batch...")
	err = testMixedBatch(testService, 25)
	if err != nil {
		fmt.Printf("❌ Mixed batch failed: %v\n", err)
	} else {
		fmt.Println("✅ Mixed batch completed!")
	}
	
	// Check final database state
	fmt.Println("\n🔍 Checking final database state...")
	time.Sleep(2 * time.Second) // Allow async inserts to complete
	finalEvents, finalPayments := checkDatabaseCounts(testService)
	
	fmt.Printf("   📊 Final events: %d (added: %d)\n", finalEvents, finalEvents-initialEvents)
	fmt.Printf("   💳 Final payments: %d (added: %d)\n", finalPayments, finalPayments-initialPayments)
	
	// Performance summary
	duration := time.Since(testService.startTime)
	fmt.Printf("\n📈 Performance Summary:\n")
	fmt.Printf("   ⏱️  Total duration: %v\n", duration)
	fmt.Printf("   📊 Events written: %d\n", testService.eventCount)
	fmt.Printf("   💳 LTV records written: %d\n", testService.ltvCount)
	fmt.Printf("   🚀 Events per second: %.2f\n", float64(testService.eventCount)/duration.Seconds())
	fmt.Printf("   💾 Total records: %d\n", testService.eventCount+testService.ltvCount)
	
	// Verify data with sample queries
	fmt.Println("\n🔍 Sample data verification...")
	verifySampleData(testService)
	
	fmt.Println("\n===============================================")
	fmt.Println("🎉 Batch write test completed successfully!")
	fmt.Printf("📊 Successfully wrote %d events and %d LTV records to ClickHouse\n", testService.eventCount, testService.ltvCount)
	fmt.Println("🔍 Check the database with: clickhouse client --query \"SELECT COUNT(*) FROM sfpla.events WHERE created_at >= now() - INTERVAL 1 HOUR\"")
}

func loadConfiguration() (*Configuration, error) {
	data, err := ioutil.ReadFile("config.json")
	if err != nil {
		return nil, err
	}
	
	var config Configuration
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	
	return &config, nil
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
	
	// Insert into events table using actual schema
	err := service.Session.Exec(ctx, `INSERT INTO events (
		eid, eid_created_at, vid, vid_created_at, sid, sid_created_at,
		created_at, updated_at, uid, uid_created_at,
		url, ip, source, medium, campaign, country, region, city, lat, lon,
		app, ver, ename, params
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		writeArgs.EventID, now, visitorID, now, sessionID, now,
		now, now, userID, now,
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
	paymentID, _ := uuid.Parse(values["payment_id"].(string))
	productID := uuid.New() // Generate if not provided
	
	// Insert into payments table using actual schema
	err := service.Session.Exec(ctx, `INSERT INTO payments (
		id, product, product_id, price, revenue, total, payment, currency, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		paymentID,
		getStringValue(values, "product", ""),
		productID,
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getFloatValue(values, "amount", 0.0),
		getStringValue(values, "currency", "USD"),
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

func testSingleEventWrite(service *TestClickhouseService) error {
	eventID := uuid.New()
	visitorID := uuid.New()
	sessionID := uuid.New()
	userID := uuid.New()
	
	eventParams := map[string]interface{}{
		"vid": visitorID.String(),
		"sid": sessionID.String(),
		"uid": userID.String(),
		"url": "https://test.example.com/single-event",
		"event": "single_test_event",
		"utm_source": "test",
		"utm_medium": "batch_test",
		"utm_campaign": "single_event_test",
		"country": "US",
		"region": "CA",
		"city": "San Francisco",
		"lat": 37.7749,
		"lon": -122.4194,
		"app": "test_app",
		"ver": 1,
	}
	
	writeArgs := &WriteArgs{
		WriteType: WRITE_EVENT,
		EventID:   eventID,
		IP:        "203.0.113.10",
		Browser:   "Test-Browser/1.0",
		Language:  "en-US",
		URI:       "/single-event-test",
		Host:      "test.example.com",
		IsServer:  false,
		Values:    &eventParams,
		CallingService: service.Configuration,
	}
	
	err := service.write(writeArgs)
	if err != nil {
		return err
	}
	
	fmt.Printf("   📊 Single event written: %s\n", eventID.String()[:8])
	return nil
}

func testSingleLTVWrite(service *TestClickhouseService) error {
	eventID := uuid.New()
	paymentID := uuid.New()
	
	ltvParams := map[string]interface{}{
		"payment_id": paymentID.String(),
		"product": "Single Test Product",
		"amount": 99.99,
		"currency": "USD",
		"status": "completed",
		"payment_method": "test_card",
	}
	
	writeArgs := &WriteArgs{
		WriteType: WRITE_LTV,
		EventID:   eventID,
		IP:        "203.0.113.20",
		Browser:   "Test-Browser/1.0",
		Language:  "en-US",
		URI:       "/single-ltv-test",
		Host:      "billing.test.example.com",
		IsServer:  false,
		Values:    &ltvParams,
		CallingService: service.Configuration,
	}
	
	err := service.write(writeArgs)
	if err != nil {
		return err
	}
	
	fmt.Printf("   💳 Single LTV written: %s\n", paymentID.String()[:8])
	return nil
}

func testBatchWrite(service *TestClickhouseService, batchSize int) error {
	fmt.Printf("   🚀 Writing %d events in batch...\n", batchSize)
	
	startTime := time.Now()
	
	for i := 0; i < batchSize; i++ {
		eventID := uuid.New()
		visitorID := uuid.New()
		sessionID := uuid.New()
		userID := uuid.New()
		
		eventParams := map[string]interface{}{
			"vid": visitorID.String(),
			"sid": sessionID.String(),
			"uid": userID.String(),
			"url": fmt.Sprintf("https://test.example.com/batch-event/%d", i),
			"event": "batch_test_event",
			"batch_index": i,
			"utm_source": "batch_test",
			"utm_medium": "automated",
			"utm_campaign": "batch_performance_test",
			"country": "US",
			"region": "CA",
			"city": "San Francisco",
			"lat": 37.7749,
			"lon": -122.4194,
			"app": "batch_test_app",
			"ver": 2,
			"price": float64(i * 10),
			"currency": "USD",
		}
		
		writeArgs := &WriteArgs{
			WriteType: WRITE_EVENT,
			EventID:   eventID,
			IP:        fmt.Sprintf("203.0.113.%d", 50+(i%200)),
			Browser:   "Batch-Test-Browser/2.0",
			Language:  "en-US",
			URI:       fmt.Sprintf("/batch-test/%d", i),
			Host:      "batch.test.example.com",
			IsServer:  false,
			Values:    &eventParams,
			CallingService: service.Configuration,
		}
		
		err := service.write(writeArgs)
		if err != nil {
			return fmt.Errorf("batch write failed at index %d: %v", i, err)
		}
		
		if i%10 == 0 && i > 0 {
			fmt.Printf("   📊 Written %d/%d events...\n", i, batchSize)
		}
	}
	
	duration := time.Since(startTime)
	fmt.Printf("   ✅ Batch of %d events written in %v\n", batchSize, duration)
	fmt.Printf("   🚀 Rate: %.2f events/second\n", float64(batchSize)/duration.Seconds())
	
	return nil
}

func testMixedBatch(service *TestClickhouseService, count int) error {
	fmt.Printf("   🔄 Writing %d mixed events and LTV records...\n", count*2)
	
	startTime := time.Now()
	
	for i := 0; i < count; i++ {
		// Write event
		eventID := uuid.New()
		visitorID := uuid.New()
		sessionID := uuid.New()
		userID := uuid.New()
		
		eventParams := map[string]interface{}{
			"vid": visitorID.String(),
			"sid": sessionID.String(),
			"uid": userID.String(),
			"url": fmt.Sprintf("https://test.example.com/mixed-event/%d", i),
			"event": "mixed_batch_event",
			"utm_campaign": "mixed_batch_test",
			"country": "US",
			"app": "mixed_test",
			"ver": 3,
		}
		
		eventWriteArgs := &WriteArgs{
			WriteType: WRITE_EVENT,
			EventID:   eventID,
			IP:        fmt.Sprintf("203.0.113.%d", 100+(i%150)),
			Browser:   "Mixed-Test-Browser/1.0",
			URI:       fmt.Sprintf("/mixed-event/%d", i),
			Host:      "mixed.test.example.com",
			Values:    &eventParams,
			CallingService: service.Configuration,
		}
		
		err := service.write(eventWriteArgs)
		if err != nil {
			return fmt.Errorf("mixed event write failed at index %d: %v", i, err)
		}
		
		// Write LTV
		paymentID := uuid.New()
		ltvParams := map[string]interface{}{
			"payment_id": paymentID.String(),
			"product": fmt.Sprintf("Mixed Test Product %d", i),
			"amount": float64(100 + (i * 50)),
			"currency": "USD",
		}
		
		ltvWriteArgs := &WriteArgs{
			WriteType: WRITE_LTV,
			EventID:   uuid.New(),
			IP:        fmt.Sprintf("203.0.113.%d", 100+(i%150)),
			Browser:   "Mixed-Test-Browser/1.0",
			URI:       fmt.Sprintf("/mixed-ltv/%d", i),
			Host:      "billing.mixed.test.example.com",
			Values:    &ltvParams,
			CallingService: service.Configuration,
		}
		
		err = service.write(ltvWriteArgs)
		if err != nil {
			return fmt.Errorf("mixed LTV write failed at index %d: %v", i, err)
		}
	}
	
	duration := time.Since(startTime)
	fmt.Printf("   ✅ Mixed batch of %d records written in %v\n", count*2, duration)
	fmt.Printf("   🚀 Rate: %.2f records/second\n", float64(count*2)/duration.Seconds())
	
	return nil
}

func checkDatabaseCounts(service *TestClickhouseService) (int64, int64) {
	ctx := context.Background()
	
	var eventCount int64
	err := service.Session.QueryRow(ctx, "SELECT COUNT(*) FROM events WHERE created_at >= now() - INTERVAL 1 HOUR").Scan(&eventCount)
	if err != nil {
		fmt.Printf("   ⚠️  Could not check events: %v\n", err)
		eventCount = -1
	}
	
	var paymentCount int64
	err = service.Session.QueryRow(ctx, "SELECT COUNT(*) FROM payments WHERE created_at >= now() - INTERVAL 1 HOUR").Scan(&paymentCount)
	if err != nil {
		fmt.Printf("   ⚠️  Could not check payments: %v\n", err)
		paymentCount = -1
	}
	
	return eventCount, paymentCount
}

func verifySampleData(service *TestClickhouseService) {
	ctx := context.Background()
	
	// Sample recent events
	fmt.Println("   📊 Recent events sample:")
	rows, err := service.Session.Query(ctx, "SELECT eid, url, campaign, created_at FROM events WHERE created_at >= now() - INTERVAL 1 HOUR ORDER BY created_at DESC LIMIT 5")
	if err != nil {
		fmt.Printf("   ❌ Failed to query events: %v\n", err)
		return
	}
	defer rows.Close()
	
	for rows.Next() {
		var eid uuid.UUID
		var url, campaign string
		var createdAt time.Time
		
		if err := rows.Scan(&eid, &url, &campaign, &createdAt); err != nil {
			continue
		}
		
		fmt.Printf("      • %s | %s | %s | %s\n", 
			eid.String()[:8], 
			truncateString(url, 30), 
			truncateString(campaign, 20),
			createdAt.Format("15:04:05"))
	}
	
	// Sample recent payments
	fmt.Println("   💳 Recent payments sample:")
	rows, err = service.Session.Query(ctx, "SELECT id, product, payment, currency, created_at FROM payments WHERE created_at >= now() - INTERVAL 1 HOUR ORDER BY created_at DESC LIMIT 5")
	if err != nil {
		fmt.Printf("   ❌ Failed to query payments: %v\n", err)
		return
	}
	defer rows.Close()
	
	for rows.Next() {
		var id uuid.UUID
		var product, currency string
		var payment float64
		var createdAt time.Time
		
		if err := rows.Scan(&id, &product, &payment, &currency, &createdAt); err != nil {
			continue
		}
		
		fmt.Printf("      • %s | %s | $%.2f %s | %s\n", 
			id.String()[:8], 
			truncateString(product, 20), 
			payment,
			currency,
			createdAt.Format("15:04:05"))
	}
}

// Helper functions
func getStringValue(values map[string]interface{}, key, defaultValue string) string {
	if val, ok := values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return defaultValue
}

func getFloatValue(values map[string]interface{}, key string, defaultValue float64) float64 {
	if val, ok := values[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return defaultValue
}

func getIntValue(values map[string]interface{}, key string, defaultValue int) int {
	if val, ok := values[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return defaultValue
}

func formatParamsAsJSON(values map[string]interface{}) string {
	// Create a simplified params object for storage
	params := make(map[string]interface{})
	for key, value := range values {
		// Store non-core fields as params
		if key != "vid" && key != "sid" && key != "uid" && key != "url" && 
		   key != "utm_source" && key != "utm_medium" && key != "utm_campaign" &&
		   key != "country" && key != "region" && key != "city" && key != "lat" && key != "lon" {
			params[key] = value
		}
	}
	
	if len(params) == 0 {
		return "{}"
	}
	
	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return "{}"
	}
	
	return string(jsonBytes)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}