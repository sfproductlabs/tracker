package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// Simple test with synchronous inserts to debug the events table issue
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

func main() {
	fmt.Println("🔍 SYNC TEST - Debug Events Table Issue")
	fmt.Println("=======================================")
	
	// Change to parent directory
	originalDir, _ := os.Getwd()
	err := os.Chdir("..")
	if err != nil {
		log.Fatalf("❌ Failed to change directory: %v", err)
	}
	defer os.Chdir(originalDir)
	
	// Load configuration
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
		log.Fatalf("❌ ClickHouse service not found")
	}
	
	// Connect with SYNCHRONOUS inserts (no async)
	opts := &clickhouse.Options{
		Addr: clickhouseConfig.Hosts,
		Auth: clickhouse.Auth{
			Database: clickhouseConfig.Context,
			Username: clickhouseConfig.Username,
			Password: clickhouseConfig.Password,
		},
		Settings: clickhouse.Settings{
			// Disable async inserts for debugging
			"async_insert": 0,
		},
		Debug:           config.Debug,
		DialTimeout:     time.Duration(clickhouseConfig.Timeout) * time.Second,
		MaxOpenConns:    clickhouseConfig.Connections,
		MaxIdleConns:    clickhouseConfig.Connections,
		ConnMaxLifetime: time.Hour,
	}
	
	conn, err := clickhouse.Open(opts)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()
	
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("❌ Failed to ping: %v", err)
	}
	
	fmt.Println("✅ Connected with synchronous inserts")
	
	// Check initial counts
	var eventCount, paymentCount int64
	conn.QueryRow(ctx, "SELECT COUNT(*) FROM events").Scan(&eventCount)
	conn.QueryRow(ctx, "SELECT COUNT(*) FROM payments").Scan(&paymentCount)
	fmt.Printf("📊 Initial counts - Events: %d, Payments: %d\n", eventCount, paymentCount)
	
	// Test 1: Simple event insert
	fmt.Println("\n📝 Testing simple synchronous event insert...")
	
	eventID := uuid.New()
	visitorID := uuid.New()
	sessionID := uuid.New()
	userID := uuid.New()
	now := time.Now().UTC()
	
	err = conn.Exec(ctx, `INSERT INTO events (
		eid, eid_created_at, vid, vid_created_at, sid, sid_created_at,
		created_at, updated_at, uid, uid_created_at,
		url, ip, source, medium, campaign, country, region, city, lat, lon,
		app, ver, ename, params
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		eventID, now, visitorID, now, sessionID, now,
		now, now, userID, now,
		"https://debug.test.com/sync-test",
		"203.0.113.99",
		"debug",
		"sync_test",
		"debug_campaign",
		"US",
		"CA", 
		"Debug City",
		37.7749,
		-122.4194,
		"debug_app",
		"1.0.0",
		"sync_test_event",
		`{"test": "sync_debug"}`,
	)
	
	if err != nil {
		fmt.Printf("❌ Sync insert failed: %v\n", err)
	} else {
		fmt.Printf("✅ Sync insert successful: %s\n", eventID.String()[:8])
		
		// Immediately check count
		var newEventCount int64
		conn.QueryRow(ctx, "SELECT COUNT(*) FROM events").Scan(&newEventCount)
		fmt.Printf("📊 Events count after insert: %d (was %d)\n", newEventCount, eventCount)
		
		// Check recent events
		fmt.Println("\n🔍 Recent events in table:")
		rows, err := conn.Query(ctx, "SELECT eid, url, ename, created_at FROM events ORDER BY created_at DESC LIMIT 5")
		if err != nil {
			fmt.Printf("❌ Query failed: %v\n", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var eid uuid.UUID
				var url, ename string
				var createdAt time.Time
				
				if err := rows.Scan(&eid, &url, &ename, &createdAt); err == nil {
					fmt.Printf("   • %s | %s | %s | %s\n", 
						eid.String()[:8], 
						url, 
						ename,
						createdAt.Format("15:04:05"))
				}
			}
		}
	}
	
	// Test 2: Check if async inserts are stuck
	fmt.Println("\n🔄 Testing async insert status...")
	rows, err := conn.Query(ctx, "SELECT query_id, status FROM system.asynchronous_inserts WHERE status != 'ok' LIMIT 5")
	if err != nil {
		fmt.Printf("⚠️  Can't check async inserts: %v\n", err)
	} else {
		defer rows.Close()
		hasRows := false
		for rows.Next() {
			hasRows = true
			var queryID, status string
			if err := rows.Scan(&queryID, &status); err == nil {
				fmt.Printf("   • Query: %s, Status: %s\n", queryID, status)
			}
		}
		if !hasRows {
			fmt.Println("   ✅ No failed async inserts found")
		}
	}
	
	fmt.Println("\n=======================================")
	fmt.Println("🔍 Sync test completed")
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