/*===----------- clickhouse.go - clickhouse interface in go  -------------===
 *
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2018 Andrew Grosser. All Rights Reserved.
 *
 *                                     `...
 *                                    yNMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMd`
 *                                    dMMMm.
 *                                    dMMMm.
 *                                    dMMMm.               /hdy.
 *                  ohs+`             yMMMd.               yMMM-
 *                 .mMMm.             yMMMm.               oMMM/
 *                 :MMMd`             sMMMN.               oMMMo
 *                 +MMMd`             oMMMN.               oMMMy
 *                 sMMMd`             /MMMN.               oMMMh
 *                 sMMMd`             /MMMN-               oMMMd
 *                 oMMMd`             :NMMM-               oMMMd
 *                 /MMMd`             -NMMM-               oMMMm
 *                 :MMMd`             .mMMM-               oMMMm`
 *                 -NMMm.             `mMMM:               oMMMm`
 *                 .mMMm.              dMMM/               +MMMm`
 *                 `hMMm.              hMMM/               /MMMm`
 *                  yMMm.              yMMM/               /MMMm`
 *                  oMMm.              oMMMo               -MMMN.
 *                  +MMm.              +MMMo               .MMMN-
 *                  +MMm.              /MMMo               .NMMN-
 *           `      +MMm.              -MMMs               .mMMN:  `.-.
 *          /hys:`  +MMN-              -NMMy               `hMMN: .yNNy
 *          :NMMMy` sMMM/              .NMMy                yMMM+-dMMMo
 *           +NMMMh-hMMMo              .mMMy                +MMMmNMMMh`
 *            /dMMMNNMMMs              .dMMd                -MMMMMNm+`
 *             .+mMMMMMN:              .mMMd                `NMNmh/`
 *               `/yhhy:               `dMMd                 /+:`
 *                                     `hMMm`
 *                                     `hMMm.
 *                                     .mMMm:
 *                                     :MMMd-
 *                                     -NMMh.
 *                                      ./:.
 *
 *===----------------------------------------------------------------------===
 */
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

////////////////////////////////////////
// Performance Monitoring & Metrics
////////////////////////////////////////

// PerformanceMetrics tracks operation performance and health
type PerformanceMetrics struct {
	EventCount      int64        `json:"event_count"`
	ErrorCount      int64        `json:"error_count"`
	CampaignEvents  int64        `json:"campaign_events"`
	MThreadsOps     int64        `json:"mthreads_ops"`
	MStoreOps       int64        `json:"mstore_ops"`
	MTriageOps      int64        `json:"mtriage_ops"`
	TotalLatency    int64        `json:"total_latency_ns"`
	LastProcessed   int64        `json:"last_processed_unix"`
	ConnectionCount int32        `json:"connection_count"`
	HealthStatus    string       `json:"health_status"`
	StartTime       int64        `json:"start_time_unix"`
	mu              sync.RWMutex `json:"-"`
}

// Global performance metrics instance
var globalMetrics = &PerformanceMetrics{
	StartTime:    time.Now().Unix(),
	HealthStatus: "initializing",
}

// UpdateEventCount atomically increments event count
func (pm *PerformanceMetrics) UpdateEventCount() {
	atomic.AddInt64(&pm.EventCount, 1)
	atomic.StoreInt64(&pm.LastProcessed, time.Now().Unix())
}

// UpdateErrorCount atomically increments error count
func (pm *PerformanceMetrics) UpdateErrorCount() {
	atomic.AddInt64(&pm.ErrorCount, 1)
}

// UpdateLatency atomically adds to total latency
func (pm *PerformanceMetrics) UpdateLatency(duration time.Duration) {
	atomic.AddInt64(&pm.TotalLatency, int64(duration))
}

// GetAverageLatency calculates average latency
func (pm *PerformanceMetrics) GetAverageLatency() time.Duration {
	events := atomic.LoadInt64(&pm.EventCount)
	if events == 0 {
		return 0
	}
	total := atomic.LoadInt64(&pm.TotalLatency)
	return time.Duration(total / events)
}

// GetMetricsSnapshot returns a thread-safe snapshot of metrics
func (pm *PerformanceMetrics) GetMetricsSnapshot() PerformanceMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return PerformanceMetrics{
		EventCount:      atomic.LoadInt64(&pm.EventCount),
		ErrorCount:      atomic.LoadInt64(&pm.ErrorCount),
		CampaignEvents:  atomic.LoadInt64(&pm.CampaignEvents),
		MThreadsOps:     atomic.LoadInt64(&pm.MThreadsOps),
		MStoreOps:       atomic.LoadInt64(&pm.MStoreOps),
		MTriageOps:      atomic.LoadInt64(&pm.MTriageOps),
		TotalLatency:    atomic.LoadInt64(&pm.TotalLatency),
		LastProcessed:   atomic.LoadInt64(&pm.LastProcessed),
		ConnectionCount: atomic.LoadInt32(&pm.ConnectionCount),
		HealthStatus:    pm.HealthStatus,
		StartTime:       pm.StartTime,
	}
}

// ErrorTypes for better error handling
type ErrorType string

const (
	ErrorTypeConnection ErrorType = "connection"
	ErrorTypeQuery      ErrorType = "query"
	ErrorTypeTimeout    ErrorType = "timeout"
	ErrorTypeValidation ErrorType = "validation"
	ErrorTypeRateLimit  ErrorType = "rate_limit"
	ErrorTypeCampaign   ErrorType = "campaign"
	ErrorTypeMetrics    ErrorType = "metrics"
)

// TrackerError provides enhanced error information
type TrackerError struct {
	Type      ErrorType              `json:"type"`
	Message   string                 `json:"message"`
	Operation string                 `json:"operation"`
	Timestamp time.Time              `json:"timestamp"`
	Context   map[string]interface{} `json:"context,omitempty"`
	Retryable bool                   `json:"retryable"`
}

func (e *TrackerError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Type, e.Operation, e.Message)
}

// NewTrackerError creates a new TrackerError
func NewTrackerError(errType ErrorType, operation, message string, retryable bool) *TrackerError {
	return &TrackerError{
		Type:      errType,
		Message:   message,
		Operation: operation,
		Timestamp: time.Now().UTC(),
		Retryable: retryable,
		Context:   make(map[string]interface{}),
	}
}

// Circuit breaker for handling connection failures
type CircuitBreaker struct {
	maxFailures  int
	resetTimeout time.Duration
	failures     int32
	lastFailTime int64
	state        int32 // 0=closed, 1=open, 2=half-open
	mu           sync.RWMutex
}

func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) Execute(operation func() error) error {
	if !cb.canExecute() {
		return NewTrackerError(ErrorTypeConnection, "circuit_breaker", "circuit breaker is open", true)
	}

	err := operation()
	if err != nil {
		cb.recordFailure()
		return err
	}

	cb.recordSuccess()
	return nil
}

func (cb *CircuitBreaker) canExecute() bool {
	state := atomic.LoadInt32(&cb.state)
	if state == 0 { // closed
		return true
	}

	if state == 1 { // open
		lastFail := atomic.LoadInt64(&cb.lastFailTime)
		if time.Since(time.Unix(0, lastFail)) > cb.resetTimeout {
			// Try to move to half-open
			if atomic.CompareAndSwapInt32(&cb.state, 1, 2) {
				return true
			}
		}
		return false
	}

	return true // half-open
}

func (cb *CircuitBreaker) recordFailure() {
	failures := atomic.AddInt32(&cb.failures, 1)
	atomic.StoreInt64(&cb.lastFailTime, time.Now().UnixNano())

	if int(failures) >= cb.maxFailures {
		atomic.StoreInt32(&cb.state, 1) // open
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	atomic.StoreInt32(&cb.failures, 0)
	atomic.StoreInt32(&cb.state, 0) // closed
}

////////////////////////////////////////
// Utility Functions
////////////////////////////////////////

// maxInt returns the larger of two integers

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

// ////////////////////////////////////// ClickHouse
// Connect initiates the primary connection to the range of provided URLs
func (i *ClickhouseService) connect() error {
	// Initialize circuit breaker for connection reliability
	if i.circuitBreaker == nil {
		i.circuitBreaker = NewCircuitBreaker(5, 30*time.Second)
	}

	err := fmt.Errorf("Could not connect to ClickHouse")

	// Build connection options with performance optimizations
	opts := &clickhouse.Options{
		Addr: i.Configuration.Hosts,
		Auth: clickhouse.Auth{
			Database: i.Configuration.Context,
			Username: i.Configuration.Username,
			Password: i.Configuration.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":           60,
			"max_block_size":               10000,
			"max_insert_block_size":        1048576,
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   10000000,
			"async_insert_busy_timeout_ms": 200,
			"max_threads":                  0, // Auto-detect
			"send_logs_level":              "error",
			"enable_http_compression":      1,
			"http_zlib_compression_level":  1,
		},
		DialTimeout:          time.Duration(i.Configuration.Timeout) * time.Millisecond,
		MaxOpenConns:         maxInt(i.Configuration.Connections, 10),
		MaxIdleConns:         maxInt(i.Configuration.Connections/2, 5),
		ConnMaxLifetime:      time.Hour * 2,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// Add SSL configuration if provided
	if i.Configuration.CACert != "" {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: !i.Configuration.Secure,
		}
	}

	// Establish connection with circuit breaker protection
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Starting ClickHouse connection to hosts: %v\n", i.Configuration.Hosts)
		fmt.Printf("[DEBUG] Using database: %s, username: %s\n", i.Configuration.Context, i.Configuration.Username)
		fmt.Printf("[DEBUG] Connection timeout: %v\n", time.Duration(i.Configuration.Timeout)*time.Millisecond)
	}

	err = i.circuitBreaker.Execute(func() error {
		if i.AppConfig.Debug {
			fmt.Println("[DEBUG] Creating ClickHouse connection...")
		}

		var connErr error
		if conn, connErr := clickhouse.Open(opts); connErr != nil {
			globalMetrics.UpdateErrorCount()
			fmt.Println("[ERROR] Connecting to ClickHouse:", connErr)
			if i.AppConfig.Debug {
				fmt.Printf("[DEBUG] Connection failed with options: %+v\n", opts)
			}
			return NewTrackerError(ErrorTypeConnection, "connect", connErr.Error(), true)
		} else {
			if i.AppConfig.Debug {
				fmt.Println("[DEBUG] ClickHouse connection created successfully")
			}
			i.Session = &conn
		}

		// Test connection with timeout
		if i.AppConfig.Debug {
			fmt.Println("[DEBUG] Testing ClickHouse connection with ping...")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if connErr = (*i.Session).Ping(ctx); connErr != nil {
			globalMetrics.UpdateErrorCount()
			fmt.Println("[ERROR] Pinging ClickHouse:", connErr)
			if i.AppConfig.Debug {
				fmt.Printf("[DEBUG] Ping failed after connection established\n")
			}
			return NewTrackerError(ErrorTypeConnection, "ping", connErr.Error(), true)
		}

		if i.AppConfig.Debug {
			fmt.Println("[DEBUG] ClickHouse ping successful")
		}

		return nil
	})

	if err != nil {
		globalMetrics.mu.Lock()
		globalMetrics.HealthStatus = "connection_failed"
		globalMetrics.mu.Unlock()
		return err
	}

	// Update connection count and health status
	atomic.AddInt32(&globalMetrics.ConnectionCount, 1)
	globalMetrics.mu.Lock()
	globalMetrics.HealthStatus = "connected"
	globalMetrics.mu.Unlock()

	// Start health check monitoring
	i.startHealthMonitoring()

	// Initialize and start batch manager
	i.batchingEnabled = true // Re-enabled with UUID null handling fix
	if i.batchingEnabled {
		if i.AppConfig.Debug {
			fmt.Println("[DEBUG] Creating batch manager...")
		}
		i.batchManager = NewBatchManager(*i.Session, i.AppConfig)
		if i.AppConfig.Debug {
			fmt.Println("[DEBUG] Starting batch manager synchronously...")
		}
		if err := i.batchManager.Start(); err != nil {
			fmt.Printf("[WARNING] Failed to start batch manager: %v\n", err)
			i.batchingEnabled = false
		} else {
			if i.AppConfig.Debug {
				fmt.Println("[DEBUG] Batch manager started successfully")
			}
			fmt.Println("✅ Batch Manager initialized for optimal ClickHouse performance")
		}
	}

	i.Configuration.Session = i

	//Setup rand
	rand.Seed(time.Now().UTC().UnixNano())

	//Setup limit checker (ClickHouse)
	if i.AppConfig.ProxyDailyLimit > 0 && i.AppConfig.ProxyDailyLimitCheck == nil && i.AppConfig.ProxyDailyLimitChecker == SERVICE_TYPE_CLICKHOUSE {
		i.AppConfig.ProxyDailyLimitCheck = func(ip string) uint64 {
			var total uint64
			if err := (*i.Session).QueryRow(context.Background(), `SELECT total FROM dailies WHERE ip=? AND day=?`, ip, time.Now().UTC().Format("2006-01-02")).Scan(&total); err != nil {
				return 0xFFFFFFFFFFFFFFFF
			}
			return total
		}
	}
	return nil
}

// ////////////////////////////////////// ClickHouse
// Close will terminate the session to the backend, returning error if an issue arises
func (i *ClickhouseService) close() error {
	// Stop batch manager first (to flush remaining batches)
	if i.batchManager != nil {
		if err := i.batchManager.Stop(); err != nil {
			fmt.Printf("[WARNING] Error stopping batch manager: %v\n", err)
		}
	}

	// Stop health monitoring
	i.stopHealthMonitoring()

	// Update connection count
	atomic.AddInt32(&globalMetrics.ConnectionCount, -1)

	// Close session
	if i.Session != nil {
		err := (*i.Session).Close()
		if err != nil {
			globalMetrics.UpdateErrorCount()
			return NewTrackerError(ErrorTypeConnection, "close", err.Error(), false)
		}

		globalMetrics.mu.Lock()
		globalMetrics.HealthStatus = "disconnected"
		globalMetrics.mu.Unlock()
	}
	return nil
}

// startHealthMonitoring begins periodic health checks
func (i *ClickhouseService) startHealthMonitoring() {
	if i.healthTicker != nil {
		return // Already running
	}

	i.healthTicker = time.NewTicker(30 * time.Second)
	i.healthDone = make(chan bool)

	go func() {
		for {
			select {
			case <-i.healthTicker.C:
				i.performHealthCheck()
			case <-i.healthDone:
				return
			}
		}
	}()
}

// stopHealthMonitoring stops the health check ticker
func (i *ClickhouseService) stopHealthMonitoring() {
	if i.healthTicker != nil {
		i.healthTicker.Stop()
		close(i.healthDone)
		i.healthTicker = nil
	}
}

// performHealthCheck executes a health check
func (i *ClickhouseService) performHealthCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result uint8
	err := (*i.Session).QueryRow(ctx, "SELECT 1").Scan(&result)

	globalMetrics.mu.Lock()
	if err != nil {
		globalMetrics.HealthStatus = "unhealthy"
		globalMetrics.UpdateErrorCount()
		if i.AppConfig.Debug {
			fmt.Printf("[HEALTH] ClickHouse health check failed: %v\n", err)
		}
	} else if result == 1 {
		globalMetrics.HealthStatus = "healthy"
	} else {
		globalMetrics.HealthStatus = "degraded"
	}
	globalMetrics.mu.Unlock()
}

func (i *ClickhouseService) listen() error {
	//TODO: Listen for ClickHouse triggers
	return fmt.Errorf("[ERROR] ClickHouse listen not implemented")
}

func (i *ClickhouseService) auth(s *ServiceArgs) error {
	//TODO: AG implement JWT
	//TODO: AG implement creds (check domain level auth)
	if *s.Values == nil {
		return fmt.Errorf("User not provided")
	}
	uid := (*s.Values)["uid"]
	if uid == "" {
		return fmt.Errorf("User ID not provided")
	}
	password := (*s.Values)["password"]
	if password == "" {
		return fmt.Errorf("User pass not provided")
	}
	var pwd string
	if err := (*i.Session).QueryRow(context.Background(), `SELECT pwd FROM accounts WHERE uid=?`, uid).Scan(&pwd); err == nil {
		if pwd != sha(password) {
			return fmt.Errorf("Bad pass")
		}
		return nil
	} else {
		return err
	}
}

func (i *ClickhouseService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	// Handle metrics endpoint
	if r.URL.Path == "/metrics" || r.URL.Path == "/health" {
		return i.handleMetricsEndpoint(w, r)
	}

	// Handle health check endpoint
	if r.URL.Path == "/ping" {
		return i.handlePingEndpoint(w, r)
	}
	ctx := context.Background()

	switch s.ServiceType {
	case SVC_POST_AGREE:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				created := time.Now().UTC()
				//[hhash]
				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}
				ip := getIP(r)
				var iphash string
				//128 bits = ipv6
				iphash = strconv.FormatInt(int64(hash(ip)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)
				browser := r.Header.Get("user-agent")
				var bhash *string
				if browser != "" {
					temp := strconv.FormatInt(int64(hash(browser)), 36)
					bhash = &temp
				}
				var cflags *int64
				if com, ok := b["cflags"].(int64); ok {
					cflags = &com
				} else if com, ok := b["cflags"].(float64); ok {
					temp := int64(com)
					cflags = &temp
				}
				//[country]
				var country *string
				var region *string
				if tz, ok := b["tz"].(string); ok {
					cleanString(&tz)
					if ct, oktz := countries[tz]; oktz {
						country = &ct
					}
				}
				//[latlon]
				var lat, lon *float64
				latf, oklatf := b["lat"].(float64)
				lonf, oklonf := b["lon"].(float64)
				if oklatf && oklonf {
					lat = &latf
					lon = &lonf
				} else {
					//String
					lats, oklats := b["lat"].(string)
					lons, oklons := b["lon"].(string)
					if oklats && oklons {
						latfp, _ := strconv.ParseFloat(lats, 64)
						lonfp, _ := strconv.ParseFloat(lons, 64)
						lat = &latfp
						lon = &lonfp
					}
				}
				if lat == nil || lon == nil {
					if gip, err := GetGeoIP(net.ParseIP(ip)); err == nil && gip != nil {
						var geoip GeoIP
						if err := json.Unmarshal(gip, &geoip); err == nil && geoip.Latitude != 0 && geoip.Longitude != 0 {
							lat = &geoip.Latitude
							lon = &geoip.Longitude
							if geoip.CountryISO2 != "" {
								country = &geoip.CountryISO2
							}
							if geoip.Region != "" {
								region = &geoip.Region
							}
						}
					}
				}
				//Self identification of geo_pol overrules geoip
				if ct, ok := b["country"].(string); ok {
					country = &ct
				}
				if r, ok := b["region"].(string); ok {
					region = &r
				}
				upperString(country)
				cleanString(region)

				// Convert vid to UUID if present
				var vid *uuid.UUID
				if vidStr, ok := b["vid"].(string); ok {
					if parsedVid, err := uuid.Parse(vidStr); err == nil {
						vid = &parsedVid
					}
				}
				// Convert sid to UUID if present
				var sid *uuid.UUID
				if sidStr, ok := b["sid"].(string); ok {
					if parsedSid, err := uuid.Parse(sidStr); err == nil {
						sid = &parsedSid
					}
				}
				// Convert uid to UUID if present
				var uid *uuid.UUID
				if uidStr, ok := b["uid"].(string); ok {
					if parsedUid, err := uuid.Parse(uidStr); err == nil {
						uid = &parsedUid
					}
				}
				// Convert avid to UUID if present
				var avid *uuid.UUID
				if avidStr, ok := b["avid"].(string); ok {
					if parsedAvid, err := uuid.Parse(avidStr); err == nil {
						avid = &parsedAvid
					}
				}
				// Convert owner to UUID if present
				var owner *uuid.UUID
				if ownerStr, ok := b["owner"].(string); ok {
					if parsedOwner, err := uuid.Parse(ownerStr); err == nil {
						owner = &parsedOwner
					}
				}
				// Convert oid to UUID if present
				var oid *uuid.UUID
				if orgStr, ok := b["oid"].(string); ok {
					if parsedOrg, err := uuid.Parse(orgStr); err == nil {
						oid = &parsedOrg
					}
				}

				// Insert into agreements table
				if err := (*i.Session).Exec(ctx, `INSERT INTO agreements (
					vid, created_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid, 
					country, region, culture, source, medium, campaign, term, ref, rcode, aff, 
					browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					vid, created, cflags, sid, uid, avid, hhash, b["app"], b["rel"], b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"],
					country, region, b["culture"], b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"],
					browser, bhash, b["device"], b["os"], b["tz"], b["w"], b["h"], lat, lon, b["zip"], owner, oid,
				); err != nil {
					return err
				}

				// Insert into agreed table (history)
				if err := (*i.Session).Exec(ctx, `INSERT INTO agreed (
					vid, created_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid, 
					country, region, culture, source, medium, campaign, term, ref, rcode, aff, 
					browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					vid, created, cflags, sid, uid, avid, hhash, b["app"], b["rel"], b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"],
					country, region, b["culture"], b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"],
					browser, bhash, b["device"], b["os"], b["tz"], b["w"], b["h"], lat, lon, b["zip"], owner, oid,
				); err != nil {
					return err
				}

				(*w).WriteHeader(http.StatusOK)
				return nil
			} else {
				return fmt.Errorf("Bad request (data)")
			}
		} else {
			return fmt.Errorf("Bad request (body)")
		}
	case SVC_GET_AGREE:
		var vid string
		if len(r.URL.Query()["vid"]) > 0 {
			vid = r.URL.Query()["vid"][0]
			rows, err := (*i.Session).Query(ctx, `SELECT * FROM agreements WHERE vid=?`, vid)
			if err != nil {
				return err
			}
			defer rows.Close()

			var results []map[string]interface{}
			for rows.Next() {
				row := make(map[string]interface{})
				if err := rows.ScanStruct(&row); err != nil {
					continue
				}
				results = append(results, row)
			}

			js, err := json.Marshal(results)
			(*w).WriteHeader(http.StatusOK)
			(*w).Header().Set("Content-Type", "application/json")
			(*w).Write(js)
			return err
		} else {
			(*w).WriteHeader(http.StatusNotFound)
			(*w).Header().Set("Content-Type", "application/json")
			(*w).Write([]byte("[]"))
		}
		return nil
	case SVC_GET_JURISDICTIONS:
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM jurisdictions`)
		if err != nil {
			return err
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			row := make(map[string]interface{})
			if err := rows.ScanStruct(&row); err != nil {
				continue
			}
			results = append(results, row)
		}

		js, err := json.Marshal(results)
		(*w).WriteHeader(http.StatusOK)
		(*w).Header().Set("Content-Type", "application/json")
		(*w).Write(js)
		return err
	case SVC_GET_GEOIP:
		ip := getIP(r)
		if len(r.URL.Query()["ip"]) > 0 {
			ip = r.URL.Query()["ip"][0]
		}
		pip := net.ParseIP(ip)
		if gip, err := GetGeoIP(pip); err == nil && gip != nil {
			(*w).WriteHeader(http.StatusOK)
			(*w).Header().Set("Content-Type", "application/json")
			(*w).Write(gip)
			return nil
		} else {
			if err == nil {
				return fmt.Errorf("Not Found (IP)")
			}
			return err
		}
	case SVC_GET_REDIRECTS:
		if err := i.auth(s); err != nil {
			return err
		}
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM redirect_history`)
		if err != nil {
			return err
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			row := make(map[string]interface{})
			if err := rows.ScanStruct(&row); err != nil {
				continue
			}
			results = append(results, row)
		}

		json, _ := json.Marshal(map[string]interface{}{"results": results})
		(*w).Header().Set("Content-Type", "application/json")
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(json)
		return nil
	case SVC_GET_REDIRECT:
		//TODO: AG ADD CACHE
		var redirect string
		if err := (*i.Session).QueryRow(ctx, `SELECT urlto FROM redirects WHERE urlfrom=?`, getFullURL(r)).Scan(&redirect); err == nil {
			s.Values = &map[string]string{"Redirect": redirect}
			http.Redirect(*w, r, redirect, http.StatusFound)
			return nil
		} else {
			return err
		}
	case SVC_POST_REDIRECT:
		if err := i.auth(s); err != nil {
			return err
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				updated := time.Now().UTC()
				urlfrom := strings.ToLower(strings.TrimSpace(b["urlfrom"].(string)))
				urlto := strings.TrimSpace(b["urlto"].(string))
				if urlfrom == "" || urlto == "" {
					return fmt.Errorf("Bad URL (null)")
				}
				if strings.EqualFold(urlfrom, urlto) {
					return fmt.Errorf("Bad URL (equal)")
				}
				var urltoURL url.URL
				if checkTo, err := url.Parse(urlto); err != nil {
					return fmt.Errorf("Bad URL (destination)")
				} else {
					urltoURL = *checkTo
					if !strings.Contains(checkTo.Path, "/rdr/") {
						for _, d := range i.AppConfig.Domains {
							if strings.EqualFold(checkTo.Host, strings.TrimSpace(d)) {
								return fmt.Errorf("Bad URL (self-referential)")
							}
						}
					}
				}
				var urlfromURL url.URL
				if checkFrom, err := url.Parse(urlfrom); err != nil {
					return fmt.Errorf("Bad URL (from)")
				} else {
					urlfromURL = *checkFrom
				}
				if len(urlfromURL.Path) < 2 {
					return fmt.Errorf("Bad URL (from path)")
				}
				//[hhash]
				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}

				// Parse updater UUID
				var updater *uuid.UUID
				if updaterStr, ok := (*s.Values)["uid"]; ok {
					if parsedUpdater, err := uuid.Parse(updaterStr); err == nil {
						updater = &parsedUpdater
					}
				}
				// Parse oid UUID if present
				var oid *uuid.UUID
				if orgStr, ok := b["oid"].(string); ok {
					if parsedOrg, err := uuid.Parse(orgStr); err == nil {
						oid = &parsedOrg
					}
				}

				// Insert into redirects table
				if err := (*i.Session).Exec(ctx, `INSERT INTO redirects (
					hhash, urlfrom, urlto, updated_at, updater, oid
				) VALUES (?, ?, ?, ?, ?, ?)`,
					hhash,
					strings.ToLower(urlfromURL.Host)+strings.ToLower(urlfromURL.Path),
					urlto,
					updated,
					updater,
					oid,
				); err != nil {
					return err
				}

				// Insert into redirect_history table
				if err := (*i.Session).Exec(ctx, `INSERT INTO redirect_history (
					urlfrom, hostfrom, slugfrom, urlto, hostto, pathto, searchto, 
					updater, oid
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					urlfrom,
					strings.ToLower(urlfromURL.Host),
					strings.ToLower(urlfromURL.Path),
					urlto,
					strings.ToLower(urltoURL.Host),
					strings.ToLower(urlfromURL.Path),
					b["searchto"],
					updater,
					oid,
				); err != nil {
					return err
				}
				(*w).WriteHeader(http.StatusOK)
				return nil
			} else {
				return fmt.Errorf("Bad request (data)")
			}
		} else {
			return fmt.Errorf("Bad request (body)")
		}
	default:
		return fmt.Errorf("[ERROR] ClickHouse service not implemented %d", s.ServiceType)
	}
}

// ////////////////////////////////////// ClickHouse
func (i *ClickhouseService) prune() error {
	ctx := context.Background()
	var lastCreated time.Time
	var err error

	if !i.AppConfig.PruneLogsOnly {
		for _, p := range i.Configuration.Prune {
			var pruned = 0
			var total = 0
			var pageSize = 5000
			if p.PageSize > 1 {
				pageSize = p.PageSize
			}

			query := ""
			switch p.Table {
			case "visitors", "sessions", "events", "events_recent":
				query = fmt.Sprintf(`SELECT * FROM %s ORDER BY created_at LIMIT %d OFFSET %d`, p.Table, pageSize, total)
			default:
				err = fmt.Errorf("Table %s not supported for pruning", p.Table)
				continue
			}

			for {
				rows, queryErr := (*i.Session).Query(ctx, query)
				if queryErr != nil {
					fmt.Printf("[[WARNING]] ERROR READING ROWS [%s] %v\n", p.Table, queryErr)
					break
				}

				rowCount := 0
				for rows.Next() {
					row := make(map[string]interface{})
					if err := rows.ScanStruct(&row); err != nil {
						fmt.Printf("[[WARNING]] ERROR SCANNING ROW [%s] %v\n", p.Table, err)
						continue
					}
					total += 1
					rowCount += 1

					//CHECK IF ALREADY CLEANED
					// Note: updated_at field was removed from schema, so we skip this check

					//PROCESS THE ROW
					expired, created := checkRowExpiredClickHouse(row, p, i.AppConfig.PruneSkipToTimestamp)
					if expired {
						pruned += 1
						if created.After(lastCreated) {
							lastCreated = *created
						}

						if p.ClearAll {
							switch p.Table {
							case "visitors":
								(*i.Session).Exec(ctx, `ALTER TABLE visitors DELETE WHERE vid=?`, row["vid"])
							case "sessions":
								(*i.Session).Exec(ctx, `ALTER TABLE sessions DELETE WHERE vid=? AND sid=?`, row["vid"], row["sid"])
							case "events", "events_recent":
								(*i.Session).Exec(ctx, `ALTER TABLE ? DELETE WHERE eid=?`, p.Table, row["eid"])
							}
						} else {
							// Update with nullified fields
							update := make([]string, 0)
							for _, f := range p.Fields {
								update = append(update, fmt.Sprintf("%s=NULL", f.Id))
							}
							if len(update) > 0 {
								updateSQL := strings.Join(update, ",")
								switch p.Table {
								case "visitors":
									(*i.Session).Exec(ctx, fmt.Sprintf(`ALTER TABLE visitors UPDATE %s WHERE vid=?`, updateSQL), row["vid"])
								case "sessions":
									(*i.Session).Exec(ctx, fmt.Sprintf(`ALTER TABLE sessions UPDATE %s WHERE vid=? AND sid=?`, updateSQL), row["vid"], row["sid"])
								case "events", "events_recent":
									(*i.Session).Exec(ctx, fmt.Sprintf(`ALTER TABLE %s UPDATE %s WHERE eid=?`, p.Table, updateSQL), row["eid"])
								}
							}
						}
					}
				}
				rows.Close()

				fmt.Printf("Processed %d rows %d pruned\n", total, pruned)
				if i.AppConfig.PruneLimit != 0 && i.AppConfig.PruneLimit > total {
					break
				}
				if rowCount < pageSize {
					break
				}
				// Update offset for next iteration
				query = fmt.Sprintf(`SELECT * FROM %s ORDER BY created_at LIMIT %d OFFSET %d`, p.Table, pageSize, total)
			}
			fmt.Printf("Pruned [ClickHouse].[%s].[%v]: %d/%d rows\n", i.Configuration.Context, p.Table, pruned, total)
		}
	}

	if i.AppConfig.PruneUpdateConfig && lastCreated.Unix() > i.AppConfig.PruneSkipToTimestamp {
		s, error := ioutil.ReadFile(i.AppConfig.ConfigFile)
		var j interface{}
		json.Unmarshal(s, &j)
		SetValueInJSON(j, "PruneSkipToTimestamp", lastCreated.Unix())
		s, _ = json.Marshal(j)
		var prettyJSON bytes.Buffer
		error = json.Indent(&prettyJSON, s, "", "    ")
		if error == nil {
			ioutil.WriteFile(i.AppConfig.ConfigFile, prettyJSON.Bytes(), 0644)
		}
	}

	//Now Prune the LOGS table
	if !i.AppConfig.PruneLogsSkip {
		var pruned = 0
		var total = 0
		var pageSize = 10000
		if i.AppConfig.PruneLogsPageSize > 0 {
			pageSize = i.AppConfig.PruneLogsPageSize
		}
		ttl := 2592000
		if i.AppConfig.PruneLogsTTL > 0 {
			ttl = i.AppConfig.PruneLogsTTL
		}

		query := fmt.Sprintf(`SELECT id, created_at FROM logs ORDER BY created_at LIMIT %d OFFSET %d`, pageSize, total)
		for {
			rows, queryErr := (*i.Session).Query(ctx, query)
			if queryErr != nil {
				break
			}

			rowCount := 0
			for rows.Next() {
				var id uuid.UUID
				var createdAt time.Time
				if err := rows.Scan(&id, &createdAt); err != nil {
					continue
				}
				total += 1
				rowCount += 1

				//PROCESS THE ROW
				expired := checkIdExpiredClickHouse(&id, &createdAt, ttl)
				if expired {
					pruned += 1
					(*i.Session).Exec(ctx, `ALTER TABLE logs DELETE WHERE id=?`, id)
				}
			}
			rows.Close()

			fmt.Printf("Processed %d rows %d pruned\n", total, pruned)
			if rowCount < pageSize {
				break
			}
			query = fmt.Sprintf(`SELECT id, created_at FROM logs ORDER BY created_at LIMIT %d OFFSET %d`, pageSize, total)
		}
		fmt.Printf("Pruned [ClickHouse].[%s].[logs]: %d/%d rows\n", i.Configuration.Context, pruned, total)
	}
	return err
}

// ////////////////////////////////////// ClickHouse
func (i *ClickhouseService) write(w *WriteArgs) error {
	ctx := context.Background()
	err := fmt.Errorf("Could not write to any ClickHouse server in cluster")
	v := *w.Values

	switch w.WriteType {
	case WRITE_COUNT:
		if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_COUNTERS)) {
			if i.AppConfig.Debug {
				fmt.Printf("COUNT %s\n", w)
			}
			return (*i.Session).Exec(ctx, `INSERT INTO counters (id, total, date) VALUES (?, 1, today())`,
				v["id"])
		}
		return nil
	case WRITE_UPDATE:
		if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_UPDATES)) {
			if i.AppConfig.Debug {
				fmt.Printf("UPDATE %s\n", w)
			}
			timestamp := time.Now().UTC()
			updated, ok := v["updated"].(string)
			if ok {
				millis, err := strconv.ParseInt(updated, 10, 64)
				if err == nil {
					timestamp = time.Unix(0, millis*int64(time.Millisecond))
				}
			}
			return (*i.Session).Exec(ctx, `INSERT INTO updates (id, updated_at, msg) VALUES (?, ?, ?)`,
				v["id"],
				timestamp,
				v["msg"])
		}
		return nil
	case WRITE_LOG:
		if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_LOGS)) {
			if i.AppConfig.Debug {
				fmt.Printf("LOG %s\n", w)
			}
			//////////////////////////////////////////////
			//FIX VARS
			//////////////////////////////////////////////
			//[params]
			var params *map[string]interface{}
			if ps, ok := v["params"].(string); ok {
				temp := make(map[string]interface{})
				json.Unmarshal([]byte(ps), &temp)
				params = &temp
			} else if ps, ok := v["params"].(map[string]interface{}); ok {
				params = &ps
			}
			//[ltimenss] ltime as nanosecond string
			var ltime time.Time
			if lts, ok := v["ltimenss"].(string); ok {
				ns, _ := strconv.ParseInt(lts, 10, 64)
				ltime = time.Unix(0, ns)
			}
			//[level]
			var level *int32
			if lvl, ok := v["level"].(float64); ok {
				temp := int32(lvl)
				level = &temp
			}

			var topic string
			if ttemp1, ok := v["topic"].(string); ok {
				topic = ttemp1
			} else {
				if ttemp2, ok2 := v["id"].(string); ok2 {
					topic = ttemp2
				}
			}

			cleanInterfaceString(v["ip"])
			cleanInterfaceString(v["topic"])
			cleanInterfaceString(v["name"])
			cleanInterfaceString(v["host"])
			cleanInterfaceString(v["hostname"])
			cleanInterfaceString(v["msg"])

			var iphash string
			if temp, ok := v["ip"].(string); ok && temp != "" {
				//128 bits = ipv6
				iphash = strconv.FormatInt(int64(hash(temp)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
			}

			// Parse owner UUID if present
			var owner *uuid.UUID
			if ownerStr, ok := v["owner"].(string); ok {
				if parsedOwner, err := uuid.Parse(ownerStr); err == nil {
					owner = &parsedOwner
				}
			}

			return (*i.Session).Exec(ctx, `INSERT INTO logs
			  (
				  id,
				  ldate,
				  created_at,
				  ltime,
				  topic, 
				  name, 
				  host, 
				  hostname, 
				  owner,
				  ip,
				  iphash,
				  level, 
				  msg,
				  params,
				  oid
			  ) 
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, //15
				uuid.Must(uuid.NewUUID()),
				v["ldate"],
				time.Now().UTC(),
				ltime,
				topic,
				v["name"],
				v["host"],
				v["hostname"],
				owner,
				v["ip"],
				iphash,
				level,
				v["msg"],
				jsonOrNull(params),
				parseUUID(v["oid"]))
		}
		return nil
	case WRITE_EVENT:
		return i.writeEvent(ctx, w, v)
	case WRITE_LTV:
		return i.writeLTV(ctx, w, v)
	default:
		//TODO: Manually run query via query in config.json
		if i.AppConfig.Debug {
			fmt.Printf("UNHANDLED %s\n", w)
		}
	}

	return err
}

// Helper function for expired row checking in ClickHouse
func checkRowExpiredClickHouse(row map[string]interface{}, p Prune, skipTimestamp int64) (bool, *time.Time) {
	// Simplified expiration check - implement according to your pruning logic
	if createdAt, ok := row["created_at"].(time.Time); ok {
		if createdAt.Unix() < skipTimestamp {
			return false, &createdAt
		}
		// Check if row is older than configured retention period
		if time.Since(createdAt) > time.Duration(p.TTL)*time.Second {
			return true, &createdAt
		}
	}
	return false, nil
}

// Helper function for expired ID checking in ClickHouse
func checkIdExpiredClickHouse(id *uuid.UUID, idCreated *time.Time, ttl int) bool {
	if idCreated == nil {
		return false
	}
	return time.Since(*idCreated) > time.Duration(ttl)*time.Second
}

// writeEvent handles WRITE_EVENT with integrated campaign telemetry functionality
// handleMetricsEndpoint serves performance metrics as JSON
func (i *ClickhouseService) handleMetricsEndpoint(w *http.ResponseWriter, r *http.Request) error {
	metrics := globalMetrics.GetMetricsSnapshot()

	// Get batch metrics if available
	var batchMetrics BatchMetrics
	if i.batchManager != nil {
		batchMetrics = i.batchManager.GetMetrics()
	}

	// Add calculated fields
	responseData := map[string]interface{}{
		"metrics": metrics,
		"batching": map[string]interface{}{
			"enabled":              i.batchingEnabled,
			"total_batches":        batchMetrics.TotalBatches,
			"total_items":          batchMetrics.TotalItems,
			"failed_batches":       batchMetrics.FailedBatches,
			"avg_batch_size":       batchMetrics.AvgBatchSize,
			"avg_flush_latency_ms": batchMetrics.AvgFlushLatencyMs,
			"queued_items":         batchMetrics.QueuedItems,
			"memory_usage_mb":      batchMetrics.MemoryUsageMB,
			"last_flush_time":      time.Unix(batchMetrics.LastFlushTime, 0).Format(time.RFC3339),
		},
		"calculated": map[string]interface{}{
			"avg_latency_ms":      float64(metrics.TotalLatency) / float64(time.Millisecond) / math.Max(float64(metrics.EventCount), 1),
			"error_rate":          float64(metrics.ErrorCount) / math.Max(float64(metrics.EventCount), 1),
			"uptime_seconds":      time.Now().Unix() - metrics.StartTime,
			"events_per_second":   float64(metrics.EventCount) / math.Max(float64(time.Now().Unix()-metrics.StartTime), 1),
			"batch_success_rate":  float64(batchMetrics.TotalBatches-batchMetrics.FailedBatches) / math.Max(float64(batchMetrics.TotalBatches), 1),
			"avg_items_per_batch": float64(batchMetrics.TotalItems) / math.Max(float64(batchMetrics.TotalBatches), 1),
		},
		"timestamp": time.Now().UTC(),
	}

	(*w).Header().Set("Content-Type", "application/json")
	(*w).WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(*w)
	return encoder.Encode(responseData)
}

// handlePingEndpoint provides a simple health check
func (i *ClickhouseService) handlePingEndpoint(w *http.ResponseWriter, r *http.Request) error {
	status := "ok"
	statusCode := http.StatusOK

	globalMetrics.mu.RLock()
	healthStatus := globalMetrics.HealthStatus
	globalMetrics.mu.RUnlock()

	if healthStatus != "healthy" && healthStatus != "connected" {
		status = "degraded"
		statusCode = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":    status,
		"health":    healthStatus,
		"timestamp": time.Now().UTC(),
	}

	(*w).Header().Set("Content-Type", "application/json")
	(*w).WriteHeader(statusCode)

	encoder := json.NewEncoder(*w)
	return encoder.Encode(response)
}

// convertParamsToTypes converts param values to proper Go types in place
func convertParamsToTypes(params *map[string]interface{}) {
	for npk, npv := range *params {
		// Handle existing numeric types - keep as-is
		if _, ok := npv.(float64); ok {
			continue
		}
		if _, ok := npv.(int); ok {
			continue
		}
		if _, ok := npv.(int64); ok {
			continue
		}

		// Handle existing bool values - keep as boolean
		if _, ok := npv.(bool); ok {
			continue
		}

		// Handle string values - attempt type conversion
		if nps, ok := npv.(string); ok {
			nps = strings.TrimSpace(nps)
			if nps == "" {
				(*params)[npk] = ""
				continue
			}

			// Try boolean conversion
			if strings.ToLower(nps) == "true" {
				(*params)[npk] = true
				continue
			}
			if strings.ToLower(nps) == "false" {
				(*params)[npk] = false
				continue
			}

			// Try integer conversion first
			if npint, err := strconv.ParseInt(nps, 10, 64); err == nil {
				// Check if it might be a timestamp (large number)
				if npint > 946684800000 { // Milliseconds after year 2000
					(*params)[npk] = npint // Keep as timestamp
				} else if npint > 946684800 { // Seconds after year 2000
					(*params)[npk] = npint * 1000 // Convert to milliseconds
				} else {
					(*params)[npk] = npint // Regular integer
				}
				continue
			}

			// Try float conversion
			if npf, err := strconv.ParseFloat(nps, 64); err == nil {
				(*params)[npk] = npf
				continue
			}

			// Try date/time conversion to UTC milliseconds
			if t, err := time.Parse(time.RFC3339, nps); err == nil {
				(*params)[npk] = t.UTC().UnixMilli()
				continue
			}

			// Try other common date formats
			dateFormats := []string{
				"2006-01-02T15:04:05Z", // RFC3339 without nanoseconds
				"2006-01-02 15:04:05",  // SQL datetime
				"2006-01-02T15:04:05",  // ISO without timezone
				"2006-01-02",           // Date only
				"01/02/2006",           // US format
				"02/01/2006",           // EU format
			}

			for _, format := range dateFormats {
				if t, err := time.Parse(format, nps); err == nil {
					(*params)[npk] = t.UTC().UnixMilli()
					goto nextParam
				}
			}

		nextParam:
			// Try to parse as JSON object/array
			if len(nps) > 1 && ((nps[0] == '{' && nps[len(nps)-1] == '}') || (nps[0] == '[' && nps[len(nps)-1] == ']')) {
				var jsonObj interface{}
				if err := json.Unmarshal([]byte(nps), &jsonObj); err == nil {
					// Recursively convert nested objects
					if subMap, ok := jsonObj.(map[string]interface{}); ok {
						convertParamsToTypes(&subMap)
						(*params)[npk] = subMap
					} else {
						(*params)[npk] = jsonObj
					}
					continue
				}
			}

			// Keep as string if no conversion possible
			(*params)[npk] = nps
		} else {
			// Handle maps and slices recursively
			if subMap, ok := npv.(map[string]interface{}); ok {
				// Recursively convert nested map
				convertParamsToTypes(&subMap)
				(*params)[npk] = subMap
				continue
			}

			// Handle other types - convert to string
			(*params)[npk] = fmt.Sprintf("%+v", npv)
		}
	}
}

// jsonOrNull converts a map to JSON string for ClickHouse JSON type
// TODO: could use []byte("null") or json.RawMessage("null") in the place of json.RawMessage("{}")
func jsonOrNull(m interface{}) interface{} {
	if m == nil {
		return json.RawMessage("{}")
	}
	switch v := m.(type) {
	case *map[string]interface{}:
		if v == nil {
			return json.RawMessage("{}")
		}
		if jsonBytes, err := json.Marshal(*v); err == nil {
			return json.RawMessage(jsonBytes)
		}
		return json.RawMessage("{}")
	case *map[string]float64:
		if v == nil {
			return json.RawMessage("{}")
		}
		if jsonBytes, err := json.Marshal(*v); err == nil {
			return json.RawMessage(jsonBytes)
		}
		return json.RawMessage("{}")
	case map[string]interface{}:
		if jsonBytes, err := json.Marshal(v); err == nil {
			return json.RawMessage(jsonBytes)
		}
		return json.RawMessage("{}")
	case map[string]float64:
		if jsonBytes, err := json.Marshal(v); err == nil {
			return json.RawMessage(jsonBytes)
		}
		return json.RawMessage("{}")
	case string:
		// If it's already a JSON string
		if v == "" {
			return json.RawMessage("{}")
		}
		return json.RawMessage(v)
	case json.RawMessage:
		// Return RawMessage as-is
		if len(v) == 0 {
			return json.RawMessage("{}")
		}
		return v
	default:
		if v == nil {
			return json.RawMessage("{}")
		}
		// Try to marshal other types
		if jsonBytes, err := json.Marshal(v); err == nil {
			return json.RawMessage(jsonBytes)
		}
		return json.RawMessage("{}")
	}
}

// batchInsert adds an item to the batch manager or executes directly if batching is disabled
func (i *ClickhouseService) batchInsert(tableName, sql string, args []interface{}, data map[string]interface{}) error {
	if i.batchingEnabled && i.batchManager != nil {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Adding item to batch for table: %s\n", tableName)
		}
		item := BatchItem{
			TableName: tableName,
			SQL:       sql,
			Args:      args,
			Data:      data,
		}
		err := i.batchManager.AddItem(item)
		if i.AppConfig.Debug {
			if err != nil {
				fmt.Printf("[DEBUG] Batch add failed for table %s: %v\n", tableName, err)
			} else {
				fmt.Printf("[DEBUG] Batch add successful for table: %s\n", tableName)
			}
		}
		return err
	}

	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Using direct insert for table: %s (batching disabled)\n", tableName)
	}
	// Fallback to direct insert
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := (*i.Session).Exec(ctx, sql, args...)
	if i.AppConfig.Debug {
		if err != nil {
			fmt.Printf("[DEBUG] Direct insert failed for table %s: %v\n", tableName, err)
		} else {
			fmt.Printf("[DEBUG] Direct insert successful for table: %s\n", tableName)
		}
	}
	return err
}

func (i *ClickhouseService) writeEvent(ctx context.Context, w *WriteArgs, v map[string]interface{}) error {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		globalMetrics.UpdateLatency(latency)
		globalMetrics.UpdateEventCount()
	}()
	//////////////////////////////////////////////
	//FIX CASE
	//////////////////////////////////////////////
	delete(v, "cleanIP")
	cleanString(&(w.Browser))
	cleanString(&(w.Host))
	cleanInterfaceString(v["app"])
	cleanInterfaceString(v["rel"])
	cleanInterfaceString(v["ptyp"])
	cleanInterfaceString(v["xid"])
	cleanInterfaceString(v["split"])
	cleanInterfaceString(v["ename"])
	cleanInterfaceString(v["etyp"])
	cleanInterfaceString(v["sink"])
	cleanInterfaceString(v["source"])
	cleanInterfaceString(v["medium"])
	cleanInterfaceString(v["campaign"])
	cleanInterfaceString(v["term"])
	cleanInterfaceString(v["rcode"])
	cleanInterfaceString(v["aff"])
	cleanInterfaceString(v["device"])
	cleanInterfaceString(v["os"])
	cleanInterfaceString(v["relation"])

	//////////////////////////////////////////////
	//FIX VARS
	//////////////////////////////////////////////
	//[hhash]
	var hhash *string
	if w.Host != "" {
		temp := strconv.FormatInt(int64(hash(w.Host)), 36)
		hhash = &temp
	}
	//[iphash]
	var iphash string
	if w.IP != "" {
		//128 bits = ipv6
		iphash = strconv.FormatInt(int64(hash(w.IP)), 36)
		iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
		iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
		iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
	}
	//check host account id
	//don't track without it
	//SEVERELY LIMITING SO DON'T USE IT
	var hAccountID *string
	if w.Host != "" && i.AppConfig.AccountHashMixer != "" {
		temp := strconv.FormatInt(int64(hash(w.Host+i.AppConfig.AccountHashMixer)), 36)
		hAccountID = &temp
		if v["acct"].(string) != *hAccountID {
			err := fmt.Errorf("[ERROR] Host: %s Account-ID: %s Incorrect for (acct): %s", w.Host, *hAccountID, v["acct"])
			return err
		}
	}
	//[updated]
	updated := time.Now().UTC()

	// Parse UUID fields
	var vid, sid, uid, auth, rid, oid, paymentID *uuid.UUID
	if temp, ok := v["vid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			vid = &parsed
		}
	}
	if temp, ok := v["sid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			sid = &parsed
		}
	}
	if temp, ok := v["uid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			uid = &parsed
		}
	}
	if temp, ok := v["auth"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			auth = &parsed
		}
	}
	if temp, ok := v["rid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			rid = &parsed
		}
	}
	if temp, ok := v["oid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			oid = &parsed
		}
	}
	if temp, ok := v["payment_id"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			paymentID = &parsed
		}
	}

	//[country]
	var country, region, city *string
	zip := v["zip"]
	ensureInterfaceString(zip)
	if tz, ok := v["tz"].(string); ok {
		if ct, oktz := countries[tz]; oktz {
			country = &ct
		}
	}
	//[latlon]
	var lat, lon *float64
	latf, oklatf := v["lat"].(float64)
	lonf, oklonf := v["lon"].(float64)
	if oklatf && oklonf {
		lat = &latf
		lon = &lonf
	} else {
		//String
		lats, oklats := v["lat"].(string)
		lons, oklons := v["lon"].(string)
		if oklats && oklons {
			latfp, _ := strconv.ParseFloat(lats, 64)
			lonfp, _ := strconv.ParseFloat(lons, 64)
			lat = &latfp
			lon = &lonfp
		}
	}
	if lat == nil || lon == nil {
		if gip, err := GetGeoIP(net.ParseIP(w.IP)); err == nil && gip != nil {
			var geoip GeoIP
			if err := json.Unmarshal(gip, &geoip); err == nil && geoip.Latitude != 0 && geoip.Longitude != 0 {
				lat = &geoip.Latitude
				lon = &geoip.Longitude
				if geoip.CountryISO2 != "" {
					country = &geoip.CountryISO2
				}
				if geoip.Region != "" {
					region = &geoip.Region
				}
				if geoip.City != "" {
					city = &geoip.City
				}
				if zip == nil && geoip.Zip != "" {
					zip = &geoip.Zip
				}
			}
		}
	}
	if !i.AppConfig.UseRegionDescriptions {
		//country = nil
		region = nil
		city = nil
	}
	//Self identification of geo_pol overrules geoip
	if ct, ok := v["country"].(string); ok {
		country = &ct
	}
	if r, ok := v["region"].(string); ok {
		region = &r
	}
	if r, ok := v["city"].(string); ok {
		city = &r
	}
	upperString(country)
	cleanString(region)
	cleanString(city)

	//[duration]
	var duration *int64
	if d, ok := v["duration"].(string); ok {
		temp, _ := strconv.ParseInt(d, 10, 64)
		duration = &temp
	}
	if d, ok := v["duration"].(float64); ok {
		temp := int64(d)
		duration = &temp
	}
	//[ver]
	var version *int32
	if ver, ok := v["version"].(string); ok {
		temp, _ := strconv.ParseInt(ver, 10, 32)
		temp32 := int32(temp)
		version = &temp32
	}
	if ver, ok := v["version"].(float64); ok {
		temp := int32(ver)
		version = &temp
	}
	//[cflags] - compliance flags
	var cflags *int64
	if com, ok := v["cflags"].(int64); ok {
		cflags = &com
	} else if com, ok := v["cflags"].(float64); ok {
		temp := int64(com)
		cflags = &temp
	}
	//[bhash]
	var bhash *string
	if w.Browser != "" {
		temp := strconv.FormatInt(int64(hash(w.Browser)), 36)
		bhash = &temp
	}
	//[score]
	var score *float64
	if s, ok := v["score"].(string); ok {
		temp, _ := strconv.ParseFloat(s, 64)
		score = &temp
	} else if s, ok := v["score"].(float64); ok {
		score = &s
	}

	//Exclude the following from **all** params in events,visitors and sessions. Note: further exclusions after events insert.
	//[params]
	var params *map[string]interface{}
	if ps, ok := v["params"].(string); ok {
		json.Unmarshal([]byte(ps), &params)
	} else if ps, ok := v["params"].(map[string]interface{}); ok {
		params = &ps
	}
	if params != nil {
		//De-identify data
		excludeFields := []string{
			"uri", "hhash", "iphash", "cell", "chash", "email", "ehash", "uname", "acct",
			"first", "lat", "lon", "w", "h", "params", "eid", "tr", "time", "vid", "did", "sid",
			"app", "rel", "cflags", "created", "uid", "last", "url", "ip", "latlon", "ptyp",
			"bhash", "auth", "duration", "xid", "split", "etyp", "ver", "sink", "score",
			"gaid", "idfa", "msid", "fbid", "country", "region", "city", "zip", "culture",
			"ref", "aff", "browser", "device", "os", "tz", "vp", "targets", "rid", "relation",
			"rcode", "ename", "source", "content", "medium", "campaign", "term", "v1",
		}
		for _, field := range excludeFields {
			delete(*params, field)
		}
		if len(*params) == 0 {
			params = nil
		}
	}

	// Convert param values to proper types in place
	if params != nil {
		convertParamsToTypes(params)
	}

	//[culture]
	var culture *string
	c := strings.Split(w.Language, ",")
	if len(c) > 0 {
		culture = &c[0]
		cleanString(culture)
	}

	//WARNING: w.URI has destructive changes here
	//[last],[url]
	if i.AppConfig.IsUrlFiltered {
		if last, ok := v["last"].(string); ok {
			filterUrl(i.AppConfig, &last, &i.AppConfig.UrlFilterMatchGroup)
			filterUrlPrefix(&last)
			v["last"] = last
		}
		if url, ok := v["url"].(string); ok {
			filterUrl(i.AppConfig, &url, &i.AppConfig.UrlFilterMatchGroup)
			filterUrlPrefix(&url)
			v["url"] = url
		} else {
			//check for /tr/ /pub/ /img/ (ignore)
			if !regexInternalURI.MatchString(w.URI) {
				filterUrl(i.AppConfig, &w.URI, &i.AppConfig.UrlFilterMatchGroup)
				filterUrlPrefix(&w.URI)
				v["url"] = w.URI
			} else {
				delete(v, "url")
			}
		}
	} else {
		if last, ok := v["last"].(string); ok {
			filterUrlPrefix(&last)
			filterUrlAppendix(&last)
			v["last"] = last
		}
		if url, ok := v["url"].(string); ok {
			filterUrlPrefix(&url)
			filterUrlAppendix(&url)
			v["url"] = url
		} else {
			//check for /tr/ /pub/ /img/ (ignore)
			if !regexInternalURI.MatchString(w.URI) {
				filterUrlPrefix(&w.URI)
				filterUrlAppendix(&w.URI)
				v["url"] = w.URI
			} else {
				delete(v, "url")
			}
		}
	}

	//[Cell Phone]
	var chash *string
	if temp, ok := v["chash"].(string); ok {
		chash = &temp
	} else if temp, ok := v["cell"].(string); ok {
		temp = strings.ToLower(strings.TrimSpace(temp))
		temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		chash = &temp
	}
	delete(v, "cell")

	//[Email]
	var ehash *string
	if temp, ok := v["ehash"].(string); ok {
		ehash = &temp
	} else if temp, ok := v["email"].(string); ok {
		temp = strings.ToLower(strings.TrimSpace(temp))
		temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		ehash = &temp
	}
	delete(v, "email")

	//[uname]
	var uhash *string
	if temp, ok := v["uhash"].(string); ok {
		uhash = &temp
	} else if temp, ok := v["uname"].(string); ok {
		temp = strings.ToLower(strings.TrimSpace(temp))
		temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		uhash = &temp
	}
	delete(v, "uname")

	//EventID
	if temp, ok := v["eid"].(string); ok {
		if evt, err := uuid.Parse(temp); err == nil {
			w.EventID = evt
		}
	}
	//Double check
	if w.EventID == uuid.Nil {
		w.EventID = uuid.Must(uuid.NewUUID())
	}

	//[vid] - default
	isNew := false
	if vidstring, ok := v["vid"].(string); !ok {
		v["vid"] = uuid.Must(uuid.NewUUID()).String()
		isNew = true
	} else {
		//Let's override the event id too
		if _, err := uuid.Parse(vidstring); err != nil {
			v["vid"] = uuid.Must(uuid.NewUUID()).String()
			isNew = true
		}
	}
	// //[uid] - let's overwrite the vid if we have a uid
	// if uidstring, ok := v["uid"].(string); ok {
	// 	if _, err := uuid.Parse(uidstring); err == nil {
	// 		v["vid"] = v["uid"]
	// 		isNew = false
	// 	}
	// }
	//[sid]
	if sidstring, ok := v["sid"].(string); !ok {
		if isNew {
			v["sid"] = v["vid"]
		} else {
			v["sid"] = uuid.Must(uuid.NewUUID()).String()
		}
	} else {
		if _, err := uuid.Parse(sidstring); err != nil {
			v["sid"] = uuid.Must(uuid.NewUUID()).String()
		}
	}

	//////////////////////////////////////////////
	//CAMPAIGN TELEMETRY INTEGRATION
	//////////////////////////////////////////////

	// Check if this is a campaign-related event and handle campaign telemetry
	var tid *uuid.UUID
	if tidStr, ok := v["tid"].(string); ok {
		if parsedTid, err := uuid.Parse(tidStr); err == nil {
			tid = &parsedTid

			// Integrate campaign tracking functionality from telemetry_campaign.go
			if err := i.handleCampaignEvent(ctx, &CampaignEventData{
				TID:         *tid,
				EventID:     w.EventID,
				VisitorID:   *vid,
				UserID:      uid,
				OrgID:       oid,
				EventType:   getStringValue(v["ename"]),
				VariantID:   getStringValue(v["xid"]),
				Channel:     getStringValue(v["medium"]),
				ContentType: getStringValue(v["etyp"]),
				UTMSource:   getStringValue(v["source"]),
				UTMCampaign: getStringValue(v["campaign"]),
				UTMMedium:   getStringValue(v["medium"]),
				UTMContent:  getStringValue(v["content"]),
				UTMTerm:     getStringValue(v["term"]),
				Properties:  *params,
				Revenue:     score, // Using score as revenue proxy
				IPAddress:   &w.IP,
				UserAgent:   &w.Browser,
				Referrer:    getStringPtr(v["last"]),
				PageURL:     getStringPtr(v["url"]),
				Timestamp:   updated,
			}); err != nil {
				// Log but don't fail the event tracking
				fmt.Printf("[WARNING] Failed to handle campaign event: %v\n", err)
			}
		}
	}

	//////////////////////////////////////////////
	//Persist
	//////////////////////////////////////////////

	//ips
	if xerr := (*i.Session).Exec(ctx, `INSERT INTO ips (hhash, ip, total, date) VALUES (?, ?, 1, today())`,
		hhash, w.IP); xerr != nil && i.AppConfig.Debug {
		fmt.Println("CH[ips]:", xerr)
	}

	//routed
	if xerr := (*i.Session).Exec(ctx, `INSERT INTO routed (hhash, ip, url, updated_at) VALUES (?, ?, ?, ?)`,
		hhash, w.IP, v["url"], updated); xerr != nil && i.AppConfig.Debug {
		fmt.Println("CH[routed]:", xerr)
	}

	//events_recent (batched)
	if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_EVENTS_RECENT)) {
		if xerr := i.batchInsert("events_recent", `INSERT INTO events_recent (
			eid, vid, sid, oid, hhash, app, rel, cflags, 
			created_at, uid, tid, last, url, ip, iphash, lat, lon, ptyp, 
			bhash, auth, duration, xid, split, ename, source, medium, campaign, 
			country, region, city, zip, term, etyp, ver, sink, score, params, 
			payment_id, targets, relation, rid, ja4h
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]interface{}{
				w.EventID, parseUUID(vid), parseUUID(sid), parseUUID(v["oid"]), hhash, v["app"], v["rel"], cflags,
				updated, parseUUID(uid), tid, v["last"], v["url"], w.IP, iphash, lat, lon, v["ptyp"],
				bhash, parseUUID(auth), duration, v["xid"], v["split"], v["ename"], v["source"], v["medium"], v["campaign"],
				country, region, city, zip, v["term"], v["etyp"], version, v["sink"], score, jsonOrNull(params),
				parseUUID(paymentID), jsonOrNull(v["targets"]), v["relation"], parseUUID(rid), w.JA4H,
			}, v); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[events_recent]:", xerr)
		}
	}

	if !i.AppConfig.UseRemoveIP {
		v["cleanIP"] = w.IP
	}

	//events (batched)
	if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_EVENTS)) {
		if xerr := i.batchInsert("events", `INSERT INTO events (
			eid, vid, sid, oid, hhash, app, rel, cflags, 
			created_at, uid, tid, last, url, ip, iphash, lat, lon, ptyp, 
			bhash, auth, duration, xid, split, ename, source, medium, campaign, 
			country, region, city, zip, term, etyp, ver, sink, score, params, 
			payment_id, targets, relation, rid, ja4h
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]interface{}{
				w.EventID, parseUUID(vid), parseUUID(sid), parseUUID(v["oid"]), hhash, v["app"], v["rel"], cflags,
				updated, parseUUID(uid), tid, v["last"], v["url"], v["cleanIP"], iphash, lat, lon, v["ptyp"],
				bhash, parseUUID(auth), duration, v["xid"], v["split"], v["ename"], v["source"], v["medium"], v["campaign"],
				country, region, city, zip, v["term"], v["etyp"], version, v["sink"], score, jsonOrNull(params),
				parseUUID(paymentID), jsonOrNull(v["targets"]), v["relation"], parseUUID(rid), w.JA4H,
			}, v); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[events]:", xerr)
		}
	}

	//Exclude from params in sessions and visitors. Note: more above.
	if params != nil {
		delete(*params, "campaign")
		delete(*params, "source")
		delete(*params, "medium")
		if len(*params) == 0 {
			params = nil
		}
	}

	// Handle message thread updates for mthreads, mstore, and mtriage
	if tid != nil {
		if err := i.updateMThreadsTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[WARNING] Failed to update mthreads: %v\n", err)
		}

		if err := i.updateMStoreTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[WARNING] Failed to update mstore: %v\n", err)
		}

		if err := i.updateMTriageTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[WARNING] Failed to update mtriage: %v\n", err)
		}
	}

	if !w.IsServer {
		w.SaveCookie = true

		//[first]
		isFirst := isNew || (v["first"] != "false")

		//hits
		if _, ok := v["url"].(string); ok {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO hits (hhash, url, total, date) VALUES (?, ?, 1, today())`,
				hhash, v["url"]); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[hits]:", xerr)
			}
		}

		//daily
		if xerr := (*i.Session).Exec(ctx, `INSERT INTO dailies (ip, day, total) VALUES (?, today(), 1)`, w.IP); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[dailies]:", xerr)
		}

		//unknown vid
		if isNew {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO counters (id, total, date) VALUES ('vids_created', 1, today())`); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[counters]vids_created:", xerr)
			}
		}

		//outcome
		if outcome, ok := v["outcome"].(string); ok {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO outcomes (hhash, outcome, sink, created, url, total) VALUES (?, ?, ?, ?, ?, 1)`,
				hhash, outcome, v["sink"], updated.Format("2006-01-02"), v["url"]); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[outcomes]:", xerr)
			}
		}

		//referrers
		if _, ok := v["last"].(string); ok {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO referrers (hhash, url, total, date) VALUES (?, ?, 1, today())`,
				hhash, v["last"]); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[referrers]:", xerr)
			}
		}

		//referrals
		if v["ref"] != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO referrals (hhash, ref, vid, gen) VALUES (?, ?, ?, 0)`,
				hhash, v["ref"], vid); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[referrals]:", xerr)
			}
		}

		//referred
		if v["rcode"] != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO referred (hhash, rcode, vid, gen) VALUES (?, ?, ?, 0)`,
				hhash, v["rcode"], vid); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[referred]:", xerr)
			}
		}

		//hosts
		if w.Host != "" {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO hosts (hhash, hostname) VALUES (?, ?)`,
				hhash, w.Host); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[hosts]:", xerr)
			}
		}

		//browsers
		if xerr := (*i.Session).Exec(ctx, `INSERT INTO browsers (hhash, bhash, browser, total, date) VALUES (?, ?, ?, 1, today())`,
			hhash, bhash, w.Browser); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[browsers]:", xerr)
		}

		//nodes
		if xerr := (*i.Session).Exec(ctx, `INSERT INTO nodes (hhash, vid, uid, iphash, ip, sid) VALUES (?, ?, ?, ?, ?, ?)`,
			hhash, parseUUID(vid), parseUUID(uid), iphash, w.IP, parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[nodes]:", xerr)
		}

		//locations
		if lat != nil && lon != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO locations (hhash, vid, lat, lon, uid, sid) VALUES (?, ?, ?, ?, ?, ?)`,
				hhash, parseUUID(vid), lat, lon, parseUUID(uid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[locations]:", xerr)
			}
		}

		//alias
		if uid != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO aliases (hhash, vid, uid, sid) VALUES (?, ?, ?, ?)`,
				hhash, parseUUID(vid), parseUUID(uid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[aliases]:", xerr)
			}
		}

		//userhosts
		if uid != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO userhosts (hhash, uid, vid, sid) VALUES (?, ?, ?, ?)`,
				hhash, parseUUID(uid), parseUUID(vid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[userhosts]:", xerr)
			}
		}

		//uhash
		if uhash != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO usernames (hhash, uhash, vid, sid) VALUES (?, ?, ?, ?)`,
				hhash, uhash, parseUUID(vid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[usernames]:", xerr)
			}
		}

		//ehash
		if ehash != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO emails (hhash, ehash, vid, sid) VALUES (?, ?, ?, ?)`,
				hhash, ehash, parseUUID(vid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[emails]:", xerr)
			}
		}

		//chash
		if chash != nil {
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO cells (hhash, chash, vid, sid) VALUES (?, ?, ?, ?)`,
				hhash, chash, parseUUID(vid), parseUUID(sid)); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[cells]:", xerr)
			}
		}

		//reqs
		if xerr := (*i.Session).Exec(ctx, `INSERT INTO reqs (hhash, vid, total, date) VALUES (?, ?, 1, today())`,
			hhash, parseUUID(vid)); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[reqs]:", xerr)
		}

		if isNew || isFirst {
			//visitors
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO visitors (
				vid, did, sid, hhash, app, rel, cflags, 
				created_at, uid, tid, last, url, ip, iphash, lat, lon, 
				ptyp, bhash, auth, xid, split, ename, etyp, ver, sink, score, 
				params, gaid, idfa, msid, fbid, country, region, city, zip, culture, 
				source, medium, campaign, term, ref, rcode, aff, browser, device, os, tz, vp_w, vp_h, ja4h, oid, version_ts
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, //51
				parseUUID(vid), v["did"], parseUUID(sid), hhash, v["app"], v["rel"], cflags,
				updated, parseUUID(uid), tid, v["last"], v["url"], v["cleanIP"], iphash, lat, lon,
				v["ptyp"], bhash, parseUUID(auth), v["xid"], v["split"], v["ename"], v["etyp"], version, v["sink"], score,
				jsonOrNull(params), v["gaid"], v["idfa"], v["msid"], v["fbid"], country, region, city, zip, culture,
				v["source"], v["medium"], v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"], w.Browser, v["device"], v["os"], v["tz"], v["w"], v["h"], w.JA4H, parseUUID(v["oid"]), -updated.Unix()); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[visitors]:", xerr)
			}

			//sessions
			if xerr := (*i.Session).Exec(ctx, `INSERT INTO sessions (
				vid, did, sid, hhash, app, rel, cflags, 
				created_at, uid, tid, last, url, ip, iphash, lat, lon, 
				ptyp, bhash, auth, duration, xid, split, ename, etyp, ver, sink, score, 
				params, gaid, idfa, msid, fbid, country, region, city, zip, culture, 
				source, medium, campaign, term, ref, rcode, aff, browser, device, os, tz, vp_w, vp_h, ja4h, oid
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, //51
				parseUUID(vid), v["did"], parseUUID(sid), hhash, v["app"], v["rel"], cflags,
				updated, parseUUID(uid), tid, v["last"], v["url"], v["cleanIP"], iphash, lat, lon,
				v["ptyp"], bhash, parseUUID(auth), duration, v["xid"], v["split"], v["ename"], v["etyp"], version, v["sink"], score,
				jsonOrNull(params), v["gaid"], v["idfa"], v["msid"], v["fbid"], country, region, city, zip, culture,
				v["source"], v["medium"], v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"], w.Browser, v["device"], v["os"], v["tz"], v["w"], v["h"], w.JA4H, parseUUID(v["oid"])); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[sessions]:", xerr)
			}
		}

		// Update visitors_latest
		if xerr := (*i.Session).Exec(ctx, `INSERT INTO visitors_latest (
			vid, did, sid, hhash, app, rel, cflags, 
			created_at, uid, tid, last, url, ip, iphash, lat, lon, 
			ptyp, bhash, auth, xid, split, ename, etyp, ver, sink, score, 
			params, gaid, idfa, msid, fbid, country, region, city, zip, culture, 
			source, medium, campaign, term, ref, rcode, aff, browser, device, os, tz, vp_w, vp_h, ja4h, oid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, //50
			parseUUID(vid), v["did"], parseUUID(sid), hhash, v["app"], v["rel"], cflags,
			updated, parseUUID(uid), tid, v["last"], v["url"], v["cleanIP"], iphash, lat, lon,
			v["ptyp"], bhash, parseUUID(auth), v["xid"], v["split"], v["ename"], v["etyp"], version, v["sink"], score,
			jsonOrNull(params), v["gaid"], v["idfa"], v["msid"], v["fbid"], country, region, city, zip, culture,
			v["source"], v["medium"], v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"], w.Browser, v["device"], v["os"], v["tz"], v["w"], v["h"], w.JA4H, parseUUID(v["oid"])); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[visitors_latest]:", xerr)
		}
	}

	return nil
}

// writeLTV handles WRITE_LTV with integrated payment processing
func (i *ClickhouseService) writeLTV(ctx context.Context, w *WriteArgs, v map[string]interface{}) error {
	cleanString(&(w.Host))
	//////////////////////////////////////////////
	//FIX VARS
	//////////////////////////////////////////////
	//[updated]
	updated := time.Now().UTC()
	created := &updated

	//[hhash]
	var hhash *string
	if w.Host != "" {
		temp := strconv.FormatInt(int64(hash(w.Host)), 36)
		hhash = &temp
	}

	// Parse UUID fields
	var uid, oid, paymentID *uuid.UUID
	if temp, ok := v["uid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			uid = &parsed
		}
	}
	if temp, ok := v["oid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			oid = &parsed
		}
	}
	if temp, ok := v["payment_id"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			paymentID = &parsed
		}
	}

	//[payment] - create payment record
	var payment PaymentData
	payment.ID = paymentID
	payment.CreatedAt = updated
	payment.UpdatedAt = updated

	// Parse payment fields
	if temp, ok := v["product"].(string); ok {
		payment.Product = &temp
	}
	if temp, ok := v["amount"].(string); ok {
		if amount, err := strconv.ParseFloat(temp, 64); err == nil {
			payment.Amount = &amount
		}
	} else if amount, ok := v["amount"].(float64); ok {
		payment.Amount = &amount
	}
	if temp, ok := v["currency"].(string); ok {
		payment.Currency = &temp
	}
	if temp, ok := v["status"].(string); ok {
		payment.Status = &temp
	}

	// Insert into payments table
	if err := (*i.Session).Exec(ctx, `INSERT INTO payments (
		id, uid, oid, amount, currency, status, product, 
		created_at, updated_at, hhash
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		payment.ID, uid, oid, payment.Amount, payment.Currency, payment.Status,
		payment.Product, payment.CreatedAt, payment.UpdatedAt, hhash); err != nil {
		return err
	}

	// Update LTV calculations (simplified version)
	if payment.Amount != nil {
		if err := (*i.Session).Exec(ctx, `INSERT INTO ltv (
			uid, oid, total_revenue, payment_count, 
			last_payment, created_at, updated_at, hhash
		) VALUES (?, ?, ?, 1, ?, ?, ?, ?)`,
			uid, oid, payment.Amount, updated, created, updated, hhash); err != nil {
			return err
		}
	}

	return nil
}

// CampaignEventData represents campaign telemetry event data
type CampaignEventData struct {
	TID         uuid.UUID              `json:"tid"`
	EventID     uuid.UUID              `json:"eid"`
	VisitorID   uuid.UUID              `json:"vid"`
	UserID      *uuid.UUID             `json:"uid"`
	OrgID       *uuid.UUID             `json:"oid"`
	EventType   string                 `json:"etyp"`
	VariantID   string                 `json:"variant_id"`
	Channel     string                 `json:"channel"`
	ContentType string                 `json:"content_type"`
	UTMSource   string                 `json:"utm_source"`
	UTMCampaign string                 `json:"utm_campaign"`
	UTMMedium   string                 `json:"utm_medium"`
	UTMContent  string                 `json:"utm_content"`
	UTMTerm     string                 `json:"utm_term"`
	Properties  map[string]interface{} `json:"properties"`
	Revenue     *float64               `json:"revenue"`
	IPAddress   *string                `json:"ip_address"`
	UserAgent   *string                `json:"user_agent"`
	Referrer    *string                `json:"referrer"`
	PageURL     *string                `json:"page_url"`
	Timestamp   time.Time              `json:"timestamp"`
}

// PaymentData represents payment information
type PaymentData struct {
	ID        *uuid.UUID `json:"id"`
	Product   *string    `json:"product"`
	Amount    *float64   `json:"amount"`
	Currency  *string    `json:"currency"`
	Status    *string    `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// handleCampaignEvent processes campaign telemetry events
func (i *ClickhouseService) handleCampaignEvent(ctx context.Context, event *CampaignEventData) error {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		globalMetrics.UpdateLatency(latency)
		atomic.AddInt64(&globalMetrics.CampaignEvents, 1)
	}()

	// Validate required fields
	if event == nil {
		err := NewTrackerError(ErrorTypeValidation, "handleCampaignEvent", "event data is nil", false)
		globalMetrics.UpdateErrorCount()
		return err
	}

	if event.EventType == "" {
		err := NewTrackerError(ErrorTypeValidation, "handleCampaignEvent", "event_type is required", false)
		err.Context["event_id"] = event.EventID.String()
		globalMetrics.UpdateErrorCount()
		return err
	}
	// Update mthreads table
	if err := i.updateMThreadsTable(ctx, &event.TID, event.OrgID, map[string]interface{}{
		"event_type": event.EventType,
		"variant_id": event.VariantID,
		"channel":    event.Channel,
		"revenue":    event.Revenue,
	}, event.Timestamp); err != nil {
		return fmt.Errorf("failed to update mthreads: %v", err)
	}

	// Update mstore table with event data
	if err := i.updateMStoreTable(ctx, &event.TID, event.OrgID, map[string]interface{}{
		"event_id":   event.EventID.String(),
		"visitor_id": event.VisitorID.String(),
		"event_type": event.EventType,
		"properties": event.Properties,
	}, event.Timestamp); err != nil {
		return fmt.Errorf("failed to update mstore: %v", err)
	}

	// Update campaign metrics if this is a conversion event
	if event.EventType == "conversion" || event.Revenue != nil {
		if err := i.updateCampaignMetrics(ctx, event); err != nil {
			fmt.Printf("[WARNING] Failed to update campaign metrics: %v\n", err)
		}
	}

	return nil
}

// updateMThreadsTable updates the mthreads table with campaign performance data
func (i *ClickhouseService) updateMThreadsTable(ctx context.Context, tid *uuid.UUID, oid *uuid.UUID, v map[string]interface{}, updated time.Time) error {
	atomic.AddInt64(&globalMetrics.MThreadsOps, 1)

	// Validate inputs
	if tid == nil {
		err := NewTrackerError(ErrorTypeValidation, "updateMThreadsTable", "tid cannot be nil", false)
		globalMetrics.UpdateErrorCount()
		return err
	}

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Insert or update mthreads record (batched)
	return i.batchInsert("mthreads", `INSERT INTO mthreads (
		tid, oid, thread_type, status, metadata, 
		provider_metrics, performance_metrics, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			tid, oid, "campaign", "active",
			jsonOrNull(v), jsonOrNull(v), jsonOrNull(v), updated, updated,
		}, v)
}

// updateMStoreTable updates the mstore table with individual event data
func (i *ClickhouseService) updateMStoreTable(ctx context.Context, tid *uuid.UUID, oid *uuid.UUID, v map[string]interface{}, updated time.Time) error {
	atomic.AddInt64(&globalMetrics.MStoreOps, 1)

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	eventID := uuid.Must(uuid.NewUUID())
	return i.batchInsert("mstore", `INSERT INTO mstore (
		id, tid, oid, event_type, 
		content, metadata, parent_id, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			eventID, tid, oid,
			getStringValue(v["event_type"]), jsonOrNull(v), jsonOrNull(v), nil, updated, updated,
		}, v)
}

// updateMTriageTable updates the mtriage table for outbound message processing
func (i *ClickhouseService) updateMTriageTable(ctx context.Context, tid *uuid.UUID, oid *uuid.UUID, v map[string]interface{}, updated time.Time) error {
	atomic.AddInt64(&globalMetrics.MTriageOps, 1)

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Only create triage entries for specific event types that require follow-up
	eventType := getStringValue(v["event_type"])
	if eventType != "conversion" && eventType != "high_value_action" {
		return nil // Skip non-actionable events
	}

	triageID := uuid.Must(uuid.NewUUID())
	return i.batchInsert("mtriage", `INSERT INTO mtriage (
		id, tid, oid, priority, 
		message_type, content, metadata, status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			triageID, tid, oid,
			"high", "follow_up", jsonOrNull(v), jsonOrNull(v), "pending", updated, updated,
		}, v)
}

// updateCampaignMetrics updates campaign performance metrics
func (i *ClickhouseService) updateCampaignMetrics(ctx context.Context, event *CampaignEventData) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)

	// Update impression metrics
	conversions := 0
	if event.EventType == "conversion" {
		conversions = 1
	}

	revenue := 0.0
	if event.Revenue != nil {
		revenue = *event.Revenue
	}

	return i.batchInsert("impression_daily", `INSERT INTO impression_daily (
		date, tid, variant_id, total_impressions, conversions, revenue, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			today, event.TID, event.VariantID, 1, conversions, revenue, event.Timestamp,
		}, map[string]interface{}{
			"event_type": event.EventType,
			"variant_id": event.VariantID,
			"revenue":    revenue,
		})
}

// Helper functions for campaign telemetry integration
