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
// Helper Functions for Data Conversion
////////////////////////////////////////

// getStringOrDefault returns a string from interface{} or default value
func getStringOrDefault(val interface{}, defaultVal string) string {
	if val == nil {
		return defaultVal
	}
	if str, ok := val.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", val)
}

// getJSONOrDefault returns JSON string from interface{} or default value
func getJSONOrDefault(val interface{}, defaultVal string) string {
	if val == nil {
		return defaultVal
	}
	switch v := val.(type) {
	case string:
		if v == "" {
			return defaultVal
		}
		return v
	case map[string]interface{}:
		if len(v) == 0 {
			return defaultVal
		}
		if data, err := json.Marshal(v); err == nil {
			return string(data)
		}
	}
	return defaultVal
}

// getBoolOrDefault returns bool from interface{} or default value
func getBoolOrDefault(val interface{}, defaultVal bool) bool {
	if val == nil {
		return defaultVal
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if str, ok := val.(string); ok {
		return str == "true" || str == "1"
	}
	return defaultVal
}

// getInt64OrDefault returns int64 from interface{} or default value
func getInt64OrDefault(val interface{}, defaultVal int64) int64 {
	if val == nil {
		return defaultVal
	}
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

// getUUIDArrayOrDefault returns UUID array from interface{} or default value
func getUUIDArrayOrDefault(val interface{}, defaultVal []uuid.UUID) []uuid.UUID {
	if val == nil {
		return defaultVal
	}
	switch v := val.(type) {
	case []uuid.UUID:
		return v
	case []interface{}:
		result := make([]uuid.UUID, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				if uid, err := uuid.Parse(str); err == nil {
					result = append(result, uid)
				}
			}
		}
		return result
	case []string:
		result := make([]uuid.UUID, 0, len(v))
		for _, str := range v {
			if uid, err := uuid.Parse(str); err == nil {
				result = append(result, uid)
			}
		}
		return result
	}
	return defaultVal
}


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
			"wait_for_async_insert":        1, // Wait for async inserts to be flushed
			"async_insert_max_data_size":   10000000,
			"async_insert_busy_timeout_ms": 200,
			"max_threads":                  0, // Auto-detect
			"send_logs_level":              "error",
			"enable_http_compression":      1,
			"http_zlib_compression_level":  1,
			"insert_deduplication_token":   "",
			"insert_deduplicate":           0,
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
			if err := (*i.Session).QueryRow(context.Background(), `SELECT total FROM sfpla.dailies FINAL WHERE ip=? AND day=?`, ip, time.Now().UTC().Format("2006-01-02")).Scan(&total); err != nil {
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
	if err := (*i.Session).QueryRow(context.Background(), `SELECT pwd FROM sfpla.accounts FINAL WHERE uid=?`, uid).Scan(&pwd); err == nil {
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
				agreementsData := map[string]interface{}{
					"vid": parseUUID(vid), "created_at": created, "updated_at": created, "cflags": cflags, "sid": parseUUID(sid), "uid": parseUUID(uid), "avid": parseUUID(avid), "hhash": hhash,
					"app": b["app"], "rel": b["rel"], "url": b["url"], "ip": ip, "iphash": iphash, "gaid": b["gaid"], "idfa": b["idfa"], "msid": b["msid"], "fbid": b["fbid"],
					"country": country, "region": region, "culture": b["culture"], "source": b["source"], "medium": b["medium"], "campaign": b["campaign"], "term": b["term"], "ref": b["ref"], "rcode": b["rcode"], "aff": b["aff"],
					"browser": browser, "bhash": bhash, "device": b["device"], "os": b["os"], "tz": b["tz"], "vp_w": b["w"], "vp_h": b["h"], "lat": lat, "lon": lon, "zip": b["zip"], "owner": parseUUID(owner), "oid": parseUUID(oid), "org": getStringValue(b["org"]),
				}
				if err := i.batchInsert("agreements", `INSERT INTO sfpla.agreements (
					vid, created_at, updated_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid,
					country, region, culture, source, medium, campaign, term, ref, rcode, aff,
					browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid, org
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[]interface{}{parseUUID(vid), created, created, cflags, parseUUID(sid), parseUUID(uid), parseUUID(avid), hhash, b["app"], b["rel"], b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"],
						country, region, b["culture"], b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"],
						browser, bhash, b["device"], b["os"], b["tz"], b["w"], b["h"], lat, lon, b["zip"], parseUUID(owner), parseUUID(oid), getStringValue(b["org"])}, agreementsData); err != nil {
					return err
				}

				// Insert into agreed table (history)
				agreedData := map[string]interface{}{
					"vid": parseUUID(vid), "created_at": created, "updated_at": created, "cflags": cflags, "sid": parseUUID(sid), "uid": parseUUID(uid), "avid": parseUUID(avid), "hhash": hhash,
					"app": b["app"], "rel": b["rel"], "url": b["url"], "ip": ip, "iphash": iphash, "gaid": b["gaid"], "idfa": b["idfa"], "msid": b["msid"], "fbid": b["fbid"],
					"country": country, "region": region, "culture": b["culture"], "source": b["source"], "medium": b["medium"], "campaign": b["campaign"], "term": b["term"], "ref": b["ref"], "rcode": b["rcode"], "aff": b["aff"],
					"browser": browser, "bhash": bhash, "device": b["device"], "os": b["os"], "tz": b["tz"], "vp_w": b["w"], "vp_h": b["h"], "lat": lat, "lon": lon, "zip": b["zip"], "owner": parseUUID(owner), "oid": parseUUID(oid), "org": getStringValue(b["org"]),
				}
				if err := i.batchInsert("agreed", `INSERT INTO sfpla.agreed (
					vid, created_at, updated_at, cflags, sid, uid, avid, hhash, app, rel, url, ip, iphash, gaid, idfa, msid, fbid,
					country, region, culture, source, medium, campaign, term, ref, rcode, aff,
					browser, bhash, device, os, tz, vp_w, vp_h, lat, lon, zip, owner, oid, org
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[]interface{}{parseUUID(vid), created, created, cflags, parseUUID(sid), parseUUID(uid), parseUUID(avid), hhash, b["app"], b["rel"], b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"],
						country, region, b["culture"], b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"],
						browser, bhash, b["device"], b["os"], b["tz"], b["w"], b["h"], lat, lon, b["zip"], parseUUID(owner), parseUUID(oid), getStringValue(b["org"])}, agreedData); err != nil {
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
			rows, err := (*i.Session).Query(ctx, `SELECT * FROM sfpla.agreements FINAL WHERE vid=?`, vid)
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
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM sfpla.jurisdictions FINAL`)
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
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM sfpla.redirect_history FINAL`)
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
		if err := (*i.Session).QueryRow(ctx, `SELECT urlto FROM sfpla.redirects FINAL WHERE urlfrom=?`, getFullURL(r)).Scan(&redirect); err == nil {
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
				redirectsData := map[string]interface{}{
					"hhash":      hhash,
					"urlfrom":    strings.ToLower(urlfromURL.Host) + strings.ToLower(urlfromURL.Path),
					"urlto":      urlto,
					"updated_at": updated,
					"updater":    parseUUID(updater),
					"oid":        parseUUID(oid),
					"org":        getStringValue(b["org"]),
					"created_at": updated,
				}
				if err := i.batchInsert("redirects", `INSERT INTO sfpla.redirects (
					hhash, urlfrom, urlto, updated_at, updater, oid, org, created_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					[]interface{}{hhash, strings.ToLower(urlfromURL.Host) + strings.ToLower(urlfromURL.Path), urlto, updated, parseUUID(updater), parseUUID(oid), getStringValue(b["org"]), updated}, redirectsData); err != nil {
					return err
				}

				// Insert into redirect_history table
				redirectHistoryData := map[string]interface{}{
					"urlfrom":    urlfrom,
					"hostfrom":   strings.ToLower(urlfromURL.Host),
					"slugfrom":   strings.ToLower(urlfromURL.Path),
					"urlto":      urlto,
					"hostto":     strings.ToLower(urltoURL.Host),
					"pathto":     strings.ToLower(urlfromURL.Path),
					"searchto":   b["searchto"],
					"updater":    parseUUID(updater),
					"oid":        parseUUID(oid),
					"org":        getStringValue(b["org"]),
					"updated_at": updated,
				}
				if err := i.batchInsert("redirect_history", `INSERT INTO sfpla.redirect_history (
					urlfrom, hostfrom, slugfrom, urlto, hostto, pathto, searchto,
					updater, oid, org, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[]interface{}{urlfrom, strings.ToLower(urlfromURL.Host), strings.ToLower(urlfromURL.Path), urlto, strings.ToLower(urltoURL.Host), strings.ToLower(urlfromURL.Path), b["searchto"], parseUUID(updater), parseUUID(oid), getStringValue(b["org"]), updated}, redirectHistoryData); err != nil {
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
			countersData := map[string]interface{}{
				"id":    v["id"],
				"total": 1,
				"date":  time.Now().UTC().Format("2006-01-02"),
			}
			return i.batchInsert("counters", `INSERT INTO sfpla.counters (id, total, date) VALUES (?, ?, ?)`,
				[]interface{}{v["id"], 1, time.Now().UTC().Format("2006-01-02")}, countersData)
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
			updatesData := map[string]interface{}{
				"id":         v["id"],
				"updated_at": timestamp,
				"msg":        v["msg"],
			}
			return i.batchInsert("updates", `INSERT INTO sfpla.updates (id, updated_at, msg) VALUES (?, ?, ?)`,
				[]interface{}{v["id"], timestamp, v["msg"]}, updatesData)
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

			logId := uuid.Must(uuid.NewUUID())
			currentTime := time.Now().UTC()
			logsData := map[string]interface{}{
				"id":         logId,
				"ldate":      v["ldate"],
				"created_at": currentTime,
				"ltime":      ltime,
				"topic":      topic,
				"name":       v["name"],
				"host":       v["host"],
				"hostname":   v["hostname"],
				"owner":      owner,
				"ip":         v["ip"],
				"iphash":     iphash,
				"level":      level,
				"msg":        v["msg"],
				"params":     jsonOrNull(params),
				"oid":        parseUUID(v["oid"]),
			}
			return i.batchInsert("logs", `INSERT INTO sfpla.logs
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
				[]interface{}{logId, v["ldate"], currentTime, ltime, topic, v["name"], v["host"], v["hostname"], owner, v["ip"], iphash, level, v["msg"], jsonOrNull(params), parseUUID(v["oid"])}, logsData)
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

// mustMarshalJSON marshals a value to JSON string, returns "{}" on error
func mustMarshalJSON(v interface{}) string {
	if v == nil {
		return "{}"
	}
	if jsonBytes, err := json.Marshal(v); err == nil {
		return string(jsonBytes)
	}
	return "{}"
}

// parseTagsFromValue parses tags from various formats:
// - []interface{} (from JSON POST)
// - string with commas (from GET query string)
// Returns []string of tags
func parseTagsFromValue(val interface{}) []string {
	if val == nil {
		return []string{}
	}

	switch v := val.(type) {
	case []interface{}:
		// JSON array format (POST)
		tags := make([]string, 0, len(v))
		for _, tag := range v {
			if tagStr, ok := tag.(string); ok && tagStr != "" {
				tags = append(tags, strings.TrimSpace(tagStr))
			}
		}
		return tags

	case string:
		// Comma-separated format (GET query string)
		if v == "" {
			return []string{}
		}
		parts := strings.Split(v, ",")
		tags := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				tags = append(tags, trimmed)
			}
		}
		return tags

	default:
		return []string{}
	}
}

// batchInsert adds an item to the batch manager or executes directly if batching is disabled
func (i *ClickhouseService) batchInsert(tableName, sql string, args []interface{}, data map[string]interface{}) error {
	return i.batchInsertWithOptions(tableName, sql, args, data, false)
}

// batchInsertWithOptions adds an item to the batch manager or executes directly with instant processing option
func (i *ClickhouseService) batchInsertWithOptions(tableName, sql string, args []interface{}, data map[string]interface{}, instant bool) error {
	// Force instant processing if requested
	if instant {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Using instant insert for table: %s (instant flag set)\n", tableName)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := (*i.Session).Exec(ctx, sql, args...)
		if err != nil && i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Instant insert failed for table %s: %v\n", tableName, err)
		}
		return err
	}

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
		if err != nil && i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Batch add failed for table %s: %v\n", tableName, err)
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
	if err != nil && i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Direct insert failed for table %s: %v\n", tableName, err)
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
	cleanInterfaceString(v["content"])
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
	var vid, sid, uid, authID, rid, oid, invoiceID *uuid.UUID
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
	if temp, ok := v["auth_id"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			authID = &parsed
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
	if temp, ok := v["invoice_id"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			invoiceID = &parsed
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
			"bhash", "auth_id", "duration", "xid", "split", "etyp", "ver", "sink", "score",
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
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Found tid string in event: %s\n", tidStr)
		}
		if parsedTid, err := uuid.Parse(tidStr); err == nil {
			tid = &parsedTid
			if i.AppConfig.Debug {
				fmt.Printf("[DEBUG] Successfully parsed tid: %v\n", *tid)
			}

			// Integrate campaign tracking functionality from telemetry_campaign.go
			// Use parseUUID to handle nil safely - it returns a zero UUID when nil
			parsedVid := parseUUID(vid)
			if parsedVid == nil {
				parsedVid = &uuid.UUID{}
			}

			// Handle nil params by providing empty map
			campaignProperties := make(map[string]interface{})
			if params != nil {
				campaignProperties = *params
			}

			if err := i.handleCampaignEvent(ctx, &CampaignEventData{
				TID:         *tid,
				EventID:     w.EventID,
				VisitorID:   *parsedVid,
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
				Properties:  campaignProperties,
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
	ipsData := map[string]interface{}{
		"hhash": *hhash,
		"ip":    w.IP,
		"total": 1,
		"date":  time.Now().UTC().Format("2006-01-02"),
	}
	if xerr := i.batchInsert("ips", `INSERT INTO sfpla.ips (hhash, ip, total, date) VALUES (?, ?, ?, ?)`,
		[]interface{}{*hhash, w.IP, 1, time.Now().UTC().Format("2006-01-02")}, ipsData); xerr != nil && i.AppConfig.Debug {
		fmt.Println("CH[ips]:", xerr)
	}

	// //Used to confirm ReplicatedSummingMergeTree is working
	// var count *uint64
	// if err := (*i.Session).QueryRow(ctx, `SELECT SUM(total) FROM sfpla.ips WHERE hhash = ? AND ip = ? AND date = today()`, *hhash, w.IP).Scan(&count); err == nil {
	// 	fmt.Println("CH[ips] CURRENT TOTAL:", *count)
	// } else {
	// 	fmt.Println("CH[ips] ERROR:", err)
	// }

	// NOTE: events_recent is a VIEW, not a table - batch writes to VIEWs are not supported by ClickHouse
	// All events are written to the events table instead, and events_recent query reads from it

	if !i.AppConfig.UseRemoveIP {
		v["cleanIP"] = w.IP
	}

	//events (batched)
	if w.CallingService == nil || (w.CallingService != nil && w.CallingService.ProxyRealtimeStorageServiceTables.Has(TABLE_EVENTS)) {
		if xerr := i.batchInsert("events", `INSERT INTO sfpla.events (
			eid, vid, sid, oid, org, hhash, app, rel, cflags,
			created_at, uid, tid, last, url, ip, iphash, lat, lon, ptyp,
			bhash, auth_id, duration, xid, split, ename, source, medium, campaign, content,
			country, region, city, zip, term, etyp, ver, sink, score, params,
			invoice_id, targets, relation, rid, ja4h
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 SETTINGS insert_deduplicate = 1`,
			[]interface{}{
				w.EventID, parseUUID(vid), parseUUID(sid), parseUUID(v["oid"]), getStringValue(v["org"]), hhash, v["app"], v["rel"], cflags,
				updated, parseUUID(uid), parseUUID(tid), v["last"], v["url"], v["cleanIP"], iphash, lat, lon, v["ptyp"],
				bhash, parseUUID(authID), duration, v["xid"], v["split"], v["ename"], v["source"], v["medium"], v["campaign"], v["content"],
				country, region, city, zip, v["term"], v["etyp"], version, v["sink"], score, jsonOrNull(params),
				parseUUID(invoiceID), jsonOrNull(v["targets"]), v["relation"], parseUUID(rid), w.JA4H,
			}, v); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[events]:", xerr)
		}
	}

	// Record visitor interests (TWO-DIMENSIONAL: audience + content tags)
	// Python service will aggregate and generate embeddings later
	if vid != nil && oid != nil {
		// Parse AUDIENCE tags (WHO they are: ceo, executive, developer, etc.)
		audienceTags := parseTagsFromValue(v["audience"])
		if len(audienceTags) == 0 {
			audienceTags = parseTagsFromValue(v["audience_tags"])
		}

		// Parse CONTENT tags (WHAT they're interested in: sports, tech, etc.)
		contentTags := parseTagsFromValue(v["content"])
		if len(contentTags) == 0 {
			contentTags = parseTagsFromValue(v["content_tags"])
		}

		// Backward compatibility: if only "tags" provided, treat as content tags
		if len(contentTags) == 0 && len(audienceTags) == 0 {
			contentTags = parseTagsFromValue(v["tags"])
		}

		// Combine for interests array (backward compat)
		allTags := append(audienceTags, contentTags...)

		if len(allTags) > 0 {
			// Build campaign sources JSON (two-dimensional structure)
			campaignID := ""
			if cid, ok := v["campaign"].(string); ok && cid != "" {
				campaignID = cid
			}

			// Enhanced campaign sources with audience + content separation
			campaignSourcesMap := map[string]interface{}{
				campaignID: map[string]interface{}{
					"audience": audienceTags,
					"content":  contentTags,
				},
			}

			// Build counts for all three dimensions
			audienceCounts := make(map[string]int)
			for _, tag := range audienceTags {
				audienceCounts[tag] = 1
			}

			contentCounts := make(map[string]int)
			for _, tag := range contentTags {
				contentCounts[tag] = 1
			}

			interestCounts := make(map[string]int)
			for _, tag := range allTags {
				interestCounts[tag] = 1
			}

			interestData := map[string]interface{}{
				"vid":                  parseUUID(vid),
				"uid":                  parseUUID(uid),
				"oid":                  parseUUID(oid),
				"audience_tags":        audienceTags,
				"audience_counts":      mustMarshalJSON(audienceCounts),
				"content_tags":         contentTags,
				"content_counts":       mustMarshalJSON(contentCounts),
				"interests":            allTags,
				"interest_counts":      mustMarshalJSON(interestCounts),
				"campaign_sources":     mustMarshalJSON(campaignSourcesMap),
				"total_interactions":   1,
				"unique_campaigns":     1,
				"first_seen":           updated,
				"last_updated":         updated,
				"updated_at":           updated,
				"last_iphash":          iphash,
				"last_lat":             lat,
				"last_lon":             lon,
				"last_country":         country,
				"last_region":          region,
				"last_city":            city,
				"last_zip":             zip,
				"location_updated_at":  updated,
			}

			if xerr := i.batchInsert("visitor_interests", `INSERT INTO sfpla.visitor_interests (
				vid, uid, oid, org,
				audience_tags, audience_counts,
				content_tags, content_counts,
				interests, interest_counts, campaign_sources,
				total_interactions, unique_campaigns, first_seen, last_updated, updated_at,
				last_iphash, last_lat, last_lon, last_country, last_region, last_city, last_zip, location_updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				[]interface{}{
					parseUUID(vid), parseUUID(uid), parseUUID(oid), getStringValue(v["org"]),
					audienceTags, mustMarshalJSON(audienceCounts),
					contentTags, mustMarshalJSON(contentCounts),
					allTags, mustMarshalJSON(interestCounts), mustMarshalJSON(campaignSourcesMap),
					1, 1, updated, updated, updated,
					iphash, lat, lon, country, region, city, zip, updated,
				}, interestData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[visitor_interests]:", xerr)
			}
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
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Found tid: %v, updating messaging tables\n", *tid)
		}
		if err := i.updateMThreadsTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[WARNING] Failed to update mthreads: %v\n", err)
		}

		if err := i.updateMStoreTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[ERROR] Failed to update mstore: %v\n", err)
		}

		if err := i.updateMTriageTable(ctx, tid, oid, v, updated); err != nil {
			fmt.Printf("[WARNING] Failed to update mtriage: %v\n", err)
		}
	}

	if !w.IsServer {
		w.SaveCookie = true

		//hits
		if _, ok := v["url"].(string); ok {
			hitsData := map[string]interface{}{
				"hhash": hhash,
				"url":   v["url"],
				"total": 1,
				"date":  time.Now().UTC().Format("2006-01-02"),
			}
			if xerr := i.batchInsert("hits", `INSERT INTO sfpla.hits (hhash, url, total, date) VALUES (?, ?, ?, ?)`,
				[]interface{}{hhash, v["url"], 1, time.Now().UTC().Format("2006-01-02")}, hitsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[hits]:", xerr)
			}
		}

		//daily
		dailiesData := map[string]interface{}{
			"ip":    w.IP,
			"day":   time.Now().UTC().Format("2006-01-02"),
			"total": 1,
		}
		if xerr := i.batchInsert("dailies", `INSERT INTO sfpla.dailies (ip, day, total) VALUES (?, ?, ?)`,
			[]interface{}{w.IP, time.Now().UTC().Format("2006-01-02"), 1}, dailiesData); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[dailies]:", xerr)
		}

		//unknown vid
		if isNew {
			countersVidsData := map[string]interface{}{
				"id":    "vids_created",
				"total": 1,
				"date":  time.Now().UTC().Format("2006-01-02"),
			}
			if xerr := i.batchInsert("counters", `INSERT INTO sfpla.counters (id, total, date) VALUES (?, ?, ?)`,
				[]interface{}{"vids_created", 1, time.Now().UTC().Format("2006-01-02")}, countersVidsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[counters]vids_created:", xerr)
			}
		}

		//outcome
		if outcome, ok := v["outcome"].(string); ok {
			outcomesData := map[string]interface{}{
				"hhash":   hhash,
				"outcome": outcome,
				"sink":    v["sink"],
				"created": updated.Format("2006-01-02"),
				"url":     v["url"],
				"total":   1,
			}
			if xerr := i.batchInsert("outcomes", `INSERT INTO sfpla.outcomes (hhash, outcome, sink, created, url, total) VALUES (?, ?, ?, ?, ?, ?)`,
				[]interface{}{hhash, outcome, v["sink"], updated.Format("2006-01-02"), v["url"], 1}, outcomesData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[outcomes]:", xerr)
			}
		}

		//referrers
		if _, ok := v["last"].(string); ok {
			referrersData := map[string]interface{}{
				"hhash": hhash,
				"url":   v["last"],
				"total": 1,
				"date":  time.Now().UTC().Format("2006-01-02"),
			}
			if xerr := i.batchInsert("referrers", `INSERT INTO sfpla.referrers (hhash, url, total, date) VALUES (?, ?, ?, ?)`,
				[]interface{}{hhash, v["last"], 1, time.Now().UTC().Format("2006-01-02")}, referrersData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[referrers]:", xerr)
			}
		}

		//hosts
		if w.Host != "" {
			hostsData := map[string]interface{}{
				"hhash":      hhash,
				"hostname":   w.Host,
				"created_at": updated,
			}
			if xerr := i.batchInsert("hosts", `INSERT INTO sfpla.hosts (hhash, hostname, created_at) VALUES (?, ?, ?)`,
				[]interface{}{hhash, w.Host, updated}, hostsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[hosts]:", xerr)
			}
		}

		//browsers
		browsersData := map[string]interface{}{
			"hhash":   hhash,
			"bhash":   bhash,
			"browser": w.Browser,
			"total":   1,
			"date":    time.Now().UTC().Format("2006-01-02"),
		}
		if xerr := i.batchInsert("browsers", `INSERT INTO sfpla.browsers (hhash, bhash, browser, total, date) VALUES (?, ?, ?, ?, ?)`,
			[]interface{}{hhash, bhash, w.Browser, 1, time.Now().UTC().Format("2006-01-02")}, browsersData); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[browsers]:", xerr)
		}

		//userhosts
		if uid != nil {
			userhostsData := map[string]interface{}{
				"hhash":      hhash,
				"uid":        parseUUID(uid),
				"vid":        parseUUID(vid),
				"sid":        parseUUID(sid),
				"created_at": updated,
			}
			if xerr := i.batchInsert("userhosts", `INSERT INTO sfpla.userhosts (hhash, uid, vid, sid, created_at) VALUES (?, ?, ?, ?, ?)`,
				[]interface{}{hhash, parseUUID(uid), parseUUID(vid), parseUUID(sid), updated}, userhostsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[userhosts]:", xerr)
			}
		}

		//uhash
		if uhash != nil {
			usernamesData := map[string]interface{}{
				"hhash":      hhash,
				"uhash":      uhash,
				"vid":        parseUUID(vid),
				"sid":        parseUUID(sid),
				"created_at": updated,
			}
			if xerr := i.batchInsert("usernames", `INSERT INTO sfpla.usernames (hhash, uhash, vid, sid, created_at) VALUES (?, ?, ?, ?, ?)`,
				[]interface{}{hhash, uhash, parseUUID(vid), parseUUID(sid), updated}, usernamesData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[usernames]:", xerr)
			}
		}

		//ehash
		if ehash != nil {
			emailsData := map[string]interface{}{
				"hhash":      hhash,
				"ehash":      ehash,
				"vid":        parseUUID(vid),
				"sid":        parseUUID(sid),
				"created_at": updated,
			}
			if xerr := i.batchInsert("emails", `INSERT INTO sfpla.emails (hhash, ehash, vid, sid, created_at) VALUES (?, ?, ?, ?, ?)`,
				[]interface{}{hhash, ehash, parseUUID(vid), parseUUID(sid), updated}, emailsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[emails]:", xerr)
			}
		}

		//chash
		if chash != nil {
			cellsData := map[string]interface{}{
				"hhash": hhash,
				"chash": chash,
				"vid":   parseUUID(vid),
				"sid":   parseUUID(sid),
			}
			if xerr := i.batchInsert("cells", `INSERT INTO sfpla.cells (hhash, chash, vid, sid) VALUES (?, ?, ?, ?)`,
				[]interface{}{hhash, chash, parseUUID(vid), parseUUID(sid)}, cellsData); xerr != nil && i.AppConfig.Debug {
				fmt.Println("CH[cells]:", xerr)
			}
		}

		//reqs
		reqsData := map[string]interface{}{
			"hhash": hhash,
			"vid":   parseUUID(vid),
			"total": 1,
			"date":  time.Now().UTC().Format("2006-01-02"),
		}
		if xerr := i.batchInsert("reqs", `INSERT INTO sfpla.reqs (hhash, vid, total, date) VALUES (?, ?, ?, ?)`,
			[]interface{}{hhash, parseUUID(vid), 1, time.Now().UTC().Format("2006-01-02")}, reqsData); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[reqs]:", xerr)
		}

	}

	return nil
}

// writeLTV handles WRITE_LTV with integrated payment processing
// Accepts either a single payment object or an array of payment objects
// Single: v = {"uid":"...", "product":"Widget", "revenue":99.99, ...}
// Batch: v = {"uid":"...", "invid":"...", "payments":[{"product":"A", "revenue":50}, {"product":"B", "revenue":25}]}
func (i *ClickhouseService) writeLTV(ctx context.Context, w *WriteArgs, v map[string]interface{}) error {
	cleanString(&(w.Host))

	// Check if "payments" field contains an array of line items
	if paymentsArray, ok := v["payments"].([]interface{}); ok && len(paymentsArray) > 0 {
		// Process multiple line items
		return i.writeLTVBatch(ctx, w, v, paymentsArray)
	}

	// Process single line item (backward compatible)
	return i.writeLTVSingle(ctx, w, v)
}

// writeLTVBatch processes multiple payment line items in one call
func (i *ClickhouseService) writeLTVBatch(ctx context.Context, w *WriteArgs, v map[string]interface{}, paymentsArray []interface{}) error {
	// Parse common fields once (shared across all line items)
	updated := time.Now().UTC()
	created := &updated

	var hhash *string
	if w.Host != "" {
		temp := strconv.FormatInt(int64(hash(w.Host)), 36)
		hhash = &temp
	}

	// Parse common UUID fields
	var uid, oid, tid, invid, vid, sid, updater, owner *uuid.UUID
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
	if temp, ok := v["tid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			tid = &parsed
		}
	}
	if temp, ok := v["invid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			invid = &parsed
		}
	}
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
	if temp, ok := v["updater"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			updater = &parsed
		}
	}
	if temp, ok := v["owner"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			owner = &parsed
		}
	}

	orgValue := getStringValue(v["org"])
	var totalRevenue float64 = 0.0

	// Process each line item
	for _, paymentItem := range paymentsArray {
		paymentMap, ok := paymentItem.(map[string]interface{})
		if !ok {
			continue // Skip invalid items
		}

		// Process this line item
		lineItemRevenue, err := i.processLineItem(ctx, paymentMap, hhash, oid, &orgValue, tid, uid, invid, vid, sid, created, &updated)
		if err != nil {
			return fmt.Errorf("failed to process line item: %v", err)
		}

		totalRevenue += lineItemRevenue
	}

	// Update LTV records with total revenue across all line items
	if totalRevenue > 0.0 {
		if err := i.updateLTVRecords(hhash, uid, vid, invid, oid, &orgValue, totalRevenue, &updated, created, updater, owner); err != nil {
			return err
		}
	}

	return nil
}

// writeLTVSingle processes a single payment line item (backward compatible)
func (i *ClickhouseService) writeLTVSingle(ctx context.Context, w *WriteArgs, v map[string]interface{}) error {
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
	var uid, oid, tid, invid, vid, sid, updater, owner *uuid.UUID
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
	if temp, ok := v["tid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			tid = &parsed
		}
	}
	if temp, ok := v["invid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			invid = &parsed
		}
	}
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
	if temp, ok := v["updater"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			updater = &parsed
		}
	}
	if temp, ok := v["owner"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			owner = &parsed
		}
	}

	orgValue := getStringValue(v["org"])

	// Process this single line item
	revenue, err := i.processLineItem(ctx, v, hhash, oid, &orgValue, tid, uid, invid, vid, sid, created, &updated)
	if err != nil {
		return err
	}

	// Update LTV records
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Checking revenue condition: revenue = %v, revenue > 0.0 = %v\n", revenue, revenue > 0.0)
	}
	if revenue > 0.0 {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Calling updateLTVRecords with revenue = %v\n", revenue)
		}
		if err := i.updateLTVRecords(hhash, uid, vid, invid, oid, &orgValue, revenue, &updated, created, updater, owner); err != nil {
			return err
		}
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] updateLTVRecords completed successfully\n")
		}
	} else if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Skipping updateLTVRecords because revenue <= 0.0\n")
	}

	return nil
}

// processLineItem handles a single payment line item insertion
func (i *ClickhouseService) processLineItem(ctx context.Context, lineItem map[string]interface{}, hhash *string, oid *uuid.UUID, org *string, tid, uid, invid, vid, sid *uuid.UUID, created, updated *time.Time) (float64, error) {
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] processLineItem called with lineItem keys: %v\n", func() []string {
			keys := make([]string, 0, len(lineItem))
			for k := range lineItem {
				keys = append(keys, k)
			}
			return keys
		}())
		if amt, ok := lineItem["amt"]; ok {
			fmt.Printf("[DEBUG] lineItem['amt'] = %v (type: %T)\n", amt, amt)
		}
		if rev, ok := lineItem["revenue"]; ok {
			fmt.Printf("[DEBUG] lineItem['revenue'] = %v (type: %T)\n", rev, rev)
		}
	}

	lineItemID := uuid.New()
	var productID, orid *uuid.UUID
	var product, pcat, man, model, duration, currency, country, rcode, region, campaignID *string
	var qty, price, discount, revenue, margin, cost, tax, taxRate, commission, referral, fees, subtotal, total, paymentAmount *float64
	var starts, ends, invoicedAt, paidAt *time.Time

	// Parse product_id UUID
	if temp, ok := lineItem["product_id"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			productID = &parsed
		}
	}

	// Parse orid UUID (order ID)
	if temp, ok := lineItem["orid"].(string); ok {
		if parsed, err := uuid.Parse(temp); err == nil {
			orid = &parsed
		}
	}

	// String fields
	if temp, ok := lineItem["product"].(string); ok { product = &temp }
	if temp, ok := lineItem["pcat"].(string); ok { pcat = &temp }
	if temp, ok := lineItem["man"].(string); ok { man = &temp }
	if temp, ok := lineItem["model"].(string); ok { model = &temp }
	if temp, ok := lineItem["duration"].(string); ok { duration = &temp }
	if temp, ok := lineItem["currency"].(string); ok { currency = &temp } else { defaultCurrency := "USD"; currency = &defaultCurrency }
	if temp, ok := lineItem["country"].(string); ok { country = &temp }
	if temp, ok := lineItem["rcode"].(string); ok { rcode = &temp }
	if temp, ok := lineItem["region"].(string); ok { region = &temp }
	if temp, ok := lineItem["campaign_id"].(string); ok { campaignID = &temp }

	// Float fields
	parseFloat := func(key string) *float64 {
		if temp, ok := lineItem[key].(string); ok {
			if val, err := strconv.ParseFloat(temp, 64); err == nil {
				return &val
			}
		} else if val, ok := lineItem[key].(float64); ok {
			return &val
		}
		return nil
	}
	qty = parseFloat("qty")
	price = parseFloat("price")
	discount = parseFloat("discount")
	revenue = parseFloat("revenue")
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] After parseFloat('revenue'): revenue = %v\n", revenue)
	}
	if revenue == nil {
		revenue = parseFloat("amt") // Fallback to "amt" field for backward compatibility
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] After fallback parseFloat('amt'): revenue = %v\n", revenue)
		}
	}
	margin = parseFloat("margin")
	cost = parseFloat("cost")
	tax = parseFloat("tax")
	taxRate = parseFloat("tax_rate")
	commission = parseFloat("commission")
	referral = parseFloat("referral")
	fees = parseFloat("fees")
	subtotal = parseFloat("subtotal")
	total = parseFloat("total")
	paymentAmount = parseFloat("payment")

	// Date fields
	parseTime := func(key string) *time.Time {
		if temp, ok := lineItem[key].(string); ok {
			if t, err := time.Parse(time.RFC3339, temp); err == nil {
				return &t
			}
		}
		return nil
	}
	starts = parseTime("starts")
	ends = parseTime("ends")
	invoicedAt = parseTime("invoiced_at")
	paidAt = parseTime("paid_at")

	// Insert into payments table with full line item schema
	if err := i.batchInsert("payments", `INSERT INTO sfpla.payments (
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
		[]interface{}{
			lineItemID, parseUUID(oid), org, parseUUID(tid), parseUUID(uid), parseUUID(vid), parseUUID(sid), parseUUID(invid), parseUUID(orid), invoicedAt,
			product, parseUUID(productID), pcat, man, model,
			qty, duration, starts, ends,
			price, discount, revenue, margin, cost,
			tax, taxRate, commission, referral, fees,
			subtotal, total, paymentAmount,
			currency, country, rcode, region,
			campaignID, paidAt,
			formatClickHouseDateTime(created), formatClickHouseDateTime(updated),
		}, lineItem); err != nil {
		return 0.0, err
	}

	// Return revenue for LTV aggregation
	if revenue != nil {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Returning revenue: %v\n", *revenue)
		}
		return *revenue, nil
	}
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] Returning 0.0 (revenue was nil)\n")
	}
	return 0.0, nil
}

// updateLTVRecords updates LTV records for uid, vid, and orid
func (i *ClickhouseService) updateLTVRecords(hhash *string, uid, vid, invid, oid *uuid.UUID, org *string, revenue float64, updated, created *time.Time, updater, owner *uuid.UUID) error {
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] updateLTVRecords: uid=%v, vid=%v, invid=%v, revenue=%v\n", uid, vid, invid, revenue)
	}
	// Write LTV record for uid (if available)
	if uid != nil {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Adding LTV for uid\n")
		}
		ltvDataUID := map[string]interface{}{
			"hhash":      hhash,
			"id":         parseUUID(uid),
			"id_type":    "uid",
			"paid":       revenue,
			"oid":        parseUUID(oid),
			"org":        org,
			"updated_at": updated,
			"updater":    parseUUID(updater),
			"created_at": created,
			"owner":      parseUUID(owner),
		}
		if err := i.batchInsert("ltv", `INSERT INTO sfpla.ltv (
			hhash, id, id_type, paid, oid, org,
			updated_at, updater, created_at, owner
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]interface{}{hhash, uid, "uid", revenue, parseUUID(oid), org, formatClickHouseDateTime(updated), parseUUID(updater), formatClickHouseDateTime(created), parseUUID(owner)}, ltvDataUID); err != nil {
			return err
		}
	}

	// Write LTV record for vid (if available)
	if vid != nil {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] Adding LTV for vid\n")
		}
		ltvDataVID := map[string]interface{}{
			"hhash":      hhash,
			"id":         parseUUID(vid),
			"id_type":    "vid",
			"paid":       revenue,
			"oid":        parseUUID(oid),
			"org":        org,
			"updated_at": updated,
			"updater":    parseUUID(updater),
			"created_at": created,
			"owner":      parseUUID(owner),
		}
		if err := i.batchInsert("ltv", `INSERT INTO sfpla.ltv (
			hhash, id, id_type, paid, oid, org,
			updated_at, updater, created_at, owner
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]interface{}{hhash, vid, "vid", revenue, parseUUID(oid), org, formatClickHouseDateTime(updated), parseUUID(updater), formatClickHouseDateTime(created), parseUUID(owner)}, ltvDataVID); err != nil {
			return err
		}
	}

	// Write LTV record for orid (if invid is available, use as order ID)
	if invid != nil {
		ltvDataORID := map[string]interface{}{
			"hhash":      hhash,
			"id":         parseUUID(invid),
			"id_type":    "orid",
			"paid":       revenue,
			"oid":        parseUUID(oid),
			"org":        org,
			"updated_at": updated,
			"updater":    parseUUID(updater),
			"created_at": created,
			"owner":      parseUUID(owner),
		}
		if err := i.batchInsert("ltv", `INSERT INTO sfpla.ltv (
			hhash, id, id_type, paid, oid, org,
			updated_at, updater, created_at, owner
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]interface{}{hhash, invid, "orid", revenue, parseUUID(oid), org, formatClickHouseDateTime(updated), parseUUID(updater), formatClickHouseDateTime(created), parseUUID(owner)}, ltvDataORID); err != nil {
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
	Org         string                 `json:"org"`
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

	// Parse all fields using the comprehensive parser
	fields := ParseMThreadsFields(*tid, oid, v, updated)

	// Insert all 133 fields into mthreads
	err := i.batchInsert("mthreads", `INSERT INTO sfpla.mthreads (
		tid, alias, xstatus, name, ddata, provider, medium, xid, post, mtempl, mcert_id,
		cats, mtypes, fmtypes, cmtypes, admins, perms_ids, cohorts, splits,
		sent, outs, subs, pubs, vid_targets, audience_segments, content_keywords,
		consent_types, content_history, content_editors,
		prefs, interest, perf, variants, variant_weights, audience_params, audience_metrics,
		content_assumptions, content_metrics, regional_compliance, frequency_caps,
		provider_metrics, abz_params, abz_param_space, abz_model_params, attribution_params,
		opens, openp, derive, ftrack, strack, sys, archived, broadcast,
		abz_enabled, abz_auto_optimize, abz_auto_stop, abz_infinite_armed,
		requires_consent, creator_compensation,
		app, rel, ver, ptyp, etyp, ename, auth_name, source, campaign, term, promo, ref, aff,
		provider_campaign_id, provider_account_id, provider_cost, provider_status,
		funnel_stage, winner_variant, content_intention, campaign_id, campaign_status, campaign_phase,
		urgency, ephemeral, planned_impressions, actual_impressions, impression_goal,
		impression_budget, cost_per_impression, total_conversions, conversion_value,
		campaign_priority, campaign_budget_allocation, attribution_weight, attribution_window,
		data_retention, content_version, creator_compensation_rate, creator_compensation_cap,
		abz_algorithm, abz_reward_metric, abz_reward_value, abz_exploration_rate,
		abz_learning_rate, abz_start_time, abz_sample_size, abz_min_sample_size,
		abz_confidence_level, abz_winner_threshold, abz_status, abz_model_type,
		abz_acquisition_function, deleted, updatedms, content_approval_date,
		attribution_model, creator_id, content_id, content_approver, content_creator,
		creator_compensation_model, creator_notes, content_status,
		oid, org, owner, uid, vid, created_at, updated_at, updater
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?,
		?, ?, ?,
		?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?,
		?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?
	) SETTINGS insert_deduplicate = 1`,
		[]interface{}{
			fields.TID, fields.Alias, fields.XStatus, fields.Name, fields.DData,
			fields.Provider, fields.Medium, fields.XID, fields.Post, fields.MTempl, fields.MCertID,
			fields.Cats, fields.MTypes, fields.FMTypes, fields.CMTypes,
			fields.Admins, fields.PermsIDs, fields.Cohorts, fields.Splits,
			fields.Sent, fields.Outs, fields.Subs, fields.Pubs, fields.VidTargets,
			fields.AudienceSegments, fields.ContentKeywords,
			fields.ConsentTypes, fields.ContentHistory, fields.ContentEditors,
			fields.Prefs, fields.Interest, fields.Perf, fields.Variants, fields.VariantWeights,
			fields.AudienceParams, fields.AudienceMetrics,
			fields.ContentAssumptions, fields.ContentMetrics, fields.RegionalCompliance, fields.FrequencyCaps,
			fields.ProviderMetrics, fields.ABZParams, fields.ABZParamSpace, fields.ABZModelParams, fields.AttributionParams,
			fields.Opens, fields.OpenP, fields.Derive, fields.FTrack, fields.STrack,
			fields.Sys, fields.Archived, fields.Broadcast,
			fields.ABZEnabled, fields.ABZAutoOptimize, fields.ABZAutoStop, fields.ABZInfiniteArmed,
			fields.RequiresConsent, fields.CreatorCompensation,
			fields.App, fields.Rel, fields.Ver, fields.PTyp, fields.ETyp, fields.EName,
			getStringOrDefault(v["auth_name"], ""), fields.Source, fields.Campaign, fields.Term, fields.Promo, fields.Ref, fields.Aff,
			fields.ProviderCampaignID, fields.ProviderAccountID, fields.ProviderCost, fields.ProviderStatus,
			fields.FunnelStage, fields.WinnerVariant, fields.ContentIntention,
			fields.CampaignID, fields.CampaignStatus, fields.CampaignPhase,
			fields.Urgency, fields.Ephemeral, fields.PlannedImpressions, fields.ActualImpressions, fields.ImpressionGoal,
			fields.ImpressionBudget, fields.CostPerImpression, fields.TotalConversions, fields.ConversionValue,
			fields.CampaignPriority, fields.CampaignBudgetAllocation, fields.AttributionWeight, fields.AttributionWindow,
			fields.DataRetention, fields.ContentVersion, fields.CreatorCompensationRate, fields.CreatorCompensationCap,
			fields.ABZAlgorithm, fields.ABZRewardMetric, fields.ABZRewardValue, fields.ABZExplorationRate,
			fields.ABZLearningRate, fields.ABZStartTime, fields.ABZSampleSize, fields.ABZMinSampleSize,
			fields.ABZConfidenceLevel, fields.ABZWinnerThreshold, fields.ABZStatus, fields.ABZModelType,
			fields.ABZAcquisitionFunction, fields.Deleted, fields.UpdatedMS, fields.ContentApprovalDate,
			fields.AttributionModel, fields.CreatorID, fields.ContentID, fields.ContentApprover, fields.ContentCreator,
			fields.CreatorCompensationModel, fields.CreatorNotes, fields.ContentStatus,
			fields.OID, fields.Org, fields.Owner, fields.UID, fields.VID,
			fields.CreatedAt, fields.UpdatedAt, fields.Updater,
		}, v)

	if err != nil {
		return err
	}

	return nil
}

// updateMStoreTable updates the mstore table with individual event data
func (i *ClickhouseService) updateMStoreTable(ctx context.Context, tid *uuid.UUID, oid *uuid.UUID, v map[string]interface{}, updated time.Time) error {
	atomic.AddInt64(&globalMetrics.MStoreOps, 1)

	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] updateMStoreTable called with tid=%v, oid=%v\n", tid, oid)
		fmt.Printf("[DEBUG] mstore: subject='%v', msg='%v'\n", v["subject"], v["msg"])
	}

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Generate message ID
	mid := uuid.Must(uuid.NewUUID())

	// Parse all fields using the comprehensive parser
	fields := ParseMStoreFields(mid, *tid, oid, v, updated)

	// Insert fields that actually exist in mstore table (46 columns)
	err := i.batchInsert("mstore", `INSERT INTO sfpla.mstore (
		tid, mid, pmid, subject, msg, data, urgency, sys, broadcast,
		mtempl, repl, svc, qid, rid, relation, meta,
		planned, scheduled, started, completed,
		mtypes, users, deliveries, failures,
		xid, split, perms_ids, deleted, keep, createdms,
		created_at, oid, org, owner, uid, vid, updated_at, updater,
		interest, perf, hide, hidden, funnel_stage, conversion_event_count
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?
	) SETTINGS insert_deduplicate = 1`,
		[]interface{}{
			fields.TID, fields.MID, fields.PMID, fields.Subject, fields.Msg,
			getStringOrDefault(v["data"], ""), // data - from event or empty
			fields.Urgency, fields.Sys, fields.Broadcast,
			getStringOrDefault(v["mtempl"], ""), // mtempl - from event
			getJSONOrDefault(v["repl"], "{}"), // repl - JSON
			getStringOrDefault(v["svc"], ""), // svc - from event
			fields.QID, fields.RID,
			getStringOrDefault(v["relation"], ""), // relation
			getJSONOrDefault(v["meta"], "{}"), // meta - JSON
			getDateTimeOrZero(v["planned"]), // planned is DateTime64(3) - only in mstore
			getDateTimeOrZero(v["scheduled"]), getDateTimeOrZero(v["started"]),
			getDateTimeOrZero(v["completed"]),
			fields.MTypes,
			getUUIDArrayOrDefault(v["users"], []uuid.UUID{}), // users
			getUUIDArrayOrDefault(v["deliveries"], []uuid.UUID{}), // deliveries
			getUUIDArrayOrDefault(v["failures"], []uuid.UUID{}), // failures
			getStringOrDefault(v["xid"], ""), getStringOrDefault(v["split"], ""),
			getUUIDArrayOrDefault(v["perms_ids"], []uuid.UUID{}),
			formatClickHouseDateTime(&fields.Deleted), fields.Keep, fields.CreatedMS,
			formatClickHouseDateTime(&fields.CreatedAt), fields.OID, fields.Org, fields.Owner,
			fields.UID, fields.VID, formatClickHouseDateTime(&fields.UpdatedAt), fields.Updater,
			getJSONOrDefault(v["interest"], "{}"), // interest
			getJSONOrDefault(v["perf"], "{}"), // perf
			getDateTimeOrZero(v["hide"]), // hide
			getBoolOrDefault(v["hidden"], false), // hidden
			getStringOrDefault(v["funnel_stage"], ""), // funnel_stage
			getInt64OrDefault(v["conversion_event_count"], 0), // conversion_event_count
		}, v)

	if i.AppConfig.Debug {
		if err != nil {
			fmt.Printf("[DEBUG] mstore: INSERT ERROR: %v\n", err)
		} else {
			fmt.Printf("[DEBUG] mstore: INSERT SUCCESS - added to batch for table mstore\n")
		}
	}
	return err
}

// updateMTriageTable updates the mtriage table for outbound message processing
func (i *ClickhouseService) updateMTriageTable(ctx context.Context, tid *uuid.UUID, oid *uuid.UUID, v map[string]interface{}, updated time.Time) error {
	atomic.AddInt64(&globalMetrics.MTriageOps, 1)

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Debug: Log all relevant fields
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] mtriage called with tid=%v, oid=%v\n", tid, oid)
		fmt.Printf("[DEBUG] mtriage: etyp='%v', event_type='%v', ename='%v'\n", v["etyp"], v["event_type"], v["ename"])
	}

	// Only create triage entries for specific event types that require follow-up
	// Check etyp first, then event_type, then fall back to ename
	eventType := getStringValue(v["etyp"])
	if eventType == "" {
		eventType = getStringValue(v["event_type"])
	}
	if eventType == "" {
		// Fall back to ename for conversion events
		eventType = getStringValue(v["ename"])
	}
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] mtriage: final eventType='%s'\n", eventType)
	}
	if eventType != "conversion" && eventType != "high_value_action" {
		if i.AppConfig.Debug {
			fmt.Printf("[DEBUG] mtriage: skipping event='%s' (not conversion or high_value_action)\n", eventType)
		}
		return nil // Skip non-actionable events
	}
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] mtriage: PROCEEDING - event type '%s' matches criteria\n", eventType)
	}
	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] mtriage: processing event_type='%s' - will create triage entry\n", eventType)
	}

	// Generate message ID
	mid := uuid.Must(uuid.NewUUID())

	// Set default urgency to 8 for triage entries
	if _, hasUrgency := v["urgency"]; !hasUrgency {
		v["urgency"] = int32(8)
	}

	// Set default keep to true for triage entries
	if _, hasKeep := v["keep"]; !hasKeep {
		v["keep"] = true
	}

	// Parse all fields using the comprehensive parser
	fields := ParseMTriageFields(mid, *tid, oid, v, updated)

	if i.AppConfig.Debug {
		fmt.Printf("[DEBUG] mtriage: Inserting record with mid=%v, subject='%s', msg='%s'\n",
			mid, fields.Subject, fields.Msg)
	}

	// Insert fields that actually exist in mtriage table (37 columns)
	// Note: mtriage uses priority processing so no SETTINGS clause
	err := i.batchInsert("mtriage", `INSERT INTO sfpla.mtriage (
		tid, mid, pmid, subject, msg, data, urgency, sys, broadcast,
		mtempl, repl, svc, qid, rid, relation, meta,
		scheduled, started, completed,
		mtypes, users, deliveries, failures,
		xid, split, perms_ids, deleted, keep, createdms,
		created_at, oid, org, owner, uid, vid, updated_at, updater
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?,
		?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?
	)`,
		[]interface{}{
			fields.TID, fields.MID, fields.PMID, fields.Subject, fields.Msg,
			getStringOrDefault(v["data"], ""), // data
			fields.Urgency, fields.Sys, fields.Broadcast,
			getStringOrDefault(v["mtempl"], ""), // mtempl
			getJSONOrDefault(v["repl"], "{}"), // repl
			getStringOrDefault(v["svc"], ""), // svc
			fields.QID, fields.RID,
			getStringOrDefault(v["relation"], ""), // relation
			getJSONOrDefault(v["meta"], "{}"), // meta
			getDateTimeOrZero(v["scheduled"]), getDateTimeOrZero(v["started"]),
			getDateTimeOrZero(v["completed"]),
			fields.MTypes,
			getUUIDArrayOrDefault(v["users"], []uuid.UUID{}), // users
			getUUIDArrayOrDefault(v["deliveries"], []uuid.UUID{}), // deliveries
			getUUIDArrayOrDefault(v["failures"], []uuid.UUID{}), // failures
			getStringOrDefault(v["xid"], ""), getStringOrDefault(v["split"], ""),
			getUUIDArrayOrDefault(v["perms_ids"], []uuid.UUID{}),
			formatClickHouseDateTime(&fields.Deleted), fields.Keep, fields.CreatedMS,
			formatClickHouseDateTime(&fields.CreatedAt), fields.OID, fields.Org, fields.Owner,
			fields.UID, fields.VID, formatClickHouseDateTime(&fields.UpdatedAt), fields.Updater,
		}, v)

	if i.AppConfig.Debug {
		if err != nil {
			fmt.Printf("[DEBUG] mtriage: INSERT ERROR: %v\n", err)
		} else {
			fmt.Printf("[DEBUG] mtriage: INSERT SUCCESS - added to batch for table mtriage\n")
		}
	}
	return err
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

	// Extract oid - required field
	var oid uuid.UUID
	if event.OrgID != nil {
		oid = *event.OrgID
	} else {
		return fmt.Errorf("oid is required for impression_daily")
	}

	return i.batchInsert("impression_daily", `INSERT INTO sfpla.impression_daily (
		oid, org, tid, day, variant_id, total_impressions, anonymous_impressions, identified_impressions, unique_visitors, conversions, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			parseUUID(oid), event.Org, event.TID, today, event.VariantID, 1, 1, 0, 1, conversions, event.Timestamp,
		}, map[string]interface{}{
			"event_type": event.EventType,
			"variant_id": event.VariantID,
			"revenue":    revenue,
		})
}

// Helper functions for campaign telemetry integration
