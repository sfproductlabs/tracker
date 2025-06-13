package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// CampaignTelemetryService extends the existing telemetry service with campaign tracking
type CampaignTelemetryService struct {
	*ClickhouseService
}

// CampaignEvent represents a campaign tracking event
type CampaignEvent struct {
	EventID       uuid.UUID              `json:"event_id"`
	TID           uuid.UUID              `json:"tid"`           // Thread ID from mthreads
	CampaignID    uuid.UUID              `json:"campaign_id"`   // Campaign ID
	VariantID     string                 `json:"variant_id"`    // Content variant identifier
	Channel       string                 `json:"channel"`       // Marketing channel
	ContentType   string                 `json:"content_type"`  // Content type
	EventType     string                 `json:"event_type"`    // impression, click, conversion, etc.
	VisitorID     uuid.UUID              `json:"visitor_id"`    // Visitor ID
	UserID        *uuid.UUID             `json:"user_id"`       // User ID (if authenticated)
	OrgID         uuid.UUID              `json:"org_id"`        // Organization ID
	Timestamp     time.Time              `json:"timestamp"`     // Event timestamp
	Properties    map[string]interface{} `json:"properties"`    // Additional event properties
	Revenue       *float64               `json:"revenue"`       // Revenue (for conversion events)
	Cost          *float64               `json:"cost"`          // Campaign cost
	UTMSource     *string                `json:"utm_source"`    // UTM source
	UTMCampaign   *string                `json:"utm_campaign"`  // UTM campaign
	UTMMedium     *string                `json:"utm_medium"`    // UTM medium
	UTMContent    *string                `json:"utm_content"`   // UTM content
	UTMTerm       *string                `json:"utm_term"`      // UTM term
	UserAgent     *string                `json:"user_agent"`    // User agent
	IPAddress     *string                `json:"ip_address"`    // IP address
	Referrer      *string                `json:"referrer"`      // Referrer URL
	PageURL       *string                `json:"page_url"`      // Page URL
}

// CampaignContext represents contextual information for campaign events
type CampaignContext struct {
	DeviceType       *string `json:"device_type"`
	BrowserType      *string `json:"browser_type"`
	OperatingSystem  *string `json:"operating_system"`
	Country          *string `json:"country"`
	Region           *string `json:"region"`
	City             *string `json:"city"`
	TimeZone         *string `json:"timezone"`
	Language         *string `json:"language"`
	ScreenResolution *string `json:"screen_resolution"`
}

// NewCampaignTelemetryService creates a new campaign telemetry service
func NewCampaignTelemetryService(clickhouseService *ClickhouseService) *CampaignTelemetryService {
	return &CampaignTelemetryService{
		ClickhouseService: clickhouseService,
	}
}

// TrackCampaignEvent tracks a campaign-related event
func (c *CampaignTelemetryService) TrackCampaignEvent(event *CampaignEvent) error {
	ctx := context.Background()

	// Insert into events table with campaign context
	err := (*c.Session).Exec(ctx, `
		INSERT INTO events (
			eid, vid, uid, org, hhash, created, updated, ename, etyp, 
			source, medium, campaign, content, term, params, nparams,
			xid, split, tags, host, ip, country, city, region, ref,
			pv, sc, os, browser, device, lang, tz, last_x, tid,
			custom_properties, revenue, cost
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, 
			?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?
		)`,
		event.EventID,
		event.VisitorID,
		event.UserID,
		event.OrgID,
		c.generateHostHash(event.PageURL),
		event.Timestamp,
		event.Timestamp,
		event.EventType,
		"campaign",
		event.UTMSource,
		event.UTMMedium,
		event.UTMCampaign,
		event.UTMContent,
		event.UTMTerm,
		c.convertPropertiesToParams(event.Properties),
		c.convertPropertiesToNParams(event.Properties),
		event.VariantID,                              // xid - experiment ID
		fmt.Sprintf("%s_%s", event.Channel, event.ContentType), // split
		[]string{event.Channel, event.ContentType},  // tags
		c.extractHost(event.PageURL),
		event.IPAddress,
		c.extractCountryFromIP(event.IPAddress),
		c.extractCityFromIP(event.IPAddress),
		c.extractRegionFromIP(event.IPAddress),
		event.Referrer,
		event.PageURL,
		c.extractScreenResolution(event.UserAgent),
		c.extractOS(event.UserAgent),
		c.extractBrowser(event.UserAgent),
		c.extractDeviceType(event.UserAgent),
		c.extractLanguage(event.UserAgent),
		c.extractTimezone(event.IPAddress),
		event.Timestamp,
		event.TID,
		event.Properties,
		event.Revenue,
		event.Cost,
	)

	if err != nil {
		return fmt.Errorf("failed to insert campaign event: %v", err)
	}

	// Update mthreads with latest event
	err = c.updateMThreads(event)
	if err != nil {
		// Log but don't fail the event tracking
		fmt.Printf("[WARNING] Failed to update mthreads: %v\n", err)
	}

	// Update campaign performance metrics
	err = c.updateCampaignMetrics(event)
	if err != nil {
		// Log but don't fail the event tracking
		fmt.Printf("[WARNING] Failed to update campaign metrics: %v\n", err)
	}

	// Update bandit performance if this is a conversion event
	if event.EventType == "conversion" {
		err = c.updateBanditPerformance(event)
		if err != nil {
			fmt.Printf("[WARNING] Failed to update bandit performance: %v\n", err)
		}
	}

	return nil
}

// updateMThreads updates the mthreads table with campaign performance
func (c *CampaignTelemetryService) updateMThreads(event *CampaignEvent) error {
	ctx := context.Background()

	// Get current thread data
	var currentMetrics map[string]interface{}
	err := (*c.Session).QueryRow(ctx, `
		SELECT provider_metrics 
		FROM mthreads 
		WHERE tid = ? AND org = ?
	`, event.TID, event.OrgID).Scan(&currentMetrics)

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Initialize metrics if empty
	if currentMetrics == nil {
		currentMetrics = make(map[string]interface{})
	}

	// Update metrics based on event type
	switch event.EventType {
	case "impression":
		currentMetrics["total_impressions"] = c.incrementMetric(currentMetrics, "total_impressions")
	case "click":
		currentMetrics["total_clicks"] = c.incrementMetric(currentMetrics, "total_clicks")
	case "conversion":
		currentMetrics["total_conversions"] = c.incrementMetric(currentMetrics, "total_conversions")
		if event.Revenue != nil {
			currentMetrics["total_revenue"] = c.addToMetric(currentMetrics, "total_revenue", *event.Revenue)
		}
	}

	if event.Cost != nil {
		currentMetrics["total_cost"] = c.addToMetric(currentMetrics, "total_cost", *event.Cost)
	}

	currentMetrics["last_event_time"] = event.Timestamp
	currentMetrics["last_event_type"] = event.EventType

	// Calculate derived metrics
	impressions := c.getMetricValue(currentMetrics, "total_impressions")
	clicks := c.getMetricValue(currentMetrics, "total_clicks")
	conversions := c.getMetricValue(currentMetrics, "total_conversions")

	if impressions > 0 {
		currentMetrics["ctr"] = clicks / impressions
		currentMetrics["conversion_rate"] = conversions / impressions
	}

	cost := c.getMetricValue(currentMetrics, "total_cost")
	revenue := c.getMetricValue(currentMetrics, "total_revenue")

	if cost > 0 {
		currentMetrics["roas"] = revenue / cost
		if conversions > 0 {
			currentMetrics["cpa"] = cost / conversions
		}
	}

	// Update mthreads table
	return (*c.Session).Exec(ctx, `
		INSERT INTO mthreads (
			tid, org, provider_metrics, updated_at
		) VALUES (?, ?, ?, ?)
	`, event.TID, event.OrgID, currentMetrics, event.Timestamp)
}

// updateCampaignMetrics updates campaign-specific metrics tables
func (c *CampaignTelemetryService) updateCampaignMetrics(event *CampaignEvent) error {
	ctx := context.Background()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)

	// Update daily impression metrics
	err := (*c.Session).Exec(ctx, `
		INSERT INTO impression_daily (
			date, tid, variant_id, total_impressions, conversions, 
			unique_visitors, revenue, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		today,
		event.TID,
		event.VariantID,
		c.getEventValue(event.EventType, "impression"),
		c.getEventValue(event.EventType, "conversion"),
		1, // Assume each event represents a unique visitor interaction
		c.getRevenueValue(event),
		event.Timestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to update daily metrics: %v", err)
	}

	// Update hourly impression metrics
	err = (*c.Session).Exec(ctx, `
		INSERT INTO impression_hourly (
			hour, tid, variant_id, total_impressions, conversions,
			unique_visitors, revenue, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		currentHour,
		event.TID,
		event.VariantID,
		c.getEventValue(event.EventType, "impression"),
		c.getEventValue(event.EventType, "conversion"),
		1,
		c.getRevenueValue(event),
		event.Timestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to update hourly metrics: %v", err)
	}

	// Update channel metrics
	err = (*c.Session).Exec(ctx, `
		INSERT INTO channel_metrics (
			date, channel, impressions, conversions, conversion_value, updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`,
		today,
		event.Channel,
		c.getEventValue(event.EventType, "impression"),
		c.getEventValue(event.EventType, "conversion"),
		c.getRevenueValue(event),
		event.Timestamp,
	)

	return err
}

// updateBanditPerformance updates bandit decision performance
func (c *CampaignTelemetryService) updateBanditPerformance(event *CampaignEvent) error {
	ctx := context.Background()

	// Update variant performance
	err := (*c.Session).Exec(ctx, `
		INSERT INTO variant_performance (
			tid, variant_id, impressions, rewards, probability,
			expected_value, confidence_interval_lower, 
			confidence_interval_upper, last_updated
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		event.TID,
		event.VariantID,
		c.getEventValue(event.EventType, "impression"),
		c.getEventValue(event.EventType, "conversion"),
		0.0, // Will be calculated by bandit algorithm
		c.getRevenueValue(event),
		0.0, // Will be calculated by bandit algorithm
		1.0, // Will be calculated by bandit algorithm
		event.Timestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to update variant performance: %v", err)
	}

	// Find and update related bandit decision
	var decisionID uuid.UUID
	err = (*c.Session).QueryRow(ctx, `
		SELECT decision_id 
		FROM bandit_decisions 
		WHERE tid = ? AND chosen_variant = ? 
		ORDER BY created_at DESC 
		LIMIT 1
	`, event.TID, event.VariantID).Scan(&decisionID)

	if err == nil {
		// Update the bandit decision with reward
		err = (*c.Session).Exec(ctx, `
			INSERT INTO bandit_decisions (
				decision_id, tid, chosen_variant, context, reward_received,
				reward_value, algorithm_state, exploration, belief_states, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			decisionID,
			event.TID,
			event.VariantID,
			c.convertPropertiesToJSON(event.Properties),
			true,
			c.getRevenueValue(event),
			"{}",
			false,
			"{}",
			event.Timestamp,
		)
	}

	return err
}

// Helper functions
func (c *CampaignTelemetryService) generateHostHash(pageURL *string) *string {
	if pageURL == nil {
		return nil
	}
	host := c.extractHost(pageURL)
	if host == nil {
		return nil
	}
	hashValue := strconv.FormatInt(int64(hash(*host)), 36)
	return &hashValue
}

func (c *CampaignTelemetryService) extractHost(pageURL *string) *string {
	if pageURL == nil {
		return nil
	}
	// Simple host extraction - in production, use proper URL parsing
	if strings.Contains(*pageURL, "://") {
		parts := strings.Split(*pageURL, "://")
		if len(parts) > 1 {
			hostPart := strings.Split(parts[1], "/")[0]
			return &hostPart
		}
	}
	return nil
}

func (c *CampaignTelemetryService) convertPropertiesToParams(properties map[string]interface{}) map[string]string {
	if properties == nil {
		return nil
	}
	
	params := make(map[string]string)
	for key, value := range properties {
		params[key] = fmt.Sprintf("%v", value)
	}
	return params
}

func (c *CampaignTelemetryService) convertPropertiesToNParams(properties map[string]interface{}) map[string]float64 {
	if properties == nil {
		return nil
	}
	
	nparams := make(map[string]float64)
	for key, value := range properties {
		if numValue, ok := value.(float64); ok {
			nparams[key] = numValue
		} else if intValue, ok := value.(int); ok {
			nparams[key] = float64(intValue)
		}
	}
	return nparams
}

func (c *CampaignTelemetryService) convertPropertiesToJSON(properties map[string]interface{}) string {
	if properties == nil {
		return "{}"
	}
	
	jsonBytes, err := json.Marshal(properties)
	if err != nil {
		return "{}"
	}
	return string(jsonBytes)
}

func (c *CampaignTelemetryService) incrementMetric(metrics map[string]interface{}, key string) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue + 1
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue) + 1
		}
	}
	return 1
}

func (c *CampaignTelemetryService) addToMetric(metrics map[string]interface{}, key string, addition float64) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue + addition
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue) + addition
		}
	}
	return addition
}

func (c *CampaignTelemetryService) getMetricValue(metrics map[string]interface{}, key string) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue)
		}
	}
	return 0
}

func (c *CampaignTelemetryService) getEventValue(eventType, targetType string) int {
	if eventType == targetType {
		return 1
	}
	return 0
}

func (c *CampaignTelemetryService) getRevenueValue(event *CampaignEvent) float64 {
	if event.Revenue != nil {
		return *event.Revenue
	}
	return 0
}

// Extraction helper functions (simplified implementations)
func (c *CampaignTelemetryService) extractCountryFromIP(ip *string) *string {
	// In production, use a proper GeoIP service
	if ip == nil {
		return nil
	}
	country := "Unknown"
	return &country
}

func (c *CampaignTelemetryService) extractCityFromIP(ip *string) *string {
	if ip == nil {
		return nil
	}
	city := "Unknown"
	return &city
}

func (c *CampaignTelemetryService) extractRegionFromIP(ip *string) *string {
	if ip == nil {
		return nil
	}
	region := "Unknown"
	return &region
}

func (c *CampaignTelemetryService) extractScreenResolution(userAgent *string) *string {
	if userAgent == nil {
		return nil
	}
	// Extract from user agent or use default
	resolution := "1920x1080"
	return &resolution
}

func (c *CampaignTelemetryService) extractOS(userAgent *string) *string {
	if userAgent == nil {
		return nil
	}
	if strings.Contains(*userAgent, "Windows") {
		os := "Windows"
		return &os
	} else if strings.Contains(*userAgent, "Mac") {
		os := "macOS"
		return &os
	} else if strings.Contains(*userAgent, "Linux") {
		os := "Linux"
		return &os
	}
	os := "Unknown"
	return &os
}

func (c *CampaignTelemetryService) extractBrowser(userAgent *string) *string {
	if userAgent == nil {
		return nil
	}
	if strings.Contains(*userAgent, "Chrome") {
		browser := "Chrome"
		return &browser
	} else if strings.Contains(*userAgent, "Firefox") {
		browser := "Firefox"
		return &browser
	} else if strings.Contains(*userAgent, "Safari") {
		browser := "Safari"
		return &browser
	}
	browser := "Unknown"
	return &browser
}

func (c *CampaignTelemetryService) extractDeviceType(userAgent *string) *string {
	if userAgent == nil {
		return nil
	}
	if strings.Contains(*userAgent, "Mobile") {
		device := "Mobile"
		return &device
	} else if strings.Contains(*userAgent, "Tablet") {
		device := "Tablet"
		return &device
	}
	device := "Desktop"
	return &device
}

func (c *CampaignTelemetryService) extractLanguage(userAgent *string) *string {
	if userAgent == nil {
		return nil
	}
	lang := "en"
	return &lang
}

func (c *CampaignTelemetryService) extractTimezone(ip *string) *string {
	if ip == nil {
		return nil
	}
	tz := "UTC"
	return &tz
}

// HTTP handlers for campaign tracking

// HandleCampaignTracking handles HTTP requests for campaign event tracking
func (c *CampaignTelemetryService) HandleCampaignTracking(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event CampaignEvent
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Set defaults
	if event.EventID == uuid.Nil {
		event.EventID = uuid.New()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	// Extract IP and User Agent from request
	if event.IPAddress == nil {
		ip := getIP(r)
		event.IPAddress = &ip
	}
	if event.UserAgent == nil {
		ua := r.UserAgent()
		event.UserAgent = &ua
	}

	// Track the event
	err = c.TrackCampaignEvent(&event)
	if err != nil {
		http.Error(w, "Failed to track event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"event_id":  event.EventID,
		"timestamp": event.Timestamp,
	})
}

// HandleCampaignRedirect handles redirect tracking with campaign context
func (c *CampaignTelemetryService) HandleCampaignRedirect(w http.ResponseWriter, r *http.Request) {
	// Extract campaign parameters from URL
	tidParam := r.URL.Query().Get("tid")
	campaignID := r.URL.Query().Get("cid")
	variantID := r.URL.Query().Get("vid")
	channel := r.URL.Query().Get("ch")
	contentType := r.URL.Query().Get("ct")

	// Get the target URL from redirects table
	ctx := context.Background()
	var targetURL string
	err := (*c.Session).QueryRow(ctx,
		`SELECT urlto FROM redirects WHERE urlfrom = ?`,
		fmt.Sprintf("%s%s", r.Host, r.URL.Path)).Scan(&targetURL)

	if err != nil {
		http.Error(w, "Redirect not found", http.StatusNotFound)
		return
	}

	// Track click event if campaign parameters are present
	if tidParam != "" && campaignID != "" {
		tid, _ := uuid.Parse(tidParam)
		cid, _ := uuid.Parse(campaignID)
		vid := uuid.New() // Generate visitor ID

		clickEvent := &CampaignEvent{
			EventID:     uuid.New(),
			TID:         tid,
			CampaignID:  cid,
			VariantID:   variantID,
			Channel:     channel,
			ContentType: contentType,
			EventType:   "click",
			VisitorID:   vid,
			Timestamp:   time.Now().UTC(),
			Properties: map[string]interface{}{
				"redirect_url": targetURL,
				"source_url":   r.URL.String(),
			},
		}

		// Extract additional context
		ip := getIP(r)
		ua := r.UserAgent()
		ref := r.Referer()

		clickEvent.IPAddress = &ip
		clickEvent.UserAgent = &ua
		clickEvent.Referrer = &ref
		clickEvent.PageURL = &targetURL

		// Track asynchronously to not delay redirect
		go func() {
			err := c.TrackCampaignEvent(clickEvent)
			if err != nil {
				fmt.Printf("[ERROR] Failed to track click event: %v\n", err)
			}
		}()
	}

	// Perform redirect
	http.Redirect(w, r, targetURL, http.StatusFound)
}

// getIP extracts IP address from request
func getIP(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		return strings.Split(forwarded, ",")[0]
	}
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}
	return r.RemoteAddr
}

// hash function for generating host hashes (simplified)
func hash(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}