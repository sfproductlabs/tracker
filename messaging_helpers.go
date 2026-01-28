package main

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// MessagingFieldParser helps parse all fields for messaging tables
type MessagingFieldParser struct {
	data map[string]interface{}
}

// NewMessagingFieldParser creates a new parser instance
func NewMessagingFieldParser(data map[string]interface{}) *MessagingFieldParser {
	return &MessagingFieldParser{data: data}
}

// Parse UUID arrays
func (p *MessagingFieldParser) ParseUUIDArray(key string) []string {
	result := []string{}
	if val, ok := p.data[key]; ok {
		switch v := val.(type) {
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok {
					if _, err := uuid.Parse(str); err == nil {
						result = append(result, str)
					}
				}
			}
		case []string:
			for _, str := range v {
				if _, err := uuid.Parse(str); err == nil {
					result = append(result, str)
				}
			}
		}
	}
	return result
}

// Parse string arrays
func (p *MessagingFieldParser) ParseStringArray(key string) []string {
	result := []string{}
	if val, ok := p.data[key]; ok {
		switch v := val.(type) {
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok {
					result = append(result, str)
				}
			}
		case []string:
			result = v
		}
	}
	return result
}

// Parse optional UUID
func (p *MessagingFieldParser) ParseOptionalUUID(key string) string {
	if val, ok := p.data[key].(string); ok {
		if _, err := uuid.Parse(val); err == nil {
			return val
		}
	}
	return "00000000-0000-0000-0000-000000000000"
}

// Parse boolean with default
func (p *MessagingFieldParser) ParseBool(key string, defaultVal bool) bool {
	if val, ok := p.data[key].(bool); ok {
		return val
	}
	return defaultVal
}

// Parse Int32 with default
func (p *MessagingFieldParser) ParseInt32(key string, defaultVal int32) int32 {
	if val, ok := p.data[key]; ok {
		switch v := val.(type) {
		case float64:
			return int32(v)
		case int:
			return int32(v)
		case int32:
			return v
		case int64:
			return int32(v)
		}
	}
	return defaultVal
}

// Parse Int64 with default
func (p *MessagingFieldParser) ParseInt64(key string, defaultVal int64) int64 {
	if val, ok := p.data[key]; ok {
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case int64:
			return v
		}
	}
	return defaultVal
}

// Parse Float64 with default
func (p *MessagingFieldParser) ParseFloat64(key string, defaultVal float64) float64 {
	if val, ok := p.data[key]; ok {
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
	return defaultVal
}

// Parse string with default
func (p *MessagingFieldParser) ParseString(key string, defaultVal string) string {
	if val, ok := p.data[key].(string); ok {
		return val
	}
	return defaultVal
}

// Parse JSON field
func (p *MessagingFieldParser) ParseJSON(key string) string {
	if val, ok := p.data[key]; ok {
		// If it's already a string, assume it's JSON
		if str, ok := val.(string); ok {
			return str
		}
		// Otherwise, marshal it to JSON
		if jsonBytes, err := json.Marshal(val); err == nil {
			return string(jsonBytes)
		}
	}
	return "{}"
}

// Parse DateTime64 with default
func (p *MessagingFieldParser) ParseDateTime(key string) time.Time {
	if val, ok := p.data[key]; ok {
		switch v := val.(type) {
		case time.Time:
			return v
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return t
			}
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return t
			}
		case int64:
			// Unix timestamp
			return time.Unix(v, 0)
		case float64:
			// Unix timestamp as float
			return time.Unix(int64(v), 0)
		}
	}
	return time.Time{} // Zero time
}

// MThreadsFields contains all 133 fields for mthreads table
type MThreadsFields struct {
	// Core identification
	TID                uuid.UUID
	Alias              string
	XStatus            string
	Name               string
	DData              string
	Provider           string
	Medium             string

	// Thread metadata
	XID                string
	Post               string
	MTempl             string
	MCertID            string

	// Arrays
	Cats               []string
	MTypes             []string
	FMTypes            []string
	CMTypes            []string
	Admins             []string
	PermsIDs           []string
	Cohorts            []string
	Splits             string // JSON
	Sent               []string
	Outs               []string
	Subs               []string
	Pubs               []string
	VidTargets         []string
	AudienceSegments   []string
	ContentKeywords    []string
	ConsentTypes       []string
	ContentHistory     []string
	ContentEditors     []string

	// JSON fields
	Prefs              string
	Interest           string
	Perf               string
	Variants           string
	VariantWeights     string
	AudienceParams     string
	AudienceMetrics    string
	ContentAssumptions string
	ContentMetrics     string
	RegionalCompliance string
	FrequencyCaps      string
	ProviderMetrics    string
	ABZParams          string
	ABZParamSpace      string
	ABZModelParams     string
	AttributionParams  string

	// Boolean flags
	Opens              bool
	OpenP              bool
	Derive             bool
	FTrack             bool
	STrack             bool
	Sys                bool
	Archived           bool
	Broadcast          bool
	ABZEnabled         bool
	ABZAutoOptimize    bool
	ABZAutoStop        bool
	ABZInfiniteArmed   bool
	RequiresConsent    bool
	CreatorCompensation bool

	// Campaign fields
	App                string
	Rel                string
	Ver                int32
	PTyp               string
	ETyp               string
	EName              string
	Auth               string
	Source             string
	Campaign           string
	Term               string
	Promo              string
	Ref                string
	Aff                string
	ProviderCampaignID string
	ProviderAccountID  string
	ProviderCost       float64
	ProviderStatus     string
	FunnelStage        string
	WinnerVariant      string
	ContentIntention   string
	CampaignID         string
	CampaignStatus     string
	CampaignPhase      string

	// Numeric metrics
	Urgency            int32
	Ephemeral          int32
	PlannedImpressions int64
	ActualImpressions  int64
	ImpressionGoal     int64
	ImpressionBudget   float64
	CostPerImpression  float64
	TotalConversions   int64
	ConversionValue    float64
	CampaignPriority   int32
	CampaignBudgetAllocation float64
	AttributionWeight  float64
	AttributionWindow  int32
	DataRetention      int32
	ContentVersion     int32
	CreatorCompensationRate float64
	CreatorCompensationCap  float64

	// ABZ Testing fields
	ABZAlgorithm       string
	ABZRewardMetric    string
	ABZRewardValue     float64
	ABZExplorationRate float64
	ABZLearningRate    float64
	ABZStartTime       time.Time
	ABZSampleSize      int64
	ABZMinSampleSize   int64
	ABZConfidenceLevel float64
	ABZWinnerThreshold float64
	ABZStatus          string
	ABZModelType       string
	ABZAcquisitionFunction string

	// Timestamps
	Deleted            time.Time
	UpdatedMS          int64
	ContentApprovalDate time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time

	// Attribution
	AttributionModel   string

	// Creator fields
	CreatorID          string
	ContentID          string
	ContentApprover    string
	ContentCreator     string
	CreatorCompensationModel string
	CreatorNotes       string
	ContentStatus      string

	// Organization
	OID                string
	Org                string
	Owner              string
	UID                string
	VID                string
	Updater            string
}

// ParseMThreadsFields parses all fields for mthreads table
func ParseMThreadsFields(tid uuid.UUID, oid *uuid.UUID, data map[string]interface{}, updated time.Time) *MThreadsFields {
	p := NewMessagingFieldParser(data)

	// Convert oid to string for storage
	oidStr := "00000000-0000-0000-0000-000000000000"
	if oid != nil {
		oidStr = oid.String()
	}

	return &MThreadsFields{
		// Core identification
		TID:      tid,
		Alias:    p.ParseString("alias", ""),
		XStatus:  p.ParseString("xstatus", ""),
		Name:     p.ParseString("name", ""),
		DData:    p.ParseString("ddata", ""),
		Provider: p.ParseString("provider", ""),
		Medium:   p.ParseString("medium", ""),

		// Thread metadata
		XID:      p.ParseString("xid", ""),
		Post:     p.ParseString("post", ""),
		MTempl:   p.ParseJSON("mtempl"),
		MCertID:  p.ParseOptionalUUID("mcert_id"),

		// Arrays
		Cats:             p.ParseStringArray("cats"),
		MTypes:           p.ParseStringArray("mtypes"),
		FMTypes:          p.ParseStringArray("fmtypes"),
		CMTypes:          p.ParseStringArray("cmtypes"),
		Admins:           p.ParseUUIDArray("admins"),
		PermsIDs:         p.ParseUUIDArray("perms_ids"),
		Cohorts:          p.ParseStringArray("cohorts"),
		Splits:           p.ParseJSON("splits"),
		Sent:             p.ParseUUIDArray("sent"),
		Outs:             p.ParseUUIDArray("outs"),
		Subs:             p.ParseUUIDArray("subs"),
		Pubs:             p.ParseUUIDArray("pubs"),
		VidTargets:       p.ParseUUIDArray("vid_targets"),
		AudienceSegments: p.ParseStringArray("audience_segments"),
		ContentKeywords:  p.ParseStringArray("content_keywords"),
		ConsentTypes:     p.ParseStringArray("consent_types"),
		ContentHistory:   p.ParseUUIDArray("content_history"),
		ContentEditors:   p.ParseUUIDArray("content_editors"),

		// JSON fields
		Prefs:              p.ParseJSON("prefs"),
		Interest:           p.ParseJSON("interest"),
		Perf:               p.ParseJSON("perf"),
		Variants:           p.ParseJSON("variants"),
		VariantWeights:     p.ParseJSON("variant_weights"),
		AudienceParams:     p.ParseJSON("audience_params"),
		AudienceMetrics:    p.ParseJSON("audience_metrics"),
		ContentAssumptions: p.ParseJSON("content_assumptions"),
		ContentMetrics:     p.ParseJSON("content_metrics"),
		RegionalCompliance: p.ParseJSON("regional_compliance"),
		FrequencyCaps:      p.ParseJSON("frequency_caps"),
		ProviderMetrics:    p.ParseJSON("provider_metrics"),
		ABZParams:          p.ParseJSON("abz_params"),
		ABZParamSpace:      p.ParseJSON("abz_param_space"),
		ABZModelParams:     p.ParseJSON("abz_model_params"),
		AttributionParams:  p.ParseJSON("attribution_params"),

		// Boolean flags
		Opens:               p.ParseBool("opens", false),
		OpenP:               p.ParseBool("openp", false),
		Derive:              p.ParseBool("derive", false),
		FTrack:              p.ParseBool("ftrack", false),
		STrack:              p.ParseBool("strack", false),
		Sys:                 p.ParseBool("sys", false),
		Archived:            p.ParseBool("archived", false),
		Broadcast:           p.ParseBool("broadcast", false),
		ABZEnabled:          p.ParseBool("abz_enabled", false),
		ABZAutoOptimize:     p.ParseBool("abz_auto_optimize", false),
		ABZAutoStop:         p.ParseBool("abz_auto_stop", false),
		ABZInfiniteArmed:    p.ParseBool("abz_infinite_armed", false),
		RequiresConsent:     p.ParseBool("requires_consent", false),
		CreatorCompensation: p.ParseBool("creator_compensation", false),

		// Campaign fields
		App:                p.ParseString("app", ""),
		Rel:                p.ParseString("rel", ""),
		Ver:                p.ParseInt32("ver", 0),
		PTyp:               p.ParseString("ptyp", ""),
		ETyp:               p.ParseString("etyp", ""),
		EName:              p.ParseString("ename", ""),
		Auth:               p.ParseString("auth", ""),
		Source:             p.ParseString("source", ""),
		Campaign:           p.ParseString("campaign", ""),
		Term:               p.ParseString("term", ""),
		Promo:              p.ParseString("promo", ""),
		Ref:                p.ParseOptionalUUID("ref"),
		Aff:                p.ParseString("aff", ""),
		ProviderCampaignID: p.ParseString("provider_campaign_id", ""),
		ProviderAccountID:  p.ParseString("provider_account_id", ""),
		ProviderCost:       p.ParseFloat64("provider_cost", 0.0),
		ProviderStatus:     p.ParseString("provider_status", ""),
		FunnelStage:        p.ParseString("funnel_stage", ""),
		WinnerVariant:      p.ParseString("winner_variant", ""),
		ContentIntention:   p.ParseString("content_intention", ""),
		CampaignID:         p.ParseString("campaign_id", ""),
		CampaignStatus:     p.ParseString("campaign_status", ""),
		CampaignPhase:      p.ParseString("campaign_phase", ""),

		// Numeric metrics
		Urgency:                  p.ParseInt32("urgency", 0),
		Ephemeral:                p.ParseInt32("ephemeral", 0),
		PlannedImpressions:       p.ParseInt64("planned_impressions", 0),
		ActualImpressions:        p.ParseInt64("actual_impressions", 0),
		ImpressionGoal:           p.ParseInt64("impression_goal", 0),
		ImpressionBudget:         p.ParseFloat64("impression_budget", 0.0),
		CostPerImpression:        p.ParseFloat64("cost_per_impression", 0.0),
		TotalConversions:         p.ParseInt64("total_conversions", 0),
		ConversionValue:          p.ParseFloat64("conversion_value", 0.0),
		CampaignPriority:         p.ParseInt32("campaign_priority", 0),
		CampaignBudgetAllocation: p.ParseFloat64("campaign_budget_allocation", 0.0),
		AttributionWeight:        p.ParseFloat64("attribution_weight", 0.0),
		AttributionWindow:        p.ParseInt32("attribution_window", 0),
		DataRetention:            p.ParseInt32("data_retention", 0),
		ContentVersion:           p.ParseInt32("content_version", 0),
		CreatorCompensationRate:  p.ParseFloat64("creator_compensation_rate", 0.0),
		CreatorCompensationCap:   p.ParseFloat64("creator_compensation_cap", 0.0),

		// ABZ Testing fields
		ABZAlgorithm:           p.ParseString("abz_algorithm", ""),
		ABZRewardMetric:        p.ParseString("abz_reward_metric", ""),
		ABZRewardValue:         p.ParseFloat64("abz_reward_value", 0.0),
		ABZExplorationRate:     p.ParseFloat64("abz_exploration_rate", 0.0),
		ABZLearningRate:        p.ParseFloat64("abz_learning_rate", 0.0),
		ABZStartTime:           p.ParseDateTime("abz_start_time"),
		ABZSampleSize:          p.ParseInt64("abz_sample_size", 0),
		ABZMinSampleSize:       p.ParseInt64("abz_min_sample_size", 0),
		ABZConfidenceLevel:     p.ParseFloat64("abz_confidence_level", 0.0),
		ABZWinnerThreshold:     p.ParseFloat64("abz_winner_threshold", 0.0),
		ABZStatus:              p.ParseString("abz_status", ""),
		ABZModelType:           p.ParseString("abz_model_type", ""),
		ABZAcquisitionFunction: p.ParseString("abz_acquisition_function", ""),

		// Timestamps
		Deleted:             p.ParseDateTime("deleted"),
		UpdatedMS:           p.ParseInt64("updatedms", 0),
		ContentApprovalDate: p.ParseDateTime("content_approval_date"),
		CreatedAt:           updated,
		UpdatedAt:           updated,

		// Attribution
		AttributionModel: p.ParseString("attribution_model", ""),

		// Creator fields
		CreatorID:                p.ParseOptionalUUID("creator_id"),
		ContentID:                p.ParseOptionalUUID("content_id"),
		ContentApprover:          p.ParseOptionalUUID("content_approver"),
		ContentCreator:           p.ParseOptionalUUID("content_creator"),
		CreatorCompensationModel: p.ParseString("creator_compensation_model", ""),
		CreatorNotes:             p.ParseString("creator_notes", ""),
		ContentStatus:            p.ParseString("content_status", ""),

		// Organization
		OID:     oidStr,
		Org:     p.ParseString("org", ""),
		Owner:   p.ParseOptionalUUID("owner"),
		UID:     p.ParseOptionalUUID("uid"),
		VID:     p.ParseOptionalUUID("vid"),
		Updater: p.ParseOptionalUUID("updater"),
	}
}

// MStoreFields contains all 47 fields for mstore table
type MStoreFields struct {
	// Core identification
	MID         uuid.UUID
	TID         uuid.UUID
	PMID        string
	QID         string
	RID         string
	Category    string

	// Message metadata
	Subject     string
	Msg         string
	Encoding    string
	Priority    int32
	Urgency     int32
	Deleted     time.Time
	Archived    bool

	// Message types and properties
	MTypes      []string
	ImpCount    int64
	ClickCount  int64
	OpenCount   int64
	ConversionEventCount int64
	ConversionRevenue float64

	// Timestamps
	PausedAt    time.Time
	CancelledAt time.Time
	CreatedMS   int64
	UpdatedMS   int64
	DeliveredAt time.Time
	OpenedAt    time.Time
	ClickedAt   time.Time
	ConvertedAt time.Time
	Declined    time.Time

	// Status flags
	Status      string
	Delivered   bool
	Failed      bool
	Bounced     bool
	Complained  bool
	Unsubscribed bool
	Sys         bool
	Broadcast   bool
	Keep        bool
	Hidden      bool

	// Organization
	OID         string
	Org         string
	Owner       string
	UID         string
	VID         string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Updater     string
}

// ParseMStoreFields parses all fields for mstore table
func ParseMStoreFields(mid uuid.UUID, tid uuid.UUID, oid *uuid.UUID, data map[string]interface{}, updated time.Time) *MStoreFields {
	p := NewMessagingFieldParser(data)

	// Convert oid to string for storage
	oidStr := "00000000-0000-0000-0000-000000000000"
	if oid != nil {
		oidStr = oid.String()
	}

	return &MStoreFields{
		// Core identification
		MID:      mid,
		TID:      tid,
		PMID:     p.ParseOptionalUUID("pmid"),
		QID:      p.ParseOptionalUUID("qid"),
		RID:      p.ParseOptionalUUID("rid"),
		Category: p.ParseString("category", ""),

		// Message metadata
		Subject:  p.ParseString("subject", ""),
		Msg:      p.ParseString("msg", ""),
		Encoding: p.ParseString("encoding", ""),
		Priority: p.ParseInt32("priority", 0),
		Urgency:  p.ParseInt32("urgency", 0),
		Deleted:  p.ParseDateTime("deleted"),
		Archived: p.ParseBool("archived", false),

		// Message types and properties
		MTypes:               p.ParseStringArray("mtypes"),
		ImpCount:             p.ParseInt64("imp_count", 0),
		ClickCount:           p.ParseInt64("click_count", 0),
		OpenCount:            p.ParseInt64("open_count", 0),
		ConversionEventCount: p.ParseInt64("conversion_event_count", 0),
		ConversionRevenue:    p.ParseFloat64("conversion_revenue", 0.0),

		// Timestamps
		PausedAt:    p.ParseDateTime("paused_at"),
		CancelledAt: p.ParseDateTime("cancelled_at"),
		CreatedMS:   p.ParseInt64("createdms", 0),
		UpdatedMS:   p.ParseInt64("updatedms", 0),
		DeliveredAt: p.ParseDateTime("delivered_at"),
		OpenedAt:    p.ParseDateTime("opened_at"),
		ClickedAt:   p.ParseDateTime("clicked_at"),
		ConvertedAt: p.ParseDateTime("converted_at"),
		Declined:    p.ParseDateTime("declined"),

		// Status flags
		Status:       p.ParseString("status", ""),
		Delivered:    p.ParseBool("delivered", false),
		Failed:       p.ParseBool("failed", false),
		Bounced:      p.ParseBool("bounced", false),
		Complained:   p.ParseBool("complained", false),
		Unsubscribed: p.ParseBool("unsubscribed", false),
		Sys:          p.ParseBool("sys", false),
		Broadcast:    p.ParseBool("broadcast", false),
		Keep:         p.ParseBool("keep", false),
		Hidden:       p.ParseBool("hidden", false),

		// Organization
		OID:       oidStr,
		Org:       p.ParseString("org", ""),
		Owner:     p.ParseOptionalUUID("owner"),
		UID:       p.ParseOptionalUUID("uid"),
		VID:       p.ParseOptionalUUID("vid"),
		CreatedAt: updated,
		UpdatedAt: updated,
		Updater:   p.ParseOptionalUUID("updater"),
	}
}

// MTriageFields contains all 43 fields for mtriage table (subset of mstore)
type MTriageFields struct {
	// Core identification
	MID         uuid.UUID
	TID         uuid.UUID
	PMID        string
	QID         string
	RID         string
	Category    string

	// Message metadata
	Subject     string
	Msg         string
	Encoding    string
	Priority    int32
	Urgency     int32
	Deleted     time.Time
	Archived    bool

	// Message types and properties
	MTypes      []string
	ImpCount    int64
	ClickCount  int64
	OpenCount   int64
	ConversionEventCount int64
	ConversionRevenue float64

	// Timestamps (no 'planned' in mtriage)
	PausedAt    time.Time
	CancelledAt time.Time
	CreatedMS   int64
	UpdatedMS   int64
	DeliveredAt time.Time
	OpenedAt    time.Time
	ClickedAt   time.Time
	ConvertedAt time.Time
	Declined    time.Time

	// Status flags
	Status      string
	Delivered   bool
	Failed      bool
	Bounced     bool
	Complained  bool
	Unsubscribed bool
	Sys         bool
	Broadcast   bool
	Keep        bool
	Hidden      bool

	// Organization
	OID         string
	Org         string
	Owner       string
	UID         string
	VID         string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Updater     string
}

// ParseMTriageFields parses all fields for mtriage table (same as mstore minus 'planned')
func ParseMTriageFields(mid uuid.UUID, tid uuid.UUID, oid *uuid.UUID, data map[string]interface{}, updated time.Time) *MTriageFields {
	// Use same parser as mstore since fields are identical except 'planned'
	store := ParseMStoreFields(mid, tid, oid, data, updated)

	return &MTriageFields{
		// Core identification
		MID:      store.MID,
		TID:      store.TID,
		PMID:     store.PMID,
		QID:      store.QID,
		RID:      store.RID,
		Category: store.Category,

		// Message metadata
		Subject:  store.Subject,
		Msg:      store.Msg,
		Encoding: store.Encoding,
		Priority: store.Priority,
		Urgency:  store.Urgency,
		Deleted:  store.Deleted,
		Archived: store.Archived,

		// Message types and properties
		MTypes:               store.MTypes,
		ImpCount:             store.ImpCount,
		ClickCount:           store.ClickCount,
		OpenCount:            store.OpenCount,
		ConversionEventCount: store.ConversionEventCount,
		ConversionRevenue:    store.ConversionRevenue,

		// Timestamps
		PausedAt:    store.PausedAt,
		CancelledAt: store.CancelledAt,
		CreatedMS:   store.CreatedMS,
		UpdatedMS:   store.UpdatedMS,
		DeliveredAt: store.DeliveredAt,
		OpenedAt:    store.OpenedAt,
		ClickedAt:   store.ClickedAt,
		ConvertedAt: store.ConvertedAt,
		Declined:    store.Declined,

		// Status flags
		Status:       store.Status,
		Delivered:    store.Delivered,
		Failed:       store.Failed,
		Bounced:      store.Bounced,
		Complained:   store.Complained,
		Unsubscribed: store.Unsubscribed,
		Sys:          store.Sys,
		Broadcast:    store.Broadcast,
		Keep:         store.Keep,
		Hidden:       store.Hidden,

		// Organization
		OID:       store.OID,
		Org:       store.Org,
		Owner:     store.Owner,
		UID:       store.UID,
		VID:       store.VID,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
		Updater:   store.Updater,
	}
}