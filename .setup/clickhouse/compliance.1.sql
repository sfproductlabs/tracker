-- Compliance + Queues - Jurisdictions, agreements, queues, cohorts, budget management
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

CREATE TABLE zips_local ON CLUSTER tracker_cluster (

    country String DEFAULT '', -- Country code (e.g., US)
    zip String DEFAULT '', -- ZIP/postal code
    region String DEFAULT '', -- Region/state name (e.g., California)
    rcode String DEFAULT '', -- Region code (e.g., CA)
    county String DEFAULT '', -- County name
    city String DEFAULT '', -- City name
    culture String DEFAULT '', -- Culture/language code (e.g., en-US)
    population Int32 DEFAULT 0, -- Total population count
    men Int32 DEFAULT 0, -- Male population count
    women Int32 DEFAULT 0, -- Female population count
    hispanic Float64 DEFAULT 0.0, -- Hispanic population percentage
    white Float64 DEFAULT 0.0, -- White population percentage
    black Float64 DEFAULT 0.0, -- Black population percentage
    native Float64 DEFAULT 0.0, -- Native American population percentage
    asian Float64 DEFAULT 0.0, -- Asian population percentage
    pacific Float64 DEFAULT 0.0, -- Pacific Islander population percentage
    voters Int32 DEFAULT 0, -- Registered voters count
    income Float64 DEFAULT 0.0, -- Average income
    incomeerr Float64 DEFAULT 0.0, -- Income margin of error
    incomepercap Float64 DEFAULT 0.0, -- Per capita income
    incomepercaperr Float64 DEFAULT 0.0, -- Per capita income margin of error
    poverty Float64 DEFAULT 0.0, -- Poverty rate percentage
    childpoverty Float64 DEFAULT 0.0, -- Child poverty rate percentage
    professional Float64 DEFAULT 0.0, -- Professional occupation percentage
    service Float64 DEFAULT 0.0, -- Service occupation percentage
    office Float64 DEFAULT 0.0, -- Office/administrative occupation percentage
    construction Float64 DEFAULT 0.0, -- Construction occupation percentage
    production Float64 DEFAULT 0.0, -- Production occupation percentage
    drive Float64 DEFAULT 0.0, -- Drive to work percentage
    carpool Float64 DEFAULT 0.0, -- Carpool to work percentage
    transit Float64 DEFAULT 0.0, -- Public transit percentage
    walk Float64 DEFAULT 0.0, -- Walk to work percentage
    othertransport Float64 DEFAULT 0.0, -- Other transportation percentage
    workathome Float64 DEFAULT 0.0, -- Work at home percentage
    meancommute Float64 DEFAULT 0.0, -- Mean commute time in minutes
    employed Int32 DEFAULT 0, -- Employed persons count
    privatework Float64 DEFAULT 0.0, -- Private sector percentage
    publicwork Float64 DEFAULT 0.0, -- Public sector percentage
    selfemployed Float64 DEFAULT 0.0, -- Self-employed percentage
    familywork Float64 DEFAULT 0.0, -- Family business percentage
    unemployment Float64 DEFAULT 0.0, -- Unemployment rate percentage
    lat Float64 DEFAULT 0.0, -- Latitude coordinate
    lon Float64 DEFAULT 0.0, -- Longitude coordinate
    loc JSON DEFAULT '{}', -- Structured location data (replaced frozen<geo_pol>)
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (country, zip);

-- Distributed table for zips
CREATE TABLE IF NOT EXISTS zips ON CLUSTER tracker_cluster
AS zips_local
ENGINE = Distributed(tracker_cluster, sfpla, zips_local, rand());

-- Jurisdictions table - Regulatory compliance requirements by geographic region
CREATE TABLE jurisdictions_local ON CLUSTER tracker_cluster (

    regulation String DEFAULT '', -- Regulation type (e.g., GDPR, CCPA)
    compliance String DEFAULT '', -- Compliance requirement (e.g., cookie-time)
    seq Int32 DEFAULT 0, -- Sequence number for ordering multiple rules
    rules JSON DEFAULT '{}', -- Rules configuration (e.g., essential,session:comfort,30:analytics,730)
    locs JSON DEFAULT '{}', -- Geographic locations where this regulation applies
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY regulation
ORDER BY (regulation, compliance, seq);

-- Distributed table for jurisdictions
CREATE TABLE IF NOT EXISTS jurisdictions ON CLUSTER tracker_cluster
AS jurisdictions_local
ENGINE = Distributed(tracker_cluster, sfpla, jurisdictions_local, rand());

-- Create a materialized view for locs - Enables efficient querying by location
CREATE MATERIALIZED VIEW jurisdictions_by_locs
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (country, region, city)
POPULATE AS
SELECT *, 
       JSONExtractString(toString(locs), 'country') AS country, 
       JSONExtractString(toString(locs), 'region') AS region, 
       JSONExtractString(toString(locs), 'city') AS city 
FROM jurisdictions;

-- Agreements table - Stores user consent and compliance agreements
CREATE TABLE agreements_local ON CLUSTER tracker_cluster (

    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - unique identifier for the visitor who made the agreement
    created_at DateTime64(3) DEFAULT now64(3), -- Agreement creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Agreement update timestamp
    compliances JSON DEFAULT '{}', -- Compliance settings agreed to (e.g., cookies, marketing)
    cflags Int64 DEFAULT 0, -- Compliance flags as bitfield for efficient querying
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID when agreement was made
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID if visitor was authenticated
    avid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Anonymized visitor ID for privacy
    hhash String DEFAULT '', -- Host hash where agreement was made
    app String DEFAULT '', -- Application name
    rel String DEFAULT '', -- Application release/version
    url String DEFAULT '', -- URL where agreement was made
    ip String DEFAULT '', -- IP address at time of agreement
    iphash String DEFAULT '', -- Hashed IP for privacy
    gaid String DEFAULT '', -- Google Advertising ID
    idfa String DEFAULT '', -- Apple Advertising ID
    msid String DEFAULT '', -- Microsoft Advertising ID
    fbid String DEFAULT '', -- Facebook Advertising ID
    country String DEFAULT '', -- Country code
    region String DEFAULT '', -- Region/state
    culture String DEFAULT '', -- Culture/language code
    source String DEFAULT '', -- Traffic source
    medium String DEFAULT '', -- Traffic medium
    campaign String DEFAULT '', -- Campaign name
    term String DEFAULT '', -- Search term
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer visitor ID
    rcode String DEFAULT '', -- Referral code
    aff String DEFAULT '', -- Affiliate ID
    browser String DEFAULT '', -- Browser name/version
    bhash String DEFAULT '', -- Browser hash
    device String DEFAULT '', -- Device type
    os String DEFAULT '', -- Operating system
    tz String DEFAULT '', -- Timezone
    vp_w Int64 DEFAULT 0, -- Viewport width
    vp_h Int64 DEFAULT 0, -- Viewport height
    loc JSON DEFAULT '{}', -- Location data
    lat Float64 DEFAULT 0.0, -- Latitude
    lon Float64 DEFAULT 0.0, -- Longitude
    zip String DEFAULT '', -- ZIP/postal code
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (vid, created_at, updated_at);

-- Distributed table for agreements
CREATE TABLE IF NOT EXISTS agreements ON CLUSTER tracker_cluster
AS agreements_local
ENGINE = Distributed(tracker_cluster, sfpla, agreements_local, rand());

-- Agreed table (history of agreements) - Tracks all historical agreements for compliance auditing
CREATE TABLE agreed_local ON CLUSTER tracker_cluster (

    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - unique identifier for the visitor who made the agreement
    created_at DateTime64(3) DEFAULT now64(3), -- Agreement creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Agreement update timestamp
    compliances JSON DEFAULT '{}', -- Compliance settings agreed to (e.g., cookies, marketing)
    cflags Int64 DEFAULT 0, -- Compliance flags as bitfield for efficient querying
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID when agreement was made
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID if visitor was authenticated
    avid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Anonymized visitor ID for privacy
    hhash String DEFAULT '', -- Host hash where agreement was made
    app String DEFAULT '', -- Application name
    rel String DEFAULT '', -- Application release/version
    url String DEFAULT '', -- URL where agreement was made
    ip String DEFAULT '', -- IP address at time of agreement
    iphash String DEFAULT '', -- Hashed IP for privacy
    gaid String DEFAULT '', -- Google Advertising ID
    idfa String DEFAULT '', -- Apple Advertising ID
    msid String DEFAULT '', -- Microsoft Advertising ID
    fbid String DEFAULT '', -- Facebook Advertising ID
    country String DEFAULT '', -- Country code
    region String DEFAULT '', -- Region/state
    culture String DEFAULT '', -- Culture/language code
    source String DEFAULT '', -- Traffic source
    medium String DEFAULT '', -- Traffic medium
    campaign String DEFAULT '', -- Campaign name
    term String DEFAULT '', -- Search term
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer visitor ID
    rcode String DEFAULT '', -- Referral code
    aff String DEFAULT '', -- Affiliate ID
    browser String DEFAULT '', -- Browser name/version
    bhash String DEFAULT '', -- Browser hash
    device String DEFAULT '', -- Device type
    os String DEFAULT '', -- Operating system
    tz String DEFAULT '', -- Timezone
    vp_w Int64 DEFAULT 0, -- Viewport width
    vp_h Int64 DEFAULT 0, -- Viewport height
    loc JSON DEFAULT '{}', -- Location data
    lat Float64 DEFAULT 0.0, -- Latitude
    lon Float64 DEFAULT 0.0, -- Longitude
    zip String DEFAULT '', -- ZIP/postal code
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (vid, created_at, updated_at);

-- Distributed table for agreed
CREATE TABLE IF NOT EXISTS agreed ON CLUSTER tracker_cluster
AS agreed_local
ENGINE = Distributed(tracker_cluster, sfpla, agreed_local, rand());

-- LTV table (total lifetime value, by user) - Tracks total value/revenue generated by each user
CREATE TABLE queues_local ON CLUSTER tracker_cluster (

    svc String DEFAULT '', -- Service origin - which service created this task
    qid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Queue ID - unique identifier for this task
    qtype String DEFAULT '', -- Queue type - categorization of task (e.g., "email", "sms", "notification")
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Source ID - identifier of the source entity (e.g., message ID)
    skey String DEFAULT '', -- Source key - alternative identifier if not a UUID (e.g., cohort name)
    ip String DEFAULT '', -- IP address - of the requestor who initiated this task
    host String DEFAULT '', -- Host executing - server executing this service
    schedule DateTime64(3) DEFAULT toDateTime64(0, 3), -- Schedule time - when this task is scheduled to run
    started DateTime64(3) DEFAULT toDateTime64(0, 3), -- Start time - when execution of this task began
    completed DateTime64(3) DEFAULT toDateTime64(0, 3), -- Completion time - when execution of this task finished
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp - when this task was created
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this task belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this task
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this task

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY svc
ORDER BY qid
TTL toDateTime(created_at) + INTERVAL 14 DAY;

-- Distributed table for queues
CREATE TABLE IF NOT EXISTS queues ON CLUSTER tracker_cluster
AS queues_local
ENGINE = Distributed(tracker_cluster, sfpla, queues_local, rand());

-- Create indices for queues
CREATE MATERIALIZED VIEW queues_by_qtype
ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY qtype
POPULATE AS
SELECT * FROM queues;

CREATE MATERIALIZED VIEW queues_by_completed
ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY completed
POPULATE AS
SELECT * FROM queues
WHERE completed IS NOT NULL;

CREATE MATERIALIZED VIEW queues_by_started
ENGINE = ReplicatedReplacingMergeTree
ORDER BY started
POPULATE AS
SELECT * FROM queues
WHERE started IS NOT NULL;

-- Enhanced Cohorts table - Manages groups of users for targeting and experimentation
CREATE TABLE cohorts_local ON CLUSTER tracker_cluster (

    cohort String DEFAULT '', -- Cohort name - unique identifier for this user group
    uids_url String DEFAULT '', -- UIDs URL - location of the cohort member list
    imported Int32 DEFAULT 0, -- Imported count - number of successfully imported users
    started DateTime64(3) DEFAULT toDateTime64(0, 3), -- Start time - when import process began
    completed DateTime64(3) DEFAULT toDateTime64(0, 3), -- Completion time - when import process finished
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this cohort belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this cohort
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this cohort

    -- Visitor tracking and user attributes
    uids Array(UUID) DEFAULT [], -- user ids
    vids Array(UUID) DEFAULT [], -- visitor ids (in case uid is not known yet), this can come directly from events table
    user_attributes JSON DEFAULT '{}', -- User attributes for matching/clustering (JSON format)
    user_attributes_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of user attributes (384-dim)

    -- Targeting and segmentation
    target_props Array(String) DEFAULT [], -- Target properties for splits (e.g., ["age_25_34", "interest_tech"])
    target_props_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of target properties (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) DEFAULT toDateTime64(0, 3) -- When embeddings were generated

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY cohort;

-- Distributed table for cohorts
CREATE TABLE IF NOT EXISTS cohorts ON CLUSTER tracker_cluster
AS cohorts_local
ENGINE = Distributed(tracker_cluster, sfpla, cohorts_local, rand());

-- Splits table - Defines experiment splits for A/B testing across cohorts
CREATE TABLE splits_local ON CLUSTER tracker_cluster (

    cohort String DEFAULT '', -- Cohort name - which user group this split applies to (* = all users)
    sfam String DEFAULT '', -- Split family - grouping element for related splits (e.g., "males2020")
    split String DEFAULT '', -- Split identifier - unique name for this variant (e.g., "A", "B")
    seq Int32 DEFAULT 0, -- Sequence number - ordering for this split variant
    uids Array(UUID) DEFAULT [], -- Users list - explicit set of users in this split
    vids Array(UUID) DEFAULT [], -- Visitors list - explicit set of visitors in this split (if uid is not known yet), this can come directly from events table
    pct Float64 DEFAULT 0.0, -- Percentage - traffic allocation for this variant (mutually exclusive with users)
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this split belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this split
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this split

    -- Visitor tracking and targeting
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - for anonymous visitor tracking in this split
    target_props Array(String) DEFAULT [], -- Target properties for this split variant
    target_props_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of target properties (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) DEFAULT toDateTime64(0, 3) -- When embeddings were generated

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY split;

-- Distributed table for splits
CREATE TABLE IF NOT EXISTS splits ON CLUSTER tracker_cluster
AS splits_local
ENGINE = Distributed(tracker_cluster, sfpla, splits_local, rand());

-- MCert type as structured data - Stores marketing certification and encryption keys
CREATE TABLE mcerts_local ON CLUSTER tracker_cluster (

    id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Certificate ID - unique identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    name String DEFAULT '', -- Certificate name - human-readable identifier
    algo String DEFAULT '', -- Algorithm - cryptographic algorithm type (e.g., "ecc")
    spec String DEFAULT '', -- Specification - algorithm specification (e.g., "192spec2k1")
    sz String DEFAULT '', -- Serialization type - format for serialization (e.g., "hex", "num", "json")
    opub String DEFAULT '', -- Owner public key - asymmetric owner public key (DER format)
    opriv String DEFAULT '', -- Owner private key - asymmetric owner private key (optional)
    tpub String DEFAULT '', -- Thread public key - asymmetric thread public key
    tpriv String DEFAULT '', -- Thread private key - asymmetric thread private key (optional)
    sym String DEFAULT '', -- Symmetric encryption method - (e.g., "aes", "rsa", "elgamal")
    sver String DEFAULT '', -- Specification version
    sspec String DEFAULT '', -- Security specification (e.g., "dhpubprivsha256")
    skey String DEFAULT '', -- Symmetric/shared key (optional)
    dhpub String DEFAULT '', -- Diffie-Hellman public key
    dhpriv String DEFAULT '', -- Diffie-Hellman private key (optional)
    updated_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    expires DateTime64(3) DEFAULT toDateTime64(0, 3), -- Expiration timestamp
    owner_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner ID - who owns this certificate
    object_type String DEFAULT '' -- Object type - what type of object this certificate belongs to

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, id);

-- Distributed table for mcerts
CREATE TABLE IF NOT EXISTS mcerts ON CLUSTER tracker_cluster
AS mcerts_local
ENGINE = Distributed(tracker_cluster, sfpla, mcerts_local, rand());

-- Security audit table - Tracks security-related approvals and permissions
CREATE TABLE compliance_settings_local ON CLUSTER tracker_cluster (

    region String DEFAULT '', -- Geographic region code (e.g., "EU", "US", "APAC")
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    setting_key String DEFAULT '', -- Configuration key for compliance setting
    setting_value String DEFAULT '', -- Configuration value
    description String DEFAULT '', -- Detailed description of the compliance requirement
    applies_to Array(String) DEFAULT [], -- Components/systems this setting applies to
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (region, setting_key);

-- Distributed table for compliance_settings
CREATE TABLE IF NOT EXISTS compliance_settings ON CLUSTER tracker_cluster
AS compliance_settings_local
ENGINE = Distributed(tracker_cluster, sfpla, compliance_settings_local, rand());

-- Competitor Analysis - Stores information about market competitors
CREATE TABLE payment_settings_local ON CLUSTER tracker_cluster (

    setting_id String DEFAULT '', -- Unique identifier for the setting (e.g., "creator_base_rate", "min_payout")
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    effective_date DateTime64(3) DEFAULT toDateTime64(0, 3), -- Date when setting becomes effective
    value Float64 DEFAULT 0.0, -- Numeric value for the setting
    description String DEFAULT '', -- Detailed description of the setting
    end_date DateTime64(3) DEFAULT toDateTime64(0, 3), -- Date when setting expires (NULL if current)
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID who last updated this setting

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, setting_id, effective_date);

-- Distributed table for payment_settings
CREATE TABLE IF NOT EXISTS payment_settings ON CLUSTER tracker_cluster
AS payment_settings_local
ENGINE = Distributed(tracker_cluster, sfpla, payment_settings_local, rand());

-- Contextual Bandit Features - Defines features used in contextual bandit algorithms
CREATE TABLE budget_limits_local ON CLUSTER tracker_cluster (

    limit_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Unique limit identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID) DEFAULT NULL, -- Campaign ID (NULL for oid-wide limits)
    limit_type LowCardinality(String) DEFAULT '', -- Type of limit: daily, weekly, monthly, campaign_total
    amount Float64 DEFAULT 0.0, -- Budget limit amount
    currency String DEFAULT 'USD', -- Currency code (e.g., EUR, USD)
    warning_threshold Float64 DEFAULT 0.8, -- Warning threshold (0-1, e.g., 0.8 = 80%)
    critical_threshold Float64 DEFAULT 0.95, -- Critical threshold (0-1, e.g., 0.95 = 95%)
    auto_pause_enabled Boolean DEFAULT true, -- Enable automatic campaign pausing
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    expires_at Nullable(DateTime64(3)) DEFAULT NULL, -- Limit expiration date (NULL for permanent)
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater Nullable(UUID) DEFAULT NULL -- User who last updated this limit

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, limit_id, limit_type, created_at);

-- Distributed table for budget_limits
CREATE TABLE IF NOT EXISTS budget_limits ON CLUSTER tracker_cluster
AS budget_limits_local
ENGINE = Distributed(tracker_cluster, sfpla, budget_limits_local, rand());

-- Budget Alerts table - Stores budget alerts and notifications
CREATE TABLE budget_alerts_local ON CLUSTER tracker_cluster (

    alert_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Unique alert identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID) DEFAULT NULL, -- Campaign ID (NULL for oid-wide alerts)
    limit_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Reference to budget_limits.limit_id
    alert_level LowCardinality(String) DEFAULT '', -- Alert level: info, warning, critical, emergency
    message String DEFAULT '', -- Alert message text
    current_spend Float64 DEFAULT 0.0, -- Current spend amount when alert was triggered
    budget_limit Float64 DEFAULT 0.0, -- Budget limit amount
    percentage_used Float64 DEFAULT 0.0, -- Percentage of budget used (0-100)
    actions_taken Array(String) DEFAULT [], -- Actions taken (e.g., "campaign_paused", "notification_sent")
    resolved Boolean DEFAULT false, -- Whether the alert has been resolved
    resolved_at Nullable(DateTime64(3)) DEFAULT NULL, -- When the alert was resolved
    created_at DateTime64(3) DEFAULT now64(3), -- Alert creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, alert_id, alert_level, created_at);

-- Distributed table for budget_alerts
CREATE TABLE IF NOT EXISTS budget_alerts ON CLUSTER tracker_cluster
AS budget_alerts_local
ENGINE = Distributed(tracker_cluster, sfpla, budget_alerts_local, rand());

-- Budget Emergency Actions table - Tracks emergency actions taken by Budget Guardian
CREATE TABLE budget_emergency_actions_local ON CLUSTER tracker_cluster (

    action_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Unique action identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID) DEFAULT NULL, -- Campaign ID (NULL for oid-wide actions)
    action_type LowCardinality(String) DEFAULT '', -- Action type: campaign_paused, spending_restricted, alert_sent
    reason String DEFAULT '', -- Reason for the emergency action
    triggered_by String DEFAULT '', -- What triggered the action: budget_exceeded, velocity_threshold, manual
    limit_id Nullable(UUID) DEFAULT NULL, -- Reference to budget_limits.limit_id if applicable
    alert_id Nullable(UUID) DEFAULT NULL, -- Reference to budget_alerts.alert_id if applicable
    metadata JSON DEFAULT '{}', -- Additional action metadata in JSON format
    success Boolean DEFAULT false, -- Whether the action was successful
    error_message Nullable(String) DEFAULT NULL, -- Error message if action failed
    executed_by Nullable(UUID) DEFAULT NULL, -- User who executed the action (NULL for automatic)
    updated_at DateTime64(3) DEFAULT now64(3) -- Action execution timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(updated_at))
ORDER BY (oid, org, action_id, action_type, updated_at);

-- Distributed table for budget_emergency_actions
CREATE TABLE IF NOT EXISTS budget_emergency_actions ON CLUSTER tracker_cluster
AS budget_emergency_actions_local
ENGINE = Distributed(tracker_cluster, sfpla, budget_emergency_actions_local, rand());

-- Spend Velocity Analytics table - Stores spend velocity calculations for trend analysis
CREATE TABLE spend_velocity_analytics_local ON CLUSTER tracker_cluster (

    analysis_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Unique analysis identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID) DEFAULT NULL, -- Campaign ID (NULL for oid-wide analysis)
    analysis_time DateTime64(3) DEFAULT now64(3), -- When the analysis was performed
    window_hours Int32 DEFAULT 0, -- Analysis window in hours (e.g., 24, 168)
    current_spend Float64 DEFAULT 0.0, -- Current spend amount
    hourly_burn_rate Float64 DEFAULT 0.0, -- Hourly burn rate
    daily_burn_rate Float64 DEFAULT 0.0, -- Daily burn rate
    projected_daily_spend Float64 DEFAULT 0.0, -- Projected daily spend
    projected_weekly_spend Float64 DEFAULT 0.0, -- Projected weekly spend
    projected_monthly_spend Float64 DEFAULT 0.0, -- Projected monthly spend
    confidence_score Float64 DEFAULT 0.0, -- Confidence score (0-1)
    trend LowCardinality(String) DEFAULT '', -- Trend: increasing, stable, decreasing
    data_points Int32 DEFAULT 0, -- Number of data points used in analysis
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(updated_at))
ORDER BY (oid, org, analysis_id, analysis_time);

-- Distributed table for spend_velocity_analytics
CREATE TABLE IF NOT EXISTS spend_velocity_analytics ON CLUSTER tracker_cluster
AS spend_velocity_analytics_local
ENGINE = Distributed(tracker_cluster, sfpla, spend_velocity_analytics_local, rand());

-- Landing Pages Management with Multi-Tenant Support and Semantic Search
