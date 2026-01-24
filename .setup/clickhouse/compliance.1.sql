-- Compliance + Queues - Jurisdictions, agreements, queues, cohorts, budget management
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

CREATE TABLE zips ON CLUSTER my_cluster (
    country String, -- Country code (e.g., US)
    zip String, -- ZIP/postal code
    region String, -- Region/state name (e.g., California)
    rcode String, -- Region code (e.g., CA)
    county String, -- County name
    city String, -- City name
    culture String, -- Culture/language code (e.g., en-US)
    population Int32, -- Total population count
    men Int32, -- Male population count
    women Int32, -- Female population count
    hispanic Float64, -- Hispanic population percentage
    white Float64, -- White population percentage
    black Float64, -- Black population percentage
    native Float64, -- Native American population percentage
    asian Float64, -- Asian population percentage
    pacific Float64, -- Pacific Islander population percentage
    voters Int32, -- Registered voters count
    income Float64, -- Average income
    incomeerr Float64, -- Income margin of error
    incomepercap Float64, -- Per capita income
    incomepercaperr Float64, -- Per capita income margin of error
    poverty Float64, -- Poverty rate percentage
    childpoverty Float64, -- Child poverty rate percentage
    professional Float64, -- Professional occupation percentage
    service Float64, -- Service occupation percentage
    office Float64, -- Office/administrative occupation percentage
    construction Float64, -- Construction occupation percentage
    production Float64, -- Production occupation percentage
    drive Float64, -- Drive to work percentage
    carpool Float64, -- Carpool to work percentage
    transit Float64, -- Public transit percentage
    walk Float64, -- Walk to work percentage
    othertransport Float64, -- Other transportation percentage
    workathome Float64, -- Work at home percentage
    meancommute Float64, -- Mean commute time in minutes
    employed Int32, -- Employed persons count
    privatework Float64, -- Private sector percentage
    publicwork Float64, -- Public sector percentage
    selfemployed Float64, -- Self-employed percentage
    familywork Float64, -- Family business percentage
    unemployment Float64, -- Unemployment rate percentage
    lat Float64, -- Latitude coordinate
    lon Float64, -- Longitude coordinate
    loc JSON, -- Structured location data (replaced frozen<geo_pol>)
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (country, zip);

-- Jurisdictions table - Regulatory compliance requirements by geographic region
CREATE TABLE jurisdictions ON CLUSTER my_cluster (
    regulation String, -- Regulation type (e.g., GDPR, CCPA)
    compliance String, -- Compliance requirement (e.g., cookie-time)
    seq Int32, -- Sequence number for ordering multiple rules
    rules JSON, -- Rules configuration (e.g., essential,session:comfort,30:analytics,730)
    locs JSON, -- Geographic locations where this regulation applies
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY regulation
ORDER BY (regulation, compliance, seq);

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
CREATE TABLE agreements ON CLUSTER my_cluster (
    vid UUID, -- Visitor ID - unique identifier for the visitor who made the agreement
    created_at DateTime64(3) DEFAULT now64(3), -- Agreement creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Agreement update timestamp
    compliances JSON DEFAULT '{}', -- Compliance settings agreed to (e.g., cookies, marketing)
    cflags Int64, -- Compliance flags as bitfield for efficient querying
    sid UUID, -- Session ID when agreement was made
    uid UUID, -- User ID if visitor was authenticated
    avid UUID, -- Anonymized visitor ID for privacy
    hhash String, -- Host hash where agreement was made
    app String, -- Application name
    rel String, -- Application release/version
    url String, -- URL where agreement was made
    ip String, -- IP address at time of agreement
    iphash String, -- Hashed IP for privacy
    gaid String, -- Google Advertising ID
    idfa String, -- Apple Advertising ID
    msid String, -- Microsoft Advertising ID
    fbid String, -- Facebook Advertising ID
    country String, -- Country code
    region String, -- Region/state
    culture String, -- Culture/language code
    source String, -- Traffic source
    medium String, -- Traffic medium
    campaign String, -- Campaign name
    term String, -- Search term
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer visitor ID
    rcode String, -- Referral code
    aff String, -- Affiliate ID
    browser String, -- Browser name/version
    bhash String, -- Browser hash
    device String, -- Device type
    os String, -- Operating system
    tz String, -- Timezone
    vp_w Int64, -- Viewport width
    vp_h Int64, -- Viewport height
    loc JSON DEFAULT '{}', -- Location data
    lat Float64, -- Latitude
    lon Float64, -- Longitude
    zip String, -- ZIP/postal code
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID
    oid UUID, -- Organization ID
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (vid, created_at, updated_at);

-- Agreed table (history of agreements) - Tracks all historical agreements for compliance auditing
CREATE TABLE agreed ON CLUSTER my_cluster (
    vid UUID, -- Visitor ID - unique identifier for the visitor who made the agreement
    created_at DateTime64(3) DEFAULT now64(3), -- Agreement creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Agreement update timestamp
    compliances JSON DEFAULT '{}', -- Compliance settings agreed to (e.g., cookies, marketing)
    cflags Int64, -- Compliance flags as bitfield for efficient querying
    sid UUID, -- Session ID when agreement was made
    uid UUID, -- User ID if visitor was authenticated
    avid UUID, -- Anonymized visitor ID for privacy
    hhash String, -- Host hash where agreement was made
    app String, -- Application name
    rel String, -- Application release/version
    url String, -- URL where agreement was made
    ip String, -- IP address at time of agreement
    iphash String, -- Hashed IP for privacy
    gaid String, -- Google Advertising ID
    idfa String, -- Apple Advertising ID
    msid String, -- Microsoft Advertising ID
    fbid String, -- Facebook Advertising ID
    country String, -- Country code
    region String, -- Region/state
    culture String, -- Culture/language code
    source String, -- Traffic source
    medium String, -- Traffic medium
    campaign String, -- Campaign name
    term String, -- Search term
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer visitor ID
    rcode String, -- Referral code
    aff String, -- Affiliate ID
    browser String, -- Browser name/version
    bhash String, -- Browser hash
    device String, -- Device type
    os String, -- Operating system
    tz String, -- Timezone
    vp_w Int64, -- Viewport width
    vp_h Int64, -- Viewport height
    loc JSON DEFAULT '{}', -- Location data
    lat Float64, -- Latitude
    lon Float64, -- Longitude
    zip String, -- ZIP/postal code
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID
    oid UUID, -- Organization ID
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (vid, created_at, updated_at);

-- LTV table (total lifetime value, by user) - Tracks total value/revenue generated by each user
CREATE TABLE queues ON CLUSTER my_cluster (
    svc String, -- Service origin - which service created this task
    qid UUID, -- Queue ID - unique identifier for this task
    qtype String, -- Queue type - categorization of task (e.g., "email", "sms", "notification")
    sid UUID, -- Source ID - identifier of the source entity (e.g., message ID)
    skey String, -- Source key - alternative identifier if not a UUID (e.g., cohort name)
    ip String, -- IP address - of the requestor who initiated this task
    host String, -- Host executing - server executing this service
    schedule DateTime64(3), -- Schedule time - when this task is scheduled to run
    started DateTime64(3), -- Start time - when execution of this task began
    completed DateTime64(3), -- Completion time - when execution of this task finished
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp - when this task was created
    oid UUID, -- Organization ID - which oid this task belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this task
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this task
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY svc
ORDER BY qid
TTL toDateTime(created_at) + INTERVAL 14 DAY;

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
CREATE TABLE cohorts ON CLUSTER my_cluster (
    cohort String, -- Cohort name - unique identifier for this user group
    uids_url String, -- UIDs URL - location of the cohort member list
    imported Int32, -- Imported count - number of successfully imported users
    started DateTime64(3), -- Start time - when import process began
    completed DateTime64(3), -- Completion time - when import process finished
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this cohort belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this cohort
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this cohort

    -- Visitor tracking and user attributes
    uids Array(UUID), -- user ids
    vids Array(UUID), -- visitor ids (in case uid is not known yet), this can come directly from events table
    user_attributes JSON, -- User attributes for matching/clustering (JSON format)
    user_attributes_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of user attributes (384-dim)

    -- Targeting and segmentation
    target_props Array(String), -- Target properties for splits (e.g., ["age_25_34", "interest_tech"])
    target_props_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of target properties (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) -- When embeddings were generated
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY cohort;

-- Splits table - Defines experiment splits for A/B testing across cohorts
CREATE TABLE splits ON CLUSTER my_cluster (
    cohort String, -- Cohort name - which user group this split applies to (* = all users)
    sfam String, -- Split family - grouping element for related splits (e.g., "males2020")
    split String, -- Split identifier - unique name for this variant (e.g., "A", "B")
    seq Int32, -- Sequence number - ordering for this split variant
    uids Array(UUID), -- Users list - explicit set of users in this split
    vids Array(UUID), -- Visitors list - explicit set of visitors in this split (if uid is not known yet), this can come directly from events table
    pct Float64, -- Percentage - traffic allocation for this variant (mutually exclusive with users)
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this split belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this split
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this split

    -- Visitor tracking and targeting
    vid UUID, -- Visitor ID - for anonymous visitor tracking in this split
    target_props Array(String), -- Target properties for this split variant
    target_props_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of target properties (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) -- When embeddings were generated
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY split;

-- MCert type as structured data - Stores marketing certification and encryption keys
CREATE TABLE mcerts ON CLUSTER my_cluster (
    id UUID, -- Certificate ID - unique identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    name String, -- Certificate name - human-readable identifier
    algo String, -- Algorithm - cryptographic algorithm type (e.g., "ecc")
    spec String, -- Specification - algorithm specification (e.g., "192spec2k1")
    sz String, -- Serialization type - format for serialization (e.g., "hex", "num", "json")
    opub String, -- Owner public key - asymmetric owner public key (DER format)
    opriv String, -- Owner private key - asymmetric owner private key (optional)
    tpub String, -- Thread public key - asymmetric thread public key
    tpriv String, -- Thread private key - asymmetric thread private key (optional)
    sym String, -- Symmetric encryption method - (e.g., "aes", "rsa", "elgamal")
    sver String, -- Specification version
    sspec String, -- Security specification (e.g., "dhpubprivsha256")
    skey String, -- Symmetric/shared key (optional)
    dhpub String, -- Diffie-Hellman public key
    dhpriv String, -- Diffie-Hellman private key (optional)
    updated_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    expires DateTime64(3), -- Expiration timestamp
    owner_id UUID, -- Owner ID - who owns this certificate
    object_type String -- Object type - what type of object this certificate belongs to
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, id);

-- Security audit table - Tracks security-related approvals and permissions
CREATE TABLE compliance_settings ON CLUSTER my_cluster (
    region String, -- Geographic region code (e.g., "EU", "US", "APAC")
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    setting_key String, -- Configuration key for compliance setting
    setting_value String, -- Configuration value
    description String, -- Detailed description of the compliance requirement
    applies_to Array(String), -- Components/systems this setting applies to
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (region, setting_key);

-- Competitor Analysis - Stores information about market competitors
CREATE TABLE payment_settings ON CLUSTER my_cluster (
    setting_id String, -- Unique identifier for the setting (e.g., "creator_base_rate", "min_payout")
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    effective_date DateTime64(3), -- Date when setting becomes effective
    value Float64, -- Numeric value for the setting
    description String, -- Detailed description of the setting
    end_date DateTime64(3), -- Date when setting expires (NULL if current)
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- User ID who last updated this setting
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, setting_id, effective_date);

-- Contextual Bandit Features - Defines features used in contextual bandit algorithms
CREATE TABLE IF NOT EXISTS budget_limits ON CLUSTER my_cluster (
    limit_id UUID, -- Unique limit identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID), -- Campaign ID (NULL for oid-wide limits)
    limit_type LowCardinality(String), -- Type of limit: daily, weekly, monthly, campaign_total
    amount Float64, -- Budget limit amount
    currency String DEFAULT 'USD', -- Currency code (e.g., EUR, USD)
    warning_threshold Float64 DEFAULT 0.8, -- Warning threshold (0-1, e.g., 0.8 = 80%)
    critical_threshold Float64 DEFAULT 0.95, -- Critical threshold (0-1, e.g., 0.95 = 95%)
    auto_pause_enabled Boolean DEFAULT true, -- Enable automatic campaign pausing
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    expires_at Nullable(DateTime64(3)), -- Limit expiration date (NULL for permanent)
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater Nullable(UUID) -- User who last updated this limit
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, limit_id, limit_type, created_at);

-- Budget Alerts table - Stores budget alerts and notifications
CREATE TABLE IF NOT EXISTS budget_alerts ON CLUSTER my_cluster (
    alert_id UUID, -- Unique alert identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation  
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID), -- Campaign ID (NULL for oid-wide alerts)
    limit_id UUID, -- Reference to budget_limits.limit_id
    alert_level LowCardinality(String), -- Alert level: info, warning, critical, emergency
    message String, -- Alert message text
    current_spend Float64, -- Current spend amount when alert was triggered
    budget_limit Float64, -- Budget limit amount
    percentage_used Float64, -- Percentage of budget used (0-100)
    actions_taken Array(String), -- Actions taken (e.g., "campaign_paused", "notification_sent")
    resolved Boolean DEFAULT false, -- Whether the alert has been resolved
    resolved_at Nullable(DateTime64(3)), -- When the alert was resolved
    created_at DateTime64(3) DEFAULT now64(3), -- Alert creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, alert_id, alert_level, created_at);

-- Budget Emergency Actions table - Tracks emergency actions taken by Budget Guardian
CREATE TABLE IF NOT EXISTS budget_emergency_actions ON CLUSTER my_cluster (
    action_id UUID, -- Unique action identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID), -- Campaign ID (NULL for oid-wide actions)
    action_type LowCardinality(String), -- Action type: campaign_paused, spending_restricted, alert_sent
    reason String, -- Reason for the emergency action
    triggered_by String, -- What triggered the action: budget_exceeded, velocity_threshold, manual
    limit_id Nullable(UUID), -- Reference to budget_limits.limit_id if applicable
    alert_id Nullable(UUID), -- Reference to budget_alerts.alert_id if applicable
    metadata JSON, -- Additional action metadata in JSON format
    success Boolean, -- Whether the action was successful
    error_message Nullable(String), -- Error message if action failed
    executed_by Nullable(UUID), -- User who executed the action (NULL for automatic)
    updated_at DateTime64(3) DEFAULT now64(3) -- Action execution timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(updated_at))
ORDER BY (oid, org, action_id, action_type, updated_at);

-- Spend Velocity Analytics table - Stores spend velocity calculations for trend analysis
CREATE TABLE IF NOT EXISTS spend_velocity_analytics ON CLUSTER my_cluster (
    analysis_id UUID, -- Unique analysis identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    campaign_id Nullable(UUID), -- Campaign ID (NULL for oid-wide analysis)
    analysis_time DateTime64(3), -- When the analysis was performed
    window_hours Int32, -- Analysis window in hours (e.g., 24, 168)
    current_spend Float64, -- Current spend amount
    hourly_burn_rate Float64, -- Hourly burn rate
    daily_burn_rate Float64, -- Daily burn rate
    projected_daily_spend Float64, -- Projected daily spend
    projected_weekly_spend Float64, -- Projected weekly spend
    projected_monthly_spend Float64, -- Projected monthly spend
    confidence_score Float64, -- Confidence score (0-1)
    trend LowCardinality(String), -- Trend: increasing, stable, decreasing
    data_points Int32, -- Number of data points used in analysis
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(updated_at))
ORDER BY (oid, org, analysis_id, analysis_time);

-- Landing Pages Management with Multi-Tenant Support and Semantic Search
