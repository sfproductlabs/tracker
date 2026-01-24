-- Messages + Redirects - Message threads, storage, triage, redirects, impressions
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

CREATE TABLE affiliates ON CLUSTER my_cluster (
    hhash String, -- Host hash - identifies the website/application
    aff String, -- Affiliate ID or code
    vid UUID, -- Visitor ID - the visitor who came through the affiliate
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    version_ts Int64 -- Version timestamp (negative created_at for keeping oldest record)
) ENGINE = ReplicatedReplacingMergeTree(version_ts)
PARTITION BY hhash
ORDER BY (hhash, vid);

-- Create a materialized view for time-based queries - Enables efficient queries by creation time
CREATE MATERIALIZED VIEW affiliates_by_time 
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY created_at
POPULATE AS
SELECT * FROM affiliates;

-- Create a materialized view for affiliate ID lookups - Enables efficient queries by affiliate code
CREATE MATERIALIZED VIEW affiliates_by_aff 
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY hhash
ORDER BY aff
POPULATE AS
SELECT * FROM affiliates;

-- Redirects table - Stores URL redirection mappings
CREATE TABLE redirects ON CLUSTER my_cluster (
    hhash String, -- Host hash - identifies the website/application
    urlfrom String, -- Source URL without protocol (e.g., 'example.com/old-path')
    urlto String, -- Destination URL with protocol (e.g., 'https://example.com/new-path')
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization identifier for multi-tenant support
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updated_at DateTime64(3) DEFAULT now64(3), -- When this redirect was last updated
    updater UUID, -- User ID of the person who created/updated this redirect
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY urlfrom;

-- Redirect history table - Tracks the history of redirects for auditing
CREATE TABLE redirect_history ON CLUSTER my_cluster (
    urlfrom String, -- Source URL without protocol
    hostfrom String, -- Source host/domain
    slugfrom String, -- Source path/slug
    urlto String, -- Destination URL with protocol
    hostto String, -- Destination host/domain
    pathto String, -- Destination path
    searchto String, -- Destination query parameters
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization identifier for multi-tenant support
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updater UUID, -- User ID of the person who created this redirect
    updated_at DateTime64(3) DEFAULT now64(3) -- Record updated timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY hostfrom
ORDER BY (urlfrom, updated_at);

-- Create a materialized view for hostto lookup - Enables querying redirects by destination host
CREATE MATERIALIZED VIEW redirect_history_by_hostto
ENGINE = ReplicatedReplacingMergeTree
ORDER BY hostto
POPULATE AS
SELECT * FROM redirect_history;

-- Accounts table - Stores user authentication and permission information
CREATE TABLE msec ON CLUSTER my_cluster (
    tid UUID, -- Thread ID - reference to message thread
    secid UUID, -- Security record ID - unique identifier for this security record
    perm_id UUID, -- Permission ID - reference to associated permission
    pending Boolean, -- Pending flag - whether this security request is awaiting approval
    approver UUID, -- Approver user ID - who approved this security request
    approved DateTime64(3), -- Approval timestamp - when this request was approved
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this security record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created this security record
    updatedms Int64, -- Update milliseconds - participant updated timestamp in ms
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this security record
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, secid, created_at);

-- Materialized view for finding pending security requests - Enables efficient approval workflows
CREATE MATERIALIZED VIEW msec_by_pending
ENGINE = ReplicatedReplacingMergeTree
ORDER BY pending
POPULATE AS
SELECT * FROM msec;

-- Message threads table - Comprehensive messaging system for user communications and campaigns
CREATE TABLE mthreads ON CLUSTER my_cluster (
    tid UUID, -- Thread ID - unique identifier for this message thread
    alias String, -- Alias - identifier for the thread, for chat it will be the alphabetized list of participants like user1:user2 or with the chat title if it exists chat_title:user1:user2 or chat_id for web pages it will be the full url ex. https://www.google.com/page1/page2/page3, for ads it may be the ad_with_experiment_and_variant_name
    xid String, -- Experiment ID - for A/B testing
    xstatus String, -- Experiment status - running, paused, completed
    name String, -- Name - human-readable name for the thread
    ddata String, -- Default data - default content to send
    post String, -- Post/description - note or description of thread purpose
    mtempl JSON, -- Message template - formatting templates for messages
    mcert_id UUID, -- Marketing certification ID - reference to security certificate
    cats Array(String), -- Categories - topical categories for this thread
    mtypes Array(String), -- Message types - delivery methods (email, sms, push, etc.)
    fmtypes Array(String), -- Fallback message types - backup delivery methods
    cmtypes Array(String), -- Child message types - delivery methods for child messages
    urgency Int32, -- Urgency level - priority of messages in this thread
    sys Boolean, -- System message flag - whether this is a system-generated thread
    ephemeral Int32, -- Ephemeral timeout - seconds until expiry (null = keep forever)
    archived Boolean, -- Archived flag - whether this thread is archived
    admins Array(UUID), -- Administrators - users who can approve new members
    opens Boolean, -- Publicly subscribable - can users subscribe themselves
    openp Boolean, -- Publicly publishable - can users publish themselves
    perms_ids Array(UUID), -- Permission IDs - additional permissions for this thread
    app String, -- Application name - which app this thread is for
    rel String, -- Release version - which app version this is for
    ver Int32, -- Version - thread template version
    ptyp String, -- Page type - category of message
    etyp String, -- Event type - category of events
    ename String, -- Event name - specific event identifier
    auth String, -- Author - who created the content
    cohorts Array(String), -- Target cohorts - groups to send to
    splits JSON, -- Split configuration - for A/B testing variants
    source String, -- Source - referring domain/service
    medium String, -- Medium - referring channel & provider method - method of delivery (web page, ad, chat, sms, push, etc.)
    campaign String, -- Campaign name - for tracking
    term String, -- Search term - for SEO tracking
    promo String, -- Promo code - included in messages
    ref UUID, -- Referrer user ID - for referral tracking
    aff String, -- Affiliate code - for partner tracking
    provider String, -- Provider name - external service (Facebook, Mailchimp, etc.)    
    provider_campaign_id String, -- Provider campaign ID - ID in provider's system
    provider_account_id String, -- Provider account ID - account in provider's system
    provider_metrics JSON, -- Provider metrics - performance data from provider
    provider_cost Float64, -- Provider cost - cost reported by provider
    provider_status String, -- Provider status - status in provider's system
    broadcast Boolean, -- Broadcast flag - send to all subscribers
    derive Boolean, -- Derive subscribers - update based on cohorts/splits
    sent Array(UUID), -- Sent list - users message has been sent to
    outs Array(UUID), -- Outstanding list - users still to receive message
    subs Array(UUID), -- Subscribers - users who can receive messages
    pubs Array(UUID), -- Publishers - users who can send messages
    prefs JSON, -- User preferences - message delivery preferences by user
    ftrack Boolean, -- Failed delivery tracking - track failed deliveries
    strack Boolean, -- Successful delivery tracking - track successful deliveries
    interest JSON, -- Interest metrics - engagement statistics
    perf JSON, -- Performance metrics - key performance indicators
    planned_impressions Int64, -- Planned impression count - expected impressions
    actual_impressions Int64, -- Actual impression count - measured impressions
    impression_goal Int64, -- Impression goal - target number of impressions
    impression_budget Float64, -- Impression budget - allocated budget for impressions
    cost_per_impression Float64, -- Cost per impression - actual cost per impression
    vid_targets Array(UUID), -- Visitor ID targets - specific visitors to target
    funnel_stage String, -- Funnel stage - awareness, consideration, conversion, etc.
    total_conversions Int64, -- Total conversions - count of successful conversions
    conversion_value Float64, -- Conversion value - monetary value of conversions
    variants JSON, -- Variants - different versions for testing
    variant_weights JSON, -- Variant weights - traffic allocation by variant
    winner_variant String, -- Winner variant - best performing variant
    audience_params JSON, -- Audience parameters - targeting parameters
    audience_metrics JSON, -- Audience metrics - performance by audience segment
    audience_segments Array(String), -- Audience segments - predefined user segments
    content_keywords Array(String), -- Content keywords - for SEO and categorization
    content_assumptions JSON, -- Content assumptions - hypotheses about content
    content_metrics JSON, -- Content metrics - performance by content attributes
    content_intention String, -- Content intention - inform, persuade, entertain, etc.
    abz_enabled Boolean, -- A/B testing enabled - whether testing is active
    abz_algorithm String, -- A/B testing algorithm - which testing approach to use
    abz_reward_metric String, -- Reward metric - primary KPI for optimization
    abz_reward_value Float64, -- Reward value - cumulative reward value
    abz_exploration_rate Float64, -- Exploration rate - balance between explore/exploit
    abz_learning_rate Float64, -- Learning rate - speed of adaptation
    abz_start_time DateTime64(3), -- Test start time - when testing began
    abz_sample_size Int64, -- Sample size - current number of samples
    abz_min_sample_size Int64, -- Minimum sample size - threshold for optimization
    abz_confidence_level Float64, -- Confidence level - statistical confidence
    abz_winner_threshold Float64, -- Winner threshold - when to declare a winner
    abz_auto_optimize Boolean, -- Auto-optimize - whether to automatically optimize
    abz_auto_stop Boolean, -- Auto-stop - whether to automatically end test
    abz_status String, -- Status - running, paused, completed
    abz_params JSON, -- Algorithm parameters - specific configuration
    abz_infinite_armed Boolean, -- Infinite armed - whether using continuous parameters
    abz_param_space JSON, -- Parameter space - ranges for optimization
    abz_model_type String, -- Model type - GP, random forest, neural network, etc.
    abz_model_params JSON, -- Model parameters - hyperparameters for model
    abz_acquisition_function String, -- Acquisition function - EI, UCB, PI, etc.
    campaign_id UUID, -- Campaign ID - reference to campaign
    campaign_phase String, -- Campaign phase - teaser, launch, follow-up, etc.
    campaign_priority Int32, -- Campaign priority - importance within campaign
    campaign_budget_allocation Float64, -- Budget allocation - portion of campaign budget
    campaign_status String, -- Campaign status - active, paused, scheduled, etc.
    attribution_model String, -- Attribution model - how to attribute conversions
    attribution_weight Float64, -- Attribution weight - importance in multi-touch
    attribution_window Int32, -- Attribution window - time period for attribution
    attribution_params JSON, -- Attribution parameters - model-specific settings
    requires_consent Boolean, -- Requires consent - whether consent is required
    consent_types Array(String), -- Consent types - marketing, profiling, etc.
    regional_compliance JSON, -- Regional compliance - by jurisdiction
    frequency_caps JSON, -- Frequency caps - limits on message frequency
    data_retention Int32, -- Data retention - period in days
    content_version Int32, -- Content version - version number
    content_status String, -- Content status - draft, review, approved, etc.
    content_approver UUID, -- Content approver - who approved the content
    content_approval_date DateTime64(3), -- Content approval date
    content_history Array(UUID), -- Content history - previous versions
    content_creator UUID, -- Content creator - who created the content
    content_editors Array(UUID), -- Content editors - who edited the content
    creator_id UUID, -- Creator ID - reference to content creator
    content_id UUID, -- Content ID - reference to specific creator content
    creator_compensation Boolean, -- Creator compensation - whether creator is paid
    creator_compensation_rate Float64, -- Compensation rate - $ per 1000 likes
    creator_compensation_model String, -- Compensation model - likes, impressions, etc.
    creator_compensation_cap Float64, -- Compensation cap - maximum payout
    creator_notes String, -- Creator notes - additional information
    deleted DateTime64(3), -- Deletion timestamp - when thread was deleted
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this thread belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created this thread
    updatedms Int64, -- Update milliseconds - participant updated timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this thread
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, created_at);

DROP VIEW IF EXISTS mthreads_by_website_page;
CREATE MATERIALIZED VIEW mthreads_by_website_page
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (provider, medium, alias)
POPULATE AS
SELECT tid, provider, medium, alias, xid, xstatus, abz_status, campaign_status, content_status, created_at, updated_at
FROM mthreads
WHERE provider = 'website' AND medium = 'page';


DROP VIEW IF EXISTS mthreads_by_chat;
CREATE MATERIALIZED VIEW mthreads_by_chat
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (provider, medium, alias)
POPULATE AS
SELECT tid, provider, medium, alias, xid, xstatus, abz_status, campaign_status, content_status, created_at, updated_at
FROM mthreads
WHERE medium = 'chat';

-- Message triage table - Tracks messages in processing state for delivery and management
CREATE TABLE mtriage ON CLUSTER my_cluster (
    tid UUID, -- Thread ID - reference to parent thread
    mid UUID, -- Message ID - unique identifier for this message
    pmid UUID, -- Parent message ID - for threaded replies
    subject String, -- Subject line - for email messages
    msg String, -- Message text - actual content
    data String, -- JSON data - structured content
    urgency Int32, -- Urgency level - priority of this message
    sys Boolean, -- System message flag - whether this is system-generated
    broadcast Boolean, -- Broadcast flag - send to all subscribers
    mtempl String, -- Message template - reference to template URL
    repl JSON, -- Replacement tokens - for personalization
    svc String, -- Service - delivery service (e.g., "SES", "message", "sms")
    qid UUID, -- Queue ID - executing queue task
    rid UUID, -- Relation ID - related entity ID
    relation String, -- Relation source - source of related entity
    meta JSON, -- Metadata - additional information about the message
    scheduled DateTime64(3), -- Schedule time - when message is scheduled to send
    started DateTime64(3), -- Start time - when message processing began
    completed DateTime64(3), -- Completion time - when processing finished
    mtypes Array(String), -- Message types - attempted delivery methods
    users Array(UUID), -- Users list - targeted recipients
    deliveries Array(UUID), -- Deliveries - users who received the message
    failures Array(UUID), -- Failures - users who failed to receive the message
    xid String, -- Experiment ID - for A/B testing
    split String, -- Split variant - A/B test variant
    perms_ids Array(UUID), -- Permission IDs - additional permissions
    deleted DateTime64(3), -- Deletion timestamp - when message was deleted
    keep Boolean, -- Keep flag - whether to retain on server
    createdms Int64, -- Creation milliseconds - precise creation time for sorting
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this message belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created this message
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this message
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (tid, mid, created_at);

-- Message store table - Permanent archive for messages that have been scheduled or sent
CREATE TABLE mstore ON CLUSTER my_cluster (
    tid UUID,
    mid UUID,
    pmid UUID,
    subject String,
    msg String,
    data String,
    urgency Int32,
    sys Boolean,
    broadcast Boolean,
    mtempl String,
    repl JSON,
    svc String,
    qid UUID,
    rid UUID,
    relation String,
    meta JSON,
    planned DateTime64(3),
    scheduled DateTime64(3),
    started DateTime64(3),
    completed DateTime64(3),
    mtypes Array(String),
    users Array(UUID),
    deliveries Array(UUID),
    failures Array(UUID),
    xid String,
    split String,
    perms_ids Array(UUID),
    deleted DateTime64(3),
    keep Boolean,
    createdms Int64,
    created_at DateTime64(3),
    oid UUID,
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID,
    updated_at DateTime64(3),
    updater UUID,
    interest JSON,
    perf JSON,
    hide DateTime64(3),
    hidden Boolean,
    funnel_stage String,
    conversion_events Int64
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, mid, created_at);

-- Materialized view for finding scheduled messages - Enables efficient message scheduling
CREATE MATERIALIZED VIEW mstore_by_scheduled
ENGINE = ReplicatedReplacingMergeTree
ORDER BY scheduled
POPULATE AS
SELECT * FROM mstore
WHERE scheduled IS NOT NULL;

-- Message device type as structured data - Stores device information for message delivery
CREATE TABLE mdevices ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Device ID - unique identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    mtype String, -- Message type - delivery method (e.g., "apn", "fcm", "email")
    did String, -- Device identifier - token or address for the device
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    created_at DateTime64(3) DEFAULT now64(3) -- Creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, id);

-- Message failures table - Tracks delivery failures for retry and reporting
CREATE TABLE mfailures ON CLUSTER my_cluster (
    tid UUID, -- Thread ID - reference to parent thread
    mid UUID, -- Message ID - reference to failed message
    uid UUID, -- User ID - recipient who didn't receive the message
    mtype String, -- Message type - delivery method that failed
    mdevice_id UUID, -- Device ID - specific device that failed
    failure String, -- Failure type - reason for failure (e.g., "nopened", "noack")
    retries Int32, -- Retry count - number of retry attempts
    died DateTime64(3), -- Death timestamp - when to stop retrying
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID, -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created the original message
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this failure record
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, mid, uid, mtype);

-- Thread-Visitor Relationship table - Tracks anonymous visitor interactions with message threads
CREATE TABLE thread_visitors ON CLUSTER my_cluster (
    oid UUID, -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID, -- Thread ID - reference to message thread
    vid UUID, -- Visitor ID - anonymous visitor identifier
    variant_id String, -- Variant ID - which content variant they saw
    first_impression DateTime64(3), -- First impression timestamp - initial view
    last_impression DateTime64(3), -- Last impression timestamp - most recent view
    impression_count Int32, -- Impression count - total number of views
    interactions Array(String), -- Interactions - types of engagement (click, view, etc.)
    conversion Boolean, -- Conversion flag - whether visitor converted
    conversion_value Float64, -- Conversion value - monetary value of conversion
    created_at DateTime64(3) DEFAULT now64(3) -- Creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY (oid, tid)
ORDER BY (oid, org, tid, vid);

-- Daily impression tracking - Aggregates impression metrics by day for reporting
CREATE TABLE impression_daily ON CLUSTER my_cluster (
    oid UUID, -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID, -- Thread ID - reference to message thread
    day Date, -- Day - date for aggregation
    variant_id String, -- Variant ID - content variant ('all' for all variants)
    total_impressions Int64, -- Total impressions - all views
    anonymous_impressions Int64, -- Anonymous impressions - from visitors without UID
    identified_impressions Int64, -- Identified impressions - from known users
    unique_visitors Int64, -- Unique visitors - distinct visitors
    conversions Int64, -- Conversions - successful conversion events
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, tid, day, variant_id);

-- Channel-specific Tracking - Measures effectiveness of different marketing channels
CREATE TABLE channel_metrics ON CLUSTER my_cluster (
    channel String, -- Channel name - marketing channel (web, social, email, etc.)
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    day Date, -- Day - date for aggregation
    impressions Int64, -- Impressions - total views
    unique_visitors Int64, -- Unique visitors - distinct visitors
    clicks Int64, -- Clicks - click events
    conversions Int64, -- Conversions - successful conversion events
    conversion_value Float64, -- Conversion value - monetary value of conversions
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedSummingMergeTree((impressions, unique_visitors, clicks, conversions, conversion_value))
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, channel, day);

-- Hourly metrics - Provides more granular time-based metrics for analysis
CREATE TABLE impression_hourly ON CLUSTER my_cluster (
    tid UUID, -- Thread ID - reference to message thread
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    day Date, -- Day - date component
    hour Int32, -- Hour - hour of day (0-23)
    variant_id String, -- Variant ID - content variant
    total_impressions Int64, -- Total impressions - all views
    anonymous_impressions Int64, -- Anonymous impressions - from visitors without UID
    identified_impressions Int64, -- Identified impressions - from known users
    unique_visitors Int64, -- Unique visitors - distinct visitors
    conversions Int64, -- Conversions - successful conversion events
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedSummingMergeTree((total_impressions, anonymous_impressions, identified_impressions, unique_visitors, conversions))
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, tid, day, hour, variant_id);

-- Files table - Stores information about uploaded files and media assets
