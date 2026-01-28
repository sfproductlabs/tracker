-- Messages + Redirects - Message threads, storage, triage, redirects, impressions
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

CREATE TABLE affiliates_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    aff String DEFAULT '', -- Affiliate ID or code
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - the visitor who came through the affiliate
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    version_ts Int64 DEFAULT 0 -- Version timestamp (negative created_at for keeping oldest record)

) ENGINE = ReplicatedReplacingMergeTree(version_ts)
PARTITION BY hhash
ORDER BY (hhash, vid);

-- Distributed table for affiliates
CREATE TABLE IF NOT EXISTS affiliates ON CLUSTER tracker_cluster
AS affiliates_local
ENGINE = Distributed(tracker_cluster, sfpla, affiliates_local, rand());

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
CREATE TABLE redirects_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    urlfrom String DEFAULT '', -- Source URL without protocol (e.g., 'example.com/old-path')
    urlto String DEFAULT '', -- Destination URL with protocol (e.g., 'https://example.com/new-path')
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization identifier for multi-tenant support
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updated_at DateTime64(3) DEFAULT now64(3), -- When this redirect was last updated
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID of the person who created/updated this redirect
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY urlfrom;

-- Distributed table for redirects
CREATE TABLE IF NOT EXISTS redirects ON CLUSTER tracker_cluster
AS redirects_local
ENGINE = Distributed(tracker_cluster, sfpla, redirects_local, rand());

-- Redirect history table - Tracks the history of redirects for auditing
CREATE TABLE redirect_history_local ON CLUSTER tracker_cluster (

    urlfrom String DEFAULT '', -- Source URL without protocol
    hostfrom String DEFAULT '', -- Source host/domain
    slugfrom String DEFAULT '', -- Source path/slug
    urlto String DEFAULT '', -- Destination URL with protocol
    hostto String DEFAULT '', -- Destination host/domain
    pathto String DEFAULT '', -- Destination path
    searchto String DEFAULT '', -- Destination query parameters
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization identifier for multi-tenant support
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID of the person who created this redirect
    updated_at DateTime64(3) DEFAULT now64(3) -- Record updated timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY hostfrom
ORDER BY (urlfrom, updated_at);

-- Distributed table for redirect_history
CREATE TABLE IF NOT EXISTS redirect_history ON CLUSTER tracker_cluster
AS redirect_history_local
ENGINE = Distributed(tracker_cluster, sfpla, redirect_history_local, rand());

-- Create a materialized view for hostto lookup - Enables querying redirects by destination host
CREATE MATERIALIZED VIEW redirect_history_by_hostto
ENGINE = ReplicatedReplacingMergeTree
ORDER BY hostto
POPULATE AS
SELECT * FROM redirect_history;

-- Accounts table - Stores user authentication and permission information
CREATE TABLE msec_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to message thread
    secid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Security record ID - unique identifier for this security record
    perm_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Permission ID - reference to associated permission
    pending Boolean DEFAULT false, -- Pending flag - whether this security request is awaiting approval
    approver UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Approver user ID - who approved this security request
    approved DateTime64(3) DEFAULT toDateTime64(0, 3), -- Approval timestamp - when this request was approved
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this security record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this security record
    updatedms Int64 DEFAULT 0, -- Update milliseconds - participant updated timestamp in ms
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this security record

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, secid, created_at);

-- Distributed table for msec
CREATE TABLE IF NOT EXISTS msec ON CLUSTER tracker_cluster
AS msec_local
ENGINE = Distributed(tracker_cluster, sfpla, msec_local, rand());

-- Materialized view for finding pending security requests - Enables efficient approval workflows
CREATE MATERIALIZED VIEW msec_by_pending
ENGINE = ReplicatedReplacingMergeTree
ORDER BY pending
POPULATE AS
SELECT * FROM msec;

-- Message threads table - Comprehensive messaging system for user communications and campaigns
CREATE TABLE mthreads_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - unique identifier for this message thread
    alias String DEFAULT '', -- Alias - identifier for the thread, for chat it will be the alphabetized list of participants like user1:user2 or with the chat title if it exists chat_title:user1:user2 or chat_id for web pages it will be the full url ex. https://www.google.com/page1/page2/page3, for ads it may be the ad_with_experiment_and_variant_name
    xid String DEFAULT '', -- Experiment ID - for A/B testing
    xstatus String DEFAULT '', -- Experiment status - running, paused, completed
    name String DEFAULT '', -- Name - human-readable name for the thread
    ddata String DEFAULT '', -- Default data - default content to send
    post String DEFAULT '', -- Post/description - note or description of thread purpose
    mtempl JSON DEFAULT '{}', -- Message template - formatting templates for messages
    mcert_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Marketing certification ID - reference to security certificate
    cats Array(String) DEFAULT [], -- Categories - topical categories for this thread
    mtypes Array(String) DEFAULT [], -- Message types - delivery methods (email, sms, push, etc.)
    fmtypes Array(String) DEFAULT [], -- Fallback message types - backup delivery methods
    cmtypes Array(String) DEFAULT [], -- Child message types - delivery methods for child messages
    urgency Int32 DEFAULT 0, -- Urgency level - priority of messages in this thread
    sys Boolean DEFAULT false, -- System message flag - whether this is a system-generated thread
    ephemeral Int32 DEFAULT 0, -- Ephemeral timeout - seconds until expiry (null = keep forever)
    archived Boolean DEFAULT false, -- Archived flag - whether this thread is archived
    admins Array(UUID) DEFAULT [], -- Administrators - users who can approve new members
    opens Boolean DEFAULT false, -- Publicly subscribable - can users subscribe themselves
    openp Boolean DEFAULT false, -- Publicly publishable - can users publish themselves
    perms_ids Array(UUID) DEFAULT [], -- Permission IDs - additional permissions for this thread
    app String DEFAULT '', -- Application name - which app this thread is for
    rel String DEFAULT '', -- Release version - which app version this is for
    ver Int32 DEFAULT 0, -- Version - thread template version
    ptyp String DEFAULT '', -- Page type - category of message
    etyp String DEFAULT '', -- Event type - category of events
    ename String DEFAULT '', -- Event name - specific event identifier
    auth_name String DEFAULT '', -- Author name - who created the content
    cohorts Array(String) DEFAULT [], -- Target cohorts - groups to send to
    splits JSON DEFAULT '{}', -- Split configuration - for A/B testing variants
    source String DEFAULT '', -- Source - referring domain/service
    medium String DEFAULT '', -- Medium - referring channel & provider method - method of delivery (web page, ad, chat, sms, push, etc.)
    campaign String DEFAULT '', -- Campaign name - for tracking
    term String DEFAULT '', -- Search term - for SEO tracking
    promo String DEFAULT '', -- Promo code - included in messages
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer user ID - for referral tracking
    aff String DEFAULT '', -- Affiliate code - for partner tracking
    provider String DEFAULT '', -- Provider name - external service (Facebook, Mailchimp, etc.)
    provider_campaign_id String DEFAULT '', -- Provider campaign ID - ID in provider's system
    provider_account_id String DEFAULT '', -- Provider account ID - account in provider's system
    provider_metrics JSON DEFAULT '{}', -- Provider metrics - performance data from provider
    provider_cost Float64 DEFAULT 0.0, -- Provider cost - cost reported by provider
    provider_status String DEFAULT '', -- Provider status - status in provider's system
    broadcast Boolean DEFAULT false, -- Broadcast flag - send to all subscribers
    derive Boolean DEFAULT false, -- Derive subscribers - update based on cohorts/splits
    sent Array(UUID) DEFAULT [], -- Sent list - users message has been sent to
    outs Array(UUID) DEFAULT [], -- Outstanding list - users still to receive message
    subs Array(UUID) DEFAULT [], -- Subscribers - users who can receive messages
    pubs Array(UUID) DEFAULT [], -- Publishers - users who can send messages
    prefs JSON DEFAULT '{}', -- User preferences - message delivery preferences by user
    ftrack Boolean DEFAULT false, -- Failed delivery tracking - track failed deliveries
    strack Boolean DEFAULT false, -- Successful delivery tracking - track successful deliveries
    interest JSON DEFAULT '{}', -- Interest metrics - engagement statistics
    perf JSON DEFAULT '{}', -- Performance metrics - key performance indicators
    planned_impressions Int64 DEFAULT 0, -- Planned impression count - expected impressions
    actual_impressions Int64 DEFAULT 0, -- Actual impression count - measured impressions
    impression_goal Int64 DEFAULT 0, -- Impression goal - target number of impressions
    impression_budget Float64 DEFAULT 0.0, -- Impression budget - allocated budget for impressions
    cost_per_impression Float64 DEFAULT 0.0, -- Cost per impression - actual cost per impression
    vid_targets Array(UUID) DEFAULT [], -- Visitor ID targets - specific visitors to target
    funnel_stage String DEFAULT '', -- Funnel stage - awareness, consideration, conversion, etc.
    total_conversions Int64 DEFAULT 0, -- Total conversions - count of successful conversions
    conversion_value Float64 DEFAULT 0.0, -- Conversion value - monetary value of conversions
    variants JSON DEFAULT '{}', -- Variants - different versions for testing
    variant_weights JSON DEFAULT '{}', -- Variant weights - traffic allocation by variant
    winner_variant String DEFAULT '', -- Winner variant - best performing variant
    audience_params JSON DEFAULT '{}', -- Audience parameters - targeting parameters
    audience_metrics JSON DEFAULT '{}', -- Audience metrics - performance by audience segment
    audience_segments Array(String) DEFAULT [], -- Audience segments - predefined user segments
    content_keywords Array(String) DEFAULT [], -- Content keywords - for SEO and categorization
    content_assumptions JSON DEFAULT '{}', -- Content assumptions - hypotheses about content
    content_metrics JSON DEFAULT '{}', -- Content metrics - performance by content attributes
    content_intention String DEFAULT '', -- Content intention - inform, persuade, entertain, etc.
    abz_enabled Boolean DEFAULT false, -- A/B testing enabled - whether testing is active
    abz_algorithm String DEFAULT '', -- A/B testing algorithm - which testing approach to use
    abz_reward_metric String DEFAULT '', -- Reward metric - primary KPI for optimization
    abz_reward_value Float64 DEFAULT 0.0, -- Reward value - cumulative reward value
    abz_exploration_rate Float64 DEFAULT 0.0, -- Exploration rate - balance between explore/exploit
    abz_learning_rate Float64 DEFAULT 0.0, -- Learning rate - speed of adaptation
    abz_start_time DateTime64(3) DEFAULT toDateTime64(0, 3), -- Test start time - when testing began
    abz_sample_size Int64 DEFAULT 0, -- Sample size - current number of samples
    abz_min_sample_size Int64 DEFAULT 0, -- Minimum sample size - threshold for optimization
    abz_confidence_level Float64 DEFAULT 0.0, -- Confidence level - statistical confidence
    abz_winner_threshold Float64 DEFAULT 0.0, -- Winner threshold - when to declare a winner
    abz_auto_optimize Boolean DEFAULT false, -- Auto-optimize - whether to automatically optimize
    abz_auto_stop Boolean DEFAULT false, -- Auto-stop - whether to automatically end test
    abz_status String DEFAULT '', -- Status - running, paused, completed
    abz_params JSON DEFAULT '{}', -- Algorithm parameters - specific configuration
    abz_infinite_armed Boolean DEFAULT false, -- Infinite armed - whether using continuous parameters
    abz_param_space JSON DEFAULT '{}', -- Parameter space - ranges for optimization
    abz_model_type String DEFAULT '', -- Model type - GP, random forest, neural network, etc.
    abz_model_params JSON DEFAULT '{}', -- Model parameters - hyperparameters for model
    abz_acquisition_function String DEFAULT '', -- Acquisition function - EI, UCB, PI, etc.
    campaign_id String DEFAULT '', -- Campaign ID - reference to campaign
    campaign_phase String DEFAULT '', -- Campaign phase - teaser, launch, follow-up, etc.
    campaign_priority Int32 DEFAULT 0, -- Campaign priority - importance within campaign
    campaign_budget_allocation Float64 DEFAULT 0.0, -- Budget allocation - portion of campaign budget
    campaign_status String DEFAULT '', -- Campaign status - active, paused, scheduled, etc.
    attribution_model String DEFAULT '', -- Attribution model - how to attribute conversions
    attribution_weight Float64 DEFAULT 0.0, -- Attribution weight - importance in multi-touch
    attribution_window Int32 DEFAULT 0, -- Attribution window - time period for attribution
    attribution_params JSON DEFAULT '{}', -- Attribution parameters - model-specific settings
    requires_consent Boolean DEFAULT false, -- Requires consent - whether consent is required
    consent_types Array(String) DEFAULT [], -- Consent types - marketing, profiling, etc.
    regional_compliance JSON DEFAULT '{}', -- Regional compliance - by jurisdiction
    frequency_caps JSON DEFAULT '{}', -- Frequency caps - limits on message frequency
    data_retention Int32 DEFAULT 0, -- Data retention - period in days
    content_version Int32 DEFAULT 0, -- Content version - version number
    content_status String DEFAULT '', -- Content status - draft, review, approved, etc.
    content_approver UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Content approver - who approved the content
    content_approval_date DateTime64(3) DEFAULT toDateTime64(0, 3), -- Content approval date
    content_history Array(UUID) DEFAULT [], -- Content history - previous versions
    content_creator UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Content creator - who created the content
    content_editors Array(UUID) DEFAULT [], -- Content editors - who edited the content
    creator_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Creator ID - reference to content creator
    content_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Content ID - reference to specific creator content
    creator_compensation Boolean DEFAULT false, -- Creator compensation - whether creator is paid
    creator_compensation_rate Float64 DEFAULT 0.0, -- Compensation rate - $ per 1000 likes
    creator_compensation_model String DEFAULT '', -- Compensation model - likes, impressions, etc.
    creator_compensation_cap Float64 DEFAULT 0.0, -- Compensation cap - maximum payout
    creator_notes String DEFAULT '', -- Creator notes - additional information
    deleted DateTime64(3) DEFAULT toDateTime64(0, 3), -- Deletion timestamp - when thread was deleted
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this thread belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this thread
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - authenticated user who owns/created this thread
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - anonymous visitor who initiated this thread
    updatedms Int64 DEFAULT 0, -- Update milliseconds - participant updated timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this thread

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, created_at);

-- Distributed table for mthreads
CREATE TABLE IF NOT EXISTS mthreads ON CLUSTER tracker_cluster
AS mthreads_local
ENGINE = Distributed(tracker_cluster, sfpla, mthreads_local, rand());

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
CREATE TABLE mtriage_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to parent thread
    mid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Message ID - unique identifier for this message
    pmid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Parent message ID - for threaded replies
    subject String DEFAULT '', -- Subject line - for email messages
    msg String DEFAULT '', -- Message text - actual content
    data String DEFAULT '', -- JSON data - structured content
    urgency Int32 DEFAULT 0, -- Urgency level - priority of this message
    sys Boolean DEFAULT false, -- System message flag - whether this is system-generated
    broadcast Boolean DEFAULT false, -- Broadcast flag - send to all subscribers
    mtempl String DEFAULT '', -- Message template - reference to template URL
    repl JSON DEFAULT '{}', -- Replacement tokens - for personalization
    svc String DEFAULT '', -- Service - delivery service (e.g., "SES", "message", "sms")
    qid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Queue ID - executing queue task
    rid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Relation ID - related entity ID
    relation String DEFAULT '', -- Relation source - source of related entity
    meta JSON DEFAULT '{}', -- Metadata - additional information about the message
    scheduled DateTime64(3) DEFAULT toDateTime64(0, 3), -- Schedule time - when message is scheduled to send
    started DateTime64(3) DEFAULT toDateTime64(0, 3), -- Start time - when message processing began
    completed DateTime64(3) DEFAULT toDateTime64(0, 3), -- Completion time - when processing finished
    mtypes Array(String) DEFAULT [], -- Message types - attempted delivery methods
    users Array(UUID) DEFAULT [], -- Users list - targeted recipients
    deliveries Array(UUID) DEFAULT [], -- Deliveries - users who received the message
    failures Array(UUID) DEFAULT [], -- Failures - users who failed to receive the message
    xid String DEFAULT '', -- Experiment ID - for A/B testing
    split String DEFAULT '', -- Split variant - A/B test variant
    perms_ids Array(UUID) DEFAULT [], -- Permission IDs - additional permissions
    deleted DateTime64(3) DEFAULT toDateTime64(0, 3), -- Deletion timestamp - when message was deleted
    keep Boolean DEFAULT false, -- Keep flag - whether to retain on server
    createdms Int64 DEFAULT 0, -- Creation milliseconds - precise creation time for sorting
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this message belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this message
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - user requiring follow-up
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - visitor requiring follow-up
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this message

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (tid, mid, created_at);

-- Distributed table for mtriage
CREATE TABLE IF NOT EXISTS mtriage ON CLUSTER tracker_cluster
AS mtriage_local
ENGINE = Distributed(tracker_cluster, sfpla, mtriage_local, rand());

-- Message store table - Permanent archive for messages that have been scheduled or sent
CREATE TABLE mstore_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    mid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    pmid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    subject String DEFAULT '',
    msg String DEFAULT '',
    data String DEFAULT '',
    urgency Int32 DEFAULT 0,
    sys Boolean DEFAULT false,
    broadcast Boolean DEFAULT false,
    mtempl String DEFAULT '',
    repl JSON DEFAULT '{}',
    svc String DEFAULT '',
    qid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    rid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    relation String DEFAULT '',
    meta JSON DEFAULT '{}',
    planned DateTime64(3) DEFAULT toDateTime64(0, 3),
    scheduled DateTime64(3) DEFAULT toDateTime64(0, 3),
    started DateTime64(3) DEFAULT toDateTime64(0, 3),
    completed DateTime64(3) DEFAULT toDateTime64(0, 3),
    mtypes Array(String) DEFAULT [],
    users Array(UUID) DEFAULT [],
    deliveries Array(UUID) DEFAULT [],
    failures Array(UUID) DEFAULT [],
    xid String DEFAULT '',
    split String DEFAULT '',
    perms_ids Array(UUID) DEFAULT [],
    deleted DateTime64(3) DEFAULT toDateTime64(0, 3),
    keep Boolean DEFAULT false,
    createdms Int64 DEFAULT 0,
    created_at DateTime64(3) DEFAULT toDateTime64(0, 3),
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - authenticated user who sent this message
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - anonymous visitor who sent this message
    updated_at DateTime64(3) DEFAULT toDateTime64(0, 3),
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    interest JSON DEFAULT '{}',
    perf JSON DEFAULT '{}',
    hide DateTime64(3) DEFAULT toDateTime64(0, 3),
    hidden Boolean DEFAULT false,
    funnel_stage String DEFAULT '',
    conversion_event_count Int64 DEFAULT 0  -- Number of conversion events

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, mid, created_at);

-- Distributed table for mstore
CREATE TABLE IF NOT EXISTS mstore ON CLUSTER tracker_cluster
AS mstore_local
ENGINE = Distributed(tracker_cluster, sfpla, mstore_local, rand());

-- Materialized view for finding scheduled messages - Enables efficient message scheduling
CREATE MATERIALIZED VIEW mstore_by_scheduled
ENGINE = ReplicatedReplacingMergeTree
ORDER BY scheduled
POPULATE AS
SELECT * FROM mstore
WHERE scheduled IS NOT NULL;

-- Message device type as structured data - Stores device information for message delivery
CREATE TABLE mdevices_local ON CLUSTER tracker_cluster (

    id UUID DEFAULT generateUUIDv4(), -- Device ID - unique identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    mtype String DEFAULT '', -- Message type - delivery method (e.g., "apn", "fcm", "email")
    device_token String DEFAULT '', -- Device token or address for push notifications
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    created_at DateTime64(3) DEFAULT now64(3) -- Creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, id);

-- Distributed table for mdevices
CREATE TABLE IF NOT EXISTS mdevices ON CLUSTER tracker_cluster
AS mdevices_local
ENGINE = Distributed(tracker_cluster, sfpla, mdevices_local, rand());

-- Message failures table - Tracks delivery failures for retry and reporting
CREATE TABLE mfailures_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to parent thread
    mid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Message ID - reference to failed message
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - recipient who didn't receive the message
    mtype String DEFAULT '', -- Message type - delivery method that failed
    mdevice_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Device ID - specific device that failed
    failure String DEFAULT '', -- Failure type - reason for failure (e.g., "nopened", "noack")
    retries Int32 DEFAULT 0, -- Retry count - number of retry attempts
    died DateTime64(3) DEFAULT toDateTime64(0, 3), -- Death timestamp - when to stop retrying
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created the original message
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this failure record

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
ORDER BY (oid, org, tid, mid, uid, mtype);

-- Distributed table for mfailures
CREATE TABLE IF NOT EXISTS mfailures ON CLUSTER tracker_cluster
AS mfailures_local
ENGINE = Distributed(tracker_cluster, sfpla, mfailures_local, rand());

-- Thread-Visitor Relationship table - Tracks anonymous visitor interactions with message threads
CREATE TABLE thread_visitors_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to message thread
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - anonymous visitor identifier
    variant_id String DEFAULT '', -- Variant ID - which content variant they saw
    first_impression DateTime64(3) DEFAULT toDateTime64(0, 3), -- First impression timestamp - initial view
    last_impression DateTime64(3) DEFAULT toDateTime64(0, 3), -- Last impression timestamp - most recent view
    impression_count Int32 DEFAULT 0, -- Impression count - total number of views
    interactions Array(String) DEFAULT [], -- Interactions - types of engagement (click, view, etc.)
    conversion Boolean DEFAULT false, -- Conversion flag - whether visitor converted
    conversion_value Float64 DEFAULT 0.0, -- Conversion value - monetary value of conversion
    created_at DateTime64(3) DEFAULT now64(3) -- Creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY (oid, tid)
ORDER BY (oid, org, tid, vid);

-- Distributed table for thread_visitors
CREATE TABLE IF NOT EXISTS thread_visitors ON CLUSTER tracker_cluster
AS thread_visitors_local
ENGINE = Distributed(tracker_cluster, sfpla, thread_visitors_local, rand());

-- Daily impression tracking - Aggregates impression metrics by day for reporting
CREATE TABLE impression_daily_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this failure record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to message thread
    campaign_id String DEFAULT '', -- Campaign ID - for campaign-level aggregation (alternative to tid-based aggregation)
    day Date DEFAULT today(), -- Day - date for aggregation
    variant_id String DEFAULT '', -- Variant ID - content variant ('all' for all variants)
    total_impressions Int64 DEFAULT 0, -- Total impressions - all views
    anonymous_impressions Int64 DEFAULT 0, -- Anonymous impressions - from visitors without UID
    identified_impressions Int64 DEFAULT 0, -- Identified impressions - from known users
    unique_visitors Int64 DEFAULT 0, -- Unique visitors - distinct visitors
    conversions Int64 DEFAULT 0, -- Conversions - successful conversion events
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, tid, day, variant_id);

-- Distributed table for impression_daily
CREATE TABLE IF NOT EXISTS impression_daily ON CLUSTER tracker_cluster
AS impression_daily_local
ENGINE = Distributed(tracker_cluster, sfpla, impression_daily_local, rand());

-- Channel-specific Tracking - Measures effectiveness of different marketing channels
CREATE TABLE channel_metrics_local ON CLUSTER tracker_cluster (

    channel String DEFAULT '', -- Channel name - marketing channel (web, social, email, etc.)
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    day Date DEFAULT today(), -- Day - date for aggregation
    impressions Int64 DEFAULT 0, -- Impressions - total views
    unique_visitors Int64 DEFAULT 0, -- Unique visitors - distinct visitors
    clicks Int64 DEFAULT 0, -- Clicks - click events
    conversions Int64 DEFAULT 0, -- Conversions - successful conversion events
    conversion_value Float64 DEFAULT 0.0, -- Conversion value - monetary value of conversions
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp

) ENGINE = ReplicatedSummingMergeTree((impressions, unique_visitors, clicks, conversions, conversion_value))
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, channel, day);

-- Distributed table for channel_metrics
CREATE TABLE IF NOT EXISTS channel_metrics ON CLUSTER tracker_cluster
AS channel_metrics_local
ENGINE = Distributed(tracker_cluster, sfpla, channel_metrics_local, rand());

-- Hourly metrics - Provides more granular time-based metrics for analysis
CREATE TABLE impression_hourly_local ON CLUSTER tracker_cluster (

    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - reference to message thread
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    day Date DEFAULT today(), -- Day - date component
    hour Int32 DEFAULT 0, -- Hour - hour of day (0-23)
    variant_id String DEFAULT '', -- Variant ID - content variant
    total_impressions Int64 DEFAULT 0, -- Total impressions - all views
    anonymous_impressions Int64 DEFAULT 0, -- Anonymous impressions - from visitors without UID
    identified_impressions Int64 DEFAULT 0, -- Identified impressions - from known users
    unique_visitors Int64 DEFAULT 0, -- Unique visitors - distinct visitors
    conversions Int64 DEFAULT 0, -- Conversions - successful conversion events
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp

) ENGINE = ReplicatedSummingMergeTree((total_impressions, anonymous_impressions, identified_impressions, unique_visitors, conversions))
PARTITION BY (oid, toYYYYMM(day))
ORDER BY (oid, org, tid, day, hour, variant_id);

-- Distributed table for impression_hourly
CREATE TABLE IF NOT EXISTS impression_hourly ON CLUSTER tracker_cluster
AS impression_hourly_local
ENGINE = Distributed(tracker_cluster, sfpla, impression_hourly_local, rand());

-- Files table - Stores information about uploaded files and media assets
