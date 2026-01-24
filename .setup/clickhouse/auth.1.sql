-- Auth + Logging - Accounts, services, actions, permissions, logs
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

CREATE TABLE accounts ON CLUSTER my_cluster (
    uid UUID, -- User ID - unique identifier for the user
    pwd String, -- Password hash - securely stored password
    ip String, -- Client IP - IP address used during account creation/last login
    msg String, -- Message/notes about this account
    expires DateTime64(3), -- Account expiration date
    creds JSON, -- Credentials and permissions as JSON (host, claim[yes])
    created_at DateTime64(3) DEFAULT now64(3), -- Account creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Account updated timestamp
    owner UUID, -- Owner user ID - who created this account
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY uid;

-- -- Initial admin user
INSERT INTO accounts (
    uid, 
    pwd, 
    ip,
    msg, 
    expires, 
    creds, 
    created_at, 
    owner
) VALUES (
    toUUID('14fb0860-b4bf-11e9-8971-7b80435315ac'),
    'W6ph5Mm5Pz8GgiULbPgzG37mj9g=',
    '127.0.0.1',
    'demo admin user',
    toDateTime64('2024-01-01 00:00:00', 3),
    '{"*":{"*":"*"}}',
    toDateTime64('2019-08-07 00:00:00', 3),
    toUUID('14fb0860-b4bf-11e9-8971-7b80435315ac')
);

-- Services table - INTERNAL & EXTERNAL SERVICES - Stores service authentication and permission information
CREATE TABLE services ON CLUSTER my_cluster (
    name String, -- Service name - unique identifier for the service
    secret String, -- Secret hash - securely stored authentication secret
    roles Array(String), -- Service roles - array of role names granted to this service
    expiry Date, -- Expiration date - when service access expires
    created_at DateTime64(3) DEFAULT now64(3), -- Service creation timestamp
    oid UUID, -- Organization ID - which oid this service belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created this service
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- User ID of who last updated this service
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY name;

-- Action names table - Registry of valid action types in the system
CREATE TABLE action_names ON CLUSTER my_cluster (
    name String, -- Action name - unique identifier for this action type
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY name;

-- Actions table - Tracks execution of various actions in the system
CREATE TABLE actions ON CLUSTER my_cluster (
    oid UUID, -- Organization ID - which oid this action belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    sid UUID, -- Source ID - identifier of the source entity (e.g., message ID)
    src String, -- Source type - what kind of action (e.g., "message", "queues")
    did UUID, -- Differentiator ID - additional identifier (e.g., user ID)
    dsrc String, -- Differentiator source - what the differentiator represents (e.g., "uid")
    meta JSON, -- Metadata - additional information about the action (e.g., split info)
    exqid UUID, -- Executing queue ID - links to the queue handling this action
    created_at DateTime64(3) DEFAULT now64(3), -- Action creation timestamp
    started DateTime64(3), -- When action execution started
    completed DateTime64(3), -- When action execution completed
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (sid, did, created_at);

-- External actions table - Tracks actions from external systems (e.g., email delivery services)
CREATE TABLE actions_ext ON CLUSTER my_cluster (
    sid String, -- Source ID - external identifier (e.g., SES message ID)
    svc String, -- Service - name of the external service (e.g., "SES", "message", "sms")
    iid UUID, -- Internal ID - corresponding internal record ID
    uid UUID, -- User ID - optional link to affected user
    oid UUID, -- Organization ID - which oid this action belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    meta JSON -- Metadata - additional information about the action (e.g., email hash, bounce status)
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (sid, svc)
TTL toDateTime(created_at) + INTERVAL 14 DAY;

-- Dailies table (replacing counter with SummingMergeTree) - NATS Specializations - limit service usage
CREATE TABLE dailies ON CLUSTER my_cluster (
    ip String, -- client IP
    day Date, -- day for aggregation
    total UInt64 -- counter
) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(day)
ORDER BY (ip, day);

-- Counters table (replacing counter with SummingMergeTree)
CREATE TABLE counters ON CLUSTER my_cluster (
    id String, -- Unique identifier for the counter
    total UInt64, -- Accumulating counter value
    date Date DEFAULT today() -- Date of counter record for aggregation
) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY id;

-- Logs table - Server debugging and audit logs
CREATE TABLE logs ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique log entry identifier
    ldate Date, -- Log date for partitioning and querying
    created_at DateTime64(3) DEFAULT now64(3), -- When the log entry was created
    ltime DateTime64(9), -- Nanosecond precision time for detailed server debugging
    topic String, -- Log topic/category
    name String, -- Component/service name generating the log
    host String, -- Host IP or identifier
    hostname String, -- Human-readable hostname
    oid UUID, -- Organization ID - which oid this log belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- User responsible for the action being logged
    ip String, -- IP address associated with the event
    iphash String, -- Hashed version of IP for privacy
    level Int32, -- Log severity level (info, warning, error, etc.)
    msg String, -- The actual log message content
    params JSON, -- Additional parameters as structured JSON data
    PROJECTION level_proj
    (
        SELECT _part_offset ORDER BY level
    ),
    PROJECTION topic_proj
    (
        SELECT _part_offset ORDER BY topic
    )
) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(ldate)
ORDER BY (created_at, id)
SETTINGS index_granularity = 8192, 
         min_bytes_for_wide_part = 0,
         deduplicate_merge_projection_mode = 'rebuild';

-- Updates table - For tracking system-wide updates
CREATE TABLE updates ON CLUSTER my_cluster (
    id String, -- Unique identifier for the update
    updated_at DateTime64(3) DEFAULT now64(3), -- When the update occurred
    msg String, -- Description of the update
    PROJECTION updated_at_proj
    (
        SELECT _part_offset ORDER BY updated_at
    )
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id
SETTINGS index_granularity = 8192, 
         min_bytes_for_wide_part = 0,
         deduplicate_merge_projection_mode = 'rebuild';

-- Zips table - Geographic and demographic data by ZIP/postal code
CREATE TABLE permissions ON CLUSTER my_cluster (
    id UUID, -- Permission ID - unique identifier for this permission
    oid UUID, -- Organization ID - the organization granting the permission
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    rtype String, -- Resource type (e.g., "file", "user", "cohort")
    rpath String, -- Resource path - hierarchical path to the resource
    obj String, -- Object identifier - specific resource being accessed
    ref UUID, -- Reference ID - user or entity receiving the permission
    action String, -- Action being permitted (e.g., "read", "write", "delete")
    effect Boolean, -- Effect - true=allow, false=deny
    updated_at DateTime64(3) DEFAULT now64(3) -- Creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (oid, org, rtype, rpath, ref, action);

-- Create indices for permissions
-- Index for looking up permissions by ID
CREATE MATERIALIZED VIEW permissions_by_id
ENGINE = ReplicatedReplacingMergeTree
ORDER BY id
POPULATE AS
SELECT * FROM permissions;


-- Index for looking up permissions by organization
CREATE MATERIALIZED VIEW permissions_by_org
ENGINE = ReplicatedReplacingMergeTree
ORDER BY oid
POPULATE AS
SELECT * FROM permissions;

-- Index for looking up permissions by resource path
CREATE MATERIALIZED VIEW permissions_by_rpath
ENGINE = ReplicatedReplacingMergeTree
ORDER BY rpath
POPULATE AS
SELECT * FROM permissions;

-- Index for looking up permissions by reference ID (typically user)
CREATE MATERIALIZED VIEW permissions_by_ref
ENGINE = ReplicatedReplacingMergeTree
ORDER BY ref
POPULATE AS
SELECT * FROM permissions;

-- Index for looking up permissions by action
CREATE MATERIALIZED VIEW permissions_by_action
ENGINE = ReplicatedReplacingMergeTree
ORDER BY action
POPULATE AS
SELECT * FROM permissions;

-- Platform Credentials - OAuth2 token storage for Google & Bing Ads
CREATE TABLE IF NOT EXISTS platform_credentials ON CLUSTER my_cluster (
    oid UUID,                               -- Organization ID - multi-tenant isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid
    platform String,                        -- Platform: 'google_ads', 'bing_ads', etc.
    account_id String,                      -- Platform-specific account ID
    account_email String,                   -- Email associated with account
    encrypted_access_token String,          -- Fernet-encrypted access token
    encrypted_refresh_token String,         -- Fernet-encrypted refresh token
    token_expires_at DateTime64(3),         -- When access token expires (for auto-refresh)
    scopes String DEFAULT '',               -- OAuth scopes granted (comma-separated)
    connected_at DateTime64(3) DEFAULT now64(3),  -- When first connected
    updated_at DateTime64(3) DEFAULT now64(3),    -- Last token refresh
    is_valid Bool DEFAULT true,             -- False if token refresh fails
    metadata JSON DEFAULT '{}',             -- Additional platform-specific data

    -- Index for token expiry checks
    INDEX idx_expiry token_expires_at TYPE minmax GRANULARITY 1

) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/platform_credentials', '{replica}', updated_at)
PARTITION BY (oid, platform)
ORDER BY (oid, org, platform, account_id)
SETTINGS index_granularity = 8192;

-- View: Active Platform Credentials (valid and not expired)
CREATE OR REPLACE VIEW active_platform_credentials AS
SELECT
    oid,
    org,
    platform,
    account_id,
    account_email,
    token_expires_at,
    connected_at,
    updated_at,
    CASE
        WHEN token_expires_at < now() THEN 'expired'
        WHEN token_expires_at < now() + INTERVAL 1 HOUR THEN 'expiring_soon'
        ELSE 'valid'
    END as token_status,
    dateDiff('day', connected_at, now()) as days_connected
FROM platform_credentials
WHERE is_valid = true
ORDER BY oid, org, platform, connected_at DESC;

-- View: Platform Credentials Summary per Organization
CREATE OR REPLACE VIEW platform_credentials_summary AS
SELECT
    oid,
    org,
    platform,
    count() as total_accounts,
    countIf(is_valid = true) as valid_accounts,
    countIf(token_expires_at < now()) as expired_accounts,
    min(token_expires_at) as earliest_expiry,
    max(connected_at) as latest_connection
FROM platform_credentials
GROUP BY oid, org, platform
ORDER BY oid, org, platform;

-- Multi-armed Bandit testing tables

-- Variant Performance table - Tracks performance metrics for A/B test variants
