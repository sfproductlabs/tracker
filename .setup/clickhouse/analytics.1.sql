-- Analytics + Traffic - Visitors, sessions, events, hits, traffic analytics
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;


-- Events table - Records all user interactions and events with full context
-- FOREIGN KEY RELATIONSHIPS:
--   * vid -> visitors.vid (many-to-one)
--   * sid -> sessions.sid (many-to-one)
--   * uid -> users.uid (many-to-one)
--   * auth -> users.uid (many-to-one)
--   * invoice_id -> payments.invid (many-to-many: one invoice has multiple line items)
--   * rid -> depends on relation field (many-to-one)
CREATE TABLE events_local ON CLUSTER tracker_cluster (

    eid UUID DEFAULT generateUUIDv4(), -- Event ID - unique identifier for each event
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - links to the visitor who triggered this event
    device_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Device ID
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID - links to the session this event occurred in
    hhash String DEFAULT '', -- Host hash - identifies the website/application
    app String DEFAULT '', -- Application name
    rel String DEFAULT '', -- Application release/version
    cflags Int64 DEFAULT 0, -- Compliance flags
    created_at DateTime64(3) DEFAULT now64(3), -- When this event was recorded
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID if authenticated
    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Thread ID - linked to message thread
    last String DEFAULT '', -- Previous action/URL
    url String DEFAULT '', -- Current URL where event occurred
    ip String DEFAULT '', -- Client IP address
    iphash String DEFAULT '', -- Hashed IP for privacy
    lat Float64 DEFAULT 0.0, -- Location latitude
    lon Float64 DEFAULT 0.0, -- Location longitude
    ptyp String DEFAULT '', -- Page type where event occurred
    bhash String DEFAULT '', -- Browser hash
    auth_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Content author ID
    duration Int64 DEFAULT 0, -- Time since last interaction (in milliseconds)
    -- Experiment and Tracking Fields
    xid String DEFAULT '', -- Experiment ID
    split String DEFAULT '', -- Experiment split (A/B)
    ename String DEFAULT '', -- Event name/action (e.g., "click", "purchase")
    source String DEFAULT '', -- Traffic source (utm_source)
    medium String DEFAULT '', -- Traffic medium (utm_medium)
    campaign String DEFAULT '', -- Campaign name (utm_campaign)
    content String DEFAULT '', -- Content ID for bandit optimization and A/B testing
    country String DEFAULT '', -- Country code
    region String DEFAULT '', -- Region name
    city String DEFAULT '', -- City name
    zip String DEFAULT '', -- ZIP/postal code
    term String DEFAULT '', -- Search term (utm_term)
    etyp String DEFAULT '', -- Event category
    ver Int32 DEFAULT 0, -- Version or variation
    sink String DEFAULT '', -- Target conversion goal
    score Float64 DEFAULT 0.0, -- Conversion funnel progress score
    params JSON DEFAULT '{}', -- Additional parameters as JSON format:
               -- {
               --   "utm_source": "string", // Original traffic source if available
               --   "utm_medium": "string", // Original medium if available
               --   "utm_campaign": "string", // Original campaign if available
               --   "xid": "string", // ID of active experiment
               --   "variant": "string", // Experiment variant being shown
               --   "interaction_type": "string", // Type of user interaction
               --   "element_id": "string", // ID of UI element interacted with
               --   "custom_dimensions": {...} // Custom tracking dimensions
               -- }
    -- Additional Fields
    invoice_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Reference to invoice (payments.invid) if this event involved a transaction
                     -- Events track at INVOICE level, not line-item level
                     -- To get line items: JOIN payments ON payments.invid = events.invoice_id
    targets JSON DEFAULT '{}', -- Components interacted with (videos, ads, etc.)
    relation String DEFAULT '', -- Related object reference (e.g., "content_library")
    rid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Related ID (content_library ID,newsletter ID, etc.)
    ja4h String DEFAULT '', -- JA4H HTTP client fingerprint for advanced client identification
    gaid String DEFAULT '', -- Google Analytics ID
    idfa String DEFAULT '', -- Apple IDFA
    msid String DEFAULT '', -- Microsoft ID
    fbid String DEFAULT '', -- Facebook ID
    culture String DEFAULT '', -- Culture/locale
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer
    rcode String DEFAULT '', -- Referral code
    aff String DEFAULT '', -- Affiliate
    browser String DEFAULT '', -- Browser
    device String DEFAULT '', -- Device type
    os String DEFAULT '', -- Operating system
    tz String DEFAULT '', -- Timezone
    vp_w Int64 DEFAULT 0, -- Viewport width
    vp_h Int64 DEFAULT 0, -- Viewport height
    PROJECTION tid_proj
    (
        SELECT _part_offset ORDER BY tid
    ),
    PROJECTION uid_proj
    (
        SELECT _part_offset ORDER BY uid
    ),
    PROJECTION vid_proj
    (
        SELECT _part_offset ORDER BY vid
    )


) ENGINE = MergeTree()
PARTITION BY (oid, hhash, toYYYYMM(created_at))
ORDER BY (created_at, eid)
SETTINGS index_granularity = 8192,
         min_bytes_for_wide_part = 0,
         deduplicate_merge_projection_mode = 'rebuild';

-- Distributed table for events
CREATE TABLE IF NOT EXISTS events ON CLUSTER tracker_cluster
AS events_local
ENGINE = Distributed(tracker_cluster, sfpla, events_local, rand());


-- Visitors view - Gets first visit information for each visitor (earliest event by created_at)
CREATE VIEW visitors AS
SELECT
    oid,
    org,
    vid,
    created_at,
    device_id,
    sid,
    hhash,
    app,
    rel,
    cflags,
    uid,
    tid,
    last,
    url,
    ip,
    iphash,
    lat,
    lon,
    ptyp,
    bhash,
    auth_id,
    xid,
    split,
    ename,
    etyp,
    ver,
    sink,
    score,
    params,
    source,
    medium,
    campaign,
    country,
    region,
    city,
    zip,
    term,
    gaid,
    idfa,
    msid,
    fbid,
    culture,
    ref,
    rcode,
    aff,
    browser,
    device,
    os,
    tz,
    vp_w,
    vp_h,
    ja4h
FROM events
WHERE vid IS NOT NULL
ORDER BY created_at ASC
LIMIT 1 BY oid, vid;

-- Create a materialized view for time-based queries - Enables efficient queries by creation time
CREATE MATERIALIZED VIEW visitors_by_time
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY created_at
POPULATE AS
SELECT * FROM visitors;

-- Create a materialized view for uid lookup - Enables efficient queries by user ID
CREATE MATERIALIZED VIEW visitors_by_uid
ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY uid
POPULATE AS
SELECT * FROM visitors;

-- Visitors latest (most recent visitors) - Gets most recent event for each visitor
CREATE VIEW visitors_latest AS
SELECT
    oid,
    org,
    vid,
    created_at,
    device_id,
    sid,
    hhash,
    app,
    rel,
    cflags,
    uid,
    tid,
    last,
    url,
    ip,
    iphash,
    lat,
    lon,
    ptyp,
    bhash,
    auth_id,
    xid,
    split,
    ename,
    etyp,
    ver,
    sink,
    score,
    params,
    source,
    medium,
    campaign,
    country,
    region,
    city,
    zip,
    term,
    gaid,
    idfa,
    msid,
    fbid,
    culture,
    ref,
    rcode,
    aff,
    browser,
    device,
    os,
    tz,
    vp_w,
    vp_h,
    ja4h
FROM events
WHERE vid IS NOT NULL
ORDER BY created_at DESC
LIMIT 1 BY oid, vid;

-- Create a materialized view for uid lookup in visitors_latest
CREATE MATERIALIZED VIEW visitors_latest_by_uid
ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY uid
POPULATE AS
SELECT * FROM visitors_latest;

-- Create a materialized view for time-based queries - Enables efficient queries by creation time
CREATE MATERIALIZED VIEW visitors_latest_by_time
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY created_at
POPULATE AS
SELECT * FROM visitors_latest;

-- Sessions view - Gets first event per session with session stats
CREATE VIEW sessions AS
SELECT
    oid,
    org,
    sid,
    vid,
    uid,
    created_at,
    device_id,
    hhash,
    app,
    rel,
    cflags,
    tid,
    last,
    url,
    ip,
    iphash,
    lat,
    lon,
    ptyp,
    bhash,
    auth_id,
    duration,
    xid,
    split,
    ename,
    etyp,
    ver,
    sink,
    score,
    params,
    source,
    medium,
    campaign,
    country,
    region,
    city,
    zip,
    term,
    gaid,
    idfa,
    msid,
    fbid,
    culture,
    ref,
    rcode,
    aff,
    browser,
    device,
    os,
    tz,
    vp_w,
    vp_h,
    ja4h
FROM events
WHERE sid IS NOT NULL
ORDER BY created_at ASC
LIMIT 1 BY oid, sid;

-- Create a materialized view for time-based queries - Enables efficient queries by creation time
CREATE MATERIALIZED VIEW sessions_by_time
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY created_at
POPULATE AS
SELECT * FROM sessions;

-- Events recent view - Shows events from the last 7 days for recent data analysis
CREATE VIEW events_recent AS
SELECT *
FROM events
WHERE created_at >= now() - INTERVAL 7 DAY;

-- Nodes view - Maps visitors to IP addresses with host context (first occurrence)
CREATE VIEW nodes AS
SELECT
    hhash,
    vid,
    uid,
    iphash,
    ip,
    sid,
    created_at
FROM events
WHERE vid IS NOT NULL AND ip != '' AND iphash != ''
ORDER BY created_at ASC
LIMIT 1 BY hhash, vid, iphash;

-- Locations view - Geographic location data for visitors (first occurrence)
CREATE VIEW locations AS
SELECT
    hhash,
    vid,
    lat,
    lon,
    uid,
    sid,
    created_at
FROM events
WHERE vid IS NOT NULL AND lat != 0 AND lon != 0
ORDER BY created_at ASC
LIMIT 1 BY hhash, vid, lat, lon;

-- Aliases view - Maps visitors to authenticated users (first occurrence)
CREATE VIEW aliases AS
SELECT
    hhash,
    vid,
    uid,
    sid,
    created_at
FROM events
WHERE vid IS NOT NULL AND uid IS NOT NULL AND uid != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY created_at ASC
LIMIT 1 BY hhash, vid, uid;

-- User sessions view - Cross-reference between users and their visitor IDs (first occurrence)
CREATE VIEW user_sessions AS
SELECT
    hhash,
    uid,
    vid,
    sid,
    created_at
FROM events
WHERE uid IS NOT NULL AND uid != toUUID('00000000-0000-0000-0000-000000000000') AND vid IS NOT NULL AND sid IS NOT NULL
ORDER BY created_at ASC
LIMIT 1 BY hhash, uid, vid, sid;

-- Usernames table - Maps usernames to visitor IDs for username tracking
CREATE TABLE usernames_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    uhash String DEFAULT '', -- Username hash - hashed for privacy and efficient lookups
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - links to a visitor record
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID - identifies the session when username was recorded
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY hhash
ORDER BY (hhash, uhash, vid);

-- Distributed table for usernames
CREATE TABLE IF NOT EXISTS usernames ON CLUSTER tracker_cluster
AS usernames_local
ENGINE = Distributed(tracker_cluster, sfpla, usernames_local, rand());

-- Cells table - Maps cell phone hashes to visitor IDs for phone number tracking
CREATE TABLE cells_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    chash String DEFAULT '', -- Cell phone hash - hashed for privacy and efficient lookups
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - links to a visitor record
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID - identifies the session when cell number was recorded
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY hhash
ORDER BY (hhash, chash, vid);

-- Distributed table for cells
CREATE TABLE IF NOT EXISTS cells ON CLUSTER tracker_cluster
AS cells_local
ENGINE = Distributed(tracker_cluster, sfpla, cells_local, rand());

-- Emails table - Maps email hashes to visitor IDs for email tracking
CREATE TABLE emails_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    ehash String DEFAULT '', -- Email hash - hashed for privacy and efficient lookups
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - links to a visitor record
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID - identifies the session when email was recorded
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY hhash
ORDER BY (hhash, ehash);

-- Distributed table for emails
CREATE TABLE IF NOT EXISTS emails ON CLUSTER tracker_cluster
AS emails_local
ENGINE = Distributed(tracker_cluster, sfpla, emails_local, rand());

-- Hits table - URL hit counter (replacing Cassandra counter)
CREATE TABLE hits_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    url String DEFAULT '', -- URL path that was visited
    total UInt64 DEFAULT 0, -- Counter for number of hits to this URL
    date Date DEFAULT today() -- Date when hits were recorded, for partitioning

) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY (hhash, url, date);

-- Distributed table for hits
CREATE TABLE IF NOT EXISTS hits ON CLUSTER tracker_cluster
AS hits_local
ENGINE = Distributed(tracker_cluster, sfpla, hits_local, rand());

-- Materialized view to auto-populate hits from events
CREATE MATERIALIZED VIEW hits_mv TO hits AS
SELECT
    hhash,
    url,
    1 as total,
    toDate(created_at) as date
FROM events
WHERE url != '';

-- IPs table - IP address counter (replacing Cassandra counter)
CREATE TABLE ips_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    ip String DEFAULT '', -- IP address
    total UInt64 DEFAULT 0, -- Counter for number of requests from this IP
    date Date DEFAULT today() -- Date when requests were recorded, for partitioning

) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY (hhash, ip, date);

-- Distributed table for ips
CREATE TABLE IF NOT EXISTS ips ON CLUSTER tracker_cluster
AS ips_local
ENGINE = Distributed(tracker_cluster, sfpla, ips_local, rand());

-- Materialized view to auto-populate ips from events
CREATE MATERIALIZED VIEW ips_mv TO ips AS
SELECT
    hhash,
    ip,
    1 as total,
    toDate(created_at) as date
FROM events
WHERE ip != '';

-- Routed view - Last route for each IP address (most recent)
CREATE VIEW routed AS
SELECT
    hhash,
    ip,
    url,
    created_at as updated_at
FROM events
WHERE ip != '' AND url != ''
ORDER BY created_at DESC
LIMIT 1 BY hhash, ip;

-- Reqs table - Visitor request counter (replacing Cassandra counter)
CREATE TABLE reqs_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - links to a visitor record
    total UInt64 DEFAULT 0, -- Counter for number of requests from this visitor
    date Date DEFAULT today() -- Date when requests were recorded, for partitioning

) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY (hhash, vid, date);

-- Distributed table for reqs
CREATE TABLE IF NOT EXISTS reqs ON CLUSTER tracker_cluster
AS reqs_local
ENGINE = Distributed(tracker_cluster, sfpla, reqs_local, rand());

-- Materialized view to auto-populate reqs from events
CREATE MATERIALIZED VIEW reqs_mv TO reqs AS
SELECT
    hhash,
    vid,
    1 as total,
    toDate(created_at) as date
FROM events
WHERE vid IS NOT NULL;

-- Browsers table - Browser usage statistics (replacing Cassandra counter)
CREATE TABLE browsers_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    bhash String DEFAULT '', -- Browser hash - hashed browser identifier
    browser String DEFAULT '', -- User agent string
    total UInt64 DEFAULT 0, -- Counter for usage frequency of this browser
    date Date DEFAULT today() -- Date when browser was used, for partitioning

) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY (hhash, bhash, browser, date);

-- Distributed table for browsers
CREATE TABLE IF NOT EXISTS browsers ON CLUSTER tracker_cluster
AS browsers_local
ENGINE = Distributed(tracker_cluster, sfpla, browsers_local, rand());

-- Materialized view to auto-populate browsers from events
CREATE MATERIALIZED VIEW browsers_mv TO browsers AS
SELECT
    hhash,
    bhash,
    browser,
    1 as total,
    toDate(created_at) as date
FROM events
WHERE bhash != '' AND browser != '';

-- Referrers table - Tracks referring URLs (replacing Cassandra counter)
CREATE TABLE referrers_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifies the website/application
    url String DEFAULT '', -- Referring URL
    total UInt64 DEFAULT 0, -- Counter for number of visits from this referrer
    date Date DEFAULT today() -- Date when referrals occurred, for partitioning

) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(date)
ORDER BY (hhash, url, date);

-- Distributed table for referrers
CREATE TABLE IF NOT EXISTS referrers ON CLUSTER tracker_cluster
AS referrers_local
ENGINE = Distributed(tracker_cluster, sfpla, referrers_local, rand());

-- Materialized view to auto-populate referrers from events
CREATE MATERIALIZED VIEW referrers_mv TO referrers AS
SELECT
    hhash,
    last as url,
    1 as total,
    toDate(created_at) as date
FROM events
WHERE last != '';

-- Referrals view - Tracks user-to-user referrals (first occurrence)
CREATE VIEW referrals AS
SELECT
    hhash,
    ref,
    vid,
    0 as gen, -- Generation can be calculated separately if needed
    created_at,
    -toInt64(created_at) as version_ts
FROM events
WHERE vid IS NOT NULL AND ref IS NOT NULL AND ref != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY created_at ASC
LIMIT 1 BY hhash, vid;

-- Referred view - Tracks referrals by referral code (first occurrence)
CREATE VIEW referred AS
SELECT
    hhash,
    rcode,
    vid,
    0 as gen, -- Generation can be calculated separately if needed
    created_at,
    -toInt64(created_at) as version_ts
FROM events
WHERE vid IS NOT NULL AND rcode != ''
ORDER BY created_at ASC
LIMIT 1 BY hhash, vid;

-- Affiliates table - Tracks affiliate referrals

-- Last event occurrence by visitor/user/event name (most recent)
CREATE VIEW last_event_by_vid_uid_ename AS
SELECT
    oid,
    org,
    vid,
    uid,
    ename,
    created_at as last_occurred,
    tid as last_thread,
    dateDiff('day', created_at, now()) as days_since
FROM events
WHERE vid IS NOT NULL AND uid IS NOT NULL AND ename IS NOT NULL
ORDER BY created_at DESC
LIMIT 1 BY oid, vid, uid, ename;



-- Last event by user (for recency targeting - most recent)
CREATE OR REPLACE VIEW last_event_by_user AS
SELECT
    oid,
    org,
    uid,
    ename as event_type,
    created_at as last_event_time,
    dateDiff('day', created_at, now()) as days_since_event
FROM events
WHERE uid IS NOT NULL
ORDER BY created_at DESC
LIMIT 1 BY oid, uid, ename;

-- Conversion funnel with lag times
-- NOTE: Uses events.invoice_id to link to invoice-level payment data
CREATE OR REPLACE VIEW conversion_funnel AS
SELECT
    oid,
    org,
    vid,
    min(created_at) as first_touch,
    maxIf(created_at, ename IN ('conversion', 'purchase', 'paid')) as conversion_time,
    dateDiff('day', min(created_at), maxIf(created_at, ename IN ('conversion', 'purchase', 'paid'))) as days_to_conversion,
    maxIf(score, ename IN ('purchase', 'paid')) as conversion_value,
    anyIf(invoice_id, ename IN ('conversion', 'purchase', 'paid')) as invoice_id -- Link to invoice
FROM events
WHERE vid IS NOT NULL
GROUP BY oid, org, vid
HAVING conversion_time IS NOT NULL;

-- Event frequency by type (for pattern detection)
CREATE OR REPLACE VIEW event_frequency AS
SELECT
    oid,
    org,
    ename as event_type,
    count() as total_events,
    uniq(vid) as unique_visitors,
    uniq(uid) as unique_users,
    min(created_at) as first_seen,
    max(created_at) as last_seen,
    avg(score) as avg_score
FROM events
GROUP BY oid, org, ename
ORDER BY total_events DESC;
