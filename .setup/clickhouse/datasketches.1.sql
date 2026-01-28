-- ====================================================================
-- DataSketches Schema Migration
-- High-dimensional contextual bandit with sketch-based aggregation
-- ====================================================================

-- ====================================================================
-- RAW BANDIT EVENTS (High volume, short retention)
-- ====================================================================

CREATE TABLE bandit_events_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    event_id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64(3) DEFAULT now64(3),

    -- User identification
    user_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    visitor_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',

    -- Experiment
    xid String DEFAULT '',
    arm_id String DEFAULT '',

    -- High-dimensional context (1000s of attributes)
    -- Map allows flexible schema for any attributes
    context Map(String, String) DEFAULT map(),

    -- Context embedding (feature hashed: 1000+ → 256 dims)
    context_embedding Array(Float32) DEFAULT [],

    -- Rewards (multiple types)
    reward Float64 DEFAULT 0.0,
    converted UInt8 DEFAULT 0,
    engagement_score Float32 DEFAULT 0.0,
    revenue Float64 DEFAULT 0.0,
    cost Float64 DEFAULT 0.0,

    -- Metadata
    platform String DEFAULT '',
    campaign_id String DEFAULT '',
    decision_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',  -- Links to bandit_decisions

    -- Partitioning
    date Date MATERIALIZED toDate(timestamp)

) ENGINE = ReplicatedMergeTree
PARTITION BY (oid, toYYYYMM(timestamp))
ORDER BY (oid, org, xid, arm_id, timestamp)
TTL date + INTERVAL 90 DAY;

-- Distributed table for bandit_events
CREATE TABLE IF NOT EXISTS bandit_events ON CLUSTER tracker_cluster
AS bandit_events_local
ENGINE = Distributed(tracker_cluster, sfpla, bandit_events_local, rand());  -- Raw events retained 90 days

-- ====================================================================
-- ARM-LEVEL SKETCHES (Permanent storage with ClickHouse native sketches)
-- ====================================================================

CREATE TABLE bandit_arm_sketches_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    xid String DEFAULT '',
    arm_id String DEFAULT '',

    -- ClickHouse native sketch aggregates (SKIP DEFAULT - aggregate functions)
    reward_sketch AggregateFunction(quantileTDigest(0.1), Float64),
    users_sketch AggregateFunction(uniqCombined64, UUID),
    conversions_sketch AggregateFunction(uniqCombined64, UUID),

    -- Simple aggregates (SKIP DEFAULT - aggregate functions)
    total_reward SimpleAggregateFunction(sum, Float64),
    total_revenue SimpleAggregateFunction(sum, Float64),
    total_cost SimpleAggregateFunction(sum, Float64),
    event_count SimpleAggregateFunction(sum, UInt64),

    -- Timestamps
    first_event DateTime64(3) DEFAULT toDateTime64(0, 3),
    last_updated DateTime64(3) DEFAULT toDateTime64(0, 3)

) ENGINE = ReplicatedAggregatingMergeTree
PARTITION BY oid
ORDER BY (oid, org, xid, arm_id);

-- Distributed table for bandit_arm_sketches
CREATE TABLE IF NOT EXISTS bandit_arm_sketches ON CLUSTER tracker_cluster
AS bandit_arm_sketches_local
ENGINE = Distributed(tracker_cluster, sfpla, bandit_arm_sketches_local, rand());

-- Materialized view: Auto-aggregate events → arm sketches
CREATE MATERIALIZED VIEW IF NOT EXISTS bandit_arm_sketches_mv
TO bandit_arm_sketches
AS SELECT
    oid,
    xid,
    arm_id,

    -- ClickHouse native sketches (automatic merging)
    quantileTDigestState(0.1)(reward) as reward_sketch,
    uniqCombined64State(user_id) as users_sketch,
    uniqCombined64State(if(converted = 1, user_id, toUUID('00000000-0000-0000-0000-000000000000'))) as conversions_sketch,

    -- Aggregated metrics
    sum(reward) as total_reward,
    sum(revenue) as total_revenue,
    sum(cost) as total_cost,
    count() as event_count,

    -- Timestamps
    min(timestamp) as first_event,
    max(timestamp) as last_updated
FROM bandit_events
GROUP BY oid, xid, arm_id;

-- ====================================================================
-- ATTRIBUTE IMPORTANCE (From DataSketches frequent_strings_sketch)
-- ====================================================================

CREATE TABLE bandit_attribute_importance_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    xid String DEFAULT '',

    -- Attribute key (not key=value, just the key)
    attribute_key String DEFAULT '',

    -- Importance metrics
    importance_score Float64 DEFAULT 0.0,  -- Frequency from frequent_strings_sketch
    sample_count UInt64 DEFAULT 0,       -- Number of events with this attribute

    -- Computed by DataSketches Python library
    computed_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3)

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY oid
ORDER BY (oid, org, xid, importance_score DESC)
SETTINGS allow_experimental_reverse_key = 1;

-- Distributed table for bandit_attribute_importance
CREATE TABLE IF NOT EXISTS bandit_attribute_importance ON CLUSTER tracker_cluster
AS bandit_attribute_importance_local
ENGINE = Distributed(tracker_cluster, sfpla, bandit_attribute_importance_local, rand());

-- ====================================================================
-- SEGMENT-LEVEL SKETCHES (Top-K attributes only)
-- ====================================================================

CREATE TABLE bandit_segment_sketches_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    xid String DEFAULT '',
    arm_id String DEFAULT '',

    -- Segmentation dimensions (top-K important attributes only)
    attribute_key String DEFAULT '',
    attribute_value String DEFAULT '',

    -- Sketches per segment (SKIP DEFAULT - aggregate functions)
    reward_sketch AggregateFunction(quantileTDigest(0.1), Float64),
    users_sketch AggregateFunction(uniqCombined64, UUID),
    event_count SimpleAggregateFunction(sum, UInt64),

    last_updated DateTime64(3) DEFAULT toDateTime64(0, 3)

) ENGINE = ReplicatedAggregatingMergeTree
PARTITION BY oid
ORDER BY (oid, org, xid, arm_id, attribute_key, attribute_value);

-- Distributed table for bandit_segment_sketches
CREATE TABLE IF NOT EXISTS bandit_segment_sketches ON CLUSTER tracker_cluster
AS bandit_segment_sketches_local
ENGINE = Distributed(tracker_cluster, sfpla, bandit_segment_sketches_local, rand());

-- Materialized view: Auto-aggregate to segment sketches
-- Note: Tracks all attributes (filter by top-K in queries using bandit_attribute_importance)
CREATE MATERIALIZED VIEW IF NOT EXISTS bandit_segment_sketches_mv
TO bandit_segment_sketches
AS SELECT
    oid,
    xid,
    arm_id,
    attribute_key,
    attribute_value,

    quantileTDigestState(0.1)(reward) as reward_sketch,
    uniqCombined64State(user_id) as users_sketch,
    count() as event_count,
    max(timestamp) as last_updated
FROM bandit_events
ARRAY JOIN
    mapKeys(context) AS attribute_key,
    mapValues(context) AS attribute_value
GROUP BY oid, xid, arm_id, attribute_key, attribute_value;

-- To query top-K attributes only, use:
-- SELECT * FROM bandit_segment_sketches
-- WHERE attribute_key IN (SELECT attribute_key FROM bandit_attribute_importance WHERE ... ORDER BY importance_score DESC LIMIT 100)

-- ====================================================================
-- ARM WEIGHTS (For linear contextual bandit)
-- ====================================================================

CREATE TABLE bandit_arm_weights_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    xid String DEFAULT '',
    arm_id String DEFAULT '',

    -- Learned weights for context features
    -- Stored as Map for flexible feature sets
    weights Map(String, Float64) DEFAULT map(),
    confidence Map(String, Float64) DEFAULT map(),  -- Uncertainty per weight

    -- Training metadata
    n_observations UInt64 DEFAULT 0,
    training_algorithm String DEFAULT 'ridge_regression',

    -- Timestamps
    last_updated DateTime64(3) DEFAULT now64(3)

) ENGINE = ReplicatedReplacingMergeTree(last_updated)
PARTITION BY oid
ORDER BY (oid, org, xid, arm_id);

-- Distributed table for bandit_arm_weights
CREATE TABLE IF NOT EXISTS bandit_arm_weights ON CLUSTER tracker_cluster
AS bandit_arm_weights_local
ENGINE = Distributed(tracker_cluster, sfpla, bandit_arm_weights_local, rand());

-- ====================================================================
-- SEGMENT OVERLAP ANALYSIS (From theta_sketch)
-- ====================================================================

CREATE TABLE segment_overlap_analysis_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    analysis_id UUID DEFAULT generateUUIDv4(),

    -- Segments being compared
    segment_a_name String DEFAULT '',
    segment_b_name String DEFAULT '',
    segment_a_query String DEFAULT '',  -- SQL WHERE clause
    segment_b_query String DEFAULT '',

    -- Set operation results (from theta_sketch)
    segment_a_size UInt64 DEFAULT 0,
    segment_b_size UInt64 DEFAULT 0,
    intersection_size UInt64 DEFAULT 0,
    union_size UInt64 DEFAULT 0,
    a_only_size UInt64 DEFAULT 0,
    b_only_size UInt64 DEFAULT 0,
    overlap_percentage Float64 DEFAULT 0.0,

    -- Computed by DataSketches theta_sketch
    computed_at DateTime64(3) DEFAULT now64(3)

) ENGINE = ReplicatedMergeTree
PARTITION BY oid
ORDER BY (oid, org, computed_at DESC);

-- Distributed table for segment_overlap_analysis
CREATE TABLE IF NOT EXISTS segment_overlap_analysis ON CLUSTER tracker_cluster
AS segment_overlap_analysis_local
ENGINE = Distributed(tracker_cluster, sfpla, segment_overlap_analysis_local, rand());

-- ====================================================================
-- INDEXES FOR PERFORMANCE
-- ====================================================================

-- Index for attribute importance queries
ALTER TABLE bandit_attribute_importance_local ADD INDEX IF NOT EXISTS
    attr_key_idx attribute_key TYPE bloom_filter;

-- Index for segment queries
ALTER TABLE bandit_segment_sketches_local ADD INDEX IF NOT EXISTS
    segment_attr_idx (attribute_key, attribute_value) TYPE bloom_filter;

-- ====================================================================
-- HELPER VIEWS
-- ====================================================================

-- View for querying arm performance with sketch statistics
CREATE OR REPLACE VIEW bandit_arm_performance AS
SELECT
    oid,
    xid,
    arm_id,

    -- Extract sketch values
    quantileTDigestMerge(0.5)(reward_sketch) as median_reward,
    quantileTDigestMerge(0.95)(reward_sketch) as p95_reward,
    uniqCombined64Merge(users_sketch) as unique_users,
    uniqCombined64Merge(conversions_sketch) as unique_conversions,

    -- Computed metrics
    uniqCombined64Merge(conversions_sketch) / uniqCombined64Merge(users_sketch) as conversion_rate,
    total_revenue / event_count as avg_revenue_per_event,
    total_cost / uniqCombined64Merge(conversions_sketch) as cost_per_acquisition,

    -- Aggregates
    total_reward,
    total_revenue,
    total_cost,
    event_count,

    -- Timestamps
    first_event,
    last_updated
FROM bandit_arm_sketches
GROUP BY
    oid,
    xid,
    arm_id,
    total_reward,
    total_revenue,
    total_cost,
    event_count,
    first_event,
    last_updated;
