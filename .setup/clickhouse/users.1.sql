-- Users + LTV - Organizations, users, lifetime value, files, payment auth
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

-- Payments table - Stores individual payment/transaction line items
CREATE TABLE payments_local ON CLUSTER tracker_cluster (

    id UUID,                      -- Payment/line item ID - unique identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                     -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                     -- Thread ID - reference to message thread
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                     -- User ID - customer who made the payment
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                     -- Visitor ID - anonymous identifier
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                     -- Session ID - session when payment was made
    invid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                   -- Invoice ID - reference to invoice
    orid UUID DEFAULT '00000000-0000-0000-0000-000000000000',                    -- Order ID - unique identifier for the order
    invoiced_at DateTime64(3) DEFAULT now64(3),   -- Invoice date - when invoice was issued
    product String DEFAULT '',               -- Product name - what was purchased
    product_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',              -- Product identifier - reference to product catalog
    pcat String DEFAULT '',                  -- Product category - for grouping/reporting
    man String DEFAULT '',                   -- Manufacturer - product manufacturer
    model String DEFAULT '',                 -- Model - product model/variant
    qty Float64 DEFAULT 0.0,                  -- Quantity - number of units purchased
    duration Int64 DEFAULT 0,               -- Duration - for subscription products (in days/months)
    starts DateTime64(3) DEFAULT now64(3),         -- Start date - when service/subscription begins
    ends DateTime64(3) DEFAULT now64(3),           -- End date - when service/subscription ends
    price Float64 DEFAULT 0.0,                -- Unit price - price per unit
    discount Float64 DEFAULT 0.0,             -- Discount amount - total discount applied
    revenue Float64 DEFAULT 0.0,              -- Revenue - actual revenue after discounts
    margin Float64 DEFAULT 0.0,               -- Margin - profit margin
    cost Float64 DEFAULT 0.0,                 -- Cost - cost of goods sold
    tax Float64 DEFAULT 0.0,                  -- Tax amount - total tax charged
    tax_rate Float64 DEFAULT 0.0,             -- Tax rate - percentage tax rate applied
    commission Float64 DEFAULT 0.0,           -- Commission - sales commission amount
    referral Float64 DEFAULT 0.0,             -- Referral fee - affiliate/referral payout
    fees Float64 DEFAULT 0.0,                 -- Fees - processing or other fees
    subtotal Float64 DEFAULT 0.0,             -- Subtotal - before tax and fees
    total Float64 DEFAULT 0.0,                -- Total - final amount charged
    payment Float64 DEFAULT 0.0,              -- Payment amount - actual amount paid
    currency String DEFAULT 'USD',              -- Currency code - ISO currency code (USD, EUR, etc.)
    country String DEFAULT '',               -- Country - where payment was made
    rcode String DEFAULT '',                 -- Region code - region/state code
    region String DEFAULT '',                -- Region name - region/state name
    campaign_id String DEFAULT '',             -- Campaign ID - marketing campaign attribution
    paid_at DateTime64(3) DEFAULT now64(3),        -- Payment date - when payment was received
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp

    -- Projection for fast invoice line item lookups
    PROJECTION invid_proj
    (
        SELECT _part_offset ORDER BY invid
    )


) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (toYYYYMM(created_at), oid)
ORDER BY (oid, org, id)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- Distributed table for payments
CREATE TABLE IF NOT EXISTS payments ON CLUSTER tracker_cluster
AS payments_local
ENGINE = Distributed(tracker_cluster, sfpla, payments_local, rand());

-- Invoice totals view - Aggregates line items to invoice level
-- Use this view to get complete invoice summaries without manually aggregating
CREATE VIEW invoice_totals ON CLUSTER tracker_cluster AS
SELECT
    oid,
    org,
    invid,
    tid, -- Thread ID from first line item
    any(invoiced_at) as invoiced_at, -- Invoice creation time (same across line items)
    any(paid_at) as paid_at, -- Payment time (same across line items)
    any(currency) as currency, -- Currency (same across line items)
    any(country) as country, -- Country (same across line items)
    any(campaign_id) as campaign_id, -- Campaign (same across line items)
    count() as line_item_count, -- Number of products in invoice
    SUM(qty) as total_qty, -- Total quantity across all line items
    SUM(price * qty) as total_list_price, -- Total list price before discounts
    SUM(discount) as total_discount, -- Total discount amount
    SUM(subtotal) as invoice_subtotal, -- Invoice subtotal (sum of line items before tax)
    SUM(tax) as invoice_tax, -- Total tax across all line items
    SUM(total) as invoice_total, -- Total invoice amount including tax
    SUM(revenue) as total_revenue, -- Total revenue (amount hitting bank)
    SUM(margin) as total_margin, -- Total profit margin
    SUM(cost) as total_cost, -- Total costs
    any(commission) as commission, -- Commission (typically same across line items)
    any(referral) as referral, -- Referral fee (typically same across line items)
    any(fees) as fees, -- Service fees (typically same across line items)
    groupArray(product) as products, -- Array of all products in invoice
    groupArray(product_id) as product_ids, -- Array of all product IDs
    min(created_at) as created_at, -- When first line item was created
    max(updated_at) as updated_at -- When last line item was updated
FROM payments
GROUP BY oid, org, invid, tid;

-- Revenue by campaign by month
-- NOTE: payments table contains LINE ITEMS (multiple rows per invoice)
-- This view aggregates at line item level - use invoice_totals view for invoice-level aggregation
CREATE OR REPLACE VIEW campaign_revenue_monthly AS
SELECT
    oid,
    campaign_id,
    toYYYYMM(created_at) as month,
    sum(revenue) as total_revenue, -- Revenue across all line items
    count() as line_item_count, -- Count of line items (NOT invoice count)
    uniq(invid) as invoice_count, -- Actual number of invoices
    avg(revenue) as avg_line_item_value, -- Average revenue per line item
    max(created_at) as latest_purchase
FROM payments
WHERE campaign_id IS NOT NULL AND campaign_id != ''
GROUP BY oid, campaign_id, month;

-- Materialized view for looking up payments by user - Enables efficient user payment history
CREATE MATERIALIZED VIEW payments_by_user ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (uid, created_at)
POPULATE AS
SELECT * FROM payments
WHERE uid IS NOT NULL;

-- Materialized view for looking up payments by campaign - Enables campaign revenue tracking
CREATE MATERIALIZED VIEW payments_by_campaign ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (campaign_id, created_at)
POPULATE AS
SELECT * FROM payments
WHERE campaign_id IS NOT NULL;

-- Materialized view for looking up payments by organization - Enables multi-tenant payment queries
CREATE MATERIALIZED VIEW payments_by_org ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (oid, org, created_at)
POPULATE AS
SELECT * FROM payments;

-- LTV table - Aggregated lifetime value by any identifier type
-- Generic design: multiple rows per entity (one per id_type)
-- Note: Payment details are stored in the payments table, not here
-- SummingMergeTree automatically sums the 'paid' column for matching keys
CREATE TABLE ltv_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifier for the site/app
    id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Generic identifier - can be uid, vid, sid, orid, etc.
    id_type LowCardinality(String) DEFAULT '', -- Type of identifier: 'uid', 'vid', 'sid', 'orid'
    paid Float64 DEFAULT 0.0, -- Total amount paid - cumulative lifetime value for this id (auto-summed)
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this record
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this record

) ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id, id_type);

-- Distributed table for ltv
CREATE TABLE IF NOT EXISTS ltv ON CLUSTER tracker_cluster
AS ltv_local
ENGINE = Distributed(tracker_cluster, sfpla, ltv_local, rand());

-- Materialized view: ltv by user ID - Fast queries for user lifetime value
CREATE MATERIALIZED VIEW ltv_by_uid ON CLUSTER tracker_cluster
ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id)
POPULATE AS
SELECT * FROM ltv
WHERE id_type = 'uid';

-- Materialized view: ltv by visitor ID - Fast queries for visitor lifetime value
CREATE MATERIALIZED VIEW ltv_by_vid ON CLUSTER tracker_cluster
ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id)
POPULATE AS
SELECT * FROM ltv
WHERE id_type = 'vid';

-- Materialized view: ltv by order ID - Fast queries for order lifetime value
CREATE MATERIALIZED VIEW ltv_by_orid ON CLUSTER tracker_cluster
ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id)
POPULATE AS
SELECT * FROM ltv
WHERE id_type = 'orid';

-- Additional schema from schema.4.cql

-- Version update
INSERT INTO sequences (name, seq) VALUES ('MSGXC_VER', 2);
INSERT INTO sequences (name, seq) VALUES ('VISITOR_TRACKING_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('MARKETING_EXTENDED_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('ABZ_TESTING_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('HIERARCHICAL_OPT_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('PMF_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('CREATOR_ECONOMY_VER', 1);
INSERT INTO sequences (name, seq) VALUES ('SFPL_VER', 1);

-- Userhosts table - Maps relationships between users, visitors, and host sites
CREATE TABLE userhosts_local ON CLUSTER tracker_cluster (

    hhash String DEFAULT '', -- Host hash - identifier for the site/app
    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - authenticated user identifier
    vid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Visitor ID - anonymous tracking identifier
    sid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Session ID - for the current/last session
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY hhash
ORDER BY (hhash, uid, vid);

-- Distributed table for userhosts
CREATE TABLE IF NOT EXISTS userhosts ON CLUSTER tracker_cluster
AS userhosts_local
ENGINE = Distributed(tracker_cluster, sfpla, userhosts_local, rand());

-- Address type as a structured format - Stores physical address information
CREATE TABLE addresses_local ON CLUSTER tracker_cluster (

    id UUID DEFAULT generateUUIDv4(), -- Address ID - unique identifier
    fullname String DEFAULT '', -- Full name - recipient name
    st1 String DEFAULT '', -- Street line 1 - primary address line
    st2 String DEFAULT '', -- Street line 2 - secondary address line (apt, suite, etc.)
    city String DEFAULT '', -- City name
    province String DEFAULT '', -- Province/state/region
    country String DEFAULT '', -- Country code (ISO format)
    zip String DEFAULT '', -- ZIP/postal code
    active Boolean DEFAULT false, -- Active flag - whether this address is currently valid
    type String DEFAULT '', -- Address type (e.g., "billing", "shipping", "home", "work")
    phone String DEFAULT '', -- Contact phone number for this address
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Distributed table for addresses
CREATE TABLE IF NOT EXISTS addresses ON CLUSTER tracker_cluster
AS addresses_local
ENGINE = Distributed(tracker_cluster, sfpla, addresses_local, rand());

-- Organizations table - Stores organization information and hierarchical relationships
CREATE TABLE orgs_local ON CLUSTER tracker_cluster (

    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - unique identifier
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    parent UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Parent organization ID - for hierarchical structure
    root UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Root organization ID - top-level parent in hierarchy
    lname String DEFAULT '', -- Legal name - official registered name
    hname String DEFAULT '', -- Human-friendly display name
    notes String DEFAULT '', -- Administrative notes about this organization
    roles Array(String) DEFAULT [], -- Roles defined within this organization
    rights Array(String) DEFAULT [], -- Rights/permissions defined within this organization
    etype String DEFAULT '', -- Entity type (e.g., "company", "nonprofit", "government")
    country String DEFAULT '', -- Primary country of operation
    lang String DEFAULT '', -- Primary language/locale
    hq_id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Headquarters address ID - reference to addresses table
    addresses JSON DEFAULT '{}', -- All registered addresses for this organization
    host String DEFAULT '', -- Primary host domain
    hhash String DEFAULT '', -- Host hash - for efficient lookups
    taxid String DEFAULT '', -- Tax identification number
    terms_accepted DateTime64(3) DEFAULT toDateTime64(0, 3), -- When terms of service were accepted
    mcerts JSON DEFAULT '{}', -- Marketing certifications/compliance records
    expiry Date DEFAULT today(), -- Expiration date for organization account
    email String DEFAULT '', -- Primary contact email
    phone String DEFAULT '', -- Primary contact phone number
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this organization
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this record

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (oid, org);

-- Distributed table for orgs
CREATE TABLE IF NOT EXISTS orgs ON CLUSTER tracker_cluster
AS orgs_local
ENGINE = Distributed(tracker_cluster, sfpla, orgs_local, rand());

-- Create a materialized view for conglomerate (orgs by root) - Enables efficient querying of organization hierarchies
CREATE MATERIALIZED VIEW orgs_conglomerate
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (root, oid)
POPULATE AS
SELECT * FROM orgs
WHERE root IS NOT NULL;

-- Enhanced Users table - Comprehensive user information with authentication and profile data
CREATE TABLE users_local ON CLUSTER tracker_cluster (

    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - unique identifier
    username String DEFAULT '', -- Username - unique login identifier
    pwd String DEFAULT '', -- Password hash - stored securely
    uhash String DEFAULT '', -- Username hash - for efficient lookups
    email String DEFAULT '', -- Email address - for communications and recovery
    ehash String DEFAULT '', -- Email hash - for efficient lookups
    vids Array(UUID) DEFAULT [], -- Visitor IDs - associated anonymous sessions
    roles Array(String) DEFAULT [], -- User roles - for permission management
    rights Array(String) DEFAULT [], -- Direct rights/permissions assigned to user
    ref UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Referrer user ID - who invited/referred this user
    aff String DEFAULT '', -- Affiliate code - for tracking marketing attribution
    promo String DEFAULT '', -- Promotion code - special offers applied
    origin_url String DEFAULT '', -- Origin URL - where user first registered
    ip String DEFAULT '', -- IP address - at registration time
    ips Array(String) DEFAULT [], -- IP history - all IPs this user has connected from
    params JSON DEFAULT '{}', -- Additional parameters - customizable user attributes
    cohorts Array(String) DEFAULT [], -- Cohort assignments - for segmentation
    splits JSON DEFAULT '{}', -- A/B test assignments - experiment participations
    lang String DEFAULT '', -- Language preference
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - primary organization affiliation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this user
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this user
    -- Additional fields from schema.4
    mdevices JSON DEFAULT '{}', -- Message devices - registered notification endpoints
    mtypes Array(String) DEFAULT [], -- Message types - what kinds of messages user accepts
    mlast Int64 DEFAULT 0, -- Message last - timestamp of last message received
    cell String DEFAULT '', -- Cell phone number - for SMS/text messaging
    additional_emails JSON DEFAULT '{}', -- Additional emails - {"email@example.com": {"created_at": "2024-01-01", "verified_at": "2024-01-02"}}
    additional_cells JSON DEFAULT '{}', -- Additional cell numbers - {"+1234567890": {"created_at": "2024-01-01", "verified_at": "2024-01-02"}}
    chash String DEFAULT '', -- Cell hash - for efficient lookups
    mcerts JSON DEFAULT '{}', -- Marketing certifications - consent records
    -- Experimental schema fields
    lat Float64 DEFAULT 0.0, -- Latitude - geographical location
    lon Float64 DEFAULT 0.0, -- Longitude - geographical location
    caption String DEFAULT '', -- Profile caption/bio
    gender Boolean DEFAULT false, -- Gender flag - if tracked for demographic purposes
    dob Date DEFAULT today(), -- Date of birth - for age verification
    image_url String DEFAULT '', -- Profile image URL - processed/optimized version
    image_url_original String DEFAULT '', -- Original profile image URL - pre-processing
    v_score Decimal64(4) DEFAULT 0.0, -- Verification score - confidence level in identity
    fn String DEFAULT '', -- First name
    ln String DEFAULT '', -- Last name
    locked DateTime64(3) DEFAULT toDateTime64(0, 3), -- Account lock timestamp - if account is locked
    last_active DateTime64(3) DEFAULT toDateTime64(0, 3), -- Last active timestamp
    magic String DEFAULT '', -- Magic link token - for passwordless authentication
    magic_exp DateTime64(3) DEFAULT toDateTime64(0, 3), -- Magic link expiration
    magic_attempts Int32 DEFAULT 0, -- Magic link attempts - for rate limiting
    magic_attempted DateTime64(3) DEFAULT toDateTime64(0, 3), -- Last magic link attempt timestamp
    terms_accepted DateTime64(3) DEFAULT toDateTime64(0, 3), -- When terms of service were accepted
    perms JSON DEFAULT '{}', -- Permissions - detailed permission configuration

    -- Use cases and user attributes for targeting/matching
    use_cases Array(String) DEFAULT [], -- User use cases for this product (e.g., ["email_marketing", "analytics"])
    use_case_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of use cases (384-dim)
    user_attribs JSON DEFAULT '{}', -- User attributes for clustering and matching (JSON format)
    user_attribs_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of user attributes (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) DEFAULT toDateTime64(0, 3) -- When embeddings were generated

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY uid;

-- Distributed table for users
CREATE TABLE IF NOT EXISTS users ON CLUSTER tracker_cluster
AS users_local
ENGINE = Distributed(tracker_cluster, sfpla, users_local, rand());

-- Note: Old simple user lookup views removed - replaced by comprehensive views at end of schema
-- (user_by_email, user_by_username, user_by_cell superseded by user_by_all_emails, user_by_username, user_by_all_cells)

-- Materialized view for finding users in specific cohorts
CREATE MATERIALIZED VIEW user_by_cohorts ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY cohorts
POPULATE AS
SELECT uid, cohorts FROM users
ARRAY JOIN cohorts;

-- Note: Duplicate user_by_all_emails and user_by_all_cells views removed - final versions at end of schema

-- User verifications table - Tracks identity verification methods and statuses
CREATE TABLE user_verifications_local ON CLUSTER tracker_cluster (

    uid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- User ID - whose identity is being verified
    vmethod String DEFAULT '', -- Verification method (e.g., "email", "phone", "id", "document")
    verifier UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Verifier user ID - who performed the verification
    created_at DateTime64(3) DEFAULT now64(3), -- Verification timestamp - when verification occurred
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - which oid this verification is for
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who initiated this verification
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this verification

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (uid, vmethod);

-- Distributed table for user_verifications
CREATE TABLE IF NOT EXISTS user_verifications ON CLUSTER tracker_cluster
AS user_verifications_local
ENGINE = Distributed(tracker_cluster, sfpla, user_verifications_local, rand());

-- Enhanced Queues table - Advanced task management system with additional metadata
CREATE TABLE files_local ON CLUSTER tracker_cluster (

    slug String DEFAULT '', -- Slug - unique URL-friendly identifier
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    fprint String DEFAULT '', -- Fingerprint - hash of file contents (SHA256)
    ftype String DEFAULT '', -- File type - category (v=video, i=image, d=document)
    suffix String DEFAULT '', -- Suffix - file extension (jpg, jpeg, doc, etc.)
    name String DEFAULT '', -- Name - optimized/reconstructed filename
    mime String DEFAULT '', -- MIME type - content type for browsers
    description String DEFAULT '', -- Description - notes entered at upload
    lat Float64 DEFAULT 0.0, -- Latitude - geographic location
    lon Float64 DEFAULT 0.0, -- Longitude - geographic location
    oname String DEFAULT '', -- Original name - original filename
    ocreated DateTime64(3) DEFAULT toDateTime64(0, 3), -- Original created date - from file metadata
    omime String DEFAULT '', -- Original MIME type - before processing
    alt String DEFAULT '', -- Alt text - for accessibility
    meta JSON DEFAULT '{}', -- Metadata - additional file information
    x Int32 DEFAULT 0, -- Width - in pixels for images/videos
    y Int32 DEFAULT 0, -- Height - in pixels for images/videos
    size Int32 DEFAULT 0, -- Size - file size in bytes
    private Boolean DEFAULT false, -- Private flag - requires authentication
    bucket String DEFAULT '', -- Storage bucket - where file is stored
    url_prefix String DEFAULT '', -- URL prefix - for proxy access (private/public)
    ourl String DEFAULT '', -- Original URL - source URL if imported
    hqurl String DEFAULT '', -- High quality URL - high resolution version
    mqurl String DEFAULT '', -- Medium quality URL - medium resolution version
    lqurl String DEFAULT '', -- Low quality URL - low resolution version
    thumb String DEFAULT '', -- Thumbnail URL - preview image
    used_in Array(String) DEFAULT [], -- Usage references - where file is used
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who uploaded the file
    o_r Boolean DEFAULT false, -- Owner read permission
    o_w Boolean DEFAULT false, -- Owner write permission
    orgs Array(UUID) DEFAULT [], -- Organization IDs - associated organizations
    g_r Boolean DEFAULT false, -- Group/oid read permission
    g_w Boolean DEFAULT false, -- Group/oid write permission
    roles Array(String) DEFAULT [], -- Role permissions - which roles can access
    r_r Boolean DEFAULT false, -- Role read permission
    r_w Boolean DEFAULT false, -- Role write permission
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    expires DateTime64(3) DEFAULT toDateTime64(0, 3), -- Expiration timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000' -- Updater user ID - who last modified this file

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY slug;

-- Distributed table for files
CREATE TABLE IF NOT EXISTS files ON CLUSTER tracker_cluster
AS files_local
ENGINE = Distributed(tracker_cluster, sfpla, files_local, rand());

-- Payment Provider table - Stores information about payment service providers
CREATE TABLE pprovider_local ON CLUSTER tracker_cluster (

    name String DEFAULT '', -- Provider name - unique identifier (e.g., "stripe", "paypal")
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who created this provider record
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this provider

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY name;

-- Distributed table for pprovider
CREATE TABLE IF NOT EXISTS pprovider ON CLUSTER tracker_cluster
AS pprovider_local
ENGINE = Distributed(tracker_cluster, sfpla, pprovider_local, rand());

-- Materialized view for looking up payment providers by owner - Enables efficient provider management
CREATE MATERIALIZED VIEW pprovider_by_owner
ENGINE = ReplicatedReplacingMergeTree
ORDER BY owner
POPULATE AS
SELECT * FROM pprovider;

-- Payment Authorization table - Stores user payment method information
CREATE TABLE pauth_local ON CLUSTER tracker_cluster (

    id UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Authorization ID - unique identifier for this payment method
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    pprovider String DEFAULT '', -- Payment provider name (references pprovider.name)
    pcustomer String DEFAULT '', -- Customer ID in the payment provider's system
    psource String DEFAULT '', -- Payment source identifier (e.g., card ID, bank account token)
    meta String DEFAULT '', -- Metadata about the payment method in JSON format
    priority Int32 DEFAULT 0, -- Priority order for multiple payment methods
    active_since DateTime64(3) DEFAULT toDateTime64(0, 3), -- Timestamp when this payment method became active
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who owns this payment method
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this payment method

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Distributed table for pauth
CREATE TABLE IF NOT EXISTS pauth ON CLUSTER tracker_cluster
AS pauth_local
ENGINE = Distributed(tracker_cluster, sfpla, pauth_local, rand());

-- Materialized view for looking up payment methods by owner - Enables efficient user payment management
CREATE MATERIALIZED VIEW pauth_by_owner
ENGINE = ReplicatedReplacingMergeTree
ORDER BY owner
POPULATE AS
SELECT * FROM pauth;

-- Payment Confirmation table - Stores payment event confirmations
CREATE TABLE pconfirmation_local ON CLUSTER tracker_cluster (

    id String DEFAULT '', -- Event ID - unique identifier for this payment event
    oid UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    ctype String DEFAULT '', -- Confirmation type (e.g., "charge", "refund", "subscription")
    external_ref String DEFAULT '', -- External reference ID - typically a transaction ID from payment provider
    rtype String DEFAULT '', -- Reference type - describes what the ref field references
    meta String DEFAULT '', -- Metadata about the payment event in JSON format
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Owner user ID - who owns this payment confirmation
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID DEFAULT '00000000-0000-0000-0000-000000000000', -- Updater user ID - who last modified this confirmation

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Distributed table for pconfirmation
CREATE TABLE IF NOT EXISTS pconfirmation ON CLUSTER tracker_cluster
AS pconfirmation_local
ENGINE = Distributed(tracker_cluster, sfpla, pconfirmation_local, rand());

-- Permissions table - Stores access control permissions for resources
CREATE MATERIALIZED VIEW user_by_username ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY username
POPULATE AS
SELECT uid, username FROM users WHERE username IS NOT NULL AND username != '';

-- Materialized view for all emails (primary + additional) lookups
CREATE MATERIALIZED VIEW user_by_all_emails ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY email_address
POPULATE AS
SELECT 
    uid, 
    email AS email_address,
    'primary' AS email_type,
    created_at,
    NULL AS verified_at
FROM users 
WHERE email IS NOT NULL AND email != ''
UNION ALL
SELECT 
    uid,
    email_key AS email_address,
    'additional' AS email_type,
    parseDateTimeBestEffort(JSONExtractString(toString(additional_emails), email_key, 'created_at')) AS created_at,
    parseDateTimeBestEffort(JSONExtractString(toString(additional_emails), email_key, 'verified_at')) AS verified_at
FROM users 
ARRAY JOIN JSONExtractKeys(toString(additional_emails)) AS email_key
WHERE additional_emails IS NOT NULL;

-- Materialized view for all cell numbers (primary + additional) lookups
CREATE MATERIALIZED VIEW user_by_all_cells ON CLUSTER tracker_cluster
ENGINE = ReplicatedReplacingMergeTree
ORDER BY cell_number
POPULATE AS
SELECT 
    uid, 
    cell AS cell_number,
    'primary' AS cell_type,
    created_at,
    NULL AS verified_at
FROM users 
WHERE cell IS NOT NULL AND cell != ''
UNION ALL
SELECT 
    uid,
    cell_key AS cell_number,
    'additional' AS cell_type,
    parseDateTimeBestEffort(JSONExtractString(toString(additional_cells), cell_key, 'created_at')) AS created_at,
    parseDateTimeBestEffort(JSONExtractString(toString(additional_cells), cell_key, 'verified_at')) AS verified_at
FROM users 
ARRAY JOIN JSONExtractKeys(toString(additional_cells)) AS cell_key
WHERE additional_cells IS NOT NULL;



-- Budget Guardian Migration
-- Add budget_limits table for enterprise budget monitoring

-- Budget Limits table - Stores budget limits and thresholds for organizations and campaigns
