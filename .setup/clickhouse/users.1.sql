-- Users + LTV - Organizations, users, lifetime value, files, payment auth
-- Part of the unified ClickHouse schema
SET enable_json_type = 1;
USE sfpla;

-- Payments table - Stores individual payment/transaction line items
CREATE TABLE IF NOT EXISTS payments ON CLUSTER my_cluster (
    id UUID,                      -- Payment/line item ID - unique identifier
    oid UUID,                     -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID,                     -- Thread ID - reference to message thread
    uid UUID,                     -- User ID - customer who made the payment
    vid UUID,                     -- Visitor ID - anonymous identifier
    sid UUID,                     -- Session ID - session when payment was made
    invid UUID,                   -- Invoice ID - reference to invoice
    orid UUID,                    -- Order ID - unique identifier for the order
    invoiced_at DateTime64(3),   -- Invoice date - when invoice was issued
    product String,               -- Product name - what was purchased
    product_id UUID,              -- Product identifier - reference to product catalog
    pcat String,                  -- Product category - for grouping/reporting
    man String,                   -- Manufacturer - product manufacturer
    model String,                 -- Model - product model/variant
    qty Float64,                  -- Quantity - number of units purchased
    duration Int32,               -- Duration - for subscription products (in days/months)
    starts DateTime64(3),         -- Start date - when service/subscription begins
    ends DateTime64(3),           -- End date - when service/subscription ends
    price Float64,                -- Unit price - price per unit
    discount Float64,             -- Discount amount - total discount applied
    revenue Float64,              -- Revenue - actual revenue after discounts
    margin Float64,               -- Margin - profit margin
    cost Float64,                 -- Cost - cost of goods sold
    tax Float64,                  -- Tax amount - total tax charged
    tax_rate Float64,             -- Tax rate - percentage tax rate applied
    commission Float64,           -- Commission - sales commission amount
    referral Float64,             -- Referral fee - affiliate/referral payout
    fees Float64,                 -- Fees - processing or other fees
    subtotal Float64,             -- Subtotal - before tax and fees
    total Float64,                -- Total - final amount charged
    payment Float64,              -- Payment amount - actual amount paid
    currency String,              -- Currency code - ISO currency code (USD, EUR, etc.)
    country String,               -- Country - where payment was made
    rcode String,                 -- Region code - region/state code
    region String,                -- Region name - region/state name
    campaign_id UUID,             -- Campaign ID - marketing campaign attribution
    paid_at DateTime64(3),        -- Payment date - when payment was received
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

-- Invoice totals view - Aggregates line items to invoice level
-- Use this view to get complete invoice summaries without manually aggregating
CREATE VIEW invoice_totals AS
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
WHERE campaign_id IS NOT NULL AND campaign_id != '00000000-0000-0000-0000-000000000000'
GROUP BY oid, campaign_id, month;

-- Materialized view for looking up payments by user - Enables efficient user payment history
CREATE MATERIALIZED VIEW payments_by_user
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (uid, created_at)
POPULATE AS
SELECT * FROM payments
WHERE uid IS NOT NULL;

-- Materialized view for looking up payments by campaign - Enables campaign revenue tracking
CREATE MATERIALIZED VIEW payments_by_campaign
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (campaign_id, created_at)
POPULATE AS
SELECT * FROM payments
WHERE campaign_id IS NOT NULL;

-- Materialized view for looking up payments by organization - Enables multi-tenant payment queries
CREATE MATERIALIZED VIEW payments_by_org
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (oid, org, created_at)
POPULATE AS
SELECT * FROM payments;

-- LTV table - Aggregated lifetime value by any identifier type
-- Generic design: multiple rows per entity (one per id_type)
-- Note: Payment details are stored in the payments table, not here
-- SummingMergeTree automatically sums the 'paid' column for matching keys
CREATE TABLE ltv ON CLUSTER my_cluster (
    hhash String, -- Host hash - identifier for the site/app
    id UUID, -- Generic identifier - can be uid, vid, sid, orid, etc.
    id_type LowCardinality(String), -- Type of identifier: 'uid', 'vid', 'sid', 'orid'
    paid Float64, -- Total amount paid - cumulative lifetime value for this id (auto-summed)
    oid UUID, -- Organization ID - which oid this record belongs to
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this record
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    owner UUID, -- Owner user ID - who created this record
) ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id, id_type);

-- Materialized view: ltv by user ID - Fast queries for user lifetime value
CREATE MATERIALIZED VIEW ltv_by_uid
ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id)
POPULATE AS
SELECT * FROM ltv
WHERE id_type = 'uid';

-- Materialized view: ltv by visitor ID - Fast queries for visitor lifetime value
CREATE MATERIALIZED VIEW ltv_by_vid
ENGINE = ReplicatedSummingMergeTree(paid)
ORDER BY (hhash, id)
POPULATE AS
SELECT * FROM ltv
WHERE id_type = 'vid';

-- Materialized view: ltv by order ID - Fast queries for order lifetime value
CREATE MATERIALIZED VIEW ltv_by_orid
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
CREATE TABLE userhosts ON CLUSTER my_cluster (
    hhash String, -- Host hash - identifier for the site/app
    uid UUID, -- User ID - authenticated user identifier
    vid UUID, -- Visitor ID - anonymous tracking identifier
    sid UUID, -- Session ID - for the current/last session
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY hhash
ORDER BY (hhash, uid, vid);

-- Address type as a structured format - Stores physical address information
CREATE TABLE addresses ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Address ID - unique identifier
    fullname String, -- Full name - recipient name
    st1 String, -- Street line 1 - primary address line
    st2 String, -- Street line 2 - secondary address line (apt, suite, etc.)
    city String, -- City name
    province String, -- Province/state/region
    country String, -- Country code (ISO format)
    zip String, -- ZIP/postal code
    active Boolean, -- Active flag - whether this address is currently valid
    type String, -- Address type (e.g., "billing", "shipping", "home", "work")
    phone String, -- Contact phone number for this address
    updated_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Organizations table - Stores organization information and hierarchical relationships
CREATE TABLE orgs ON CLUSTER my_cluster (
    oid UUID, -- Organization ID - unique identifier
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    parent UUID, -- Parent organization ID - for hierarchical structure
    root UUID, -- Root organization ID - top-level parent in hierarchy
    lname String, -- Legal name - official registered name
    hname String, -- Human-friendly display name
    notes String, -- Administrative notes about this organization
    roles Array(String), -- Roles defined within this organization
    rights Array(String), -- Rights/permissions defined within this organization
    etype String, -- Entity type (e.g., "company", "nonprofit", "government")
    country String, -- Primary country of operation
    lang String, -- Primary language/locale
    hq_id UUID, -- Headquarters address ID - reference to addresses table
    addresses JSON, -- All registered addresses for this organization
    host String, -- Primary host domain
    hhash String, -- Host hash - for efficient lookups
    taxid String, -- Tax identification number
    terms_accepted DateTime64(3), -- When terms of service were accepted
    mcerts JSON, -- Marketing certifications/compliance records
    expiry Date, -- Expiration date for organization account
    email String, -- Primary contact email
    phone String, -- Primary contact phone number
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    owner UUID, -- Owner user ID - who created this organization
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this record
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (oid, org);

-- Create a materialized view for conglomerate (orgs by root) - Enables efficient querying of organization hierarchies
CREATE MATERIALIZED VIEW orgs_conglomerate
ENGINE = ReplicatedReplacingMergeTree
ORDER BY (root, oid)
POPULATE AS
SELECT * FROM orgs
WHERE root IS NOT NULL;

-- Enhanced Users table - Comprehensive user information with authentication and profile data
CREATE TABLE users ON CLUSTER my_cluster (
    uid UUID, -- User ID - unique identifier
    username String, -- Username - unique login identifier
    pwd String, -- Password hash - stored securely
    uhash String, -- Username hash - for efficient lookups
    email String, -- Email address - for communications and recovery
    ehash String, -- Email hash - for efficient lookups
    vids Array(UUID), -- Visitor IDs - associated anonymous sessions
    roles Array(String), -- User roles - for permission management
    rights Array(String), -- Direct rights/permissions assigned to user
    ref UUID, -- Referrer user ID - who invited/referred this user
    aff String, -- Affiliate code - for tracking marketing attribution
    promo String, -- Promotion code - special offers applied
    origin_url String, -- Origin URL - where user first registered
    ip String, -- IP address - at registration time
    ips Array(String), -- IP history - all IPs this user has connected from
    params JSON, -- Additional parameters - customizable user attributes
    cohorts Array(String), -- Cohort assignments - for segmentation
    splits JSON, -- A/B test assignments - experiment participations
    lang String, -- Language preference
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    oid UUID, -- Organization ID - primary organization affiliation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who created this user
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this user
    -- Additional fields from schema.4
    mdevices JSON, -- Message devices - registered notification endpoints
    mtypes Array(String), -- Message types - what kinds of messages user accepts
    mlast Int64, -- Message last - timestamp of last message received
    cell String, -- Cell phone number - for SMS/text messaging
    additional_emails JSON, -- Additional emails - {"email@example.com": {"created_at": "2024-01-01", "verified_at": "2024-01-02"}}
    additional_cells JSON, -- Additional cell numbers - {"+1234567890": {"created_at": "2024-01-01", "verified_at": "2024-01-02"}}
    chash String, -- Cell hash - for efficient lookups
    mcerts JSON, -- Marketing certifications - consent records
    -- Experimental schema fields
    lat Float64, -- Latitude - geographical location
    lon Float64, -- Longitude - geographical location
    caption String, -- Profile caption/bio
    gender Boolean, -- Gender flag - if tracked for demographic purposes
    dob Date, -- Date of birth - for age verification
    image_url String, -- Profile image URL - processed/optimized version
    image_url_original String, -- Original profile image URL - pre-processing
    v_score Decimal64(4), -- Verification score - confidence level in identity
    fn String, -- First name
    ln String, -- Last name
    locked DateTime64(3), -- Account lock timestamp - if account is locked
    active DateTime64(3), -- Last active timestamp
    magic String, -- Magic link token - for passwordless authentication
    magic_exp DateTime64(3), -- Magic link expiration
    magic_attempts Int32, -- Magic link attempts - for rate limiting
    magic_attempted DateTime64(3), -- Last magic link attempt timestamp
    terms_accepted DateTime64(3), -- When terms of service were accepted
    perms JSON, -- Permissions - detailed permission configuration

    -- Use cases and user attributes for targeting/matching
    use_cases Array(String), -- User use cases for this product (e.g., ["email_marketing", "analytics"])
    use_case_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of use cases (384-dim)
    user_attribs JSON, -- User attributes for clustering and matching (JSON format)
    user_attribs_gtesmall Array(Float32) DEFAULT [], -- GTE-small embedding of user attributes (384-dim)

    -- Embedding metadata
    embedding_model String DEFAULT 'gte-small', -- Embedding model used
    embedding_dimensions UInt16 DEFAULT 384, -- Dimensions of embedding vectors
    embedding_generated_at DateTime64(3) -- When embeddings were generated
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY uid;

-- Note: Old simple user lookup views removed - replaced by comprehensive views at end of schema
-- (user_by_email, user_by_username, user_by_cell superseded by user_by_all_emails, user_by_username, user_by_all_cells)

-- Materialized view for finding users in specific cohorts
CREATE MATERIALIZED VIEW user_by_cohorts
ENGINE = ReplicatedReplacingMergeTree
ORDER BY cohorts
POPULATE AS
SELECT uid, cohorts FROM users
ARRAY JOIN cohorts;

-- Note: Duplicate user_by_all_emails and user_by_all_cells views removed - final versions at end of schema

-- User verifications table - Tracks identity verification methods and statuses
CREATE TABLE user_verifications ON CLUSTER my_cluster (
    uid UUID, -- User ID - whose identity is being verified
    vmethod String, -- Verification method (e.g., "email", "phone", "id", "document")
    verifier UUID, -- Verifier user ID - who performed the verification
    created_at DateTime64(3) DEFAULT now64(3), -- Verification timestamp - when verification occurred
    oid UUID, -- Organization ID - which oid this verification is for
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    owner UUID, -- Owner user ID - who initiated this verification
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this verification
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (uid, vmethod);

-- Enhanced Queues table - Advanced task management system with additional metadata
CREATE TABLE files ON CLUSTER my_cluster (
    slug String, -- Slug - unique URL-friendly identifier
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    fprint String, -- Fingerprint - hash of file contents (SHA256)
    ftype String, -- File type - category (v=video, i=image, d=document)
    suffix String, -- Suffix - file extension (jpg, jpeg, doc, etc.)
    name String, -- Name - optimized/reconstructed filename
    mime String, -- MIME type - content type for browsers
    description String, -- Description - notes entered at upload
    lat Float64, -- Latitude - geographic location
    lon Float64, -- Longitude - geographic location
    oname String, -- Original name - original filename
    ocreated DateTime64(3), -- Original created date - from file metadata
    omime String, -- Original MIME type - before processing
    alt String, -- Alt text - for accessibility
    meta JSON, -- Metadata - additional file information
    x Int32, -- Width - in pixels for images/videos
    y Int32, -- Height - in pixels for images/videos
    size Int32, -- Size - file size in bytes
    private Boolean, -- Private flag - requires authentication
    bucket String, -- Storage bucket - where file is stored
    url_prefix String, -- URL prefix - for proxy access (private/public)
    ourl String, -- Original URL - source URL if imported
    hqurl String, -- High quality URL - high resolution version
    mqurl String, -- Medium quality URL - medium resolution version
    lqurl String, -- Low quality URL - low resolution version
    thumb String, -- Thumbnail URL - preview image
    used_in Array(String), -- Usage references - where file is used
    owner UUID, -- Owner user ID - who uploaded the file
    o_r Boolean, -- Owner read permission
    o_w Boolean, -- Owner write permission
    orgs Array(UUID), -- Organization IDs - associated organizations
    g_r Boolean, -- Group/oid read permission
    g_w Boolean, -- Group/oid write permission
    roles Array(String), -- Role permissions - which roles can access
    r_r Boolean, -- Role read permission
    r_w Boolean, -- Role write permission
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    expires DateTime64(3), -- Expiration timestamp
    updater UUID -- Updater user ID - who last modified this file
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY slug;

-- Payment Provider table - Stores information about payment service providers
CREATE TABLE pprovider ON CLUSTER my_cluster (
    name String, -- Provider name - unique identifier (e.g., "stripe", "paypal")
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID, -- Owner user ID - who created this provider record
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this provider
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY name;

-- Materialized view for looking up payment providers by owner - Enables efficient provider management
CREATE MATERIALIZED VIEW pprovider_by_owner
ENGINE = ReplicatedReplacingMergeTree
ORDER BY owner
POPULATE AS
SELECT * FROM pprovider;

-- Payment Authorization table - Stores user payment method information
CREATE TABLE pauth ON CLUSTER my_cluster (
    id UUID, -- Authorization ID - unique identifier for this payment method
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    pprovider String, -- Payment provider name (references pprovider.name)
    pcustomer String, -- Customer ID in the payment provider's system
    psource String, -- Payment source identifier (e.g., card ID, bank account token)
    meta String, -- Metadata about the payment method in JSON format
    priority Int32, -- Priority order for multiple payment methods
    active DateTime64(3), -- Timestamp when this payment method became active
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID, -- Owner user ID - who owns this payment method
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this payment method
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Materialized view for looking up payment methods by owner - Enables efficient user payment management
CREATE MATERIALIZED VIEW pauth_by_owner
ENGINE = ReplicatedReplacingMergeTree
ORDER BY owner
POPULATE AS
SELECT * FROM pauth;

-- Payment Confirmation table - Stores payment event confirmations
CREATE TABLE pconfirmation ON CLUSTER my_cluster (
    id String, -- Event ID - unique identifier for this payment event
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    ctype String, -- Confirmation type (e.g., "charge", "refund", "subscription")
    ref String, -- Reference ID - typically an external transaction ID
    rtype String, -- Reference type - describes what the ref field references
    meta String, -- Metadata about the payment event in JSON format
    created_at DateTime64(3) DEFAULT now64(3), -- Creation timestamp
    owner UUID, -- Owner user ID - who owns this payment confirmation
    updated_at DateTime64(3) DEFAULT now64(3), -- Last update timestamp
    updater UUID, -- Updater user ID - who last modified this confirmation
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY id;

-- Permissions table - Stores access control permissions for resources
CREATE MATERIALIZED VIEW user_by_username
ENGINE = ReplicatedReplacingMergeTree
ORDER BY username
POPULATE AS
SELECT uid, username FROM users WHERE username IS NOT NULL AND username != '';

-- Materialized view for all emails (primary + additional) lookups
CREATE MATERIALIZED VIEW user_by_all_emails
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
CREATE MATERIALIZED VIEW user_by_all_cells
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
