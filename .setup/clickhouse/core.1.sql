-- Unlicensed Copyright (c) 2025 SF Product Labs. All Rights Reserved.
-- See LICENSE
SET enable_json_type = 1;
-- ClickHouse adaptation of Cassandra Schema
--ERASE PREVIOUS DATABASE
-- drop database sfpla sync;
-- clickhouse keeper-client --host 0.0.0.0 --port 2181 --query "rmr '/clickhouse'"
-- select * from ips final;
-- OPTIMIZE TABLE ips FINAL;
-- select * from ips final;

-- Create database if it doesn't exist (Atomic engine supports config defaults)
CREATE DATABASE IF NOT EXISTS sfpla ENGINE = Atomic;

-- Use the database
USE sfpla;

-- Sequences table - For tracking versioning and sequences across the system
CREATE TABLE IF NOT EXISTS sequences ON CLUSTER my_cluster (
    name String, -- Name of the sequence
    seq UInt32, -- Current sequence value
    updated_at DateTime64(3) DEFAULT now64(3) -- Last update timestamp
) ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY name;

-- Insert initial version
INSERT INTO sequences (name, seq) VALUES ('DB_VER', 3);

-- Define geo_point as a Nested data structure
-- Nested types in ClickHouse to replace Cassandra user-defined types
-- geo_point structure - Dictionary for location coordinates
CREATE TABLE IF NOT EXISTS geo_points_dictionary ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique identifier for this geographic point
    lat Float64, -- Latitude coordinate
    lon Float64, -- Longitude coordinate
    created_at DateTime64(3) DEFAULT now64(3) -- Created timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- viewport structure - Dictionary for browser viewport dimensions
CREATE TABLE IF NOT EXISTS viewport_dictionary ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique identifier for this viewport
    w Int64, -- Width in pixels
    h Int64, -- Height in pixels
    created_at DateTime64(3) DEFAULT now64(3) -- Created timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- payment structure - converted to a table with JSON for complex data
-- Payments table - LINE ITEM LEVEL (one row per product per invoice)
-- MULTI-LINE ITEM INVOICE DESIGN:
--   * One invoice (invid) can have MULTIPLE rows (one per product/line item)
--   * Invoice-level fields (invoiced_at, paid_at, tax, commission, fees) are DUPLICATED across line items
--   * To get invoice totals: SELECT invid, SUM(subtotal), SUM(tax), MAX(paid_at) FROM payments WHERE invid = ? GROUP BY invid
--   * Line item fields (product, qty, price, discount, revenue, margin, cost) are UNIQUE per row
--   * Each line item gets its own id (UUID) for tracking
--
-- Example invoice with 3 line items:
--   invid: "invoice-123", product: "Product A", qty: 2, subtotal: 50.00, tax: 5.00, paid_at: 2025-01-21
--   invid: "invoice-123", product: "Product B", qty: 1, subtotal: 30.00, tax: 3.00, paid_at: 2025-01-21
--   invid: "invoice-123", product: "Shipping",  qty: 1, subtotal: 10.00, tax: 1.00, paid_at: 2025-01-21
--   → Invoice total: SUM(subtotal) = $90.00, SUM(tax) = $9.00, Grand Total = $99.00
--
CREATE TABLE IF NOT EXISTS payments ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique line item identifier (NOT invoice ID)
    oid UUID, -- Organization ID - for multi-tenant data isolation
    org LowCardinality(String) DEFAULT '', -- Sub-organization within oid (e.g., client's client like "microsoft" under "acme")
    tid UUID, -- Thread ID - links to mthreads conversation that led to this payment
    uid UUID, -- User ID - for LTV aggregation and user-based attribution (extracted from events/visitors)
    invid UUID, -- Invoice ID (can appear in MULTIPLE rows for multi-line item invoices)
    invoiced_at DateTime64(3), -- When the invoice was created (DUPLICATED per line item)

    -- LINE ITEM FIELDS (unique per row)
    product String, -- Product name for THIS line item
    product_id UUID, -- Product ID for THIS line item
    pcat String, -- Product category (subscription/premium)
    man String, -- Manufacturer name
    model String, -- Manufacturer's model code
    qty Float64, -- Quantity purchased for THIS line item
    duration String, -- Subscription duration/type
    starts Date, -- Product start date
    ends Date, -- Product end date
    price Float64, -- Original list price for THIS line item
    discount Float64, -- Discount amount (not percentage) for THIS line item
    revenue Float64, -- Amount that hits the bank for THIS line item
    margin Float64, -- Profit margin for THIS line item
    cost Float64, -- Estimated costs for THIS line item (can include intangible costs)

    -- INVOICE-LEVEL FIELDS (duplicated across line items - aggregate when querying invoice totals)
    tax Float64, -- Tax amount for THIS line item (invoice total = SUM across line items)
    tax_rate Float64, -- Tax rate applied (may vary per line item)
    commission Float64, -- Fees for app store, play store, etc. (typically invoice-level, duplicated)
    referral Float64, -- User/entity fee for referral (typically invoice-level, duplicated)
    fees Float64, -- Additional service fees (typically invoice-level, duplicated)
    subtotal Float64, -- Amount before tax for THIS line item
    total Float64, -- Total amount including tax for THIS line item
    payment Float64, -- Actual amount customer paid (invoice-level, duplicated across line items)

    currency String, -- Currency code (e.g., EUR, USD)
    country String, -- ISO-2 country code (e.g., US, DE, AU)
    rcode String, -- State/region code (e.g., CA, BW)
    region String, -- State/region name (e.g., California)
    campaign_id String DEFAULT '', -- Campaign identifier for attribution
    paid_at DateTime64(3), -- When payment was received (invoice-level, duplicated)
    created_at DateTime64(3) DEFAULT now64(3), -- Record creation timestamp
    updated_at DEFAULT now64(3), -- Record updated

    -- Projection for fast invoice line item lookups
    PROJECTION invid_proj
    (
        SELECT _part_offset ORDER BY invid
    )

) ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY (oid, toYYYYMM(created_at))
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

-- geo_pol structure - Geographic political data dictionary
CREATE TABLE geo_pols ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique identifier
    country String, -- ISO-2 country code (e.g., US, DE)
    rcode String, -- State/region code (e.g., CA, BW)
    region String, -- State/region name (e.g., California)
    county String, -- County/legislative sub-region
    city String, -- City name
    zip String, -- Postal/ZIP code
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- Countries table - Reference table for country information
CREATE TABLE countries ON CLUSTER my_cluster (
    country String, -- ISO-2 country code (e.g., US)
    name String, -- Full country name (e.g., United States of America)
    continent String, -- Continent name (e.g., North America)
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY country;

-- GeoIP lookup table - For IP address to location mapping
CREATE TABLE geo_ip ON CLUSTER my_cluster (
    ipc String, -- IP class for distributing primary key
    ips String, -- IP start range (as string for easier handling)
    ipe String, -- IP end range (as string for easier handling)
    ipis String, -- IP start address representation
    ipie String, -- IP end address representation
    country String, -- Country code of the IP range
    region String, -- Region/state of the IP range
    city String, -- City of the IP range
    lat Float64, -- Latitude coordinate
    lon Float64, -- Longitude coordinate
    tz String, -- Timezone of the IP range
    zip String, -- Postal/ZIP code
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY ipc
ORDER BY (ipc, ips, ipe);

-- Hosts table - Maps host hash to hostname
CREATE TABLE hosts ON CLUSTER my_cluster (
    hhash String, -- Host hash for privacy and efficient lookups
    hostname String, -- Actual hostname
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY (hhash, hostname);

-- Outcomes table (using SummingMergeTree for counter replacement) - Tracks successful outcomes and intentions
CREATE TABLE outcomes ON CLUSTER my_cluster (
    hhash String, -- Host hash
    outcome String, -- Outcome type/name
    sink String, -- Local optimum/intention
    created Date, -- Date of the outcome
    url String, -- Associated URL
    total UInt64 -- Counter for number of successful outcomes
) ENGINE = ReplicatedSummingMergeTree((total))
PARTITION BY toYYYYMM(created)
ORDER BY (hhash, outcome, sink, url, created);

-- Visitors table - Main table for tracking visitor data, written once per first visit (acquisitions)
-- FOREIGN KEY RELATIONSHIPS:
--   * vid -> sessions.vid (one-to-many)
--   * sid -> sessions.sid (one-to-one)
--   * uid -> users.uid (many-to-one)
--   * auth -> users.uid (many-to-one)
--   * ref -> visitors.vid (many-to-one)
