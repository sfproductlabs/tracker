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
    name String DEFAULT '', -- Name of the sequence
    seq UInt32 DEFAULT 0, -- Current sequence value
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
    lat Float64 DEFAULT 0.0, -- Latitude coordinate
    lon Float64 DEFAULT 0.0, -- Longitude coordinate
    created_at DateTime64(3) DEFAULT now64(3) -- Created timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- viewport structure - Dictionary for browser viewport dimensions
CREATE TABLE IF NOT EXISTS viewport_dictionary ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique identifier for this viewport
    w Int64 DEFAULT 0, -- Width in pixels
    h Int64 DEFAULT 0, -- Height in pixels
    created_at DateTime64(3) DEFAULT now64(3) -- Created timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- geo_pol structure - Geographic political data dictionary
CREATE TABLE geo_pols ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(), -- Unique identifier
    country String DEFAULT '', -- ISO-2 country code (e.g., US, DE)
    rcode String DEFAULT '', -- State/region code (e.g., CA, BW)
    region String DEFAULT '', -- State/region name (e.g., California)
    county String DEFAULT '', -- County/legislative sub-region
    city String DEFAULT '', -- City name
    zip String DEFAULT '', -- Postal/ZIP code
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY id;

-- Countries table - Reference table for country information
CREATE TABLE countries ON CLUSTER my_cluster (
    country String DEFAULT '', -- ISO-2 country code (e.g., US)
    name String DEFAULT '', -- Full country name (e.g., United States of America)
    continent String DEFAULT '', -- Continent name (e.g., North America)
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY country;

-- GeoIP lookup table - For IP address to location mapping
CREATE TABLE geo_ip ON CLUSTER my_cluster (
    ipc String DEFAULT '', -- IP class for distributing primary key
    ips String DEFAULT '', -- IP start range (as string for easier handling)
    ipe String DEFAULT '', -- IP end range (as string for easier handling)
    ipis String DEFAULT '', -- IP start address representation
    ipie String DEFAULT '', -- IP end address representation
    country String DEFAULT '', -- Country code of the IP range
    region String DEFAULT '', -- Region/state of the IP range
    city String DEFAULT '', -- City of the IP range
    lat Float64 DEFAULT 0.0, -- Latitude coordinate
    lon Float64 DEFAULT 0.0, -- Longitude coordinate
    tz String DEFAULT '', -- Timezone of the IP range
    zip String DEFAULT '', -- Postal/ZIP code
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
PARTITION BY ipc
ORDER BY (ipc, ips, ipe);

-- Hosts table - Maps host hash to hostname
CREATE TABLE hosts ON CLUSTER my_cluster (
    hhash String DEFAULT '', -- Host hash for privacy and efficient lookups
    hostname String DEFAULT '', -- Actual hostname
    created_at DateTime64(3) DEFAULT now64(3) -- Record creation timestamp
) ENGINE = ReplicatedReplacingMergeTree(created_at)
ORDER BY (hhash, hostname);

-- Outcomes table (using SummingMergeTree for counter replacement) - Tracks successful outcomes and intentions
CREATE TABLE outcomes ON CLUSTER my_cluster (
    hhash String DEFAULT '', -- Host hash
    outcome String DEFAULT '', -- Outcome type/name
    sink String DEFAULT '', -- Local optimum/intention
    created Date DEFAULT today(), -- Date of the outcome
    url String DEFAULT '', -- Associated URL
    total UInt64 DEFAULT 0 -- Counter for number of successful outcomes
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
