# Go Tracker with ClickHouse + Keeper

High-performance user telemetry and event tracking system built in Go. Currently in production use, capturing hundreds of millions of records with enterprise-grade reliability.

## 🎯 Overview

Track every visitor click, setup growth experiments, and measure user outcomes and growth loops - all under one roof for all your sites/assets. Built on the same infrastructure used by Yandex, CERN, Netflix, Apple, and Github (see Cassandra, ClickHouse), this system provides:

- **Data Sovereignty**: Keep your data under your control, solve GDPR compliance
- **Privacy-First**: Built-in GDPR compliance with configurable retention policies
- **Enterprise Scale**: Proven at hundreds of millions of records
- **High Performance**: 5-10x performance improvements via intelligent batching
- **Multi-Database**: ClickHouse (primary), Cassandra, DuckDB support

## 🚀 Quick Start

### Using Makefile (Recommended)

```bash
# Build Docker image and run single-node setup
make docker-build
make docker-run

# Wait 60 seconds for initialization, then verify
make docker-verify-tables
# Expected: 236 tables loaded

# Run comprehensive tests
make docker-test-all

# View logs
make docker-logs
```

### Manual Setup (Tracker Self-Contained)

```bash
# Clone and build
git clone https://github.com/sfproductlabs/tracker.git
cd tracker
go build -o tracker

# Start ClickHouse (tracker-local instance)
make clickhouse-start

# Load schema
make schema-load

# Run tracker with local ClickHouse
./tracker config.json
```

**Note**: The tracker is fully self-contained with its own ClickHouse instance:
- Config files: `clickhouse-config.xml`, `clickhouse-users.xml` (local copies)
- Data directory: `./tmp/clickhouse/` (all local, no external dependencies)
- Schema files: `.setup/clickhouse/` (6 core open-source schemas)
- See [QUICK_START_SELF_CONTAINED.md](./QUICK_START_SELF_CONTAINED.md) for details

## 📚 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Makefile Commands](#-makefile-commands)
- [Docker Setup](#-docker-setup)
- [API Endpoints](#-api-endpoints)
- [Intelligent Batching System](#-intelligent-batching-system)
- [Configuration](#-configuration)
- [Testing](#-testing)
- [Production Deployment](#-production-deployment)
- [Privacy & Compliance](#-privacy--compliance)
- [Cluster Topology Guide](./CLUSTER_TOPOLOGY.md) - Sharded, Replicated, and Hybrid configurations

## ✨ Features

### Core Capabilities
- **Event Tracking**: URL/JSON/WebSocket tracking with LZ4 compression
- **URL Shortening**: Built-in redirect management for campaigns
- **Privacy Controls**: GDPR consent management and IP anonymization
- **GeoIP Lookup**: IP2Location integration for geographic data
- **Lifetime Value (LTV)**: Customer value tracking with batch support
- **Real-time Messaging**: NATS integration for distributed processing
- **Reverse Proxy**: Built-in proxy (replaces Traefik/NGINX functionality)

### Technical Features
- **Horizontal Scaling**: Clustered NATS, Clustered ClickHouse, Docker Swarm ready
- **TLS/SSL**: LetsEncrypt one-line configuration or custom certificates
- **Rate Limiting**: Configurable per-IP daily limits
- **Circuit Breakers**: Automatic failover and health checks
- **File Server**: Static content serving with caching
- **Chrome Extension**: Tracking URL Generator extension available

### Database Compatibility
- ✅ ClickHouse (primary, optimized with batching)
- ✅ Cassandra / Elassandra
- ✅ DuckDB (with S3/Hive export)
- ✅ NATS.io messaging
- ✅ Apache Spark / Elastic Search / Apache Superset (via exports)

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                    Go HTTP Server                       │
│  TLS/SSL, Rate Limiting, WebSocket, LZ4 Compression    │
└─────────────┬───────────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────────────┐
│              Intelligent Batch Manager                  │
│  5-10x Performance • 6 Strategies • Circuit Breakers   │
└─────┬───────┬───────┬───────┬───────┬───────────────────┘
      │       │       │       │       │
┌─────▼──┐ ┌──▼───┐ ┌▼────┐ ┌▼────┐ ┌▼──────────┐
│ClickHouse│ │Cassandra│ │DuckDB│ │NATS │ │Facebook │
│ Primary │ │Optional│ │Local│ │Queue│ │CAPI     │
└─────────┘ └────────┘ └─────┘ └─────┘ └─────────┘
```

### Event Processing Pipeline

1. **HTTP Request** → URL/JSON/WebSocket parsing → Cookie processing
2. **Data Normalization** → GeoIP lookup → Privacy filtering
3. **Intelligent Batching** → Strategy selection → Batch accumulation
4. **Database Write** → Circuit breaker protection → Automatic retries
5. **Real-time Notifications** → NATS publish → Downstream consumers

### Session Interface

All database backends implement a unified `session` interface:

```go
type session interface {
    connect() error
    close() error
    write(w *WriteArgs) error
    serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error
    prune() error
}
```

## 🛠️ Makefile Commands

The tracker includes a comprehensive Makefile for streamlined development. Use `make help` to see all commands.

### Build Commands

```bash
make build          # Build tracker binary
make run            # Build and run tracker (local mode)
make clean          # Clean build artifacts
make deps           # Download Go dependencies
make fmt            # Format Go code
make lint           # Run golangci-lint (if installed)
```

### Docker Commands (Single Node)

```bash
# Setup
make docker-build              # Build Docker image
make docker-run                # Run single-node container with persistent volumes
make docker-stop               # Stop and remove container
make docker-clean              # Remove container, image, and volumes

# Development
make docker-logs               # Show container logs (tail -f)
make docker-shell              # Open shell in running container
make docker-clickhouse-shell   # Open ClickHouse client

# Verification
make docker-verify-tables      # Verify tables loaded (expect: 236 tables)
make docker-test-events        # Test events table with sample data
make docker-test-messaging     # Test messaging tables (mthreads/mstore/mtriage)
make docker-test-all           # Run all Docker tests
make docker-rebuild-test       # Clean rebuild and full test
```

### Docker Commands (3-Node Cluster)

```bash
make cluster-start      # Start 3-node cluster with persistent volumes
make cluster-stop       # Stop 3-node cluster
make cluster-test       # Test cluster connectivity and tables
make cluster-logs       # Show logs from all 3 nodes
```

### Test Scripts

All test scripts are located in the `tests/` directory:

```bash
# Core test scripts (in tests/ directory)
make test                          # Run Go unit tests
make test-single                   # Run tests/test-single.sh (single-node verification)
make test-cluster                  # Run tests/test-cluster.sh (3-node cluster test)
make test-db-writes                # Run tests/test_db_writes.sh (100% database write verification)
make test-all                      # Run all tests (Go + single + db-writes)
```

### Functional Endpoint Tests

```bash
make test-functional-health        # Test /health, /ping, /status, /metrics
make test-functional-ltv           # Test LTV tracking (single payment)
make test-functional-ltv-batch     # Test LTV tracking (batch payments)
make test-functional-redirects     # Test redirect/short URL API
make test-functional-privacy       # Test privacy/agreement API
make test-functional-jurisdictions # Test jurisdictions endpoint
make test-functional-batch         # Test batch processing (100 events)
make test-functional-e2e           # Test complete end-to-end workflow
make test-functional-all           # Run ALL functional tests
```

### Schema Management

```bash
make schema-update      # Update hard links from api schema files
make schema-verify      # Verify hard links are correct
make schema-load        # Load all 6 core schema files (177 tables)
```

### Database Management (Self-Contained ClickHouse)

```bash
# Start/stop ClickHouse (tracker-local instance)
make clickhouse-start   # Start ClickHouse with embedded Keeper
make clickhouse-stop    # Stop ClickHouse
make clickhouse-status  # Check ClickHouse status

# Database operations
make db-console         # Open ClickHouse CLI
make db-reset           # Drop database + reload schema
make db-clean           # Clean database without schema reload
make db-admin           # Set up admin account
```

### Development Helpers

```bash
make info               # Show configuration information
make status             # Check build and container status
make watch              # Watch for changes (requires fswatch)
make benchmark          # Run Go benchmarks
make coverage           # Generate test coverage report
```

## 🐳 Docker Setup

### Quick Start: Single Node

```bash
# 1. Build Docker image
make docker-build

# 2. Start container (creates /tmp/clickhouse-test with persistent data)
make docker-run

# 3. Wait 60 seconds for full initialization
sleep 60

# 4. Verify tables loaded
make docker-verify-tables
# Should show: 236 tables

# 5. Test events table
make docker-test-events
# Sends 5 test events, verifies results

# 6. Open ClickHouse client for manual queries
make docker-clickhouse-shell
# Then run: SELECT count() FROM sfpla.events FINAL;

# 7. Clean up when done
make docker-stop
```

### Access Points

Once running, the tracker exposes:

| Service | URL | Description |
|---------|-----|-------------|
| Tracker HTTP | http://localhost:8080/health | Main API endpoint |
| Tracker HTTPS | https://localhost:8443/health | Secure API endpoint |
| Tracker Alt | http://localhost:8880/health | Alternative port |
| ClickHouse HTTP | http://localhost:8123 | ClickHouse HTTP interface |
| ClickHouse Native | localhost:9000 | Native protocol (TCP) |
| ClickHouse Keeper | localhost:2181 | ZooKeeper-compatible coordination |

### 3-Node Cluster Setup

For production-like testing with replication:

```bash
# Start 3-node cluster
make cluster-start

# Each node gets these ports (example for node 1):
# - Tracker: 8080, 8443, 8880
# - ClickHouse: 9000 (native), 8123 (HTTP)
# - Keeper: 2181 (client), 9444 (raft)

# Test cluster health
make cluster-test

# View logs from all nodes
make cluster-logs

# Stop cluster
make cluster-stop
```

**See [CLUSTER_TOPOLOGY.md](./CLUSTER_TOPOLOGY.md) for:**
- ✅ Detailed topology comparison (Sharded vs Replicated vs Hybrid)
- ✅ Example configurations for 2-node, 3-node, and 6-node clusters
- ✅ Decision tree for choosing the right topology
- ✅ Environment variable reference and troubleshooting
- ✅ Production deployment examples

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SHARD` | `1` | Shard number for this node (1-N) |
| `REPLICA` | `1` | Replica number within shard |
| `SERVER_ID` | `1` | Unique Keeper server ID (must match shard) |
| `NUM_NODES` | `1` | Total nodes in cluster |
| `CONTAINER_NAME_PATTERN` | `v4-tracker` | Base name for auto-discovery |
| `CLUSTER_NAME` | `tracker_cluster` | ClickHouse cluster name |
| `CLICKHOUSE_DATA_DIR` | `/var/lib/clickhouse` | Data directory path |
| `CLICKHOUSE_LOG_DIR` | `/var/log/clickhouse-server` | Log directory path |

### Persistent Storage

#### Local Development

```bash
# Volumes automatically created at /tmp/clickhouse-test
make docker-run
```

#### AWS EBS Volume

```bash
# 1. Create and attach EBS volume
sudo mkfs -t ext4 /dev/xvdf
sudo mkdir -p /mnt/clickhouse
sudo mount /dev/xvdf /mnt/clickhouse
sudo chown -R 999:999 /mnt/clickhouse  # ClickHouse UID

# 2. Run with custom data directory
docker run -d --name tracker \
  -v /mnt/clickhouse:/data/clickhouse \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/data/clickhouse/logs" \
  --restart unless-stopped \
  tracker
```

## 📡 API Endpoints

### Event Tracking

#### Track Event (Client-side)
```bash
# REST/URL format
curl -k "https://localhost:8443/tr/v1/tr/vid/USER_ID/ename/page_view/etyp/view"

# JSON format (recommended)
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/tr/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "sid": "session-123",
    "ename": "page_view",
    "etyp": "view",
    "url": "https://example.com/page",
    "tz": "America/Los_Angeles",
    "device": "Desktop",
    "os": "macOS"
  }'
```

#### Track Event (Server-side)
```bash
# Server-side tracking (returns event ID)
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/str/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "oid": "org-id",
    "ename": "server_event",
    "etyp": "conversion",
    "revenue": "99.99"
  }'
```

#### WebSocket Streaming
```javascript
// LZ4-compressed WebSocket for high-volume streaming
const ws = new WebSocket('wss://localhost:8443/tr/v1/ws');

ws.onopen = () => {
  ws.send(JSON.stringify({
    vid: 'user-123',
    ename: 'websocket_event',
    etyp: 'stream'
  }));
};
```

### Lifetime Value (LTV) Tracking

```bash
# Single payment
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ltv/" \
  -d '{
    "vid": "user-id",
    "uid": "user-123",
    "oid": "org-id",
    "amt": 99.99,
    "currency": "USD",
    "orid": "order-123"
  }'

# Batch payments
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ltv/" \
  -d '{
    "vid": "user-id",
    "uid": "user-123",
    "oid": "org-id",
    "payments": [
      {"amt": 50.00, "currency": "USD", "orid": "order-124"},
      {"amt": 25.00, "currency": "USD", "orid": "order-125"}
    ]
  }'
```

### URL Shortening / Redirects

```bash
# Create shortened URL
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/rpi/redirect/USER_ID/PASSWORD" \
  -d '{
    "urlfrom": "https://yourdomain.com/short",
    "slugfrom": "/short",
    "urlto": "https://example.com/long/path?utm_source=test",
    "oid": "org-id"
  }'

# List all redirects for a host
curl -k -X GET \
  "https://localhost:8443/tr/v1/rpi/redirects/USER_ID/PASSWORD/yourdomain.com"

# Test redirect (visit in browser)
curl -k -L "https://localhost:8443/short"
```

### Privacy & Compliance

```bash
# Post GDPR consent
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ppi/agree" \
  -d '{
    "vid": "user-id",
    "cflags": 1024,
    "tz": "America/Los_Angeles",
    "lat": 37.7749,
    "lon": -122.4194
  }'

# Get agreements for visitor
curl -k -X GET "https://localhost:8443/tr/v1/ppi/agree?vid=user-id"

# Get jurisdictions (privacy regions)
curl -k -X GET "https://localhost:8443/tr/v1/ppi/jds"

# GeoIP lookup
curl -k -X GET "https://localhost:8443/tr/v1/ppi/geoip?ip=8.8.8.8"
```

### Health & Monitoring

```bash
# Health check
curl -k "https://localhost:8443/health"

# Ping endpoint
curl -k "https://localhost:8443/ping"

# Metrics (Prometheus format + batching stats)
curl -k "https://localhost:8443/metrics"

# Status endpoint
curl -k "https://localhost:8443/status"
```

## 🚀 Intelligent Batching System

The tracker includes an advanced batching system that provides **5-10x performance improvements** for ClickHouse.


### Batching Strategies

| Strategy | Use Case | Trigger | Best For |
|----------|----------|---------|----------|
| **Immediate** | Critical data (payments, errors) | Every event | Financial transactions |
| **Time-Based** | Analytics (mthreads) | Every X seconds | Periodic aggregation |
| **Size-Based** | Background data (visitors) | N events collected | High-volume processing |
| **Hybrid** | Core events | Time OR size threshold | Balanced performance |
| **Memory-Based** | Large payloads | Memory threshold | Memory-efficient processing |
| **Adaptive** | Dynamic workloads | AI-driven optimization | Variable load patterns |

### Table-Specific Configurations

#### High-Performance Events
```go
events: {
    Strategy:         StrategyHybridBatch,
    MaxBatchSize:     1000,           // Large batches for throughput
    MaxBatchTime:     2 * time.Second, // Quick flush for latency
    MaxMemoryMB:      10,
    Priority:         3,
    EnableCompression: true,
}
```

#### Critical Financial Data
```go
payments: {
    Strategy:         StrategyImmediateBatch,
    MaxBatchSize:     1,              // No batching delay
    MaxBatchTime:     0,
    Priority:         1,              // Highest priority
    RetryAttempts:    5,
}
```

#### Campaign Telemetry
```go
mthreads: {
    Strategy:         StrategyTimeBasedBatch,
    MaxBatchSize:     100,
    MaxBatchTime:     5 * time.Second, // Allow time for aggregation
    Priority:         2,
}
```

### Monitoring Batch Performance

```bash
# Check batch metrics
curl http://localhost:8080/metrics | jq '.batching'

# Expected output:
{
  "enabled": true,
  "total_batches": 15420,
  "total_items": 1542000,
  "failed_batches": 12,
  "avg_batch_size": 100,
  "avg_flush_latency_ms": 25,
  "queued_items": 234,
  "memory_usage_mb": 45,
  "batch_success_rate": 0.9992,
  "events_per_second": 2840.5
}
```

## ⚙️ Configuration

The tracker uses a single `config.json` file for all configuration.

### Core Configuration Sections

#### Database Connections (Notify array)
```json
{
  "Notify": [
    {
      "Type": "clickhouse",
      "Host": "localhost",
      "Port": 9000,
      "BatchingEnabled": true,
      "MaxBatchSize": 1000,
      "MaxBatchTime": "2s"
    },
    {
      "Type": "cassandra",
      "Hosts": ["localhost:9042"],
      "Keyspace": "tracker"
    }
  ]
}
```

#### Security and Performance
```json
{
  "UseLocalTLS": true,
  "LetsEncryptDomains": ["yourdomain.com"],
  "RateLimitPerDay": 10000,
  "MaxConnections": 10,
  "ConnectionTimeout": 30
}
```

#### Privacy Controls
```json
{
  "GeoIPDatabase": ".setup/geoip/IP2LOCATION.BIN",
  "AnonymizeIP": true,
  "DataRetentionDays": 365,
  "PruneOldData": true
}
```

## 🧪 Testing

### Quick Test Workflow

```bash
# 1. Start tracker
make docker-build
make docker-run

# 2. Wait for initialization
sleep 60

# 3. Run all tests
make test-functional-all

# 4. Check specific functionality
make docker-test-events        # Events table
make docker-test-messaging     # Messaging tables (mthreads/mstore/mtriage)
```

### Messaging Tables (Universal Message System)

The tracker integrates with the universal message system via three tables:

#### mthreads (Thread metadata - 140+ columns)
- Core: `tid`, `alias`, `xstatus`, `name`, `provider`, `medium`
- Campaigns: `campaign_id`, `campaign_status`, `campaign_priority`
- A/B testing: 20+ `abz_*` fields
- Attribution: `attribution_model`, `attribution_weight`

#### mstore (Permanent message archive - 47 columns)
- Content: `mid`, `subject`, `msg`, `data`
- Delivery: `urgency`, `sys`, `broadcast`, `svc`
- Timing: `planned`, `scheduled`, `started`, `completed`
- Performance: `interest` (JSON), `perf` (JSON)

#### mtriage (Messages in triage - 43 columns)
- Same as mstore but for messages being processed
- Default `urgency=8` for high-priority triage

Test messaging tables:
```bash
make docker-test-messaging
```

### Manual ClickHouse Queries

```bash
# Open ClickHouse client
make docker-clickhouse-shell

# Example queries
SELECT count() FROM sfpla.events FINAL;
SELECT ename, count() FROM sfpla.events FINAL GROUP BY ename;
SELECT * FROM sfpla.ltv FINAL ORDER BY updated_at DESC LIMIT 10;

# Flush async inserts before querying
SYSTEM FLUSH ASYNC INSERT QUEUE;
```

### End-to-End Test Example

```bash
#!/bin/bash
VID=$(uuidgen)
UID=$(uuidgen)
OID=$(uuidgen)

# 1. Page view
curl -sk -X POST https://localhost:8443/tr/v1/tr/ \
  -H "Content-Type: application/json" \
  -d "{\"vid\":\"$VID\",\"ename\":\"page_view\",\"etyp\":\"view\"}"

# 2. User signup
curl -sk -X POST https://localhost:8443/tr/v1/str/ \
  -H "Content-Type: application/json" \
  -d "{\"vid\":\"$VID\",\"uid\":\"$UID\",\"oid\":\"$OID\",\"ename\":\"signup\",\"etyp\":\"conversion\"}"

# 3. Purchase
curl -sk -X POST https://localhost:8443/tr/v1/ltv/ \
  -H "Content-Type: application/json" \
  -d "{\"vid\":\"$VID\",\"uid\":\"$UID\",\"oid\":\"$OID\",\"amt\":149.99}"

# 4. GDPR consent
curl -sk -X POST https://localhost:8443/tr/v1/ppi/agree \
  -H "Content-Type: application/json" \
  -d "{\"vid\":\"$VID\",\"cflags\":1024}"

# Wait and verify
sleep 3
clickhouse client --query "SELECT ename FROM sfpla.events WHERE vid='$VID'"
```

## 🚢 Production Deployment

### Performance Tuning

#### High-Volume Applications
- Increase `MaxBatchSize` to 2000-5000
- Use `StrategyHybridBatch` for most tables
- Enable compression for network efficiency

#### Low-Latency Applications
- Reduce `MaxBatchTime` to 500ms-1s
- Use smaller `MaxBatchSize` (100-500)
- Prioritize critical events

#### Memory-Constrained Environments
- Use `StrategyMemoryBasedBatch`
- Set conservative `MaxMemoryMB` limits
- Enable adaptive optimization

### Horizontal Scaling

```
┌────────────────┐
│ Load Balancer  │
└───┬────┬───┬───┘
    │    │   │
┌───▼┐ ┌─▼┐ ┌▼───┐
│Tr-1│ │Tr-2│ │Tr-3│  (Multiple tracker instances)
└───┬┘ └─┬┘ └┬───┘
    │    │   │
┌───▼────▼───▼───┐
│  ClickHouse    │  (Shared cluster)
│    Cluster     │
└────────────────┘
```

### Monitoring

```bash
# ClickHouse metrics
docker exec tracker clickhouse-client --query \
  "SELECT * FROM system.metrics"

# Keeper metrics
docker exec tracker clickhouse-client --query \
  "SELECT * FROM system.zookeeper"

# Application metrics
curl http://localhost:8080/metrics
```

### Replication Architectures

Understanding your replication strategy is critical for fault tolerance and backup planning.

#### Option 1: Non-Replicated MergeTree (Fastest)

**Architecture**:
```
Node 1: 100% of data (MergeTree)
Node 2: 100% of data (MergeTree)
Node 3: 100% of data (MergeTree)
```

**Characteristics**:
- ✅ **Highest throughput** - no replication overhead (up to 100x faster writes)
- ✅ **Simplest setup** - no ZooKeeper coordination needed
- ❌ **No automatic failover** - node failure = data unavailable
- ❌ **Requires backups** - critical for disaster recovery

**Best for**: Development, testing, high-throughput scenarios where you manage backups

#### Option 2: Single Shard with Multiple Replicas (Default, Full Replication)

**Architecture**:
```
Shard 1:
  ├─ Node 1: Replica 1 (100% of data)
  ├─ Node 2: Replica 2 (100% of data)
  └─ Node 3: Replica 3 (100% of data)
```

**Configuration**:

```sql
-- On Node 1
CREATE TABLE my_table (
    id UUID,
    timestamp DateTime,
    data String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/my_table', 'replica1')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp);

-- On Node 2 (same ZooKeeper path, different replica name)
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/my_table', 'replica2')
...

-- On Node 3
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/my_table', 'replica3')
...

-- Distributed table (on all nodes)
CREATE TABLE my_table_dist AS my_table
ENGINE = Distributed(my_cluster, my_database, my_table, rand());
```

**Cluster Configuration** (`/etc/clickhouse-server/config.xml`):
```xml
<remote_servers>
    <my_cluster>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
        </shard>
    </my_cluster>
</remote_servers>
```

**Characteristics**:
- ✅ **Automatic failover** - queries succeed if any replica is available
- ✅ **Read load balancing** - distributes reads across replicas
- ✅ **Automatic catch-up** - failed nodes sync when they return
- ⚠️ **Slower writes** - replication overhead (can be 10-100x slower)
- ⚠️ **No horizontal scaling** - each node stores all data
- ⚠️ **Higher storage costs** - 3x storage for 3 replicas

**Failover behavior**:
- **Reads**: Distributed table automatically queries healthy replicas ✅
- **Writes to distributed table**: Routes to healthy replica ✅
- **Writes to failed node directly**: Write fails (but other replicas stay available) ⚠️
- **Recovery**: Failed node automatically replicates missing data when restored ✅

**Best for**: Production systems requiring high availability with moderate write volume

#### Option 3: Multiple Shards with Replication (Horizontal Scaling + Fault Tolerance)

**Architecture**:
```
Shard 1 (50% of data):
  ├─ Node 1: Replica 1
  └─ Node 2: Replica 2

Shard 2 (50% of data):
  ├─ Node 3: Replica 1
  └─ Node 4: Replica 2
```

**Configuration**:

```sql
-- Node 1 (Shard 1, Replica 1)
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/my_table', 'replica1')
...

-- Node 2 (Shard 1, Replica 2 - same shard path)
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/my_table', 'replica2')
...

-- Node 3 (Shard 2, Replica 1 - different shard path)
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard2/my_table', 'replica1')
...

-- Node 4 (Shard 2, Replica 2)
CREATE TABLE my_table (...)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard2/my_table', 'replica2')
...

-- Distributed table
CREATE TABLE my_table_dist AS my_table
ENGINE = Distributed(my_cluster, my_database, my_table, rand());
```

**Cluster Configuration**:
```xml
<remote_servers>
    <my_cluster>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </my_cluster>
</remote_servers>
```

**Characteristics**:
- ✅ **Horizontal scaling** - storage and query performance scales with shards
- ✅ **Fault tolerance** - each shard has replicas
- ✅ **Efficient storage** - only 2x storage for 2 replicas per shard
- ⚠️ **Replication overhead** - still pays replication cost
- ⚠️ **Complex setup** - requires careful shard key selection

**Best for**: Large-scale production with high data volume and availability requirements

#### Architecture Comparison

| Feature | Non-Replicated | Single Shard Replicated | Multi-Shard Replicated |
|---------|----------------|------------------------|------------------------|
| **Write Speed** | ⚡⚡⚡ Fastest | 🐢 Slowest | 🐢 Slow |
| **Read Speed** | ⚡ Single node | ⚡⚡ Load balanced | ⚡⚡⚡ Parallel + balanced |
| **Fault Tolerance** | ❌ None | ✅ Full | ✅ Full |
| **Horizontal Scaling** | ❌ No | ❌ No | ✅ Yes |
| **Storage Efficiency** | ✅ 1x | ❌ 3x (for 3 replicas) | ⚠️ 2x (for 2 replicas/shard) |
| **Setup Complexity** | ✅ Simple | ⚠️ Moderate | ❌ Complex |
| **Backup Importance** | 🔴 Critical | 🟡 Important | 🟡 Important |

#### Choosing Your Architecture

**Use Non-Replicated MergeTree when**:
- Write performance is critical (analytics, high-frequency telemetry)
- You can tolerate brief downtime for restores
- You have reliable backup infrastructure
- Development/testing environments

**Use Single Shard Replication when**:
- High availability is required
- Data volume fits on a single server
- Write volume is moderate
- You want simple setup with automatic failover

**Use Multi-Shard Replication when**:
- You need to scale beyond single-server capacity
- High availability AND high performance required
- You have budget for 4+ servers
- Enterprise production deployments

#### Migration Path

```bash
# Start with non-replicated for development
make docker-run

# Test with single-shard replication for staging
make cluster-start  # 3-node cluster

# Scale to multi-shard for production
# (requires custom cluster configuration)
```

### Backups

#### ClickHouse MergeTree Backup & Restore Summary

##### The Problem with Non-Replicated MergeTree

When using regular MergeTree tables (not ReplicatedMergeTree) with a distributed table:
- **No automatic replication** - each node holds unique data
- **Node failure = data unavailable** - no automatic failover
- **Higher throughput** - avoids replication overhead (100x better in some cases)
- **Trade-off**: Speed vs. fault tolerance

###### Handling Failed Nodes in Queries

When a node fails, distributed queries will timeout or fail by default. You can configure tolerance:

**skip_unavailable_shards**
```sql
SET skip_unavailable_shards = 1;
SELECT * FROM distributed_table;
```
Allows queries to succeed with partial results from available shards only. **Warning**: You get incomplete data without necessarily knowing which shards were skipped.

**distributed_replica_error_cap & distributed_replica_error_half_life**
```sql
SET distributed_replica_error_cap = 1000;
SET distributed_replica_error_half_life = 60;
```
These settings track error counts per replica and exclude problematic ones from queries. However, they're less useful with MergeTree since there are no replicas to fall back to - just different shards with unique data.

**Best Practice**: With non-replicated MergeTree, avoid `skip_unavailable_shards` in production unless you can tolerate incomplete results. Better to fail fast and restore from backup.

##### Backup Methods

###### 1. Built-in BACKUP Command (Recommended)
Available in ClickHouse 22.8+
```sql
-- Backup
BACKUP DATABASE my_database TO Disk('backups', 'backup_name/');
BACKUP DATABASE my_database TO S3('https://bucket.s3.amazonaws.com/backups/', 'key', 'secret');

-- Restore
RESTORE DATABASE my_database FROM Disk('backups', 'backup_name/');
```
**Best for**: Automated backups, S3 storage, incremental backups

###### 2. FREEZE Command (Fast, No Downtime)
```sql
-- Freeze entire table (all partitions)
ALTER TABLE my_table FREEZE;

-- Or freeze specific partition
ALTER TABLE my_table FREEZE PARTITION '2024-01-28';
```

**How it works**:
- Creates hardlinks (not copies) in `/var/lib/clickhouse/shadow/N/` where N is an incremental number
- Original data directory: `/var/lib/clickhouse/data/my_database/my_table/`
- Shadow directory: `/var/lib/clickhouse/shadow/N/data/my_database/my_table/`
- Hardlinks mean instant "snapshot" with no disk space used initially
- Frozen data is protected from ClickHouse merges and mutations

**Complete process**:
```bash
# 1. Freeze the table
clickhouse-client --query "ALTER TABLE my_database.my_table FREEZE"

# 2. Copy shadow directory to backup location
cp -r /var/lib/clickhouse/shadow/1/ /backup/location/freeze_backup_$(date +%Y%m%d)/

# 3. Clean up shadow directory
clickhouse-client --query "ALTER TABLE my_database.my_table UNFREEZE"
# Or manually: rm -rf /var/lib/clickhouse/shadow/1/
```

**Best for**: Quick snapshots during operation, no query disruption

###### 3. clickhouse-backup Tool
```bash
clickhouse-backup create my_backup
clickhouse-backup upload my_backup
clickhouse-backup restore my_backup
```
**Best for**: Advanced features, scheduling, compression

###### 4. EBS Snapshots (AWS)
```bash
# Create snapshot
aws ec2 create-snapshot \
  --volume-id vol-xxxxx \
  --description "Tracker ClickHouse data $(date +%Y%m%d)"

# Automated daily backups
cat > /usr/local/bin/backup-clickhouse.sh <<'EOF'
#!/bin/bash
aws ec2 create-snapshot \
  --volume-id vol-xxxxx \
  --description "Daily backup $(date +%Y%m%d)"
EOF
chmod +x /usr/local/bin/backup-clickhouse.sh
echo "0 2 * * * /usr/local/bin/backup-clickhouse.sh" | crontab -
```
**Best for**: Full system backups with point-in-time recovery

##### Restore Methods

###### From BACKUP Command
```sql
RESTORE TABLE my_database.my_table FROM Disk('backups', 'backup_name/');
RESTORE TABLE my_database.my_table AS my_database.my_table_test FROM S3(...);
```

###### From FREEZE or File Backup

**Method A: Using ATTACH PARTITION FROM** (No downtime for other partitions)
```sql
-- 1. Create temporary table from backup
CREATE TABLE my_database.my_table_backup AS my_database.my_table;

-- 2. Copy frozen data to the temp table directory
-- On filesystem (outside ClickHouse):
```
```bash
cp -r /backup/location/freeze_backup_20240128/shadow/1/data/my_database/my_table/* \
   /var/lib/clickhouse/data/my_database/my_table_backup/

chown -R clickhouse:clickhouse /var/lib/clickhouse/data/my_database/my_table_backup/
```
```sql
-- 3. Attach partitions from temp table to main table
ALTER TABLE my_database.my_table ATTACH PARTITION '2024-01-28' FROM my_database.my_table_backup;

-- Repeat for each partition you need to restore

-- 4. Drop temp table
DROP TABLE my_database.my_table_backup;
```

**Method B: Direct file replacement** (Requires downtime)
```bash
# 1. Stop ClickHouse
sudo systemctl stop clickhouse-server

# 2. Backup current table data (safety measure)
mv /var/lib/clickhouse/data/my_database/my_table \
   /var/lib/clickhouse/data/my_database/my_table.old

# 3. Copy frozen backup data
cp -r /backup/location/freeze_backup_20240128/shadow/1/data/my_database/my_table \
   /var/lib/clickhouse/data/my_database/

# 4. Fix permissions
chown -R clickhouse:clickhouse /var/lib/clickhouse/data/my_database/my_table

# 5. Start ClickHouse
sudo systemctl start clickhouse-server

# 6. Verify
clickhouse-client --query "SELECT count() FROM my_database.my_table"
```

###### From clickhouse-backup
```bash
clickhouse-backup restore my_backup
clickhouse-backup restore --table my_database.my_table my_backup
```

##### Best Practices

1. **Automate backups** - Schedule during low-traffic periods
2. **Store off-node** - Use S3 or separate storage for disaster recovery
3. **Test restores regularly** - Backups are worthless if restoration fails
4. **Monitor backup operations** - Track size, duration, and success
5. **Retention policy** - Keep recent backups locally, older ones in S3
6. **Validate after restore** - Check row counts and sample data

##### Recommended Setup for High-Throughput MergeTree

- Use BACKUP command with S3 storage
- Schedule automated backups during off-peak hours
- Keep latest backup locally for fast recovery
- Test restoration monthly
- Document recovery procedures and RTO/RPO requirements

##### Recovery Validation
```sql
-- After restore, verify data
SELECT count() FROM my_table;
SELECT * FROM my_table ORDER BY timestamp DESC LIMIT 100;
```

##### Docker Container Example
```bash
# Backup from Docker container
docker exec tracker clickhouse-client --query \
  "BACKUP DATABASE sfpla TO Disk('default', 'backup_$(date +%Y%m%d).zip')"

# Or using FREEZE within container
docker exec tracker clickhouse-client --query "ALTER TABLE sfpla.events FREEZE"
docker exec tracker bash -c "cp -r /var/lib/clickhouse/shadow/1/ /data/clickhouse/backups/freeze_$(date +%Y%m%d)/"
```

## 🔒 Privacy & Compliance

### GDPR Features

- **IP Anonymization**: Automatic hashing and anonymization
- **Data Retention**: Configurable retention periods
- **Cookie Consent**: GDPR-compliant consent management
- **Right to be Forgotten**: Privacy pruning functionality

### Standard Cookies

The tracker uses these cookies (all configurable):

| Cookie | Purpose | Example |
|--------|---------|---------|
| `vid` | Visitor ID (persistent) | `14fb0860-b4bf-11e9-8971-7b80435315ac` |
| `sid` | Session ID | `session-123` |
| `CookieConsent` | GDPR consent flags | `1024` |
| `ref` | Referral entity | `campaign-id` |
| `xid` | Experiment ID | `experiment-123` |
| `jwt` | Encrypted user token | (JWT format) |

### Privacy Pruning

```bash
# Run privacy pruning
./tracker --prune config.json

# Logs-only pruning (don't delete data)
./tracker --prune --logs-only config.json
```

### Data Residency

- Per-jurisdiction data filtering
- Configurable geographic restrictions
- Local data processing requirements

## 📊 Database Schema

The tracker supports a comprehensive schema with:

- **Core Events**: `events`, `events_recent`, `visitors`, `sessions`
- **Analytics**: `dailies`, `outcomes`, `referrers`, `ltv`, `ltvu`, `ltvv`
- **Messaging**: `mthreads`, `mstore`, `mtriage` (140+ columns for universal message system)
- **URL Management**: `redirects`, `redirect_history`
- **Privacy**: `agreements`, `agreed`, `jurisdictions`
- **Total**: 236 tables when fully loaded

### Schema Management

```bash
# Update hard links from api schema
make schema-update

# Verify hard links
make schema-verify
```

Schema files are hard-linked from `../../api/scripts/clickhouse/schema/`:
- `compliance.1.sql`
- `core.1.sql`
- `analytics.1.sql`
- `messaging.1.sql`
- `users.1.sql`
- `visitor_interests.1.sql`
- `auth.1.sql`

## 🔧 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs v4-tracker-1

# Check ClickHouse logs
docker exec v4-tracker-1 tail -100 /var/log/clickhouse-server/clickhouse-server.err.log
```

### Schema Not Loading

```bash
# Check schema files exist
docker exec v4-tracker-1 ls -la /app/tracker/.setup/clickhouse/

# Manually load schema
docker exec -i v4-tracker-1 clickhouse-client --multiquery < .setup/clickhouse/core.1.sql
```

### No Events in Database

```bash
# Check batching is enabled
curl http://localhost:8080/metrics | jq '.batching.enabled'

# Flush async insert queue
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE"
sleep 2

# Query with FINAL
clickhouse client --query "SELECT count() FROM sfpla.events FINAL"
```

### Keeper Connection Issues

```bash
# Check keeper is running
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.zookeeper WHERE path='/'"

# Test DNS resolution (cluster mode)
docker exec v4-tracker-1 ping -c 3 v4-tracker-2
```

## 📝 Development

### Local Development Setup

1. Start ClickHouse: `clickhouse server`
2. Configure domains in `config.json`
3. Generate or use test certificates
4. Run tracker: `./tracker config.json`

### Code Structure

```
packages/tracker/
├── tracker.go           # Main HTTP server and routing
├── clickhouse.go        # ClickHouse interface (101KB, main DB)
├── batch_manager.go     # Intelligent batching system
├── cassandra.go         # Cassandra interface
├── duckdb.go           # DuckDB interface
├── nats.go             # NATS messaging
├── utils.go            # Utility functions
├── geoip.go            # GeoIP lookup
├── fb.go               # Facebook CAPI integration
├── Makefile            # Comprehensive build/test commands
├── Dockerfile          # Container definition
├── entrypoint.sh       # Container startup script
├── config.json         # Configuration file
└── .setup/
    ├── clickhouse/     # Schema files (hard-linked from api)
    ├── geoip/          # IP2Location databases
    └── keys/           # TLS certificates
```

### Building from Source

```bash
# Install Go 1.19+
brew install go  # macOS

# Build tracker
go build -o tracker

# Run with config
./tracker config.json
```

## 🎯 Credits

- [DragonGate](https://github.com/dioptre/DragonGate)
- [SF Product Labs](https://sfproductlabs.com)
- IP2Location LITE data: https://lite.ip2location.com

## 📄 License

Licensed under Apache 2.0. See LICENSE for details.

Copyright (c) 2018-2024 Andrew Grosser. All Rights Reserved.

## 🔗 Related Projects

- **Horizontal web scraper**: https://github.com/dioptre/scrp
- **Chrome extension**: Tracking URL Generator (see repository)
- **Load testing**: Custom wrk fork at https://github.com/sfproductlabs/wrk

---

**Production Note**: This project is actively used in production and has demonstrated significant revenue improvements for its users. Proven at hundreds of millions of events with enterprise-grade reliability.
