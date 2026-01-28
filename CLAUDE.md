# CLAUDE.md - Go Tracker Development Guide

This file provides guidance to Claude Code (claude.ai/code) when working with the Go Tracker codebase.

## Project Overview

**Go Tracker** is a high-performance user telemetry and event tracking system built in Go, currently handling hundreds of millions of records in production. It provides enterprise-grade tracking infrastructure comparable to what CERN, Netflix, Apple, and Github use (ClickHouse-based).

### Primary Purpose

Track every visitor interaction, run growth experiments, and measure outcomes - all while maintaining data sovereignty and GDPR compliance. Replaces Google Analytics with a privacy-first, self-hosted solution.

## Quick Reference

### Essential Commands (Use Makefile)

```bash
# Development
make help                    # Show all available commands
make build                   # Build tracker binary
make run                     # Build and run locally

# Docker Development (Recommended)
make docker-build            # Build Docker image
make docker-run              # Run single-node with ClickHouse + Keeper
make docker-verify-tables    # Verify 236 tables loaded
make docker-test-all         # Run comprehensive tests
make docker-logs             # View logs

# Testing
make test                    # Run Go unit tests
make test-single             # Run tests/test-single.sh
make test-cluster            # Run tests/test-cluster.sh
make test-db-writes          # Run tests/test_db_writes.sh (100% verification)
make test-all                # Run all tests
make test-functional-all     # Test all endpoints
make docker-test-events      # Test events table
make docker-test-messaging   # Test mthreads/mstore/mtriage

# Production Cluster
make cluster-start           # Start 3-node cluster
make cluster-test            # Verify cluster health
make cluster-stop            # Stop cluster

# Schema Management
make schema-update           # Update hard links from ../../api/scripts/clickhouse/schema/
make schema-verify           # Verify hard links intact
```

### Key File Locations

```
packages/tracker/
├── tracker.go           # Main HTTP server (52KB) - entry point
├── clickhouse.go        # ClickHouse interface (101KB) - primary database
├── batch_manager.go     # Intelligent batching (31KB) - 5-10x performance
├── cassandra.go         # Cassandra interface (62KB)
├── duckdb.go           # DuckDB interface (38KB)
├── nats.go             # NATS messaging (6KB)
├── fb.go               # Facebook CAPI (11KB)
├── utils.go            # Utilities (16KB)
├── geoip.go            # GeoIP lookup (2.7KB)
├── Makefile            # Comprehensive build system
├── Dockerfile          # Container definition
├── entrypoint.sh       # Container startup
├── config.json         # Main configuration
├── README.md           # User documentation
├── tests/              # Test scripts
│   ├── test-single.sh  # Single-node verification
│   ├── test-cluster.sh # 3-node cluster test
│   └── test_db_writes.sh # Database write verification
└── .setup/
    ├── clickhouse/     # Schema files (7 hard-linked from api)
    ├── geoip/          # IP2Location databases
    └── keys/           # TLS certificates
```

## Architecture Deep Dive

### Core Design Principles

1. **Session Interface Pattern**: All database backends implement the unified `session` interface
2. **Intelligent Batching**: 5-10x performance via advanced batch management
3. **Circuit Breakers**: Automatic failover and health checks
4. **Privacy First**: GDPR compliance built-in
5. **Horizontal Scaling**: Designed for distributed deployment

### Session Interface (tracker.go)

Every database backend implements:

```go
type session interface {
    connect() error               // Initialize connection
    close() error                 // Cleanup
    write(w *WriteArgs) error     // Write data
    serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error
    prune() error                 // Privacy pruning
    listen() error                // Listen for events
    auth(s *ServiceArgs) error    // Authentication
}
```

**Implementations**:
- `clickhouse.go` - Primary analytics database with batching
- `cassandra.go` - Optional secondary storage
- `duckdb.go` - Local analytics with S3 export
- `nats.go` - Message queue integration

### Intelligent Batching System (batch_manager.go)

**Performance Impact**: 5-10x throughput improvement

**Six Strategies**:
1. **Immediate**: No batching (payments, errors)
2. **Time-Based**: Flush every X seconds (analytics)
3. **Size-Based**: Flush when N events collected (high-volume)
4. **Hybrid**: Time OR size threshold (most common)
5. **Memory-Based**: Memory threshold (large payloads)
6. **Adaptive**: AI-driven optimization (variable load)

**Table-Specific Configuration Example**:
```go
// High-volume events
events: {
    Strategy: StrategyHybridBatch,
    MaxBatchSize: 1000,
    MaxBatchTime: 2 * time.Second,
    MaxMemoryMB: 10,
    Priority: 3,
    EnableCompression: true,
}

// Critical financial data
payments: {
    Strategy: StrategyImmediateBatch,
    MaxBatchSize: 1,
    Priority: 1,
    RetryAttempts: 5,
}
```

### Event Processing Pipeline

```
1. HTTP Request (tracker.go)
   ↓
2. Parse URL/JSON/WebSocket → Extract cookies → Validate
   ↓
3. Data Normalization → GeoIP lookup → Privacy filtering
   ↓
4. Batch Manager (batch_manager.go)
   ↓
5. Strategy Selection → Batch Accumulation → Trigger Check
   ↓
6. ClickHouse Write (clickhouse.go)
   ↓
7. Circuit Breaker Protection → Automatic Retries
   ↓
8. NATS Publish (nats.go) - Optional real-time notifications
```

## API Endpoints Reference

### Event Tracking

| Endpoint | Method | Purpose | Returns |
|----------|--------|---------|---------|
| `/tr/v1/tr/` | POST | Client-side tracking (JSON) | 200 OK |
| `/tr/v1/tr/{params}` | GET | Client-side tracking (URL) | 200 OK |
| `/tr/v1/str/` | POST | Server-side tracking | Event ID |
| `/tr/v1/img/` | GET | 1x1 pixel tracking (emails) | Image |
| `/tr/v1/ws` | WebSocket | Real-time streaming (LZ4) | Binary |

### Campaign Management

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/tr/v1/rpi/redirect/{uid}/{pw}` | POST | Create shortened URL |
| `/tr/v1/rpi/redirects/{uid}/{pw}/{host}` | GET | List redirects for host |
| `/{slug}` | GET | Redirect handler |

### Lifetime Value (LTV)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/tr/v1/ltv/` | POST | Track payment (single or batch) |

### Privacy & Compliance

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/tr/v1/ppi/agree` | POST | Store GDPR consent |
| `/tr/v1/ppi/agree?vid={id}` | GET | Get visitor agreements |
| `/tr/v1/ppi/geoip` | GET | GeoIP lookup |
| `/tr/v1/ppi/jds` | GET | List jurisdictions |

### Health & Monitoring

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Basic health check |
| `/ping` | GET | Connectivity test |
| `/status` | GET | Connection pool status |
| `/metrics` | GET | Prometheus format + batching stats |

## Database Schema

### Schema Management (CRITICAL)

**Schema files are HARD-LINKED from `../../api/scripts/clickhouse/schema/`**:

```bash
# Location of source schemas
packages/api/scripts/clickhouse/schema/
├── compliance.1.sql
├── core.1.sql
├── analytics.1.sql
├── messaging.1.sql          # Universal message system
├── users.1.sql
├── visitor_interests.1.sql
└── auth.1.sql

# Hard-linked to tracker
packages/tracker/.setup/clickhouse/
├── compliance.1.sql         # Same inode
├── core.1.sql              # Same inode
├── analytics.1.sql         # Same inode
└── ... (7 files total)
```

**IMPORTANT**: When schema changes:
1. Edit master files in `packages/api/scripts/clickhouse/schema/`
2. Run `make schema-update` to recreate hard links
3. Run `make schema-verify` to confirm
4. Test with `make docker-rebuild-test`

### Key Tables

**Core Events** (core.1.sql):
- `events` - Primary event storage (ReplicatedReplacingMergeTree)
- `events_recent` - Recent events for fast queries
- `visitors` - Unique visitor tracking
- `sessions` - Session management

**Analytics** (analytics.1.sql):
- `dailies` - Daily aggregations
- `outcomes` - Conversion tracking
- `referrers` - Traffic source analysis
- `ltv`, `ltvu`, `ltvv` - Lifetime value by visitor/user

**Messaging** (messaging.1.sql) - Universal Message System:
- `mthreads` - Thread metadata (140+ columns: tid, alias, campaign_id, abz_*, etc.)
- `mstore` - Permanent message archive (47 columns: mid, subject, msg, planned, etc.)
- `mtriage` - Messages in triage (43 columns: same as mstore minus planned)

**URL Management** (core.1.sql):
- `redirects` - Short URL mappings
- `redirect_history` - Redirect change audit

**Privacy** (compliance.1.sql):
- `agreements` - GDPR consent records
- `agreed` - Consent history
- `jurisdictions` - Privacy regions

**Total**: 236 tables when fully loaded (verified via `make docker-verify-tables`)

## Configuration (config.json)

### Core Sections

```json
{
  "Notify": [                    // Write services (databases)
    {
      "Type": "clickhouse",
      "Host": "localhost",
      "Port": 9000,
      "BatchingEnabled": true,
      "MaxBatchSize": 1000,
      "MaxBatchTime": "2s"
    }
  ],
  "Consume": [],                 // Read services (message queues)
  "Domains": ["localhost"],      // Allowed domains
  "UseLocalTLS": true,           // Use local certificates
  "LetsEncryptDomains": [],      // Or use Let's Encrypt
  "RateLimitPerDay": 10000,      // Per-IP rate limiting
  "GeoIPDatabase": ".setup/geoip/IP2LOCATION.BIN",
  "AnonymizeIP": true,           // GDPR compliance
  "DataRetentionDays": 365,
  "Debug": false                 // Enable console logging
}
```

### Service Configuration Pattern

Each service in `Notify` array implements `session` interface:
- **Type**: "clickhouse", "cassandra", "duckdb", "nats", "facebook"
- **Credentials**: Host, Port, Username, Password
- **Batching**: BatchingEnabled, MaxBatchSize, MaxBatchTime
- **Circuit Breaker**: MaxFailures, ResetTimeout

## Development Workflows

### Local Development

```bash
# 1. Start ClickHouse
clickhouse server

# 2. Build tracker
make build

# 3. Run with config
./tracker config.json

# 4. Test endpoints
curl -k https://localhost:8443/health
```

### Docker Development (Recommended)

```bash
# 1. Build and start
make docker-build
make docker-run

# 2. Wait for initialization (60s)
sleep 60

# 3. Verify setup
make docker-verify-tables  # Should show 236 tables

# 4. Run tests
make docker-test-all

# 5. Manual testing
make docker-clickhouse-shell
# Then: SELECT count() FROM sfpla.events FINAL;

# 6. View logs
make docker-logs

# 7. Clean up
make docker-stop
```

### Testing Workflow

```bash
# Functional endpoint tests (requires running tracker)
make test-functional-health      # /health, /ping, /status, /metrics
make test-functional-ltv         # LTV tracking
make test-functional-redirects   # Short URLs
make test-functional-privacy     # GDPR consent
make test-functional-batch       # Batch processing (100 events)
make test-functional-e2e         # Complete user journey
make test-functional-all         # Run ALL tests

# Docker-integrated tests
make docker-test-events          # Events table
make docker-test-messaging       # mthreads/mstore/mtriage

# Full rebuild and test
make docker-rebuild-test
```

### Cluster Development

```bash
# Start 3-node cluster
make cluster-start

# Verify cluster health
make cluster-test

# View logs from all nodes
make cluster-logs

# Stop cluster
make cluster-stop
```

## Important Development Notes

### ClickHouse Best Practices

1. **Always use FINAL** in SELECT queries (ReplicatedReplacingMergeTree)
   ```sql
   SELECT count() FROM sfpla.events FINAL;
   ```

2. **Flush async inserts** before querying
   ```sql
   SYSTEM FLUSH ASYNC INSERT QUEUE;
   ```

3. **Query batching metrics**
   ```bash
   curl http://localhost:8080/metrics | jq '.batching'
   ```

### Universal Message System Integration

The tracker integrates with the platform's universal message system via three tables:

```go
// When recording campaign events
event := map[string]interface{}{
    "tid":         threadID,        // Links to mthreads
    "mid":         messageID,       // Message identifier
    "campaign_id": campaignID,      // Campaign tracking
    "subject":     "Email Subject", // Message content
    "msg":         "Body content",  // Full message
    "urgency":     5,               // Priority level
    "status":      "sent",          // Delivery status
}
```

**Data flows**:
- `mthreads` - Thread metadata with campaign context
- `mstore` - Permanent message archive
- `mtriage` - Messages being processed (urgency=8 default)

Test with: `make docker-test-messaging`

### Privacy and Compliance

**IP Anonymization**:
- Automatic hashing before storage
- Configurable via `AnonymizeIP` in config.json
- Implemented in utils.go

**Data Pruning**:
```bash
# Run privacy pruning
./tracker --prune config.json

# Logs-only (don't delete data)
./tracker --prune --logs-only config.json
```

**GDPR Cookies**:
- `vid` - Visitor ID (persistent across sessions)
- `sid` - Session ID
- `CookieConsent` - Consent flags
- All cookies configurable in tracker.go

### Circuit Breaker Pattern

All database connections include circuit breakers:
- Max failures before opening circuit (default: 5)
- Reset timeout for recovery (default: 30s)
- Health check integration
- Implemented in each session interface

### Debugging

**Enable Debug Mode** (config.json):
```json
{
  "Debug": true
}
```

**View Logs**:
```bash
# Docker logs
make docker-logs

# ClickHouse error log
docker exec v4-tracker-1 tail -100 /var/log/clickhouse-server/clickhouse-server.err.log

# Manual docker run with console output
docker run -it --rm tracker
```

**Common Issues**:

1. **No events in database**
   ```bash
   # Flush async inserts
   clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE"
   sleep 2
   clickhouse client --query "SELECT count() FROM sfpla.events FINAL"
   ```

2. **Schema not loading**
   ```bash
   # Check schema files exist
   docker exec v4-tracker-1 ls -la /app/tracker/.setup/clickhouse/

   # Manually load
   docker exec -i v4-tracker-1 clickhouse-client --multiquery < .setup/clickhouse/core.1.sql
   ```

3. **Keeper connection issues** (cluster mode)
   ```bash
   # Check keeper running
   docker exec v4-tracker-1 clickhouse-client --query \
     "SELECT * FROM system.zookeeper WHERE path='/'"

   # Test DNS
   docker exec v4-tracker-1 ping -c 3 v4-tracker-2
   ```

## Performance Optimization

### Batching Configuration

**High-Volume Applications** (1M+ events/day):
- MaxBatchSize: 2000-5000
- MaxBatchTime: 1-2 seconds
- Strategy: StrategyHybridBatch
- Enable compression

**Low-Latency Applications** (<100ms response):
- MaxBatchSize: 100-500
- MaxBatchTime: 500ms-1s
- Strategy: StrategyHybridBatch
- Prioritize critical events

**Memory-Constrained** (<2GB available):
- MaxMemoryMB: 5-10
- Strategy: StrategyMemoryBasedBatch
- Enable adaptive optimization

### Horizontal Scaling

```
Load Balancer
    ↓
Multiple Tracker Instances (stateless)
    ↓
Shared ClickHouse Cluster
```

Each tracker instance:
- Independent event processing
- Shared ClickHouse cluster
- NATS for real-time distribution
- No session affinity required

### Monitoring Production

```bash
# Application metrics
curl http://localhost:8080/metrics

# ClickHouse metrics
docker exec tracker clickhouse-client --query \
  "SELECT * FROM system.metrics"

# Keeper metrics (cluster mode)
docker exec tracker clickhouse-client --query \
  "SELECT * FROM system.zookeeper"
```

## Production Deployment

### Docker Deployment

```bash
# Build
docker build -t tracker:latest .

# Run with persistent volumes
docker run -d --name tracker \
  -v /mnt/clickhouse:/data/clickhouse \
  -p 8080:8080 -p 8443:8443 -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  --restart unless-stopped \
  tracker:latest
```

### AWS ECS Deployment

```bash
# Use AWS Parameter Store for secrets
# Deploy via dockercmd.sh (see repository)
# Use EBS volumes for ClickHouse data
# Configure ALB for load balancing
```

### Security Checklist

- [ ] Change default ClickHouse password
- [ ] Use TLS/SSL certificates (Let's Encrypt or custom)
- [ ] Configure rate limiting
- [ ] Enable IP anonymization
- [ ] Set up backups (EBS snapshots)
- [ ] Configure CORS allowlist
- [ ] Review GeoIP database license
- [ ] Test privacy pruning

## Code Patterns and Conventions

### Adding New Database Backend

1. Implement `session` interface
2. Add to `Configuration.Notify` array
3. Handle circuit breaker pattern
4. Support batching if applicable
5. Add tests
6. Update README

### Adding New API Endpoint

1. Define route in `tracker.go` (main HTTP server)
2. Implement handler function
3. Add to appropriate service file
4. Update API documentation in README
5. Add functional test in Makefile
6. Test with `make test-functional-{name}`

### Modifying Schema

1. Edit master schema in `../../api/scripts/clickhouse/schema/`
2. Run `make schema-update` to update hard links
3. Run `make schema-verify` to confirm
4. Test with `make docker-rebuild-test`
5. Update documentation

## Key Dependencies

### Go Packages

```go
"github.com/ClickHouse/clickhouse-go/v2"  // Primary database
"github.com/gocql/gocql"                   // Cassandra
"github.com/nats-io/nats.go"               // NATS messaging
"github.com/gorilla/mux"                   // HTTP routing
"github.com/gorilla/websocket"             // WebSocket support
"github.com/bkaradzic/go-lz4"              // LZ4 compression
"github.com/google/uuid"                   // UUID generation
"golang.org/x/crypto/acme/autocert"        // Let's Encrypt
```

### External Services

- **ClickHouse**: Primary analytics database
- **ClickHouse Keeper**: ZooKeeper-compatible coordination
- **IP2Location**: GeoIP database (optional)
- **NATS**: Message queue (optional)
- **Cassandra**: Secondary storage (optional)

## Testing Infrastructure

### Test Levels

1. **Unit Tests**: `go test ./...`
2. **Integration Tests**: `make docker-test-all`
3. **Functional Tests**: `make test-functional-all`
4. **Load Tests**: Custom wrk fork (https://github.com/sfproductlabs/wrk)
5. **Cluster Tests**: `make cluster-test`

### Continuous Testing

```bash
# Watch for changes and rebuild
make watch  # Requires fswatch

# Generate coverage report
make coverage  # Creates coverage.html

# Run benchmarks
make benchmark
```

## Related Documentation

- **README.md**: User-facing documentation and setup guide
- **Makefile**: Complete command reference (`make help`)
- **DOCKER_SETUP.md**: REMOVED - content now in README.md
- **BATCHING_SYSTEM.md**: REMOVED - content now in README.md
- **config.json**: Configuration file with inline comments
- **API Documentation**: See README.md API Endpoints section

## Credits and License

- **Author**: Andrew Grosser
- **Company**: SF Product Labs (https://sfproductlabs.com)
- **License**: Apache 2.0 (see LICENSE file)
- **Related Projects**:
  - DragonGate: https://github.com/dioptre/DragonGate
  - Horizontal scraper: https://github.com/dioptre/scrp
  - Load testing: https://github.com/sfproductlabs/wrk

---

**Note to Claude**: This is a production system handling hundreds of millions of events. Always test changes thoroughly using the Makefile commands before suggesting modifications to users. Prefer using `make` commands over manual operations.
