# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a **high-performance user telemetry and tracking system** built in Go, designed to handle hundreds of millions of records in production. The system serves as a comprehensive user analytics platform that can replace Google Analytics while providing enhanced privacy controls and data sovereignty.

### Core Architecture Components

- **Go HTTP Server**: High-performance web server with TLS/SSL support and Let's Encrypt integration
- **Multiple Database Backends**: ClickHouse (primary), Cassandra, DuckDB support with intelligent connection pooling
- **Advanced Batching System**: Intelligent ClickHouse batching with 5-10x performance improvements
- **Real-time Messaging**: NATS integration for distributed event processing
- **Privacy-First Design**: GDPR compliance with configurable data retention and IP anonymization
- **Reverse Proxy**: Built-in proxy functionality replacing much of Traefik/NGINX

## Key Commands

### Development and Building
```bash
# Build the tracker
go build

# Run with custom config
./tracker config.json

# Build with dependencies and Docker
./build.sh

# Run the tracker (production)
./run.sh

# Clean and setup directories
rm -rf pdb pdbwal tmp && mkdir pdb pdbwal tmp
```

### Testing
```bash
# Run ClickHouse integration tests
cd tests && ./run_test.sh

# Test basic connectivity
curl -k https://localhost:8443/ping

# Test tracking endpoint
curl -k --header "Content-Type: application/json" \
  --request POST \
  --data '{"vid":"test-id","ename":"test_event","etyp":"test"}' \
  https://localhost:8443/tr/v1/tr/
```

### Database Operations
```bash
# Connect to ClickHouse and check data
clickhouse client --query "SELECT COUNT(*) FROM sfpla.events"

# Flush batch queue before querying
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2

# Run privacy pruning
./tracker --prune config.json

# Logs-only pruning
./tracker --prune --logs-only config.json
```

### Development with Docker
```bash
# Start development environment (ClickHouse, NATS, Cassandra)
docker-compose up

# Build and run tracker in Docker
sudo docker build -t tracker .
sudo docker run -p 8443:443 tracker
```

## Configuration Architecture

### Core Configuration (config.json)

The system uses a single JSON configuration file with several key sections:

#### **Database Connections (Notify array)**
- **ClickHouse**: Primary analytics database with batching enabled
- **Cassandra**: Optional secondary storage (can be skipped)
- **DuckDB**: Local analytics with S3 export capabilities
- **NATS**: Message queue for distributed processing
- **Facebook**: CAPI integration for marketing attribution

#### **Security and Performance**
- **TLS Configuration**: Local certificates or Let's Encrypt
- **Rate Limiting**: Configurable daily limits per IP
- **Connection Pooling**: Maximum connections and timeouts
- **Circuit Breakers**: Automatic failover and health checks

#### **Privacy Controls**
- **GeoIP Integration**: IP2Location database for geography
- **Data Pruning**: Automatic PII removal and retention policies
- **Cookie Consent**: GDPR-compliant consent management

## High-Performance Batching System

### Intelligent Batching Strategies

The ClickHouse integration includes an advanced batching system that provides **5-10x performance improvements**:

#### **Strategy Types**
- **Immediate**: No batching for critical events (payments, errors)
- **Time-Based**: Flush every X seconds for periodic aggregation
- **Size-Based**: Flush when N events collected for high-volume processing
- **Hybrid**: Flush on time OR size threshold (most common)
- **Memory-Based**: Flush when memory threshold hit
- **Adaptive**: AI-driven optimization based on load patterns

#### **Table-Specific Configuration**
```go
// High-volume events table
events: {
    Strategy: StrategyHybridBatch,
    MaxBatchSize: 1000,
    MaxBatchTime: 2 * time.Second,
    MaxMemoryMB: 10,
    EnableCompression: true,
}

// Critical financial data
payments: {
    Strategy: StrategyImmediateBatch,
    MaxBatchSize: 1,
    Priority: 1, // Highest priority
    RetryAttempts: 5,
}
```

## API Architecture

### Core Tracking Endpoints

#### **Event Tracking**
- `POST /tr/v1/tr/` - Primary tracking endpoint (JSON)
- `GET /tr/v1/img/` - 1x1 pixel tracking (emails)
- `POST /tr/v1/str/` - Server-side tracking
- `WebSocket /tr/v1/ws` - Real-time tracking with LZ4 compression

#### **URL Shortening and Redirects**
- `GET /` - Redirect handler for shortened URLs
- `POST /tr/v1/rpi/redirect/{uid}/{password}` - Create shortened URL
- `GET /tr/v1/rpi/redirects/{uid}/{password}/{host}` - List redirects

#### **Privacy and Compliance**
- `POST /tr/v1/ppi/agree` - Cookie consent management
- `GET /tr/v1/ppi/geoip` - GeoIP lookup for privacy compliance
- `GET /tr/v1/ppi/jds` - Jurisdictions for data residency

#### **Lifetime Value (LTV)**
- `POST /tr/v1/ltv/` - Track customer lifetime value events

## Data Architecture

### Event Processing Pipeline

1. **HTTP Request** → **URL/JSON Parsing** → **Cookie Processing**
2. **Data Normalization** → **GeoIP Lookup** → **Privacy Filtering**
3. **Batching System** → **Database Write** → **Real-time Notifications**

### Database Schema

The system supports multiple database schemas through the TableType system:

#### **Core Event Tables**
- `events` - Primary event storage
- `events_recent` - Recent events for fast queries
- `visitors` - Unique visitor tracking
- `sessions` - Session management

#### **Analytics Tables**
- `dailies` - Daily aggregations
- `outcomes` - Conversion tracking
- `referrers` - Traffic source analysis
- `ltv` - Lifetime value calculations

### Cookie and Privacy Management

#### **Standard Cookies**
- `vid` - Visitor ID (persistent across sessions)
- `sid` - Session ID
- `CookieConsent` - GDPR consent flags
- `ehash` - Email hash for attribution
- `user` - Encrypted user data (JWT)

## Production Deployment

### Performance Monitoring

The system includes comprehensive monitoring via `/metrics` endpoint:

```json
{
  "batching": {
    "total_batches": 15420,
    "avg_batch_size": 100,
    "avg_flush_latency_ms": 25,
    "batch_success_rate": 0.9992,
    "events_per_second": 2840.5
  }
}
```

### Scaling Considerations

#### **Horizontal Scaling**
- Multiple tracker instances behind load balancer
- Shared ClickHouse cluster for analytics
- NATS clustering for message distribution
- Docker Swarm or ECS deployment

#### **Performance Tuning**
- Batch sizes: 1000-5000 for high volume
- Connection pooling: 5-10 connections per service
- Memory limits: 50-100MB per batch manager
- Circuit breaker: 5 failures in 30 seconds

### Security Features

- **TLS Termination**: Let's Encrypt or custom certificates
- **Rate Limiting**: Per-IP daily request limits
- **CORS Support**: Configurable origin allowlists
- **IP Anonymization**: Automatic PII hashing
- **Certificate Validation**: Client certificate support for internal services

## Important Development Notes

### Code Architecture Patterns

#### **Session Interface**
All database backends implement the `session` interface:
```go
type session interface {
    connect() error
    close() error
    write(w *WriteArgs) error
    serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error
    prune() error
}
```

#### **Service Configuration**
Services are configured through the `Service` struct with pluggable backends:
- `Configuration.Notify[]` - Write services (databases)
- `Configuration.Consume[]` - Read services (message queues)

#### **Circuit Breaker Pattern**
All database connections include circuit breakers for reliability:
- Max failures before opening circuit
- Reset timeout for recovery attempts
- Health check integration

### Testing and Development

#### **Local Development Setup**
1. Start ClickHouse: `clickhouse server`
2. Configure domains in `config.json`
3. Generate or use test certificates
4. Run tracker: `./tracker config.json`

#### **Production Health Checks**
- `/ping` - Basic connectivity
- `/status` - Connection pool status
- `/metrics` - Comprehensive performance metrics

### Privacy and Compliance

#### **GDPR Features**
- Automatic IP hashing and anonymization
- Configurable data retention periods
- Cookie consent management
- Right to be forgotten via pruning

#### **Data Residency**
- Per-jurisdiction data filtering
- Configurable geographic restrictions
- Local data processing requirements

## Common File Locations

### Configuration
- `config.json` - Main configuration file
- `.setup/keys/` - TLS certificates
- `.setup/geoip/` - GeoIP databases

### Source Code Structure
- `tracker.go` - Main HTTP server and routing
- `clickhouse.go` - ClickHouse database interface
- `batch_manager.go` - Intelligent batching system
- `cassandra.go` - Cassandra interface
- `nats.go` - NATS messaging interface
- `utils.go` - Utility functions and helpers

### Data Directories
- `pdb/` - Pebble database storage
- `pdbwal/` - Write-ahead log
- `tmp/` - Temporary files
- `public/` - Static web content

### Testing
- `tests/` - Integration tests
- `tests/run_test.sh` - Test runner script