# ClickHouse Production Deployment Guide

## 🚀 Production-Ready Implementation Overview

This guide covers deploying the enhanced ClickHouse implementation with all performance optimizations, monitoring, and reliability features.

## ✅ Implementation Status

### **Core Features Completed:**
- ✅ Complete Cassandra → ClickHouse migration
- ✅ Campaign telemetry integration (mthreads, mstore, mtriage)
- ✅ Performance optimizations & connection pooling
- ✅ Enhanced error handling & circuit breaker
- ✅ Real-time monitoring & metrics
- ✅ Health checks & automatic recovery
- ✅ Comprehensive testing suite

## 🔧 Performance Optimizations Added

### **Connection Pool Settings:**
```go
MaxOpenConns:     max(configured_connections, 10)
MaxIdleConns:     max(configured_connections/2, 5)
ConnMaxLifetime:  2 hours
BlockBufferSize:  10
MaxCompressionBuffer: 10240
```

### **ClickHouse Settings:**
```go
"async_insert":                 1           // Enable async inserts
"wait_for_async_insert":        0           // Don't wait for completion
"async_insert_max_data_size":   10000000    // 10MB batch size
"async_insert_busy_timeout_ms": 200         // 200ms timeout
"max_block_size":               10000       // Optimize block size
"max_insert_block_size":        1048576     // 1MB insert blocks
"enable_http_compression":      1           // Enable compression
"http_zlib_compression_level":  1           // Light compression
```

## 📊 Monitoring & Metrics

### **Available Endpoints:**

#### **Health Check:**
```bash
GET /ping
```
Response:
```json
{
  "status": "ok",
  "health": "healthy", 
  "timestamp": "2024-01-15T10:30:00Z"
}
```

#### **Performance Metrics:**
```bash
GET /metrics
```
Response:
```json
{
  "metrics": {
    "event_count": 1500000,
    "error_count": 25,
    "campaign_events": 45000,
    "mthreads_ops": 12500,
    "mstore_ops": 89000,
    "mtriage_ops": 3400,
    "connection_count": 8,
    "health_status": "healthy",
    "start_time_unix": 1705312200
  },
  "calculated": {
    "avg_latency_ms": 2.3,
    "error_rate": 0.000017,
    "uptime_seconds": 3600,
    "events_per_second": 416.7
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### **Health Status Values:**
- `initializing` - Service starting up
- `connected` - Database connected
- `healthy` - All systems operational
- `degraded` - Some issues detected
- `unhealthy` - Critical issues
- `connection_failed` - Database connection failed
- `disconnected` - Database disconnected

## 🛡️ Error Handling & Reliability

### **Circuit Breaker:**
- **Max Failures:** 5 consecutive failures
- **Reset Timeout:** 30 seconds
- **Automatic Recovery:** Yes
- **Retry Logic:** Built-in for retryable errors

### **Error Types:**
- `connection` - Database connectivity issues
- `query` - SQL execution problems  
- `timeout` - Operation timeouts
- `validation` - Data validation failures
- `rate_limit` - Rate limiting exceeded
- `campaign` - Campaign telemetry errors
- `metrics` - Monitoring system errors

### **Timeout Settings:**
- **Connection:** 30 seconds
- **Health Check:** 5 seconds  
- **Query Execution:** 5 seconds
- **Event Processing:** 2 seconds

## 📈 Campaign Telemetry Features

### **Integrated Tables:**

#### **mthreads (Experiment Management):**
- Multi-armed bandit testing
- A/B test performance tracking
- Variant optimization
- Real-time metric updates

#### **mstore (Event Storage):**
- Individual event tracking
- Chat interactions
- Ad click events
- Parent-child event relationships

#### **mtriage (Message Queue):**
- Priority-based message processing
- Automated follow-up sequences
- High-value customer triggers
- SLA-based escalation

## 🚀 Deployment Steps

### **1. Pre-Deployment Checklist**

```bash
# Verify ClickHouse is running and accessible
clickhouse-client --query "SELECT 1"

# Check schema is up to date
clickhouse-client --multiquery < packages/api/scripts/clickhouse/schema/schema.1.ch.sql

# Run migration if needed
clickhouse-client --multiquery < packages/api/scripts/clickhouse/schema/migrate.1-2.sql

# Test connection settings
cd packages/tracker/tests && ./run_test.sh
```

### **2. Configuration Updates**

Update your `config.yaml`:
```yaml
database:
  clickhouse:
    hosts: ["localhost:9000"]  # Production ClickHouse cluster
    connections: 20            # Increase for production load
    timeout: 30000            # 30 second timeout
    context: "analytics"      # Production database
    username: "analytics_user"
    password: "${CLICKHOUSE_PASSWORD}"
    secure: true
    ca_cert: "/path/to/ca.pem"
```

### **3. Performance Testing**

```bash
# Run comprehensive integration tests
cd packages/tracker/tests
go run clickhouse_integration_test.go

# Run performance monitoring tests  
go run monitoring_test.go

# Load testing (optional)
go run usage_example.go
```

### **4. Production Deployment**

```bash
# Build optimized binary
cd packages/tracker
go build -ldflags="-s -w" -o tracker

# Start with production settings
./tracker -config=/path/to/production/config.yaml

# Verify metrics endpoint
curl http://localhost:8080/metrics | jq

# Verify health endpoint
curl http://localhost:8080/ping | jq
```

## 📊 Production Monitoring

### **Recommended Alerts:**

#### **Critical Alerts:**
- Error rate > 1%
- Average latency > 10ms
- Health status != "healthy" for > 2 minutes
- Connection count = 0

#### **Warning Alerts:**
- Error rate > 0.1%
- Average latency > 5ms  
- Events per second drops > 50%
- Circuit breaker opens

### **Monitoring Tools Integration:**

#### **Prometheus/Grafana:**
```bash
# Scrape metrics endpoint
curl http://localhost:8080/metrics
```

#### **Custom Dashboards:**
Monitor these key metrics:
- Events processed per second
- Average processing latency
- Error rates by type
- Campaign conversion rates
- Database connection health
- Memory and CPU usage

## 🔒 Security Considerations

### **Database Security:**
- Use SSL/TLS for all connections
- Implement proper authentication
- Network isolation for ClickHouse cluster
- Regular security updates

### **Application Security:**
- Input validation on all parameters
- Rate limiting on API endpoints
- Secure secret management
- Audit logging for sensitive operations

## 📋 Performance Benchmarks

### **Expected Performance (Production Hardware):**

| Metric | Target | Exceptional |
|--------|--------|-------------|
| Events/second | 1,000+ | 5,000+ |
| Average Latency | <5ms | <2ms |
| Error Rate | <0.1% | <0.01% |
| Uptime | 99.9% | 99.99% |

### **Scaling Recommendations:**

#### **Horizontal Scaling:**
- Load balance across multiple tracker instances
- Use ClickHouse cluster for high availability
- Implement Redis for session state (if needed)

#### **Vertical Scaling:**
- Increase connection pool size
- Add more CPU cores for ClickHouse
- Optimize memory allocation

## 🐛 Troubleshooting

### **Common Issues:**

#### **High Latency:**
1. Check ClickHouse query performance
2. Verify network connectivity
3. Monitor connection pool utilization
4. Review async insert settings

#### **Connection Errors:**
1. Verify ClickHouse server status
2. Check network connectivity
3. Validate authentication credentials
4. Review connection pool settings

#### **Memory Issues:**
1. Monitor batch sizes
2. Check for memory leaks
3. Optimize data structures
4. Review garbage collection

### **Debug Commands:**
```bash
# Check service health
curl http://localhost:8080/ping

# View detailed metrics
curl http://localhost:8080/metrics | jq '.calculated'

# Test database connectivity
clickhouse-client --query "SELECT COUNT(*) FROM events WHERE created_at > now() - INTERVAL 1 HOUR"

# View recent errors (if logging enabled)
tail -f /var/log/tracker/error.log
```

## 📚 Additional Resources

### **ClickHouse Documentation:**
- [Performance Optimization](https://clickhouse.com/docs/en/guides/best-practices/)
- [Monitoring Guide](https://clickhouse.com/docs/en/operations/monitoring/)

### **Testing Scripts:**
- `tests/clickhouse_integration_test.go` - Full integration testing
- `tests/monitoring_test.go` - Performance and monitoring validation
- `tests/usage_example.go` - Usage examples and scenarios

## 🎉 Success Metrics

Your deployment is successful when:

- ✅ All health checks return "healthy"
- ✅ Error rate consistently < 0.1%
- ✅ Average latency < 5ms
- ✅ Campaign telemetry data flowing correctly
- ✅ Monitoring dashboards showing expected metrics
- ✅ No circuit breaker activations under normal load

## 🔄 Maintenance

### **Regular Tasks:**
- Monitor performance metrics daily
- Review error logs weekly
- Update ClickHouse schema as needed
- Performance testing after changes
- Security updates monthly

### **Capacity Planning:**
- Monitor growth trends
- Scale before hitting limits
- Plan for peak traffic periods
- Regular performance reviews

---

**🎯 Your ClickHouse implementation is now production-ready with enterprise-grade performance, monitoring, and reliability features!**