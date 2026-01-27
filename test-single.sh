#!/bin/bash
set -e

echo "=== Testing Single-Node Tracker with ClickHouse + Keeper ==="
echo ""

# Clean up any existing container
echo "Cleaning up old containers..."
docker stop tracker 2>/dev/null || true
docker rm tracker 2>/dev/null || true

# Create test directories in /tmp
echo "Creating test directories in /tmp..."
mkdir -p /tmp/clickhouse-test/data
mkdir -p /tmp/clickhouse-test/logs
echo "✓ Test directories created"

echo ""
echo "Starting single-node tracker with persistent volumes..."
docker run -d --name tracker \
  -v /tmp/clickhouse-test/data:/data/clickhouse \
  -v /tmp/clickhouse-test/logs:/logs/clickhouse \
  -p 8080:8080 \
  -p 9000:9000 \
  -p 8123:8123 \
  -p 2181:2181 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
  tracker

echo ""
echo "Waiting for container to start (60 seconds)..."
sleep 60

echo ""
echo "=== Container Logs ==="
docker logs tracker

echo ""
echo "=== Testing ClickHouse ==="
docker exec tracker clickhouse-client --query "SELECT version() as version" 2>&1 || echo "ClickHouse not ready yet"

echo ""
echo "=== Testing Database ==="
docker exec tracker clickhouse-client --query "SHOW DATABASES" 2>&1 || echo "Database query failed"

echo ""
echo "=== Checking Cluster Config ==="
docker exec tracker clickhouse-client --query "SELECT * FROM system.clusters" 2>&1 || echo "Cluster query failed"

echo ""
echo "=== Testing Tracker ==="
curl -v http://localhost:8080/health 2>&1 || echo "Tracker not responding"

echo ""
echo "=== Verifying Persistent Storage ==="
echo "Data directory contents:"
ls -lah /tmp/clickhouse-test/data/ 2>&1 || echo "Data directory not accessible"
echo ""
echo "Log directory contents:"
ls -lah /tmp/clickhouse-test/logs/ 2>&1 || echo "Log directory not accessible"
echo ""
echo "Checking data directory inside container:"
docker exec tracker ls -lah /data/clickhouse 2>&1 || echo "Container path not accessible"

echo ""
echo "Test complete! To clean up:"
echo "  docker stop tracker && docker rm tracker"
echo "  rm -rf /tmp/clickhouse-test"
