#!/bin/bash
set -e

echo "Building tracker image..."
docker build -t tracker .

echo ""
echo "Creating Docker network..."
docker network create tracker-net 2>/dev/null || echo "Network already exists"

# Create test directories for each node in /tmp
echo ""
echo "Creating test directories in /tmp for 3-node cluster..."
mkdir -p /tmp/clickhouse-cluster/node1/{data,logs}
mkdir -p /tmp/clickhouse-cluster/node2/{data,logs}
mkdir -p /tmp/clickhouse-cluster/node3/{data,logs}
echo "✓ Test directories created"

echo ""
echo "Starting 3-node cluster with persistent volumes..."

# Start node 1 (Shard 1, Replica 1)
echo "Starting v4-tracker-1 (Shard 1, Replica 1 of all data)..."
docker run -d --name v4-tracker-1 \
  --network tracker-net \
  -v /tmp/clickhouse-cluster/node1/data:/data/clickhouse \
  -v /tmp/clickhouse-cluster/node1/logs:/logs/clickhouse \
  -p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=1 \
  -e REPLICA=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  -e CONTAINER_NAME_PATTERN="v4-tracker" \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
  tracker

# Start node 2 (Shard 1, Replica 2)
echo "Starting v4-tracker-2 (Shard 1, Replica 2 of all data)..."
docker run -d --name v4-tracker-2 \
  --network tracker-net \
  -v /tmp/clickhouse-cluster/node2/data:/data/clickhouse \
  -v /tmp/clickhouse-cluster/node2/logs:/logs/clickhouse \
  -p 8081:8080 -p 8444:8443 -p 8881:8880 -p 9001:9000 -p 8124:8123 -p 2182:2181 -p 9445:9444 \
  -e SHARD=1 \
  -e REPLICA=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=3 \
  -e CONTAINER_NAME_PATTERN="v4-tracker" \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
  tracker

# Start node 3 (Shard 1, Replica 3)
echo "Starting v4-tracker-3 (Shard 1, Replica 3 of all data)..."
docker run -d --name v4-tracker-3 \
  --network tracker-net \
  -v /tmp/clickhouse-cluster/node3/data:/data/clickhouse \
  -v /tmp/clickhouse-cluster/node3/logs:/logs/clickhouse \
  -p 8082:8080 -p 8445:8443 -p 8882:8880 -p 9002:9000 -p 8125:8123 -p 2183:2181 -p 9446:9444 \
  -e SHARD=1 \
  -e REPLICA=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=3 \
  -e CONTAINER_NAME_PATTERN="v4-tracker" \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
  tracker

echo ""
echo "Waiting for containers to start..."
sleep 5

echo ""
echo "Checking logs..."
echo "=== v4-tracker-1 ==="
docker logs v4-tracker-1 | tail -20

echo ""
echo "Cluster started! Access points:"
echo "  Node 1: http://localhost:8080 (ClickHouse: 9000, 8123)"
echo "  Node 2: http://localhost:8081 (ClickHouse: 9001, 8124)"
echo "  Node 3: http://localhost:8082 (ClickHouse: 9002, 8125)"
echo ""
echo "To check cluster status:"
echo "  docker exec v4-tracker-1 clickhouse-client --query \"SELECT * FROM system.clusters WHERE cluster='tracker_cluster'\""
echo ""
echo "To verify persistent storage:"
echo "  ls -lah /tmp/clickhouse-cluster/node1/data/"
echo "  ls -lah /tmp/clickhouse-cluster/node2/data/"
echo "  ls -lah /tmp/clickhouse-cluster/node3/data/"
echo ""
echo "To stop cluster:"
echo "  docker stop v4-tracker-1 v4-tracker-2 v4-tracker-3"
echo "  docker rm v4-tracker-1 v4-tracker-2 v4-tracker-3"
echo "  rm -rf /tmp/clickhouse-cluster"
