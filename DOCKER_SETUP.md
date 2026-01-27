# Tracker with ClickHouse + Keeper - Docker Setup

Complete Docker container that includes Go Tracker, ClickHouse Server, and ClickHouse Keeper with automatic cluster configuration.

## Features

- **Go Tracker**: High-performance event tracking service (port 8080)
- **ClickHouse Server**: Native (9000) and HTTP (8123) interfaces
- **ClickHouse Keeper**: ZooKeeper-compatible coordination (port 2181)
- **Auto Schema Loading**: Loads SQL schemas on first startup from `.setup/clickhouse/*.sql`
- **Dynamic Cluster Configuration**: Auto-generates keeper servers and topology from `NUM_NODES`
- **Container Name Discovery**: Uses Docker DNS for cluster member resolution

## Key Innovation: Dynamic Cluster Discovery

Instead of hardcoding IPs and keeper servers, the system auto-generates configuration based on container naming pattern:

### Environment Variables:
- `CONTAINER_NAME_PATTERN="v4-tracker"` - Base name for containers (default)
- `NUM_NODES=3` - Number of nodes in cluster
- `SHARD=1` - This node's shard number (must be unique per node)
- `SERVER_ID=1` - Keeper server ID (must match shard number)

### What Gets Auto-Generated:

With `NUM_NODES=3` and `CONTAINER_NAME_PATTERN="v4-tracker"`, the entrypoint script automatically creates:

```bash
KEEPER_SERVERS="1:v4-tracker-1:9444,2:v4-tracker-2:9444,3:v4-tracker-3:9444"

CLUSTER_TOPOLOGY="
  <shard><internal_replication>true</internal_replication>
    <replica><host>v4-tracker-1</host><port>9000</port></replica>
  </shard>
  <shard><internal_replication>true</internal_replication>
    <replica><host>v4-tracker-2</host><port>9000</port></replica>
  </shard>
  <shard><internal_replication>true</internal_replication>
    <replica><host>v4-tracker-3</host><port>9000</port></replica>
  </shard>
"
```

## Files Included

1. **Dockerfile** - Container definition with ClickHouse installation
2. **entrypoint.sh** - Startup script with dynamic config generation
3. **clickhouse-config.template.xml** - ClickHouse server config template
4. **clickhouse-users.xml** - User authentication and quotas configuration
5. **test-single.sh** - Test script for single-node setup
6. **test-cluster.sh** - Test script for 3-node cluster
7. **DOCKER_SETUP.md** - This documentation

## Quick Start

### Single Node (simplest):

```bash
# Build
docker build -t tracker .

# Run
docker run -d --name tracker \
  -p 8080:8080 \
  -p 9000:9000 \
  -p 8123:8123 \
  -p 2181:2181 \
  tracker

# Test
docker logs -f tracker
docker exec tracker clickhouse-client --query "SELECT version()"
```

### 3-Node Cluster (same machine, different ports):

```bash
# Create network for container DNS resolution
docker network create tracker-net

# Node 1
docker run -d --name v4-tracker-1 \
  --network tracker-net \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  tracker

# Node 2
docker run -d --name v4-tracker-2 \
  --network tracker-net \
  -p 8081:8080 -p 9001:9000 -p 8124:8123 -p 2182:2181 -p 9445:9444 \
  -e SHARD=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=3 \
  tracker

# Node 3
docker run -d --name v4-tracker-3 \
  --network tracker-net \
  -p 8082:8080 -p 9002:9000 -p 8125:8123 -p 2183:2181 -p 9446:9444 \
  -e SHARD=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=3 \
  tracker
```

### 3-Node Cluster (different machines):

On each physical machine, containers must be able to resolve `v4-tracker-1`, `v4-tracker-2`, `v4-tracker-3` hostnames. Use DNS, `/etc/hosts`, or Docker Swarm for cross-host networking.

```bash
# Machine 1
docker run -d --name v4-tracker-1 \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  tracker

# Machine 2
docker run -d --name v4-tracker-2 \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=3 \
  tracker

# Machine 3
docker run -d --name v4-tracker-3 \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=3 \
  tracker
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SHARD` | `1` | Shard number for this node (1-N) |
| `REPLICA` | `1` | Replica number for this node |
| `LAYER` | `1` | Layer identifier for multi-layer setups |
| `SERVER_ID` | `1` | Unique server ID for Keeper (must match shard) |
| `CONTAINER_NAME_PATTERN` | `v4-tracker` | Base name for auto-discovery |
| `NUM_NODES` | `1` | Total number of nodes in cluster |
| `KEEPER_SERVERS` | _(auto)_ | Comma-separated list: `id:host:port` |
| `CLUSTER_NAME` | `tracker_cluster` | Name of the ClickHouse cluster |
| `CLUSTER_TOPOLOGY` | _(auto)_ | XML fragment defining shards/replicas |
| `CLICKHOUSE_DATA_DIR` | `/var/lib/clickhouse` | ClickHouse data directory path |
| `CLICKHOUSE_LOG_DIR` | `/var/log/clickhouse-server` | ClickHouse log directory path |

**Note:** If `KEEPER_SERVERS` or `CLUSTER_TOPOLOGY` are empty, they'll be auto-generated from `NUM_NODES` and `CONTAINER_NAME_PATTERN`.

## Ports

| Port | Service | Description |
|------|---------|-------------|
| 8080 | Tracker | HTTP API |
| 9000 | ClickHouse | Native protocol (TCP) |
| 8123 | ClickHouse | HTTP interface |
| 2181 | Keeper | ZooKeeper-compatible |
| 9009 | ClickHouse | Interserver communication |
| 9444 | Keeper | Raft protocol |

## Verifying Cluster Setup

```bash
# Check ClickHouse version
docker exec v4-tracker-1 clickhouse-client --query "SELECT version()"

# Check cluster configuration
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.clusters WHERE cluster='tracker_cluster'"

# Check keeper status
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.zookeeper WHERE path='/'"

# Check replicas health
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.replicas WHERE database='sfpla'"

# Test distributed query across cluster
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT hostName(), count() FROM cluster('tracker_cluster', system.one) GROUP BY hostName()"

# Verify database exists
docker exec v4-tracker-1 clickhouse-client --query "SHOW DATABASES"

# Check schema loaded
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT count() FROM system.tables WHERE database='sfpla'"
```

## Testing Commands

```bash
# View container logs
docker logs -f v4-tracker-1

# Check ClickHouse error log
docker exec v4-tracker-1 tail -100 /var/log/clickhouse-server/clickhouse-server.err.log

# Test tracker API
curl http://localhost:8080/health

# Interactive ClickHouse client
docker exec -it v4-tracker-1 clickhouse-client

# View generated config
docker exec v4-tracker-1 cat /etc/clickhouse-server/config.d/cluster.xml
```

## Troubleshooting

### Container won't start

```bash
# Check logs
docker logs v4-tracker-1

# Check ClickHouse logs
docker exec v4-tracker-1 tail -100 /var/log/clickhouse-server/clickhouse-server.err.log

# Verify files copied correctly
docker exec v4-tracker-1 ls -la /app/tracker/
```

### Schema not loading

```bash
# Check if schema files exist
docker exec v4-tracker-1 ls -la /app/tracker/schema/

# Manually load schema
docker exec -i v4-tracker-1 clickhouse-client --multiquery < .setup/clickhouse/core.1.sql

# Check for errors
docker exec v4-tracker-1 clickhouse-client --query "SHOW CREATE DATABASE sfpla"
```

### Keeper connection issues

```bash
# Check keeper is running
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.zookeeper WHERE path='/'"

# Verify keeper servers config
docker exec v4-tracker-1 cat /etc/clickhouse-server/config.d/cluster.xml | grep keeper

# Test connectivity to other nodes
docker exec v4-tracker-1 ping -c 3 v4-tracker-2
docker exec v4-tracker-1 nslookup v4-tracker-2
```

### Cluster nodes can't see each other

1. **Check network**: `docker network inspect tracker-net`
2. **Verify KEEPER_SERVERS**: Should list all 3 nodes with correct hostnames
3. **Check DNS resolution**: `docker exec v4-tracker-1 ping v4-tracker-2`
4. **Verify port 9444 open**: Raft communication requires this port
5. **Check CLUSTER_TOPOLOGY**: Should have correct replica hosts

### Replicas show total_replicas=0

```bash
# This means replicas didn't register with Keeper
# Check keeper is accessible
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.replicas WHERE database='sfpla' AND total_replicas=0"

# Try restarting the container
docker restart v4-tracker-1
```

## Production Considerations

### 1. Persistent Storage

Mount volumes for data persistence using environment variables to configure custom paths:

```bash
# Method 1: Using default paths with volume mounts
docker run -d --name v4-tracker-1 \
  -v /data/clickhouse:/var/lib/clickhouse \
  -v /logs/clickhouse:/var/log/clickhouse-server \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
  tracker

# Method 2: Using custom paths with CLICKHOUSE_DATA_DIR and CLICKHOUSE_LOG_DIR
# Perfect for AWS EBS volumes or other mounted storage
docker run -d --name v4-tracker-1 \
  -v /mnt/ebs-data:/data/clickhouse \
  -v /mnt/ebs-logs:/logs/clickhouse \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
  tracker

# Method 3: Using docker-compose (see docker-compose.example.yml)
# Recommended for production deployments
```

**AWS EBS Volume Example**:
```bash
# 1. Create and attach EBS volume to EC2 instance
# 2. Format and mount the volume
sudo mkfs -t ext4 /dev/xvdf
sudo mkdir -p /mnt/clickhouse-data
sudo mount /dev/xvdf /mnt/clickhouse-data

# 3. Run tracker with custom data directory
docker run -d --name tracker \
  -v /mnt/clickhouse-data:/data/clickhouse \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/data/clickhouse/logs" \
  tracker
```

**Docker Compose Example** (see `docker-compose.example.yml`):
```yaml
version: '3.8'
services:
  tracker:
    image: tracker:latest
    environment:
      - CLICKHOUSE_DATA_DIR=/data/clickhouse
      - CLICKHOUSE_LOG_DIR=/logs/clickhouse
    volumes:
      - /mnt/ebs-volume:/data/clickhouse
      - /mnt/ebs-logs:/logs/clickhouse
    ports:
      - "8080:8080"
      - "9000:9000"
      - "8123:8123"
      - "2181:2181"
```

### 2. Memory Limits

Set appropriate memory limits:

```bash
docker run -d --name v4-tracker-1 \
  --memory=4g \
  --memory-swap=4g \
  ...
```

### 3. Security

- Change default ClickHouse password in `clickhouse-users.xml`
- Use network policies to restrict access
- Enable SSL/TLS for production

### 4. Monitoring

```bash
# ClickHouse metrics
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.metrics"

# Keeper metrics
docker exec v4-tracker-1 clickhouse-client --query \
  "SELECT * FROM system.zookeeper"
```

### 5. Backups

Regular backups of `/var/lib/clickhouse`:

```bash
docker exec v4-tracker-1 clickhouse-client --query \
  "BACKUP DATABASE sfpla TO Disk('default', 'backup_$(date +%Y%m%d).zip')"
```

## Architecture Benefits

1. **No hardcoded IPs** - Uses Docker DNS for container discovery
2. **Easy scaling** - Just change `NUM_NODES` environment variable
3. **Template-based** - Clean separation of config and logic via `envsubst`
4. **Self-documenting** - Container naming pattern clearly indicates cluster membership
5. **Production-ready** - Includes system tuning, health checks, and proper logging

## Persistent Storage Configuration

### Overview

The tracker container supports configurable data and log directories via environment variables, making it easy to use AWS EBS volumes or any other persistent storage.

### Environment Variables

- **`CLICKHOUSE_DATA_DIR`** (default: `/var/lib/clickhouse`)
  - Contains all ClickHouse data files, coordination logs, and snapshots
  - This is where your database data lives
  - Mount to persistent storage for production

- **`CLICKHOUSE_LOG_DIR`** (default: `/var/log/clickhouse-server`)
  - Contains ClickHouse server logs and error logs
  - Important for troubleshooting and monitoring

### Directory Structure

When you set custom paths, the entrypoint creates this structure:

```
${CLICKHOUSE_DATA_DIR}/
├── coordination/
│   ├── log/              # Keeper Raft logs
│   └── snapshots/        # Keeper snapshots
├── tmp/                  # Temporary files
├── user_files/           # User uploaded files
├── format_schemas/       # Custom format schemas
└── data/                 # Actual database files (created by ClickHouse)

${CLICKHOUSE_LOG_DIR}/
├── clickhouse-server.log     # Main server log
└── clickhouse-server.err.log # Error log
```

### AWS Deployment Guide

#### Single EC2 Instance with EBS

```bash
# 1. Create EBS volume (e.g., 100GB gp3)
aws ec2 create-volume \
  --availability-zone us-east-1a \
  --size 100 \
  --volume-type gp3 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=clickhouse-data}]'

# 2. Attach to EC2 instance
aws ec2 attach-volume \
  --volume-id vol-xxxxx \
  --instance-id i-xxxxx \
  --device /dev/xvdf

# 3. Format and mount (on EC2 instance)
sudo mkfs -t ext4 /dev/xvdf
sudo mkdir -p /mnt/clickhouse
sudo mount /dev/xvdf /mnt/clickhouse
sudo chown -R 999:999 /mnt/clickhouse  # ClickHouse UID in container

# 4. Add to /etc/fstab for auto-mount on reboot
echo '/dev/xvdf /mnt/clickhouse ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab

# 5. Run tracker
docker run -d --name tracker \
  -v /mnt/clickhouse:/data/clickhouse \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/data/clickhouse/logs" \
  --restart unless-stopped \
  tracker
```

#### Multi-Node Cluster with EBS per Node

```bash
# On each EC2 instance, repeat steps 1-4 above
# Then run with cluster configuration:

# Node 1
docker run -d --name v4-tracker-1 \
  -v /mnt/clickhouse:/data/clickhouse \
  --network tracker-net \
  -p 8080:8080 -p 9000:9000 -p 8123:8123 -p 2181:2181 -p 9444:9444 \
  -e SHARD=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  -e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
  -e CLICKHOUSE_LOG_DIR="/data/clickhouse/logs" \
  --restart unless-stopped \
  tracker
```

### Using Docker Compose for Production

Create a `docker-compose.yml` based on `docker-compose.example.yml`:

```yaml
version: '3.8'

services:
  tracker:
    image: tracker:latest
    container_name: tracker
    restart: unless-stopped
    ports:
      - "8080:8080"
      - "9000:9000"
      - "8123:8123"
      - "2181:2181"
    environment:
      - SHARD=1
      - REPLICA=1
      - SERVER_ID=1
      - NUM_NODES=1
      - CLUSTER_NAME=tracker_cluster
      - CLICKHOUSE_DATA_DIR=/data/clickhouse
      - CLICKHOUSE_LOG_DIR=/logs/clickhouse
    volumes:
      - /mnt/ebs-volume:/data/clickhouse
      - /mnt/ebs-logs:/logs/clickhouse
```

Then deploy:
```bash
docker-compose up -d
```

### Backup Strategies

#### Using EBS Snapshots (Recommended for AWS)

```bash
# Create snapshot
aws ec2 create-snapshot \
  --volume-id vol-xxxxx \
  --description "Tracker ClickHouse data $(date +%Y%m%d)"

# Automated daily backups with cron
cat > /usr/local/bin/backup-clickhouse.sh <<'EOF'
#!/bin/bash
VOLUME_ID="vol-xxxxx"
aws ec2 create-snapshot \
  --volume-id $VOLUME_ID \
  --description "Daily backup $(date +%Y%m%d)" \
  --tag-specifications 'ResourceType=snapshot,Tags=[{Key=Type,Value=Automated},{Key=Date,Value='$(date +%Y%m%d)'}]'
EOF
chmod +x /usr/local/bin/backup-clickhouse.sh

# Add to crontab (daily at 2am)
echo "0 2 * * * /usr/local/bin/backup-clickhouse.sh" | crontab -
```

#### Using ClickHouse BACKUP Command

```bash
# Backup to local disk
docker exec tracker clickhouse-client --query \
  "BACKUP DATABASE sfpla TO Disk('default', 'backup_$(date +%Y%m%d).zip')"

# The backup will be in ${CLICKHOUSE_DATA_DIR}/backups/
```

### Monitoring Storage

```bash
# Check disk usage
docker exec tracker df -h /data/clickhouse

# Check ClickHouse disk usage
docker exec tracker clickhouse-client --query \
  "SELECT
    database,
    table,
    formatReadableSize(sum(bytes)) as size
   FROM system.parts
   GROUP BY database, table
   ORDER BY sum(bytes) DESC"
```

## Clean Up

```bash
# Stop and remove containers
docker stop v4-tracker-1 v4-tracker-2 v4-tracker-3
docker rm v4-tracker-1 v4-tracker-2 v4-tracker-3

# Remove network
docker network rm tracker-net

# Remove image
docker rmi tracker
```
