#!/bin/bash
set -e

# Set default data and log directories if not provided
export CLICKHOUSE_DATA_DIR="${CLICKHOUSE_DATA_DIR:-/var/lib/clickhouse}"
export CLICKHOUSE_LOG_DIR="${CLICKHOUSE_LOG_DIR:-/var/log/clickhouse-server}"

echo "=========================================="
echo "Tracker + ClickHouse + Clickhouse Keeper Container"
echo "=========================================="
echo "Shard: $SHARD | Replica: $REPLICA | Layer: $LAYER"
echo "Server ID: $SERVER_ID | Cluster: $CLUSTER_NAME"
echo "Container Name Pattern: $CONTAINER_NAME_PATTERN"
echo "Number of Nodes: $NUM_NODES"
echo "Data Directory: $CLICKHOUSE_DATA_DIR"
echo "Log Directory: $CLICKHOUSE_LOG_DIR"
echo "=========================================="

# Generate KEEPER_SERVERS dynamically if not provided
if [ -z "$KEEPER_SERVERS" ]; then
    echo "Auto-generating KEEPER_SERVERS..."
    KEEPER_SERVERS=""
    for i in $(seq 1 $NUM_NODES); do
        if [ $i -gt 1 ]; then
            KEEPER_SERVERS="${KEEPER_SERVERS},"
        fi
        # Use localhost for single-node deployments
        if [ "$NUM_NODES" -eq 1 ]; then
            KEEPER_SERVERS="${KEEPER_SERVERS}${i}:localhost:9444"
        else
            KEEPER_SERVERS="${KEEPER_SERVERS}${i}:${CONTAINER_NAME_PATTERN}-${i}:9444"
        fi
    done
    export KEEPER_SERVERS
    echo "Generated: $KEEPER_SERVERS"
fi

# Generate KEEPER_RAFT_CONFIG from KEEPER_SERVERS
# Format: "1:host1:port1,2:host2:port2,3:host3:port3"
echo "Generating Keeper raft configuration..."
KEEPER_RAFT_CONFIG=""
IFS=',' read -ra SERVERS <<< "$KEEPER_SERVERS"
for server in "${SERVERS[@]}"; do
    IFS=':' read -ra PARTS <<< "$server"
    KEEPER_RAFT_CONFIG+="<server><id>${PARTS[0]}</id><hostname>${PARTS[1]}</hostname><port>${PARTS[2]}</port></server>"
done
export KEEPER_RAFT_CONFIG
echo "✓ Keeper raft config generated with ${#SERVERS[@]} nodes"

# Generate CLUSTER_TOPOLOGY dynamically if not provided
if [ -z "$CLUSTER_TOPOLOGY" ]; then
    echo "Auto-generating CLUSTER_TOPOLOGY..."

    # Check if REPLICA is set - if yes, use single-shard-multiple-replicas (recommended)
    # If REPLICA is not set, fall back to sharded topology
    if [ -z "$REPLICA" ]; then
        echo "REPLICA not set - using sharded topology ($NUM_NODES shards)"
        CLUSTER_TOPOLOGY=""
        for i in $(seq 1 $NUM_NODES); do
            CLUSTER_TOPOLOGY+="<shard><internal_replication>true</internal_replication>"
            # Use localhost for single-node deployments
            if [ "$NUM_NODES" -eq 1 ]; then
                CLUSTER_TOPOLOGY+="<replica><host>localhost</host><port>9000</port></replica>"
            else
                CLUSTER_TOPOLOGY+="<replica><host>${CONTAINER_NAME_PATTERN}-${i}</host><port>9000</port></replica>"
            fi
            CLUSTER_TOPOLOGY+="</shard>"
        done
        echo "✓ Cluster topology generated with $NUM_NODES shards"
    else
        echo "REPLICA=$REPLICA set - using single-shard-multiple-replicas topology ($NUM_NODES replicas)"
        CLUSTER_TOPOLOGY="<shard><internal_replication>true</internal_replication>"
        # Use localhost for single-node deployments
        if [ "$NUM_NODES" -eq 1 ]; then
            CLUSTER_TOPOLOGY+="<replica><host>localhost</host><port>9000</port></replica>"
        else
            # Generate all replicas in a single shard
            for i in $(seq 1 $NUM_NODES); do
                CLUSTER_TOPOLOGY+="<replica><host>${CONTAINER_NAME_PATTERN}-${i}</host><port>9000</port></replica>"
            done
        fi
        CLUSTER_TOPOLOGY+="</shard>"
        echo "✓ Cluster topology generated with 1 shard and $NUM_NODES replicas"
    fi
    export CLUSTER_TOPOLOGY
fi

# Replace variables in config template
echo "Generating ClickHouse configuration..."
envsubst < /app/tracker/clickhouse-config.template.xml > /etc/clickhouse-server/config.d/cluster.xml
echo "✓ ClickHouse config generated"

# Create data and log directories
echo "Creating data and log directories..."
mkdir -p "${CLICKHOUSE_DATA_DIR}/coordination/"{log,snapshots}
mkdir -p "${CLICKHOUSE_DATA_DIR}/tmp"
mkdir -p "${CLICKHOUSE_DATA_DIR}/user_files"
mkdir -p "${CLICKHOUSE_DATA_DIR}/format_schemas"
mkdir -p "${CLICKHOUSE_LOG_DIR}"
chown -R clickhouse:clickhouse "${CLICKHOUSE_DATA_DIR}" "${CLICKHOUSE_LOG_DIR}"
echo "✓ Directories created"

# Start ClickHouse
echo "Starting ClickHouse server..."
su - clickhouse -s /bin/bash -c 'clickhouse-server --config-file=/etc/clickhouse-server/config.xml' &

# Wait for ClickHouse
echo -n "Waiting for ClickHouse"
for i in {1..30}; do
    if clickhouse-client --query "SELECT 1" &>/dev/null 2>&1; then
        echo " ✓ Ready!"
        break
    fi
    [ $i -eq 30 ] && echo " ✗ Failed!" && cat "${CLICKHOUSE_LOG_DIR}/clickhouse-server.err.log" && exit 1
    echo -n "."
    sleep 1
done

# Create database if it doesn't exist
if ! clickhouse-client --query "EXISTS DATABASE sfpla" 2>/dev/null | grep -q "1"; then
    echo "Creating sfpla database..."
    clickhouse-client --query "CREATE DATABASE IF NOT EXISTS sfpla ON CLUSTER tracker_cluster" 2>&1 && echo "✓ Database created"
fi

# Load schema if needed
if ! clickhouse-client --query "EXISTS DATABASE sfpla" 2>/dev/null | grep -q "1"; then
    echo "✗ Database creation failed"
    exit 1
fi

# Count existing tables
TABLE_COUNT=$(clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'sfpla'" 2>/dev/null || echo "0")
if [ "$TABLE_COUNT" -lt 50 ]; then
    echo "Loading schema (current tables: $TABLE_COUNT)..."
    for schema in /app/tracker/schema/*.sql; do
        if [ -f "$schema" ]; then
            echo "  → $(basename $schema)"
            if ! clickhouse-client --multiquery < "$schema" 2>&1 | tee /tmp/schema-$(basename $schema).log; then
                echo "  ✗ Error loading $(basename $schema)"
                cat /tmp/schema-$(basename $schema).log
            else
                echo "  ✓ Success"
            fi
        fi
    done
    echo "✓ Schema loaded"
else
    echo "✓ Schema already loaded ($TABLE_COUNT tables)"
fi

# Show status
echo "=========================================="
echo "Cluster Status:"
clickhouse-client --query "SELECT version() as version, getMacro('shard') as shard, getMacro('replica') as replica" 2>/dev/null || true
echo "=========================================="

# Start tracker
echo "Starting tracker..."
cd /app/tracker
exec /usr/bin/nice -n 5 /app/tracker/tracker
