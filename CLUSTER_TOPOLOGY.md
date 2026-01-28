# ClickHouse Cluster Topology Examples

This document explains the two supported cluster topologies and how to configure them using environment variables.

## Topology Modes

### Mode 1: Sharded Topology (Legacy)
**Use When**: You want horizontal scaling with data split across nodes
**Nodes**: Multiple shards, 1 replica each
**Data Distribution**: Each node stores different data

```
Node 1: Shard 1 (33% of data)
Node 2: Shard 2 (33% of data)
Node 3: Shard 3 (33% of data)
```

**Configuration**:
```bash
# Don't set REPLICA env var (or leave it empty)
-e SHARD=1
-e NUM_NODES=3
-e SERVER_ID=1
```

**Result**: 3 shards, 1 replica each

---

### Mode 2: Replicated Topology (Recommended for HA)
**Use When**: You want full redundancy with fault tolerance
**Nodes**: 1 shard, multiple replicas
**Data Distribution**: Each node stores 100% of all data

```
Node 1: Shard 1, Replica 1 (100% of data)
Node 2: Shard 1, Replica 2 (100% of data)
Node 3: Shard 1, Replica 3 (100% of data)
```

**Configuration**:
```bash
# SET REPLICA env var for each node (different value per node)
-e SHARD=1        # SAME on all nodes
-e REPLICA=1      # DIFFERENT per node (1, 2, 3)
-e NUM_NODES=3
-e SERVER_ID=1    # DIFFERENT per node (1, 2, 3)
```

**Result**: 1 shard with 3 replicas

---

### Mode 3: Hybrid Topology (Sharding + Replication)
**Use When**: You need both horizontal scaling AND fault tolerance
**Nodes**: Multiple shards, multiple replicas per shard
**Data Distribution**: Data split across shards, each shard replicated

Example: 2 Shards × 3 Replicas Each = 6 Nodes
```
Shard 1, Replica 1: 50% of data
Shard 1, Replica 2: 50% of data
Shard 1, Replica 3: 50% of data
Shard 2, Replica 1: 50% of data
Shard 2, Replica 2: 50% of data
Shard 2, Replica 3: 50% of data
```

**Configuration** (6 nodes):
```bash
# Shard 1 nodes
Node 1: -e SHARD=1 -e REPLICA=1 -e SERVER_ID=1
Node 2: -e SHARD=1 -e REPLICA=2 -e SERVER_ID=2
Node 3: -e SHARD=1 -e REPLICA=3 -e SERVER_ID=3

# Shard 2 nodes
Node 4: -e SHARD=2 -e REPLICA=1 -e SERVER_ID=4
Node 5: -e SHARD=2 -e REPLICA=2 -e SERVER_ID=5
Node 6: -e SHARD=2 -e REPLICA=3 -e SERVER_ID=6

# All nodes:
-e NUM_NODES=6
-e CLUSTER_NAME=tracker_cluster
```

**Result**:
- 2 shards (horizontal scaling)
- 3 replicas per shard (fault tolerance)
- 50% data on each shard
- Each shard can lose 2 replicas safely

**Advantages**:
- ✅ Horizontal scaling (2×3 = 6 nodes holding data efficiently)
- ✅ Fault tolerance (replicas within each shard)
- ✅ Better than pure replication (not 3× storage)

**Limitations**:
- ⚠️ More complex to manage
- ⚠️ Replication overhead per shard
- ⚠️ Requires 6+ nodes for meaningful scaling

---

## Implementation: How It Works

### entrypoint.sh Logic

The `entrypoint.sh` script detects which mode to use:

```bash
if [ -z "$REPLICA" ]; then
    # SHARDED MODE: Generate N shards, 1 replica each
    for i in $(seq 1 $NUM_NODES); do
        <shard>
            <replica>v4-tracker-${i}</replica>
        </shard>
    done
else
    # REPLICATED MODE: Generate 1 shard with N replicas
    <shard>
        <replica>v4-tracker-1</replica>
        <replica>v4-tracker-2</replica>
        <replica>v4-tracker-3</replica>
    </shard>
fi
```

**Decision Point**: Check if `REPLICA` environment variable is set

---

## Test Script Example: Replicated Topology

From `packages/tracker/tests/test-cluster.sh`:

```bash
# Node 1: Shard 1, Replica 1
docker run -d --name v4-tracker-1 \
  -e SHARD=1 \
  -e REPLICA=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  tracker

# Node 2: Shard 1, Replica 2
docker run -d --name v4-tracker-2 \
  -e SHARD=1 \
  -e REPLICA=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=3 \
  tracker

# Node 3: Shard 1, Replica 3
docker run -d --name v4-tracker-3 \
  -e SHARD=1 \
  -e REPLICA=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=3 \
  tracker
```

**Key Points**:
- `SHARD=1` on all nodes (they're all replicas of the same shard)
- `REPLICA=1,2,3` different on each node
- `SERVER_ID=1,2,3` different on each node (for Keeper coordination)
- `NUM_NODES=3` same on all nodes (tells each node about the cluster size)

---

## Generated Cluster Configuration

When the above env vars are used, `entrypoint.sh` generates:

```xml
<remote_servers>
    <tracker_cluster>
        <shard>
            <internal_replication>true</internal_replication>
            <replica><host>v4-tracker-1</host><port>9000</port></replica>
            <replica><host>v4-tracker-2</host><port>9000</port></replica>
            <replica><host>v4-tracker-3</host><port>9000</port></replica>
        </shard>
    </tracker_cluster>
</remote_servers>
```

And macros on each node:

```xml
<!-- Node 1 -->
<macros>
    <shard>1</shard>
    <replica>1</replica>
</macros>

<!-- Node 2 -->
<macros>
    <shard>1</shard>
    <replica>2</replica>
</macros>

<!-- Node 3 -->
<macros>
    <shard>1</shard>
    <replica>3</replica>
</macros>
```

---

## Verification

Check cluster status from any node:

```bash
# From Node 1
docker exec v4-tracker-1 clickhouse-client \
  --query "SELECT * FROM system.clusters WHERE cluster='tracker_cluster'"
```

Expected output (Replicated Mode):
```
tracker_cluster  1  1  1  v4-tracker-1  9000  1  0
tracker_cluster  1  2  1  v4-tracker-2  9000  1  0
tracker_cluster  1  3  1  v4-tracker-3  9000  1  0
```

Column meanings:
- `cluster` - Cluster name (tracker_cluster)
- `shard_num` - Shard number (1 for all)
- `shard_weight` - Weight (1 = equal weight)
- `replica_num` - Replica number (1, 2, 3)
- `host_name` - Hostname
- `port` - ClickHouse native port
- `is_local` - 1 if this is the local node
- `user_name` - User for connections

---

## Choosing Between Modes

| Aspect | Sharded (N:1) | Replicated (1:N) | Hybrid (M:N) |
|--------|---------------|------------------|--------------|
| **Nodes** | 3 (one shard each) | 3 (all replicas) | 6 (2 shards × 3 replicas) |
| **Fault Tolerance** | ❌ None | ✅ Full | ✅ Per-shard |
| **Horizontal Scale** | ✅ Yes (3×) | ❌ No | ✅ Yes (2×) |
| **Storage per Node** | 33% of data | 100% of data | 50% of data |
| **Write Performance** | ✅ Fastest | ❌ Slowest (3× replication) | ⚠️ Medium (replication within shard) |
| **Complexity** | Simple | Simple | Complex |
| **Best For** | Analytics, scaling | Production HA | High-volume HA |
| **SHARD values** | 1,2,3 | 1 | 1,1,1,2,2,2 |
| **REPLICA values** | (empty) | 1,2,3 | 1,2,3,1,2,3 |

### Quick Decision Tree

```
Do you need fault tolerance?
├─ NO → Use SHARDED (faster writes)
└─ YES → Do you need horizontal scaling?
    ├─ NO → Use REPLICATED (simpler, fewer nodes)
    └─ YES → Use HYBRID (most complex, best balance)
```

### Use Sharded Topology If:
- You need horizontal scaling
- Each node should have different data
- You're OK with no redundancy
- You have many nodes (4+)
- Write throughput is critical

### Use Replicated Topology If:
- You need fault tolerance (recommended for production)
- Full data redundancy is acceptable
- You want automatic failover
- You have 2-3 nodes
- You're willing to accept replication overhead

### Use Hybrid Topology If:
- You need BOTH scaling AND fault tolerance
- You have 6+ nodes available
- You can tolerate more operational complexity
- You want to minimize replication overhead while keeping HA

---

## Production Deployment Examples

### Example 1: Simple HA (3 nodes, Replicated)

For production with basic HA, use **Replicated Topology**:

```bash
# Node 1
docker run -d \
  -e SHARD=1 \
  -e REPLICA=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=3 \
  --name clickhouse-1 \
  tracker

# Node 2
docker run -d \
  -e SHARD=1 \
  -e REPLICA=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=3 \
  --name clickhouse-2 \
  tracker

# Node 3
docker run -d \
  -e SHARD=1 \
  -e REPLICA=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=3 \
  --name clickhouse-3 \
  tracker
```

**Automatic Behavior**:
- All 3 nodes have 100% of data
- Writes replicated to all nodes
- Any node failure = automatic failover for reads
- Replication lag typically <1 second
- Failed node auto-syncs on restart

---

### Example 2: High-Volume HA (6 nodes, Hybrid: 2 Shards × 3 Replicas)

For production with scaling and HA, use **Hybrid Topology**:

```bash
# Shard 1, Replica 1
docker run -d \
  -e SHARD=1 \
  -e REPLICA=1 \
  -e SERVER_ID=1 \
  -e NUM_NODES=6 \
  --name clickhouse-shard1-replica1 \
  tracker

# Shard 1, Replica 2
docker run -d \
  -e SHARD=1 \
  -e REPLICA=2 \
  -e SERVER_ID=2 \
  -e NUM_NODES=6 \
  --name clickhouse-shard1-replica2 \
  tracker

# Shard 1, Replica 3
docker run -d \
  -e SHARD=1 \
  -e REPLICA=3 \
  -e SERVER_ID=3 \
  -e NUM_NODES=6 \
  --name clickhouse-shard1-replica3 \
  tracker

# Shard 2, Replica 1
docker run -d \
  -e SHARD=2 \
  -e REPLICA=1 \
  -e SERVER_ID=4 \
  -e NUM_NODES=6 \
  --name clickhouse-shard2-replica1 \
  tracker

# Shard 2, Replica 2
docker run -d \
  -e SHARD=2 \
  -e REPLICA=2 \
  -e SERVER_ID=5 \
  -e NUM_NODES=6 \
  --name clickhouse-shard2-replica2 \
  tracker

# Shard 2, Replica 3
docker run -d \
  -e SHARD=2 \
  -e REPLICA=3 \
  -e SERVER_ID=6 \
  -e NUM_NODES=6 \
  --name clickhouse-shard2-replica3 \
  tracker
```

**Generated Topology**:
```xml
<remote_servers>
  <tracker_cluster>
    <!-- Shard 1: 50% of data with 3 replicas -->
    <shard>
      <internal_replication>true</internal_replication>
      <replica><host>clickhouse-shard1-replica1</host><port>9000</port></replica>
      <replica><host>clickhouse-shard1-replica2</host><port>9000</port></replica>
      <replica><host>clickhouse-shard1-replica3</host><port>9000</port></replica>
    </shard>
    <!-- Shard 2: 50% of data with 3 replicas -->
    <shard>
      <internal_replication>true</internal_replication>
      <replica><host>clickhouse-shard2-replica1</host><port>9000</port></replica>
      <replica><host>clickhouse-shard2-replica2</host><port>9000</port></replica>
      <replica><host>clickhouse-shard2-replica3</host><port>9000</port></replica>
    </shard>
  </tracker_cluster>
</remote_servers>
```

**Automatic Behavior**:
- Data split 50/50 between shards
- Each shard replicated to 3 nodes
- Can lose 2 nodes per shard (1 replica survives)
- Better write throughput than pure replication (replication per shard only)
- Complex failover scenarios (losing entire shard = data loss)

---

## Example Config Files

See also:
- `clickhouse-config.xml` - Single-node configuration
- `clickhouse-config.cluster-3node.example.xml` - Replicated cluster example
- `clickhouse-config.template.xml` - Template used by entrypoint.sh

---

## Environment Variable Reference

| Variable | Purpose | Replicated Mode | Sharded Mode |
|----------|---------|-----------------|--------------|
| `SHARD` | Shard number | Same (1) | Different (1,2,3) |
| `REPLICA` | Replica number | Different (1,2,3) | Omitted |
| `SERVER_ID` | Keeper server ID | Different (1,2,3) | Different (1,2,3) |
| `NUM_NODES` | Cluster size | Same (3) | Same (3) |
| `CONTAINER_NAME_PATTERN` | Container name prefix | v4-tracker | v4-tracker |
| `CLUSTER_NAME` | Cluster name | tracker_cluster | tracker_cluster |

---

## Troubleshooting

**Q: How do I switch from Sharded to Replicated mode?**
A: Change the env vars:
- Add `-e REPLICA=1` (different per node)
- Set all nodes to same SHARD

**Q: Can I mix modes?**
A: No. Each cluster is either fully sharded or fully replicated.

**Q: What if REPLICA is partially set?**
A: entrypoint.sh checks `if [ -z "$REPLICA" ]`. All nodes must have it set or none.

**Q: Cluster shows wrong topology?**
A: Verify `NUM_NODES` is correct and matches the number of containers.

