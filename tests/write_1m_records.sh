#!/bin/bash

# Write 1 million records to MergeTree tables (events, mthreads, mstore, mtriage)
# Using async inserts for high performance

echo "🚀 WRITING 1 MILLION RECORDS TO MERGETREE TABLES"
echo "=============================================="
echo ""

BASE_URL="http://localhost:8880/tr/v1/tr/"
TOTAL_RECORDS=1000000
BATCH_SIZE=50000

echo "Configuration:"
echo "- Total records: $TOTAL_RECORDS"
echo "- Batch size: $BATCH_SIZE"
echo "- Target tables: events_local, mthreads_local, mstore_local, mtriage_local"
echo ""

# Check tracker health
echo "📊 Checking tracker health..."
tracker_pid=$(pgrep -f "tracker config.json")
if [ -z "$tracker_pid" ]; then
    echo "   ❌ Tracker not running!"
    exit 1
fi
echo "   ✅ Tracker running (PID: $tracker_pid)"

# Check ClickHouse async status
async_status=$(clickhouse client --query "SELECT value FROM system.settings WHERE name = 'async_insert'" 2>/dev/null)
echo "   Async inserts: $async_status"

# Check initial record counts
echo ""
echo "📈 Initial record counts:"
initial_events=$(clickhouse client --query "SELECT count() FROM sfpla.events_local" 2>/dev/null)
initial_mthreads=$(clickhouse client --query "SELECT count() FROM sfpla.mthreads_local" 2>/dev/null)
initial_mstore=$(clickhouse client --query "SELECT count() FROM sfpla.mstore_local" 2>/dev/null)
initial_mtriage=$(clickhouse client --query "SELECT count() FROM sfpla.mtriage_local" 2>/dev/null)

echo "   events_local: $initial_events"
echo "   mthreads_local: $initial_mthreads"
echo "   mstore_local: $initial_mstore"
echo "   mtriage_local: $initial_mtriage"

echo ""
echo "🚀 STARTING WRITE TEST..."
echo "=========================="

start_time=$(date +%s%N)

# Function to send a tracking event
send_event() {
    local i=$1
    curl -s -X POST "$BASE_URL" \
        -H "Content-Type: application/json" \
        -d "{
            \"tid\":\"$(uuidgen)\",
            \"vid\":\"$(uuidgen)\",
            \"uid\":\"$(uuidgen)\",
            \"oid\":\"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
            \"etyp\":\"mergetree_test\",
            \"ename\":\"test_$i\",
            \"msg\":\"Test message $i\",
            \"subject\":\"Subject $i\",
            \"org\":\"test-org\",
            \"created_at\":\"$(date -u '+%Y-%m-%dT%H:%M:%S.000Z')\"
        }" \
        -o /dev/null 2>&1 &
}

sent=0
for ((batch=1; batch<=TOTAL_RECORDS/BATCH_SIZE; batch++)); do
    echo "   Batch $batch/$((TOTAL_RECORDS/BATCH_SIZE)): Sending $BATCH_SIZE records..."

    for ((i=1; i<=BATCH_SIZE; i++)); do
        send_event $((sent + i))
    done

    sent=$((sent + BATCH_SIZE))
    wait  # Wait for batch to complete
    echo "   ✓ Sent $sent records"
done

end_time=$(date +%s%N)
duration_ns=$((end_time - start_time))
duration_s=$(echo "scale=3; $duration_ns / 1000000000" | bc)

echo ""
echo "📊 SEND COMPLETE:"
echo "================="
echo "Total records sent: $sent"
echo "Total time: ${duration_s} seconds"
echo "Rate: $(echo "scale=0; $sent / $duration_s" | bc) records/sec"

# Wait for async inserts to flush
echo ""
echo "⏳ Waiting for async insert queue to flush (15 seconds)..."
sleep 15

# Force flush
echo "🔄 Forcing async insert queue flush..."
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

# Final counts
echo ""
echo "📈 FINAL RECORD COUNTS:"
echo "======================"
final_events=$(clickhouse client --query "SELECT count() FROM sfpla.events_local" 2>/dev/null)
final_mthreads=$(clickhouse client --query "SELECT count() FROM sfpla.mthreads_local" 2>/dev/null)
final_mstore=$(clickhouse client --query "SELECT count() FROM sfpla.mstore_local" 2>/dev/null)
final_mtriage=$(clickhouse client --query "SELECT count() FROM sfpla.mtriage_local" 2>/dev/null)

events_written=$((final_events - initial_events))
mthreads_written=$((final_mthreads - initial_mthreads))
mstore_written=$((final_mstore - initial_mstore))
mtriage_written=$((final_mtriage - initial_mtriage))

echo "events_local:     $initial_events → $final_events (+$events_written)"
echo "mthreads_local:   $initial_mthreads → $final_mthreads (+$mthreads_written)"
echo "mstore_local:     $initial_mstore → $final_mstore (+$mstore_written)"
echo "mtriage_local:    $initial_mtriage → $final_mtriage (+$mtriage_written)"

total_written=$((events_written + mthreads_written + mstore_written + mtriage_written))
echo ""
echo "Total records written: $total_written"
echo "Success rate: $(echo "scale=2; $total_written * 100 / (4 * $sent)" | bc)%"
echo ""

# Check tracker health
echo "🏥 Checking tracker after write..."
if kill -0 $tracker_pid 2>/dev/null; then
    echo "   ✅ Tracker still healthy"
    mem=$(ps aux | grep "$tracker_pid" | grep -v grep | awk '{print $6}')
    echo "   Memory: ${mem}KB"
else
    echo "   ❌ Tracker crashed!"
fi

echo ""
echo "🏁 Test complete!"
