#!/bin/bash

# 1 MILLION REQUESTS WITH ASYNC INSERTS ENABLED

echo "🚀 1 MILLION REQUESTS - ASYNC INSERT MODE"
echo "========================================="
echo "✅ Async inserts ENABLED in ClickHouse"
echo ""

BASE_URL="http://localhost:8880/tr/v1/tr/"
TOTAL_REQUESTS=1000000
BATCH_SIZE=10000  # Send in batches to avoid fork bomb
CONCURRENT_WORKERS=100  # More parallel workers with async

echo "Configuration:"
echo "- Total requests: $TOTAL_REQUESTS"
echo "- Batch size: $BATCH_SIZE"
echo "- Concurrent workers: $CONCURRENT_WORKERS"
echo "- Async inserts: ENABLED"
echo "- Wait for confirmation: NO (fire-and-forget)"
echo ""

# Check initial state
echo "📊 Initial state:"
initial_count=$(clickhouse client --query "SELECT count() FROM sfpla.events" 2>/dev/null)
echo "   Events in DB: $initial_count"

tracker_pid=$(pgrep -f "tracker config.json")
echo "   Tracker PID: $tracker_pid"

# Get initial memory
initial_mem=$(ps aux | grep "$tracker_pid" | grep -v grep | awk '{print $6}')
echo "   Initial memory: ${initial_mem}KB"

# Check async insert status
async_status=$(clickhouse client --query "SELECT value FROM system.settings WHERE name = 'async_insert'" 2>/dev/null)
echo "   Async insert enabled: $async_status"

echo ""
echo "🚀 LAUNCHING 1 MILLION REQUESTS WITH ASYNC..."
echo "============================================="

start_time=$(date +%s%N)

# Function to send a single request (minimal payload for speed)
send_request() {
    curl -s -X POST "$BASE_URL" \
        -H "Content-Type: application/json" \
        -d "{\"tid\":\"$(uuidgen)\",\"oid\":\"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",\"etyp\":\"async_million\",\"ename\":\"test_$1\"}" \
        -o /dev/null 2>&1 &
}

# Launch requests with more parallelism
sent=0
for ((batch=1; batch<=TOTAL_REQUESTS/BATCH_SIZE; batch++)); do
    echo "   Batch $batch: Sending $BATCH_SIZE requests..."

    # Launch batch with high parallelism
    for ((i=1; i<=BATCH_SIZE; i++)); do
        send_request $((sent + i))

        # Limit concurrent connections
        if (( i % CONCURRENT_WORKERS == 0 )); then
            wait
        fi
    done

    sent=$((sent + BATCH_SIZE))
    echo "   ✓ Sent $sent total requests"
done

# Wait for any remaining
wait

end_time=$(date +%s%N)
duration_ns=$((end_time - start_time))
duration_s=$(echo "scale=3; $duration_ns / 1000000000" | bc)

echo ""
echo "📊 SENDING COMPLETE:"
echo "===================="
echo "Total time: ${duration_s} seconds"
echo "Rate: $(echo "scale=0; $TOTAL_REQUESTS / $duration_s" | bc) req/s"

# Check if tracker survived
echo ""
echo "🏥 Checking tracker health..."
if kill -0 $tracker_pid 2>/dev/null; then
    echo "   ✅ Tracker still alive!"

    # Check memory after
    final_mem=$(ps aux | grep "$tracker_pid" | grep -v grep | awk '{print $6}')
    echo "   Final memory: ${final_mem}KB"
    echo "   Memory increase: $((final_mem - initial_mem))KB"
else
    echo "   💀 TRACKER IS DEAD!"
fi

# Check for errors
echo ""
echo "📋 Checking logs..."
errors=$(tail -1000 /tmp/tracker_extreme.log 2>/dev/null | grep -c "ERROR\|panic\|fatal" || echo 0)
timeouts=$(tail -1000 /tmp/tracker_extreme.log 2>/dev/null | grep -c "timeout\|deadline" || echo 0)
echo "   Errors: $errors"
echo "   Timeouts: $timeouts"

# Check async insert queues
echo ""
echo "📈 Async insert queue status:"
clickhouse client --query "
    SELECT
        database,
        table,
        total_rows,
        total_bytes,
        first_update,
        last_update
    FROM system.asynchronous_insert_log
    WHERE database = 'sfpla'
    ORDER BY last_update DESC
    LIMIT 5
" 2>/dev/null || echo "   (No async insert log available)"

# Wait for async inserts to complete
echo ""
echo "⏳ Waiting for async inserts to flush (10 seconds)..."
sleep 10

# Force flush
echo "🔄 Forcing async insert queue flush..."
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

# Final count
echo ""
echo "📊 FINAL RESULTS:"
echo "================="
final_count=$(clickhouse client --query "SELECT count() FROM sfpla.events WHERE etyp = 'async_million'" 2>/dev/null)
echo "   Events with etyp='async_million': $final_count"
echo "   Success rate: $(echo "scale=2; $final_count * 100 / $TOTAL_REQUESTS" | bc)%"
echo "   Events per second stored: $(echo "scale=0; $final_count / ($duration_s + 10)" | bc)"

# Performance comparison
echo ""
echo "📊 PERFORMANCE COMPARISON:"
echo "========================="
echo "Without async inserts: ~250-300 req/s (observed)"
echo "With async inserts: $(echo "scale=0; $TOTAL_REQUESTS / $duration_s" | bc) req/s (current)"
echo "Improvement: $(echo "scale=1; ($TOTAL_REQUESTS / $duration_s) / 275" | bc)x faster"

echo ""
echo "🏁 Async insert test complete!"