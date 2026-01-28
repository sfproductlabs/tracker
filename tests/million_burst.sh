#!/bin/bash

# 1 MILLION REQUESTS IN 1 SECOND - ULTIMATE STRESS TEST

echo "💥 1 MILLION REQUESTS IN 1 SECOND TEST"
echo "======================================="

BASE_URL="http://localhost:8880/tr/v1/tr/"
TOTAL_REQUESTS=1000000
BATCH_SIZE=10000  # Send in batches to avoid fork bomb

echo "Configuration:"
echo "- Total requests: $TOTAL_REQUESTS"
echo "- Batch size: $BATCH_SIZE"
echo "- Target: 1 second total"
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

echo ""
echo "🚀 LAUNCHING 1 MILLION REQUESTS..."
echo "=================================="

start_time=$(date +%s%N)

# Function to send a single request (minimal payload for speed)
send_request() {
    curl -s -X POST "$BASE_URL" \
        -H "Content-Type: application/json" \
        -d "{\"tid\":\"$(uuidgen)\",\"oid\":\"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",\"etyp\":\"million\"}" \
        -o /dev/null 2>&1 &
}

# Launch all requests as fast as possible
for ((i=1; i<=TOTAL_REQUESTS; i++)); do
    send_request

    # Every batch_size, wait briefly to avoid fork exhaustion
    if (( i % BATCH_SIZE == 0 )); then
        echo "   Sent $i requests..."
        wait  # Wait for batch to complete
    fi
done

# Wait for any remaining
wait

end_time=$(date +%s%N)
duration_ns=$((end_time - start_time))
duration_s=$(echo "scale=3; $duration_ns / 1000000000" | bc)

echo ""
echo "📊 RESULTS:"
echo "==========="
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
errors=$(tail -1000 /tmp/tracker_extreme.log | grep -c "ERROR\|panic\|fatal" || echo 0)
timeouts=$(tail -1000 /tmp/tracker_extreme.log | grep -c "timeout\|deadline" || echo 0)
echo "   Errors: $errors"
echo "   Timeouts: $timeouts"

if [ $errors -gt 0 ]; then
    echo ""
    echo "   Last 5 errors:"
    tail -1000 /tmp/tracker_extreme.log | grep -E "ERROR|panic|fatal" | tail -5
fi

# Check database
echo ""
echo "📈 Database check:"
sleep 10  # Give it time to process
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

final_count=$(clickhouse client --query "SELECT count() FROM sfpla.events WHERE etyp = 'million'" 2>/dev/null)
echo "   Events with etyp='million': $final_count"
echo "   Success rate: $(echo "scale=2; $final_count * 100 / $TOTAL_REQUESTS" | bc)%"

echo ""
echo "🏁 Test complete!"