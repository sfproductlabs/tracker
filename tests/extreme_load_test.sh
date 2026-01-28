#!/bin/bash

# EXTREME LOAD TEST - Push until it breaks!

echo "💀 EXTREME LOAD TEST: Breaking point finder"
echo "==========================================="

# Escalating load test
WAVES=(50 100 200 500 1000 2000 5000 10000)
BASE_URL="http://localhost:8880/tr/v1/tr/"

# Function to send burst
send_burst() {
    local count=$1
    local wave=$2

    echo "🌊 Wave $wave: Sending $count requests in parallel..."

    local start_time=$(date +%s%N)
    local success=0
    local failed=0

    # Create temp file for results
    local tmpfile=$(mktemp)

    # Launch all requests in parallel
    for i in $(seq 1 $count); do
        {
            curl -s -X POST "$BASE_URL" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$(uuidgen | tr '[:upper:]' '[:lower:]')\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"vid\": \"$(uuidgen | tr '[:upper:]' '[:lower:]')\",
                    \"org\": \"extreme-test-wave-$wave\",
                    \"etyp\": \"extreme_load\",
                    \"ename\": \"burst_$wave_$i\",
                    \"msg\": \"Wave $wave request $i\",
                    \"urgency\": $((RANDOM % 10))
                }" \
                -w "%{http_code}\n" \
                -o /dev/null \
                2>/dev/null >> "$tmpfile"
        } &

        # Limit parallel jobs to avoid "fork: Resource temporarily unavailable"
        if (( i % 100 == 0 )); then
            wait
        fi
    done

    # Wait for all remaining jobs
    wait

    local end_time=$(date +%s%N)
    local duration_ns=$((end_time - start_time))
    local duration_ms=$((duration_ns / 1000000))

    # Count successes and failures
    success=$(grep -c "200" "$tmpfile" 2>/dev/null || echo 0)
    failed=$(grep -vc "200" "$tmpfile" 2>/dev/null || echo 0)

    rm "$tmpfile"

    local rate=0
    if [ $duration_ms -gt 0 ]; then
        rate=$((count * 1000 / duration_ms))
    fi

    echo "   ✅ Success: $success"
    echo "   ❌ Failed: $failed"
    echo "   ⏱️  Duration: ${duration_ms}ms"
    echo "   🚀 Rate: $rate req/s"

    # If more than 10% failed, we found a limit
    if [ $failed -gt $((count / 10)) ]; then
        echo "   ⚠️  HIGH FAILURE RATE DETECTED!"
        return 1
    fi

    return 0
}

# Function to check tracker health
check_tracker_health() {
    echo "🏥 Checking tracker health..."

    # Check if tracker is responsive
    local health_check=$(curl -s -w "%{http_code}" -o /dev/null "http://localhost:8880/health" 2>/dev/null)

    if [ "$health_check" != "200" ]; then
        echo "   ❌ Tracker not responding to health check!"
        return 1
    fi

    # Check memory usage
    local tracker_pid=$(pgrep -f "tracker config.json")
    if [ ! -z "$tracker_pid" ]; then
        echo "   📊 Tracker PID: $tracker_pid"
        ps aux | grep -E "^USER|$tracker_pid" | grep -v grep
    fi

    # Check ClickHouse connection
    if clickhouse client --query "SELECT 1" &>/dev/null; then
        echo "   ✅ ClickHouse responsive"
    else
        echo "   ❌ ClickHouse not responding!"
        return 1
    fi

    return 0
}

# Function to check for errors in logs
check_logs_for_errors() {
    echo "📋 Checking logs for errors..."

    if [ -f /tmp/tracker_clean.log ]; then
        local errors=$(tail -1000 /tmp/tracker_clean.log | grep -c "ERROR\|panic\|fatal" || echo 0)
        local timeouts=$(tail -1000 /tmp/tracker_clean.log | grep -c "timeout\|deadline" || echo 0)

        echo "   Errors found: $errors"
        echo "   Timeouts found: $timeouts"

        if [ $errors -gt 10 ] || [ $timeouts -gt 10 ]; then
            echo "   ⚠️  High error rate in logs!"
            tail -20 /tmp/tracker_clean.log | grep -E "ERROR|timeout|panic|deadline"
        fi
    fi
}

# Main extreme test
echo ""
echo "🎯 Starting extreme load test..."
echo "================================"

breaking_point=0

for wave_size in "${WAVES[@]}"; do
    echo ""
    echo "======================================"
    echo "WAVE SIZE: $wave_size requests"
    echo "======================================"

    # Check health before wave
    if ! check_tracker_health; then
        echo "❌ Tracker unhealthy before wave $wave_size!"
        breaking_point=$((wave_size / 2))
        break
    fi

    # Send the burst
    if ! send_burst $wave_size $wave_size; then
        echo "❌ Wave $wave_size caused failures!"
        breaking_point=$wave_size

        # Check what broke
        check_logs_for_errors

        # Try to recover
        echo "🔧 Attempting recovery..."
        sleep 5

        if ! check_tracker_health; then
            echo "💀 TRACKER IS DEAD! Breaking point: $wave_size requests"
            break
        else
            echo "✅ Tracker recovered, continuing..."
        fi
    fi

    # Brief pause between waves
    sleep 2
done

# Final analysis
echo ""
echo "📊 EXTREME LOAD TEST COMPLETE"
echo "============================="

if [ $breaking_point -gt 0 ]; then
    echo "💥 Breaking point found: ~$breaking_point parallel requests"
else
    echo "💪 Tracker survived all waves up to ${WAVES[-1]} parallel requests!"
fi

# Database stats
echo ""
echo "📈 Database Statistics:"
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

echo -n "Total events: "
clickhouse client --query "SELECT count() FROM sfpla.events WHERE etyp IN ('extreme_load', 'stress_test')" 2>/dev/null

echo -n "Unique TIDs: "
clickhouse client --query "SELECT uniq(tid) FROM sfpla.events WHERE etyp IN ('extreme_load', 'stress_test')" 2>/dev/null

# Check for data corruption
echo ""
echo "🔍 Checking for data corruption..."
corrupted=$(clickhouse client --query "SELECT count() FROM sfpla.events WHERE tid = '00000000-0000-0000-0000-000000000000' AND etyp IN ('extreme_load', 'stress_test')" 2>/dev/null)

if [ "$corrupted" -gt 0 ]; then
    echo "⚠️  Found $corrupted records with null UUIDs!"
else
    echo "✅ No data corruption detected"
fi

echo ""
echo "🏁 Test complete!"