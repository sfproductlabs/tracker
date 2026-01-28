#!/bin/bash

# STRESS TEST - Find the breaking point of the Go tracker

echo "🔥 STRESS TEST: Finding the breaking point..."
echo "============================================"

# Configuration
BASE_URL="http://localhost:8880/tr/v1/tr/"
PARALLEL_REQUESTS=100
ITERATIONS=10
TOTAL_REQUESTS=$((PARALLEL_REQUESTS * ITERATIONS))

echo "Configuration:"
echo "- Parallel requests: $PARALLEL_REQUESTS"
echo "- Iterations: $ITERATIONS"
echo "- Total requests: $TOTAL_REQUESTS"
echo ""

# Function to generate random UUID
generate_uuid() {
    echo "$(uuidgen | tr '[:upper:]' '[:lower:]')"
}

# Function to send a single request
send_request() {
    local tid=$(generate_uuid)
    local vid=$(generate_uuid)
    local uid=$(generate_uuid)
    local oid="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    local iteration=$1
    local request_num=$2

    # Generate massive payload with all fields
    local json_payload=$(cat <<EOF
{
    "tid": "$tid",
    "oid": "$oid",
    "vid": "$vid",
    "uid": "$uid",
    "org": "stress-test-org-$iteration",
    "url": "http://stresstest.com/page/$iteration/$request_num",
    "ip": "192.168.$((RANDOM % 255)).$((RANDOM % 255))",
    "etyp": "stress_test",
    "ename": "load_test_$iteration_$request_num",
    "audience": "stress_tester",
    "content": "performance_test",
    "source": "stress",
    "medium": "automated",
    "campaign": "stress_campaign_$iteration",
    "term": "stress_term_$((RANDOM % 1000))",
    "score": $((RANDOM % 100)),
    "duration": $((RANDOM % 3600)),
    "lat": $(echo "scale=6; $RANDOM/1000" | bc),
    "lon": $(echo "scale=6; $RANDOM/1000" | bc),
    "subject": "Stress test message with very long subject line that should test the limits of the database field storage capacity and see if truncation happens correctly",
    "msg": "$(printf 'This is a very long message that repeats. %.0s' {1..100})",
    "urgency": $((RANDOM % 10)),
    "alias": "stress-alias-$tid",
    "campaign_id": "campaign-$(generate_uuid)",
    "abz_algorithm": "stress_sampling",
    "planned_impressions": $((RANDOM * 1000)),
    "total_conversions": $((RANDOM * 100)),
    "conversion_value": $(echo "scale=2; $RANDOM * 100" | bc),
    "data": {
        "nested": {
            "deeply": {
                "nested": {
                    "json": {
                        "structure": "with lots of data",
                        "array": [1,2,3,4,5,6,7,8,9,10],
                        "more": "data to stress test JSON parsing"
                    }
                }
            }
        }
    },
    "cats": ["cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7", "cat8", "cat9", "cat10"],
    "mtypes": ["type1", "type2", "type3", "type4", "type5"],
    "admins": ["$(generate_uuid)", "$(generate_uuid)", "$(generate_uuid)"],
    "perms_ids": ["$(generate_uuid)", "$(generate_uuid)", "$(generate_uuid)", "$(generate_uuid)", "$(generate_uuid)"]
}
EOF
    )

    # Send request and capture response
    response=$(curl -s -X POST "$BASE_URL" \
        -H "Content-Type: application/json" \
        -d "$json_payload" \
        -w "\n%{http_code}" \
        2>/dev/null)

    http_code=$(echo "$response" | tail -n1)

    if [ "$http_code" != "200" ]; then
        echo "❌ Request $iteration-$request_num failed with HTTP $http_code"
        return 1
    fi

    return 0
}

# Function to run parallel requests
run_parallel_batch() {
    local iteration=$1
    local start_time=$(date +%s)

    echo "📊 Iteration $iteration: Sending $PARALLEL_REQUESTS parallel requests..."

    # Use GNU parallel or xargs for parallel execution
    if command -v parallel &> /dev/null; then
        seq 1 $PARALLEL_REQUESTS | parallel -j $PARALLEL_REQUESTS send_request $iteration {}
    else
        # Fallback to background jobs
        for i in $(seq 1 $PARALLEL_REQUESTS); do
            send_request $iteration $i &
        done
        wait
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo "✅ Iteration $iteration completed in ${duration}s"
    echo "   Rate: $((PARALLEL_REQUESTS / duration)) req/s"

    return 0
}

# Main stress test loop
echo ""
echo "🚀 Starting stress test..."
echo "=========================="

start_time=$(date +%s)
failed_iterations=0

for i in $(seq 1 $ITERATIONS); do
    if ! run_parallel_batch $i; then
        ((failed_iterations++))
    fi

    # Brief pause between iterations to avoid overwhelming
    sleep 0.5
done

end_time=$(date +%s)
total_duration=$((end_time - start_time))

echo ""
echo "📈 Stress Test Results:"
echo "======================="
echo "Total requests sent: $TOTAL_REQUESTS"
echo "Total duration: ${total_duration}s"
echo "Average rate: $((TOTAL_REQUESTS / total_duration)) req/s"
echo "Failed iterations: $failed_iterations"

# Check database for results
echo ""
echo "🔍 Checking database..."
sleep 5  # Wait for batches to flush

clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

echo -n "Events in database: "
clickhouse client --query "SELECT count() FROM sfpla.events WHERE etyp = 'stress_test'" 2>/dev/null

echo -n "mthreads records: "
clickhouse client --query "SELECT count() FROM sfpla.mthreads WHERE org LIKE 'stress-test-org-%'" 2>/dev/null

echo -n "mstore records: "
clickhouse client --query "SELECT count() FROM sfpla.mstore" 2>/dev/null

echo ""
echo "✅ Stress test phase 1 complete!"