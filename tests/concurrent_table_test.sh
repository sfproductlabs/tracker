#!/bin/bash

# CONCURRENT TABLE STRESS TEST - Hit all tables simultaneously

echo "🎯 CONCURRENT MULTI-TABLE STRESS TEST"
echo "====================================="

BASE_URL="http://localhost:8880"
CONCURRENT_OPERATIONS=50

# Function to generate random data
generate_uuid() {
    uuidgen | tr '[:upper:]' '[:lower:]'
}

# Test 1: Bombard all endpoints simultaneously
test_all_endpoints() {
    echo "💣 Test 1: Hitting all endpoints simultaneously..."

    local start_time=$(date +%s)

    for i in $(seq 1 $CONCURRENT_OPERATIONS); do
        # Regular tracking event
        {
            curl -s -X POST "$BASE_URL/tr/v1/tr/" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$(generate_uuid)\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"vid\": \"$(generate_uuid)\",
                    \"org\": \"concurrent-test\",
                    \"etyp\": \"concurrent_test\",
                    \"ename\": \"event_$i\",
                    \"msg\": \"Testing concurrent writes to all tables\",
                    \"audience\": \"tester\",
                    \"content\": \"stress\"
                }" -o /dev/null 2>&1
        } &

        # Server-side event
        {
            curl -s -X POST "$BASE_URL/tr/v1/str/" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$(generate_uuid)\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"secret\": \"test\",
                    \"etyp\": \"server_concurrent\",
                    \"ename\": \"server_$i\"
                }" -o /dev/null 2>&1
        } &

        # LTV event
        {
            curl -s -X POST "$BASE_URL/tr/v1/ltv/" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$(generate_uuid)\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"uid\": \"$(generate_uuid)\",
                    \"vid\": \"$(generate_uuid)\",
                    \"invoice_id\": \"$(generate_uuid)\",
                    \"amount\": $((RANDOM % 10000)),
                    \"status\": \"completed\"
                }" -o /dev/null 2>&1
        } &

        # Redirect creation
        {
            curl -s -X POST "$BASE_URL/tr/v1/rpi/redirect/testuser/testpass" \
                -H "Content-Type: application/json" \
                -d "{
                    \"urlfrom\": \"http://short.url/test$i\",
                    \"urlto\": \"http://destination.com/page$i\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"uid\": \"$(generate_uuid)\",
                    \"org\": \"concurrent-test\"
                }" -o /dev/null 2>&1
        } &

        # Agreement/GDPR consent
        {
            curl -s -X POST "$BASE_URL/tr/v1/ppi/agree" \
                -H "Content-Type: application/json" \
                -d "{
                    \"vid\": \"$(generate_uuid)\",
                    \"cflags\": $((RANDOM % 100)),
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"org\": \"concurrent-test\"
                }" -o /dev/null 2>&1
        } &
    done

    # Wait for all background jobs
    wait

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo "   ✅ Completed in ${duration}s"
    echo "   📊 Rate: $((CONCURRENT_OPERATIONS * 5 / duration)) total ops/s"
}

# Test 2: Rapid fire same TID updates (race condition test)
test_race_conditions() {
    echo ""
    echo "🏁 Test 2: Race condition test (same TID updates)..."

    local shared_tid=$(generate_uuid)
    local shared_vid=$(generate_uuid)
    local start_time=$(date +%s)

    for i in $(seq 1 100); do
        {
            curl -s -X POST "$BASE_URL/tr/v1/tr/" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$shared_tid\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"vid\": \"$shared_vid\",
                    \"org\": \"race-test\",
                    \"etyp\": \"race_condition\",
                    \"ename\": \"update_$i\",
                    \"msg\": \"Race condition test update $i\",
                    \"urgency\": $((RANDOM % 10))
                }" -o /dev/null 2>&1
        } &
    done

    wait

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo "   ✅ 100 concurrent updates to same TID in ${duration}s"
}

# Test 3: Maximum payload size test
test_large_payloads() {
    echo ""
    echo "📦 Test 3: Large payload stress test..."

    # Generate a very large message
    local huge_msg=$(python3 -c "print('X' * 100000)")  # 100KB message
    local huge_array=$(python3 -c "import json; print(json.dumps(['item' + str(i) for i in range(1000)]))")

    for i in $(seq 1 10); do
        {
            curl -s -X POST "$BASE_URL/tr/v1/tr/" \
                -H "Content-Type: application/json" \
                -d "{
                    \"tid\": \"$(generate_uuid)\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"vid\": \"$(generate_uuid)\",
                    \"org\": \"large-payload-test\",
                    \"etyp\": \"huge_payload\",
                    \"ename\": \"large_$i\",
                    \"msg\": \"$huge_msg\",
                    \"cats\": $huge_array,
                    \"mtypes\": $huge_array,
                    \"data\": {
                        \"huge_nested\": {
                            \"array\": $huge_array,
                            \"message\": \"$huge_msg\"
                        }
                    }
                }" -o /dev/null 2>&1 &
        } &
    done

    wait
    echo "   ✅ Large payload test complete"
}

# Test 4: Connection pool exhaustion
test_connection_exhaustion() {
    echo ""
    echo "🔌 Test 4: Connection pool exhaustion test..."

    # Hold connections open
    for i in $(seq 1 200); do
        {
            curl -s -X POST "$BASE_URL/tr/v1/tr/" \
                -H "Content-Type: application/json" \
                -H "Connection: keep-alive" \
                --max-time 30 \
                -d "{
                    \"tid\": \"$(generate_uuid)\",
                    \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
                    \"vid\": \"$(generate_uuid)\",
                    \"org\": \"connection-test\",
                    \"etyp\": \"connection_exhaust\",
                    \"ename\": \"conn_$i\"
                }" -o /dev/null 2>&1
        } &

        # Slight delay to avoid fork bomb
        if (( i % 50 == 0 )); then
            sleep 0.1
        fi
    done

    wait
    echo "   ✅ Connection exhaustion test complete"
}

# Test 5: Invalid data injection
test_invalid_data() {
    echo ""
    echo "💉 Test 5: Invalid data injection test..."

    # Test with invalid UUIDs
    curl -s -X POST "$BASE_URL/tr/v1/tr/" \
        -H "Content-Type: application/json" \
        -d '{
            "tid": "not-a-uuid",
            "oid": "also-not-a-uuid",
            "vid": "invalid",
            "org": null,
            "etyp": "",
            "ename": null,
            "msg": null,
            "urgency": "not-a-number",
            "data": "not-json"
        }' -o /dev/null 2>&1

    # Test with SQL injection attempts
    curl -s -X POST "$BASE_URL/tr/v1/tr/" \
        -H "Content-Type: application/json" \
        -d "{
            \"tid\": \"$(generate_uuid)\",
            \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
            \"org\": \"'; DROP TABLE events; --\",
            \"ename\": \"1' OR '1'='1\",
            \"msg\": \"<script>alert('XSS')</script>\"
        }" -o /dev/null 2>&1

    # Test with extremely long field values
    local long_string=$(python3 -c "print('A' * 1000000)")  # 1MB string
    curl -s -X POST "$BASE_URL/tr/v1/tr/" \
        -H "Content-Type: application/json" \
        -d "{
            \"tid\": \"$(generate_uuid)\",
            \"oid\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",
            \"org\": \"$long_string\"
        }" -o /dev/null 2>&1

    echo "   ✅ Invalid data injection test complete"
}

# Main execution
echo ""
echo "🚀 Starting concurrent table stress tests..."
echo "==========================================="

# Check initial state
echo ""
echo "📊 Initial database state:"
initial_events=$(clickhouse client --query "SELECT count() FROM sfpla.events" 2>/dev/null)
initial_mthreads=$(clickhouse client --query "SELECT count() FROM sfpla.mthreads" 2>/dev/null)
echo "   Events: $initial_events"
echo "   mthreads: $initial_mthreads"

# Run all tests
test_all_endpoints
test_race_conditions
test_large_payloads
test_connection_exhaustion
test_invalid_data

# Check tracker health
echo ""
echo "🏥 Checking tracker health after stress..."
tracker_health=$(curl -s -w "%{http_code}" -o /dev/null "http://localhost:8880/health" 2>/dev/null)

if [ "$tracker_health" = "200" ]; then
    echo "   ✅ Tracker still healthy!"
else
    echo "   ❌ Tracker not responding! Health check returned: $tracker_health"
fi

# Check for errors in log
echo ""
echo "📋 Checking for errors in tracker log..."
if [ -f /tmp/tracker_clean.log ]; then
    errors=$(tail -500 /tmp/tracker_clean.log | grep -c "panic\|fatal\|ERROR" || echo 0)
    timeouts=$(tail -500 /tmp/tracker_clean.log | grep -c "deadline\|timeout" || echo 0)
    echo "   Errors: $errors"
    echo "   Timeouts: $timeouts"

    if [ $errors -gt 0 ]; then
        echo ""
        echo "   Recent errors:"
        tail -500 /tmp/tracker_clean.log | grep -E "panic|fatal|ERROR" | tail -5
    fi
fi

# Final database check
echo ""
echo "📊 Final database state:"
sleep 5  # Wait for batches
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

final_events=$(clickhouse client --query "SELECT count() FROM sfpla.events" 2>/dev/null)
final_mthreads=$(clickhouse client --query "SELECT count() FROM sfpla.mthreads" 2>/dev/null)
final_mtriage=$(clickhouse client --query "SELECT count() FROM sfpla.mtriage" 2>/dev/null)

echo "   Events: $initial_events → $final_events ($(($final_events - $initial_events)) new)"
echo "   mthreads: $initial_mthreads → $final_mthreads ($(($final_mthreads - $initial_mthreads)) new)"
echo "   mtriage: $final_mtriage total"

# Check for data integrity
echo ""
echo "🔍 Data integrity check..."
null_tids=$(clickhouse client --query "SELECT count() FROM sfpla.events WHERE tid = '00000000-0000-0000-0000-000000000000'" 2>/dev/null)
if [ "$null_tids" -gt 0 ]; then
    echo "   ⚠️  Warning: $null_tids records with null TIDs"
else
    echo "   ✅ No null TIDs found"
fi

echo ""
echo "🏁 Concurrent table stress test complete!"