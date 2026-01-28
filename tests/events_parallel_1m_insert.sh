#!/bin/bash

# 1 million record insert with PARALLEL batches
# Sends multiple INSERT statements concurrently instead of sequentially

echo "🚀 PARALLEL 1M RECORD INSERT - EVENTS_LOCAL TABLE"
echo "================================================="
echo ""

TOTAL_RECORDS=1000000
ROWS_PER_INSERT=10000
NUM_INSERTS=$((TOTAL_RECORDS / ROWS_PER_INSERT))
PARALLEL_JOBS=10  # Number of concurrent INSERT jobs

echo "Configuration:"
echo "- Total records: $TOTAL_RECORDS"
echo "- Rows per INSERT: $ROWS_PER_INSERT"
echo "- Number of INSERT statements: $NUM_INSERTS"
echo "- Parallel jobs: $PARALLEL_JOBS"
echo ""

# Check initial count
echo "📈 Initial record count:"
initial_events=$(clickhouse client --query "SELECT count() FROM sfpla.events_local" 2>/dev/null)
echo "   events_local: $initial_events"
echo ""

echo "🚀 STARTING PARALLEL BULK INSERT..."
echo "==================================="

start_time=$(date +%s%N)

# Create temp directory for SQL files
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Generate all INSERT statements in parallel
for ((batch=1; batch<=NUM_INSERTS; batch++)); do
    # Generate INSERT file
    sql_file="$TEMP_DIR/insert_$batch.sql"

    cat > "$sql_file" << EOF
INSERT INTO sfpla.events_local (tid, vid, uid, oid, etyp, ename, org, created_at) VALUES
EOF

    for ((i=1; i<=ROWS_PER_INSERT; i++)); do
        if [ $i -gt 1 ]; then
            echo "," >> "$sql_file"
        fi
        echo -n "(generateUUIDv4(), generateUUIDv4(), generateUUIDv4(), 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'bulk_insert', 'test_$((batch * ROWS_PER_INSERT + i))', 'test-org', now64(3))" >> "$sql_file"
    done

    # Execute in background with parallel job limit
    (
        clickhouse client --multiquery < "$sql_file" 2>/dev/null
    ) &

    # Limit concurrent jobs
    if (( batch % PARALLEL_JOBS == 0 )); then
        wait  # Wait for PARALLEL_JOBS to complete before continuing
        if (( batch % 50 == 0 )); then
            echo "   Processed $batch/$NUM_INSERTS batches..."
        fi
    fi
done

# Wait for all remaining jobs
wait

end_time=$(date +%s%N)
duration_ns=$((end_time - start_time))
duration_s=$(echo "scale=3; $duration_ns / 1000000000" | bc)

echo ""
echo "📊 INSERT COMPLETE:"
echo "=================="
echo "Total time: ${duration_s} seconds"
echo "Rate: $(echo "scale=0; $TOTAL_RECORDS / $duration_s" | bc) records/sec"

# Wait for async queue to flush
echo ""
echo "⏳ Waiting for async queue to flush (5 seconds)..."
sleep 5

echo "🔄 Forcing flush..."
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 2

# Final count
echo ""
echo "📈 FINAL RECORD COUNT:"
echo "====================="
final_events=$(clickhouse client --query "SELECT count() FROM sfpla.events_local" 2>/dev/null)

events_written=$((final_events - initial_events))

echo "events_local:     $initial_events → $final_events (+$events_written)"
echo ""
echo "Total written: $events_written"
echo "Actual rate: $(echo "scale=0; $events_written / $duration_s" | bc) records/sec"
echo ""
echo "🏁 Complete!"
