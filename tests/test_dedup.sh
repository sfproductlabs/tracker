#!/bin/bash

echo "=== Testing Deduplication for mtriage ==="

# Send the same conversion event 3 times quickly
for i in {1..3}; do
  echo "Sending duplicate event #$i..."
  curl -k -X POST https://localhost:8443/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
      "tid": "dedup-test-thread-1111-2222-3333-4444",
      "event_type": "conversion",
      "urgency": 9,
      "subject": "DUPLICATE TEST MESSAGE",
      "msg": "This should only appear once due to deduplication",
      "oid": "00000000-0000-0000-0000-000000000001"
    }'
  echo
done

echo "Waiting for batch flush..."
sleep 3

# Check how many records were actually inserted
echo "Checking mtriage table for duplicates..."
COUNT=$(clickhouse client --query "SELECT count() FROM sfpla.mtriage WHERE subject = 'DUPLICATE TEST MESSAGE' FINAL" 2>/dev/null)
echo "Records with 'DUPLICATE TEST MESSAGE': $COUNT"

if [ "$COUNT" = "1" ]; then
  echo "✓ Deduplication working! Only 1 record inserted despite 3 attempts"
else
  echo "✗ Deduplication may not be working. Found $COUNT records (expected 1)"
fi

# Also check mthreads and mstore
echo
echo "Checking other tables..."
MTHREADS_COUNT=$(clickhouse client --query "SELECT count() FROM sfpla.mthreads WHERE tid = toUUID('dedup-test-thread-1111-2222-3333-4444') FINAL" 2>/dev/null)
MSTORE_COUNT=$(clickhouse client --query "SELECT count() FROM sfpla.mstore WHERE tid = toUUID('dedup-test-thread-1111-2222-3333-4444') FINAL" 2>/dev/null)

echo "mthreads records: $MTHREADS_COUNT (with deduplication)"
echo "mstore records: $MSTORE_COUNT (with deduplication)"
