#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== VERIFYING DATA INTEGRITY: TEST INPUTS VS DATABASE RECORDS ===${NC}"
echo ""

# 1. Events Table Verification
echo -e "${YELLOW}1. EVENTS TABLE:${NC}"
echo "Test inputs:"
echo "  - POST: eid='test-post-1', ename='test_event'"
echo "  - GET: eid='test-get-1', ename='test_event_get'"
echo "  - Server: ename='server_event'"
echo "  - Batch: 100 events with ename='batch_event_1' to 'batch_event_100'"
echo ""
echo "Database records:"
clickhouse client --query "SELECT ename, COUNT(*) as count FROM sfpla.events WHERE ename IN ('test_event', 'test_event_get', 'server_event') OR ename LIKE 'batch_event_%' GROUP BY ename ORDER BY ename LIMIT 10" --format Pretty 2>/dev/null
echo ""

# 2. MStore Table Verification
echo -e "${YELLOW}2. MSTORE TABLE (Message Storage):${NC}"
echo "Test inputs:"
echo "  - Conversion: tid='11111111-2222-3333-4444-555555555555', subject='Test Conversion', msg='User completed purchase'"
echo "  - High Value: tid='22222222-3333-4444-5555-666666666666', subject='Premium Signup', msg='User upgraded to premium'"
echo ""
echo "Database records:"
clickhouse client --query "SELECT tid, subject, msg, urgency FROM sfpla.mstore WHERE tid IN ('11111111-2222-3333-4444-555555555555', '22222222-3333-4444-5555-666666666666')" --format Pretty 2>/dev/null
echo ""

# 3. MThreads Table Verification
echo -e "${YELLOW}3. MTHREADS TABLE (Message Threads):${NC}"
echo "Test inputs:"
echo "  - Thread 1: tid='11111111-2222-3333-4444-555555555555', alias='http://test.com/success'"
echo "  - Thread 2: tid='22222222-3333-4444-5555-666666666666', alias='http://test.com/premium'"
echo ""
echo "Database records:"
clickhouse client --query "SELECT tid, alias, provider, medium FROM sfpla.mthreads WHERE tid IN ('11111111-2222-3333-4444-555555555555', '22222222-3333-4444-5555-666666666666')" --format Pretty 2>/dev/null
echo ""

# 4. LTV Table Verification
echo -e "${YELLOW}4. LTV TABLE (Lifetime Value):${NC}"
echo "Test inputs:"
echo "  - Single payment: amt=99.99, currency='USD'"
echo "  - Batch payments: amt=50.00 and amt=25.00, currency='USD'"
echo ""
echo "Database records:"
clickhouse client --query "SELECT id_type, paid, oid FROM sfpla.ltv WHERE oid='a1b2c3d4-e5f6-7890-abcd-ef1234567890'" --format Pretty 2>/dev/null
echo ""

# 5. Payments Table Verification
echo -e "${YELLOW}5. PAYMENTS TABLE:${NC}"
echo "Test inputs:"
echo "  - Payment 1: 99.99 USD"
echo "  - Payment 2: 50.00 USD"
echo "  - Payment 3: 25.00 USD"
echo ""
echo "Database records:"
clickhouse client --query "SELECT revenue, currency, oid FROM sfpla.payments WHERE oid='a1b2c3d4-e5f6-7890-abcd-ef1234567890'" --format Pretty 2>/dev/null
echo ""

# 6. Redirects Verification
echo -e "${YELLOW}6. REDIRECTS TABLE:${NC}"
echo "Test input:"
echo "  - urlfrom='https://yourdomain.com/test-short'"
echo "  - urlto='https://example.com/long/path?utm_source=test'"
echo ""
echo "Database records:"
clickhouse client --query "SELECT urlfrom, urlto FROM sfpla.redirects WHERE urlfrom LIKE '%test-short%'" --format Pretty 2>/dev/null
echo ""

# 7. Agreements Verification
echo -e "${YELLOW}7. AGREEMENTS TABLE:${NC}"
echo "Test input:"
echo "  - vid='b8c9d0e1-f2a3-4567-abcd-ef1234567890'"
echo "  - cflags=1024, tz='America/Los_Angeles', lat=37.7749, lon=-122.4194"
echo ""
echo "Database records:"
clickhouse client --query "SELECT vid, cflags, tz, lat, lon FROM sfpla.agreements WHERE vid='b8c9d0e1-f2a3-4567-abcd-ef1234567890'" --format Pretty 2>/dev/null
echo ""

# Summary
echo -e "${GREEN}=== DATA INTEGRITY SUMMARY ===${NC}"
echo "Checking record counts..."
for table in events mstore mthreads ltv payments redirects agreements agreed mtriage; do
    count=$(clickhouse client --query "SELECT count() FROM sfpla.$table FINAL" 2>/dev/null)
    if [ "$count" -gt 0 ]; then
        echo -e "${GREEN}✓ $table: $count records${NC}"
    else
        echo -e "${RED}✗ $table: 0 records${NC}"
    fi
done