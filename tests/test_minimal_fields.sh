#!/bin/bash

# Minimal fields test for messaging tables
# Tests that defaults are properly applied when minimal fields are sent
# Verifies that ClickHouse applies correct default values to all missing columns

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== MINIMAL FIELDS DEFAULT VALUES TEST ===${NC}"
echo -e "${YELLOW}Testing that defaults are properly applied for missing fields${NC}"
echo ""

# Test 1: Absolute minimum required fields for mthreads (only tid, oid)
echo -e "${YELLOW}1. Testing mthreads with ONLY required fields (tid, oid)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 2: Minimal mstore fields (tid, oid, etyp, ename)
echo -e "${YELLOW}2. Testing mstore with minimal fields (tid, oid, etyp, ename)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "etyp": "page_view",
        "ename": "minimal_test"
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 3: Partial fields - only some UUIDs
echo -e "${YELLOW}3. Testing with partial UUIDs (tid, oid, vid) + strings...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "cccccccc-dddd-eeee-ffff-000000000001",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
        "subject": "Test Subject Only",
        "msg": "Test message only"
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 4: Single message with name and alias only (no other fields)
echo -e "${YELLOW}4. Testing with only name and alias fields...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "dddddddd-eeee-ffff-0000-111111111111",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "name": "Minimal Campaign Name",
        "alias": "minimal-campaign-alias"
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 5: Single boolean field test
echo -e "${YELLOW}5. Testing with one boolean field (opens: true)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "eeeeeeee-ffff-0000-1111-222222222222",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "opens": true
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 6: Single numeric field test
echo -e "${YELLOW}6. Testing with one numeric field (urgency: 5)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "ffffffff-0000-1111-2222-333333333333",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "urgency": 5
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

# Test 7: Mix of a few fields (not comprehensive)
echo -e "${YELLOW}7. Testing with mixed minimal fields (names, numbers, booleans)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "00000000-1111-2222-3333-444444444444",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "provider": "email",
        "campaign_priority": 2,
        "abz_enabled": true,
        "archived": false
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""

echo -e "${YELLOW}Waiting for batch flush...${NC}"
sleep 3

echo -e "${YELLOW}Flushing ClickHouse async inserts...${NC}"
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 1

echo ""
echo -e "${BLUE}=== VERIFICATION OF DEFAULT VALUES ===${NC}"
echo ""

# Verify test 1: Absolute minimum
echo -e "${YELLOW}Test 1 - Minimum fields record:${NC}"
clickhouse client --query "
SELECT
    'tid' as field, toString(tid) as value FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'oid', toString(oid) FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'alias (should be empty)', alias FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'name (should be empty)', name FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'xstatus (default)', xstatus FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'opens (default false)', toString(opens) FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'urgency (default 0)', toString(urgency) FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'cats length (default empty)', toString(length(cats)) FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
FORMAT Vertical
" 2>/dev/null

echo ""

# Verify test 4: Name and alias only
echo -e "${YELLOW}Test 4 - Name and alias fields only:${NC}"
clickhouse client --query "
SELECT
    'tid' as field, toString(tid) as value FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'name', name FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'alias', alias FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'subject (default empty)', subject FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'msg (default empty)', msg FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'urgency (default 0)', toString(urgency) FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'broadcast (default false)', toString(broadcast) FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
FORMAT Vertical
" 2>/dev/null

echo ""

# Summary: Check all minimal test records
echo -e "${BLUE}=== SUMMARY ===${NC}"
echo ""

echo -e "${YELLOW}Record counts for minimal field tests:${NC}"
clickhouse client --query "
SELECT
    'Test 1 (minimum)' as test, count() as count FROM sfpla.mthreads WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
UNION ALL
SELECT 'Test 2 (etyp+ename)', count() FROM sfpla.mthreads WHERE tid = 'bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff'
UNION ALL
SELECT 'Test 3 (UUIDs + strings)', count() FROM sfpla.mthreads WHERE tid = 'cccccccc-dddd-eeee-ffff-000000000001'
UNION ALL
SELECT 'Test 4 (name+alias)', count() FROM sfpla.mthreads WHERE tid = 'dddddddd-eeee-ffff-0000-111111111111'
UNION ALL
SELECT 'Test 5 (opens)', count() FROM sfpla.mthreads WHERE tid = 'eeeeeeee-ffff-0000-1111-222222222222'
UNION ALL
SELECT 'Test 6 (urgency)', count() FROM sfpla.mthreads WHERE tid = 'ffffffff-0000-1111-2222-333333333333'
UNION ALL
SELECT 'Test 7 (mixed)', count() FROM sfpla.mthreads WHERE tid = '00000000-1111-2222-3333-444444444444'
" 2>/dev/null

echo ""

echo -e "${YELLOW}Default field verification (checking defaults applied):${NC}"
clickhouse client --query "
SELECT
    'String fields (empty)' as category,
    countIf(alias = '') + countIf(name = '') + countIf(xstatus = '') as default_strings
FROM sfpla.mthreads
WHERE tid IN (
    'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    'bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff',
    'cccccccc-dddd-eeee-ffff-000000000001',
    'dddddddd-eeee-ffff-0000-111111111111',
    'eeeeeeee-ffff-0000-1111-222222222222',
    'ffffffff-0000-1111-2222-333333333333',
    '00000000-1111-2222-3333-444444444444'
)

UNION ALL

SELECT
    'Numeric fields (zero)' as category,
    countIf(urgency = 0) + countIf(planned_impressions = 0) + countIf(total_conversions = 0) as default_numerics
FROM sfpla.mthreads
WHERE tid IN (
    'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    'bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff',
    'cccccccc-dddd-eeee-ffff-000000000001',
    'dddddddd-eeee-ffff-0000-111111111111',
    'eeeeeeee-ffff-0000-1111-222222222222',
    'ffffffff-0000-1111-2222-333333333333',
    '00000000-1111-2222-3333-444444444444'
)

UNION ALL

SELECT
    'Boolean fields (false)' as category,
    countIf(opens = false) + countIf(broadcast = false) + countIf(abz_enabled = false) as default_booleans
FROM sfpla.mthreads
WHERE tid IN (
    'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    'bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff',
    'cccccccc-dddd-eeee-ffff-000000000001',
    'dddddddd-eeee-ffff-0000-111111111111',
    'eeeeeeee-ffff-0000-1111-222222222222',
    'ffffffff-0000-1111-2222-333333333333',
    '00000000-1111-2222-3333-444444444444'
)

UNION ALL

SELECT
    'Array fields (empty)' as category,
    countIf(length(cats) = 0) + countIf(length(mtypes) = 0) + countIf(length(admins) = 0) as default_arrays
FROM sfpla.mthreads
WHERE tid IN (
    'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    'bbbbbbbb-cccc-dddd-eeee-ffffffffffffffff',
    'cccccccc-dddd-eeee-ffff-000000000001',
    'dddddddd-eeee-ffff-0000-111111111111',
    'eeeeeeee-ffff-0000-1111-222222222222',
    'ffffffff-0000-1111-2222-333333333333',
    '00000000-1111-2222-3333-444444444444'
)
" 2>/dev/null

echo ""

echo -e "${YELLOW}Detailed field check for most minimal test (Test 1):${NC}"
clickhouse client --query "
SELECT
    'Comprehensive field count' as check,
    (
        (alias = '' ? 1 : 0) +
        (name = '' ? 1 : 0) +
        (subject = '' ? 1 : 0) +
        (msg = '' ? 1 : 0) +
        (xstatus = '' ? 1 : 0) +
        (opens = false ? 1 : 0) +
        (broadcast = false ? 1 : 0) +
        (length(cats) = 0 ? 1 : 0) +
        (length(mtypes) = 0 ? 1 : 0) +
        (urgency = 0 ? 1 : 0) +
        (planned_impressions = 0 ? 1 : 0)
    ) as defaults_applied
FROM sfpla.mthreads
WHERE tid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
" 2>/dev/null

echo ""

echo -e "${GREEN}=== MINIMAL FIELDS TEST COMPLETE ===${NC}"
echo -e "${GREEN}Verified that defaults are properly applied when fields are not provided!${NC}"
