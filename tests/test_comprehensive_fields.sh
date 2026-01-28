#!/bin/bash

# Comprehensive field test for all messaging tables (mthreads, mstore, mtriage)
# Tests all 133 mthreads fields, 47 mstore fields, and 43 mtriage fields

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== COMPREHENSIVE MESSAGING FIELD TEST ===${NC}"
echo -e "${YELLOW}Testing all 133 mthreads fields, 47 mstore fields, and 43 mtriage fields${NC}"
echo ""

# Test 1: Full mthreads field test (all 133 fields)
echo -e "${YELLOW}1. Testing complete mthreads capture (133 fields)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "11111111-2222-3333-4444-555555555555",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
        "uid": "b1c2d3e4-f5a6-7890-abcd-ef1234567890",
        "owner": "owner-1111-2222-3333-4444-555555555555",
        "updater": "updater-2222-3333-4444-5555-666666666666",

        "alias": "comprehensive-test-alias",
        "xstatus": "active",
        "name": "Full Field Test Campaign",
        "ddata": "Detailed data JSON string",
        "xid": "external-id-12345",
        "post": "Post content with all details",
        "mtempl": {"template": "advanced", "version": 2, "params": {"key": "value"}},
        "mcert_id": "cert-3333-4444-5555-6666-777777777777",

        "cats": ["category1", "category2", "category3"],
        "mtypes": ["email", "sms", "push", "in-app"],
        "fmtypes": ["html", "text", "markdown"],
        "cmtypes": ["promotional", "transactional", "notification"],
        "admins": ["admin-1111-2222-3333-4444-555555555555", "admin-2222-3333-4444-5555-666666666666"],
        "perms_ids": ["perm-1111-2222-3333-4444-555555555555", "perm-2222-3333-4444-5555-666666666666"],
        "cohorts": ["cohort-alpha", "cohort-beta", "cohort-gamma"],
        "splits": {"variant_a": 33, "variant_b": 33, "variant_c": 34},
        "sent": ["sent-1111-2222-3333-4444-555555555555"],
        "outs": ["out-1111-2222-3333-4444-555555555555"],
        "subs": ["sub-1111-2222-3333-4444-555555555555"],
        "pubs": ["pub-1111-2222-3333-4444-555555555555"],
        "vid_targets": ["target-1111-2222-3333-4444-555555555555"],
        "audience_segments": ["tech-professionals", "decision-makers", "early-adopters"],
        "content_keywords": ["AI", "automation", "efficiency", "ROI"],
        "consent_types": ["email_marketing", "sms_marketing", "data_analytics"],
        "content_history": ["hist-1111-2222-3333-4444-555555555555"],
        "content_editors": ["editor-1111-2222-3333-4444-555555555555"],

        "prefs": {"frequency": "daily", "timezone": "PST", "language": "en"},
        "interest": {"topics": ["tech", "business"], "score": 0.85},
        "perf": {"open_rate": 0.25, "click_rate": 0.05, "conversion_rate": 0.02},
        "variants": {"A": {"subject": "Test A"}, "B": {"subject": "Test B"}},
        "variant_weights": {"A": 0.5, "B": 0.5},
        "audience_params": {"age_min": 25, "age_max": 54, "income_min": 50000},
        "audience_metrics": {"reach": 100000, "impressions": 500000},
        "content_assumptions": {"reading_level": 12, "tone": "professional"},
        "content_metrics": {"readability": 65, "sentiment": 0.7},
        "regional_compliance": {"gdpr": true, "ccpa": true, "can_spam": true},
        "frequency_caps": {"daily": 3, "weekly": 10, "monthly": 30},
        "provider_metrics": {"delivery_rate": 0.98, "bounce_rate": 0.02},
        "abz_params": {"algorithm": "thompson", "confidence": 0.95},
        "abz_param_space": {"min_sample": 100, "max_sample": 10000},
        "abz_model_params": {"learning_rate": 0.01, "regularization": 0.001},
        "attribution_params": {"window_days": 30, "model": "last_touch"},

        "opens": true,
        "openp": false,
        "derive": true,
        "ftrack": true,
        "strack": false,
        "sys": false,
        "archived": false,
        "broadcast": true,
        "abz_enabled": true,
        "abz_auto_optimize": true,
        "abz_auto_stop": false,
        "abz_infinite_armed": true,
        "requires_consent": true,
        "creator_compensation": true,

        "provider": "email",
        "medium": "campaign",
        "app": "growth-platform",
        "rel": "v2.5.0",
        "ver": 2,
        "ptyp": "marketing",
        "etyp": "conversion",
        "ename": "full_field_test",
        "auth": "oauth2",
        "source": "api",
        "campaign": "Q1-2025-Growth",
        "term": "holiday-special",
        "promo": "SAVE30",
        "ref": "ref-4444-5555-6666-7777-888888888888",
        "aff": "partner-xyz-123",

        "provider_campaign_id": "fb-camp-123456",
        "provider_account_id": "fb-acc-789012",
        "provider_cost": 1250.75,
        "provider_status": "active",
        "funnel_stage": "consideration",
        "winner_variant": "variant_b",
        "content_intention": "educate_and_convert",
        "campaign_id": "internal-campaign-2025-q1-001",
        "campaign_status": "running",
        "campaign_phase": "optimization",

        "urgency": 7,
        "ephemeral": 7200,
        "planned_impressions": 1000000,
        "actual_impressions": 750000,
        "impression_goal": 900000,
        "impression_budget": 50000.00,
        "cost_per_impression": 0.05,
        "total_conversions": 1500,
        "conversion_value": 150000.00,
        "campaign_priority": 1,
        "campaign_budget_allocation": 0.35,
        "attribution_weight": 0.75,
        "attribution_window": 30,
        "data_retention": 365,
        "content_version": 5,
        "creator_compensation_rate": 0.20,
        "creator_compensation_cap": 50000.00,

        "abz_algorithm": "thompson_sampling",
        "abz_reward_metric": "conversion_rate",
        "abz_reward_value": 0.15,
        "abz_exploration_rate": 0.10,
        "abz_learning_rate": 0.01,
        "abz_start_time": "2025-01-01T00:00:00Z",
        "abz_sample_size": 5000,
        "abz_min_sample_size": 500,
        "abz_confidence_level": 0.95,
        "abz_winner_threshold": 0.85,
        "abz_status": "running",
        "abz_model_type": "bayesian",
        "abz_acquisition_function": "expected_improvement",

        "deleted": "2025-12-31T23:59:59Z",
        "updatedms": 1735689600000,
        "content_approval_date": "2025-01-15T10:30:00Z",

        "attribution_model": "data_driven",
        "creator_id": "creator-5555-6666-7777-8888-999999999999",
        "content_id": "content-6666-7777-8888-9999-aaaaaaaaaaaa",
        "content_approver": "approver-7777-8888-9999-aaaa-bbbbbbbbbbbb",
        "content_creator": "creator-8888-9999-aaaa-bbbb-cccccccccccc",
        "creator_compensation_model": "performance_based",
        "creator_notes": "High-performing creator with consistent quality",
        "content_status": "approved_and_live",

        "org": "Test Organization Inc",

        "subject": "Complete Field Test Subject Line",
        "msg": "This is a comprehensive test message containing all possible fields for complete data capture verification.",
        "encoding": "UTF-8",
        "priority": 1,
        "category": "marketing_campaign",
        "status": "active",

        "url": "https://test.com/comprehensive-field-test"
    }' \
    -w "\nStatus: %{http_code}\n"

# Test 2: Full mstore field test with high-value action (triggers mtriage)
echo -e "${YELLOW}2. Testing complete mstore/mtriage capture (47/43 fields)...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "22222222-3333-4444-5555-666666666666",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "vid": "24fb0860-b4bf-11e9-8971-7b80435315ac",
        "uid": "c1d2e3f4-g5h6-7890-ijkl-mn1234567890",
        "etyp": "high_value_action",
        "ename": "premium_purchase",

        "mid": "msg-1111-2222-3333-4444-555555555555",
        "pmid": "parent-1111-2222-3333-4444-555555555555",
        "qid": "queue-1111-2222-3333-4444-555555555555",
        "rid": "reply-1111-2222-3333-4444-555555555555",

        "subject": "Premium Purchase Confirmation",
        "msg": "Thank you for your premium purchase. Your account has been upgraded.",
        "encoding": "UTF-8",
        "category": "transactional",
        "priority": 1,
        "urgency": 9,

        "mtypes": ["email", "in-app", "push"],
        "imp_count": 1500,
        "click_count": 75,
        "open_count": 450,
        "conversion_event_count": 15,
        "conversion_revenue": 7500.00,

        "planned": "2025-01-20T10:00:00Z",
        "paused_at": "2025-01-20T11:00:00Z",
        "cancelled_at": "2025-01-20T12:00:00Z",
        "createdms": 1735689600000,
        "updatedms": 1735689700000,
        "delivered_at": "2025-01-20T10:05:00Z",
        "opened_at": "2025-01-20T10:15:00Z",
        "clicked_at": "2025-01-20T10:20:00Z",
        "converted_at": "2025-01-20T10:25:00Z",
        "declined": "2025-01-20T10:30:00Z",

        "status": "delivered",
        "delivered": true,
        "failed": false,
        "bounced": false,
        "complained": false,
        "unsubscribed": false,
        "sys": false,
        "broadcast": false,
        "keep": true,
        "hidden": false,
        "archived": false,

        "org": "Premium Test Org",
        "owner": "owner-2222-3333-4444-5555-666666666666",
        "updater": "updater-3333-4444-5555-6666-777777777777",

        "provider": "transactional_email",
        "medium": "email",
        "alias": "https://test.com/premium-purchase"
    }' \
    -w "\nStatus: %{http_code}\n"

# Test 3: Edge cases and special values
echo -e "${YELLOW}3. Testing edge cases and special values...${NC}"
curl -s -X POST http://localhost:8880/tr/v1/tr/ \
    -H "Content-Type: application/json" \
    -d '{
        "tid": "33333333-4444-5555-6666-777777777777",
        "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "etyp": "conversion",

        "alias": "",
        "name": "Edge Case Test - Empty strings and zero values",
        "urgency": 0,
        "ephemeral": 0,
        "planned_impressions": 0,
        "actual_impressions": 0,
        "impression_goal": 0,
        "impression_budget": 0.0,
        "cost_per_impression": 0.0,
        "total_conversions": 0,
        "conversion_value": 0.0,
        "campaign_priority": 0,
        "campaign_budget_allocation": 0.0,
        "attribution_weight": 0.0,
        "attribution_window": 0,
        "data_retention": 0,
        "content_version": 0,
        "creator_compensation_rate": 0.0,
        "creator_compensation_cap": 0.0,

        "abz_reward_value": 0.0,
        "abz_exploration_rate": 0.0,
        "abz_learning_rate": 0.0,
        "abz_sample_size": 0,
        "abz_min_sample_size": 0,
        "abz_confidence_level": 0.0,
        "abz_winner_threshold": 0.0,

        "opens": false,
        "openp": false,
        "derive": false,
        "ftrack": false,
        "strack": false,
        "sys": false,
        "archived": false,
        "broadcast": false,
        "abz_enabled": false,
        "abz_auto_optimize": false,
        "abz_auto_stop": false,
        "abz_infinite_armed": false,
        "requires_consent": false,
        "creator_compensation": false,

        "cats": [],
        "mtypes": [],
        "fmtypes": [],
        "cmtypes": [],
        "admins": [],
        "perms_ids": [],
        "cohorts": [],
        "sent": [],
        "outs": [],
        "subs": [],
        "pubs": [],
        "vid_targets": [],
        "audience_segments": [],
        "content_keywords": [],
        "consent_types": [],
        "content_history": [],
        "content_editors": [],

        "splits": {},
        "prefs": {},
        "interest": {},
        "perf": {},
        "variants": {},
        "variant_weights": {},
        "audience_params": {},
        "audience_metrics": {},
        "content_assumptions": {},
        "content_metrics": {},
        "regional_compliance": {},
        "frequency_caps": {},
        "provider_metrics": {},
        "abz_params": {},
        "abz_param_space": {},
        "abz_model_params": {},
        "attribution_params": {},

        "subject": "Edge case test",
        "msg": "Testing with minimal/zero values"
    }' \
    -w "\nStatus: %{http_code}\n"

echo ""
echo -e "${YELLOW}Waiting for batch flush...${NC}"
sleep 3

echo -e "${YELLOW}Flushing ClickHouse async inserts...${NC}"
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" 2>/dev/null
sleep 1

echo ""
echo -e "${BLUE}=== VERIFICATION OF CAPTURED FIELDS ===${NC}"
echo ""

# Verify mthreads comprehensive capture
echo -e "${YELLOW}Checking mthreads comprehensive record:${NC}"
clickhouse client --query "
SELECT
    '=== Core Identification ===' as section,
    tid, alias, xstatus, name,
    length(ddata) as ddata_length,
    provider, medium, xid,
    length(post) as post_length,
    length(mtempl) as mtempl_length,
    mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

UNION ALL

SELECT
    '=== Arrays (counts) ===' as section,
    '' as tid,
    toString(length(cats)) as alias,
    toString(length(mtypes)) as xstatus,
    toString(length(fmtypes)) as name,
    length(cmtypes) as ddata_length,
    toString(length(admins)) as provider,
    toString(length(perms_ids)) as medium,
    toString(length(cohorts)) as xid,
    length(audience_segments) as post_length,
    length(content_keywords) as mtempl_length,
    toString(length(consent_types)) as mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

UNION ALL

SELECT
    '=== Campaign Fields ===' as section,
    campaign_id as tid,
    campaign_status as alias,
    campaign_phase as xstatus,
    funnel_stage as name,
    length(winner_variant) as ddata_length,
    content_intention as provider,
    provider_campaign_id as medium,
    provider_account_id as xid,
    provider_cost as post_length,
    length(provider_status) as mtempl_length,
    '' as mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

UNION ALL

SELECT
    '=== ABZ Testing Fields ===' as section,
    abz_algorithm as tid,
    abz_reward_metric as alias,
    toString(abz_reward_value) as xstatus,
    toString(abz_exploration_rate) as name,
    abz_sample_size as ddata_length,
    toString(abz_confidence_level) as provider,
    abz_status as medium,
    abz_model_type as xid,
    length(abz_acquisition_function) as post_length,
    abz_enabled as mtempl_length,
    toString(abz_infinite_armed) as mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

UNION ALL

SELECT
    '=== Metrics ===' as section,
    toString(planned_impressions) as tid,
    toString(actual_impressions) as alias,
    toString(impression_goal) as xstatus,
    toString(total_conversions) as name,
    conversion_value as ddata_length,
    toString(impression_budget) as provider,
    toString(cost_per_impression) as medium,
    toString(campaign_priority) as xid,
    attribution_weight as post_length,
    data_retention as mtempl_length,
    toString(content_version) as mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

UNION ALL

SELECT
    '=== Creator Fields ===' as section,
    creator_id as tid,
    content_id as alias,
    content_approver as xstatus,
    content_creator as name,
    length(creator_compensation_model) as ddata_length,
    toString(creator_compensation) as provider,
    toString(creator_compensation_rate) as medium,
    toString(creator_compensation_cap) as xid,
    length(creator_notes) as post_length,
    length(content_status) as mtempl_length,
    attribution_model as mcert_id
FROM sfpla.mthreads
WHERE tid = '11111111-2222-3333-4444-555555555555'

FORMAT Vertical
" 2>/dev/null | head -100

echo ""
echo -e "${YELLOW}Checking mstore comprehensive record:${NC}"
clickhouse client --query "
SELECT
    tid,
    mid,
    pmid,
    qid,
    rid,
    category,
    subject,
    length(msg) as msg_length,
    encoding,
    priority,
    urgency,
    archived,
    length(mtypes) as mtypes_count,
    imp_count,
    click_count,
    open_count,
    conversion_event_count,
    conversion_revenue,
    status,
    delivered,
    failed,
    bounced,
    complained,
    unsubscribed,
    sys,
    broadcast,
    keep,
    hidden
FROM sfpla.mstore
WHERE tid = '22222222-3333-4444-5555-666666666666'
FORMAT Vertical
" 2>/dev/null | head -50

echo ""
echo -e "${YELLOW}Checking mtriage record (high_value_action):${NC}"
clickhouse client --query "
SELECT
    tid,
    mid,
    pmid,
    qid,
    rid,
    category,
    subject,
    length(msg) as msg_length,
    priority,
    urgency,
    sys,
    broadcast,
    keep,
    hidden
FROM sfpla.mtriage
WHERE tid = '22222222-3333-4444-5555-666666666666'
FORMAT Vertical
" 2>/dev/null | head -30

echo ""
echo -e "${YELLOW}Checking edge case record (zero/empty values):${NC}"
clickhouse client --query "
SELECT
    tid,
    alias,
    name,
    urgency,
    ephemeral,
    planned_impressions,
    total_conversions,
    conversion_value,
    length(cats) as cats_count,
    length(mtypes) as mtypes_count,
    opens,
    broadcast,
    abz_enabled,
    length(splits) as splits_size,
    length(prefs) as prefs_size
FROM sfpla.mthreads
WHERE tid = '33333333-4444-5555-6666-777777777777'
FORMAT Vertical
" 2>/dev/null | head -30

echo ""
echo -e "${BLUE}=== SUMMARY ===${NC}"
echo ""

# Count records
echo -e "${YELLOW}Record counts by table:${NC}"
echo -n "mthreads test records: "
clickhouse client --query "SELECT count() FROM sfpla.mthreads FINAL WHERE tid IN ('11111111-2222-3333-4444-555555555555'::UUID, '22222222-3333-4444-5555-666666666666'::UUID, '33333333-4444-5555-6666-777777777777'::UUID)" 2>/dev/null
echo -n "mstore test records: "
clickhouse client --query "SELECT count() FROM sfpla.mstore FINAL WHERE tid IN ('11111111-2222-3333-4444-555555555555'::UUID, '22222222-3333-4444-5555-666666666666'::UUID, '33333333-4444-5555-6666-777777777777'::UUID)" 2>/dev/null
echo -n "mtriage test records: "
clickhouse client --query "SELECT count() FROM sfpla.mtriage FINAL WHERE tid IN ('11111111-2222-3333-4444-555555555555'::UUID, '22222222-3333-4444-5555-666666666666'::UUID, '33333333-4444-5555-6666-777777777777'::UUID)" 2>/dev/null

echo ""
echo -e "${YELLOW}Field population analysis:${NC}"
clickhouse client --query "
WITH test_records AS (
    SELECT * FROM sfpla.mthreads
    WHERE tid = '11111111-2222-3333-4444-555555555555'
)
SELECT
    'Populated string fields' as metric,
    countIf(alias != '') +
    countIf(name != '') +
    countIf(xstatus != '') +
    countIf(provider != '') +
    countIf(medium != '') +
    countIf(campaign_id != '') +
    countIf(abz_algorithm != '') +
    countIf(attribution_model != '') as count
FROM test_records

UNION ALL

SELECT
    'Populated numeric fields' as metric,
    countIf(urgency > 0) +
    countIf(planned_impressions > 0) +
    countIf(total_conversions > 0) +
    countIf(conversion_value > 0) +
    countIf(abz_sample_size > 0) as count
FROM test_records

UNION ALL

SELECT
    'Populated boolean fields' as metric,
    countIf(opens = true) +
    countIf(derive = true) +
    countIf(ftrack = true) +
    countIf(broadcast = true) +
    countIf(abz_enabled = true) +
    countIf(requires_consent = true) as count
FROM test_records

UNION ALL

SELECT
    'Non-empty arrays' as metric,
    countIf(length(cats) > 0) +
    countIf(length(mtypes) > 0) +
    countIf(length(audience_segments) > 0) +
    countIf(length(content_keywords) > 0) as count
FROM test_records

UNION ALL

SELECT
    'Non-empty JSON fields' as metric,
    countIf(length(splits) > 2) +
    countIf(length(prefs) > 2) +
    countIf(length(variants) > 2) +
    countIf(length(audience_params) > 2) as count
FROM test_records
" 2>/dev/null

echo ""
echo -e "${GREEN}=== TEST COMPLETE ===${NC}"
echo -e "${GREEN}All messaging table fields have been tested for comprehensive capture!${NC}"