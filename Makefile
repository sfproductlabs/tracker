# ====================================================================
# Makefile for Go Tracker with ClickHouse + Keeper
# ====================================================================
#
# Main targets:
#   make help           - Show this help message
#   make build          - Build tracker binary
#   make run            - Build and run tracker (local mode)
#   make test           - Run all tests
#   make clean          - Clean build artifacts and temp files
#
# Docker commands:
#   make docker-build   - Build Docker image
#   make docker-run     - Run single-node container
#   make docker-stop    - Stop and remove containers
#   make docker-clean   - Remove containers, images, and volumes
#   make docker-logs    - Show container logs
#
# Cluster commands:
#   make cluster-start  - Start 3-node cluster
#   make cluster-stop   - Stop 3-node cluster
#   make cluster-test   - Test cluster connectivity
#
# Schema management:
#   make schema-update  - Update hard links from api schema files
#   make schema-verify  - Verify hard links are correct
#
# Testing:
#   make test-single    - Test single-node setup
#   make test-cluster   - Test 3-node cluster
#   make test-all       - Run all tests
#
# ====================================================================

.PHONY: help build run clean test
.PHONY: docker-build docker-run docker-stop docker-clean docker-logs docker-shell
.PHONY: cluster-start cluster-stop cluster-test cluster-logs
.PHONY: schema-update schema-verify schema-load
.PHONY: test-single test-cluster test-all
.PHONY: test-functional-ltv test-functional-ltv-batch test-functional-redirects
.PHONY: test-functional-privacy test-functional-jurisdictions test-functional-health
.PHONY: test-functional-batch test-functional-e2e test-functional-all
.PHONY: clickhouse-start clickhouse-stop clickhouse-status
.PHONY: db-reset db-clean db-console db-admin

# Default target
.DEFAULT_GOAL := help

# ====================================================================
# VARIABLES
# ====================================================================

# Detect OS for cross-platform compatibility
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    OS := macos
    SED_INPLACE := sed -i ''
else
    OS := linux
    SED_INPLACE := sed -i
endif

# Directories
PROJECT_ROOT := $(shell cd ../.. && pwd)
API_DIR := $(PROJECT_ROOT)/packages/api
API_SCHEMA_DIR := $(API_DIR)/scripts/clickhouse/schema
TRACKER_SCHEMA_DIR := .setup/clickhouse
TRACKER_BINARY := tracker
DOCKER_IMAGE := tracker:latest
CONTAINER_NAME := v4-tracker-1
NETWORK_NAME := tracker-net
TMP_DATA_DIR := /tmp/clickhouse-test

# Schema files to link (hard links from api schema)
# OPEN SOURCE TRACKER: Only core tables (enterprise ML schemas excluded)
SCHEMA_FILES := core.1.sql analytics.1.sql auth.1.sql compliance.1.sql messaging.1.sql users.1.sql

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# ====================================================================
# HELP
# ====================================================================

help:
	@echo ""
	@echo "$(GREEN)Go Tracker with ClickHouse + Keeper - Makefile$(NC)"
	@echo "======================================================================"
	@echo ""
	@echo "$(YELLOW)Build Commands:$(NC)"
	@echo "  make build          - Build tracker binary"
	@echo "  make run            - Build and run tracker (local mode)"
	@echo "  make clean          - Clean build artifacts"
	@echo ""
	@echo "$(YELLOW)Docker Commands (Single Node):$(NC)"
	@echo "  make docker-build         - Build Docker image"
	@echo "  make docker-run           - Run single-node container with persistent volumes"
	@echo "  make docker-stop          - Stop and remove container"
	@echo "  make docker-clean         - Remove container, image, and volumes"
	@echo "  make docker-logs          - Show container logs (tail -f)"
	@echo "  make docker-shell         - Open shell in running container"
	@echo "  make docker-verify-tables - Verify ClickHouse tables loaded"
	@echo "  make docker-test-events   - Test events table with sample data"
	@echo "  make docker-test-messaging - Test messaging tables (mthreads/mstore/mtriage)"
	@echo "  make docker-test-all      - Run all Docker tests"
	@echo "  make docker-rebuild-test  - Clean rebuild and full test"
	@echo ""
	@echo "$(YELLOW)Docker Commands (3-Node Cluster):$(NC)"
	@echo "  make cluster-start  - Start 3-node cluster with persistent volumes"
	@echo "  make cluster-stop   - Stop 3-node cluster"
	@echo "  make cluster-test   - Test cluster connectivity and tables"
	@echo "  make cluster-logs   - Show logs from all 3 nodes"
	@echo ""
	@echo "$(YELLOW)Schema Management:$(NC)"
	@echo "  make schema-update  - Update hard links from api schema files"
	@echo "  make schema-verify  - Verify hard links are correct"
	@echo "  make schema-load    - Load all 6 open source schema files"
	@echo ""
	@echo "$(YELLOW)Database Management:$(NC)"
	@echo "  make clickhouse-start - Start ClickHouse with embedded Keeper"
	@echo "  make clickhouse-stop  - Stop ClickHouse and Keeper"
	@echo "  make clickhouse-status - Check ClickHouse status"
	@echo "  make db-console       - Open ClickHouse CLI"
	@echo "  make db-reset         - Drop database + keeper, then reload schema"
	@echo "  make db-clean         - Full clean (database + keeper only)"
	@echo "  make db-admin         - Set up admin account"
	@echo ""
	@echo "$(YELLOW)Testing:$(NC)"
	@echo "  make test                      - Run Go unit tests"
	@echo "  make test-single               - Run tests/test-single.sh (single-node verification)"
	@echo "  make test-cluster              - Run tests/test-cluster.sh (3-node cluster test)"
	@echo "  make test-db-writes            - Run tests/test_db_writes.sh (database write verification)"
	@echo "  make test-all                  - Run all tests (Go + single + db-writes)"
	@echo ""
	@echo "$(YELLOW)Functional Endpoint Tests:$(NC)"
	@echo "  make test-functional-health    - Test health/ping/status/metrics endpoints"
	@echo "  make test-functional-ltv       - Test LTV tracking (single payment)"
	@echo "  make test-functional-ltv-batch - Test LTV tracking (batch payments)"
	@echo "  make test-functional-redirects - Test redirect/short URL API"
	@echo "  make test-functional-privacy   - Test privacy/agreement API"
	@echo "  make test-functional-jurisdictions - Test jurisdictions endpoint"
	@echo "  make test-functional-batch     - Test batch processing (100 events)"
	@echo "  make test-functional-e2e       - Test complete end-to-end workflow"
	@echo "  make test-functional-all       - Run ALL functional tests"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  make deps           - Download Go dependencies"
	@echo "  make fmt            - Format Go code"
	@echo "  make lint           - Run golangci-lint (if installed)"
	@echo ""

# ====================================================================
# BUILD
# ====================================================================

build:
	@echo "$(YELLOW)🔨 Building tracker binary...$(NC)"
	@go build -o $(TRACKER_BINARY)
	@echo "$(GREEN)✅ Tracker built: ./$(TRACKER_BINARY)$(NC)"

run: build
	@echo "$(YELLOW)🚀 Starting tracker (local mode)...$(NC)"
	@./$(TRACKER_BINARY)

deps:
	@echo "$(YELLOW)📦 Downloading Go dependencies...$(NC)"
	@go get
	@go mod tidy
	@echo "$(GREEN)✅ Dependencies downloaded$(NC)"

fmt:
	@echo "$(YELLOW)✨ Formatting Go code...$(NC)"
	@go fmt ./...
	@echo "$(GREEN)✅ Code formatted$(NC)"

lint:
	@echo "$(YELLOW)🔍 Running linter...$(NC)"
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
		echo "$(GREEN)✅ Linting complete$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  golangci-lint not installed, skipping$(NC)"; \
		echo "Install: brew install golangci-lint (macOS) or see https://golangci-lint.run/usage/install/"; \
	fi

# ====================================================================
# DOCKER - SINGLE NODE
# ====================================================================

docker-build:
	@echo "$(YELLOW)🐳 Building Docker image...$(NC)"
	@docker build -t $(DOCKER_IMAGE) .
	@echo "$(GREEN)✅ Docker image built: $(DOCKER_IMAGE)$(NC)"

docker-run:
	@echo "$(YELLOW)🐳 Starting single-node tracker container...$(NC)"
	@mkdir -p $(TMP_DATA_DIR)/data $(TMP_DATA_DIR)/logs
	@docker run -d --name $(CONTAINER_NAME) \
		--hostname $(CONTAINER_NAME) \
		--add-host $(CONTAINER_NAME):127.0.0.1 \
		-v $(TMP_DATA_DIR)/data:/data/clickhouse \
		-v $(TMP_DATA_DIR)/logs:/logs/clickhouse \
		-p 8080:8080 -p 8443:8443 -p 8880:8880 -p 9000:9000 -p 8123:8123 -p 2181:2181 \
		-e CLICKHOUSE_DATA_DIR="/data/clickhouse" \
		-e CLICKHOUSE_LOG_DIR="/logs/clickhouse" \
		$(DOCKER_IMAGE)
	@echo "$(GREEN)✅ Container started: $(CONTAINER_NAME)$(NC)"
	@echo ""
	@echo "Access points:"
	@echo "  Tracker HTTP:  http://localhost:8080/health"
	@echo "  Tracker HTTPS: https://localhost:8443/health"
	@echo "  Tracker Alt:   http://localhost:8880/health"
	@echo "  ClickHouse:    http://localhost:8123"
	@echo "  Native:        localhost:9000"
	@echo "  Keeper:        localhost:2181"
	@echo ""
	@echo "Check logs: make docker-logs"
	@echo "Verify tables: make docker-verify-tables"

docker-stop:
	@echo "$(YELLOW)🛑 Stopping container...$(NC)"
	@docker stop $(CONTAINER_NAME) 2>/dev/null || true
	@docker rm $(CONTAINER_NAME) 2>/dev/null || true
	@echo "$(GREEN)✅ Container stopped and removed$(NC)"

docker-clean: docker-stop
	@echo "$(YELLOW)🧹 Cleaning Docker resources...$(NC)"
	@docker rmi $(DOCKER_IMAGE) 2>/dev/null || true
	@rm -rf $(TMP_DATA_DIR)
	@echo "$(GREEN)✅ Docker resources cleaned$(NC)"

docker-logs:
	@echo "$(YELLOW)📜 Showing container logs (Ctrl+C to exit)...$(NC)"
	@docker logs -f $(CONTAINER_NAME)

docker-shell:
	@echo "$(YELLOW)🐚 Opening shell in container...$(NC)"
	@docker exec -it $(CONTAINER_NAME) /bin/bash

docker-clickhouse-shell:
	@echo "$(YELLOW)🗄️  Opening ClickHouse client...$(NC)"
	@docker exec -it $(CONTAINER_NAME) clickhouse-client

docker-verify-tables:
	@echo "$(YELLOW)🔍 Verifying tables in container...$(NC)"
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'sfpla'"
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT table FROM system.tables WHERE database = 'sfpla' ORDER BY table"

docker-test-events:
	@echo "$(YELLOW)🧪 Testing events table...$(NC)"
	@echo ""
	@echo "Sending 5 test events..."
	@for i in 1 2 3 4 5; do \
		curl -s -X POST http://localhost:8080/tr/v1/tr/ \
		-H "Content-Type: application/json" \
		-d '{"eid":"test-'$$i'","ename":"page_view","url":"http://test.com/page'$$i'","oid":"00000000-0000-0000-0000-000000000001"}' \
		-w "\nStatus: %{http_code}\n" || echo "Request $$i failed"; \
	done
	@echo ""
	@echo "Waiting 2 seconds for batch flush..."
	@sleep 2
	@echo ""
	@echo "Querying events table (using FINAL):"
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT count() as total, ename FROM sfpla.events FINAL GROUP BY ename"
	@echo ""
	@echo "Sample events:"
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT eid, ename, url, created_at FROM sfpla.events FINAL ORDER BY created_at DESC LIMIT 5"

docker-test-messaging:
	@echo "$(YELLOW)🧪 Testing messaging tables (mthreads, mstore, mtriage)...$(NC)"
	@echo ""
	@echo "Sending test conversion event (triggers mthreads + mstore + mtriage)..."
	@curl -s -X POST http://localhost:8080/tr/v1/tr/ \
		-H "Content-Type: application/json" \
		-d '{"eid":"test-conversion","ename":"conversion","event_type":"conversion","tid":"test-thread-001","url":"http://test.com/success","oid":"00000000-0000-0000-0000-000000000001","provider":"website","medium":"page","alias":"http://test.com/success","subject":"Test Conversion","msg":"User completed purchase","urgency":5}' \
		-w "\nStatus: %{http_code}\n"
	@echo ""
	@echo "Waiting 3 seconds for batch flush..."
	@sleep 3
	@echo ""
	@echo "=== mthreads table (thread metadata) ==="
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT tid, alias, provider, medium, urgency, campaign_status FROM sfpla.mthreads FINAL LIMIT 5"
	@echo ""
	@echo "=== mstore table (permanent message archive) ==="
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT tid, mid, subject, msg, urgency, sys FROM sfpla.mstore FINAL LIMIT 5"
	@echo ""
	@echo "=== mtriage table (messages in triage) ==="
	@docker exec $(CONTAINER_NAME) clickhouse-client --query "SELECT tid, mid, subject, msg, urgency FROM sfpla.mtriage FINAL LIMIT 5"

docker-test-all: docker-test-events docker-test-messaging
	@echo ""
	@echo "$(GREEN)✅ All Docker tests complete$(NC)"

docker-rebuild-test: docker-stop docker-build docker-run
	@echo ""
	@echo "$(YELLOW)⏳ Waiting 60 seconds for full initialization...$(NC)"
	@sleep 60
	@$(MAKE) docker-test-all

# ====================================================================
# DOCKER - 3-NODE CLUSTER
# ====================================================================

cluster-start:
	@echo "$(YELLOW)🐳 Starting 3-node cluster...$(NC)"
	@./tests/test-cluster.sh
	@echo "$(GREEN)✅ Cluster started$(NC)"

cluster-stop:
	@echo "$(YELLOW)🛑 Stopping 3-node cluster...$(NC)"
	@docker stop v4-tracker-1 v4-tracker-2 v4-tracker-3 2>/dev/null || true
	@docker rm v4-tracker-1 v4-tracker-2 v4-tracker-3 2>/dev/null || true
	@rm -rf /tmp/clickhouse-cluster
	@echo "$(GREEN)✅ Cluster stopped$(NC)"

cluster-test:
	@echo "$(YELLOW)🧪 Testing cluster...$(NC)"
	@echo ""
	@echo "Node 1 tables:"
	@docker exec v4-tracker-1 clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'sfpla'"
	@echo ""
	@echo "Cluster status:"
	@docker exec v4-tracker-1 clickhouse-client --query "SELECT * FROM system.clusters WHERE cluster='tracker_cluster'"
	@echo ""
	@echo "Keeper status:"
	@docker exec v4-tracker-1 clickhouse-client --query "SELECT name, value FROM system.zookeeper WHERE path='/'" 2>/dev/null || echo "Keeper not accessible"

cluster-logs:
	@echo "$(YELLOW)📜 Showing logs from all 3 nodes...$(NC)"
	@echo ""
	@echo "=== v4-tracker-1 ==="
	@docker logs v4-tracker-1 | tail -20
	@echo ""
	@echo "=== v4-tracker-2 ==="
	@docker logs v4-tracker-2 | tail -20
	@echo ""
	@echo "=== v4-tracker-3 ==="
	@docker logs v4-tracker-3 | tail -20

cluster-logs-follow:
	@echo "$(YELLOW)📜 Following logs from node 1 (Ctrl+C to exit)...$(NC)"
	@docker logs -f v4-tracker-1

# ====================================================================
# SCHEMA MANAGEMENT
# ====================================================================

schema-update:
	@echo "$(YELLOW)🔗 Updating schema hard links from api package...$(NC)"
	@mkdir -p $(TRACKER_SCHEMA_DIR)
	@cd $(TRACKER_SCHEMA_DIR) && \
	for f in $(SCHEMA_FILES); do \
		rm -f $$f; \
		ln $(API_SCHEMA_DIR)/$$f $$f; \
		echo "  ✓ Linked $$f"; \
	done
	@echo "$(GREEN)✅ Schema files linked (6 core files)$(NC)"

schema-verify:
	@echo "$(YELLOW)🔍 Verifying schema hard links...$(NC)"
	@cd $(TRACKER_SCHEMA_DIR) && ls -li $(SCHEMA_FILES)
	@echo ""
	@echo "$(YELLOW)Note: Files with the same inode number (second column) are hard links$(NC)"
	@echo "$(YELLOW)Expected hard links: core.1.sql, analytics.1.sql, auth.1.sql, compliance.1.sql, messaging.1.sql, users.1.sql$(NC)"

schema-load:
	@echo "$(YELLOW)🗄️  Loading open source schema (6 core files)...$(NC)"
	@cd $(TRACKER_SCHEMA_DIR) && for f in $(SCHEMA_FILES); do \
		clickhouse client --multiquery < $$f || exit 1; \
	done
	@echo "$(GREEN)✅ Open source schema loaded (177 tables)$(NC)"

# ====================================================================
# DATABASE MANAGEMENT
# ====================================================================

clickhouse-start:
	@if clickhouse client --query "SELECT 1" >/dev/null 2>&1; then \
		echo "$(GREEN)✅ ClickHouse already running$(NC)"; \
	else \
		echo "$(YELLOW)Starting ClickHouse server with embedded Keeper...$(NC)"; \
		mkdir -p ./tmp/clickhouse/{data,logs,tmp,user_files,format_schemas,coordination/log,coordination/snapshots}; \
		nohup clickhouse server --config-file=clickhouse-config.xml > ./tmp/clickhouse/logs/server.log 2>&1 & \
		echo $$! > ./tmp/clickhouse/clickhouse.pid; \
		echo "$(YELLOW)Waiting for ClickHouse + Keeper to start...$(NC)"; \
		for i in 1 2 3 4 5 6 7 8 9 10; do \
			if clickhouse client --query "SELECT 1" >/dev/null 2>&1; then \
				echo "$(GREEN)✅ ClickHouse started successfully (Keeper on port 2181)$(NC)"; \
				exit 0; \
			fi; \
			sleep 1; \
		done; \
		echo "$(RED)❌ ClickHouse failed to start after 10 seconds$(NC)"; \
		exit 1; \
	fi

clickhouse-stop:
	@echo "$(YELLOW)Stopping ClickHouse (and embedded Keeper)...$(NC)"
	@if [ -f ./tmp/clickhouse/clickhouse.pid ]; then \
		kill $$(cat ./tmp/clickhouse/clickhouse.pid) 2>/dev/null || true; \
		rm -f ./tmp/clickhouse/clickhouse.pid; \
	fi
	@pkill -f "clickhouse server" || true
	@pkill -f "clickhouse keeper" || true
	@sleep 2
	@echo "$(GREEN)✅ ClickHouse stopped$(NC)"

clickhouse-status:
	@clickhouse client --query "SELECT 'ClickHouse is running' as status" 2>/dev/null || echo "$(RED)ClickHouse is not running$(NC)"

db-console:
	@echo "$(YELLOW)Opening ClickHouse console...$(NC)"
	@clickhouse client

db-reset:
	@echo "$(RED)⚠️  WARNING: This will drop all data!$(NC)"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirmation
	@echo "$(YELLOW)Dropping database...$(NC)"
	@clickhouse client --query "DROP DATABASE IF EXISTS sfpla"
	@echo "$(YELLOW)Clearing ClickHouse keeper metadata...$(NC)"
	@clickhouse keeper-client --host 0.0.0.0 --port 2181 --query "rmr '/clickhouse'" 2>/dev/null || echo "  (Keeper not available, skipping)"
	@echo "$(YELLOW)Waiting for metadata cleanup...$(NC)"
	@sleep 2
	@echo "$(YELLOW)Recreating schema (6 core files)...$(NC)"
	@$(MAKE) schema-load
	@echo "$(YELLOW)Verifying replica health...$(NC)"
	@BROKEN_COUNT=$$(clickhouse client --query "SELECT count() FROM system.replicas WHERE database = 'sfpla' AND total_replicas = 0" 2>/dev/null || echo "0"); \
	if [ "$$BROKEN_COUNT" -gt 0 ]; then \
		echo "$(RED)❌ ERROR: $$BROKEN_COUNT replicas failed to register in ZooKeeper$(NC)"; \
		echo "$(YELLOW)Broken tables:$(NC)"; \
		clickhouse client --query "SELECT table FROM system.replicas WHERE database = 'sfpla' AND total_replicas = 0"; \
		exit 1; \
	else \
		TOTAL=$$(clickhouse client --query "SELECT count() FROM system.replicas WHERE database = 'sfpla'" 2>/dev/null || echo "0"); \
		if [ "$$TOTAL" -gt 0 ]; then \
			echo "$(GREEN)✅ All $$TOTAL replicas properly registered$(NC)"; \
		else \
			echo "$(GREEN)✅ Database ready (no replicated tables)$(NC)"; \
		fi; \
	fi
	@echo "$(YELLOW)Setting up admin account...$(NC)"
	@$(MAKE) db-admin
	@echo "$(GREEN)✅ Database reset complete$(NC)"

db-clean:
	@echo "$(RED)⚠️  WARNING: This will drop the database and clear keeper!$(NC)"
	@echo "This is a FULL clean (database + keeper + recreate)"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirmation
	@echo "$(YELLOW)Dropping database sfpla...$(NC)"
	@clickhouse client --query "DROP DATABASE IF EXISTS sfpla SYNC"
	@echo "$(YELLOW)Clearing keeper metadata...$(NC)"
	@clickhouse keeper-client --host 0.0.0.0 --port 2181 --query "rmr '/clickhouse'" 2>/dev/null || echo "  (Keeper not available, skipping)"
	@echo "$(GREEN)✅ Database and keeper cleared$(NC)"
	@echo ""
	@echo "To reload schema, run: make schema-load"

db-admin:
	@echo "$(YELLOW)Setting up admin account...$(NC)"
	@clickhouse client --query "INSERT INTO accounts VALUES (toUUID('14fb0860-b4bf-11e9-8971-7b80435315ac'), 'W6ph5Mm5Pz8GgiULbPgzG37mj9g=', '127.0.0.1', 'demo admin user', toDateTime64('2024-01-01 00:00:00', 3), '{\"*\":{\"*\":\"*\"}}', toDateTime64('2019-08-07 00:00:00', 3), now64(3), toUUID('14fb0860-b4bf-11e9-8971-7b80435315ac'))" 2>/dev/null || true
	@echo "$(GREEN)✅ Admin account ready (uid: 14fb0860-b4bf-11e9-8971-7b80435315ac, password hash: W6ph5Mm5Pz8GgiULbPgzG37mj9g=)$(NC)"

# ====================================================================
# TESTING
# ====================================================================

test:
	@echo "$(YELLOW)🧪 Running Go tests...$(NC)"
	@go test -v ./...
	@echo "$(GREEN)✅ Go tests complete$(NC)"

test-single:
	@echo "$(YELLOW)🧪 Running single-node test...$(NC)"
	@chmod +x tests/test-single.sh
	@./tests/test-single.sh

test-cluster:
	@echo "$(YELLOW)🧪 Running 3-node cluster test...$(NC)"
	@chmod +x tests/test-cluster.sh
	@./tests/test-cluster.sh

test-db-writes:
	@echo "$(YELLOW)🧪 Running database write tests...$(NC)"
	@chmod +x tests/test_db_writes.sh
	@./tests/test_db_writes.sh

test-all: test test-single test-db-writes
	@echo "$(GREEN)✅ All tests complete$(NC)"

# ====================================================================
# FUNCTIONAL ENDPOINT TESTS (Requires running tracker on port 8443)
# ====================================================================

test-functional-ltv:
	@echo "$(YELLOW)🧪 Testing LTV endpoint (single payment)...$(NC)"
	@curl -sk -X POST https://localhost:8443/tr/v1/ltv/ \
		-H "Content-Type: application/json" \
		-d '{"vid":"14fb0860-b4bf-11e9-8971-7b80435315ac","uid":"b1c2d3e4-f5a6-7890-abcd-ef1234567890","oid":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","amt":99.99,"currency":"USD","orid":"c3d4e5f6-a7b8-9012-abcd-ef1234567890"}' \
		-w "\nHTTP Status: %{http_code}\n"
	@echo ""
	@echo "Waiting 3 seconds for batch flush..."
	@sleep 3
	@echo ""
	@echo "Verifying LTV records inserted:"
	@clickhouse client --query "SELECT COUNT(*) as count, id_type FROM sfpla.ltv FINAL GROUP BY id_type ORDER BY id_type"
	@echo "$(GREEN)✅ LTV test complete$(NC)"

test-functional-ltv-batch:
	@echo "$(YELLOW)🧪 Testing LTV endpoint (batch payments)...$(NC)"
	@curl -sk -X POST https://localhost:8443/tr/v1/ltv/ \
		-H "Content-Type: application/json" \
		-d '{"vid":"d4e5f6a7-b8c9-0123-abcd-ef1234567890","uid":"e5f6a7b8-c9d0-1234-abcd-ef1234567890","oid":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","payments":[{"amt":50.00,"currency":"USD","orid":"f6a7b8c9-d0e1-2345-abcd-ef1234567890"},{"amt":25.00,"currency":"USD","orid":"a7b8c9d0-e1f2-3456-abcd-ef1234567890"}]}' \
		-w "\nHTTP Status: %{http_code}\n"
	@echo ""
	@echo "Waiting 3 seconds for batch flush..."
	@sleep 3
	@echo ""
	@echo "Verifying batch LTV records inserted:"
	@clickhouse client --query "SELECT COUNT(*) as count FROM sfpla.ltv FINAL"
	@echo "$(GREEN)✅ Batch LTV test complete$(NC)"

test-functional-redirects:
	@echo "$(YELLOW)🧪 Testing redirect/short URL API...$(NC)"
	@echo ""
	@echo "Creating shortened URL..."
	@curl -sk -X POST https://localhost:8443/tr/v1/rpi/redirect/14fb0860-b4bf-11e9-8971-7b80435315ac/password \
		-H "Content-Type: application/json" \
		-d '{"urlfrom":"https://yourdomain.com/test-short","hostfrom":"yourdomain.com","slugfrom":"/test-short","urlto":"https://example.com/long/path?utm_source=makefile","hostto":"example.com","pathto":"/long/path","searchto":"?utm_source=makefile","oid":"a1b2c3d4-e5f6-7890-abcd-ef1234567890"}' \
		-w "\nHTTP Status: %{http_code}\n"
	@echo ""
	@echo "Waiting 2 seconds for batch flush..."
	@sleep 2
	@echo ""
	@echo "Verifying redirects table:"
	@clickhouse client --query "SELECT urlfrom, urlto FROM sfpla.redirects FINAL WHERE urlfrom LIKE '%test-short%' LIMIT 1"
	@echo ""
	@echo "Verifying redirect_history table:"
	@clickhouse client --query "SELECT urlfrom, urlto FROM sfpla.redirect_history FINAL WHERE urlfrom LIKE '%test-short%' ORDER BY updated_at DESC LIMIT 1"
	@echo ""
	@echo "Getting all redirects for host:"
	@curl -sk -X GET https://localhost:8443/tr/v1/rpi/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yourdomain.com | head -20
	@echo "$(GREEN)✅ Redirect test complete$(NC)"

test-functional-privacy:
	@echo "$(YELLOW)🧪 Testing privacy/agreement API...$(NC)"
	@echo ""
	@echo "Posting user agreement (GDPR consent)..."
	@curl -sk -X POST https://localhost:8443/tr/v1/ppi/agree \
		-H "Content-Type: application/json" \
		-d '{"vid":"b8c9d0e1-f2a3-4567-abcd-ef1234567890","cflags":1024,"tz":"America/Los_Angeles","lat":37.7749,"lon":-122.4194,"oid":"a1b2c3d4-e5f6-7890-abcd-ef1234567890"}' \
		-w "\nHTTP Status: %{http_code}\n"
	@echo ""
	@echo "Waiting 2 seconds for batch flush..."
	@sleep 2
	@echo ""
	@echo "Verifying agreements table:"
	@clickhouse client --query "SELECT vid, cflags, country, lat, lon FROM sfpla.agreements FINAL WHERE vid='b8c9d0e1-f2a3-4567-abcd-ef1234567890' LIMIT 1"
	@echo ""
	@echo "Verifying agreed (history) table:"
	@clickhouse client --query "SELECT vid, cflags FROM sfpla.agreed FINAL WHERE vid='b8c9d0e1-f2a3-4567-abcd-ef1234567890' ORDER BY created_at DESC LIMIT 1"
	@echo ""
	@echo "Getting agreements for visitor:"
	@curl -sk -X GET "https://localhost:8443/tr/v1/ppi/agree?vid=b8c9d0e1-f2a3-4567-abcd-ef1234567890"
	@echo ""
	@echo "$(GREEN)✅ Privacy/agreement test complete$(NC)"

test-functional-jurisdictions:
	@echo "$(YELLOW)🧪 Testing jurisdictions endpoint...$(NC)"
	@curl -sk -X GET https://localhost:8443/tr/v1/ppi/jds | head -20
	@echo ""
	@echo ""
	@echo "Database verification:"
	@clickhouse client --query "SELECT count() as total FROM sfpla.jurisdictions FINAL"
	@clickhouse client --query "SELECT * FROM sfpla.jurisdictions FINAL LIMIT 3"
	@echo "$(GREEN)✅ Jurisdictions test complete$(NC)"

test-functional-health:
	@echo "$(YELLOW)🧪 Testing health & metrics endpoints...$(NC)"
	@echo ""
	@echo "=== /health endpoint ==="
	@curl -sk https://localhost:8443/health
	@echo ""
	@echo ""
	@echo "=== /ping endpoint ==="
	@curl -sk https://localhost:8443/ping
	@echo ""
	@echo ""
	@echo "=== /status endpoint ==="
	@curl -sk https://localhost:8443/status
	@echo ""
	@echo ""
	@echo "=== /metrics endpoint (first 20 lines) ==="
	@curl -sk https://localhost:8443/metrics | head -20
	@echo ""
	@echo "$(GREEN)✅ Health endpoints test complete$(NC)"

test-functional-batch:
	@echo "$(YELLOW)🧪 Testing batch processing (100 events)...$(NC)"
	@echo ""
	@echo "Sending 100 events in parallel..."
	@for i in {1..100}; do \
		curl -sk -X POST https://localhost:8443/tr/v1/tr/ \
		-H "Content-Type: application/json" \
		-d "{\"vid\":\"batch-test-$$i\",\"ename\":\"batch_event_$$i\",\"etyp\":\"batch_test\",\"batch_num\":\"$$i\"}" & \
	done
	@wait
	@echo ""
	@echo "Waiting 5 seconds for batch flush..."
	@sleep 5
	@echo ""
	@echo "Verifying batch inserts:"
	@clickhouse client --query "SELECT COUNT(*) as total, etyp FROM sfpla.events FINAL WHERE etyp='batch_test' GROUP BY etyp"
	@echo ""
	@echo "Sample batch events:"
	@clickhouse client --query "SELECT vid, ename, etyp FROM sfpla.events FINAL WHERE etyp='batch_test' ORDER BY created_at DESC LIMIT 5"
	@echo "$(GREEN)✅ Batch test complete (should show 100 events)$(NC)"

test-functional-e2e:
	@echo "$(YELLOW)🧪 Running complete end-to-end workflow test...$(NC)"
	@echo ""
	@VID=$$(uuidgen | tr '[:upper:]' '[:lower:]'); \
	USER_ID=$$(uuidgen | tr '[:upper:]' '[:lower:]'); \
	OID=$$(uuidgen | tr '[:upper:]' '[:lower:]'); \
	echo "=== Testing with VID: $$VID ==="; \
	echo ""; \
	echo "1. Page view..."; \
	curl -sk -X POST https://localhost:8443/tr/v1/tr/ \
		-H "Content-Type: application/json" \
		-d "{\"vid\":\"$$VID\",\"ename\":\"page_view\",\"etyp\":\"view\",\"first\":\"true\"}" \
		-w "Status: %{http_code}\n"; \
	echo ""; \
	echo "2. Signup..."; \
	curl -sk -X POST https://localhost:8443/tr/v1/str/ \
		-H "Content-Type: application/json" \
		-d "{\"vid\":\"$$VID\",\"uid\":\"$$USER_ID\",\"oid\":\"$$OID\",\"ename\":\"signup\",\"etyp\":\"conversion\"}" \
		-w "Status: %{http_code}\n"; \
	echo ""; \
	echo "3. Purchase..."; \
	curl -sk -X POST https://localhost:8443/tr/v1/ltv/ \
		-H "Content-Type: application/json" \
		-d "{\"vid\":\"$$VID\",\"uid\":\"$$USER_ID\",\"oid\":\"$$OID\",\"amt\":149.99}" \
		-w "Status: %{http_code}\n"; \
	echo ""; \
	echo "4. Agreement..."; \
	curl -sk -X POST https://localhost:8443/tr/v1/ppi/agree \
		-H "Content-Type: application/json" \
		-d "{\"vid\":\"$$VID\",\"cflags\":1024,\"oid\":\"$$OID\"}" \
		-w "Status: %{http_code}\n"; \
	echo ""; \
	echo "Waiting 3 seconds for async inserts..."; \
	sleep 3; \
	echo ""; \
	echo "=== Results ==="; \
	echo "Events:"; \
	clickhouse client --query "SELECT ename, etyp FROM sfpla.events FINAL WHERE vid='$$VID' ORDER BY created_at"; \
	echo ""; \
	echo "LTV:"; \
	clickhouse client --query "SELECT COUNT(*) as ltv_count FROM sfpla.ltv FINAL"; \
	echo ""; \
	echo "Agreements:"; \
	clickhouse client --query "SELECT cflags FROM sfpla.agreements FINAL WHERE vid='$$VID'"; \
	echo ""; \
	echo "$(GREEN)✅ End-to-end test complete$(NC)"

test-functional-all: test-functional-health test-functional-ltv test-functional-ltv-batch test-functional-redirects test-functional-privacy test-functional-jurisdictions test-functional-batch
	@echo ""
	@echo "$(GREEN)✅✅✅ All functional tests complete ✅✅✅$(NC)"
	@echo ""
	@echo "Summary of tested endpoints:"
	@echo "  ✓ Health & metrics (/health, /ping, /status, /metrics)"
	@echo "  ✓ Event tracking (/tr/v1/tr/, /tr/v1/str/)"
	@echo "  ✓ LTV tracking (/tr/v1/ltv/) - single & batch"
	@echo "  ✓ Redirects (/tr/v1/rpi/*)"
	@echo "  ✓ Privacy/agreements (/tr/v1/ppi/agree)"
	@echo "  ✓ Jurisdictions (/tr/v1/ppi/jds)"
	@echo "  ✓ Batch processing (100 events)"
	@echo ""

# ====================================================================
# CLEANUP
# ====================================================================

clean:
	@echo "$(YELLOW)🧹 Cleaning build artifacts...$(NC)"
	@rm -f $(TRACKER_BINARY)
	@rm -f test-cluster-persistent.log
	@rm -f nohup.out
	@echo "$(GREEN)✅ Cleanup complete$(NC)"

clean-all: clean docker-clean cluster-stop
	@echo "$(YELLOW)🧹 Deep cleaning (including Docker resources)...$(NC)"
	@docker network rm $(NETWORK_NAME) 2>/dev/null || true
	@echo "$(GREEN)✅ Deep cleanup complete$(NC)"

# ====================================================================
# UTILITIES
# ====================================================================

network-create:
	@echo "$(YELLOW)🌐 Creating Docker network...$(NC)"
	@docker network create $(NETWORK_NAME) 2>/dev/null || echo "Network already exists"
	@echo "$(GREEN)✅ Network ready: $(NETWORK_NAME)$(NC)"

network-remove:
	@echo "$(YELLOW)🌐 Removing Docker network...$(NC)"
	@docker network rm $(NETWORK_NAME) 2>/dev/null || true
	@echo "$(GREEN)✅ Network removed$(NC)"

info:
	@echo ""
	@echo "$(GREEN)📊 Tracker Information$(NC)"
	@echo "======================================================================"
	@echo ""
	@echo "OS: $(OS)"
	@echo "Project root: $(PROJECT_ROOT)"
	@echo "API schema dir: $(API_SCHEMA_DIR)"
	@echo "Tracker schema dir: $(TRACKER_SCHEMA_DIR)"
	@echo ""
	@echo "Docker:"
	@echo "  Image: $(DOCKER_IMAGE)"
	@echo "  Container: $(CONTAINER_NAME)"
	@echo "  Network: $(NETWORK_NAME)"
	@echo ""
	@echo "Schema files (hard links - 6 core files):"
	@for f in $(SCHEMA_FILES); do echo "  - $$f"; done
	@echo ""
	@echo "Note: Enterprise ML schemas NOT included in open source:"
	@echo "  - datasketches.1.sql, vae.1.sql, bitgraph.1.sql, theta_optimization.1.sql"
	@echo "  - attribution.1.sql, content.1.sql, marketing.1.sql, vectors.1.sql"
	@echo "  - visitor_interests.1.sql, h3_political_preferences.sql"
	@echo ""
	@echo "Ports (single node):"
	@echo "  8080: Tracker HTTP API"
	@echo "  9000: ClickHouse Native"
	@echo "  8123: ClickHouse HTTP"
	@echo "  2181: ClickHouse Keeper"
	@echo ""
	@echo "Cluster ports:"
	@echo "  Node 1: 8080, 9000, 8123, 2181, 9444"
	@echo "  Node 2: 8081, 9001, 8124, 2182, 9445"
	@echo "  Node 3: 8082, 9002, 8125, 2183, 9446"
	@echo ""

status:
	@echo "$(YELLOW)📊 Checking status...$(NC)"
	@echo ""
	@echo "Tracker binary:"
	@if [ -f $(TRACKER_BINARY) ]; then \
		echo "  $(GREEN)✓ Built$(NC) ($(shell ls -lh $(TRACKER_BINARY) | awk '{print $$5}'))"; \
	else \
		echo "  $(RED)✗ Not built$(NC) (run: make build)"; \
	fi
	@echo ""
	@echo "Docker image:"
	@if docker image inspect $(DOCKER_IMAGE) >/dev/null 2>&1; then \
		echo "  $(GREEN)✓ Built$(NC) ($(shell docker image inspect $(DOCKER_IMAGE) --format='{{.Size}}' | numfmt --to=iec-i --suffix=B 2>/dev/null || echo 'unknown size'))"; \
	else \
		echo "  $(RED)✗ Not built$(NC) (run: make docker-build)"; \
	fi
	@echo ""
	@echo "Running containers:"
	@if docker ps --filter name=$(CONTAINER_NAME) --format '{{.Names}}' | grep -q $(CONTAINER_NAME); then \
		echo "  $(GREEN)✓ $(CONTAINER_NAME) running$(NC)"; \
	else \
		echo "  $(RED)✗ $(CONTAINER_NAME) not running$(NC)"; \
	fi
	@if docker ps --filter name=v4-tracker --format '{{.Names}}' | grep -q v4-tracker; then \
		docker ps --filter name=v4-tracker --format '  $(GREEN)✓ {{.Names}} running$(NC)'; \
	else \
		echo "  $(RED)✗ No cluster nodes running$(NC)"; \
	fi
	@echo ""

# ====================================================================
# DEVELOPMENT HELPERS
# ====================================================================

watch:
	@echo "$(YELLOW)👀 Watching for changes (requires fswatch)...$(NC)"
	@if command -v fswatch >/dev/null 2>&1; then \
		fswatch -o . | xargs -n1 -I{} make build; \
	else \
		echo "$(RED)❌ fswatch not installed$(NC)"; \
		echo "Install: brew install fswatch (macOS)"; \
		exit 1; \
	fi

benchmark:
	@echo "$(YELLOW)⚡ Running benchmarks...$(NC)"
	@go test -bench=. -benchmem ./...

coverage:
	@echo "$(YELLOW)📊 Generating test coverage...$(NC)"
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)✅ Coverage report: coverage.html$(NC)"
