#!/bin/bash

# ClickHouse Integration Test Runner
# This script runs comprehensive tests for the new ClickHouse functionality

echo "🚀 ClickHouse Integration Test Runner"
echo "====================================="

# Check if ClickHouse is running
echo "📡 Checking ClickHouse connectivity..."
if ! clickhouse client --query "SELECT 1" > /dev/null 2>&1; then
    echo "❌ ClickHouse is not running or not accessible on localhost:9000"
    echo "   Please start ClickHouse with: clickhouse server"
    echo "   Or check if it's running on a different port"
    exit 1
fi

echo "✅ ClickHouse is running and accessible"

# Check if Go is available
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed or not in PATH"
    exit 1
fi

echo "✅ Go is available"

# Navigate to tests directory
cd "$(dirname "$0")"

# Initialize Go module if it doesn't exist
if [ ! -f "go.mod" ]; then
    echo "📦 Initializing Go module..."
    go mod init clickhouse-tests
    go mod tidy
fi

# Install required dependencies
echo "📦 Installing dependencies..."
go get github.com/ClickHouse/clickhouse-go/v2
go get github.com/google/uuid

# Run the integration test
echo "🧪 Running ClickHouse integration tests..."
echo ""

if go run clickhouse_integration_test.go; then
    echo ""
    echo "🎉 All tests passed successfully!"
    echo ""
    echo "📊 Test Summary:"
    echo "   ✅ Basic connectivity"
    echo "   ✅ Event insertion"
    echo "   ✅ Campaign telemetry"
    echo "   ✅ MThreads integration"
    echo "   ✅ MStore integration"
    echo "   ✅ MTriage integration"
    echo "   ✅ LTV processing"
    echo "   ✅ Complex user journey"
    echo ""
    echo "Your ClickHouse implementation is working correctly!"
else
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
    exit 1
fi