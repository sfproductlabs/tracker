/*===----------- batch_manager.go - intelligent batching for ClickHouse  -----===
 *
 * This file implements a comprehensive batching system optimized for ClickHouse
 * performance. It supports multiple batching strategies, per-table configuration,
 * and automatic flush mechanisms.
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2024 Andrew Grosser. All Rights Reserved.
 *
 *===----------------------------------------------------------------------===
 */
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// BatchStrategy defines the batching approach
type BatchStrategy string

const (
	StrategyTimeBasedBatch   BatchStrategy = "time"      // Flush every X seconds
	StrategySizeBasedBatch   BatchStrategy = "size"      // Flush when N rows collected
	StrategyHybridBatch      BatchStrategy = "hybrid"    // Flush on time OR size
	StrategyMemoryBasedBatch BatchStrategy = "memory"    // Flush when memory threshold hit
	StrategyAdaptiveBatch    BatchStrategy = "adaptive"  // Dynamically adjust based on load
	StrategyImmediateBatch   BatchStrategy = "immediate" // No batching (for critical events)
)

// BatchConfig defines configuration for a specific table's batching behavior
type BatchConfig struct {
	TableName         string        `json:"table_name"`
	Strategy          BatchStrategy `json:"strategy"`
	MaxBatchSize      int           `json:"max_batch_size"`     // Number of rows
	MaxBatchTime      time.Duration `json:"max_batch_time"`     // Time threshold
	MaxMemoryMB       int           `json:"max_memory_mb"`      // Memory threshold in MB
	Priority          int           `json:"priority"`           // 1=highest, 10=lowest
	RetryAttempts     int           `json:"retry_attempts"`     // Number of retry attempts
	RetryBackoffMs    int           `json:"retry_backoff_ms"`   // Backoff between retries
	EnableCompression bool          `json:"enable_compression"` // Compress batch data
}

// DefaultBatchConfigs provides optimized defaults for different table types
var DefaultBatchConfigs = map[string]BatchConfig{
	"events": {
		TableName:         "events",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      100000,
		MaxBatchTime:      10 * time.Second,
		MaxMemoryMB:       100,
		Priority:          2, // Higher priority for events
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	// NOTE: events_recent is a VIEW, not a table - cannot batch insert to views
	// All events are written to the events table
	"mthreads": {
		TableName:         "mthreads",
		Strategy:          StrategyTimeBasedBatch,
		MaxBatchSize:      10000,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       5,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    200,
		EnableCompression: false,
	},
	"mstore": {
		TableName:         "mstore",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      500,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       8,
		Priority:          4,
		RetryAttempts:     2,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"mtriage": {
		TableName:         "mtriage",
		Strategy:          StrategyHybridBatch, // Using HybridBatch to support SETTINGS insert_deduplicate
		MaxBatchSize:      50,                  // Increased from 10 for better batching efficiency
		MaxBatchTime:      2 * time.Second,     // 2 second window - balance between urgency and batching
		MaxMemoryMB:       2,
		Priority:          1, // Highest priority - critical for follow-ups
		RetryAttempts:     5,
		RetryBackoffMs:    50,
		EnableCompression: false,
	},
	"visitors": {
		TableName:         "visitors",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      10 * time.Second,
		MaxMemoryMB:       3,
		Priority:          5,
		RetryAttempts:     2,
		RetryBackoffMs:    300,
		EnableCompression: true,
	},
	"payments": {
		TableName:         "payments",
		Strategy:          StrategyImmediateBatch,
		MaxBatchSize:      1,
		MaxBatchTime:      0,
		MaxMemoryMB:       1,
		Priority:          1, // Critical financial data
		RetryAttempts:     5,
		RetryBackoffMs:    100,
		EnableCompression: false,
	},
	"impression_daily": {
		TableName:         "impression_daily",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      250,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       6,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    200,
		EnableCompression: true,
	},
	"instant": { //Use this for critical operations that need to be processed immediately, with retry system
		TableName:         "instant",
		Strategy:          StrategyImmediateBatch,
		MaxBatchSize:      1,
		MaxBatchTime:      0,
		MaxMemoryMB:       1,
		Priority:          1, // Highest priority for instant processing
		RetryAttempts:     3,
		RetryBackoffMs:    50,
		EnableCompression: false,
	},
	"agreements": {
		TableName:         "agreements",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      100,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       2,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: false,
	},
	"agreed": {
		TableName:         "agreed",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      100,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       2,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: false,
	},
	"redirects": {
		TableName:         "redirects",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      2 * time.Second,
		MaxMemoryMB:       3,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"redirect_history": {
		TableName:         "redirect_history",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      500,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       5,
		Priority:          4,
		RetryAttempts:     2,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"counters": {
		TableName:         "counters",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       3,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"updates": {
		TableName:         "updates",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       3,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"logs": {
		TableName:         "logs",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      1000,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       8,
		Priority:          4,
		RetryAttempts:     2,
		RetryBackoffMs:    200,
		EnableCompression: true,
	},
	"ips": {
		TableName:         "ips",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      500,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       4,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"routed": {
		TableName:         "routed",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      2 * time.Second,
		MaxMemoryMB:       3,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"hits": {
		TableName:         "hits",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      1000,
		MaxBatchTime:      2 * time.Second,
		MaxMemoryMB:       8,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"dailies": {
		TableName:         "dailies",
		Strategy:          StrategyTimeBasedBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      10 * time.Second,
		MaxMemoryMB:       4,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    200,
		EnableCompression: true,
	},
	"outcomes": {
		TableName:         "outcomes",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       4,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"referrers": {
		TableName:         "referrers",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      400,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       5,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"referrals": {
		TableName:         "referrals",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       4,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"referred": {
		TableName:         "referred",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       4,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"hosts": {
		TableName:         "hosts",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       3,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"browsers": {
		TableName:         "browsers",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       3,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"nodes": {
		TableName:         "nodes",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       3,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"locations": {
		TableName:         "locations",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       4,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"aliases": {
		TableName:         "aliases",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      100,
		MaxBatchTime:      5 * time.Second,
		MaxMemoryMB:       2,
		Priority:          4,
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true,
	},
	"userhosts": {
		TableName:         "userhosts",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       3,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"usernames": {
		TableName:         "usernames",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       3,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"emails": {
		TableName:         "emails",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       3,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"cells": {
		TableName:         "cells",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      4 * time.Second,
		MaxMemoryMB:       3,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"reqs": {
		TableName:         "reqs",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      500,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       5,
		Priority:          3,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"sessions": {
		TableName:         "sessions",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      300,
		MaxBatchTime:      3 * time.Second,
		MaxMemoryMB:       4,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"visitors_latest": {
		TableName:         "visitors_latest",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      200,
		MaxBatchTime:      2 * time.Second,
		MaxMemoryMB:       3,
		Priority:          2,
		RetryAttempts:     3,
		RetryBackoffMs:    100,
		EnableCompression: true,
	},
	"ltv": {
		TableName:         "ltv",
		Strategy:          StrategyImmediateBatch,
		MaxBatchSize:      1,
		MaxBatchTime:      0,
		MaxMemoryMB:       1,
		Priority:          1, // Critical for lifetime value tracking
		RetryAttempts:     5,
		RetryBackoffMs:    50,
		EnableCompression: false,
	},
	"visitor_interests": {
		TableName:         "visitor_interests",
		Strategy:          StrategyHybridBatch,
		MaxBatchSize:      500,             // Batch interests for performance
		MaxBatchTime:      3 * time.Second, // Flush every 3 seconds
		MaxMemoryMB:       5,
		Priority:          3, // Medium priority (not critical like payments)
		RetryAttempts:     3,
		RetryBackoffMs:    150,
		EnableCompression: true, // Compress interest data (JSON fields)
	},
}

// BatchItem represents a single item to be batched
type BatchItem struct {
	ID        uuid.UUID              `json:"id"`
	TableName string                 `json:"table_name"`
	SQL       string                 `json:"sql"`
	Args      []interface{}          `json:"args"`
	Data      map[string]interface{} `json:"data"`
	Priority  int                    `json:"priority"`
	Timestamp time.Time              `json:"timestamp"`
	Size      int                    `json:"size"` // Estimated memory size in bytes
}

// BatchMetrics tracks batching performance
type BatchMetrics struct {
	TotalBatches      int64 `json:"total_batches"`
	TotalItems        int64 `json:"total_items"`
	FailedBatches     int64 `json:"failed_batches"`
	AvgBatchSize      int64 `json:"avg_batch_size"`
	AvgFlushLatencyMs int64 `json:"avg_flush_latency_ms"`
	QueuedItems       int64 `json:"queued_items"`
	MemoryUsageMB     int64 `json:"memory_usage_mb"`
	LastFlushTime     int64 `json:"last_flush_time"`
}

// TableBatch represents a batch for a specific table
type TableBatch struct {
	Config     BatchConfig  `json:"config"`
	Items      []BatchItem  `json:"items"`
	Size       int          `json:"size"` // Total memory size in bytes
	CreatedAt  time.Time    `json:"created_at"`
	LastItemAt time.Time    `json:"last_item_at"`
	mu         sync.RWMutex `json:"-"`
	flushTimer *time.Timer  `json:"-"`
}

// BatchManager orchestrates all batching operations
type BatchManager struct {
	session      clickhouse.Conn
	batches      map[string]*TableBatch
	configs      map[string]BatchConfig
	metrics      *BatchMetrics
	stopChan     chan bool
	flushChan    chan string    // Table name to flush
	priorityChan chan BatchItem // High priority items
	mu           sync.RWMutex
	running      bool
	adaptiveMode bool
	AppConfig    *Configuration

	// Adaptive batching variables
	lastLoad    int64
	avgLatency  int64
	loadSamples []int64
}

// NewBatchManager creates a new batch manager with default configurations
func NewBatchManager(session clickhouse.Conn, appConfig *Configuration) *BatchManager {
	bm := &BatchManager{
		session:      session,
		batches:      make(map[string]*TableBatch),
		configs:      make(map[string]BatchConfig),
		metrics:      &BatchMetrics{},
		stopChan:     make(chan bool),
		flushChan:    make(chan string, 500),
		priorityChan: make(chan BatchItem, 1000),
		adaptiveMode: true,
		loadSamples:  make([]int64, 0, 100),
		AppConfig:    appConfig,
	}

	// Initialize with default configurations
	for tableName, config := range DefaultBatchConfigs {
		bm.configs[tableName] = config
		bm.batches[tableName] = &TableBatch{
			Config:    config,
			Items:     make([]BatchItem, 0, config.MaxBatchSize),
			CreatedAt: time.Now(),
		}
	}

	return bm
}

// Start begins the batch processing goroutines
func (bm *BatchManager) Start() error {
	bm.mu.Lock()

	if bm.running {
		bm.mu.Unlock()
		return fmt.Errorf("batch manager is already running")
	}

	bm.running = true

	// Start main batch processor
	go bm.processBatches()

	// Start priority item processor
	go bm.processPriorityItems()

	// Start adaptive optimizer (if enabled)
	if bm.adaptiveMode {
		go bm.adaptiveOptimizer()
	}

	// Release write lock before acquiring read lock to avoid deadlock
	bm.mu.Unlock()

	// Start flush timers for each table
	bm.mu.RLock()
	for tableName, batch := range bm.batches {
		if batch.Config.Strategy == StrategyTimeBasedBatch || batch.Config.Strategy == StrategyHybridBatch {
			bm.startFlushTimer(tableName)
		}
	}
	bm.mu.RUnlock()

	fmt.Println("✅ Batch Manager started with", len(bm.configs), "table configurations")
	return nil
}

// Stop gracefully shuts down the batch manager
func (bm *BatchManager) Stop() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if !bm.running {
		return nil
	}

	fmt.Println("🛑 Stopping Batch Manager...")

	// Stop all timers
	for _, batch := range bm.batches {
		if batch.flushTimer != nil {
			batch.flushTimer.Stop()
		}
	}

	// Flush all remaining batches
	bm.flushAllBatches()

	// Signal stop
	close(bm.stopChan)
	bm.running = false

	fmt.Println("✅ Batch Manager stopped gracefully")
	return nil
}

// AddItem adds an item to the appropriate batch
func (bm *BatchManager) AddItem(item BatchItem) error {
	start := time.Now()
	defer func() {
		latency := time.Since(start).Milliseconds()
		atomic.AddInt64(&bm.metrics.AvgFlushLatencyMs, latency)
	}()

	// Validate item
	if item.TableName == "" {
		return NewTrackerError(ErrorTypeValidation, "AddItem", "table_name is required", false)
	}

	// Check if table is configured
	config, exists := bm.configs[item.TableName]
	if !exists {
		return NewTrackerError(ErrorTypeValidation, "AddItem", fmt.Sprintf("no configuration for table %s", item.TableName), false)
	}

	// Set defaults
	item.ID = uuid.New()
	item.Timestamp = time.Now()
	if item.Priority == 0 {
		item.Priority = config.Priority
	}

	// Estimate memory size
	item.Size = bm.estimateItemSize(item)

	// Handle immediate strategy or high priority items
	if config.Strategy == StrategyImmediateBatch || item.Priority == 1 {
		select {
		case bm.priorityChan <- item:
			return nil
		default:
			// Priority channel full, process immediately
			return bm.processItemImmediately(item)
		}
	}

	// Add to batch
	return bm.addToBatch(item)
}

// addToBatch adds an item to the appropriate table batch
func (bm *BatchManager) addToBatch(item BatchItem) error {
	bm.mu.RLock()
	batch, exists := bm.batches[item.TableName]
	bm.mu.RUnlock()

	if !exists {
		return NewTrackerError(ErrorTypeValidation, "addToBatch", fmt.Sprintf("batch not found for table %s", item.TableName), false)
	}

	batch.mu.Lock()
	defer batch.mu.Unlock()

	// Add item to batch
	batch.Items = append(batch.Items, item)
	batch.Size += item.Size
	batch.LastItemAt = time.Now()

	// Update metrics
	atomic.AddInt64(&bm.metrics.QueuedItems, 1)
	atomic.AddInt64(&bm.metrics.MemoryUsageMB, int64(item.Size/1024/1024))

	// Check if batch should be flushed
	shouldFlush := false

	switch batch.Config.Strategy {
	case StrategySizeBasedBatch:
		shouldFlush = len(batch.Items) >= batch.Config.MaxBatchSize

	case StrategyMemoryBasedBatch:
		shouldFlush = batch.Size >= (batch.Config.MaxMemoryMB * 1024 * 1024)

	case StrategyHybridBatch:
		shouldFlush = len(batch.Items) >= batch.Config.MaxBatchSize ||
			batch.Size >= (batch.Config.MaxMemoryMB*1024*1024)

	case StrategyAdaptiveBatch:
		shouldFlush = bm.shouldFlushAdaptive(batch)
	}

	if shouldFlush {
		// Trigger async flush - completely non-blocking to prevent HTTP hangs
		select {
		case bm.flushChan <- item.TableName:
			// Successfully queued for flushing
		default:
			// Channel full - completely skip this flush to prevent any blocking
			// The timeout-based flush or next batch will handle it
		}
	}

	return nil
}

// processBatches handles the main batch processing loop
func (bm *BatchManager) processBatches() {
	for {
		select {
		case <-bm.stopChan:
			return

		case tableName := <-bm.flushChan:
			if err := bm.flushBatch(tableName); err != nil {
				globalMetrics.UpdateErrorCount()
				fmt.Printf("[ERROR] Failed to flush batch for table %s: %v\n", tableName, err)
			}

		case <-time.After(100 * time.Millisecond):
			// Check for timeout-based flushes
			bm.checkTimeoutFlushes()
		}
	}
}

// processPriorityItems handles high-priority items immediately
func (bm *BatchManager) processPriorityItems() {
	for {
		select {
		case <-bm.stopChan:
			return

		case item := <-bm.priorityChan:
			if bm.AppConfig.Debug {
				fmt.Printf("[DEBUG] Processing priority item for table: %s\n", item.TableName)
			}
			if err := bm.processItemImmediately(item); err != nil {
				globalMetrics.UpdateErrorCount()
				fmt.Printf("[ERROR] Failed to process priority item: %v\n", err)
			} else if bm.AppConfig.Debug {
				fmt.Printf("[DEBUG] Successfully processed priority item for table: %s\n", item.TableName)
			}
		}
	}
}

// processItemImmediately processes a single item without batching
func (bm *BatchManager) processItemImmediately(item BatchItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()

	if bm.AppConfig.Debug {
		sqlPreview := item.SQL
		if len(sqlPreview) > 100 {
			sqlPreview = sqlPreview[:100] + "..."
		}
		fmt.Printf("[DEBUG] Executing SQL for table %s: %s\n", item.TableName, sqlPreview)
	}

	err := bm.session.Exec(ctx, item.SQL, item.Args...)
	latency := time.Since(start)

	if bm.AppConfig.Debug {
		fmt.Printf("[DEBUG] Exec result for %s: error=%v, latency=%v\n", item.TableName, err, latency)
	}

	if err != nil {
		return NewTrackerError(ErrorTypeQuery, "processItemImmediately",
			fmt.Sprintf("failed to execute immediate query for table %s: %v", item.TableName, err), true)
	}

	// Update metrics
	atomic.AddInt64(&bm.metrics.TotalBatches, 1)
	atomic.AddInt64(&bm.metrics.TotalItems, 1)
	atomic.AddInt64(&bm.metrics.AvgFlushLatencyMs, latency.Milliseconds())

	return nil
}

// flushBatch flushes a specific table's batch
func (bm *BatchManager) flushBatch(tableName string) error {
	bm.mu.RLock()
	batch, exists := bm.batches[tableName]
	bm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("batch not found for table %s", tableName)
	}

	batch.mu.Lock()

	// Check if batch is empty
	if len(batch.Items) == 0 {
		batch.mu.Unlock()
		return nil
	}

	// Copy items and reset batch
	items := make([]BatchItem, len(batch.Items))
	copy(items, batch.Items)

	batch.Items = batch.Items[:0] // Reset slice but keep capacity
	batch.Size = 0
	batch.CreatedAt = time.Now()

	batch.mu.Unlock()

	// Process batch
	return bm.executeBatch(tableName, items)
}

// executeBatch executes a batch of items for a specific table
func (bm *BatchManager) executeBatch(tableName string, items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	start := time.Now()
	config := bm.configs[tableName]

	// Create batch context with timeout (reduced to prevent hanging)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Prepare batch insert
	batch, err := bm.session.PrepareBatch(ctx, bm.buildBatchSQL(tableName, items[0]))
	if err != nil {
		atomic.AddInt64(&bm.metrics.FailedBatches, 1)
		return NewTrackerError(ErrorTypeQuery, "executeBatch",
			fmt.Sprintf("failed to prepare batch for table %s: %v", tableName, err), true)
	}

	// Add all items to batch
	for i, item := range items {
		if err := batch.Append(item.Args...); err != nil {
			atomic.AddInt64(&bm.metrics.FailedBatches, 1)
			fmt.Printf("[ERROR] Batch append failed for table %s, item %d: %v\n", tableName, i, err)
			if bm.AppConfig.Debug {
				fmt.Printf("[DEBUG] Failed item args: %+v\n", item.Args)
			}
			return NewTrackerError(ErrorTypeQuery, "executeBatch",
				fmt.Sprintf("failed to append to batch for table %s: %v", tableName, err), true)
		}
	}

	// Execute batch with retry logic
	var execErr error
	for attempt := 0; attempt <= config.RetryAttempts; attempt++ {
		execErr = batch.Send()
		if execErr == nil {
			break
		}

		if attempt < config.RetryAttempts {
			time.Sleep(time.Duration(config.RetryBackoffMs) * time.Millisecond)
		}
	}

	if execErr != nil {
		atomic.AddInt64(&bm.metrics.FailedBatches, 1)

		// Log detailed error information
		fmt.Printf("[ERROR] Batch execution failed for table %s: %v\n", tableName, execErr)

		if bm.AppConfig.Debug {
			fmt.Printf("[DEBUG] SQL: %s\n", bm.buildBatchSQL(tableName, items[0]))
			fmt.Printf("[DEBUG] Number of items: %d\n", len(items))

			// Log first few items for debugging
			for i, item := range items {
				if i >= 3 { // Only log first 3 items to avoid spam
					fmt.Printf("[DEBUG] ... and %d more items\n", len(items)-3)
					break
				}
				fmt.Printf("[DEBUG] Item %d args: %+v\n", i, item.Args)
			}
		}

		return NewTrackerError(ErrorTypeQuery, "executeBatch",
			fmt.Sprintf("failed to execute batch for table %s after %d attempts: %v",
				tableName, config.RetryAttempts+1, execErr), true)
	}

	// Update metrics
	latency := time.Since(start)
	atomic.AddInt64(&bm.metrics.TotalBatches, 1)
	atomic.AddInt64(&bm.metrics.TotalItems, int64(len(items)))
	atomic.AddInt64(&bm.metrics.AvgFlushLatencyMs, latency.Milliseconds())
	atomic.AddInt64(&bm.metrics.QueuedItems, -int64(len(items)))
	atomic.StoreInt64(&bm.metrics.LastFlushTime, time.Now().Unix())

	// Update average batch size
	totalBatches := atomic.LoadInt64(&bm.metrics.TotalBatches)
	if totalBatches > 0 {
		avgSize := atomic.LoadInt64(&bm.metrics.TotalItems) / totalBatches
		atomic.StoreInt64(&bm.metrics.AvgBatchSize, avgSize)
	}

	// Debug logging for batch flush
	if bm.AppConfig.Debug {
		fmt.Printf("[BATCH] Flushed %d items to table %s in %v\n", len(items), tableName, latency)
	}

	return nil
}

// buildBatchSQL constructs the batch insert SQL for a table
func (bm *BatchManager) buildBatchSQL(tableName string, sampleItem BatchItem) string {
	// Use the SQL from the sample item, but this could be enhanced
	// to build optimized batch SQL based on table schema
	return sampleItem.SQL
}

// checkTimeoutFlushes checks for batches that have exceeded their time threshold
func (bm *BatchManager) checkTimeoutFlushes() {
	now := time.Now()

	bm.mu.RLock()
	defer bm.mu.RUnlock()

	for tableName, batch := range bm.batches {
		if batch.Config.Strategy == StrategyTimeBasedBatch || batch.Config.Strategy == StrategyHybridBatch {
			batch.mu.RLock()
			shouldFlush := len(batch.Items) > 0 &&
				now.Sub(batch.CreatedAt) >= batch.Config.MaxBatchTime
			batch.mu.RUnlock()

			if shouldFlush {
				select {
				case bm.flushChan <- tableName:
				default:
					// Channel full, skip this flush
				}
			}
		}
	}
}

// flushAllBatches flushes all pending batches (used during shutdown)
func (bm *BatchManager) flushAllBatches() {
	bm.mu.RLock()
	tableNames := make([]string, 0, len(bm.batches))
	for tableName := range bm.batches {
		tableNames = append(tableNames, tableName)
	}
	bm.mu.RUnlock()

	for _, tableName := range tableNames {
		if err := bm.flushBatch(tableName); err != nil {
			fmt.Printf("[ERROR] Failed to flush batch during shutdown for table %s: %v\n", tableName, err)
		}
	}
}

// startFlushTimer starts a timer for time-based flushing
func (bm *BatchManager) startFlushTimer(tableName string) {
	batch := bm.batches[tableName]
	if batch.flushTimer != nil {
		batch.flushTimer.Stop()
	}

	batch.flushTimer = time.AfterFunc(batch.Config.MaxBatchTime, func() {
		select {
		case bm.flushChan <- tableName:
		default:
			// Channel full, try again later
			bm.startFlushTimer(tableName)
		}
	})
}

// estimateItemSize estimates the memory size of a batch item
func (bm *BatchManager) estimateItemSize(item BatchItem) int {
	// Basic estimation - could be enhanced with more accurate measurement
	size := len(item.SQL)
	size += len(item.TableName)

	// Estimate args size
	for _, arg := range item.Args {
		switch v := arg.(type) {
		case string:
			size += len(v)
		case []byte:
			size += len(v)
		default:
			size += 8 // Rough estimate for other types
		}
	}

	// Estimate data size (JSON approximation)
	if item.Data != nil {
		if jsonData, err := json.Marshal(item.Data); err == nil {
			size += len(jsonData)
		}
	}

	return size
}

// shouldFlushAdaptive determines if a batch should be flushed using adaptive logic
func (bm *BatchManager) shouldFlushAdaptive(batch *TableBatch) bool {
	// Load-based adaptive batching
	currentLoad := atomic.LoadInt64(&globalMetrics.EventCount)
	loadDiff := currentLoad - bm.lastLoad

	// High load = smaller batches for lower latency
	// Low load = larger batches for better throughput
	if loadDiff > 100 { // High load
		return len(batch.Items) >= (batch.Config.MaxBatchSize / 2)
	} else if loadDiff < 10 { // Low load
		return len(batch.Items) >= batch.Config.MaxBatchSize
	}

	// Normal load - use hybrid approach
	return len(batch.Items) >= batch.Config.MaxBatchSize ||
		batch.Size >= (batch.Config.MaxMemoryMB*1024*1024)
}

// adaptiveOptimizer continuously optimizes batch parameters based on performance
func (bm *BatchManager) adaptiveOptimizer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bm.stopChan:
			return

		case <-ticker.C:
			bm.optimizeBatchParameters()
		}
	}
}

// optimizeBatchParameters adjusts batch parameters based on current performance
func (bm *BatchManager) optimizeBatchParameters() {
	// Get current metrics
	avgLatency := atomic.LoadInt64(&bm.metrics.AvgFlushLatencyMs)
	totalBatches := atomic.LoadInt64(&bm.metrics.TotalBatches)
	failedBatches := atomic.LoadInt64(&bm.metrics.FailedBatches)

	if totalBatches == 0 {
		return
	}

	errorRate := float64(failedBatches) / float64(totalBatches)

	// Adjust parameters based on performance
	bm.mu.Lock()
	for tableName, config := range bm.configs {
		newConfig := config

		// If error rate is high, reduce batch sizes
		if errorRate > 0.05 { // 5% error rate
			newConfig.MaxBatchSize = max(newConfig.MaxBatchSize/2, 10)
			newConfig.MaxMemoryMB = max(newConfig.MaxMemoryMB/2, 1)
		}

		// If latency is high, reduce batch sizes
		if avgLatency > 1000 { // 1 second
			newConfig.MaxBatchSize = max(newConfig.MaxBatchSize*3/4, 10)
		}

		// If latency is low and error rate is low, can increase batch sizes
		if avgLatency < 100 && errorRate < 0.01 { // 100ms, 1% error rate
			newConfig.MaxBatchSize = min(newConfig.MaxBatchSize*5/4, 5000)
			newConfig.MaxMemoryMB = min(newConfig.MaxMemoryMB*5/4, 50)
		}

		// Update if changed
		if newConfig != config {
			bm.configs[tableName] = newConfig
			if batch, exists := bm.batches[tableName]; exists {
				batch.Config = newConfig
			}
		}
	}
	bm.mu.Unlock()
}

// GetMetrics returns current batching metrics
func (bm *BatchManager) GetMetrics() BatchMetrics {
	return BatchMetrics{
		TotalBatches:      atomic.LoadInt64(&bm.metrics.TotalBatches),
		TotalItems:        atomic.LoadInt64(&bm.metrics.TotalItems),
		FailedBatches:     atomic.LoadInt64(&bm.metrics.FailedBatches),
		AvgBatchSize:      atomic.LoadInt64(&bm.metrics.AvgBatchSize),
		AvgFlushLatencyMs: atomic.LoadInt64(&bm.metrics.AvgFlushLatencyMs),
		QueuedItems:       atomic.LoadInt64(&bm.metrics.QueuedItems),
		MemoryUsageMB:     atomic.LoadInt64(&bm.metrics.MemoryUsageMB),
		LastFlushTime:     atomic.LoadInt64(&bm.metrics.LastFlushTime),
	}
}

// UpdateConfig updates the configuration for a specific table
func (bm *BatchManager) UpdateConfig(tableName string, config BatchConfig) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.configs[tableName] = config

	if batch, exists := bm.batches[tableName]; exists {
		batch.mu.Lock()
		batch.Config = config
		batch.mu.Unlock()
	} else {
		// Create new batch
		bm.batches[tableName] = &TableBatch{
			Config:    config,
			Items:     make([]BatchItem, 0, config.MaxBatchSize),
			CreatedAt: time.Now(),
		}
	}

	return nil
}
