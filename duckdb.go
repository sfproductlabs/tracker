package main

import (
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb" // Import DuckDB driver
)

// Connect initiates the primary connection to DuckDB
func (i *DuckService) connect() error {
	err := fmt.Errorf("Could not connect to DuckDB")

	// Check if connection already exists
	if i.Session != nil {
		return fmt.Errorf("database connection already exists")
	}

	// Open DuckDB connection with configuration
	i.Session, err = sql.Open("duckdb", ":memory:?access_mode=READ_WRITE&threads=4&memory_limit=4GB")
	if err != nil {
		fmt.Println("[ERROR] Connecting to DuckDB:", err)
		return err
	}

	// Configure connection pool settings
	i.Session.SetMaxOpenConns(i.Configuration.Connections)
	i.Session.SetMaxIdleConns(5)
	i.Session.SetConnMaxLifetime(time.Second * time.Duration(i.Configuration.Timeout/1000))
	i.Session.SetConnMaxIdleTime(time.Millisecond * 500)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i.Configuration.Timeout)*time.Millisecond)
	defer cancel()

	if err = i.Session.PingContext(ctx); err != nil {
		i.close() // Clean up if connection test fails
		fmt.Println("[ERROR] Verifying DuckDB connection:", err)
		return err
	}

	// Setup tables
	if err = i.createTables(); err != nil {
		i.close()
		fmt.Println("[ERROR] Creating DuckDB tables:", err)
		return err
	}

	i.Configuration.Session = i

	// Setup rand seed (following Cassandra implementation pattern)
	rand.Seed(time.Now().UTC().UnixNano())

	// Setup limit checker if configured
	if i.AppConfig.ProxyDailyLimit > 0 && i.AppConfig.ProxyDailyLimitCheck == nil && i.AppConfig.ProxyDailyLimitChecker == SERVICE_TYPE_DUCKDB {
		i.AppConfig.ProxyDailyLimitCheck = func(ip string) uint64 {
			var total uint64
			err := i.Session.QueryRow(`SELECT total FROM dailies WHERE ip = ? AND day = ?`,
				ip, time.Now().UTC().Format("2006-01-02")).Scan(&total)
			if err != nil {
				return 0xFFFFFFFFFFFFFFFF
			}
			return total
		}
	}

	// Start background health check if not already running
	if i.HealthCheckTicker == nil {
		interval := 5 * time.Minute //default interval
		if i.AppConfig.HealthCheckInterval > 0 {
			interval = time.Duration(i.AppConfig.HealthCheckInterval) * time.Second
		}

		i.HealthCheckTicker = time.NewTicker(interval)
		i.HealthCheckDone = make(chan bool)

		go i.runHealthCheck()
	}

	return nil
}

// Close terminates the DuckDB connection
func (i *DuckService) close() error {
	// Stop health check if running
	if i.HealthCheckTicker != nil {
		i.HealthCheckTicker.Stop()
		i.HealthCheckDone <- true
		close(i.HealthCheckDone)
		i.HealthCheckTicker = nil
	}

	// Existing close logic
	if i.Session != nil {
		return i.Session.Close()
	}
	return nil
}

// Listen is not implemented for DuckDB
func (i *DuckService) listen() error {
	return fmt.Errorf("[ERROR] DuckDB listen not implemented")
}

// Auth checks user credentials
func (i *DuckService) auth(s *ServiceArgs) error {
	if *s.Values == nil {
		return fmt.Errorf("User not provided")
	}
	uid := (*s.Values)["uid"]
	if uid == "" {
		return fmt.Errorf("User ID not provided")
	}
	password := (*s.Values)["password"]
	if password == "" {
		return fmt.Errorf("User pass not provided")
	}
	var pwd string
	if err := i.Session.QueryRow("SELECT pwd FROM accounts WHERE uid = ?", uid).Scan(&pwd); err == nil {
		if pwd != sha(password) {
			return fmt.Errorf("Bad pass")
		}
		return nil
	} else {
		return err
	}
}

// Serve handles HTTP requests
func (i *DuckService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	if i.Configuration.ProxyRealtimeStorageService != nil && i.Configuration.ProxyRealtimeStorageService.Session != nil {
		return i.Configuration.ProxyRealtimeStorageService.Session.serve(w, r, s)
	} else {
		return fmt.Errorf("[ERROR] DuckDB proxy storage service not implemente or connection not established")
	}
}

// Helper function to create required tables
func (i *DuckService) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS events (
			eid UUID PRIMARY KEY,
			vid UUID,
			sid UUID,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			updated TIMESTAMP,
			uid UUID,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth UUID,
			duration INTEGER,
			xid VARCHAR,
			split VARCHAR,
			ename VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			country VARCHAR,
			region VARCHAR,
			city VARCHAR,
			zip VARCHAR,
			term VARCHAR,
			etyp VARCHAR,
			ver INTEGER,
			sink VARCHAR,
			score DOUBLE,
			params JSON,
			payment JSON,
			targets JSON,
			relation VARCHAR,
			rid UUID
		)`,
		`CREATE TABLE IF NOT EXISTS events_recent (
			eid UUID PRIMARY KEY,
			vid UUID,
			sid UUID,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			updated TIMESTAMP,
			uid UUID,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth UUID,
			duration INTEGER,
			xid VARCHAR,
			split VARCHAR,
			ename VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			country VARCHAR,
			region VARCHAR,
			city VARCHAR,
			zip VARCHAR,
			term VARCHAR,
			etyp VARCHAR,
			ver INTEGER,
			sink VARCHAR,
			score DOUBLE,
			params JSON,
			payment JSON,
			targets JSON,
			relation VARCHAR,
			rid UUID
		)`,
		`CREATE TABLE IF NOT EXISTS counters (
			id UUID PRIMARY KEY,
			total INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS accounts (
			uid UUID PRIMARY KEY,
			pwd VARCHAR NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agreements (
			vid UUID,
			created TIMESTAMP,
			cflags INTEGER,
			sid UUID,
			uid UUID,
			avid VARCHAR,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			gaid VARCHAR,
			idfa VARCHAR,
			msid VARCHAR,
			fbid VARCHAR,
			country VARCHAR,
			region VARCHAR,
			culture VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			term VARCHAR,
			ref VARCHAR,
			rcode VARCHAR,
			aff VARCHAR,
			browser VARCHAR,
			bhash VARCHAR,
			device VARCHAR,
			os VARCHAR,
			tz VARCHAR,
			latlon VARCHAR,
			zip VARCHAR,
			owner VARCHAR,
			org VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS agreed (
			vid UUID,
			created TIMESTAMP,
			cflags INTEGER,
			sid UUID,
			uid UUID,
			avid VARCHAR,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			gaid VARCHAR,
			idfa VARCHAR,
			msid VARCHAR,
			fbid VARCHAR,
			country VARCHAR,
			region VARCHAR,
			culture VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			term VARCHAR,
			ref VARCHAR,
			rcode VARCHAR,
			aff VARCHAR,
			browser VARCHAR,
			bhash VARCHAR,
			device VARCHAR,
			os VARCHAR,
			tz VARCHAR,
			latlon VARCHAR,
			zip VARCHAR,
			owner VARCHAR,
			org VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS jurisdictions (
			id UUID PRIMARY KEY,
			name VARCHAR,
			description VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS redirects (
			urlfrom VARCHAR PRIMARY KEY,
			urlto VARCHAR,
			created TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS redirect_history (
			id UUID PRIMARY KEY,
			urlfrom VARCHAR,
			urlto VARCHAR,
			created TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS ltv (
			hhash VARCHAR,
			uid UUID,
			vid UUID,
			sid UUID,
			payments VARCHAR, -- Store JSON as string
			paid DOUBLE,
			org VARCHAR,
			updated TIMESTAMP,
			updater VARCHAR,
			created TIMESTAMP,
			owner VARCHAR,
			PRIMARY KEY (hhash, uid)
		)`,
		`CREATE TABLE IF NOT EXISTS ltvu (
			hhash VARCHAR,
			uid UUID,
			orid UUID,
			vid UUID,
			sid UUID,
			payments VARCHAR, -- Store JSON as string
			paid DOUBLE,
			org VARCHAR,
			updated TIMESTAMP,
			updater VARCHAR,
			created TIMESTAMP,
			owner VARCHAR,
			PRIMARY KEY (hhash, uid, orid)
		)`,
		`CREATE TABLE IF NOT EXISTS ltvv (
			hhash VARCHAR,
			vid UUID,
			orid UUID,
			uid UUID,
			sid UUID,
			payments VARCHAR, -- Store JSON as string
			paid DOUBLE,
			org VARCHAR,
			updated TIMESTAMP,
			updater VARCHAR,
			created TIMESTAMP,
			owner VARCHAR,
			PRIMARY KEY (hhash, vid, orid)
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			hhash VARCHAR,
			vid UUID,
			uid UUID,
			sid UUID,
			PRIMARY KEY (hhash, vid, uid)
		)`,
		`CREATE TABLE IF NOT EXISTS usernames (
			hhash VARCHAR,
			vid UUID,
			uhash VARCHAR,
			sid UUID,
			PRIMARY KEY (hhash, vid, uhash)
		)`,
		`CREATE TABLE IF NOT EXISTS emails (
			hhash VARCHAR,
			vid UUID,
			ehash VARCHAR,
			sid UUID,
			PRIMARY KEY (hhash, vid, ehash)
		)`,
		`CREATE TABLE IF NOT EXISTS cells (
			hhash VARCHAR,
			vid UUID,
			chash VARCHAR,
			sid UUID,
			PRIMARY KEY (hhash, vid, chash)
		)`,
		`CREATE TABLE IF NOT EXISTS reqs (
			hhash VARCHAR,
			vid UUID,
			total INTEGER DEFAULT 0,
			PRIMARY KEY (hhash, vid)
		)`,
		`CREATE TABLE IF NOT EXISTS visitors (
			vid UUID PRIMARY KEY,
			did UUID,
			sid UUID,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			uid UUID,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth VARCHAR,
			xid UUID,
			split VARCHAR,
			ename VARCHAR,
			etyp VARCHAR,
			ver VARCHAR,
			sink VARCHAR,
			score DOUBLE,
			params JSON,
			gaid VARCHAR,
			idfa VARCHAR,
			msid VARCHAR,
			fbid VARCHAR,
			country VARCHAR,
			region VARCHAR,
			city VARCHAR,
			zip VARCHAR,
			culture VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			term VARCHAR,
			ref VARCHAR,
			rcode VARCHAR,
			aff VARCHAR,
			browser VARCHAR,
			device VARCHAR,
			os VARCHAR,
			tz VARCHAR,
			vp VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS visitors_latest (
			vid UUID PRIMARY KEY,
			did UUID,
			sid UUID,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			uid UUID,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth VARCHAR,
			xid UUID,
			split VARCHAR,
			ename VARCHAR,
			etyp VARCHAR,
			ver VARCHAR,
			sink VARCHAR,
			score DOUBLE,
			params JSON,
			gaid VARCHAR,
			idfa VARCHAR,
			msid VARCHAR,
			fbid VARCHAR,
			country VARCHAR,
			region VARCHAR,
			city VARCHAR,
			zip VARCHAR,
			culture VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			term VARCHAR,
			ref VARCHAR,
			rcode VARCHAR,
			aff VARCHAR,
			browser VARCHAR,
			device VARCHAR,
			os VARCHAR,
			tz VARCHAR,
			vp VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			vid UUID,
			did UUID,
			sid UUID,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			uid UUID,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth VARCHAR,
			duration INTEGER,
			xid UUID,
			split VARCHAR,
			ename VARCHAR,
			etyp VARCHAR,
			ver VARCHAR,
			sink VARCHAR,
			score DOUBLE,
			params JSON,
			gaid VARCHAR,
			idfa VARCHAR,
			msid VARCHAR,
			fbid VARCHAR,
			country VARCHAR,
			region VARCHAR,
			city VARCHAR,
			zip VARCHAR,
			culture VARCHAR,
			source VARCHAR,
			medium VARCHAR,
			campaign VARCHAR,
			term VARCHAR,
			ref VARCHAR,
			rcode VARCHAR,
			aff VARCHAR,
			browser VARCHAR,
			device VARCHAR,
			os VARCHAR,
			tz VARCHAR,
			vp VARCHAR,
			PRIMARY KEY (vid, sid)
		)`,
		`CREATE TABLE IF NOT EXISTS logs (
			id UUID PRIMARY KEY,
			ldate TIMESTAMP,
			created TIMESTAMP,
			ltime INTEGER,
			topic VARCHAR,
			name VARCHAR,
			host VARCHAR,
			hostname VARCHAR,
			owner VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			level INTEGER,
			msg JSON,
			params JSON
		)`,
		`CREATE TABLE IF NOT EXISTS dailies (
			ip VARCHAR,
			day TIMESTAMP,
			total INTEGER DEFAULT 0,
			PRIMARY KEY (ip, day)
		)`,
		`CREATE TABLE IF NOT EXISTS table_versions (
			table_name VARCHAR PRIMARY KEY,
			version INTEGER DEFAULT 0
		)`,
	}

	for _, query := range queries {
		if _, err := i.Session.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (i *DuckService) prune() error {
	if !i.AppConfig.PruneLogsOnly {
		// Prune old records from main tables
		tables := []string{"visitors", "sessions", "events", "events_recent"}

		// Default TTL of 30 days if not specified
		ttl := 2592000
		if i.AppConfig.PruneLogsTTL > 0 {
			ttl = i.AppConfig.PruneLogsTTL
		}
		pruneTime := time.Now().Add(-time.Duration(ttl) * time.Second)

		for _, table := range tables {
			var total, pruned int64

			// First count total records
			err := i.Session.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total)
			if err != nil {
				fmt.Printf("Error counting records in %s: %v\n", table, err)
				continue
			}

			if i.AppConfig.PruneUpdateConfig {
				// Update approach - set fields to null
				result, err := i.Session.Exec(fmt.Sprintf(`
					UPDATE %s 
					SET updated = ?,
						params = NULL
					WHERE created < ?`, table),
					time.Now().UTC(), pruneTime)

				if err != nil {
					fmt.Printf("Error updating records in %s: %v\n", table, err)
					continue
				}

				pruned, _ = result.RowsAffected()

			} else {
				// Delete approach
				result, err := i.Session.Exec(fmt.Sprintf("DELETE FROM %s WHERE created < ?", table), pruneTime)
				if err != nil {
					fmt.Printf("Error deleting from %s: %v\n", table, err)
					continue
				}

				pruned, _ = result.RowsAffected()
			}

			if i.AppConfig.Debug {
				fmt.Printf("Pruned [DuckDB].[%s]: %d/%d rows\n", table, pruned, total)
			}
		}
	}

	// Prune logs table if enabled
	if !i.AppConfig.PruneLogsSkip {
		var total, pruned int64

		// Get total count
		err := i.Session.QueryRow("SELECT COUNT(*) FROM logs").Scan(&total)
		if err != nil {
			fmt.Printf("Error counting logs: %v\n", err)
			return err
		}

		// Use logs TTL from config
		ttl := 2592000 // Default 30 days
		if i.AppConfig.PruneLogsTTL > 0 {
			ttl = i.AppConfig.PruneLogsTTL
		}
		cutoffTime := time.Now().Add(-time.Duration(ttl) * time.Second)

		// Delete old logs
		result, err := i.Session.Exec("DELETE FROM logs WHERE created < ?", cutoffTime)
		if err != nil {
			fmt.Printf("Error pruning logs: %v\n", err)
			return err
		}

		pruned, _ = result.RowsAffected()

		if i.AppConfig.Debug {
			fmt.Printf("Pruned [DuckDB].[logs]: %d/%d rows\n", pruned, total)
		}
	}

	// Update config file if needed
	if i.AppConfig.PruneUpdateConfig {
		s, err := ioutil.ReadFile(i.AppConfig.ConfigFile)
		if err != nil {
			return err
		}

		var j interface{}
		if err := json.Unmarshal(s, &j); err != nil {
			return err
		}

		SetValueInJSON(j, "PruneSkipToTimestamp", time.Now().Unix())

		s, _ = json.Marshal(j)
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, s, "", "    "); err != nil {
			return err
		}

		if err := ioutil.WriteFile(i.AppConfig.ConfigFile, prettyJSON.Bytes(), 0644); err != nil {
			return err
		}
	}

	return nil
}

// Add this method to handle the background health check
func (i *DuckService) runHealthCheck() {
	if i.AppConfig.Debug {
		fmt.Println("[HealthCheck] Starting background health check service")
	}

	for {
		select {
		case <-i.HealthCheckDone:
			if i.AppConfig.Debug {
				fmt.Println("[HealthCheck] Stopping background health check service")
			}
			return
		case <-i.HealthCheckTicker.C:
			if err := i.healthCheck(); err != nil {
				fmt.Printf("[HealthCheck] Error during health check: %v\n", err)
			}
		}
	}
}

// Add a new method for health checks, checks that the database is reachable
// We also check the size of each table, if the size of any table is greater than 100MB we write the data in the table to s3
// OR if the table has not been modified in the past 15 minutes
// We then truncate the table and reset the auto increment id
func (i *DuckService) healthCheck() error {
	// Check database connection
	if i.Session == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Verify S3 configuration if needed
	if i.AppConfig.S3Bucket == "" || i.AppConfig.S3Prefix == "" {
		return fmt.Errorf("S3 configuration missing for table exports")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test connection
	if err := i.Session.PingContext(ctx); err != nil {
		return fmt.Errorf("database ping failed: %v", err)
	}

	// Get list of tables
	tables, err := i.Session.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'main'
	`)
	if err != nil {
		return fmt.Errorf("failed to query tables: %v", err)
	}
	defer tables.Close()

	const (
		maxSizeBytes    = 100 * 1024 * 1024 // 100MB
		inactivityLimit = 15 * time.Minute
	)

	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		// Check table size
		var sizeBytes int64
		err := i.Session.QueryRow(`
			SELECT sum(estimated_size)
			FROM pragma_table_info(?)
		`, tableName).Scan(&sizeBytes)
		if err != nil {
			return fmt.Errorf("failed to get size for table %s: %v", tableName, err)
		}

		// Check last modification time
		var lastModified time.Time
		err = i.Session.QueryRow(`
			SELECT COALESCE(MAX(created), '1970-01-01') 
			FROM ` + tableName).Scan(&lastModified)
		if err != nil {
			return fmt.Errorf("failed to get last modification time for table %s: %v", tableName, err)
		}

		// Export based on condition
		if sizeBytes > maxSizeBytes {
			if err := i.exportAndTruncateTable(tableName, true); err != nil {
				return fmt.Errorf("failed to process table %s: %v", tableName, err)
			}
		} else if time.Since(lastModified) > inactivityLimit {
			if err := i.exportAndTruncateTable(tableName, false); err != nil {
				return fmt.Errorf("failed to process table %s: %v", tableName, err)
			}
		}

		if i.AppConfig.Debug {
			fmt.Printf("[HealthCheck] Processed table %s (size: %.2f MB, last modified: %v)\n",
				tableName,
				float64(sizeBytes)/(1024*1024),
				lastModified)
		}
	}

	return nil
}

// Modified to handle different version behavior
func (i *DuckService) exportAndTruncateTable(tableName string, incrementVersion bool) error {
	tx, err := i.Session.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	// Get current version or create new entry
	var version int
	if incrementVersion {
		// Increment version for size-based exports
		err = tx.QueryRow(`
			INSERT INTO table_versions (table_name, version) 
			VALUES (?, 1)
			ON CONFLICT (table_name) DO UPDATE 
			SET version = table_versions.version + 1
			RETURNING version`, tableName).Scan(&version)
	} else {
		// Use existing version for inactivity-based exports
		err = tx.QueryRow(`
			INSERT INTO table_versions (table_name, version) 
			VALUES (?, 1)
			ON CONFLICT (table_name) DO UPDATE 
			SET version = table_versions.version
			RETURNING version`, tableName).Scan(&version)
	}
	if err != nil {
		return fmt.Errorf("failed to handle version: %v", err)
	}

	// Generate export path with version
	now := time.Now().UTC()
	s3Path := fmt.Sprintf("s3://%s/%s/%s/%d/%02d/%02d/%s_v%d.parquet",
		i.AppConfig.S3Bucket,
		i.AppConfig.S3Prefix,
		tableName,
		now.Year(),
		now.Month(),
		now.Day(),
		i.AppConfig.NodeId,
		version)

	// Export to S3
	_, err = tx.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM %s) 
		TO '%s' (FORMAT 'parquet')
	`, tableName, s3Path))
	if err != nil {
		return fmt.Errorf("failed to export to S3: %v", err)
	}

	// Truncate table after successful export
	_, err = tx.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to truncate table: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// ////////////////////////////////////// DuckDB
func (i *DuckService) write(w *WriteArgs) error {
	if i.Configuration.ProxyRealtimeStorageService != nil && i.Configuration.ProxyRealtimeStorageServiceTypes != 0 && i.Configuration.ProxyRealtimeStorageService.Session != nil {
		w.CallingService = i.Configuration
		go i.Configuration.ProxyRealtimeStorageService.Session.write(w)
	}
	err := fmt.Errorf("Could not write to any cassandra server in cluster")
	v := *w.Values
	switch w.WriteType {
	case WRITE_COUNT:
		if i.AppConfig.Debug {
			fmt.Printf("COUNT %s\n", w)
		}
		tx, err := i.Session.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(`INSERT INTO counters (id, total) 
            VALUES (?, 1) 
            ON CONFLICT (id) DO UPDATE 
            SET total = total + 1`,
			v["id"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[counters]:", err)
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	case WRITE_UPDATE:
		if i.AppConfig.Debug {
			fmt.Printf("UPDATE %s\n", w)
		}
		timestamp := time.Now().UTC()
		updated, ok := v["updated"].(string)
		if ok {
			millis, err := strconv.ParseInt(updated, 10, 64)
			if err == nil {
				timestamp = time.Unix(0, millis*int64(time.Millisecond))
			}
		}
		tx, err := i.Session.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(`INSERT INTO updates (id, updated, msg) 
            VALUES (?, ?, ?)`,
			v["id"], timestamp, v["msg"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[updates]:", err)
		}

		return tx.Commit()
	case WRITE_LOG:
		if i.AppConfig.Debug {
			fmt.Printf("LOG %s\n", w)
		}
		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[params]
		if ps, ok := v["params"].(string); ok {
			temp := make(map[string]string)
			json.Unmarshal([]byte(ps), &temp)
			v["params"] = &temp
		}
		//[ltimenss] ltime as nanosecond string
		var ltime time.Duration
		if lts, ok := v["ltimenss"].(string); ok {
			ns, _ := strconv.ParseInt(lts, 10, 64)
			ltime = time.Duration(ns)
		}
		//[level]
		var level *int64
		if lvl, ok := v["level"].(float64); ok {
			temp := int64(lvl)
			level = &temp
		}

		var topic string
		if ttemp1, ok := v["topic"].(string); ok {
			topic = ttemp1
		} else {
			if ttemp2, ok2 := v["id"].(string); ok2 {
				topic = ttemp2
			}
		}

		cleanInterfaceString(v["ip"])
		cleanInterfaceString(v["topic"])
		cleanInterfaceString(v["name"])
		cleanInterfaceString(v["host"])
		cleanInterfaceString(v["hostname"])
		cleanInterfaceString(v["msg"])

		var iphash string
		if temp, ok := v["ip"].(string); ok && temp != "" {
			//128 bits = ipv6
			iphash = strconv.FormatInt(int64(hash(temp)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(temp+iphash)), 36)
		}

		tx, err := i.Session.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(`INSERT INTO logs
			(id, ldate, created, ltime, topic, name, host, hostname, owner,
			 ip, iphash, level, msg, params)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?)`,
			uuid.New(),
			v["ldate"],
			time.Now().UTC(),
			ltime,
			topic,
			v["name"],
			v["host"],
			v["hostname"],
			v["owner"],
			v["ip"],
			iphash,
			level,
			v["msg"],
			v["params"])

		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[logs]:", err)
		}

		return tx.Commit()

	case WRITE_EVENT:
		//TODO: Commented for AWS, perhaps non-optimal, CHECK
		//go func() {

		//////////////////////////////////////////////
		//FIX CASE
		//////////////////////////////////////////////
		delete(v, "cleanIP")
		cleanString(&(w.Browser))
		cleanString(&(w.Host))
		cleanInterfaceString(v["app"])
		cleanInterfaceString(v["rel"])
		cleanInterfaceString(v["ptyp"])
		cleanInterfaceString(v["xid"])
		cleanInterfaceString(v["split"])
		cleanInterfaceString(v["ename"])
		cleanInterfaceString(v["etyp"])
		cleanInterfaceString(v["sink"])
		cleanInterfaceString(v["source"])
		cleanInterfaceString(v["medium"])
		cleanInterfaceString(v["campaign"])
		cleanInterfaceString(v["term"])
		cleanInterfaceString(v["rcode"])
		cleanInterfaceString(v["aff"])
		cleanInterfaceString(v["device"])
		cleanInterfaceString(v["os"])
		cleanInterfaceString(v["relation"])

		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[hhash]
		var hhash *string
		if w.Host != "" {
			temp := strconv.FormatInt(int64(hash(w.Host)), 36)
			hhash = &temp
		}
		//[iphash]
		var iphash string
		if w.IP != "" {
			//128 bits = ipv6
			iphash = strconv.FormatInt(int64(hash(w.IP)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
		}
		//check host account id
		//don't track without it
		//SEVERELY LIMITING SO DON'T USE IT
		var hAccountID *string
		if w.Host != "" && i.AppConfig.AccountHashMixer != "" {
			temp := strconv.FormatInt(int64(hash(w.Host+i.AppConfig.AccountHashMixer)), 36)
			hAccountID = &temp
			if v["acct"].(string) != *hAccountID {
				err := fmt.Errorf("[ERROR] Host: %s Account-ID: %s Incorrect for (acct): %s", w.Host, *hAccountID, v["acct"])
				return err
			}
		}
		//[updated]
		updated := time.Now().UTC()
		//[rid]
		var rid *uuid.UUID
		if temp, ok := v["rid"].(string); ok {
			if temp2, err := uuid.Parse(temp); err == nil {
				rid = &temp2
			}
		}
		//[auth]
		var auth *uuid.UUID
		if temp, ok := v["auth"].(string); ok {
			if temp2, err := uuid.Parse(temp); err == nil {
				auth = &temp2
			}
		}
		//[country]
		var country *string
		var region *string
		var city *string
		zip := v["zip"]
		ensureInterfaceString(zip)
		if tz, ok := v["tz"].(string); ok {
			if ct, oktz := countries[tz]; oktz {
				country = &ct
			}
		}
		//[latlon]
		var latlon *geo_point
		latf, oklatf := v["lat"].(float64)
		lonf, oklonf := v["lon"].(float64)
		if oklatf && oklonf {
			//Float
			latlon = &geo_point{}
			latlon.Lat = latf
			latlon.Lon = lonf
		} else {
			//String
			lats, oklats := v["lat"].(string)
			lons, oklons := v["lon"].(string)
			if oklats && oklons {
				latlon = &geo_point{}
				latlon.Lat, _ = strconv.ParseFloat(lats, 64)
				latlon.Lon, _ = strconv.ParseFloat(lons, 64)
			}
		}
		if latlon == nil {
			if gip, err := GetGeoIP(net.ParseIP(w.IP)); err == nil && gip != nil {
				var geoip GeoIP
				if err := json.Unmarshal(gip, &geoip); err == nil && geoip.Latitude != 0 && geoip.Longitude != 0 {
					latlon = &geo_point{}
					latlon.Lat = geoip.Latitude
					latlon.Lon = geoip.Longitude
					if geoip.CountryISO2 != "" {
						country = &geoip.CountryISO2
					}
					if geoip.Region != "" {
						region = &geoip.Region
					}
					if geoip.City != "" {
						city = &geoip.City
					}
					if zip == nil && geoip.Zip != "" {
						zip = &geoip.Zip
					}

				}
			}
		}
		if !i.AppConfig.UseRegionDescriptions {
			//country = nil
			region = nil
			city = nil
		}
		//Self identification of geo_pol overrules geoip
		if ct, ok := v["country"].(string); ok {
			country = &ct
		}
		if r, ok := v["region"].(string); ok {
			region = &r
		}
		if r, ok := v["city"].(string); ok {
			city = &r
		}
		upperString(country)
		cleanString(region)
		cleanString(city)
		//[vp]
		var vp *viewport
		width, okwf := v["w"].(float64)
		height, okhf := v["h"].(float64)
		if okwf && okhf {
			//Float
			vp = &viewport{}
			vp.H = int64(height)
			vp.W = int64(width)
		}
		//[duration]
		var duration *int64
		if d, ok := v["duration"].(string); ok {
			temp, _ := strconv.ParseInt(d, 10, 64)
			duration = &temp
		}
		if d, ok := v["duration"].(float64); ok {
			temp := int64(d)
			duration = &temp
		}
		//[ver]
		var version *int64
		if ver, ok := v["version"].(string); ok {
			temp, _ := strconv.ParseInt(ver, 10, 32)
			version = &temp
		}
		if ver, ok := v["version"].(float64); ok {
			temp := int64(ver)
			version = &temp
		}
		//[cflags] - compliance flags
		var cflags *int64
		if com, ok := v["cflags"].(int64); ok {
			cflags = &com
		} else if com, ok := v["cflags"].(float64); ok {
			temp := int64(com)
			cflags = &temp
		}
		//[bhash]
		var bhash *string
		if w.Browser != "" {
			temp := strconv.FormatInt(int64(hash(w.Browser)), 36)
			bhash = &temp
		}
		//[score]
		var score *float64
		if s, ok := v["score"].(string); ok {
			temp, _ := strconv.ParseFloat(s, 64)
			score = &temp

		} else if s, ok := v["score"].(float64); ok {
			score = &s
		}

		//Exclude the following from **all** params in events,visitors and sessions. Note: further exclusions after events insert.
		//[params]
		var params *map[string]interface{}
		if ps, ok := v["params"].(string); ok {
			json.Unmarshal([]byte(ps), &params)
		} else if ps, ok := v["params"].(map[string]interface{}); ok {
			params = &ps
		}
		if params != nil {
			//De-identify data
			delete(*params, "uri")
			delete(*params, "hhash")
			delete(*params, "iphash")
			delete(*params, "cell")
			delete(*params, "chash")
			delete(*params, "email")
			delete(*params, "ehash")
			delete(*params, "uname")
			delete(*params, "acct")
			//Remove column params/duplicates
			delete(*params, "first")
			delete(*params, "lat")
			delete(*params, "lon")
			delete(*params, "w")
			delete(*params, "h")
			delete(*params, "params")
			delete(*params, "eid")
			delete(*params, "tr")
			delete(*params, "time")
			delete(*params, "vid")
			delete(*params, "did")
			delete(*params, "sid")
			delete(*params, "app")
			delete(*params, "rel")
			delete(*params, "cflags")
			delete(*params, "created")
			delete(*params, "uid")
			delete(*params, "last")
			delete(*params, "url")
			delete(*params, "ip")
			delete(*params, "latlon")
			delete(*params, "ptyp")
			delete(*params, "bhash")
			delete(*params, "auth")
			delete(*params, "duration")
			delete(*params, "xid")
			delete(*params, "split")
			delete(*params, "etyp")
			delete(*params, "ver")
			delete(*params, "sink")
			delete(*params, "score")
			delete(*params, "params")
			delete(*params, "gaid")
			delete(*params, "idfa")
			delete(*params, "msid")
			delete(*params, "fbid")
			delete(*params, "country")
			delete(*params, "region")
			delete(*params, "city")
			delete(*params, "zip")
			delete(*params, "culture")
			delete(*params, "ref")
			delete(*params, "aff")
			delete(*params, "browser")
			delete(*params, "device")
			delete(*params, "os")
			delete(*params, "tz")
			delete(*params, "vp")
			delete(*params, "targets")
			delete(*params, "rid")
			delete(*params, "relation")
			delete(*params, "rcode")

			delete(*params, "ename")
			delete(*params, "source")
			delete(*params, "content")
			delete(*params, "medium")
			delete(*params, "campaign")
			delete(*params, "term")

			if len(*params) == 0 {
				params = nil
			}
		}

		var nparams *map[string]float64
		if params != nil {
			tparams := make(map[string]float64)
			for npk, npv := range *params {
				if d, ok := npv.(float64); ok {
					tparams[npk] = d
					(*params)[npk] = fmt.Sprintf("%f", d) //let's keep both string and numerical
					//delete(*params, npk) //we delete the old copy
					continue
				}
				if npb, ok := npv.(bool); ok {
					if npb {
						tparams[npk] = 1
					} else {
						tparams[npk] = 0
					}
					(*params)[npk] = fmt.Sprintf("%v", npb)
					continue
				}
				if nps, ok := npv.(string); !ok {
					//UNKNOWN TYPE
					(*params)[npk] = fmt.Sprintf("%+v", npv) //clean up instead
					//delete(*params, npk) //remove if not a string
				} else {
					if strings.TrimSpace(strings.ToLower(nps)) == "true" {
						tparams[npk] = 1
						continue
					}
					if strings.TrimSpace(strings.ToLower(nps)) == "false" {
						tparams[npk] = 0
						continue
					}
					if npf, err := strconv.ParseFloat(nps, 64); err == nil && len(nps) > 0 {
						tparams[npk] = npf
					}

				}
			}
			if len(tparams) > 0 {
				nparams = &tparams
			}
		}

		//[culture]
		var culture *string
		c := strings.Split(w.Language, ",")
		if len(c) > 0 {
			culture = &c[0]
			cleanString(culture)
		}

		//WARNING: w.URI has destructive changes here
		//[last],[url]
		if i.AppConfig.IsUrlFiltered {
			if last, ok := v["last"].(string); ok {
				filterUrl(i.AppConfig, &last, &i.AppConfig.UrlFilterMatchGroup)
				filterUrlPrefix(&last)
				v["last"] = last
			}
			if url, ok := v["url"].(string); ok {
				filterUrl(i.AppConfig, &url, &i.AppConfig.UrlFilterMatchGroup)
				filterUrlPrefix(&url)
				v["url"] = url
			} else {
				//check for /tr/ /pub/ /img/ (ignore)
				if !regexInternalURI.MatchString(w.URI) {
					filterUrl(i.AppConfig, &w.URI, &i.AppConfig.UrlFilterMatchGroup)
					filterUrlPrefix(&w.URI)
					v["url"] = w.URI
				} else {
					delete(v, "url")
				}
			}
		} else {
			if last, ok := v["last"].(string); ok {
				filterUrlPrefix(&last)
				filterUrlAppendix(&last)
				v["last"] = last
			}
			if url, ok := v["url"].(string); ok {
				filterUrlPrefix(&url)
				filterUrlAppendix(&url)
				v["url"] = url
			} else {
				//check for /tr/ /pub/ /img/ (ignore)
				if !regexInternalURI.MatchString(w.URI) {
					filterUrlPrefix(&w.URI)
					filterUrlAppendix(&w.URI)
					v["url"] = w.URI
				} else {
					delete(v, "url")
				}
			}
		}

		//[Cell Phone]
		var chash *string
		if temp, ok := v["chash"].(string); ok {
			chash = &temp
		} else if temp, ok := v["cell"].(string); ok {
			temp = strings.ToLower(strings.TrimSpace(temp))
			temp = sha(i.AppConfig.PrefixPrivateHash + temp)
			chash = &temp
		}
		delete(v, "cell")

		//[Email]
		var ehash *string
		if temp, ok := v["ehash"].(string); ok {
			ehash = &temp
		} else if temp, ok := v["email"].(string); ok {
			temp = strings.ToLower(strings.TrimSpace(temp))
			temp = sha(i.AppConfig.PrefixPrivateHash + temp)
			ehash = &temp
		}
		delete(v, "email")

		//[uname]
		var uhash *string
		if temp, ok := v["uhash"].(string); ok {
			uhash = &temp
		} else if temp, ok := v["uname"].(string); ok {
			temp = strings.ToLower(strings.TrimSpace(temp))
			temp = sha(i.AppConfig.PrefixPrivateHash + temp)
			uhash = &temp
		}
		delete(v, "uname")

		//EventID
		if temp, ok := v["eid"].(string); ok {
			evt, _ := gocql.ParseUUID(temp)
			if evt.Timestamp() != 0 {
				w.EventID = evt
			}
		}
		//Double check
		if w.EventID.Timestamp() == 0 {
			w.EventID = gocql.TimeUUID()
		}

		//[vid] - default
		isNew := false
		if vidstring, ok := v["vid"].(string); !ok {
			v["vid"] = gocql.TimeUUID()
			isNew = true
		} else {
			//Let's override the event id too
			tempvid, _ := gocql.ParseUUID(vidstring)
			if tempvid.Timestamp() == 0 {
				v["vid"] = gocql.TimeUUID()
				isNew = true
			}
		}
		//[uid] - let's overwrite the vid if we have a uid
		if uidstring, ok := v["uid"].(string); ok {
			tempuid, _ := gocql.ParseUUID(uidstring)
			if tempuid.Timestamp() != 0 {
				v["vid"] = v["uid"]
				isNew = false
			}
		}
		//[sid]
		if sidstring, ok := v["sid"].(string); !ok {
			if isNew {
				v["sid"] = v["vid"]
			} else {
				v["sid"] = gocql.TimeUUID()
			}
		} else {
			tempuuid, _ := gocql.ParseUUID(sidstring)
			if tempuuid.Timestamp() == 0 {
				v["sid"] = gocql.TimeUUID()
			} else {
				v["sid"] = tempuuid
			}
		}

		//////////////////////////////////////////////
		//Persist
		//////////////////////////////////////////////

		tx, err := i.Session.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		w.SaveCookie = true
		isFirst := isNew || (v["first"] != "false")

		// hits table - already implemented above

		// ips table
		_, err = tx.Exec(`INSERT INTO ips (hhash, ip, total) 
            VALUES (?, ?, 1) 
            ON CONFLICT (hhash, ip) DO UPDATE 
            SET total = total + 1`,
			hhash, w.IP)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ips]:", err)
		}

		// routed table
		_, err = tx.Exec(`INSERT INTO routed (hhash, ip, url) 
            VALUES (?, ?, ?) 
            ON CONFLICT (hhash, ip) DO UPDATE 
            SET url = EXCLUDED.url`,
			hhash, w.IP, v["url"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[routed]:", err)
		}

		// referrers table
		if _, ok := v["last"].(string); ok {
			_, err = tx.Exec(`INSERT INTO referrers (hhash, url, total) 
                VALUES (?, ?, 1) 
                ON CONFLICT (hhash, url) DO UPDATE 
                SET total = total + 1`,
				hhash, v["last"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[referrers]:", err)
			}
		}

		// referrals table
		if v["ref"] != nil {
			_, err = tx.Exec(`INSERT INTO referrals (hhash, vid, ref) 
                VALUES (?, ?, ?) 
                ON CONFLICT (hhash, vid) DO NOTHING`,
				hhash, v["vid"], v["ref"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[referrals]:", err)
			}
		}

		// referred table
		if v["rcode"] != nil {
			_, err = tx.Exec(`INSERT INTO referred (hhash, vid, rcode) 
                VALUES (?, ?, ?) 
                ON CONFLICT (hhash, vid) DO NOTHING`,
				hhash, v["vid"], v["rcode"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[referred]:", err)
			}
		}

		// hosts table
		if w.Host != "" {
			_, err = tx.Exec(`INSERT INTO hosts (hhash, hostname) 
                VALUES (?, ?) 
                ON CONFLICT (hhash) DO NOTHING`,
				hhash, w.Host)
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[hosts]:", err)
			}
		}

		// browsers table
		_, err = tx.Exec(`INSERT INTO browsers (hhash, browser, bhash, total) 
            VALUES (?, ?, ?, 1) 
            ON CONFLICT (hhash, browser, bhash) DO UPDATE 
            SET total = total + 1`,
			hhash, w.Browser, bhash)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[browsers]:", err)
		}

		// nodes table
		_, err = tx.Exec(`INSERT INTO nodes (hhash, vid, uid, ip, iphash, sid) 
            VALUES (?, ?, ?, ?, ?, ?)`,
			hhash, v["vid"], v["uid"], w.IP, iphash, v["sid"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[nodes]:", err)
		}

		// locations table
		if latlon != nil {
			_, err = tx.Exec(`INSERT INTO locations (hhash, vid, latlon, uid, sid) 
                VALUES (?, ?, ?, ?, ?)`,
				hhash, v["vid"], latlon, v["uid"], v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[locations]:", err)
			}
		}

		// aliases table
		if v["uid"] != nil {
			_, err = tx.Exec(`INSERT INTO aliases (hhash, vid, uid, sid) 
                VALUES (?, ?, ?, ?)`,
				hhash, v["vid"], v["uid"], v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[aliases]:", err)
			}
		}

		// users table
		if v["uid"] != nil {
			_, err = tx.Exec(`INSERT INTO users (hhash, vid, uid, sid) 
                VALUES (?, ?, ?, ?)`,
				hhash, v["vid"], v["uid"], v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[users]:", err)
			}
		}

		// usernames table
		if uhash != nil {
			_, err = tx.Exec(`INSERT INTO usernames (hhash, vid, uhash, sid) 
                VALUES (?, ?, ?, ?)`,
				hhash, v["vid"], uhash, v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[usernames]:", err)
			}
		}

		// emails table
		if ehash != nil {
			_, err = tx.Exec(`INSERT INTO emails (hhash, vid, ehash, sid) 
                VALUES (?, ?, ?, ?)`,
				hhash, v["vid"], ehash, v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[emails]:", err)
			}
		}

		// cells table
		if chash != nil {
			_, err = tx.Exec(`INSERT INTO cells (hhash, vid, chash, sid) 
                VALUES (?, ?, ?, ?)`,
				hhash, v["vid"], chash, v["sid"])
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[cells]:", err)
			}
		}

		// reqs table
		_, err = tx.Exec(`INSERT INTO reqs (hhash, vid, total) 
            VALUES (?, ?, 1) 
            ON CONFLICT (hhash, vid) DO UPDATE 
            SET total = total + 1`,
			hhash, v["vid"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[reqs]:", err)
		}

		//events table
		_, err = tx.Exec(`INSERT INTO events 
            (eid, vid, sid, hhash, app, rel, cflags, created, updated, uid, last,
             url, ip, iphash, latlon, ptyp, bhash, auth, duration, xid, split,
             ename, source, medium, campaign, country, region, city, zip, term,
             etyp, ver, sink, score, params, nparams)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?)`,
			v["eid"], v["vid"], v["sid"], hhash, v["app"], v["rel"], cflags,
			created, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
			iphash, latlon, v["ptyp"], bhash, auth, duration, v["xid"],
			v["split"], v["ename"], v["source"], v["medium"], v["campaign"],
			country, region, city, zip, v["term"], v["etyp"], version,
			v["sink"], score, params, nparams)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[events]:", err)
		}

		//events_recent table
		_, err = tx.Exec(`INSERT INTO events_recent
            (eid, vid, sid, hhash, app, rel, cflags, created, updated, uid, last,
             url, ip, iphash, latlon, ptyp, bhash, auth, duration, xid, split,
             ename, source, medium, campaign, country, region, city, zip, term,
             etyp, ver, sink, score, params, nparams)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?)`,
			v["eid"], v["vid"], v["sid"], hhash, v["app"], v["rel"], cflags,
			created, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
			iphash, latlon, v["ptyp"], bhash, auth, duration, v["xid"],
			v["split"], v["ename"], v["source"], v["medium"], v["campaign"],
			country, region, city, zip, v["term"], v["etyp"], version,
			v["sink"], score, params, nparams)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[events_recent]:", err)
		}

		if isNew || isFirst {
			// visitors table
			_, err = tx.Exec(`INSERT INTO visitors 
                (vid, did, sid, hhash, app, rel, cflags, created, uid, last,
                 url, ip, iphash, latlon, ptyp, bhash, auth, xid, split, ename,
                 etyp, ver, sink, score, params, nparams, gaid, idfa, msid, fbid,
                 country, region, city, zip, culture, source, medium, campaign,
                 term, ref, rcode, aff, browser, device, os, tz, vp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (vid) DO NOTHING`,
				v["vid"], v["did"], v["sid"], hhash, v["app"], v["rel"],
				cflags, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
				iphash, latlon, v["ptyp"], bhash, auth, v["xid"], v["split"],
				v["ename"], v["etyp"], version, v["sink"], score, params,
				nparams, v["gaid"], v["idfa"], v["msid"], v["fbid"], country,
				region, city, zip, culture, v["source"], v["medium"],
				v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"],
				w.Browser, v["device"], v["os"], v["tz"], vp)
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[visitors]:", err)
			}

			// sessions table
			_, err = tx.Exec(`INSERT INTO sessions 
                (vid, did, sid, hhash, app, rel, cflags, created, uid, last,
                 url, ip, iphash, latlon, ptyp, bhash, auth, duration, xid,
                 split, ename, etyp, ver, sink, score, params, nparams, gaid,
                 idfa, msid, fbid, country, region, city, zip, culture, source,
                 medium, campaign, term, ref, rcode, aff, browser, device, os,
                 tz, vp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (vid, sid) DO NOTHING`,
				v["vid"], v["did"], v["sid"], hhash, v["app"], v["rel"],
				cflags, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
				iphash, latlon, v["ptyp"], bhash, auth, duration, v["xid"],
				v["split"], v["ename"], v["etyp"], version, v["sink"], score,
				params, nparams, v["gaid"], v["idfa"], v["msid"], v["fbid"],
				country, region, city, zip, culture, v["source"], v["medium"],
				v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"],
				w.Browser, v["device"], v["os"], v["tz"], vp)
			if err != nil && i.AppConfig.Debug {
				fmt.Println("[ERROR] DuckDB[sessions]:", err)
			}
		}

		// visitors_latest table
		_, err = tx.Exec(`INSERT INTO visitors_latest 
            (vid, did, sid, hhash, app, rel, cflags, created, uid, last,
             url, ip, iphash, latlon, ptyp, bhash, auth, xid, split, ename,
             etyp, ver, sink, score, params, nparams, gaid, idfa, msid, fbid,
             country, region, city, zip, culture, source, medium, campaign,
             term, ref, rcode, aff, browser, device, os, tz, vp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (vid) DO UPDATE SET
            did = EXCLUDED.did,
            sid = EXCLUDED.sid,
            hhash = EXCLUDED.hhash,
            app = EXCLUDED.app,
            rel = EXCLUDED.rel,
            cflags = EXCLUDED.cflags,
            created = EXCLUDED.created,
            uid = EXCLUDED.uid,
            last = EXCLUDED.last,
            url = EXCLUDED.url,
            ip = EXCLUDED.ip,
            iphash = EXCLUDED.iphash,
            latlon = EXCLUDED.latlon,
            ptyp = EXCLUDED.ptyp,
            bhash = EXCLUDED.bhash,
            auth = EXCLUDED.auth,
            xid = EXCLUDED.xid,
            split = EXCLUDED.split,
            ename = EXCLUDED.ename,
            etyp = EXCLUDED.etyp,
            ver = EXCLUDED.ver,
            sink = EXCLUDED.sink,
            score = EXCLUDED.score,
            params = EXCLUDED.params,
            nparams = EXCLUDED.nparams,
            gaid = EXCLUDED.gaid,
            idfa = EXCLUDED.idfa,
            msid = EXCLUDED.msid,
            fbid = EXCLUDED.fbid,
            country = EXCLUDED.country,
            region = EXCLUDED.region,
            city = EXCLUDED.city,
            zip = EXCLUDED.zip,
            culture = EXCLUDED.culture,
            source = EXCLUDED.source,
            medium = EXCLUDED.medium,
            campaign = EXCLUDED.campaign,
            term = EXCLUDED.term,
            ref = EXCLUDED.ref,
            rcode = EXCLUDED.rcode,
            aff = EXCLUDED.aff,
            browser = EXCLUDED.browser,
            device = EXCLUDED.device,
            os = EXCLUDED.os,
            tz = EXCLUDED.tz,
            vp = EXCLUDED.vp`,
			v["vid"], v["did"], v["sid"], hhash, v["app"], v["rel"],
			cflags, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
			iphash, latlon, v["ptyp"], bhash, auth, v["xid"], v["split"],
			v["ename"], v["etyp"], version, v["sink"], score, params,
			nparams, v["gaid"], v["idfa"], v["msid"], v["fbid"], country,
			region, city, zip, culture, v["source"], v["medium"],
			v["campaign"], v["term"], v["ref"], v["rcode"], v["aff"],
			w.Browser, v["device"], v["os"], v["tz"], vp)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[visitors_latest]:", err)
		}

		return tx.Commit()

	case WRITE_LTV:
		cleanString(&(w.Host))
		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[updated]
		updated := time.Now().UTC()
		created := &updated

		//[hhash]
		var hhash *string
		if w.Host != "" {
			temp := strconv.FormatInt(int64(hash(w.Host)), 36)
			hhash = &temp
		}
		//[payment]
		var pmt *payment
		pmt = &payment{}

		//UUIDs
		if iid, ok := v["invid"].(string); ok {
			if temp, err := uuid.Parse(iid); err == nil {
				pmt.InvoiceID = &temp
			}
		}
		if pid, ok := v["pid"].(string); ok {
			if temp, err := uuid.Parse(pid); err == nil {
				pmt.ProductID = &temp
			}
		}

		//Timestamps
		if invoiced, ok := v["invoiced"].(string); ok {
			millis, err := strconv.ParseInt(invoiced, 10, 64)
			if err == nil {
				temp := time.Unix(0, millis*int64(time.Millisecond)).Truncate(time.Millisecond)
				pmt.Invoiced = &temp
			}
		} else if s, ok := v["invoiced"].(float64); ok {
			temp := time.Unix(0, int64(s)*int64(time.Millisecond)).Truncate(time.Millisecond)
			pmt.Invoiced = &temp
		}
		if starts, ok := v["starts"].(string); ok {
			millis, err := strconv.ParseInt(starts, 10, 64)
			if err == nil {
				temp := time.Unix(0, millis*int64(time.Millisecond)).Truncate(time.Millisecond)
				pmt.Starts = &temp
			}
		} else if s, ok := v["starts"].(float64); ok {
			temp := time.Unix(0, int64(s)*int64(time.Millisecond)).Truncate(time.Millisecond)
			pmt.Starts = &temp
		}
		if ends, ok := v["ends"].(string); ok {
			millis, err := strconv.ParseInt(ends, 10, 64)
			if err == nil {
				temp := time.Unix(0, millis*int64(time.Millisecond)).Truncate(time.Millisecond)
				pmt.Ends = &temp
			}
		} else if s, ok := v["ends"].(float64); ok {
			temp := time.Unix(0, int64(s)*int64(time.Millisecond)).Truncate(time.Millisecond)
			pmt.Ends = &temp
		}
		if paid, ok := v["paid"].(string); ok {
			millis, err := strconv.ParseInt(paid, 10, 64)
			if err == nil {
				temp := time.Unix(0, millis*int64(time.Millisecond)).Truncate(time.Millisecond)
				pmt.Paid = &temp
			}
		} else if s, ok := v["paid"].(float64); ok {
			temp := time.Unix(0, int64(s)*int64(time.Millisecond)).Truncate(time.Millisecond)
			pmt.Paid = &temp
		}
		if pmt.Paid == nil {
			//!Force an update on row
			pmt.Paid = &updated
		}

		//Strings
		if temp, ok := v["product"].(string); ok {
			pmt.Product = &temp
		}
		if temp, ok := v["pcat"].(string); ok {
			pmt.ProductCategory = &temp
		}
		if temp, ok := v["man"].(string); ok {
			pmt.Manufacturer = &temp
		}
		if temp, ok := v["model"].(string); ok {
			pmt.Model = &temp
		}
		if temp, ok := v["duration"].(string); ok {
			pmt.Duration = &temp
		}

		//Floats
		var paid *float64
		if s, ok := v["amt"].(string); ok {
			if temp, err := strconv.ParseFloat(s, 64); err == nil {
				paid = &temp
			}

		} else if s, ok := v["amt"].(float64); ok {
			paid = &s
		}

		if qty, ok := v["qty"].(string); ok {
			if temp, err := strconv.ParseFloat(qty, 64); err == nil {
				pmt.Quantity = &temp
			}
		} else if s, ok := v["qty"].(float64); ok {
			pmt.Quantity = &s
		}

		if price, ok := v["price"].(string); ok {
			if temp, err := strconv.ParseFloat(price, 64); err == nil {
				pmt.Price = &temp
			}
		} else if s, ok := v["price"].(float64); ok {
			pmt.Price = &s
		}

		if discount, ok := v["discount"].(string); ok {
			if temp, err := strconv.ParseFloat(discount, 64); err == nil {
				pmt.Discount = &temp
			}
		} else if s, ok := v["discount"].(float64); ok {
			pmt.Discount = &s
		}

		if revenue, ok := v["revenue"].(string); ok {
			if temp, err := strconv.ParseFloat(revenue, 64); err == nil {
				pmt.Revenue = &temp
			}
		} else if s, ok := v["revenue"].(float64); ok {
			pmt.Revenue = &s
		}

		if margin, ok := v["margin"].(string); ok {
			if temp, err := strconv.ParseFloat(margin, 64); err == nil {
				pmt.Margin = &temp
			}
		} else if s, ok := v["margin"].(float64); ok {
			pmt.Margin = &s
		}

		if cost, ok := v["cost"].(string); ok {
			if temp, err := strconv.ParseFloat(cost, 64); err == nil {
				pmt.Cost = &temp
			}
		} else if s, ok := v["cost"].(float64); ok {
			pmt.Cost = &s
		}

		if tax, ok := v["tax"].(string); ok {
			if temp, err := strconv.ParseFloat(tax, 64); err == nil {
				pmt.Tax = &temp
			}
		} else if s, ok := v["tax"].(float64); ok {
			pmt.Tax = &s
		}

		if taxrate, ok := v["tax_rate"].(string); ok {
			if temp, err := strconv.ParseFloat(taxrate, 64); err == nil {
				pmt.TaxRate = &temp
			}
		} else if s, ok := v["tax_rate"].(float64); ok {
			pmt.TaxRate = &s
		}

		if commission, ok := v["commission"].(string); ok {
			if temp, err := strconv.ParseFloat(commission, 64); err == nil {
				pmt.Commission = &temp
			}
		} else if s, ok := v["commission"].(float64); ok {
			pmt.Commission = &s
		}

		if referral, ok := v["referral"].(string); ok {
			if temp, err := strconv.ParseFloat(referral, 64); err == nil {
				pmt.Referral = &temp
			}
		} else if s, ok := v["referral"].(float64); ok {
			pmt.Referral = &s
		}

		if fees, ok := v["fees"].(string); ok {
			if temp, err := strconv.ParseFloat(fees, 64); err == nil {
				pmt.Fees = &temp
			}
		} else if s, ok := v["fees"].(float64); ok {
			pmt.Fees = &s
		}

		if subtotal, ok := v["subtotal"].(string); ok {
			if temp, err := strconv.ParseFloat(subtotal, 64); err == nil {
				pmt.Subtotal = &temp
			}
		} else if s, ok := v["subtotal"].(float64); ok {
			pmt.Subtotal = &s
		}

		if total, ok := v["total"].(string); ok {
			if temp, err := strconv.ParseFloat(total, 64); err == nil {
				pmt.Total = &temp
			}
		} else if s, ok := v["total"].(float64); ok {
			pmt.Total = &s
		}

		if payment, ok := v["payment"].(string); ok {
			if temp, err := strconv.ParseFloat(payment, 64); err == nil {
				pmt.Payment = &temp
			}
		} else if s, ok := v["payment"].(float64); ok {
			pmt.Payment = &s
		}
		//!Force an amount in the payment
		if pmt.Payment == nil {
			pmt.Payment = paid
		}

		tx, err := i.Session.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var pmts []payment
		var prevpaid *float64

		// [LTV]
		err = tx.QueryRow(`
			SELECT payments, created, paid 
			FROM ltv 
			WHERE hhash = ? AND uid = ?`,
			hhash, v["uid"]).Scan(&pmts, &created, &prevpaid)

		if err != nil && err != sql.ErrNoRows && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltv]:", err)
		}

		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		_, err = tx.Exec(`
			INSERT INTO ltv (
				vid, sid, payments, paid, org, updated,
				updater, created, owner, hhash, uid
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (hhash, uid) DO UPDATE SET
				vid = EXCLUDED.vid,
				sid = EXCLUDED.sid, 
				payments = EXCLUDED.payments,
				paid = EXCLUDED.paid,
				org = EXCLUDED.org,
				updated = EXCLUDED.updated,
				updater = EXCLUDED.updater,
				created = EXCLUDED.created,
				owner = EXCLUDED.owner`,
			v["vid"], v["sid"], pmts, prevpaid, v["org"], updated,
			v["uid"], created, v["uid"], hhash, v["uid"])

		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltv]:", err)
		}

		// [LTVU]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil

		err = tx.QueryRow(`
			SELECT payments, created, paid 
			FROM ltvu 
			WHERE hhash = ? AND uid = ? AND orid = ?`,
			hhash, v["uid"], v["orid"]).Scan(&pmts, &created, &prevpaid)

		if err != nil && err != sql.ErrNoRows && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltvu]:", err)
		}

		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		_, err = tx.Exec(`
			INSERT INTO ltvu (
				vid, sid, payments, paid, org, updated,
				updater, created, owner, hhash, uid, orid
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (hhash, uid, orid) DO UPDATE SET
				vid = EXCLUDED.vid,
				sid = EXCLUDED.sid,
				payments = EXCLUDED.payments,
				paid = EXCLUDED.paid,
				org = EXCLUDED.org,
				updated = EXCLUDED.updated,
				updater = EXCLUDED.updater,
				created = EXCLUDED.created,
				owner = EXCLUDED.owner`,
			v["vid"], v["sid"], pmts, prevpaid, v["org"], updated,
			v["uid"], created, v["uid"], hhash, v["uid"], v["orid"])

		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltvu]:", err)
		}

		// [LTVV]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil

		err = tx.QueryRow(`
			SELECT payments, created, paid 
			FROM ltvv 
			WHERE hhash = ? AND vid = ? AND orid = ?`,
			hhash, v["vid"], v["orid"]).Scan(&pmts, &created, &prevpaid)

		if err != nil && err != sql.ErrNoRows && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltvv]:", err)
		}

		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		_, err = tx.Exec(`
			INSERT INTO ltvv (
				uid, sid, payments, paid, org, updated,
				updater, created, owner, hhash, vid, orid
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (hhash, vid, orid) DO UPDATE SET
				uid = EXCLUDED.uid,
				sid = EXCLUDED.sid,
				payments = EXCLUDED.payments,
				paid = EXCLUDED.paid,
				org = EXCLUDED.org,
				updated = EXCLUDED.updated,
				updater = EXCLUDED.updater,
				created = EXCLUDED.created,
				owner = EXCLUDED.owner`,
			v["uid"], v["sid"], pmts, prevpaid, v["org"], updated,
			v["uid"], created, v["uid"], hhash, v["vid"], v["orid"])

		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[ltvv]:", err)
		}

		return tx.Commit()

	default:
		//TODO: Manually run query via query in config.json
		if i.AppConfig.Debug {
			fmt.Printf("UNHANDLED %s\n", w)
		}
	}

	//TODO: Retries
	return err
}
