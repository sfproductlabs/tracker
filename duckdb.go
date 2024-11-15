package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"strconv"

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
	}

	for _, query := range queries {
		if _, err := i.Session.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

// Write handles writing data to DuckDB
func (i *DuckService) write(w *WriteArgs) error {
	err := fmt.Errorf("Could not write to DuckDB")
	v := *w.Values

	switch w.WriteType {
	case WRITE_COUNT:
		if i.AppConfig.Debug {
			fmt.Printf("COUNT %s\n", w)
		}
		return i.Session.QueryRow(`UPDATE counters SET total=total+1 WHERE id=?`,
			v["id"]).Err()

	case WRITE_EVENT:
		// Get current timestamp
		updated := time.Now().UTC()

		// Handle host hash
		var hhash *string
		if w.Host != "" {
			temp := strconv.FormatInt(int64(hash(w.Host)), 36)
			hhash = &temp
		}

		// Handle IP hash
		var iphash string
		if w.IP != "" {
			iphash = strconv.FormatInt(int64(hash(w.IP)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
			iphash = iphash + strconv.FormatInt(int64(hash(w.IP+iphash)), 36)
		}

		// Handle browser hash
		var bhash *string
		if w.Browser != "" {
			temp := strconv.FormatInt(int64(hash(w.Browser)), 36)
			bhash = &temp
		}

		// Handle VID/UID logic
		isNew := false
		if vidVal, ok := v["vid"].(string); !ok {
			v["vid"] = uuid.New()
			isNew = true
		} else {
			if vidVal == "" {
				v["vid"] = uuid.New()
				isNew = true
			}
		}

		if uidVal, ok := v["uid"].(string); ok && uidVal != "" {
			v["vid"] = v["uid"]
			isNew = false
		}

		// Handle SID
		if sidVal, ok := v["sid"].(string); !ok {
			if isNew {
				v["sid"] = v["vid"]
			} else {
				v["sid"] = uuid.New()
			}
		} else if sidVal == "" {
			v["sid"] = uuid.New()
		}

		// Insert into events table
		_, err = i.Session.Exec(`INSERT INTO events (
			id, vid, sid, hhash, app, rel, cflags, created, uid, url,
			ip, iphash, browser, bhash, params
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			w.EventID, v["vid"], v["sid"], hhash, v["app"], v["rel"],
			v["cflags"], updated, v["uid"], v["url"],
			w.IP, iphash, w.Browser, bhash, v["params"])

		if err != nil {
			return err
		}

		// Handle additional tracking tables if not a server event
		if !w.IsServer {
			// Update daily stats
			_, err = i.Session.Exec(`UPDATE dailies SET total=total+1 WHERE ip=? AND day=?`,
				w.IP, updated.Format("2006-01-02"))

			if err != nil {
				return err
			}

			// Track new visitors
			if isNew {
				_, err = i.Session.Exec(`UPDATE counters SET total=total+1 WHERE id='vids_created'`)
				if err != nil {
					return err
				}
			}

			// Insert visitor record
			if isNew {
				_, err = i.Session.Exec(`INSERT INTO visitors (
					vid, sid, hhash, app, rel, created, uid, url,
					ip, iphash, browser, bhash
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					v["vid"], v["sid"], hhash, v["app"], v["rel"], updated, v["uid"], v["url"],
					w.IP, iphash, w.Browser, bhash)

				if err != nil {
					return err
				}
			}
		}

		return nil

	default:
		return fmt.Errorf("Unknown write type")
	}
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

		// Export if table is too large or inactive
		if sizeBytes > maxSizeBytes || time.Since(lastModified) > inactivityLimit {
			if err := i.exportAndTruncateTable(tableName); err != nil {
				return fmt.Errorf("failed to process table %s: %v", tableName, err)
			}

			if i.AppConfig.Debug {
				fmt.Printf("[HealthCheck] Processed table %s (size: %.2f MB, last modified: %v)\n",
					tableName,
					float64(sizeBytes)/(1024*1024),
					lastModified)
			}
		}
	}

	return nil
}

// Helper function to handle table export and truncation
func (i *DuckService) exportAndTruncateTable(tableName string) error {
	// Generate export path
	timestamp := time.Now().UTC().Format("2006-01-02-15-04-05")
	s3Path := fmt.Sprintf("s3://%s/%s/%s_%s.parquet",
		i.AppConfig.S3Bucket,
		i.AppConfig.S3Prefix,
		tableName,
		timestamp)

	// Begin transaction
	tx, err := i.Session.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback() // Will be ignored if transaction is committed

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

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
