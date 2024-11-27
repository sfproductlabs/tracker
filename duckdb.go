//TO ACCESS:
//SELECT json_extract_scalar(params::json, '$.rock') FROM read_parquet('s3://bucket/v1/tracker/events/*/*/*/*.parquet', hive_partitioning=true) where year=2025;
//SELECT params::json->'$.rock' FROM read_parquet('s3://bucket/v1/tracker/events/*/*/*/*.parquet', hive_partitioning=true) where year=2024

package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"context"
	"encoding/json"
	"math/rand"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	awsSession "github.com/aws/aws-sdk-go/aws/session" // Add alias here
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb" // Import DuckDB driver
)

// Connect initiates the primary connection to DuckDB
func (i *DuckService) connect() error {
	err := fmt.Errorf("Could not connect to DuckDB")

	i.S3Client = s3.New(awsSession.Must(awsSession.NewSession(&aws.Config{
		Region:      &i.AppConfig.S3Region,
		Credentials: credentials.NewStaticCredentials(i.AppConfig.S3AccessKeyID, i.AppConfig.S3SecretAccessKey, ""),
	})))

	//Delete old version
	testKey := "x/y/z/test/this/key/for/testing/delete/object"
	_, err = i.S3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &i.AppConfig.S3Bucket,
		Key:    &testKey,
	})
	if err != nil {
		log.Fatal("[ERROR] Could not connect to S3:", err)
	}

	// Check if connection already exists
	if i.Session != nil {
		return fmt.Errorf("database connection already exists")
	}

	// Open DuckDB connection with configuration
	i.Session, err = sql.Open("duckdb", "")
	if err != nil {
		fmt.Println("[ERROR] Connecting to DuckDB:", err)
		return err
	} else {
		_, err = i.Session.Exec("SET threads=4")
		if err != nil {
			fmt.Println("[ERROR] Setting threads:", err)
		}
		_, err = i.Session.Exec("SET memory_limit='4GB'")
		if err != nil {
			fmt.Println("[ERROR] Setting memory_limit:", err)
		}
		_, err = i.Session.Exec("SET timezone='UTC'")
		if err != nil {
			fmt.Println("[ERROR] Setting timezone:", err)
		}
		_, err = i.Session.Exec(`INSTALL httpfs; LOAD httpfs;`)
		if err != nil {
			fmt.Println("[ERROR] Installing httpfs:", err)
		}
		_, err = i.Session.Exec(`INSTALL json; LOAD json;`)
		if err != nil {
			fmt.Println("[ERROR] Installing json:", err)
		}
		_, err = i.Session.Exec(fmt.Sprintf(`CREATE SECRET IF NOT EXISTS secret_tracker (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s'
		)`, i.AppConfig.S3AccessKeyID, i.AppConfig.S3SecretAccessKey, i.AppConfig.S3Region))
		if err != nil {
			fmt.Println("[ERROR] Creating secret_tracker:", err)
		}
	}

	// Configure connection pool settings
	i.Session.SetMaxOpenConns(30) //(i.Configuration.Connections)
	i.Session.SetMaxIdleConns(5)
	//i.Session.SetConnMaxLifetime(time.Second * time.Duration(i.Configuration.Timeout/1000))
	//i.Session.SetConnMaxIdleTime(time.Millisecond * 500)

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
	if i.Configuration.ProxyRealtimeStorageService != nil && i.Configuration.ProxyRealtimeStorageService.Session != nil {
		return i.Configuration.ProxyRealtimeStorageService.Session.auth(s)
	} else {
		return fmt.Errorf("[ERROR] DuckDB proxy storage service not implemented or connection not established")
	}
}

// Serve handles HTTP requests
func (i *DuckService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	if i.Configuration.ProxyRealtimeStorageService != nil && i.Configuration.ProxyRealtimeStorageService.Session != nil {
		return i.Configuration.ProxyRealtimeStorageService.Session.serve(w, r, s)
	} else {
		return fmt.Errorf("[ERROR] DuckDB proxy storage service not implemented or connection not established")
	}
}

// Helper function to create required tables
func (i *DuckService) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS table_versions (
			table_name VARCHAR PRIMARY KEY,
			version INTEGER,
			modified TIMESTAMP,
			estimated_size INTEGER
		)`,
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
			lat DOUBLE,
			lon DOUBLE,
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
			nparams JSON,
			payment JSON,
			targets JSON,
			relation VARCHAR,
			rid UUID
		)`,
		// `CREATE TABLE IF NOT EXISTS events_recent (
		// 	eid UUID PRIMARY KEY,
		// 	vid UUID,
		// 	sid UUID,
		// 	hhash VARCHAR,
		// 	app VARCHAR,
		// 	rel VARCHAR,
		// 	cflags INTEGER,
		// 	created TIMESTAMP,
		// 	updated TIMESTAMP,
		// 	uid UUID,
		// 	last VARCHAR,
		// 	url VARCHAR,
		// 	ip VARCHAR,
		// 	iphash VARCHAR,
		// 	lat DOUBLE,
		// 	lon DOUBLE,
		// 	ptyp VARCHAR,
		// 	bhash VARCHAR,
		// 	auth UUID,
		// 	duration INTEGER,
		// 	xid VARCHAR,
		// 	split VARCHAR,
		// 	ename VARCHAR,
		// 	source VARCHAR,
		// 	medium VARCHAR,
		// 	campaign VARCHAR,
		// 	country VARCHAR,
		// 	region VARCHAR,
		// 	city VARCHAR,
		// 	zip VARCHAR,
		// 	term VARCHAR,
		// 	etyp VARCHAR,
		// 	ver INTEGER,
		// 	sink VARCHAR,
		// 	score DOUBLE,
		// 	params JSON,
		// 	nparams JSON,
		// 	payment JSON,
		// 	targets JSON,
		// 	relation VARCHAR,
		// 	rid UUID
		// )`,
		// `CREATE TABLE IF NOT EXISTS nodes (
		// 	hhash VARCHAR,
		// 	vid UUID,
		// 	uid UUID,
		// 	iphash VARCHAR,
		// 	ip VARCHAR,
		// 	sid UUID,
		// 	PRIMARY KEY (hhash, vid, iphash)
		// )`,
		// `CREATE TABLE IF NOT EXISTS locations (
		// 	hhash VARCHAR,
		// 	vid UUID,
		// 	lat DOUBLE,
		// 	lon DOUBLE,
		// 	uid UUID,
		// 	sid UUID,
		// 	PRIMARY KEY (hhash, vid, lat, lon)
		// )`,
		// `CREATE TABLE IF NOT EXISTS aliases (
		// 	hhash VARCHAR,
		// 	vid UUID,
		// 	uid UUID,
		// 	sid UUID,
		// 	PRIMARY KEY (hhash, vid, uid)
		// )`,
		// `CREATE TABLE IF NOT EXISTS hits (
		// 	hhash VARCHAR,
		// 	url VARCHAR,
		// 	total INTEGER DEFAULT 0,
		// 	PRIMARY KEY (hhash, url)
		// )`,
		// `CREATE TABLE IF NOT EXISTS counters (
		// 	id VARCHAR PRIMARY KEY,
		// 	total INTEGER DEFAULT 0
		// )`,
		`CREATE TABLE IF NOT EXISTS logs (
			id UUID PRIMARY KEY,
			ldate DATE,
			created TIMESTAMP,
			ltime TIME,
			topic VARCHAR,
			name VARCHAR,
			host VARCHAR,
			hostname VARCHAR,
			owner UUID,
			ip VARCHAR,
			iphash VARCHAR,
			level INTEGER,
			msg VARCHAR,
			params JSON
		)`,
		`CREATE TABLE IF NOT EXISTS updates (
			id VARCHAR PRIMARY KEY,
			updated TIMESTAMP,
			msg VARCHAR
		)`,
		// `CREATE TABLE IF NOT EXISTS zips (
		// 	country VARCHAR,
		// 	zip VARCHAR,
		// 	region VARCHAR,
		// 	rcode VARCHAR,
		// 	county VARCHAR,
		// 	city VARCHAR,
		// 	culture VARCHAR,
		// 	population INTEGER,
		// 	men INTEGER,
		// 	women INTEGER,
		// 	hispanic DOUBLE,
		// 	white DOUBLE,
		// 	black DOUBLE,
		// 	native DOUBLE,
		// 	asian DOUBLE,
		// 	pacific DOUBLE,
		// 	voters INTEGER,
		// 	income DOUBLE,
		// 	incomeerr DOUBLE,
		// 	incomepercap DOUBLE,
		// 	incomepercaperr DOUBLE,
		// 	poverty DOUBLE,
		// 	childpoverty DOUBLE,
		// 	professional DOUBLE,
		// 	service DOUBLE,
		// 	office DOUBLE,
		// 	construction DOUBLE,
		// 	production DOUBLE,
		// 	drive DOUBLE,
		// 	carpool DOUBLE,
		// 	transit DOUBLE,
		// 	walk DOUBLE,
		// 	othertransport DOUBLE,
		// 	workathome DOUBLE,
		// 	meancommute DOUBLE,
		// 	employed INTEGER,
		// 	privatework DOUBLE,
		// 	publicwork DOUBLE,
		// 	selfemployed DOUBLE,
		// 	familywork DOUBLE,
		// 	PRIMARY KEY (country, zip)
		// )`,
		// `CREATE TABLE IF NOT EXISTS accounts (
		// 	uid UUID PRIMARY KEY,
		// 	pwd VARCHAR NOT NULL
		// )`,
		// `CREATE TABLE IF NOT EXISTS queues (
		// 	id UUID PRIMARY KEY,
		// 	src VARCHAR,
		// 	sid UUID,
		// 	skey VARCHAR,
		// 	ip VARCHAR,
		// 	host VARCHAR,
		// 	schedule TIMESTAMP,
		// 	started TIMESTAMP,
		// 	completed TIMESTAMP,
		// 	updated TIMESTAMP,
		// 	updater UUID,
		// 	created TIMESTAMP,
		// 	owner UUID
		// )`,
		// `CREATE TABLE IF NOT EXISTS action_names (
		// 	name VARCHAR PRIMARY KEY
		// )`,
		// `CREATE TABLE IF NOT EXISTS actions (
		// 	sid UUID,
		// 	src VARCHAR,
		// 	did UUID,
		// 	dsrc VARCHAR,
		// 	meta JSON,
		// 	exqid UUID,
		// 	created TIMESTAMP,
		// 	started TIMESTAMP,
		// 	completed TIMESTAMP,
		// 	PRIMARY KEY (sid, did)
		// )`,
		// `CREATE TABLE IF NOT EXISTS actions_ext (
		// 	sid VARCHAR,
		// 	svc VARCHAR,
		// 	iid UUID,
		// 	uid UUID,
		// 	created TIMESTAMP,
		// 	updated TIMESTAMP,
		// 	meta JSON,
		// 	PRIMARY KEY (sid, svc)
		// )`,
		// `CREATE TABLE IF NOT EXISTS cohorts (
		// 	name VARCHAR PRIMARY KEY,
		// 	uids_url VARCHAR,
		// 	imported INTEGER,
		// 	started TIMESTAMP,
		// 	completed TIMESTAMP,
		// 	created TIMESTAMP,
		// 	owner UUID
		// )`,
		// `CREATE TABLE IF NOT EXISTS messages (
		// 	id UUID PRIMARY KEY,
		// 	subject VARCHAR,
		// 	template VARCHAR,
		// 	app VARCHAR,
		// 	rel VARCHAR,
		// 	ver INTEGER,
		// 	schedule TIMESTAMP,
		// 	started TIMESTAMP,
		// 	completed TIMESTAMP,
		// 	ptyp VARCHAR,
		// 	auth VARCHAR,
		// 	xid VARCHAR,
		// 	cohorts JSON,
		// 	ehashes JSON,
		// 	chashes JSON,
		// 	split DOUBLE,
		// 	splitn VARCHAR,
		// 	source VARCHAR,
		// 	medium VARCHAR,
		// 	campaign VARCHAR,
		// 	term VARCHAR,
		// 	sink VARCHAR,
		// 	score DOUBLE,
		// 	promo VARCHAR,
		// 	ref UUID,
		// 	aff VARCHAR,
		// 	repl JSON,
		// 	created TIMESTAMP,
		// 	owner UUID,
		// 	updated TIMESTAMP,
		// 	updater UUID
		// )`,
	}

	for _, query := range queries {
		if _, err := i.Session.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (i *DuckService) prune() error {
	return fmt.Errorf("[ERROR] Not implemented pruning in duck")

	// if !i.AppConfig.PruneLogsOnly {
	// 	// Prune old records from main tables
	// 	tables := []string{"visitors", "sessions", "events", "events_recent"}

	// 	// Default TTL of 30 days if not specified
	// 	ttl := 2592000
	// 	if i.AppConfig.PruneLogsTTL > 0 {
	// 		ttl = i.AppConfig.PruneLogsTTL
	// 	}
	// 	pruneTime := time.Now().Add(-time.Duration(ttl) * time.Second)

	// 	for _, table := range tables {
	// 		var total, pruned int64

	// 		// First count total records
	// 		err := i.Session.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total)
	// 		if err != nil {
	// 			fmt.Printf("Error counting records in %s: %v\n", table, err)
	// 			continue
	// 		}

	// 		if i.AppConfig.PruneUpdateConfig {
	// 			// Update approach - set fields to null
	// 			result, err := i.Session.Exec(fmt.Sprintf(`
	// 				UPDATE %s
	// 				SET updated = ?,
	// 					params = NULL
	// 				WHERE created < ?`, table),
	// 				time.Now().UTC(), pruneTime)

	// 			if err != nil {
	// 				fmt.Printf("Error updating records in %s: %v\n", table, err)
	// 				continue
	// 			}

	// 			pruned, _ = result.RowsAffected()

	// 		} else {
	// 			// Delete approach
	// 			result, err := i.Session.Exec(fmt.Sprintf("DELETE FROM %s WHERE created < ?", table), pruneTime)
	// 			if err != nil {
	// 				fmt.Printf("Error deleting from %s: %v\n", table, err)
	// 				continue
	// 			}

	// 			pruned, _ = result.RowsAffected()
	// 		}

	// 		if i.AppConfig.Debug {
	// 			fmt.Printf("Pruned [DuckDB].[%s]: %d/%d rows\n", table, pruned, total)
	// 		}
	// 	}
	// }

	// // Prune logs table if enabled
	// if !i.AppConfig.PruneLogsSkip {
	// 	var total, pruned int64

	// 	// Get total count
	// 	err := i.Session.QueryRow("SELECT COUNT(*) FROM logs").Scan(&total)
	// 	if err != nil {
	// 		fmt.Printf("Error counting logs: %v\n", err)
	// 		return err
	// 	}

	// 	// Use logs TTL from config
	// 	ttl := 2592000 // Default 30 days
	// 	if i.AppConfig.PruneLogsTTL > 0 {
	// 		ttl = i.AppConfig.PruneLogsTTL
	// 	}
	// 	cutoffTime := time.Now().Add(-time.Duration(ttl) * time.Second)

	// 	// Delete old logs
	// 	result, err := i.Session.Exec("DELETE FROM logs WHERE created < ?", cutoffTime)
	// 	if err != nil {
	// 		fmt.Printf("Error pruning logs: %v\n", err)
	// 		return err
	// 	}

	// 	pruned, _ = result.RowsAffected()

	// 	if i.AppConfig.Debug {
	// 		fmt.Printf("Pruned [DuckDB].[logs]: %d/%d rows\n", pruned, total)
	// 	}
	// }

	// // Update config file if needed
	// if i.AppConfig.PruneUpdateConfig {
	// 	s, err := ioutil.ReadFile(i.AppConfig.ConfigFile)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	var j interface{}
	// 	if err := json.Unmarshal(s, &j); err != nil {
	// 		return err
	// 	}

	// 	SetValueInJSON(j, "PruneSkipToTimestamp", time.Now().Unix())

	// 	s, _ = json.Marshal(j)
	// 	var prettyJSON bytes.Buffer
	// 	if err := json.Indent(&prettyJSON, s, "", "    "); err != nil {
	// 		return err
	// 	}

	// 	if err := ioutil.WriteFile(i.AppConfig.ConfigFile, prettyJSON.Bytes(), 0644); err != nil {
	// 		return err
	// 	}
	// }

	// return nil
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i.AppConfig.WriteTimeoutSeconds)*time.Second)
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

	inactivityLimit := time.Duration(i.AppConfig.InactivityTimeoutSeconds) * time.Second

	for tables.Next() {

		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}

		if tableName == "table_versions" {
			continue
		}

		// Check table size
		var sizeBytes int64
		row := i.Session.QueryRow(`select estimated_size from duckdb_tables() where internal=false and table_name=?`, tableName)
		err = row.Scan(&sizeBytes)
		if err != nil {
			return fmt.Errorf("failed to get size for table %s: %v", tableName, err)
		}
		if sizeBytes == 0 {
			continue
		}
		// Check last modification time
		lastModified := time.Now().UTC()
		lastSize := int64(-1)
		isNew := false
		err = i.Session.QueryRow(`
			SELECT COALESCE(modified, ?), COALESCE(estimated_size, 0) 
			FROM table_versions where table_name=?`, lastModified, tableName).Scan(&lastModified, &lastSize)

		if err != nil {
			isNew = true
		}

		if lastSize == sizeBytes {
			continue
		}
		// Export based on condition
		if sizeBytes > i.AppConfig.MaxShardSizeBytes {
			if err := i.exportAndTruncateTable(tableName, true, sizeBytes, lastModified); err != nil {
				return fmt.Errorf("failed to process table %s: %v", tableName, err)
			}
		} else if time.Since(lastModified) > inactivityLimit || isNew {
			if err := i.exportAndTruncateTable(tableName, false, sizeBytes, lastModified); err != nil {
				return fmt.Errorf("failed to process table %s: %v", tableName, err)
			}
		}

		if i.AppConfig.Debug {
			fmt.Printf("[HealthCheck] Processed table %s (size: %.2f, last modified: %v)\n",
				tableName,
				float64(sizeBytes)/(1024*1024),
				lastModified)
		}
	}

	return nil
}

// Modified to handle different version behavior
func (i *DuckService) exportAndTruncateTable(tableName string, incrementVersion bool, sizeBytes int64, lastModified time.Time) error {
	tx, err := i.Session.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	// Get current version or create new entry
	var version int
	currentTime := time.Now().UTC()
	// Force version increment if we've ticked over to a new day
	if incrementVersion {
		// Increment version for size-based exports
		err = tx.QueryRow(`
			INSERT INTO table_versions (table_name, version, modified, estimated_size) 
			VALUES (?, 1, CURRENT_TIMESTAMP, ?)
			ON CONFLICT (table_name) DO UPDATE 
			SET version = table_versions.version + 1,
			    modified = ?,
				estimated_size = ?
			RETURNING version`, tableName, sizeBytes, currentTime, sizeBytes).Scan(&version)
	} else {
		// Use existing version for inactivity-based exports
		err = tx.QueryRow(`
			INSERT INTO table_versions (table_name, version, modified, estimated_size) 
			VALUES (?, 1, CURRENT_TIMESTAMP, ?)
			ON CONFLICT (table_name) DO UPDATE 
			SET version = table_versions.version,
			    modified = ?,
				estimated_size = ?
			RETURNING version`, tableName, sizeBytes, currentTime, sizeBytes).Scan(&version)
	}
	if err != nil {
		return fmt.Errorf("failed to handle version: %v", err)
	}

	existingVersion := version
	if incrementVersion {
		existingVersion = version - 1
	}

	// Generate export path with the existing version
	s3Path := fmt.Sprintf("s3://%s/%s/%s/year=%d/month=%d/day=%d/%s_v%d.parquet",
		i.AppConfig.S3Bucket,
		i.AppConfig.S3Prefix,
		tableName,
		currentTime.Year(),
		currentTime.Month(),
		currentTime.Day(),
		i.AppConfig.NodeId,
		existingVersion)

	// Export to S3
	_, err = tx.Exec(fmt.Sprintf(`
		COPY (SELECT * FROM %s) 
		TO '%s' (FORMAT 'parquet')
	`, tableName, s3Path))
	if err != nil {
		return fmt.Errorf("failed to export to S3: %v", err)
	}

	//Remove the old version if we've ticked over to a new day
	if lastModified.Day() != currentTime.Day() {
		oldKey := fmt.Sprintf("%s/%s/year=%d/month=%d/day=%d/%s_v%d.parquet",
			i.AppConfig.S3Bucket,
			i.AppConfig.S3Prefix,
			tableName,
			lastModified.Year(),
			lastModified.Month(),
			lastModified.Day(),
			i.AppConfig.NodeId,
			existingVersion)

		//Delete old version
		_, err = i.S3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: &i.AppConfig.S3Bucket,
			Key:    &oldKey,
		})
	}

	if incrementVersion {
		// Truncate table after successful export
		_, err = tx.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
		if err != nil {
			return fmt.Errorf("failed to truncate table: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// ////////////////////////////////////// DuckDB
func (i *DuckService) write(w *WriteArgs) error {
	//First make a deep copy of the values using JSON marshal/unmarshal
	v := make(map[string]interface{})
	b, _ := json.Marshal(*w.Values)
	json.Unmarshal(b, &v)
	//Write to proxy if configured
	if i.Configuration.ProxyRealtimeStorageService != nil && i.Configuration.ProxyRealtimeStorageServiceTables != 0 && i.Configuration.ProxyRealtimeStorageService.Session != nil {
		w.CallingService = i.Configuration
		i.Configuration.ProxyRealtimeStorageService.Session.write(w)
	}
	err := fmt.Errorf("[ERROR] Could not write to duck")
	switch w.WriteType {
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
		if latlon == nil {
			latlon = &geo_point{}
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
			//First make a deep copy of the params.params into params and remove it
			if _, exists := (*params)["params"]; exists {
				if nestedParams, ok := (*params)["params"].(map[string]interface{}); ok {
					// Merge nested params into main params
					for k, v := range nestedParams {
						(*params)[k] = v
					}
				}
			}
			delete(*params, "params")
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
		// var chash *string
		// if temp, ok := v["chash"].(string); ok {
		// 	chash = &temp
		// } else if temp, ok := v["cell"].(string); ok {
		// 	temp = strings.ToLower(strings.TrimSpace(temp))
		// 	temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		// 	chash = &temp
		// }
		// delete(v, "cell")

		//[Email]
		// var ehash *string
		// if temp, ok := v["ehash"].(string); ok {
		// 	ehash = &temp
		// } else if temp, ok := v["email"].(string); ok {
		// 	temp = strings.ToLower(strings.TrimSpace(temp))
		// 	temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		// 	ehash = &temp
		// }
		// delete(v, "email")

		//[uname]
		// var uhash *string
		// if temp, ok := v["uhash"].(string); ok {
		// 	uhash = &temp
		// } else if temp, ok := v["uname"].(string); ok {
		// 	temp = strings.ToLower(strings.TrimSpace(temp))
		// 	temp = sha(i.AppConfig.PrefixPrivateHash + temp)
		// 	uhash = &temp
		// }
		// delete(v, "uname")

		//EventID
		if temp, ok := v["eid"].(string); ok {
			evt, _ := uuid.Parse(temp)
			// Check if valid UUID and specifically a v1 UUID
			if evt != uuid.Nil && evt.Version() == uuid.Version(1) {
				w.EventID = evt
			}
		}
		//If not set or not a valid v1 UUID, generate new v1 UUID
		if w.EventID == uuid.Nil || w.EventID.Version() != uuid.Version(1) {
			w.EventID = uuid.Must(uuid.NewUUID())
		}

		//[vid] - default
		isNew := false
		if vidstring, ok := v["vid"].(string); !ok {
			v["vid"] = uuid.Must(uuid.NewUUID())
			isNew = true
		} else {
			//Let's override the event id too
			tempvid, _ := gocql.ParseUUID(vidstring)
			if tempvid.Timestamp() == 0 {
				v["vid"] = uuid.Must(uuid.NewUUID())
				isNew = true
			}
		}
		//[uid] - let's overwrite the vid if we have a uid
		if uidstring, ok := v["uid"].(string); ok {
			tempuid, _ := uuid.Parse(uidstring)
			if tempuid != uuid.Nil && tempuid.Version() == uuid.Version(1) {
				v["vid"] = v["uid"]
				isNew = false
			}
		}
		//[sid]
		if sidstring, ok := v["sid"].(string); !ok {
			if isNew {
				v["sid"] = v["vid"]
			} else {
				v["sid"] = uuid.Must(uuid.NewUUID())
			}
		} else {
			tempuuid, _ := uuid.Parse(sidstring)
			if tempuuid != uuid.Nil && tempuuid.Version() == uuid.Version(1) {
				v["sid"] = tempuuid
			} else {
				v["sid"] = uuid.Must(uuid.NewUUID())
			}
		}

		if params != nil {

			for npk, npv := range *params {
				if d, ok := npv.(float64); ok {
					(*params)[npk] = d
					continue
				}
				if npb, ok := npv.(bool); ok {
					if npb {
						(*params)[npk] = true
					} else {
						(*params)[npk] = false
					}
					continue
				}
				if nps, ok := npv.(string); !ok {
					//UNKNOWN TYPE
					(*params)[npk] = fmt.Sprintf("%+v", npv) //clean up instead
					//delete(*params, npk) //remove if not a string
				} else {
					if strings.TrimSpace(strings.ToLower(nps)) == "true" {
						(*params)[npk] = true
						continue
					}
					if strings.TrimSpace(strings.ToLower(nps)) == "false" {
						(*params)[npk] = false
						continue
					}
					if npf, err := strconv.ParseFloat(nps, 64); err == nil && len(nps) > 0 {
						(*params)[npk] = npf
					}

				}
			}
		}

		var jsparams []byte
		if params != nil {
			jsparams, err = json.Marshal(*params)
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

		//events table
		_, err = tx.Exec(`INSERT INTO events 
            (eid, vid, sid, hhash, app, rel, cflags, created, updated, uid, last,
             url, ip, iphash, lat,lon, ptyp, bhash, auth, duration, xid, split,
             ename, source, medium, campaign, country, region, city, zip, term,
             etyp, ver, sink, score, params, payment, targets, relation, rid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			w.EventID, v["vid"], v["sid"], hhash, v["app"], v["rel"], cflags,
			updated, updated, v["uid"], v["last"], v["url"], v["cleanIP"],
			iphash, latlon.Lat, latlon.Lon, v["ptyp"], bhash, auth, duration, v["xid"],
			v["split"], v["ename"], v["source"], v["medium"], v["campaign"],
			country, region, city, zip, v["term"], v["etyp"], version,
			v["sink"], score, jsparams, v["payment"], v["targets"], v["relation"], rid)
		if err != nil && i.AppConfig.Debug {
			fmt.Println("[ERROR] DuckDB[events]:", err)
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
