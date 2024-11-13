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
	"net"
	"strconv"

	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb" // Import DuckDB driver
)

// Connect initiates the primary connection to DuckDB
func (i *DuckService) connect() error {
	err := fmt.Errorf("Could not connect to DuckDB")

	// Check if connection already exists
	if i.DB != nil {
		return fmt.Errorf("database connection already exists")
	}

	// Open DuckDB connection with configuration
	i.DB, err = sql.Open("duckdb", ":memory:?access_mode=READ_WRITE&threads=4&memory_limit=4GB")
	if err != nil {
		fmt.Println("[ERROR] Connecting to DuckDB:", err)
		return err
	}

	// Configure connection pool settings
	i.DB.SetMaxOpenConns(i.Configuration.Connections)
	i.DB.SetMaxIdleConns(5)
	i.DB.SetConnMaxLifetime(time.Second * time.Duration(i.Configuration.Timeout/1000))
	i.DB.SetConnMaxIdleTime(time.Millisecond * 500)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i.Configuration.Timeout)*time.Millisecond)
	defer cancel()

	if err = i.DB.PingContext(ctx); err != nil {
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
			err := i.DB.QueryRow(`SELECT total FROM dailies WHERE ip = ? AND day = ?`,
				ip, time.Now().UTC().Format("2006-01-02")).Scan(&total)
			if err != nil {
				return 0xFFFFFFFFFFFFFFFF
			}
			return total
		}
	}

	return nil
}

// Close terminates the DuckDB connection
func (i *DuckService) close() error {
	if i.DB != nil {
		return i.DB.Close()
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
	if err := i.DB.QueryRow("SELECT pwd FROM accounts WHERE uid = ?", uid).Scan(&pwd); err == nil {
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
	(*w).Header().Set("Content-Type", "application/json")

	switch s.ServiceType {
	case SVC_POST_AGREE:
		if r.Body == nil {
			return fmt.Errorf("empty request body")
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				created := time.Now().UTC()

				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}

				ip := getIP(r)
				var iphash string
				iphash = strconv.FormatInt(int64(hash(ip)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)
				iphash = iphash + strconv.FormatInt(int64(hash(ip+iphash)), 36)

				browser := r.Header.Get("user-agent")
				var bhash *string
				if browser != "" {
					temp := strconv.FormatInt(int64(hash(browser)), 36)
					bhash = &temp
				}

				var cflags *int64
				if com, ok := b["cflags"].(int64); ok {
					cflags = &com
				} else if com, ok := b["cflags"].(float64); ok {
					temp := int64(com)
					cflags = &temp
				}

				var country *string
				var region *string
				if tz, ok := b["tz"].(string); ok {
					cleanString(&tz)
					if ct, oktz := countries[tz]; oktz {
						country = &ct
					}
				}

				var latlon *geo_point
				latf, oklatf := b["lat"].(float64)
				lonf, oklonf := b["lon"].(float64)
				if oklatf && oklonf {
					latlon = &geo_point{}
					latlon.Lat = latf
					latlon.Lon = lonf
				} else {
					lats, oklats := b["lat"].(string)
					lons, oklons := b["lon"].(string)
					if oklats && oklons {
						latlon = &geo_point{}
						latlon.Lat, _ = strconv.ParseFloat(lats, 64)
						latlon.Lon, _ = strconv.ParseFloat(lons, 64)
					}
				}

				if latlon == nil {
					if gip, err := GetGeoIP(net.ParseIP(ip)); err == nil && gip != nil {
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
						}
					}
				}

				if ct, ok := b["country"].(string); ok {
					country = &ct
				}
				if r, ok := b["region"].(string); ok {
					region = &r
				}

				upperString(country)
				cleanString(region)

				// Convert latlon to string for storage
				var latlonStr *string
				if latlon != nil {
					latlonJSON, _ := json.Marshal(latlon)
					temp := string(latlonJSON)
					latlonStr = &temp
				}

				_, err := i.DB.Exec(`INSERT INTO agreements (
					vid, created, cflags, sid, uid, avid, hhash, app, rel,
					url, ip, iphash, gaid, idfa, msid, fbid, country, region, culture,
					source, medium, campaign, term, ref, rcode, aff, browser, bhash, device,
					os, tz, latlon, zip, owner, org
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					b["vid"], created, cflags, b["sid"], b["uid"], b["avid"], hhash, b["app"], b["rel"],
					b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"], country, region, b["culture"],
					b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"], browser, bhash, b["device"],
					b["os"], b["tz"], latlonStr, b["zip"], b["owner"], b["org"])

				if err != nil {
					return err
				}

				_, err = i.DB.Exec(`INSERT INTO agreed (
					vid, created, cflags, sid, uid, avid, hhash, app, rel,
					url, ip, iphash, gaid, idfa, msid, fbid, country, region, culture,
					source, medium, campaign, term, ref, rcode, aff, browser, bhash, device,
					os, tz, latlon, zip, owner, org
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					b["vid"], created, cflags, b["sid"], b["uid"], b["avid"], hhash, b["app"], b["rel"],
					b["url"], ip, iphash, b["gaid"], b["idfa"], b["msid"], b["fbid"], country, region, b["culture"],
					b["source"], b["medium"], b["campaign"], b["term"], b["ref"], b["rcode"], b["aff"], browser, bhash, b["device"],
					b["os"], b["tz"], latlonStr, b["zip"], b["owner"], b["org"])

				if err != nil {
					return err
				}

				(*w).WriteHeader(http.StatusOK)
				return nil
			} else {
				return fmt.Errorf("Bad request (data)")
			}
		} else {
			return fmt.Errorf("Bad request (body)")
		}

	case SVC_GET_AGREE:
		vid := r.URL.Query().Get("vid")
		if vid == "" {
			(*w).WriteHeader(http.StatusBadRequest)
			return fmt.Errorf("missing required vid parameter")
		}
		rows, err := i.DB.Query("SELECT * FROM agreements WHERE vid = ?", vid)
		if err != nil {
			return err
		}
		defer rows.Close()

		var results []map[string]interface{}
		cols, _ := rows.Columns()
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				return err
			}
			row := make(map[string]interface{})
			for i, colName := range cols {
				row[colName] = columns[i]
			}
			results = append(results, row)
		}

		js, err := json.Marshal(results)
		if err != nil {
			return err
		}
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(js)
		return nil

	case SVC_GET_JURISDICTIONS:
		rows, err := i.DB.Query("SELECT * FROM jurisdictions")
		if err != nil {
			return err
		}
		defer rows.Close()

		var results []map[string]interface{}
		cols, _ := rows.Columns()
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				return err
			}
			row := make(map[string]interface{})
			for i, colName := range cols {
				row[colName] = columns[i]
			}
			results = append(results, row)
		}

		js, err := json.Marshal(results)
		if err != nil {
			return err
		}
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(js)
		return nil

	case SVC_GET_GEOIP:
		ip := getIP(r)
		if len(r.URL.Query()["ip"]) > 0 {
			ip = r.URL.Query()["ip"][0]
		}
		pip := net.ParseIP(ip)
		if gip, err := GetGeoIP(pip); err == nil && gip != nil {
			(*w).WriteHeader(http.StatusOK)
			(*w).Write(gip)
			return nil
		} else {
			if err == nil {
				return fmt.Errorf("Not Found (IP)")
			}
			return err
		}

	case SVC_GET_REDIRECTS:
		if err := i.auth(s); err != nil {
			return err
		}
		rows, err := i.DB.Query("SELECT * FROM redirect_history")
		if err != nil {
			return err
		}
		defer rows.Close()

		var results []map[string]interface{}
		cols, _ := rows.Columns()
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				return err
			}
			row := make(map[string]interface{})
			for i, colName := range cols {
				row[colName] = columns[i]
			}
			results = append(results, row)
		}

		json, _ := json.Marshal(map[string]interface{}{"results": results})
		(*w).Write(json)
		return nil

	case SVC_GET_REDIRECT:
		var redirect string
		err := i.DB.QueryRow("SELECT urlto FROM redirects WHERE urlfrom = ?", fmt.Sprintf("%s%s", r.Host, r.URL.Path)).Scan(&redirect)
		if err == nil {
			s.Values = &map[string]string{"Redirect": redirect}
			http.Redirect(*w, r, redirect, http.StatusFound)
			return nil
		}
		return err

	case SVC_POST_REDIRECT:
		if err := i.auth(s); err != nil {
			return err
		}

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad request body")
		}

		var redirect map[string]string
		if err := json.Unmarshal(body, &redirect); err != nil {
			return fmt.Errorf("Invalid JSON")
		}

		created := time.Now().UTC()

		_, err = i.DB.Exec(`INSERT INTO redirects (urlfrom, urlto, created) VALUES (?, ?, ?)`,
			redirect["from"], redirect["to"], created)
		if err != nil {
			return err
		}

		_, err = i.DB.Exec(`INSERT INTO redirect_history (urlfrom, urlto, created) VALUES (?, ?, ?)`,
			redirect["from"], redirect["to"], created)
		if err != nil {
			return err
		}

		(*w).WriteHeader(http.StatusOK)
		return nil

	default:
		return fmt.Errorf("[ERROR] DuckDB service not implemented %d", s.ServiceType)
	}
}

// Helper function to create required tables
func (i *DuckService) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS counters (
			id VARCHAR PRIMARY KEY,
			total INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS accounts (
			uid VARCHAR PRIMARY KEY,
			pwd VARCHAR NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agreements (
			vid VARCHAR,
			created TIMESTAMP,
			cflags INTEGER,
			sid VARCHAR,
			uid VARCHAR,
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
			vid VARCHAR,
			created TIMESTAMP,
			cflags INTEGER,
			sid VARCHAR,
			uid VARCHAR,
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
			id VARCHAR PRIMARY KEY,
			name VARCHAR,
			description VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS redirects (
			urlfrom VARCHAR PRIMARY KEY,
			urlto VARCHAR,
			created TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS redirect_history (
			id VARCHAR PRIMARY KEY,
			urlfrom VARCHAR,
			urlto VARCHAR,
			created TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS ltv (
			hhash VARCHAR,
			uid VARCHAR,
			vid VARCHAR,
			sid VARCHAR,
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
			uid VARCHAR,
			orid VARCHAR,
			vid VARCHAR,
			sid VARCHAR,
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
			vid VARCHAR,
			orid VARCHAR,
			uid VARCHAR,
			sid VARCHAR,
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
			vid VARCHAR,
			uid VARCHAR,
			sid VARCHAR,
			PRIMARY KEY (hhash, vid, uid)
		)`,
		`CREATE TABLE IF NOT EXISTS usernames (
			hhash VARCHAR,
			vid VARCHAR,
			uhash VARCHAR,
			sid VARCHAR,
			PRIMARY KEY (hhash, vid, uhash)
		)`,
		`CREATE TABLE IF NOT EXISTS emails (
			hhash VARCHAR,
			vid VARCHAR,
			ehash VARCHAR,
			sid VARCHAR,
			PRIMARY KEY (hhash, vid, ehash)
		)`,
		`CREATE TABLE IF NOT EXISTS cells (
			hhash VARCHAR,
			vid VARCHAR,
			chash VARCHAR,
			sid VARCHAR,
			PRIMARY KEY (hhash, vid, chash)
		)`,
		`CREATE TABLE IF NOT EXISTS reqs (
			hhash VARCHAR,
			vid VARCHAR,
			total INTEGER DEFAULT 0,
			PRIMARY KEY (hhash, vid)
		)`,
		`CREATE TABLE IF NOT EXISTS visitors (
			vid VARCHAR PRIMARY KEY,
			did VARCHAR,
			sid VARCHAR,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			uid VARCHAR,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth VARCHAR,
			xid VARCHAR,
			split VARCHAR,
			ename VARCHAR,
			etyp VARCHAR,
			ver VARCHAR,
			sink VARCHAR,
			score DOUBLE,
			params VARCHAR,
			nparams INTEGER,
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
			vid VARCHAR,
			did VARCHAR,
			sid VARCHAR,
			hhash VARCHAR,
			app VARCHAR,
			rel VARCHAR,
			cflags INTEGER,
			created TIMESTAMP,
			uid VARCHAR,
			last VARCHAR,
			url VARCHAR,
			ip VARCHAR,
			iphash VARCHAR,
			latlon VARCHAR,
			ptyp VARCHAR,
			bhash VARCHAR,
			auth VARCHAR,
			duration INTEGER,
			xid VARCHAR,
			split VARCHAR,
			ename VARCHAR,
			etyp VARCHAR,
			ver VARCHAR,
			sink VARCHAR,
			score DOUBLE,
			params VARCHAR,
			nparams INTEGER,
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
			id VARCHAR PRIMARY KEY,
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
			msg VARCHAR,
			params VARCHAR
		)`,
		`CREATE TABLE IF NOT EXISTS dailies (
			ip VARCHAR,
			day TIMESTAMP,
			total INTEGER DEFAULT 0,
			PRIMARY KEY (ip, day)
		)`,
		// `CREATE INDEX IF NOT EXISTS idx_agreements_vid ON agreements(vid)`,
		// `CREATE INDEX IF NOT EXISTS idx_agreed_vid ON agreed(vid)`,
		// `CREATE INDEX IF NOT EXISTS idx_ltv_composite ON ltv(hhash, uid)`,
		// `CREATE INDEX IF NOT EXISTS idx_ltvu_composite ON ltvu(hhash, uid, orid)`,
		// `CREATE INDEX IF NOT EXISTS idx_ltvv_composite ON ltvv(hhash, vid, orid)`
	}

	for _, query := range queries {
		if _, err := i.DB.Exec(query); err != nil {
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
		return i.DB.QueryRow(`UPDATE counters SET total=total+1 WHERE id=?`,
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
		_, err = i.DB.Exec(`INSERT INTO events (
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
			_, err = i.DB.Exec(`UPDATE dailies SET total=total+1 WHERE ip=? AND day=?`,
				w.IP, updated.Format("2006-01-02"))

			if err != nil {
				return err
			}

			// Track new visitors
			if isNew {
				_, err = i.DB.Exec(`UPDATE counters SET total=total+1 WHERE id='vids_created'`)
				if err != nil {
					return err
				}
			}

			// Insert visitor record
			if isNew {
				_, err = i.DB.Exec(`INSERT INTO visitors (
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
			err := i.DB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total)
			if err != nil {
				fmt.Printf("Error counting records in %s: %v\n", table, err)
				continue
			}

			if i.AppConfig.PruneUpdateConfig {
				// Update approach - set fields to null
				result, err := i.DB.Exec(fmt.Sprintf(`
					UPDATE %s 
					SET updated = ?,
						params = NULL,
						nparams = NULL
					WHERE created < ?`, table),
					time.Now().UTC(), pruneTime)

				if err != nil {
					fmt.Printf("Error updating records in %s: %v\n", table, err)
					continue
				}

				pruned, _ = result.RowsAffected()

			} else {
				// Delete approach
				result, err := i.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE created < ?", table), pruneTime)
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
		err := i.DB.QueryRow("SELECT COUNT(*) FROM logs").Scan(&total)
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
		result, err := i.DB.Exec("DELETE FROM logs WHERE created < ?", cutoffTime)
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

// Add a new method for health checks
func (i *DuckService) healthCheck() error {
	if i.DB == nil {
		return fmt.Errorf("database connection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return i.DB.PingContext(ctx)
}
