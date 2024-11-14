/*===----------- clickhouse.go - cassandra interface   in go  -------------===
 *
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2018 Andrew Grosser. All Rights Reserved.
 *
 *                                     `...
 *                                    yNMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMh`
 *                                    dMMMd`
 *                                    dMMMm.
 *                                    dMMMm.
 *                                    dMMMm.               /hdy.
 *                  ohs+`             yMMMd.               yMMM-
 *                 .mMMm.             yMMMm.               oMMM/
 *                 :MMMd`             sMMMN.               oMMMo
 *                 +MMMd`             oMMMN.               oMMMy
 *                 sMMMd`             /MMMN.               oMMMh
 *                 sMMMd`             /MMMN-               oMMMd
 *                 oMMMd`             :NMMM-               oMMMd
 *                 /MMMd`             -NMMM-               oMMMm
 *                 :MMMd`             .mMMM-               oMMMm`
 *                 -NMMm.             `mMMM:               oMMMm`
 *                 .mMMm.              dMMM/               +MMMm`
 *                 `hMMm.              hMMM/               /MMMm`
 *                  yMMm.              yMMM/               /MMMm`
 *                  oMMm.              oMMMo               -MMMN.
 *                  +MMm.              +MMMo               .MMMN-
 *                  +MMm.              /MMMo               .NMMN-
 *           `      +MMm.              -MMMs               .mMMN:  `.-.
 *          /hys:`  +MMN-              -NMMy               `hMMN: .yNNy
 *          :NMMMy` sMMM/              .NMMy                yMMM+-dMMMo
 *           +NMMMh-hMMMo              .mMMy                +MMMmNMMMh`
 *            /dMMMNNMMMs              .dMMd                -MMMMMNm+`
 *             .+mMMMMMN:              .mMMd                `NMNmh/`
 *               `/yhhy:               `dMMd                 /+:`
 *                                     `hMMm`
 *                                     `hMMm.
 *                                     .mMMm:
 *                                     :MMMd-
 *                                     -NMMh.
 *                                      ./:.
 *
 *===----------------------------------------------------------------------===
 */
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

// ////////////////////////////////////// C*
// Connect initiates the primary connection to the range of provided URLs
func (i *ClickhouseService) connect() error {
	// Create ClickHouse connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{i.Configuration.Hosts[0]},
		Auth: clickhouse.Auth{
			Database: i.Configuration.Context,
		},
		Debug: i.AppConfig.Debug,
		Settings: map[string]interface{}{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		fmt.Println("[ERROR] Connecting to ClickHouse:", err)
		return err
	}

	// Verify connection is alive
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping clickhouse: %v", err)
	}

	i.Session = &conn
	i.Configuration.Session = i

	// Setup rand
	rand.Seed(time.Now().UTC().UnixNano())

	// Setup limit checker
	if i.AppConfig.ProxyDailyLimit > 0 && i.AppConfig.ProxyDailyLimitCheck == nil && i.AppConfig.ProxyDailyLimitChecker == SERVICE_TYPE_CLICKHOUSE {
		i.AppConfig.ProxyDailyLimitCheck = func(ip string) uint64 {
			var total uint64
			err := (*i.Session).QueryRow(context.Background(),
				`SELECT total FROM dailies WHERE ip = ? AND day = ?`,
				ip, time.Now().UTC()).Scan(&total)
			if err != nil {
				if err == sql.ErrNoRows {
					return 0
				}
				return 0xFFFFFFFFFFFFFFFF
			}
			return total
		}
	}
	return nil
}

// ////////////////////////////////////// ClickHouse
// Close will terminate the session to the backend, returning error if an issue arises
func (i *ClickhouseService) close() error {
	if i.Session != nil {
		return (*i.Session).Close()
	}
	return nil
}

func (i *ClickhouseService) listen() error {
	return fmt.Errorf("[ERROR] ClickHouse listen not implemented")
}

func (i *ClickhouseService) auth(s *ServiceArgs) error {
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

	ctx := context.Background()
	var pwd string
	err := (*i.Session).QueryRow(ctx,
		`SELECT pwd FROM accounts WHERE uid = ?`, uid).Scan(&pwd)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("User not found")
		}
		return err
	}
	if pwd != sha(password) {
		return fmt.Errorf("Bad pass")
	}
	return nil
}

func (i *ClickhouseService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	ctx := context.Background()

	switch s.ServiceType {
	case SVC_GET_AGREE:
		if vid := r.URL.Query().Get("vid"); vid != "" {
			rows, err := (*i.Session).Query(ctx,
				"SELECT * FROM agreements WHERE vid = ?", vid)
			if err != nil {
				return err
			}
			defer rows.Close()

			var result []map[string]interface{}
			for rows.Next() {
				// Get column names
				columns := rows.Columns()
				if err != nil {
					return err
				}

				// Create a slice of interface{} to hold the values
				values := make([]interface{}, len(columns))
				valuePointers := make([]interface{}, len(columns))
				for i := range values {
					valuePointers[i] = &values[i]
				}

				// Scan the row into the slice of interface{}
				if err := rows.Scan(valuePointers...); err != nil {
					return err
				}

				// Create the map and populate it
				row := make(map[string]interface{})
				for i, col := range columns {
					row[col] = values[i]
				}
				result = append(result, row)
			}

			js, err := json.Marshal(result)
			if err != nil {
				return err
			}

			(*w).Header().Set("Content-Type", "application/json")
			(*w).WriteHeader(http.StatusOK)
			(*w).Write(js)
			return nil
		}

		(*w).Header().Set("Content-Type", "application/json")
		(*w).WriteHeader(http.StatusNotFound)
		(*w).Write([]byte("[]"))
		return nil

	case SVC_GET_JURISDICTIONS:
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM jurisdictions`)
		if err != nil {
			return err
		}
		defer rows.Close()

		var result []map[string]interface{}
		for rows.Next() {
			// Get column names
			columns := rows.Columns()
			if err != nil {
				return err
			}

			// Create a slice of interface{} to hold the values
			values := make([]interface{}, len(columns))
			valuePointers := make([]interface{}, len(columns))
			for i := range values {
				valuePointers[i] = &values[i]
			}

			// Scan the row into the slice of interface{}
			if err := rows.Scan(valuePointers...); err != nil {
				return err
			}

			// Create the map and populate it
			row := make(map[string]interface{})
			for i, col := range columns {
				row[col] = values[i]
			}
			result = append(result, row)
		}

		js, err := json.Marshal(result)
		if err != nil {
			return err
		}

		(*w).Header().Set("Content-Type", "application/json")
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
			(*w).Header().Set("Content-Type", "application/json")
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

		var result []map[string]interface{}
		rows, err := (*i.Session).Query(ctx, `SELECT * FROM redirect_history`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			// Get column names
			columns := rows.Columns()
			if err != nil {
				return err
			}

			// Create a slice of interface{} to hold the values
			values := make([]interface{}, len(columns))
			valuePointers := make([]interface{}, len(columns))
			for i := range values {
				valuePointers[i] = &values[i]
			}

			// Scan the row into the slice of interface{}
			if err := rows.Scan(valuePointers...); err != nil {
				return err
			}

			// Create the map and populate it
			row := make(map[string]interface{})
			for i, col := range columns {
				row[col] = values[i]
			}
			result = append(result, row)
		}

		json, err := json.Marshal(map[string]interface{}{"results": result})
		if err != nil {
			return err
		}

		(*w).Header().Set("Content-Type", "application/json")
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(json)
		return nil

	case SVC_GET_REDIRECT:
		var redirect string
		err := (*i.Session).QueryRow(ctx,
			`SELECT urlto FROM redirects WHERE urlfrom = ?`,
			fmt.Sprintf("%s%s", r.Host, r.URL.Path)).Scan(&redirect)

		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("Redirect not found")
			}
			return err
		}

		s.Values = &map[string]string{"Redirect": redirect}
		http.Redirect(*w, r, redirect, http.StatusFound)
		return nil

	case SVC_POST_REDIRECT:
		if err := i.auth(s); err != nil {
			return err
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				updated := time.Now().UTC()
				urlfrom := strings.ToLower(strings.TrimSpace(b["urlfrom"].(string)))
				urlto := strings.TrimSpace(b["urlto"].(string))
				if urlfrom == "" || urlto == "" {
					return fmt.Errorf("Bad URL (null)")
				}
				if strings.EqualFold(urlfrom, urlto) {
					return fmt.Errorf("Bad URL (equal)")
				}
				var urltoURL url.URL
				if checkTo, err := url.Parse(urlto); err != nil {
					return fmt.Errorf("Bad URL (destination)")
				} else {
					urltoURL = *checkTo
					if !strings.Contains(checkTo.Path, "/rdr/") {
						for _, d := range i.AppConfig.Domains {
							if strings.EqualFold(checkTo.Host, strings.TrimSpace(d)) {
								return fmt.Errorf("Bad URL (self-referential)")
							}
						}
					}
				}
				var urlfromURL url.URL
				if checkFrom, err := url.Parse(urlfrom); err != nil {
					return fmt.Errorf("Bad URL (from)")
				} else {
					urlfromURL = *checkFrom
				}
				if len(urlfromURL.Path) < 2 {
					return fmt.Errorf("Bad URL (from path)")
				}

				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}

				err = (*i.Session).Exec(ctx, `
					INSERT INTO redirects (
						hhash,
						urlfrom,
						urlto,
						updated,
						updater
					) VALUES (?, ?, ?, ?, ?)`,
					hhash,
					strings.ToLower(urlfromURL.Host)+strings.ToLower(urlfromURL.Path),
					urlto,
					updated,
					(*s.Values)["uid"])

				if err != nil {
					return err
				}

				err = (*i.Session).Exec(ctx, `
					INSERT INTO redirect_history (
						urlfrom,
						hostfrom,
						slugfrom,
						urlto,
						hostto,
						pathto,
						searchto,
						updated,
						updater
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					urlfrom,
					strings.ToLower(urlfromURL.Host),
					strings.ToLower(urlfromURL.Path),
					urlto,
					strings.ToLower(urltoURL.Host),
					strings.ToLower(urltoURL.Path),
					b["searchto"],
					updated,
					(*s.Values)["uid"],
				)

				if err != nil {
					return err
				}

				(*w).WriteHeader(http.StatusOK)
				return nil
			}
			return fmt.Errorf("Bad request (data)")
		}
		return fmt.Errorf("Bad request (body)")

	default:
		return fmt.Errorf("[ERROR] ClickHouse service not implemented %d", s.ServiceType)
	}
}

// ////////////////////////////////////// Clickhouse
func (i *ClickhouseService) prune() error {
	var lastCreated time.Time
	ctx := context.Background()

	if !i.AppConfig.PruneLogsOnly {
		for _, p := range i.Configuration.Prune {
			var pruned = 0
			var total = 0

			switch p.Table {
			case "visitors", "sessions", "events", "events_recent":
				// Get rows to prune
				rows, err := (*i.Session).Query(ctx, fmt.Sprintf(`SELECT * FROM %s WHERE updated IS NULL`, p.Table))
				if err != nil {
					if i.AppConfig.Debug {
						fmt.Printf("[[WARNING]] ERROR READING TABLE [%s] %v\n", p.Table, err)
					}
					continue
				}

				for rows.Next() {
					total++
					row := make(map[string]interface{})
					// Get column names
					columns := rows.Columns()
					if err != nil {
						if i.AppConfig.Debug {
							fmt.Printf("[[WARNING]] ERROR GETTING COLUMNS [%s] %v\n", p.Table, err)
						}
						continue
					}

					// Create a slice of interface{} to hold the values
					values := make([]interface{}, len(columns))
					valuePointers := make([]interface{}, len(columns))
					for i := range values {
						valuePointers[i] = &values[i]
					}

					// Scan the row into the slice of interface{}
					if err := rows.Scan(valuePointers...); err != nil {
						if i.AppConfig.Debug {
							fmt.Printf("[[WARNING]] ERROR READING ROW [%s] %v\n", p.Table, err)
						}
						continue
					}

					// Create the map and populate it
					for i, col := range columns {
						row[col] = values[i]
					}

					// Check if row should be pruned
					expired, created := checkRowExpired(row, nil, p, i.AppConfig.PruneSkipToTimestamp)
					if !expired {
						continue
					}

					pruned++
					if created.After(lastCreated) {
						lastCreated = *created
					}

					// Build prune query
					if p.ClearAll {
						var query string
						switch p.Table {
						case "visitors":
							query = `ALTER TABLE visitors DELETE WHERE vid = ?`
						case "sessions":
							query = `ALTER TABLE sessions DELETE WHERE vid = ? AND sid = ?`
						case "events", "events_recent":
							query = fmt.Sprintf(`ALTER TABLE %s DELETE WHERE eid = ?`, p.Table)
						}

						if err := (*i.Session).Exec(ctx, query, row["vid"], row["sid"]); err != nil && i.AppConfig.Debug {
							fmt.Printf("[[WARNING]] COULD NOT DELETE ROW [%s] %v\n", p.Table, err)
						}

					} else {
						// Build partial update
						update := make([]string, 0)
						desthash := make([]string, 0)
						for _, f := range p.Fields {
							update = append(update, fmt.Sprintf("%s = NULL", f.Id))
							if v, ok := row[f.Id].(string); ok && len(f.DestParamHash) > 0 {
								desthash = append(desthash, fmt.Sprintf(`'%s':'%s'`, f.DestParamHash, sha(v)))
							}
						}

						var query string
						switch p.Table {
						case "visitors":
							query = fmt.Sprintf(`ALTER TABLE visitors UPDATE updated = ?, %s WHERE vid = ?`,
								strings.Join(update, ","))
						case "sessions":
							query = fmt.Sprintf(`ALTER TABLE sessions UPDATE updated = ?, %s WHERE vid = ? AND sid = ?`,
								strings.Join(update, ","))
						case "events", "events_recent":
							query = fmt.Sprintf(`ALTER TABLE %s UPDATE updated = ?, %s WHERE eid = ?`,
								p.Table, strings.Join(update, ","))
						}

						if err := (*i.Session).Exec(ctx, query, time.Now().UTC(), row["vid"], row["sid"]); err != nil && i.AppConfig.Debug {
							fmt.Printf("[[WARNING]] COULD NOT UPDATE ROW [%s] %v\n", p.Table, err)
						}
					}
				}
			default:
				return fmt.Errorf("Table %s not supported for pruning", p.Table)
			}

			fmt.Printf("Pruned [ClickHouse].[%s].[%v]: %d/%d rows\n", i.Configuration.Context, p.Table, pruned, total)
		}
	}

	if i.AppConfig.PruneUpdateConfig && lastCreated.Unix() > i.AppConfig.PruneSkipToTimestamp {
		s, err := ioutil.ReadFile(i.AppConfig.ConfigFile)
		if err != nil {
			return err
		}
		var j interface{}
		if err := json.Unmarshal(s, &j); err != nil {
			return err
		}
		SetValueInJSON(j, "PruneSkipToTimestamp", lastCreated.Unix())
		s, _ = json.Marshal(j)
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, s, "", "    "); err != nil {
			return err
		}
		if err := ioutil.WriteFile(i.AppConfig.ConfigFile, prettyJSON.Bytes(), 0644); err != nil {
			return err
		}
	}

	// Prune logs table
	if !i.AppConfig.PruneLogsSkip {
		var pruned = 0
		var total = 0
		ttl := 2592000
		if i.AppConfig.PruneLogsTTL > 0 {
			ttl = i.AppConfig.PruneLogsTTL
		}

		rows, err := (*i.Session).Query(ctx, `SELECT id FROM logs`)
		if err != nil {
			return err
		}

		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				continue
			}
			total++

			if checkUUIDExpired(&id, ttl) {
				pruned++
				if err := (*i.Session).Exec(ctx, `ALTER TABLE logs DELETE WHERE id = ?`, id); err != nil && i.AppConfig.Debug {
					fmt.Printf("[[WARNING]] COULD NOT DELETE LOG %v\n", err)
				}
			}
		}

		fmt.Printf("Pruned [ClickHouse].[%s].[logs]: %d/%d rows\n", i.Configuration.Context, pruned, total)
	}

	return nil
}

// ////////////////////////////////////// Clickhouse
func (i *ClickhouseService) write(w *WriteArgs) error {
	v := *w.Values
	switch w.WriteType {
	case WRITE_COUNT:
		if i.AppConfig.Debug {
			fmt.Printf("COUNT %s\n", w)
		}
		return (*i.Session).Exec(context.Background(), `
			ALTER TABLE counters 
			UPDATE total = total + 1 
			WHERE id = ?`,
			v["id"])

	case WRITE_UPDATE:
		if i.AppConfig.Debug {
			fmt.Printf("UPDATE %s\n", w)
		}
		timestamp := time.Now().UTC()
		if updated, ok := v["updated"].(string); ok {
			if millis, err := strconv.ParseInt(updated, 10, 64); err == nil {
				timestamp = time.Unix(0, millis*int64(time.Millisecond))
			}
		}
		return (*i.Session).Exec(context.Background(), `
			INSERT INTO updates (id, updated, msg) 
			VALUES (?, ?, ?)`,
			v["id"],
			timestamp,
			v["msg"])

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

		return (*i.Session).Exec(context.Background(), `
			INSERT INTO logs
			(id, ldate, created, ltime, topic, name, host, hostname, owner, ip, iphash, level, msg, params)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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

	case WRITE_LTV:
		cleanString(&(w.Host))
		//TODO: Add array of payments
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

		var pmts []payment
		var prevpaid *float64
		//[LTV]
		if xerr := (*i.Session).QueryRow(context.Background(), "SELECT payments,created,paid FROM ltv WHERE hhash=? AND uid=?", hhash, v["uid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("CH[ltv]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		// Update LTV table
		err := (*i.Session).Exec(context.Background(), `
			INSERT INTO ltv
			(hhash, uid, vid, sid, payments, paid, org, updated, updater, created, owner)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			hhash,
			v["uid"],
			v["vid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("CH[ltv]:", err)
		}

		//[LTVU]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil
		if xerr := (*i.Session).QueryRow(context.Background(), "SELECT payments,created,paid FROM ltvu WHERE hhash=? AND uid=? AND orid=?", hhash, v["uid"], v["orid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[ltvu]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		// Update LTVU table
		err = (*i.Session).Exec(context.Background(), `
			INSERT INTO ltvu
			(hhash, uid, orid, vid, sid, payments, paid, org, updated, updater, created, owner)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			hhash,
			v["uid"],
			v["orid"],
			v["vid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("CH[ltvu]:", err)
		}

		//[LTVV]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil
		if xerr := (*i.Session).QueryRow(context.Background(), "SELECT payments,created,paid FROM ltvv WHERE hhash=? AND vid=? AND orid=?", hhash, v["vid"], v["orid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[ltvv]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		// Update LTVV table
		err = (*i.Session).Exec(context.Background(), `
			INSERT INTO ltvv
			(hhash, vid, orid, uid, sid, payments, paid, org, updated, updater, created, owner)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			hhash,
			v["vid"],
			v["orid"],
			v["uid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"])
		if err != nil && i.AppConfig.Debug {
			fmt.Println("CH[ltvv]:", err)
		}

		return nil

	default:
		if i.AppConfig.Debug {
			fmt.Printf("UNHANDLED %s\n", w)
		}
		return fmt.Errorf("unhandled write type")
	}
}
