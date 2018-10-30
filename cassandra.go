/*===----------- cassandra.go - cassandra interface   in go  -------------===
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
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

//////////////////////////////////////// C*
// Connect initiates the primary connection to the range of provided URLs
func (i *CassandraService) connect() error {
	err := fmt.Errorf("Could not connect to cassandra")
	cluster := gocql.NewCluster(i.Configuration.Hosts...)
	cluster.Keyspace = i.Configuration.Context
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = i.Configuration.Timeout * time.Millisecond
	cluster.NumConns = 2
	if i.Configuration.CACert != "" {
		sslOpts := &gocql.SslOptions{
			CaPath:                 i.Configuration.CACert,
			EnableHostVerification: i.Configuration.Secure, //TODO: SECURITY THREAT
		}
		if i.Configuration.Cert != "" && i.Configuration.Key != "" {
			sslOpts.CertPath = i.Configuration.Cert
			sslOpts.KeyPath = i.Configuration.Key
		}
		cluster.SslOpts = sslOpts
	}

	if i.Session, err = cluster.CreateSession(); err != nil {
		fmt.Println("[ERROR] Connecting to C*:", err)
		return err
	}
	i.Configuration.Session = i

	//Setup rand
	rand.Seed(time.Now().UnixNano())

	//Setup limit checker (cassandra)
	if i.AppConfig.ProxyDailyLimit > 0 && i.AppConfig.ProxyDailyLimitCheck == nil && i.AppConfig.ProxyDailyLimitChecker == SERVICE_TYPE_CASSANDRA {
		i.AppConfig.ProxyDailyLimitCheck = func(ip string) uint64 {
			var total uint64
			if i.Session.Query(`SELECT total FROM dailies where ip=? AND day=?`, ip, time.Now()).Scan(&total); err != nil {
				return 0xFFFFFFFFFFFFFFFF
			}
			return total
		}
	}
	return nil
}

//////////////////////////////////////// C*
// Close will terminate the session to the backend, returning error if an issue arises
func (i *CassandraService) close() error {
	if !i.Session.Closed() {
		i.Session.Close()
	}
	return nil
}

func (i *CassandraService) listen() error {
	//TODO: Listen for cassandra triggers
	return fmt.Errorf("[ERROR] Cassandra listen not implemented")
}

//////////////////////////////////////// C*
func (i *CassandraService) write(w *WriteArgs) error {
	err := fmt.Errorf("Could not write to any cassandra server in cluster")
	v := *w.Values
	switch w.WriteType {
	case WRITE_COUNT:
		if i.AppConfig.Debug {
			fmt.Printf("COUNT %s\n", w)
		}
		return i.Session.Query(`UPDATE counters set total=total+1 where id=?`,
			v["id"]).Exec()
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
		return i.Session.Query(`INSERT INTO updates (id, updated, msg) values (?,?,?)`,
			v["id"],
			timestamp,
			v["msg"]).Exec()

	case WRITE_LOG:
		if i.AppConfig.Debug {
			fmt.Printf("LOG %s\n", w)
		}
		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[id]
		_, ok := v["id"].(string)
		if !ok {
			v["id"] = gocql.TimeUUID().String()
		}
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

		return i.Session.Query(`INSERT INTO logs
		(
			ldate,
			created,
			ltime,
			id, 
			name, 
			host, 
			hostname, 
			owner,
			ip,
			level, 
			msg,
			params
		) 
		values (?,?,?,?,?,?,?,?,?,? ,?,?)`, //12
			v["ldate"],
			time.Now().UTC(),
			ltime,
			v["id"],
			v["name"],
			v["host"],
			v["hostname"],
			v["owner"],
			v["ip"],
			&level,
			v["msg"],
			v["params"]).Exec()

	case WRITE_EVENT:
		if i.AppConfig.Debug {
			fmt.Printf("EVENT %s\n", w)
		}
		//TODO: Commented for AWS, perhaps non-optimal, CHECK
		//go func() {

		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[vid]
		isNew := false
		if _, ok := v["vid"].(string); !ok {
			v["vid"] = gocql.TimeUUID()
			isNew = true
		}
		//[sid]
		if _, ok := v["sid"].(string); !ok {
			if isNew {
				v["sid"] = v["vid"]
			} else {
				v["sid"] = gocql.TimeUUID()
			}
		}
		//[first]
		first := isNew || (v["first"] != "false")
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

		//Force reset the following types...
		//[params]
		if ps, ok := v["params"].(string); ok {
			temp := make(map[string]string)
			json.Unmarshal([]byte(ps), &temp)
			v["params"] = &temp
		}
		//[culture]
		var culture *string
		c := strings.Split(w.Language, ",")
		if len(c) > 0 {
			culture = &c[0]
		}
		//[country]
		//TODO: Use GeoIP too
		var country *string
		if tz, ok := v["tz"].(string); ok {
			if ct, oktz := countries[tz]; oktz {
				country = &ct
			}
		}

		//[last],[url]
		if i.AppConfig.FilterPrefix {
			if last, ok := v["last"].(string); ok {
				filterUrlPrefix(i.AppConfig, &last)
				v["last"] = last
			}
			if url, ok := v["url"].(string); ok {
				filterUrlPrefix(i.AppConfig, &url)
				v["url"] = url
			} else {
				//check for /tr/ /pub/ /img/ (ignore)
				if !regexInternalURI.MatchString(w.URI) {
					filterUrlPrefix(i.AppConfig, &w.URI)
					v["url"] = w.URI
				}
			}
		}

		//////////////////////////////////////////////
		//Persist
		//////////////////////////////////////////////

		//daily
		updated := time.Now().UTC()
		if xerr := i.Session.Query(`UPDATE dailies set total=total+1 where ip = ? AND day = ?`, w.Caller, updated).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println(xerr)
		}

		//unknown vid
		if isNew {
			if xerr := i.Session.Query(`UPDATE counters set total=total+1 where id='vids_created'`).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println(xerr)
			}
		}

		//outcome
		if outcome, ok := v["outcome"].(string); ok {
			if xerr := i.Session.Query(`UPDATE outcomes set total=total+1 where outcome=? AND sink=? AND created=? AND url=?`, outcome, v["sink"], updated, v["url"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println(xerr)
			}
		}

		//ips
		if xerr := i.Session.Query(`UPDATE ips set total=total+1 where ip=?`,
			w.IP).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[ips]:", xerr)
		}

		//browsers
		if xerr := i.Session.Query(`UPDATE browsers set total=total+1 where browser=? AND bhash=?`,
			w.Browser, bhash).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[browsers]:", xerr)
		}

		if isNew || first {
			//vistors
			if xerr := i.Session.Query(`INSERT into visitors 
                        (
                            vid, 
							sid, 
							app,
							created,
							uid,
                            last,
							url,
							ip,
							latlon,
							ptype,
							bhash,
							auth,
							xid,
							split,
							ename,
							etyp,
							ver,
							sink,
							score,							
                            params,
							country,
							culture,
							source,
							medium,
							campaign,
							term,
							ref,
							aff,
							browser,
							device,
							os,
							tz,
							vp
                        ) 
                        values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?) IF NOT EXISTS`, //33
				v["vid"],
				v["sid"],
				v["app"],
				updated,
				v["uid"],
				v["last"],
				v["url"],
				w.IP,
				&latlon,
				v["ptype"],
				&bhash,
				v["auth"],
				v["xid"],
				v["split"],
				v["ename"],
				v["etyp"],
				&version,
				v["sink"],
				&score,
				v["params"],
				&country,
				&culture,
				v["source"],
				v["medium"],
				v["campaign"],
				v["term"],
				v["ref"],
				v["aff"],
				w.Browser,
				v["device"],
				v["os"],
				v["tz"],
				&vp).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[visitors]:", xerr)
			}

			//starts
			if xerr := i.Session.Query(`INSERT into sessions 
                        (
                            vid, 
							sid, 
							app,
							created,
							uid,
                            last,
							url,
							ip,
							latlon,
							ptype,
							bhash,
							auth,
                            duration,
							xid,
							split,
							ename,
							etyp,
							ver,
							sink,
							score,							
                            params,
							country,
							culture,
							source,
							medium,
							campaign,
							term,
							ref,
							aff,
							browser,
							device,
							os,
							tz,
							vp                        
						) 
                        values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?) IF NOT EXISTS`, //34
				v["vid"],
				v["sid"],
				v["app"],
				updated,
				v["uid"],
				v["last"],
				v["url"],
				w.IP,
				&latlon,
				v["ptype"],
				&bhash,
				v["auth"],
				&duration,
				v["xid"],
				v["split"],
				v["ename"],
				v["etyp"],
				&version,
				v["sink"],
				&score,
				v["params"],
				&country,
				&culture,
				v["source"],
				v["medium"],
				v["campaign"],
				v["term"],
				v["ref"],
				v["aff"],
				w.Browser,
				v["device"],
				v["os"],
				v["tz"],
				&vp).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[starts]:", xerr)
			}

		}
		//events
		if xerr := i.Session.Query(`INSERT into events 
			(
				vid, 
				sid, 
				app,
				created,
				uid,
				last,
				url,
				ip,
				latlon,
				ptype,
				bhash,
				auth,
				duration,
				xid,
				split,
				ename,
				etyp,
				ver,
				sink,
				score,							
				params,
				targets
			) 
			values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?)`, //15
			v["vid"],
			v["sid"],
			v["app"],
			updated,
			v["uid"],
			v["last"],
			v["url"],
			w.IP,
			&latlon,
			v["ptype"],
			&bhash,
			v["auth"],
			&duration,
			v["xid"],
			v["split"],
			v["ename"],
			v["etyp"],
			&version,
			v["sink"],
			&score,
			v["params"],
			v["targets"]).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[events]:", xerr)
		}

		//nodes
		if xerr := i.Session.Query(`INSERT into nodes 
			(
				vid, 
				uid,
				ip,
				sid
			) 
			values (?,?,?,?)`, //4
			v["vid"],
			v["uid"],
			w.IP,
			v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[nodes]:", xerr)
		}

		//locations
		if latlon != nil {
			if xerr := i.Session.Query(`INSERT into locations 
					(
						vid, 
						latlon,
						uid,
						sid
					) 
					values (?,?,?,?)`, //4
				v["vid"],
				&latlon,
				v["uid"],
				v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[locations]:", xerr)
			}
		}

		//alias
		if v["uid"] != nil {
			if xerr := i.Session.Query(`INSERT into aliases 
			(
				vid, 
				uid,
				sid
			) 
			values (?,?,?)`, //3
				v["vid"],
				v["uid"],
				v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[aliases]:", xerr)
			}
		}

		//users
		if v["uid"] != nil {
			if xerr := i.Session.Query(`INSERT into users 
				(
					vid, 
					uid,
					sid
				) 
				values (?,?,?)`, //3
				v["vid"],
				v["uid"],
				v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[users]:", xerr)
			}
		}

		//reqs
		if xerr := i.Session.Query(`UPDATE reqs set total=total+1 where vid=?`,
			v["vid"]).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[reqs]:", xerr)
		}

		//hits
		if _, ok := v["url"].(string); ok {
			if xerr := i.Session.Query(`UPDATE hits set total=total+1 where url=?`,
				v["url"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[hits]:", xerr)
			}
		}

		//referrers
		if _, ok := v["last"].(string); ok {
			if xerr := i.Session.Query(`UPDATE referrers set total=total+1 where url=?`,
				v["last"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[referrers]:", xerr)
			}
		}

		//referrals
		if v["ref"] != nil {
			if xerr := i.Session.Query(`INSERT into referrals 
					(
						vid, 
						ref
					) 
					values (?,?) IF NOT EXISTS`, //2
				v["vid"],
				v["ref"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[referrals]:", xerr)
			}
		}

		return nil
	default:
		//TODO: Manually run query via query in config.json
		if i.AppConfig.Debug {
			fmt.Printf("UNHANDLED %s\n", w)
		}
	}

	//TODO: Retries
	return err
}
