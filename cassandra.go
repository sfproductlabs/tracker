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
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
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
	cluster.NumConns = i.Configuration.Connections
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

func (i *CassandraService) auth(s *ServiceArgs) error {
	//TODO: AG implement JWT
	//TODO: AG implement creds (check domain level auth)
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
	if err := i.Session.Query(`SELECT pwd FROM accounts where uid=?`, uid).Scan(&pwd); err == nil {
		if pwd != sha(password) {
			return fmt.Errorf("Bad pass")
		}
		return nil
	} else {
		return err
	}

}

func (i *CassandraService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	switch s.ServiceType {
	case SVC_GET_REDIRECTS:
		if err := i.auth(s); err != nil {
			return err
		}
		if results, err := i.Session.Query(`SELECT * FROM redirect_history`).Iter().SliceMap(); err == nil {
			json, _ := json.Marshal(map[string]interface{}{"results": results})
			(*w).Header().Set("Content-Type", "application/json")
			(*w).WriteHeader(http.StatusOK)
			(*w).Write(json)
			return nil
		} else {
			return err
		}
	case SVC_GET_REDIRECT:
		//TODO: AG ADD CACHE
		var redirect string
		if err := i.Session.Query(`SELECT urlto FROM redirects where urlfrom=?`, fmt.Sprintf("%s%s", r.Host, r.URL.Path)).Scan(&redirect); err == nil {
			s.Values = &map[string]string{"Redirect": redirect}
			http.Redirect(*w, r, redirect, http.StatusFound)
			return nil
		} else {
			return err
		}
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
				//[hhash]
				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}

				if /* results, */ err := i.Session.Query(`INSERT into redirects (
					 hhash,
					 urlfrom, 					
					 urlto,
					 updated, 
					 updater 
				 ) values (?,?,?,?,?)`, //NB: Removed  'IF NOT EXISTS' so can update
					hhash,
					strings.ToLower(urlfromURL.Host)+strings.ToLower(urlfromURL.Path),
					urlto,
					updated,
					(*s.Values)["uid"],
				).Exec(); err != nil {
					return err
				}
				// Removed 'IF NOT EXISTS'
				//.NoSkipMetadata().Iter().SliceMap()
				// if false == results[0]["[applied]"] {
				// 	return fmt.Errorf("URL exists")
				// }
				if err := i.Session.Query(`INSERT into redirect_history (
					 urlfrom, 
					 hostfrom,
					 slugfrom, 
					 urlto, 
					 hostto, 
					 pathto, 
					 searchto, 
					 updated, 
					 updater
				 ) values (?,?,?,?,?,?,?,?,?)`,
					urlfrom,
					strings.ToLower(urlfromURL.Host),
					strings.ToLower(urlfromURL.Path),
					urlto,
					strings.ToLower(urltoURL.Host),
					strings.ToLower(urltoURL.Path),
					b["searchto"],
					updated,
					(*s.Values)["uid"],
				).Exec(); err != nil {
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
	default:
		return fmt.Errorf("[ERROR] Cassandra service not implemented %d", s.ServiceType)
	}

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
			topic = ttemp1;			
		} else {
			if ttemp2, ok2 := v["id"].(string); ok2 {
				topic = ttemp2;
			}
		}

		return i.Session.Query(`INSERT INTO logs
		 (
			 id,
			 ldate,
			 created,
			 ltime,
			 topic, 
			 name, 
			 host, 
			 hostname, 
			 owner,
			 ip,
			 level, 
			 msg,
			 params
		 ) 
		 values (?,?,?,?,?,?,?,?,?,? ,?,?,?)`, //13
			gocql.TimeUUID(),
			v["ldate"],
			time.Now().UTC(),
			ltime,
			topic,
			v["name"],
			v["host"],
			v["hostname"],
			v["owner"],
			v["ip"],
			level,
			v["msg"],
			v["params"]).Exec()

	case WRITE_EVENT:
		if i.AppConfig.Debug {
			fmt.Printf("EVENT %s\n", w)
		}
		//TODO: Commented for AWS, perhaps non-optimal, CHECK
		//go func() {

		//////////////////////////////////////////////
		//FIX CASE
		//////////////////////////////////////////////
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

		//////////////////////////////////////////////
		//FIX VARS
		//////////////////////////////////////////////
		//[hhash]
		var hhash *string
		if w.Host != "" {
			temp := strconv.FormatInt(int64(hash(w.Host)), 36)
			hhash = &temp
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
		var rid *gocql.UUID
		if temp, ok := v["rid"].(string); ok {
			if temp2, err := gocql.ParseUUID(temp); err == nil {
				rid = &temp2
			}
		}
		//[auth]
		var auth *gocql.UUID
		if temp, ok := v["auth"].(string); ok {
			if temp2, err := gocql.ParseUUID(temp); err == nil {
				auth = &temp2
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

			delete(*params, "tr")
			delete(*params, "time")
			delete(*params, "vid")
			delete(*params, "did")
			delete(*params, "sid")
			delete(*params, "app")
			delete(*params, "rel")
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
			delete(*params, "country")
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
			delete(*params, "rcode")

			delete(*params, "ename")
			delete(*params, "source")
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
		//[country]
		//TODO: Use GeoIP too
		var country *string
		if tz, ok := v["tz"].(string); ok {
			if ct, oktz := countries[tz]; oktz {
				country = &ct
			}
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

		//////////////////////////////////////////////
		//Persist
		//////////////////////////////////////////////

		//ips
		if xerr := i.Session.Query(`UPDATE ips set total=total+1 where hhash=? AND ip=?`,
			hhash, w.IP).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[ips]:", xerr)
		}

		//routed
		if xerr := i.Session.Query(`UPDATE routed set url=? where hhash=? AND ip=?`,
			v["url"], hhash, w.IP).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[routed]:", xerr)
		}

		//events
		if xerr := i.Session.Query(`INSERT into events 
			 (
				 eid,
				 vid, 
				 sid,
				 hhash, 
				 app,
				 rel,
				 created,
				 uid,
				 last,
				 url,
				 ip,
				 latlon,
				 ptyp,
				 bhash,
				 auth,
				 duration,
				 xid,
				 split,
				 ename,
				 source,
				 medium,
				 campaign,
				 term,
				 etyp,
				 ver,
				 sink,
				 score,							
				 params,
				 targets,
				 rid
			 ) 
			 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? )`, //30
			w.EventID,
			v["vid"],
			v["sid"],
			hhash,
			v["app"],
			v["rel"],
			updated,
			v["uid"],
			v["last"],
			v["url"],
			w.IP,
			latlon,
			v["ptyp"],
			bhash,
			auth,
			duration,
			v["xid"],
			v["split"],
			v["ename"],
			v["source"],
			v["medium"],
			v["campaign"],
			v["term"],
			v["etyp"],
			version,
			v["sink"],
			score,
			params,
			v["targets"],
			rid).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[events]:", xerr)
		}

		//Exclude from params in sessions and visitors. Note: more above.
		if params != nil {
			delete(*params, "campaign")
			delete(*params, "source")
			delete(*params, "medium")
			if len(*params) == 0 {
				params = nil
			}
		}

		if !w.IsServer {

			//[vid]
			isNew := false
			if _, ok := v["vid"].(string); !ok {
				v["vid"] = w.EventID
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
			isFirst := isNew || (v["first"] != "false")

			//hits
			if _, ok := v["url"].(string); ok {
				if xerr := i.Session.Query(`UPDATE hits set total=total+1 where hhash=? AND url=?`,
					hhash, v["url"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[hits]:", xerr)
				}
			}

			//daily
			if xerr := i.Session.Query(`UPDATE dailies set total=total+1 where ip = ? AND day = ?`, w.IP, updated).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[dailies]:", xerr)
			}

			//unknown vid
			if isNew {
				if xerr := i.Session.Query(`UPDATE counters set total=total+1 where id='vids_created'`).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[counters]vids_created:", xerr)
				}
			}

			//outcome
			if outcome, ok := v["outcome"].(string); ok {
				if xerr := i.Session.Query(`UPDATE outcomes set total=total+1 where hhash=? AND outcome=? AND sink=? AND created=? AND url=?`,
					hhash, outcome, v["sink"], updated, v["url"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[outcomes]:", xerr)
				}
			}

			//referrers
			if _, ok := v["last"].(string); ok {
				if xerr := i.Session.Query(`UPDATE referrers set total=total+1 where hhash=? AND url=?`,
					hhash, v["last"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[referrers]:", xerr)
				}
			}

			//referrals
			if v["ref"] != nil {
				if xerr := i.Session.Query(`INSERT into referrals 
					 (
						 hhash,
						 vid, 
						 ref
					 ) 
					 values (?,?,?) IF NOT EXISTS`, //3
					hhash,
					v["vid"],
					v["ref"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[referrals]:", xerr)
				}
			}

			//referred
			if v["rcode"] != nil {
				if xerr := i.Session.Query(`INSERT into referred 
					 (
						 hhash,
						 vid, 
						 rcode
					 ) 
					 values (?,?,?) IF NOT EXISTS`, //3
					hhash,
					v["vid"],
					v["rcode"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[referred]:", xerr)
				}
			}

			//hosts
			if w.Host != "" {
				if xerr := i.Session.Query(`INSERT into hosts 
					 (
						 hhash,
						 hostname						
					 ) 
					 values (?,?) IF NOT EXISTS`, //2
					hhash,
					w.Host).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[hosts]:", xerr)
				}
			}

			//browsers
			if xerr := i.Session.Query(`UPDATE browsers set total=total+1 where hhash=? AND browser=? AND bhash=?`,
				hhash, w.Browser, bhash).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[browsers]:", xerr)
			}

			//nodes
			if xerr := i.Session.Query(`INSERT into nodes 
				 (
					 hhash,
					 vid, 
					 uid,
					 ip,
					 sid
				 ) 
				 values (?,?,?,?,?)`, //4
				hhash,
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
					 hhash,
					 vid, 
					 latlon,
					 uid,
					 sid
				 ) 
				 values (?,?,?,?,?)`, //5
					hhash,
					v["vid"],
					latlon,
					v["uid"],
					v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[locations]:", xerr)
				}
			}

			//alias
			if v["uid"] != nil {
				if xerr := i.Session.Query(`INSERT into aliases 
					 (
						 hhash,
						 vid, 
						 uid,
						 sid
					 ) 
					 values (?,?,?,?)`, //4
					hhash,
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
						 hhash,
						 vid, 
						 uid,
						 sid
					 ) 
					 values (?,?,?,?)`, //4
					hhash,
					v["vid"],
					v["uid"],
					v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[users]:", xerr)
				}
			}

			//uhash
			if uhash != nil {
				if xerr := i.Session.Query(`INSERT into usernames 
					 (
						 hhash,
						 vid, 
						 uhash,
						 sid
					 ) 
					 values (?,?,?,?)`, //4
					hhash,
					v["vid"],
					uhash,
					v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[usernames]:", xerr)
				}
			}

			//ehash
			if ehash != nil {
				if xerr := i.Session.Query(`INSERT into emails
					 (
						 hhash,
						 vid, 
						 ehash,
						 sid
					 ) 
					 values (?,?,?,?)`, //4
					hhash,
					v["vid"],
					ehash,
					v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[emails]:", xerr)
				}
			}

			//chash
			if chash != nil {
				if xerr := i.Session.Query(`INSERT into cells
					 (
						 hhash,
						 vid, 
						 chash,
						 sid
					 ) 
					 values (?,?,?,?)`, //4
					hhash,
					v["vid"],
					chash,
					v["sid"]).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[cells]:", xerr)
				}
			}

			//reqs
			if xerr := i.Session.Query(`UPDATE reqs set total=total+1 where hhash=? AND vid=?`,
				hhash, v["vid"]).Exec(); xerr != nil && i.AppConfig.Debug {
				fmt.Println("C*[reqs]:", xerr)
			}

			if isNew || isFirst {
				//vistors
				if xerr := i.Session.Query(`INSERT into visitors 
						 (
							 vid, 
							 did,
							 sid, 
							 hhash,
							 app,
							 rel,
							 created,
							 uid,
							 last,
							 url,
							 ip,
							 latlon,
							 ptyp,
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
							 gaid,
							 idfa,
							 country,
							 culture,
							 source,
							 medium,
							 campaign,
							 term,
							 ref,
							 rcode,
							 aff,
							 browser,
							 device,
							 os,
							 tz,
							 vp
						 ) 
						 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?) 
						 IF NOT EXISTS`, //39
					v["vid"],
					v["did"],
					v["sid"],
					hhash,
					v["app"],
					v["rel"],
					updated,
					v["uid"],
					v["last"],
					v["url"],
					w.IP,
					latlon,
					v["ptyp"],
					bhash,
					auth,
					v["xid"],
					v["split"],
					v["ename"],
					v["etyp"],
					version,
					v["sink"],
					score,
					params,
					v["gaid"],
					v["idfa"],
					country,
					culture,
					v["source"],
					v["medium"],
					v["campaign"],
					v["term"],
					v["ref"],
					v["rcode"],
					v["aff"],
					w.Browser,
					v["device"],
					v["os"],
					v["tz"],
					vp).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[visitors]:", xerr)
				}

				//starts
				if xerr := i.Session.Query(`INSERT into sessions 
						 (
							 vid, 
							 did,
							 sid, 
							 hhash,
							 app,
							 rel,
							 created,
							 uid,
							 last,
							 url,
							 ip,
							 latlon,
							 ptyp,
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
							 gaid,
							 idfa,
							 country,
							 culture,
							 source,
							 medium,
							 campaign,
							 term,
							 ref,
							 rcode,
							 aff,
							 browser,
							 device,
							 os,
							 tz,
							 vp                        
						 ) 
						 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,?) 
						 IF NOT EXISTS`, //40
					v["vid"],
					v["did"],
					v["sid"],
					hhash,
					v["app"],
					v["rel"],
					updated,
					v["uid"],
					v["last"],
					v["url"],
					w.IP,
					latlon,
					v["ptyp"],
					bhash,
					auth,
					duration,
					v["xid"],
					v["split"],
					v["ename"],
					v["etyp"],
					version,
					v["sink"],
					score,
					params,
					v["gaid"],
					v["idfa"],
					country,
					culture,
					v["source"],
					v["medium"],
					v["campaign"],
					v["term"],
					v["ref"],
					v["rcode"],
					v["aff"],
					w.Browser,
					v["device"],
					v["os"],
					v["tz"],
					vp).Exec(); xerr != nil && i.AppConfig.Debug {
					fmt.Println("C*[sessions]:", xerr)
				}

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
