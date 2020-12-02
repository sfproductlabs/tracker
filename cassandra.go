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
	"net"
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
	if i.Configuration.Retries > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: i.Configuration.Retries}
	}
	cluster.Timeout = i.Configuration.Timeout * time.Millisecond
	cluster.NumConns = i.Configuration.Connections
	cluster.ReconnectInterval = time.Second
	cluster.SocketKeepalive = time.Millisecond * 500
	cluster.MaxPreparedStmts = 10000
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
	rand.Seed(time.Now().UTC().UnixNano())

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
	case SVC_POST_AGREE:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				created := time.Now().UTC()
				//[hhash]
				var hhash *string
				addr := getHost(r)
				if addr != "" {
					temp := strconv.FormatInt(int64(hash(addr)), 36)
					hhash = &temp
				}
				ip := getIP(r)
				var iphash string
				//128 bits = ipv6
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
				if com, ok := b["cflags"].(float64); ok {
					temp := int64(com)
					cflags = &temp
				}
				//[country]
				var country *string
				var region *string
				if tz, ok := b["tz"].(string); ok {
					cleanString(&tz)
					if ct, oktz := countries[tz]; oktz {
						country = &ct
					}
				}
				//[latlon]
				var latlon *geo_point
				latf, oklatf := b["lat"].(float64)
				lonf, oklonf := b["lon"].(float64)
				if oklatf && oklonf {
					//Float
					latlon = &geo_point{}
					latlon.Lat = latf
					latlon.Lon = lonf
				} else {
					//String
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
				//Self identification of geo_pol overrules geoip
				if ct, ok := b["country"].(string); ok {
					country = &ct
				}
				if r, ok := b["region"].(string); ok {
					region = &r
				}
				upperString(country)
				cleanString(region)
				if /* results, */ err := i.Session.Query(`INSERT into agreements (
					vid, 
					created,  
					-- compliances,
					cflags,
					sid, 
					uid, 
					avid,
					hhash, 
					app, 
					rel, 

					url, 
					ip,
					iphash, 
					gaid,
					idfa,
					msid,
					fbid,
					country, 
					region,
					culture, 
					
					source,
					medium,
					campaign,
					term, 
					ref, 
					rcode, 
					aff,
					browser,
					bhash,
					device, 
					
					os, 
					tz,
					--vp,
					--loc frozen<geo_pol>,
					latlon,
					zip,
					owner,
					org
				 ) values (?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?)`, //NB: Removed  'IF NOT EXISTS' so can update
					b["vid"],
					created,
					//compliances map<text,frozen<set<text>>>,
					cflags,
					b["sid"],
					b["uid"],
					b["avid"],
					hhash,
					b["app"],
					b["rel"],

					b["url"],
					ip,
					iphash,
					b["gaid"],
					b["idfa"],
					b["msid"],
					b["fbid"],
					country,
					region,
					b["culture"],

					b["source"],
					b["medium"],
					b["campaign"],
					b["term"],
					b["ref"],
					b["rcode"],
					b["aff"],
					browser,
					bhash,
					b["device"],

					b["os"],
					b["tz"],
					//  vp frozen<viewport>,
					//  loc frozen<geo_pol>,
					latlon,
					b["zip"],
					b["owner"],
					b["org"],
				).Exec(); err != nil {
					return err
				}

				i.Session.Query(`INSERT into agreed (
					vid, 
					created,  
					-- compliances,
					cflags,
					sid, 
					uid, 
					avid,
					hhash, 
					app, 
					rel, 

					url, 
					ip,
					iphash, 
					gaid,
					idfa,
					msid,
					fbid,
					country, 
					region,
					culture, 
					
					source,
					medium,
					campaign,
					term, 
					ref, 
					rcode, 
					aff,
					browser,
					bhash,
					device, 
					
					os, 
					tz,
					--vp,
					--loc frozen<geo_pol>,
					latlon,
					zip,
					owner,
					org
				 ) values (?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?)`, //NB: Removed  'IF NOT EXISTS' so can update
					b["vid"],
					created,
					//compliances map<text,frozen<set<text>>>,
					cflags,
					b["sid"],
					b["uid"],
					b["avid"],
					hhash,
					b["app"],
					b["rel"],

					b["url"],
					ip,
					iphash,
					b["gaid"],
					b["idfa"],
					b["msid"],
					b["fbid"],
					country,
					region,
					b["culture"],

					b["source"],
					b["medium"],
					b["campaign"],
					b["term"],
					b["ref"],
					b["rcode"],
					b["aff"],
					browser,
					bhash,
					b["device"],

					b["os"],
					b["tz"],
					//  vp frozen<viewport>,
					//  loc frozen<geo_pol>,
					latlon,
					b["zip"],
					b["owner"],
					b["org"],
				).Exec()

				(*w).WriteHeader(http.StatusOK)
				return nil
			} else {
				return fmt.Errorf("Bad request (data)")
			}
		} else {
			return fmt.Errorf("Bad request (body)")
		}
	case SVC_GET_AGREE:
		var vid string
		if len(r.URL.Query()["vid"]) > 0 {
			vid = r.URL.Query()["vid"][0]
			if rows, err := i.Session.Query(`SELECT * FROM agreements where vid=?`, vid).Iter().SliceMap(); err == nil {
				js, err := json.Marshal(rows)
				(*w).WriteHeader(http.StatusOK)
				(*w).Header().Set("Content-Type", "application/json")
				(*w).Write(js)
				return err
			} else {
				return err
			}
		} else {
			(*w).WriteHeader(http.StatusNotFound)
			(*w).Header().Set("Content-Type", "application/json")
			(*w).Write([]byte("[]"))
		}
		fmt.Println(vid)
		return nil
	case SVC_GET_JURISDICTIONS:
		if jds, err := i.Session.Query(`SELECT * FROM jurisdictions`).Iter().SliceMap(); err == nil {
			js, err := json.Marshal(jds)
			(*w).WriteHeader(http.StatusOK)
			(*w).Header().Set("Content-Type", "application/json")
			(*w).Write(js)
			return err
		} else {
			return err
		}
	case SVC_GET_GEOIP:
		ip := getIP(r)
		if len(r.URL.Query()["ip"]) > 0 {
			ip = r.URL.Query()["ip"][0]
		}
		pip := net.ParseIP(ip)
		if gip, err := GetGeoIP(pip); err == nil && gip != nil {
			(*w).WriteHeader(http.StatusOK)
			(*w).Header().Set("Content-Type", "application/json")
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
			 iphash,
			 level, 
			 msg,
			 params
		 ) 
		 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?)`, //14
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
			iphash,
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

		if xerr := i.Session.Query(`INSERT into events_recent 
			 (
				 eid,
				 vid, 
				 sid,
				 hhash, 
				 app,
				 rel,
				 cflags,
				 created,
				 uid,
				 last,
				 url,
				 ip,
				 iphash,
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
				 country,
				 region,
				 city,
				 zip,
				 term,
				 etyp,
				 ver,
				 sink,
				 score,							
				 params,
				 nparams,
				 targets,
				 rid,
				 relation
			 ) 
			 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?)`, //38
			w.EventID,
			v["vid"],
			v["sid"],
			hhash,
			v["app"],
			v["rel"],
			cflags,
			updated,
			v["uid"],
			v["last"],
			v["url"],
			w.IP,
			iphash,
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
			country,
			region,
			city,
			zip,
			v["term"],
			v["etyp"],
			version,
			v["sink"],
			score,
			params,
			nparams,
			v["targets"],
			rid,
			v["relation"]).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[events_recent]:", xerr)
		}

		if !i.AppConfig.UseRemoveIP {
			v["cleanIP"] = w.IP
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
				 cflags,
				 created,
				 uid,
				 last,
				 url,
				 ip,
				 iphash,
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
				 country,
				 region,
				 city,
				 zip,
				 term,
				 etyp,
				 ver,
				 sink,
				 score,							
				 params,
				 nparams,
				 targets,
				 rid,
				 relation
			 ) 
			 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?)`, //38
			w.EventID,
			v["vid"],
			v["sid"],
			hhash,
			v["app"],
			v["rel"],
			cflags,
			updated,
			v["uid"],
			v["last"],
			v["url"],
			v["cleanIP"],
			iphash,
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
			country,
			region,
			city,
			zip,
			v["term"],
			v["etyp"],
			version,
			v["sink"],
			score,
			params,
			nparams,
			v["targets"],
			rid,
			v["relation"]).Exec(); xerr != nil && i.AppConfig.Debug {
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

			w.SaveCookie = true

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
					 iphash,
					 sid
				 ) 
				 values (?,?,?,?,?,?)`, //6
				hhash,
				v["vid"],
				v["uid"],
				w.IP,
				iphash,
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
							 cflags,
							 created,
							 uid,
							 last,
							 url,
							 ip,
							 iphash,
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
							 nparams,
							 gaid,
							 idfa,
							 msid,
							 fbid,
							 country,
							 region,
							 city,
							 zip,
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
						 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?) 
						 IF NOT EXISTS`, //47
					v["vid"],
					v["did"],
					v["sid"],
					hhash,
					v["app"],
					v["rel"],
					cflags,
					updated,
					v["uid"],
					v["last"],
					v["url"],
					v["cleanIP"],
					iphash,
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
					nparams,
					v["gaid"],
					v["idfa"],
					v["msid"],
					v["fbid"],
					country,
					region,
					city,
					zip,
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
							 cflags,
							 created,
							 uid,
							 last,
							 url,
							 ip,
							 iphash,
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
							 nparams,
							 gaid,
							 idfa,
							 msid,
							 fbid,
							 country,
							 region,
							 city,
							 zip,
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
						 values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?) 
						 IF NOT EXISTS`, //48
					v["vid"],
					v["did"],
					v["sid"],
					hhash,
					v["app"],
					v["rel"],
					cflags,
					updated,
					v["uid"],
					v["last"],
					v["url"],
					v["cleanIP"],
					iphash,
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
					nparams,
					v["gaid"],
					v["idfa"],
					v["msid"],
					v["fbid"],
					country,
					region,
					city,
					zip,
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
	case WRITE_TLV:
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
			if temp, err := gocql.ParseUUID(iid); err == nil {
				pmt.InvoiceID = &temp
			}
		}
		if pid, ok := v["pid"].(string); ok {
			if temp, err := gocql.ParseUUID(pid); err == nil {
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
		//[TLV]
		if xerr := i.Session.Query("SELECT payments,created,paid FROM tlv WHERE hhash=? AND uid=?", hhash, v["uid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlv]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		if xerr := i.Session.Query(`UPDATE tlv SET
			vid = ?, 
			sid = ?,
			payments = ?, 
			paid = ?,
			org = ?,
			updated = ?,
			updater = ?,
			created = ?,
			owner = ?
			WHERE hhash=? AND uid=?`, //11
			v["vid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"],

			hhash,
			v["uid"],
		).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlv]:", xerr)
		}

		//[TLVU]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil
		if xerr := i.Session.Query("SELECT payments,created,paid FROM tlvu WHERE hhash=? AND uid=? AND orid=?", hhash, v["uid"], v["orid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlvu]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		if xerr := i.Session.Query(`UPDATE tlvu SET
			vid = ?, 
			sid = ?,
			payments = ?, 
			paid = ?,
			org = ?,
			updated = ?,
			updater = ?,
			created = ?,
			owner = ?
			WHERE hhash=? AND uid=? AND orid = ?`, //11
			v["vid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"],

			hhash,
			v["uid"],
			v["orid"],
		).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlvu]:", xerr)
		}

		//[TLVV]
		pmts = pmts[:0]
		created = &updated
		prevpaid = nil
		if xerr := i.Session.Query("SELECT payments,created,paid FROM tlvv WHERE hhash=? AND vid=? AND orid=?", hhash, v["vid"], v["orid"]).Scan(&pmts, &created, &prevpaid); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlvv]:", xerr)
		}
		if prevpaid != nil && paid != nil {
			*prevpaid = *prevpaid + *paid
		} else {
			prevpaid = paid
		}

		pmts = append(pmts, *pmt)

		if xerr := i.Session.Query(`UPDATE tlvv SET
			uid = ?, 
			sid = ?,
			payments = ?, 
			paid = ?,
			org = ?,
			updated = ?,
			updater = ?,
			created = ?,
			owner = ?
			WHERE hhash=? AND vid=? AND orid = ?`, //11
			v["uid"],
			v["sid"],
			pmts,
			prevpaid,
			v["org"],
			updated,
			v["uid"],
			created,
			v["uid"],

			hhash,
			v["vid"],
			v["orid"],
		).Exec(); xerr != nil && i.AppConfig.Debug {
			fmt.Println("C*[tlvv]:", xerr)
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
