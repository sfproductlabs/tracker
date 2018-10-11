/*===----------- tracker.go - tracking utility written in go  -------------===
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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
	"golang.org/x/crypto/acme/autocert"
)

////////////////////////////////////////
// Get the system setup from the config.json file:
////////////////////////////////////////
type session interface {
	connect() error
	close() error
	write(w *WriteArgs) error
	listen() error
}

type KeyValue struct {
	Key   string
	Value string
}

type Field struct {
	Type    string
	Id      string
	Default string
}

type Query struct {
	Statement string
	QueryType string
	Fields    []Field
}

type Filter struct {
	Type    string
	Alias   string
	Id      string
	Queries []Query
}

type WriteArgs struct {
	WriteType int
	Values    *map[string]interface{}
	Caller    string
	IP        string
	Browser   string
	Language  string
}

type Service struct {
	Service  string
	Hosts    []string
	CACert   string
	Cert     string
	Key      string
	Secure   bool
	Critical bool

	Context      string
	Filter       []Filter
	Retry        bool
	Format       string
	MessageLimit int
	ByteLimit    int

	Consumer  bool
	Ephemeral bool
	Note      string

	Session session
}

type CassandraService struct { //Implements 'session'
	Configuration *Service
	Session       *gocql.Session
	AppConfig     *Configuration
}

type NatsService struct { //Implements 'session'
	Configuration *Service
	nc            *nats.Conn
	ec            *nats.EncodedConn
	AppConfig     *Configuration
}

type Configuration struct {
	Domains                []string //Domains in Trust, LetsEncrypt domains
	StaticDirectory        string   //Static FS Directory (./public/)
	UseLocalTLS            bool
	Notify                 []Service
	Consume                []Service
	ProxyUrl               string
	ProxyPort              string
	ProxyPortTLS           string
	ProxyDailyLimit        uint64
	ProxyDailyLimitChecker string //Service, Ex. casssandra
	ProxyDailyLimitCheck   func(string) uint64
	SchemaVersion          int
	ApiVersion             int
	Debug                  bool
}

//////////////////////////////////////// Constants
const (
	PONG              string = "pong"
	API_LIMIT_REACHED string = "API Limit Reached"

	SERVICE_TYPE_CASSANDRA string = "cassandra"
	SERVICE_TYPE_NATS      string = "nats"

	NATS_QUEUE_GROUP = "tracker"
)
const (
	WRITE_DESC_LOG    = "log"
	WRITE_DESC_UPDATE = "update"
	WRITE_DESC_COUNT  = "count"
	WRITE_DESC_EVENT  = "event"

	WRITE_LOG    = 1 << iota
	WRITE_UPDATE = 1 << iota
	WRITE_COUNT  = 1 << iota
	WRITE_EVENT  = 1 << iota
)

var (
	// Quote Ident replacer.
	qiReplacer     = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
	regexCount, _  = regexp.Compile(`\.count\.(.*)`)
	regexUpdate, _ = regexp.Compile(`\.update\.(.*)`)
)

//////////////////////////////////////// Transparent GIF
var TRACKING_GIF = []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, 0x80, 0x0, 0x0, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b}

////////////////////////////////////////
// Start here
////////////////////////////////////////
func main() {
	fmt.Println("\n\n//////////////////////////////////////////////////////////////")
	fmt.Println("Tracker.")
	fmt.Println("Software to track growth and visitor usage")
	fmt.Println("https://github.com/dioptre/tracker")
	fmt.Println("(c) Copyright 2018 SF Product Labs LLC.")
	fmt.Println("Use of this software is subject to the LICENSE agreement.")
	fmt.Println("//////////////////////////////////////////////////////////////\n\n")

	//////////////////////////////////////// LOAD CONFIG
	fmt.Println("Starting services...")
	file, _ := os.Open("config.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
	}

	//////////////////////////////////////// SETUP CACHE
	cache := cacheDir()
	if cache == "" {
		log.Fatal("Bad Cache.")
	}

	//////////////////////////////////////// SETUP CONFIG VARIABLES
	fmt.Println("Trusted domains: ", configuration.Domains)
	apiVersion := "v" + string(configuration.ApiVersion)
	//LetsEncrypt needs 443 & 80, So only override if possible
	proxyPort := ":http"
	if configuration.UseLocalTLS && configuration.ProxyPort != "" {
		proxyPort = configuration.ProxyPort
	}
	proxyPortTLS := ":https"
	if configuration.UseLocalTLS && configuration.ProxyPort != "" {
		proxyPortTLS = configuration.ProxyPortTLS
	}
	if !configuration.UseLocalTLS && (configuration.ProxyPort != "" || configuration.ProxyPortTLS != "") {
		log.Fatalln("[CRITICAL] Can not use non-standard ports with LetsEncyrpt")
	}

	//////////////////////////////////////// LOAD NOTIFIERS
	for idx := range configuration.Notify {
		s := &configuration.Notify[idx]
		switch s.Service {
		case SERVICE_TYPE_CASSANDRA:
			fmt.Printf("Notify #%d: Connecting to Cassandra Cluster: %s\n", idx, s.Hosts)
			cassandra := CassandraService{
				Configuration: s,
				AppConfig:     &configuration,
			}
			err = cassandra.connect()
			if err != nil || s.Session == nil {
				if s.Critical {
					log.Fatalf("[CRITICAL] Notify #%d. Could not connect to Cassandra Cluster. %s\n", idx, err)
				} else {
					fmt.Printf("[ERROR] Notify #%d. Could not connect to Cassandra Cluster. %s\n", idx, err)
					continue
				}
			}
			var seq int
			if err := cassandra.Session.Query(`SELECT seq FROM sequences where name='DB_VER' LIMIT 1`).Consistency(gocql.One).Scan(&seq); err != nil || seq != configuration.SchemaVersion {
				log.Fatalln("[CRITICAL] Cassandra Bad DB_VER", err)
			} else {
				fmt.Printf("Notify #%d: Connected to Cassandra: DB_VER %d\n", idx, seq)
			}
		case SERVICE_TYPE_NATS:
			//TODO:
			fmt.Printf("[ERROR] Notify #%d: NATS notifier not implemented\n", idx)
		default:
			fmt.Printf("[ERROR] %s #%d Notifier not implemented\n", s.Service, idx)
		}
	}

	//////////////////////////////////////// LOAD CONSUMERS
	for idx := range configuration.Consume {
		s := &configuration.Consume[idx]
		switch s.Service {
		case SERVICE_TYPE_CASSANDRA:
			//TODO:
			fmt.Printf("[ERROR] Consume #%d: Cassandra consumer not implemented\n", idx)
		case SERVICE_TYPE_NATS:
			fmt.Printf("Consume #%d: Connecting to NATS Cluster: %s\n", idx, s.Hosts)
			gonats := NatsService{
				Configuration: s,
				AppConfig:     &configuration,
			}
			err = gonats.connect()
			if err != nil || s.Session == nil {
				if s.Critical {
					log.Fatalf("[CRITICAL] Notify #%d. Could not connect to NATS Cluster. %s\n", idx, err)
				} else {
					fmt.Printf("[ERROR] Notify #%d. Could not connect to NATS Cluster. %s\n", idx, err)
					continue
				}

			} else {
				fmt.Printf("Consume #%d: Connected to NATS.\n", idx)
			}
			s.Session.listen()
		default:
			fmt.Printf("[ERROR] %s #%d Consumer not implemented\n", s.Service, idx)
		}

	}

	//////////////////////////////////////// SSL CERT MANAGER
	certManager := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(configuration.Domains...),
		Cache:      autocert.DirCache(cache),
	}
	server := &http.Server{ // HTTP REDIR SSL RENEW
		Addr: proxyPortTLS,
		TLSConfig: &tls.Config{ // SEC PARAMS
			GetCertificate:           certManager.GetCertificate,
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, // Required by Go (and HTTP/2 RFC), even if you only present ECDSA certs
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			},
			//MinVersion:             tls.VersionTLS12,
			//CurvePreferences:       []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		},
	}

	//////////////////////////////////////// PROXY ROUTE
	if configuration.ProxyUrl != "" {
		fmt.Println("Proxying to:", configuration.ProxyUrl)
		origin, _ := url.Parse(configuration.ProxyUrl)
		director := func(req *http.Request) {
			req.Header.Add("X-Forwarded-Host", req.Host)
			req.Header.Add("X-Origin-Host", origin.Host)
			req.URL.Scheme = "http"
			req.URL.Host = origin.Host
		}
		proxy := &httputil.ReverseProxy{Director: director}
		proxyOptions := [1]KeyValue{{Key: "Strict-Transport-Security", Value: "max-age=15768000 ; includeSubDomains"}}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			//TODO: Check certificate in cookie
			//Check API Limit
			if err := check(&configuration, r); err != nil {
				w.WriteHeader(http.StatusLocked)
				w.Write([]byte(API_LIMIT_REACHED))
				return
			}
			//Track
			track(&configuration, r)
			//Proxy
			w.Header().Set(proxyOptions[0].Key, proxyOptions[0].Value)
			proxy.ServeHTTP(w, r)
		})
	}

	//////////////////////////////////////// STATUS TEST ROUTE
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		json, _ := json.Marshal(&KeyValue{Key: "client", Value: ip})
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(json)
	})

	//////////////////////////////////////// PING PONG TEST ROUTE
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(PONG))
	})

	//////////////////////////////////////// STATIC CONTENT ROUTE
	fmt.Println("Serving static content in:", configuration.StaticDirectory)
	fs := http.FileServer(http.Dir(configuration.StaticDirectory))
	pubSlug := "/pub/" + apiVersion + "/"
	http.HandleFunc(pubSlug, func(w http.ResponseWriter, r *http.Request) {
		track(&configuration, r)
		http.StripPrefix(pubSlug, fs).ServeHTTP(w, r)
	})

	//////////////////////////////////////// 1x1 PIXEL ROUTE
	http.HandleFunc("/img/v1/", func(w http.ResponseWriter, r *http.Request) {
		track(&configuration, r)
		w.Header().Set("content-type", "image/gif")
		w.Write(TRACKING_GIF)
	})

	//////////////////////////////////////// Tracking Route
	// Ex. https://localhost:8443/tr/v1/vid/accad/ROCK/ON/lat/5/lon/6/first/true/score/6
	// OR
	// {"last":"https://localhost:5001/maps","next":"https://localhost:5001/error/maps/request/unauthorized","params":{"type":"b","origin":"maps","error":"unauthorized","method":"request"},"created":1539072857869,"duration":1959,"vid":"4883a4c0-cb96-11e8-afac-bb666b9727ed","first":"false","sid":"4883cbd0-cb96-11e8-afac-bb666b9727ed"}
	http.HandleFunc("/tr/v1/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			w.Header().Set("access-control-allow-origin", "*") //TODO Security Threat
			w.Header().Set("access-control-allow-credentials", "true")
			w.Header().Set("access-control-allow-headers", "Authorization,Accept")
			w.Header().Set("access-control-allow-methods", "GET,POST,HEAD,PUT,DELETE")
			w.Header().Set("access-control-max-age", "1728000")
		} else {
			track(&configuration, r)
		}
		w.WriteHeader(http.StatusOK)
	})

	//////////////////////////////////////// SERVE, REDIRECT AUTO to HTTPS
	go func() {
		fmt.Printf("Serving HTTP Redirect on: %s\n", proxyPort)
		if configuration.UseLocalTLS {
			http.ListenAndServe(proxyPort, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				addr, _, _ := net.SplitHostPort(req.Host)
				http.Redirect(w, req, "https://"+addr+proxyPortTLS+req.RequestURI, http.StatusFound)
			}))

		} else {
			http.ListenAndServe(proxyPort, certManager.HTTPHandler(nil))
		}

	}()
	fmt.Printf("Serving TLS requests on: %s\n", proxyPortTLS)
	if configuration.UseLocalTLS {
		server.TLSConfig.GetCertificate = nil
		log.Fatal(server.ListenAndServeTLS("server.crt", "server.key")) // SERVE HTTPS!
	} else {
		log.Fatal(server.ListenAndServeTLS("", "")) // SERVE HTTPS!
	}

}

////////////////////////////////////////
// cacheDir in /tmp for SSL
////////////////////////////////////////
func cacheDir() (dir string) {
	if u, _ := user.Current(); u != nil {
		dir = filepath.Join(os.TempDir(), "cache-golang-autocert-"+u.Username)
		//dir = filepath.Join(".", "cache-golang-autocert-"+u.Username)
		fmt.Println("Saving cache-go-lang-autocert-u.username to: ", dir)
		if err := os.MkdirAll(dir, 0700); err == nil {
			return dir
		}
	}
	return ""
}

////////////////////////////////////////
// Check
////////////////////////////////////////
func check(c *Configuration, r *http.Request) error {
	//Precheck
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if c.ProxyDailyLimit > 0 && c.ProxyDailyLimitCheck != nil && c.ProxyDailyLimitCheck(ip+";"+r.Header.Get("X-Forwarded-For")) > c.ProxyDailyLimit {
		return fmt.Errorf("API Limit Reached")
	}
	return nil
}

////////////////////////////////////////
// Trace
////////////////////////////////////////
func track(c *Configuration, r *http.Request) error {
	//Setup
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	wargs := WriteArgs{
		WriteType: WRITE_EVENT,
		Caller:    ip + ";" + r.Header.Get("X-Forwarded-For"),
		IP:        ip,
		Browser:   r.Header.Get("user-agent"),
		Language:  r.Header.Get("accept-language"),
	}

	//Process
	j := make(map[string]interface{})
	//Path
	p := strings.Split(r.URL.Path, "/")
	pmax := (len(p) - 2)
	for i := 1; i <= pmax; i += 2 {
		j[p[i]] = p[i+1] //TODO: Handle arrays
	}
	switch r.Method {
	case http.MethodGet:
		//Query
		k := r.URL.Query()
		for idx := range k {
			j[idx] = k[idx]
		}
		wargs.Values = &j
	case http.MethodPost:
		//Json (POST)
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			//r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
			if err := json.Unmarshal(body, &j); err != nil {
				return fmt.Errorf("Bad JS (parse)")
			}
			wargs.Values = &j
		}
	default:
		return nil
	}
	for idx := range c.Notify {
		s := &c.Notify[idx]
		if s.Session != nil {
			if err := s.Session.write(&wargs); err != nil {
				if c.Debug {
					fmt.Printf("[ERROR] Writing to %s: %s\n", s.Service, err)
				}
				return err
			}
		}
	}
	return nil

}

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

//////////////////////////////////////// C*
// Connect initiates the primary connection to the range of provided URLs
func (i *CassandraService) connect() error {
	err := fmt.Errorf("Could not connect to cassandra")
	cluster := gocql.NewCluster(i.Configuration.Hosts...)
	cluster.Keyspace = i.Configuration.Context
	cluster.Consistency = gocql.Quorum
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
			if i.Session.Query(`SELECT total FROM dailies where ip=? AND day=?`, ip, time.Now()).Consistency(gocql.One).Scan(&total); err != nil {
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
			v["id"]).Consistency(gocql.One).Exec()
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
			v["msg"]).Consistency(gocql.One).Exec()

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
			v["params"]).Consistency(gocql.One).Exec()

	case WRITE_EVENT:
		if i.AppConfig.Debug {
			fmt.Printf("EVENT %s\n", w)
		}
		go func() {
			//Add dailies regardless
			//[Daily]
			updated := time.Now().UTC()
			i.Session.Query(`UPDATE dailies set total=total+1 where ip = ? AND day = ?`, w.Caller, updated).Consistency(gocql.One).Exec()

			//////////////////////////////////////////////
			//FIX VARS
			//////////////////////////////////////////////
			//[first]
			first := v["first"] != "false"
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
			//[score]
			var score *float64
			if s, ok := v["score"].(string); ok {
				temp, _ := strconv.ParseFloat(s, 64)
				score = &temp
			}
			//Force reset the following types...
			//[sid]
			if _, ok := v["sid"].(string); !ok {
				v["sid"] = gocql.TimeUUID()
			}
			//Params
			if ps, ok := v["params"].(string); ok {
				temp := make(map[string]string)
				json.Unmarshal([]byte(ps), &temp)
				v["params"] = &temp
			}
			//Culture
			var culture *string
			c := strings.Split(w.Language, ",")
			if len(c) > 0 {
				culture = &c[0]
			}
			//Country
			//TODO: Use GeoIP too
			var country *string
			if tz, ok := v["tz"].(string); ok {
				if ct, oktz := countries[tz]; oktz {
					country = &ct
				}
			}

			//[Outcome]
			if outcome, ok := v["outcome"].(string); ok {
				i.Session.Query(`UPDATE outcomes set total=total+1 where outcome=? AND sink=? AND created=? AND url=?`, outcome, v["sink"], updated, v["next"]).Consistency(gocql.One).Exec()
			}

			//////////////////////////////////////////////
			//Persist
			//////////////////////////////////////////////
			if first {
				//acquisitions
				i.Session.Query(`INSERT into acquisitions 
                        (
                            vid, 
                            sid, 
							eid, 
							etyp,
							created,
							uid,
                            last,
							next,
							sink,
							ver,
							score,							
                            params,
                            duration,
                            ip,
							latlon,
							country,
							culture,
							source,
							medium,
							campaign,
							device,
							browser,
							os,
							tz,
							email,
							gender
                        ) 
                        values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?) IF NOT EXISTS`, //26
					v["vid"],
					v["sid"],
					v["eid"],
					v["etyp"],
					updated,
					v["uid"],
					v["last"],
					v["next"],
					v["sink"],
					&version,
					&score,
					v["params"],
					&duration,
					w.IP,
					&latlon,
					&country,
					&culture,
					v["source"],
					v["medium"],
					v["campaign"],
					v["device"],
					w.Browser,
					v["os"],
					v["tz"],
					v["email"],
					v["gender"]).Consistency(gocql.One).Exec()

				//starts
				i.Session.Query(`INSERT into starts 
                        (
                            vid, 
                            sid, 
							eid, 
							etyp,
							created,
							uid,
                            last,
							next,
							sink,
							ver,
							score,							
                            params,
                            duration,
                            ip,
							latlon,
							country,
							culture,
							source,
							medium,
							campaign,
							device,
							browser,
							os,
							tz
                        ) 
                        values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,? ,?,?,?,?) IF NOT EXISTS`, //24
					v["vid"],
					v["sid"],
					v["eid"],
					v["etyp"],
					updated,
					v["uid"],
					v["last"],
					v["next"],
					v["sink"],
					&version,
					&score,
					v["params"],
					&duration,
					w.IP,
					&latlon,
					&country,
					&culture,
					v["source"],
					v["medium"],
					v["campaign"],
					v["device"],
					w.Browser,
					v["os"],
					v["tz"]).Consistency(gocql.One).Exec()

			}
			//events
			i.Session.Query(`INSERT into events 
			(
				vid, 
				sid, 
				eid, 
				etyp,
				created,
				uid,
				last,
				next,
				sink,
				ver,
				score,							
				params,
				duration,
				ip,
				latlon
			) 
			values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?)`, //15
				v["vid"],
				v["sid"],
				v["eid"],
				v["etyp"],
				updated,
				v["uid"],
				v["last"],
				v["next"],
				v["sink"],
				&version,
				&score,
				v["params"],
				&duration,
				w.IP,
				&latlon).Consistency(gocql.One).Exec()

			//ends
			i.Session.Query(`INSERT into ends 
			(
				vid, 
				sid, 
				eid, 
				etyp,
				created,
				uid,
				last,
				next,
				sink,
				ver,
				score,							
				params,
				duration,
				ip,
				latlon
			) 
			values (?,?,?,?,?,?,?,?,?,? ,?,?,?,?,?)`, //15
				v["vid"],
				v["sid"],
				v["eid"],
				v["etyp"],
				updated,
				v["uid"],
				v["last"],
				v["next"],
				v["sink"],
				&version,
				&score,
				v["params"],
				&duration,
				w.IP,
				&latlon).Consistency(gocql.One).Exec()

			//nodes
			i.Session.Query(`INSERT into nodes 
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
				v["sid"]).Consistency(gocql.One).Exec()

			//locations
			i.Session.Query(`INSERT into nodes 
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
				v["sid"]).Consistency(gocql.One).Exec()

			//alias
			i.Session.Query(`INSERT into aliases 
			(
				vid, 
				uid,
				sid
			) 
			values (?,?,?)`, //3
				v["vid"],
				v["uid"],
				v["sid"]).Consistency(gocql.One).Exec()

			//users
			i.Session.Query(`INSERT into users 
				(
					vid, 
					uid,
					sid
				) 
				values (?,?,?)`, //3
				v["vid"],
				v["uid"],
				v["sid"]).Consistency(gocql.One).Exec()

			//hits
			i.Session.Query(`UPDATE hits set total=total+1 where url=?`,
				v["next"]).Consistency(gocql.One).Exec()

			//ips
			i.Session.Query(`UPDATE ips set total=total+1 where ip=?`,
				w.IP).Consistency(gocql.One).Exec()

			//reqs
			i.Session.Query(`UPDATE reqs set total=total+1 where vid=?`,
				v["vid"]).Consistency(gocql.One).Exec()

			//browsers
			i.Session.Query(`UPDATE browsers set total=total+1 where browser=?`,
				w.Browser).Consistency(gocql.One).Exec()

			//referrers
			i.Session.Query(`UPDATE referrers set total=total+1 where url=?`,
				v["last"]).Consistency(gocql.One).Exec()

			//referrals
			if v["ref"] != nil {
				i.Session.Query(`INSERT into referrals 
			(
				vid, 
				ref
			) 
			values (?,?) IF NOT EXISTS`, //2
					v["vid"],
					v["ref"]).Consistency(gocql.One).Exec()
			}

		}()
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

//////////////////////////////////////// NATS
// Connect initiates the primary connection to the range of provided URLs
func (i *NatsService) connect() error {
	err := fmt.Errorf("Could not connect to NATS")

	certFile := i.Configuration.Cert
	keyFile := i.Configuration.Key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("[ERROR] Parsing X509 certificate/key pair: %v", err)
	}

	rootPEM, err := ioutil.ReadFile(i.Configuration.CACert)

	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		log.Fatalln("[ERROR] Failed to parse root certificate.")
	}

	config := &tls.Config{
		//ServerName:         i.Configuration.Hosts[0],
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: i.Configuration.Secure, //TODO: SECURITY THREAT
	}

	if i.nc, err = nats.Connect(strings.Join(i.Configuration.Hosts[:], ","), nats.Secure(config)); err != nil {
		fmt.Println("[ERROR] Connecting to NATS:", err)
		return err
	}
	if i.ec, err = nats.NewEncodedConn(i.nc, nats.JSON_ENCODER); err != nil {
		fmt.Println("[ERROR] Encoding NATS:", err)
		return err
	}
	i.Configuration.Session = i
	return nil
}

//////////////////////////////////////// NATS
// Close
//will terminate the session to the backend, returning error if an issue arises
func (i *NatsService) close() error {
	i.ec.Drain()
	i.ec.Close()
	i.nc.Drain()
	i.nc.Close()
	return nil
}

//////////////////////////////////////// NATS
// Write
func (i *NatsService) write(w *WriteArgs) error {
	// sendCh := make(chan *map[string]interface{})
	// i.ec.Publish(i.Configuration.Context, w.Values)
	// i.ec.BindSendChan(i.Configuration.Context, sendCh)
	// sendCh <- w.Values
	return i.ec.Publish(i.Configuration.Context, w.Values)
}

//////////////////////////////////////// NATS
// Listen
func (i *NatsService) listen() error {
	for idx := range i.Configuration.Filter {
		f := &i.Configuration.Filter[idx]
		i.nc.QueueSubscribe(f.Id, NATS_QUEUE_GROUP, func(m *nats.Msg) {
			j := make(map[string]interface{})
			if err := json.Unmarshal(m.Data, &j); err == nil {
				wargs := WriteArgs{
					Values: &j,
				}
				switch f.Alias {
				case WRITE_DESC_COUNT:
					wargs.WriteType = WRITE_COUNT
				case WRITE_DESC_LOG:
					wargs.WriteType = WRITE_LOG
				case WRITE_DESC_UPDATE:
					wargs.WriteType = WRITE_UPDATE
				default:
				}
				if wargs.WriteType != 0 {
					for idx2 := range i.AppConfig.Notify {
						n := &i.AppConfig.Notify[idx2]
						if n.Session != nil {
							n.Session.write(&wargs)
						}
					}
				}
			}
		})
	}
	return nil
}
