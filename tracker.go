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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
	Value interface{}
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
	URI       string
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
	Timeout      time.Duration
	Connections  int

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
	Domains                  []string //Domains in Trust, LetsEncrypt domains
	StaticDirectory          string   //Static FS Directory (./public/)
	UseLocalTLS              bool
	TLSCert                  string
	TLSKey                   string
	Notify                   []Service
	Consume                  []Service
	ProxyUrl                 string
	ProxyPort                string
	ProxyPortTLS             string
	ProxyDailyLimit          uint64
	ProxyDailyLimitChecker   string //Service, Ex. casssandra
	ProxyDailyLimitCheck     func(string) uint64
	SchemaVersion            int
	ApiVersion               int
	Debug                    bool
	UrlPrefixFilter          string
	FilterPrefix             bool
	MaximumConnections       int
	ReadTimeoutSeconds       int
	ReadHeaderTimeoutSeconds int
	WriteTimeoutSeconds      int
	IdleTimeoutSeconds       int
	MaxHeaderBytes           int
	DefaultRedirect          string
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
	qiReplacer          = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
	regexCount, _       = regexp.Compile(`\.count\.(.*)`)
	regexUpdate, _      = regexp.Compile(`\.update\.(.*)`)
	urlPrefix, _        = regexp.Compile(`(.*)`)
	regexInternalURI, _ = regexp.Compile(`.*(/tr/|/img/|/pub/).*`)
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
	configFile := "config.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	fmt.Println("Configuration file: ", configFile)
	file, _ := os.Open(configFile)
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

	////////////////////////////////////////SETUP FILTER
	if configuration.UrlPrefixFilter != "" {
		fmt.Println("Setting up URL prefix filter...")
		configuration.FilterPrefix = true
		urlPrefix, _ = regexp.Compile(configuration.UrlPrefixFilter)
	}

	//////////////////////////////////////// SETUP CONFIG VARIABLES
	fmt.Println("Trusted domains: ", configuration.Domains)
	apiVersion := "v" + strconv.Itoa(configuration.ApiVersion)
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
		Addr:              proxyPortTLS,
		ReadTimeout:       time.Duration(configuration.ReadTimeoutSeconds) * time.Second,
		ReadHeaderTimeout: time.Duration(configuration.ReadHeaderTimeoutSeconds) * time.Second,
		WriteTimeout:      time.Duration(configuration.WriteTimeoutSeconds) * time.Second,
		IdleTimeout:       time.Duration(configuration.IdleTimeoutSeconds) * time.Second,
		MaxHeaderBytes:    configuration.MaxHeaderBytes, //1 << 20 // 1 MB
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

	//////////////////////////////////////// MAX CHANNELS
	connc := make(chan struct{}, configuration.MaximumConnections)
	for i := 0; i < configuration.MaximumConnections; i++ {
		connc <- struct{}{}
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
		proxyOptions := [1][2]string{{"Strict-Transport-Security", "max-age=15768000 ; includeSubDomains"}}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			//TODO: Check certificate in cookie
			select {
			case <-connc:
				//Check API Limit
				if err := check(&configuration, r); err != nil {
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte(API_LIMIT_REACHED))
					return
				}
				//Track
				track(&configuration, w, r)
				//Proxy
				w.Header().Set(proxyOptions[0][0], proxyOptions[0][1])
				proxy.ServeHTTP(w, r)
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		})
	}

	//////////////////////////////////////// STATUS TEST ROUTE
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		json, _ := json.Marshal([2]KeyValue{KeyValue{Key: "client", Value: ip}, KeyValue{Key: "conns", Value: configuration.MaximumConnections - len(connc)}})
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
		select {
		case <-connc:
			track(&configuration, w, r)
			http.StripPrefix(pubSlug, fs).ServeHTTP(w, r)
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	})

	//////////////////////////////////////// 1x1 PIXEL ROUTE
	http.HandleFunc("/img/"+apiVersion+"/", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			track(&configuration, w, r)
			w.Header().Set("content-type", "image/gif")
			w.Write(TRACKING_GIF)
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	})

	//////////////////////////////////////// Tracking Route
	// Ex. https://localhost:8443/tr/v1/vid/accad/ROCK/ON/lat/5/lon/6/first/true/score/6
	// OR
	// {"last":"https://localhost:5001/maps","next":"https://localhost:5001/error/maps/request/unauthorized","params":{"type":"b","origin":"maps","error":"unauthorized","method":"request"},"created":1539072857869,"duration":1959,"vid":"4883a4c0-cb96-11e8-afac-bb666b9727ed","first":"false","sid":"4883cbd0-cb96-11e8-afac-bb666b9727ed"}
	http.HandleFunc("/tr/"+apiVersion+"/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			w.Header().Set("access-control-allow-origin", "*") //TODO Security Threat
			w.Header().Set("access-control-allow-credentials", "true")
			w.Header().Set("access-control-allow-headers", "Authorization,Accept")
			w.Header().Set("access-control-allow-methods", "GET,POST,HEAD,PUT,DELETE")
			w.Header().Set("access-control-max-age", "1728000")
			w.WriteHeader(http.StatusOK)
		} else {
			select {
			case <-connc:
				track(&configuration, w, r)
				w.WriteHeader(http.StatusOK)
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		}

	})

	//////////////////////////////////////// Redirect Route
	// Ex. https://localhost:8443/rdr/v1/?redirect=https%3A%2F%2Fx.com
	http.HandleFunc("/rdr/"+apiVersion+"/", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			track(&configuration, w, r)
			rURL := r.URL.Query()["redirect"]
			fmt.Println(rURL)
			if len(rURL) > 0 {
				http.Redirect(w, r, rURL[0], http.StatusFound)
			} else {
				http.Redirect(w, r, configuration.DefaultRedirect, http.StatusFound)
			}
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}

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
		log.Fatal(server.ListenAndServeTLS(configuration.TLSCert, configuration.TLSKey)) // SERVE HTTPS!
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
func track(c *Configuration, w http.ResponseWriter, r *http.Request) error {
	//Setup
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	wargs := WriteArgs{
		WriteType: WRITE_EVENT,
		Caller:    ip + ";" + r.Header.Get("X-Forwarded-For"),
		IP:        ip,
		Browser:   r.Header.Get("user-agent"),
		Language:  r.Header.Get("accept-language"),
		URI:       r.RequestURI,
	}

	//Process
	j := make(map[string]interface{})
	//Try to get vid from cookie
	cookie, cerr := r.Cookie("vid")
	if cerr == nil {
		j["vid"] = cookie.Value
	}
	//Path
	p := strings.Split(r.URL.Path, "/")
	pmax := (len(p) - 2)
	for i := 1; i <= pmax; i += 2 {
		j[p[i]] = p[i+1] //TODO: Handle arrays
	}
	//Inject Params
	temp := j["vid"]
	delete(j, "vid")
	if params, err := json.Marshal(j); err == nil {
		j["params"] = string(params)
	}
	j["vid"] = temp
	switch r.Method {
	case http.MethodGet:
		//Query, try and get everything
		k := r.URL.Query()
		qp := make(map[string]interface{})
		for idx := range k {
			j[idx] = k[idx][0]
			qp[idx] = k[idx][0]
		}
		if len(qp) > 0 {
			//If we have query params **OVERWRITE** the split URL ones
			if params, err := json.Marshal(qp); err == nil {
				j["params"] = string(params)
			}
		}
		wargs.Values = &j
	case http.MethodPost:
		//Json (POST)
		//This is fully controlled, only send what we need (inc. params)
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
	if vid, ok := j["vid"].(string); ok {
		expiration := time.Now().Add(365 * 24 * time.Hour)
		cookie := http.Cookie{Name: "vid", Value: vid, Expires: expiration, Path: "/"}
		http.SetCookie(w, &cookie)
	}
	return nil

}

////////////////////////////////////////
// FilterUrlPrefix
////////////////////////////////////////
func filterUrlPrefix(c *Configuration, s *string) error {
	matches := urlPrefix.FindStringSubmatch(*s)
	mi := len(matches)
	if mi > 0 {
		*s = matches[mi-1]
	}
	i := strings.Index(*s, "?")
	if i > -1 {
		*s = (*s)[:i]
	}
	return nil
}
