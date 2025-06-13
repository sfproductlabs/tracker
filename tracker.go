/*===----------- tracker.go - tracking utility written in go  -------------===
 *
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2018-2024 Andrew Grosser. All Rights Reserved.
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
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go/service/s3"
	lz4 "github.com/bkaradzic/go-lz4"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/acme/autocert"
)

var upgrader = websocket.Upgrader{
	// Allow all connections for simplicity
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// //////////////////////////////////////
// Get the system setup from the config.json file:
// //////////////////////////////////////
type session interface {
	connect() error
	close() error
	prune() error
	write(w *WriteArgs) error
	listen() error
	serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error
	auth(s *ServiceArgs) error
}

type KeyValue struct {
	Key   string
	Value interface{}
}

type Field struct {
	Type          string
	Id            string
	Default       string
	DestParamHash string
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

type Prune struct {
	Table              string
	TTL                int64
	PageSize           int
	CFlagsIgnore       []int64
	ClearAll           bool
	ClearParams        bool
	ClearNumericParams bool
	Fields             []Field
}

type WriteArgs struct {
	WriteType      int
	Values         *map[string]interface{}
	IsServer       bool
	SaveCookie     bool
	IP             string
	Browser        string
	Language       string
	URI            string
	Host           string
	EventID        uuid.UUID
	CallingService *Service
}

type ServiceArgs struct {
	ServiceType int
	Values      *map[string]string
	IsServer    bool
	IP          string
	Browser     string
	Language    string
	URI         string
	EventID     uuid.UUID
}

type TableType uint64

const (
	// Primary Event Tables
	TABLE_EVENTS        TableType = 1 << iota // 1 << 0  = 1
	TABLE_EVENTS_RECENT                       // 1 << 1  = 2

	// Session/Visitor Tables
	TABLE_VISITORS        // 1 << 2  = 4
	TABLE_VISITORS_LATEST // 1 << 3  = 8
	TABLE_SESSIONS        // 1 << 4  = 16

	// Tracking/Analytics Tables
	TABLE_IPS       // 1 << 5  = 32
	TABLE_ROUTED    // 1 << 6  = 64
	TABLE_HITS      // 1 << 7  = 128
	TABLE_DAILIES   // 1 << 8  = 256
	TABLE_OUTCOMES  // 1 << 9  = 512
	TABLE_REFERRERS // 1 << 10 = 1024
	TABLE_REFERRALS // 1 << 11 = 2048
	TABLE_REFERRED  // 1 << 12 = 4096
	TABLE_HOSTS     // 1 << 13 = 8192
	TABLE_BROWSERS  // 1 << 14 = 16384
	TABLE_NODES     // 1 << 15 = 32768
	TABLE_LOCATIONS // 1 << 16 = 65536
	TABLE_ALIASES   // 1 << 17 = 131072
	TABLE_USERHOSTS // 1 << 18 = 262144
	TABLE_USERNAMES // 1 << 19 = 524288
	TABLE_EMAILS    // 1 << 20 = 1048576
	TABLE_CELLS     // 1 << 21 = 2097152
	TABLE_REQS      // 1 << 22 = 4194304

	// LTV Related Tables
	TABLE_LTV  // 1 << 23 = 8388608
	TABLE_LTVU // 1 << 24 = 16777216
	TABLE_LTVV // 1 << 25 = 33554432

	// Other Tables
	TABLE_AGREEMENTS       // 1 << 26 = 67108864
	TABLE_AGREED           // 1 << 27 = 134217728
	TABLE_JURISDICTIONS    // 1 << 28 = 268435456
	TABLE_REDIRECTS        // 1 << 29 = 536870912
	TABLE_REDIRECT_HISTORY // 1 << 30 = 1073741824
	TABLE_COUNTERS         // 1 << 31 = 2147483648
	TABLE_UPDATES          // 1 << 32 = 4294967296
	TABLE_LOGS             // 1 << 33 = 8589934592

	// Common Table Groups
	TABLE_ALL_EVENTS    = TABLE_EVENTS | TABLE_EVENTS_RECENT
	TABLE_ALL_VISITORS  = TABLE_VISITORS | TABLE_VISITORS_LATEST | TABLE_SESSIONS
	TABLE_ALL_LTV       = TABLE_LTV | TABLE_LTVU | TABLE_LTVV
	TABLE_ALL_REDIRECTS = TABLE_REDIRECTS | TABLE_REDIRECT_HISTORY
	TABLE_ALL           = ^uint64(0)
	TABLE_PROXY_DEFAULT = ^uint64(0) &^ uint64(TABLE_ALL_EVENTS|TABLE_UPDATES|TABLE_LOGS) //Default proxy tables, NO EVENTS, UPDATES, LOGS
)

// Helper methods for bitwise operations
func (t TableType) Has(table TableType) bool {
	return t&table != 0
}

func (t TableType) Add(table TableType) TableType {
	return t | table
}

func (t TableType) Remove(table TableType) TableType {
	return t &^ table
}

// String returns the string representation of the table name
func (t TableType) String() string {
	switch t {
	// Primary Event Tables
	case TABLE_EVENTS:
		return "events"
	case TABLE_EVENTS_RECENT:
		return "events_recent"

	// Session/Visitor Tables
	case TABLE_VISITORS:
		return "visitors"
	case TABLE_VISITORS_LATEST:
		return "visitors_latest"
	case TABLE_SESSIONS:
		return "sessions"

	// Tracking/Analytics Tables
	case TABLE_IPS:
		return "ips"
	case TABLE_ROUTED:
		return "routed"
	case TABLE_HITS:
		return "hits"
	case TABLE_DAILIES:
		return "dailies"
	case TABLE_OUTCOMES:
		return "outcomes"
	case TABLE_REFERRERS:
		return "referrers"
	case TABLE_REFERRALS:
		return "referrals"
	case TABLE_REFERRED:
		return "referred"
	case TABLE_HOSTS:
		return "hosts"
	case TABLE_BROWSERS:
		return "browsers"
	case TABLE_NODES:
		return "nodes"
	case TABLE_LOCATIONS:
		return "locations"
	case TABLE_ALIASES:
		return "aliases"
	case TABLE_USERHOSTS:
		return "userhosts"
	case TABLE_USERNAMES:
		return "usernames"
	case TABLE_EMAILS:
		return "emails"
	case TABLE_CELLS:
		return "cells"
	case TABLE_REQS:
		return "reqs"

	// LTV Related Tables
	case TABLE_LTV:
		return "ltv"
	case TABLE_LTVU:
		return "ltvu"
	case TABLE_LTVV:
		return "ltvv"

	// Other Tables
	case TABLE_AGREEMENTS:
		return "agreements"
	case TABLE_AGREED:
		return "agreed"
	case TABLE_JURISDICTIONS:
		return "jurisdictions"
	case TABLE_REDIRECTS:
		return "redirects"
	case TABLE_REDIRECT_HISTORY:
		return "redirect_history"
	case TABLE_COUNTERS:
		return "counters"
	case TABLE_UPDATES:
		return "updates"
	case TABLE_LOGS:
		return "logs"
	default:
		return "unknown"
	}
}

type Service struct {
	Service  string
	Hosts    []string
	CACert   string
	Cert     string
	Key      string
	Username string
	Password string
	Secure   bool
	Critical bool

	Context      string
	Filter       []Filter
	Prune        []Prune
	Format       string
	MessageLimit int
	ByteLimit    int
	Timeout      time.Duration
	Connections  int
	Retries      int
	AttemptAll   bool

	Consumer  bool
	Ephemeral bool
	Note      string

	Session session

	Skip                              bool
	ProxyRealtimeStorageService       *Service
	ProxyRealtimeStorageServiceName   string
	ProxyRealtimeStorageServiceTables TableType
}

type ClickhouseService struct { //Implements 'session'
	Configuration   *Service
	Session         *clickhouse.Conn
	AppConfig       *Configuration
	circuitBreaker  *CircuitBreaker
	healthTicker    *time.Ticker
	healthDone      chan bool
	batchManager    *BatchManager
	batchingEnabled bool
}

type DuckService struct { //Implements 'session'
	Configuration     *Service
	Session           *sql.DB
	AppConfig         *Configuration
	HealthCheckTicker *time.Ticker
	HealthCheckDone   chan bool
	S3Client          *s3.S3
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

type FacebookService struct { //Implements 'session'
	Configuration *Service
	AppConfig     *Configuration
}

type GeoIP struct {
	IPStart     string  `json:"ips"`
	IPEnd       string  `json:"ipe"`
	CountryISO2 string  `json:"iso2"`
	Country     string  `json:"country"`
	Region      string  `json:"region"`
	City        string  `json:"city"`
	Latitude    float64 `json:"lat"`
	Longitude   float64 `json:"lon"`
	Zip         string  `json:"zip"`
	Timezone    string  `json:"tz"`
}

type Configuration struct {
	ConfigFile               string
	Domains                  []string //Domains in Trust, LetsEncrypt domains
	StaticDirectory          string   //Static FS Directory (./public/)
	TempDirectory            string
	UseGeoIP                 bool
	UseRegionDescriptions    bool
	UseRemoveIP              bool
	GeoIPVersion             int
	IPv4GeoIPZip             string
	IPv6GeoIPZip             string
	IPv4GeoIPCSVDest         string
	IPv6GeoIPCSVDest         string
	UseLocalTLS              bool
	IgnoreInsecureTLS        bool
	TLSCert                  string
	TLSKey                   string
	Notify                   []Service
	Consume                  []Service
	API                      Service
	PrefixPrivateHash        string
	ProxyUrl                 string
	ProxyUrlFilter           string
	IgnoreProxyOptions       bool
	ProxyForceJson           bool
	ProxyPort                string
	ProxyPortTLS             string
	ProxyExceptHTTP          string
	ProxyPortRedirect        string
	ProxyDailyLimit          uint64
	ProxyDailyLimitChecker   string //Service, Ex. casssandra
	ProxyDailyLimitCheck     func(string) uint64
	SchemaVersion            int
	ApiVersion               int
	CFlagsMarketing          int64
	CFlagsIgnore             bool
	Debug                    bool
	UrlFilter                string
	UrlFilterMatchGroup      int
	AllowOrigin              string
	IsUrlFiltered            bool
	MaximumConnections       int
	ReadTimeoutSeconds       int
	ReadHeaderTimeoutSeconds int
	WriteTimeoutSeconds      int
	IdleTimeoutSeconds       int
	MaxHeaderBytes           int
	DefaultRedirect          string
	IgnoreQueryParamsKey     string
	AccountHashMixer         string
	PruneLogsTTL             int
	PruneLogsOnly            bool
	PruneLogsSkip            bool
	PruneLogsPageSize        int
	PruneUpdateConfig        bool
	PruneLimit               int
	PruneSkipToTimestamp     int64
	S3Bucket                 string
	S3Prefix                 string
	S3Region                 string
	S3AccessKeyID            string
	S3SecretAccessKey        string
	HealthCheckInterval      int
	NodeId                   string
	InactivityTimeoutSeconds int
	MaxShardSizeBytes        int64
}

// ////////////////////////////////////// Constants
const (
	PONG              string = "pong"
	API_LIMIT_REACHED string = "API Limit Reached"

	SERVICE_TYPE_CLICKHOUSE string = "clickhouse"
	SERVICE_TYPE_NATS       string = "nats"
	SERVICE_TYPE_FACEBOOK   string = "facebook"
	SERVICE_TYPE_DUCKDB     string = "duckdb"
	SERVICE_TYPE_CASSANDRA  string = "cassandra"

	FB_PIXEL string = "FB_PIXEL"
	FB_TOKEN string = "FB_TOKEN"

	NATS_QUEUE_GROUP = "tracker"

	IDX_PREFIX_IPV4 = "gip4::"
	IDX_PREFIX_IPV6 = "gip6::"
)
const (
	WRITE_LOG    = 1 << iota
	WRITE_UPDATE = 1 << iota
	WRITE_COUNT  = 1 << iota
	WRITE_EVENT  = 1 << iota
	WRITE_LTV    = 1 << iota
	WRITE_ALL    = ^uint64(0)

	WRITE_DESC_LOG    = "log"
	WRITE_DESC_UPDATE = "update"
	WRITE_DESC_COUNT  = "count"
	WRITE_DESC_EVENT  = "event"
)

const (
	SVC_GET_REDIRECTS     = 1 << iota
	SVC_POST_REDIRECT     = 1 << iota
	SVC_GET_REDIRECT      = 1 << iota
	SVC_GET_AGREE         = 1 << iota
	SVC_POST_AGREE        = 1 << iota
	SVC_GET_JURISDICTIONS = 1 << iota
	SVC_GET_GEOIP         = 1 << iota

	SVC_DESC_GET_REDIRECTS     = "getRedirects"
	SVC_DESC_POST_REDIRECT     = "postRedirect"
	SVC_DESC_GET_REDIRECT      = "getRedirect"
	SVC_DESC_GET_AGREE         = "getAgreememts"
	SVC_DESC_POST_AGREE        = "postAgreements"
	SVC_DESC_GET_JURISDICTIONS = "getJurisdictions"
)

var (
	// Quote Ident replacer.
	regexQiReplacer  = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
	regexCount       = regexp.MustCompile(`\.count\.(.*)`)
	regexUpdate      = regexp.MustCompile(`\.update\.(.*)`)
	regexFilterUrl   = regexp.MustCompile(`(.*)`)
	regexInternalURI = regexp.MustCompile(`.*(/tr/|/img/|/pub/|/str/|/rdr/)v[0-9]+.*`) //TODO: MUST FILTER INTERNAL ROUTES, UPDATE IF ADDING A NEW ROUTE, PROXY OK!!!
	regexUtmPrefix   = regexp.MustCompile(`utm_`)
)

// ////////////////////////////////////// Transparent GIF
var TRACKING_GIF = []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, 0x80, 0x0, 0x0, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b}

var kv = (*KV)(nil)

// //////////////////////////////////////
// Start here
// //////////////////////////////////////
func main() {
	fmt.Println("\n\n//////////////////////////////////////////////////////////////")
	fmt.Println("Tracker.")
	fmt.Println("User telemetry software")
	fmt.Println("https://github.com/sfproductlabs/tracker")
	fmt.Println("(c) Copyright 2018-2021 SF Product Labs LLC.")
	fmt.Println("Use of this software is subject to the LICENSE agreement.")
	fmt.Println("//////////////////////////////////////////////////////////////\n\n")

	//////////////////////////////////////// LOAD CONFIG
	fmt.Println("Starting services...")
	configFile := "config.json"
	var fbPixelFlag = flag.String("fb-pixel", "", "provide facebook pixel")
	var fbTokenFlag = flag.String("fb-token", "", "provide facebook token")
	var prune = flag.Bool("prune", false, "prune items")
	var logsOnly = flag.Bool("logs-only", false, "clear out log only")
	flag.Parse()
	if len(flag.Args()) > 0 {
		configFile = flag.Args()[0]
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
	configuration.ConfigFile = configFile
	configuration.NodeId = uuid.New().String()

	//////////////////////////////////////// OVERRIDE FACEBOOK VARIABLES
	var fbPixel = os.Getenv(FB_PIXEL)
	var fbToken = os.Getenv(FB_TOKEN)
	if fbPixel == "" {
		fbPixel = *fbPixelFlag
	}
	if fbToken == "" {
		fbToken = *fbTokenFlag
	}
	if fbPixel != "" || fbToken != "" {
		var fbNotify = &Service{}
		var fbFound = false
		for idx := range configuration.Notify {
			s := &configuration.Notify[idx]
			if s.Service == SERVICE_TYPE_FACEBOOK {
				fbNotify = s
				fbFound = true
			}
		}
		if fbPixel != "" {
			fbNotify.Context = fbPixel
		}
		if fbToken != "" {
			fbNotify.Key = fbToken
		}
		fbNotify.Service = SERVICE_TYPE_FACEBOOK
		if !fbFound {
			fbNotify.AttemptAll = false
			configuration.Notify = append(configuration.Notify, *fbNotify)
		}
	}

	//////////////////////////////////////// SETUP CACHE
	cache := cacheDir()
	if cache == "" {
		log.Fatal("Bad Cache.")
	}

	//////////////////////////////////////// Prime rand
	//Setup rand
	rand.Seed(time.Now().UTC().UnixNano())

	////////////////////////////////////////SETUP FILTER
	if configuration.UrlFilter != "" {
		fmt.Println("Setting up URL prefix filter...")
		configuration.IsUrlFiltered = true
		regexFilterUrl, _ = regexp.Compile(configuration.UrlFilter)
	}

	////////////////////////////////////////SETUP ORIGIN
	if configuration.AllowOrigin == "" {
		configuration.AllowOrigin = "*"
	}

	if configuration.MaxShardSizeBytes == 0 {
		configuration.MaxShardSizeBytes = 100 * 1024 * 1024 //100MB
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
	// allow redirect target port to be different than listening port (443 vs. 8443)
	proxyPortRedirect := proxyPortTLS
	if configuration.ProxyPortRedirect != "" {
		proxyPortRedirect = configuration.ProxyPortRedirect
	}
	//////////////////////////////////////// LOAD NOTIFIERS
	notification_services := make(map[string]*Service)
	for idx := range configuration.Notify {
		s := &configuration.Notify[idx]
		notification_services[s.Service] = s
		switch s.Service {
		case SERVICE_TYPE_CLICKHOUSE:
			fmt.Printf("Notify #%d: Connecting to ClickHouse: %s\n", idx, s.Hosts)
			clickhouse := ClickhouseService{
				Configuration: s,
				AppConfig:     &configuration,
			}
			err = clickhouse.connect()
			if err != nil || s.Session == nil {
				if s.Critical {
					log.Fatalf("[CRITICAL] Notify #%d. Could not connect to ClickHouse. %s\n", idx, err)
				} else {
					fmt.Printf("[ERROR] Notify #%d. Could not connect to ClickHouse. %s\n", idx, err)
					continue
				}
			}
			//Now attach the one and only API service, replace if multiple
			if !s.Skip {
				configuration.API = *s
			}
		case SERVICE_TYPE_DUCKDB:
			fmt.Printf("Notify #%d: Connecting to DuckDB: %s\n", idx, s.Hosts)
			duck := DuckService{
				Configuration: s,
				AppConfig:     &configuration,
			}
			err = duck.connect()
			if err != nil || s.Session == nil {
				if s.Critical {
					log.Fatalf("[CRITICAL] Notify #%d. Could not connect to duck. %s\n", idx, err)
				} else {
					fmt.Printf("[ERROR] Notify #%d. Could not connect to duck. %s\n", idx, err)
					continue
				}
			}
			//Now attach the one and only API service, replace if multiple
			if !s.Skip {
				configuration.API = *s
			}
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
			//Now attach the one and only API service, replace if multiple
			if !s.Skip {
				configuration.API = *s
			}
		case SERVICE_TYPE_NATS:
			//TODO:
			fmt.Printf("[ERROR] Notify #%d: NATS notifier not implemented\n", idx)
		case SERVICE_TYPE_FACEBOOK:
			facebook := FacebookService{
				Configuration: s,
				AppConfig:     &configuration,
			}
			err = facebook.connect()
			if s.Critical && (s.Context == "" || s.Key == "" || err != nil) {
				log.Fatalf("[CRITICAL] Notify #%d. Could not setup connection to Facebook CAPI.\n", idx)
			}
			if !s.Skip {
				configuration.API = *s
			}
			fmt.Printf("Notify #%d: Facebook CAPI configured for events\n", idx)
		default:
			fmt.Printf("[ERROR] %s #%d Notifier not implemented\n", s.Service, idx)
		}
	}
	//////////////////////////////////////// SETUP NOTIFICATION PROXIES
	for _, s := range notification_services {
		if s.ProxyRealtimeStorageServiceName != "" {
			s.ProxyRealtimeStorageService = notification_services[s.ProxyRealtimeStorageServiceName]
		}
	}

	//////////////////////////////////////// LETS JUST PRUNE AND QUIT?
	if *prune {
		for idx := range configuration.Notify {
			s := &configuration.Notify[idx]
			if s.Session != nil {
				switch s.Service {
				case SERVICE_TYPE_CASSANDRA:
					configuration.PruneLogsOnly = *logsOnly || configuration.PruneLogsOnly
					err := s.Session.prune()
					if err != nil {
						fmt.Println("\nLast prune error...\n", err)
					}
				default:
					fmt.Println("[ERROR]")
				}

			}
		}
		os.Exit(0)
		return
	} else {
		//////////////////////////////////////// LOAD CONSUMERS
		for idx := range configuration.Consume {
			s := &configuration.Consume[idx]
			switch s.Service {
			case SERVICE_TYPE_CASSANDRA:
				fmt.Printf("[ERROR] Consume #%d: Cassandra consumer not implemented\n", idx)
			case SERVICE_TYPE_DUCKDB:
				fmt.Printf("[ERROR] Consume #%d: DuckDB consumer not implemented\n", idx)
			case SERVICE_TYPE_CLICKHOUSE:
				fmt.Printf("[ERROR] Consume #%d: ClickHouse consumer not implemented\n", idx)
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
			case SERVICE_TYPE_FACEBOOK:
				fmt.Printf("[ERROR] Consume #%d: Facebook consumer not implemented\n", idx)
			default:
				fmt.Printf("[ERROR] %s #%d Consumer not implemented\n", s.Service, idx)
			}

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
			InsecureSkipVerify:       configuration.IgnoreInsecureTLS,
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

	//////////////////////////////////////// WEBSOCKET
	http.HandleFunc("/tr/"+apiVersion+"/ws", func(w http.ResponseWriter, r *http.Request) {

		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			handlePreflight(&w, &configuration.AllowOrigin)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Println("Error upgrading to WebSocket:", err)
			return
		}
		defer conn.Close()

		wargs := WriteArgs{
			WriteType: WRITE_EVENT,
			IP:        getIP(r),
			Browser:   r.Header.Get("user-agent"),
			Language:  r.Header.Get("accept-language"),
			URI:       r.RequestURI,
			Host:      getHost(r),
			IsServer:  false,
		}

		for {
			// Read message
			messageType, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Printf("WebSocket error: %v\n", err)
				}
				break
			}

			var data map[string]interface{}
			// Handle different message types
			switch messageType {
			case websocket.TextMessage:
				// Handle uncompressed JSON
				if err := json.Unmarshal(msg, &data); err != nil {
					fmt.Printf("Error parsing JSON: %v\n", err)
					continue
				}
			case websocket.BinaryMessage:
				// Decompress the message
				decompressed, err := lz4.Decode(nil, msg)
				if err != nil {
					fmt.Printf("Error decompressing message: %v\n", err)
					continue
				}

				// Parse JSON
				if err := json.Unmarshal(decompressed, &data); err != nil {
					fmt.Printf("Error parsing JSON: %v\n", err)
					continue
				}
			}
			wargs.EventID = uuid.Must(uuid.NewUUID())
			wargs.Values = &data
			trackWithArgs(&configuration, &w, r, &wargs)
		}
	})

	//////////////////////////////////////// REDIRECT URL & SHORTENER
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			sargs := ServiceArgs{
				ServiceType: SVC_GET_REDIRECT,
			}
			if err = serveWithArgs(&configuration, &w, r, &sargs); err != nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(err.Error()))
			} else {
				values := make(map[string]interface{})
				values["etyp"] = "redirect"
				values["ename"] = "short_rdr"
				values["last"] = r.RequestURI
				wargs := WriteArgs{
					WriteType: WRITE_EVENT,
					IP:        getIP(r),
					Browser:   r.Header.Get("user-agent"),
					Language:  r.Header.Get("accept-language"),
					EventID:   uuid.Must(uuid.NewUUID()),
					URI:       r.RequestURI,
					Host:      getHost(r),
					IsServer:  false,
					Values:    &values,
				}
				trackWithArgs(&configuration, &w, r, &wargs)
			}
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	})

	//////////////////////////////////////// PROXY API ROUTES
	if configuration.ProxyUrl != "" {
		fmt.Println("Proxying to:", configuration.ProxyUrl)
		origin, _ := url.Parse(configuration.ProxyUrl)
		director := func(req *http.Request) {
			req.Header.Add("X-Forwarded-Host", req.Host)
			req.Header.Add("X-Origin-Host", origin.Host)
			if configuration.ProxyForceJson {
				req.Header.Set("content-type", "application/json")
			}
			req.URL.Scheme = "http"
			req.URL.Host = origin.Host
		}
		proxy := &httputil.ReverseProxy{Director: director}
		proxyFilter, _ := regexp.Compile(configuration.ProxyUrlFilter)
		http.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
			if !configuration.IgnoreProxyOptions && r.Method == http.MethodOptions {
				//Lets just allow requests to this endpoint
				handlePreflight(&w, &configuration.AllowOrigin)
				return
			}
			//TODO: Check certificate in cookie
			select {
			case <-connc:
				//Check API Limit
				if err := check(&configuration, r); err != nil {
					w.WriteHeader(http.StatusTooManyRequests)
					w.Write([]byte(API_LIMIT_REACHED))
					return
				}
				//Proxy
				w.Header().Set("Strict-Transport-Security", "max-age=15768000 ; includeSubDomains")
				w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
				proxy.ServeHTTP(w, r)
				//Track
				if configuration.ProxyUrlFilter != "" && !proxyFilter.MatchString(r.URL.Path) {
					track(&configuration, &w, r)
				}
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		})
	}

	//////////////////////////////////////// STATUS TEST ROUTE
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		json, _ := json.Marshal([2]KeyValue{KeyValue{Key: "client", Value: getIP(r)}, KeyValue{Key: "conns", Value: configuration.MaximumConnections - len(connc)}})
		w.WriteHeader(http.StatusOK)
		w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
		w.Header().Set("Content-Type", "application/json")
		w.Write(json)
	})

	//////////////////////////////////////// PING PONG TEST ROUTE
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
		w.Write([]byte(PONG))
	})

	//////////////////////////////////////// CLEAR ROUTE
	http.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
		w.Header().Set("clear-site-data", "\"*\"")
		w.Write([]byte("OK"))
	})

	//////////////////////////////////////// STATIC CONTENT ROUTE
	fmt.Println("Serving static content in:", configuration.StaticDirectory)
	fs := http.FileServer(http.Dir(configuration.StaticDirectory))
	pubSlug := "/tr/" + apiVersion + "/pub/"
	http.HandleFunc(pubSlug, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			track(&configuration, &w, r)
			http.StripPrefix(pubSlug, fs).ServeHTTP(w, r)
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	})

	//////////////////////////////////////// 1x1 PIXEL ROUTE
	http.HandleFunc("/tr/"+apiVersion+"/img/", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			track(&configuration, &w, r)
			w.Header().Set("content-type", "image/gif")
			w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
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
	http.HandleFunc("/tr/"+apiVersion+"/tr/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			handlePreflight(&w, &configuration.AllowOrigin)
			return
		} else {
			select {
			case <-connc:
				track(&configuration, &w, r)
				w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
				w.WriteHeader(http.StatusOK)
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		}

	})

	//////////////////////////////////////// Track Lifetime Value
	http.HandleFunc("/tr/"+apiVersion+"/ltv/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			handlePreflight(&w, &configuration.AllowOrigin)
			return
		} else {
			select {
			case <-connc:
				ltv(&configuration, &w, r)
				w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
				w.WriteHeader(http.StatusOK)
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		}

	})

	//////////////////////////////////////// Server Tracking Route
	http.HandleFunc("/tr/"+apiVersion+"/str/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			handlePreflight(&w, &configuration.AllowOrigin)
			return
		} else {
			select {
			case <-connc:
				wargs := WriteArgs{
					WriteType: WRITE_EVENT,
					IP:        getIP(r),
					EventID:   uuid.Must(uuid.NewUUID()),
					URI:       r.RequestURI,
					Host:      getHost(r),
					IsServer:  true,
				}
				trackWithArgs(&configuration, &w, r, &wargs)
				w.WriteHeader(http.StatusOK)
				w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
				json, _ := json.Marshal(wargs.EventID)
				w.Header().Set("Content-Type", "application/json")
				w.Write(json)
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		}
	})

	//////////////////////////////////////// Redirect Route
	// Ex. https://localhost:8443/tr/v1/rdr/?r=https%3A%2F%2Fx.com
	http.HandleFunc("/tr/"+apiVersion+"/rdr/", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			track(&configuration, &w, r)
			rURL := r.URL.Query()["url"]
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

	//////////////////////////////////////// Privacy Routes
	if configuration.UseGeoIP {
		kv, _ = NewKVStore(LogDBConfig{
			expert: ExpertConfig{
				ExecShards:  defaultExecShards,
				LogDBShards: defaultLogDBShards,
			},
			KVMaxBackgroundCompactions:         2,
			KVMaxBackgroundFlushes:             2,
			KVLRUCacheSize:                     0,
			KVKeepLogFileNum:                   16,
			KVWriteBufferSize:                  128 * 1024 * 1024,
			KVMaxWriteBufferNumber:             4,
			KVLevel0FileNumCompactionTrigger:   8,
			KVLevel0SlowdownWritesTrigger:      17,
			KVLevel0StopWritesTrigger:          24,
			KVMaxBytesForLevelBase:             4 * 1024 * 1024 * 1024,
			KVMaxBytesForLevelMultiplier:       2,
			KVTargetFileSizeBase:               16 * 1024 * 1024,
			KVTargetFileSizeMultiplier:         2,
			KVLevelCompactionDynamicLevelBytes: 0,
			KVRecycleLogFileNum:                0,
			KVNumOfLevels:                      7,
			KVBlockSize:                        32 * 1024,
		}, func(busy bool) { fmt.Println("DB Busy", busy) }, "./pdb", "./pdbwal")

		kv.GetValue([]byte("DB_VER"), func(val []byte) error {
			if len(val) == 0 || val[0] != byte(configuration.GeoIPVersion) {
				fmt.Println("Restoring Geoip Database...")
				Unzip(configuration.IPv4GeoIPZip, configuration.TempDirectory)
				Unzip(configuration.IPv6GeoIPZip, configuration.TempDirectory)
				wb := kv.GetWriteBatch()
				i := 0
				load := func(src string, keyprefix string, pad int) {
					file, _ := os.Open(src)
					defer file.Close()
					r := csv.NewReader(file)
					for {
						i++
						rec, err := r.Read()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Fatal(err)
						}
						if rec[0] == "" || rec[1] == "" || rec[0] == "-" || rec[1] == "-" {
							continue
						}
						for g := 0; g < len(rec); g++ {
							if rec[g] == "-" {
								rec[g] = ""
							}
						}
						//Ipv4 has 10 decimal places
						//Ipv6 has 39 decimal places
						lat, _ := strconv.ParseFloat(rec[6], 64)
						lon, _ := strconv.ParseFloat(rec[7], 64)
						geoip := GeoIP{
							IPStart:     rec[0],
							IPEnd:       rec[1],
							CountryISO2: rec[2],
							Country:     rec[3],
							Region:      rec[4],
							City:        rec[5],
							Latitude:    lat,
							Longitude:   lon,
							Zip:         rec[8],
							Timezone:    rec[9],
						}
						if i%100000 == 0 {
							fmt.Print(".")
							kv.CommitWriteBatch(wb)
							wb.Clear()
						}
						js, err := json.Marshal(geoip)
						wb.Put([]byte(keyprefix+FixedLengthNumberString(pad, rec[0])), js)
					}
					kv.CommitWriteBatch(wb)
					wb.wb.Close()
				}
				load(configuration.TempDirectory+configuration.IPv4GeoIPCSVDest, IDX_PREFIX_IPV4, 10)
				load(configuration.TempDirectory+configuration.IPv6GeoIPCSVDest, IDX_PREFIX_IPV6, 39)
				kv.SaveValue([]byte("DB_VER"), []byte{byte(configuration.GeoIPVersion)})
			}
			return nil
		})
	}

	/// Privacy program interface (cookies)
	ctr := mux.NewRouter()
	ctr.HandleFunc("/tr/"+apiVersion+"/ppi/{action}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			//Lets just allow requests to this endpoint
			handlePreflight(&w, &configuration.AllowOrigin)
			return
		} else {
			select {
			case <-connc:
				params := mux.Vars(r)
				sargs := ServiceArgs{}
				w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
				switch params["action"] {
				case "agree": //agreements
					if r.Method == http.MethodPost {
						sargs.ServiceType = SVC_POST_AGREE
					} else {
						sargs.ServiceType = SVC_GET_AGREE
					}
				case "jds": //jurisdictions
					sargs.ServiceType = SVC_GET_JURISDICTIONS
				case "geoip": //geoip
					sargs.ServiceType = SVC_GET_GEOIP
				default:
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte("Unknown action"))
				}
				if sargs.ServiceType != 0 {
					if err = serveWithArgs(&configuration, &w, r, &sargs); err != nil {
						w.WriteHeader(http.StatusBadRequest)
						w.Write([]byte(err.Error()))
					}
				}
				connc <- struct{}{}
			default:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
			}
		}
	})
	http.Handle("/tr/"+apiVersion+"/ppi/", ctr)

	//////////////////////////////////////// Redirect API Route & Functions
	rtr := mux.NewRouter()
	rtr.HandleFunc("/tr/"+apiVersion+"/rpi/{_dummy:.*}", func(w http.ResponseWriter, r *http.Request) {
		handlePreflight(&w, &configuration.AllowOrigin)
	}).Methods("OPTIONS")
	rtr.HandleFunc("/tr/"+apiVersion+"/rpi/redirects/{uid}/{password}/{host}", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			params := mux.Vars(r)
			sargs := ServiceArgs{
				ServiceType: SVC_GET_REDIRECTS,
				Values:      &params,
			}
			w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
			if err = serveWithArgs(&configuration, &w, r, &sargs); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
			}
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	}).Methods("GET")
	rtr.HandleFunc("/tr/"+apiVersion+"/rpi/redirect/{uid}/{password}", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-connc:
			params := mux.Vars(r)
			sargs := ServiceArgs{
				ServiceType: SVC_POST_REDIRECT,
				Values:      &params,
			}
			w.Header().Set("access-control-allow-origin", configuration.AllowOrigin)
			if err = serveWithArgs(&configuration, &w, r, &sargs); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
			}
			connc <- struct{}{}
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Maximum clients reached on this node.", http.StatusServiceUnavailable)
		}
	}).Methods("POST")
	http.Handle("/tr/"+apiVersion+"/rpi/", rtr)

	//////////////////////////////////////// SERVE, REDIRECT AUTO to HTTPS
	go func() {
		fmt.Printf("Serving HTTP Redirect from %s to %s\n", proxyPort, proxyPortRedirect)
		if configuration.UseLocalTLS {
			http.ListenAndServe(proxyPort, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				http.Redirect(w, req, "https://"+getHost(req)+proxyPortRedirect+req.RequestURI, http.StatusFound)
			}))

		} else {
			http.ListenAndServe(proxyPort, certManager.HTTPHandler(nil))
		}

	}()
	go func() {
		if configuration.ProxyExceptHTTP != "" {
			fmt.Printf("Serving HTTP on %s\n", configuration.ProxyExceptHTTP)
			http.ListenAndServe(configuration.ProxyExceptHTTP, nil)
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

func handlePreflight(w *http.ResponseWriter, allowOrigin *string) {
	writer := *w
	writer.Header().Set("access-control-allow-origin", *allowOrigin)
	writer.Header().Set("access-control-allow-credentials", "true")
	writer.Header().Set("access-control-allow-headers", "Authorization,Accept,User")
	writer.Header().Set("access-control-allow-methods", "GET,POST,HEAD,PUT,DELETE")
	writer.Header().Set("access-control-max-age", "1728000")
	writer.WriteHeader(http.StatusOK)
}

// //////////////////////////////////////
// Serve APIs
// //////////////////////////////////////
func serveWithArgs(c *Configuration, w *http.ResponseWriter, r *http.Request, args *ServiceArgs) error {
	s := &c.API
	if s != nil && s.Session != nil {
		if err := s.Session.serve(w, r, args); err != nil {
			if c.Debug {
				fmt.Printf("[ERROR] Serving to %s: %s %s\n", s.Service, err, r.RequestURI)
			}
			return err
		}
	}
	return nil
}

// //////////////////////////////////////
// Check
// //////////////////////////////////////
func check(c *Configuration, r *http.Request) error {
	//Precheck
	if c.ProxyDailyLimit > 0 && c.ProxyDailyLimitCheck != nil && c.ProxyDailyLimitCheck(getIP(r)) > c.ProxyDailyLimit {
		return fmt.Errorf("API Limit Reached")
	}
	return nil
}

// //////////////////////////////////////
// Total Lifetime Value
// //////////////////////////////////////
func ltv(c *Configuration, w *http.ResponseWriter, r *http.Request) error {
	//Setup
	wargs := WriteArgs{
		WriteType: WRITE_LTV,
		IP:        getIP(r),
		Browser:   r.Header.Get("user-agent"),
		Language:  r.Header.Get("accept-language"),
		URI:       r.RequestURI,
		Host:      getHost(r),
		EventID:   uuid.Must(uuid.NewUUID()),
	}
	return trackWithArgs(c, w, r, &wargs)
}

// //////////////////////////////////////
// Telemetry
// //////////////////////////////////////
func track(c *Configuration, w *http.ResponseWriter, r *http.Request) error {
	//Setup
	wargs := WriteArgs{
		WriteType: WRITE_EVENT,
		IP:        getIP(r),
		Browser:   r.Header.Get("user-agent"),
		Language:  r.Header.Get("accept-language"),
		URI:       r.RequestURI,
		Host:      getHost(r),
		EventID:   uuid.Must(uuid.NewUUID()),
	}
	return trackWithArgs(c, w, r, &wargs)
}

func trackWithArgs(c *Configuration, w *http.ResponseWriter, r *http.Request, wargs *WriteArgs) error {
	//Normalize all data TOLOWERCASE

	//Process
	j := make(map[string]interface{})

	//Try to get user from header or user cookie
	userHeader := r.Header.Get("User")
	if userHeader != "" {
		json.Unmarshal([]byte(userHeader), &j)
	} else if cookie, cerr := r.Cookie("user"); cerr != nil && cookie != nil {
		json.Unmarshal([]byte(cookie.Value), &j)
	}

	//Try to get vid from cookie
	cookie, cerr := r.Cookie("vid")
	if cerr == nil && cookie != nil {
		j["vid"] = cookie.Value
	}
	//Try to get CookieConsent from cookie (cflags)
	cookie, cerr = r.Cookie("CookieConsent")
	if cerr == nil && cookie != nil {
		if cflags, err := strconv.ParseInt(cookie.Value, 10, 64); err == nil {
			j["cflags"] = cflags
		}
	}
	//Try to get EmailHash from cookie (ehash)
	cookie, cerr = r.Cookie("ehash")
	if cerr == nil && cookie != nil {
		j["ehash"] = cookie.Value
	}
	//Try to get sid from cookie
	cookie, cerr = r.Cookie("sid")
	if cerr == nil && cookie != nil {
		j["sid"] = cookie.Value
	}
	//Path
	p := strings.Split(r.URL.Path, "/")
	pmax := (len(p) - 2)
	for i := 2; i <= pmax; i += 2 {
		p[i] = strings.ToLower(p[i])
		switch p[i] {
		case "ehash", "bhash":
			j[p[i]] = p[i+1] //TODO: Handle arrays
			break
		default:
			j[p[i]] = strings.ToLower(p[i+1]) //TODO: Handle arrays
		}
	}
	//Merge wargs.Values into j
	if wargs.Values != nil {
		// Deep merge wargs.Values into j
		for k, v := range *wargs.Values {
			if vm, ok := v.(map[string]interface{}); ok {
				// If value is a nested map, merge recursively
				if jm, exists := j[k].(map[string]interface{}); exists {
					for mk, mv := range vm {
						jm[mk] = mv
					}
				} else {
					j[k] = vm
				}
			} else {
				// For non-map values, overwrite directly
				j[k] = v
			}
		}
	}

	//Inject Params
	// Convert j to params string
	if params, err := json.Marshal(j); err == nil {
		paramsStr := strings.ToLower(string(params))

		// Check if params already exists
		if existingParams, ok := j["params"].(string); ok {
			// Unmarshal existing params
			var existingMap map[string]interface{}
			if err := json.Unmarshal([]byte(existingParams), &existingMap); err == nil {
				// Unmarshal new params
				var newMap map[string]interface{}
				if err := json.Unmarshal([]byte(paramsStr), &newMap); err == nil {
					// Deep merge new params into existing
					for k, v := range newMap {
						existingMap[k] = v
					}
					// Marshal merged result back to string
					if mergedParams, err := json.Marshal(existingMap); err == nil {
						j["params"] = strings.ToLower(string(mergedParams))
					}
				}
			}
		} else {
			// No existing params, just set new ones
			j["params"] = paramsStr
		}
	}

	wargs.Values = &j
	switch r.Method {
	case http.MethodGet:
		//Query, try and get everything
		k := r.URL.Query()
		if c.IgnoreQueryParamsKey != "" && k[c.IgnoreQueryParamsKey] != nil {
			break
		}
		qp := make(map[string]interface{})
		for idx := range k {
			lidx := strings.ToLower(idx)
			lidx = regexUtmPrefix.ReplaceAllString(lidx, "")
			switch lidx {
			case "ehash", "bhash":
				j[lidx] = k[idx][0]  //TODO: Handle arrays
				qp[lidx] = k[idx][0] //TODO: Handle arrays
			default:
				j[lidx] = strings.ToLower(k[idx][0])  //TODO: Handle arrays
				qp[lidx] = strings.ToLower(k[idx][0]) //TODO: Handle arrays
			}

		}
		if len(qp) > 0 {
			//If we have query params merge them into existing params
			if existingParams, ok := j["params"].(string); ok {
				var existingMap map[string]interface{}
				if err := json.Unmarshal([]byte(existingParams), &existingMap); err == nil {
					//Merge qp into existingMap
					for k, v := range qp {
						existingMap[k] = v
					}
					if params, err := json.Marshal(existingMap); err == nil {
						j["params"] = strings.ToLower(string(params))
					}
				}
			} else if params, err := json.Marshal(qp); err == nil {
				j["params"] = strings.ToLower(string(params))
			}
		}
		break
	case http.MethodPut:
	case http.MethodPost:
		//Json (POST)
		//This is fully controlled, only send what we need (inc. params)
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("Bad JS (body)")
		}
		if len(body) > 0 {
			//r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
			// for idx := range body {
			// 	body[idx] = byte(unicode.ToLower(rune(body[idx])))
			// }
			b := make(map[string]interface{})
			if err := json.Unmarshal(body, &b); err == nil {
				for bpi := range b {
					lbpi := strings.ToLower(bpi)
					switch lbpi {
					case "ehash", "bhash":
						j[lbpi] = b[bpi]
					default:
						if bpiv, ok := b[bpi].(string); ok {
							j[lbpi] = strings.ToLower(bpiv)
						} else if b[bpi] != nil {
							j[lbpi] = b[bpi]
						}
					}
				}
			}
		}
		break
	default:
		return nil
	}
	_, okc := j["content"].(string)
	_, oke := j["ename"].(string)
	if okc && !oke {
		j["ename"] = j["content"]
		delete(j, "content")
	}
	if wargs.Host == "" {
		wargs.Host = getHost(r)
	}
	if c.Debug {
		fmt.Printf("[EVENT] %s\n", wargs)
	}
	for idx := range c.Notify {
		s := &c.Notify[idx]
		if s.Skip {
			continue
		}
		if s.Session != nil {
			if err := s.Session.write(wargs); err != nil {
				if c.Debug {
					fmt.Printf("[ERROR] Writing to %s: %s\n", s.Service, err)
				}
				return err
			}
		}
	}
	if !wargs.IsServer && wargs.SaveCookie {
		var dom string
		host := getHost(r)
		if net.ParseIP(host) == nil {
			ha := strings.Split(strings.ToLower(host), ".")
			dom = ha[len(ha)-1]
			if len(ha) > 1 {
				dom = ha[len(ha)-2] + "." + dom
			}
		}
		if vid, ok := j["vid"].(string); ok {
			expiration := time.Now().UTC().Add(99999 * 24 * time.Hour)
			cookie := http.Cookie{Name: "vid", Value: vid, Expires: expiration, Path: "/", Domain: dom}
			http.SetCookie(*w, &cookie)
		} else if vid, ok := j["vid"].(uuid.UUID); ok {
			expiration := time.Now().UTC().Add(99999 * 24 * time.Hour)
			cookie := http.Cookie{Name: "vid", Value: vid.String(), Expires: expiration, Path: "/", Domain: dom}
			http.SetCookie(*w, &cookie)
		}
	}
	return nil

}
