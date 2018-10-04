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
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
	"golang.org/x/crypto/acme/autocert"
)

var (
	// Quote Ident replacer.
	qiReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

////////////////////////////////////////
// Get the system setup from the config.json file:
////////////////////////////////////////
type session interface {
	connect() error
	close() error
	write(r *http.Request) error
	listen() error
}

type Field struct {
	Type string
	Id   string
}

type Filter struct {
	Type   string
	Alias  string
	Id     string
	Fields []Field
}

type Service struct {
	Service string
	Hosts   []string
	CACert  string
	Cert    string
	Key     string
	Secure  bool

	Context      string
	Filter       []Filter
	Retry        bool
	Format       string
	MessageLimit int
	ByteLimit    int

	Session   session
	Consumer  bool
	Ephemeral bool
}

type CassandraService struct { //Implements 'session'
	Configuration *Service
	Session       *gocql.Session
}

type NatsService struct { //Implements 'session'
	Configuration *Service
	nc            *nats.Conn
	ec            *nats.EncodedConn
}

type Configuration struct {
	Domains         []string //Domains in Trust, LetsEncrypt domains
	StaticDirectory string   //Static FS Directory (./public/)
	UseLocalTLS     bool
	Notify          []Service
	Consume         []Service
	ProxyUrl        string
}

//////////////////////////////////////// PING-PONG return string
const PONG string = "pong"

//////////////////////////////////////// Transparent GIF
var TRACKING_GIF = []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, 0x80, 0x0, 0x0, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b}

////////////////////////////////////////
// Start here
////////////////////////////////////////
func main() {

	//////////////////////////////////////// SETUP CACHE
	cache := cacheDir()
	if cache == "" {
		log.Fatal("Bad Cache.")
	}

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
	fmt.Println("Trusted domains: ", configuration.Domains)

	//////////////////////////////////////// LOAD NOTIFIERS
	fmt.Println("Connecting to Cassandra Cluster: ", configuration.Notify[0].Hosts)
	cassandra := CassandraService{
		Configuration: &configuration.Notify[0],
	}
	err = cassandra.connect()
	if err != nil || configuration.Notify[0].Session == nil {
		panic("Could not connect to Cassandra Cluster.")
	}
	//qid, err := gocql.ParseUUID("00000000-0000-0000-0000-000000000000")
	var seq int
	if err := cassandra.Session.Query(`SELECT seq FROM sequences where name='DB_VER' LIMIT 1`).Consistency(gocql.One).Scan(&seq); err != nil {
		fmt.Println("[ERROR] Bad DB_VER", err)
		panic("[ERROR] Could not connect to Cassandra")
	} else {
		fmt.Println("Connected to Cassandra: DB_VER ", seq)
	}

	//////////////////////////////////////// LOAD RECEIVERS
	gonats := NatsService{
		Configuration: &configuration.Consume[0],
	}
	err = gonats.connect()
	if err != nil || configuration.Consume[0].Session == nil {
		panic("[ERROR] Could not connect to Nats.")
	} else {
		fmt.Println("Connected to NATS")
	}

	type person struct {
		Name    string
		Address string
		Age     int
	}

	gonats.nc.QueueSubscribe("hello.>", "tracker", func(m *nats.Msg) {
		fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data))
	})

	sendCh := make(chan *person)
	gonats.ec.BindSendChan("hello.test", sendCh)
	me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery Street"}
	// Send via Go channels
	sendCh <- me

	//////////////////////////////////////// SSL CERT MANAGER
	certManager := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(configuration.Domains...),
		Cache:      autocert.DirCache(cache),
	}
	server := &http.Server{ // HTTP REDIR SSL RENEW
		Addr: ":https",
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
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Strict-Transport-Security", "max-age=15768000 ; includeSubDomains")
			fmt.Println("Incoming HTTP at ", r)
			proxy.ServeHTTP(w, r)
		})
	}

	//////////////////////////////////////// PING PONG ROUTE
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(PONG))
	})

	fmt.Println("Serving static content in:", configuration.StaticDirectory)
	fs := http.FileServer(http.Dir(configuration.StaticDirectory))
	http.HandleFunc("/img/v1/", func(w http.ResponseWriter, r *http.Request) {
		http.StripPrefix("/img/v1/", fs).ServeHTTP(w, r)
	})

	http.HandleFunc("/img/v1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "image/gif")
		w.Write(TRACKING_GIF)
	})

	//////////////////////////////////////// SERVE, REDIRECT AUTO to HTTPS
	go func() {
		fmt.Println("Serving HTTP Redirect on: 80")
		http.ListenAndServe(":http", certManager.HTTPHandler(nil))
	}()
	fmt.Println("Serving TLS requests on: 443")
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
// Trace
////////////////////////////////////////
func track(s session, r *http.Request) {
	s.write(r)
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
	rand.Seed(time.Now().UnixNano())
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
	return fmt.Errorf("Cassandra (listen) not implemented")
}

//////////////////////////////////////// C*
func (i *CassandraService) write(r *http.Request) error {
	// //TODO: performance test against batching
	// //fmt.Fprintf(os.Stderr, "Input packet", metrics)
	// // This will get set to nil if a successful write occurs
	err := fmt.Errorf("Could not write to any cassandra server in cluster")
	// counters := make(map[string]int)
	// regexCount, _ := regexp.Compile(`\.count\.(.*)`)
	// regexUpdate, _ := regexp.Compile(`\.update\.(.*)`)
	// //insertBatch := i.session.NewBatch(gocql.UnloggedBatch)
	// for _, metric := range metrics {
	// 	var tags = metric.Tags()
	// 	//fmt.Println("%s", tags) //Debugging only
	// 	if regexCount.MatchString(tags["name"]) {
	// 		counter := regexCount.FindStringSubmatch(tags["name"])[1]
	// 		counters[counter] = counters[counter] + 1
	// 	} else if regexUpdate.MatchString(tags["name"]) && tags["msg"] != "" {
	// 		timestamp := time.Now().UTC()
	// 		if tags["updated"] != "" {
	// 			millis, err := strconv.ParseInt(tags["updated"], 10, 64)
	// 			if err == nil {
	// 				timestamp = time.Unix(0, millis*int64(time.Millisecond))
	// 			}
	// 		}
	// 		if rowError := i.session.Query(`INSERT INTO updates (id, updated, msg) values (?,?,?)`,
	// 			regexUpdate.FindStringSubmatch(tags["name"])[1],
	// 			timestamp,
	// 			tags["msg"]).Exec(); rowError != nil {
	// 			err = rowError //And let it continue
	// 		} else {
	// 			err = nil
	// 		}
	// 	} else {
	// 		if tags["id"] == "" {
	// 			tags["id"] = gocql.TimeUUID().String()
	// 		}
	// 		serialized, _ := json.Marshal(tags)
	// 		//insertBatch.Query(`INSERT INTO logs JSON ?`, string(serialized))
	// 		if rowError := i.session.Query(`INSERT INTO logs JSON ?`, string(serialized)).Exec(); rowError != nil {
	// 			err = rowError //And let it continue
	// 		} else {
	// 			err = nil
	// 		}
	// 	}
	// }

	// for key, value := range counters {
	// 	if rowError := i.session.Query(`UPDATE counters set total=total+? where id=?;`, value, key).Exec(); rowError != nil {
	// 		err = rowError //And let it continue
	// 	} else {
	// 		err = nil
	// 	}
	// }

	// //err = i.session.ExecuteBatch(insertBatch)
	// if !i.Retry && err != nil {
	// 	fmt.Fprintf(os.Stderr, "!E CASSANDRA OUTPUT PLUGIN - NOT RETRYING %s", err.Error())
	// 	err = nil //Do not retry
	// }
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
		log.Fatalf("error parsing X509 certificate/key pair: %v", err)
	}

	rootPEM, err := ioutil.ReadFile(i.Configuration.CACert)

	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		panic("failed to parse root certificate")
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
// Close will terminate the session to the backend, returning error if an issue arises
func (i *NatsService) close() error {
	// i.ec.Drain()
	// i.ec.Close()
	i.nc.Drain()
	i.nc.Close()
	return fmt.Errorf("Nats (close) not implemented")
}

//////////////////////////////////////// NATS
// Write
func (i *NatsService) write(r *http.Request) error {
	return fmt.Errorf("Nats (write) not implemented")
}

//////////////////////////////////////// NATS
// Listen
func (i *NatsService) listen() error {
	return fmt.Errorf("Nats (listen) not implemented")
}
