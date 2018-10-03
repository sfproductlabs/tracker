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
	write() error
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
	fmt.Println("Starting web service...")
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
	var name string
	var next_seq int
	if err := cassandra.Session.Query(`SELECT name, next_seq FROM sequences where name='DB_VER' LIMIT 1`).Consistency(gocql.One).Scan(&name, &next_seq); err != nil {
		fmt.Println("[ERROR] Bad DB_VER", err)
		panic("ERROR")
	} else {
		fmt.Println("Connected to Cassandra: ", name, next_seq)
	}

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

	//////////////////////////////////////// PING PONG ROUTE
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(PONG))
	})

	fmt.Println("Serving static content in:", configuration.StaticDirectory)
	fs := http.FileServer(http.Dir(configuration.StaticDirectory))
	http.Handle("/public/", http.StripPrefix("/public/", fs))

	http.HandleFunc("/track/", func(w http.ResponseWriter, r *http.Request) {
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
func track(s session) {
	s.write()
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
func (i *CassandraService) write() error {
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
	err := fmt.Errorf("Nats (connect) not implemented")

	certFile := "./configs/certs/client-cert.pem"
	keyFile := "./configs/certs/client-key.pem"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("error parsing X509 certificate/key pair: %v", err)
	}

	rootPEM, err := ioutil.ReadFile("server.crt")

	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		panic("failed to parse root certificate")
	}

	config := &tls.Config{
		//ServerName:   opts.Host,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
		//InsecureSkipVerify: true, //SECURITY THREAT
	}

	// nc, err = nats.Connect("nats://localhost:4443", nats.Secure(config))
	// if err != nil {
	// 	t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	// }

	i.nc, err = nats.Connect("nats://localhost:4222", nats.Token("S3cretT0ken"), nats.RootCAs("./configs/certs/ca.pem"), nats.ClientCert("./configs/certs/client-cert.pem", "./configs/certs/client-key.pem"), nats.Secure(config))
	i.ec, _ = nats.NewEncodedConn(i.nc, nats.JSON_ENCODER)
	defer i.ec.Close()
	msgCh := make(chan interface{})
	i.ec.BindRecvChan("hello", msgCh)
	// Receive via Go channels
	msg := <-msgCh
	fmt.Printf("Msg received on [%s] : %s\n", "TEST", msg)
	return err
}

//////////////////////////////////////// NATS
// Close will terminate the session to the backend, returning error if an issue arises
func (i *NatsService) close() error {
	// Drain connection (Preferred for responders)
	// Close() not needed if this is called.
	i.nc.Drain()

	// Close connection
	i.nc.Close()
	return fmt.Errorf("Nats (close) not implemented")
}

//////////////////////////////////////// NATS
// Write
func (i *NatsService) write() error {
	return fmt.Errorf("Nats (write) not implemented")
}

//////////////////////////////////////// NATS
// Listen
func (i *NatsService) listen() error {
	return fmt.Errorf("Nats (listen) not implemented")
}
