/*===----------- dragongate.go - nginx alternative written in go  ----------===
 *
 *                     DragonGate Simple WebServer
 *
 * This file is licensed under the Apache 2 License. See LICENSE for details.
 *
 *  Copyright (c) 2018 Mico Malecki; Andrew Grosser. All Rights Reserved.
 *
 *
 *                  NM+                 :::::::::::   :::::::::::.
 *       oddddddddddMMmdddddddddd:     `MMhhhhhhdMM  `MMhhhhhhhMM+
 *       .---:mMs---------dNh----`     `MM-     -MM  `MM.      NM+
 *            -MM:       sMd.          `MMNmmmmmNMM  `MMmmmmmmmMM+
 *     smdddddmNNmddddddmNMmddddddd:   `MM-     -MM  `MM.      NM+
 *     ............................`   `MMysssssyMM  `MMsssssssMM+
 *        .ddddddddddddddddddddd`      `MMo++++++++  `+++++++++MM+
 *        -MM-......NMo......+MM`      `MM-                    NM+
 *        -MMsooooooNMhooooooyMM`      `MM-                    NM+
 *        -MM+//////NMy//////sMM`      `MM-                    NM+
 *        -MM-......NMo......+MM`      `MM-                    NM+
 *        -MMdddddddMMmddddddddh`.:`   `MM-                    NM+
 *        `++`      NM+          sMs   `MM-                    NM+
 *                  oMNhhhhhhhhhdMm.   `MM-              -NNNNMMm.
 *                   `.----------.      --`               .--..`
 *
 *
 *===----------------------------------------------------------------------===
 */
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"

	"golang.org/x/crypto/acme/autocert"
)

////////////////////////////////////////
// Get the system setup from the config.json file:
////////////////////////////////////////
type Configuration struct {
	Domains         []string //Domains in Trust, LetsEncrypt domains
	ProxyUrl        string   //Forward Address
	StaticDirectory string   //Static FS Directory (./public/)
	UsePingPong     bool
	Tracker         string
}

//////////////////////////////////////// PING-PONG return string
const PONG string = "pong"

//////////////////////////////////////// Transparent GIF
var TRACKING_GIF = []byte{0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, 0x80, 0x0, 0x0, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b}

////////////////////////////////////////
// Start here
////////////////////////////////////////
func main() {

	//Try this as an even simpler setup...
	//fmt.Println("Running basic server")
	//log.Fatal(http.Serve(autocert.NewListener("dev.sfproductlabs.com"), http.StripPrefix("/public/", http.FileServer(http.Dir(configuration.StaticDirectory)))))

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

	cache := cacheDir()
	if cache == "" {
		log.Fatal("Bad Cache.")
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
			fmt.Println("Incoming HTTP at ", r) // fmt.Fprintf(w, "KEY KERNEL V.23") ///
			proxy.ServeHTTP(w, r)
		})
	}

	//////////////////////////////////////// PING PONG ROUTE
	if configuration.UsePingPong {
		http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(PONG))
		})
	}

	if configuration.Tracker != "" {
		http.HandleFunc("/track/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("content-type", "image/gif")
			w.Write(TRACKING_GIF)
		})

	}

	//////////////////////////////////////// PUBLIC ROUTE (Static FS)
	if configuration.StaticDirectory != "" {
		fmt.Println("Serving static content in:", configuration.StaticDirectory)
		fs := http.FileServer(http.Dir(configuration.StaticDirectory))
		http.Handle("/public/", http.StripPrefix("/public/", fs))
	}

	//////////////////////////////////////// SERVE, REDIRECT AUTO to HTTPS
	go func() {
		fmt.Println("Serving HTTP Redirect on: 80")
		http.ListenAndServe(":http", certManager.HTTPHandler(nil))
	}()
	fmt.Println("Serving TLS requests on: 443")
	log.Fatal(server.ListenAndServeTLS("", "")) // SERVE HTTPS!
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
