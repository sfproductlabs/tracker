
package main                                                     //   .
                                                                //   /Ai
                                                                   ///sx//       WEB SEC /////
import (	                                                  //m-  -mh.      V 0.23 /////
	"os"                                                     //o      oN+
	"log"                                                   //d-        -dh.
	"fmt"                                                 //o            +X+
	"flag"                                               ///.   `.----.`   /dd.
	"os/user"                                                 //  M  M  //
	"net/url"                                                  //+ M M +/
	"net/http"                                       ///oshy+- /M X  M MN./-+shsH/N
	"crypto/tls"                                    /// .hs.   / M M M  -  //sy. `hm-
	"html/template"                               ///     `:+//:o      o://+-      /Ns:
	"path/filepath"                              //`          //:      :/           `ym:
	"net/http/httputil"                        ///             //      //             /Ny:
	"golang.org/x/crypto/acme/autocert"      //Mdhhhhhhhhhhhhhhhh      hhhhhhhhhhhhhhhhdMN/.
)
// 安全 WEB SEC
func main() {        ///                                                                    //.
	fmt.Println("       +MM    smNy:          yMm/  dmmmmmmm/            dMd           ")
	fmt.Println("       oMN     .+dh.      ./:`oMm- ::::::MM+   `////////mMm////////`  ")
	fmt.Println(" ///  +NMh//MMh///+///`   oMN            MM+   :MM:`````dMd`````:MM:  ")
	fmt.Println("     /MM+   MMh Mm/       oMN     Web    MM+   :MMmmmmmmMMMmmmmmmMM:  ")
	fmt.Println("    +NMs   .MMMNs. `/-    oMN     Sec    MM+   .yy-`````dMd`````-yy-  ")
	fmt.Println("  .yMNo.   .MMh.   -MM.   oMN            MM+            dMd           ")
	fmt.Println(" sh:       ::yhhhhhh/-    +md         +dddo`            HHH           ")
	flag.Parse() //// ARGS                                                //###//       ##/

	domains := flag.Args()
	live_domains := []string{
		"keykernel.io", "stackfund.com", "apdex.com", "x.m2fx.co", "m2fx.co",  "x.bitcoinpatent.com" }

	domains = append( live_domains , domains...)
	////////////////////////////////// SSL CERT MANAGER
	certManager := autocert.Manager{          ///CREATE
		Prompt:     autocert.AcceptTOS,   /// CACHE
		HostPolicy: autocert.HostWhitelist(domains...),
		Cache:      autocert.DirCache( cacheDir() ),
	}
	server := &http.Server{     // HTTP REDIR SSL RENEW
		Addr: ":https",
		TLSConfig: &tls.Config{           // SEC PARAMS
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
	origin, _ := url.Parse("http://pyweb:8851/")
	director := func(req *http.Request) {
		req.Header.Add("X-Forwarded-Host", req.Host)
		req.Header.Add("X-Origin-Host", origin.Host)
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}
	proxy := &httputil.ReverseProxy{Director: director}
	fs := http.FileServer(http.Dir("static"))

	///////////////////////////////////////////////////////// HTTP HANDLERS
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request){
		w.Header().Set("Strict-Transport-Security", "max-age=15768000 ; includeSubDomains")
		fmt.Println("Incoming HTTP at ",r)   // fmt.Fprintf(w, "KEY KERNEL V.23") ///
		proxy.ServeHTTP(w, r)
	})
	http.Handle("/public/", http.StripPrefix("/public/", fs))
	http.HandleFunc("/template", serveTemplate)
	//http.HandleFunc("/graph", graph.BaseHandler )

	// SERVE, REDIRECT AUTO to HTTPS
	go func() {
		http.ListenAndServe(":http", certManager.HTTPHandler(nil) )
	}()
	log.Fatal( server.ListenAndServeTLS("", "") ) // SERVE HTTPS!
	fmt.Println("安全 WebSec Golang.    ", domains)



}

// cacheDir MAKES CONSISTENT CACHE DIRECTORY IN /tmp. RETURNS "" ON ERROR.
func cacheDir() (dir string) {
	if u, _ := user.Current(); u != nil {
		fmt.Println(os.TempDir() )
		//dir = filepath.Join(os.TempDir(), "cache-golang-autocert-"+u.Username)
		dir = filepath.Join(".", "cache-golang-autocert-"+u.Username)
		fmt.Println( "Should be saving cache-go-lang-autocert-u.username to: " )
		fmt.Println( dir )
		if err := os.MkdirAll(dir, 0700); err == nil {
			return dir }}
	return ""
}



func serveTemplate(w http.ResponseWriter, r *http.Request) {
	lp := filepath.Join("templates", "layout.html")
	fp := filepath.Join("templates", filepath.Clean(r.URL.Path))

	// Return a 404 if the template doesn't exist
	info, err := os.Stat(fp)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
	}

	// Return a 404 if the request is for a directory
	if info.IsDir() {
		http.NotFound(w, r)
		return
	}

	tmpl, err := template.ParseFiles(lp, fp)
	if err != nil {
		// Log the detailed error
		log.Println(err.Error())
		// Return a generic "Internal Server Error" message
		http.Error(w, http.StatusText(500), 500)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "layout", nil); err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(500), 500)
	}
}






func main_proxy_base() {
	origin, _ := url.Parse("http://localhost:5000/")
	director := func(req *http.Request) {
		req.Header.Add("X-Forwarded-Host", req.Host)
		req.Header.Add("X-Origin-Host", origin.Host)
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}
	proxy := &httputil.ReverseProxy{Director: director}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	})
	log.Fatal(http.ListenAndServe(":9003", nil))
}
