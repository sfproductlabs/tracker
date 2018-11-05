/*===----------- utils.go - tracking utility written in go  -------------===
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
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

////////////////////////////////////////
// hash
////////////////////////////////////////
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func sha(s string) string {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
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

func getIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}
