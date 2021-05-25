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
	"archive/zip"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/gocql/gocql"
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
// filterUrl
// matchGroup is a 1 indexed array (0 is default last)
// returns last match if no group, or group
////////////////////////////////////////
func filterUrl(c *Configuration, s *string, matchGroup *int) error {
	matches := regexFilterUrl.FindStringSubmatch(*s)
	mi := len(matches)
	if matchGroup == nil || *matchGroup == 0 {
		//Take the last one by default
		if mi > 0 {
			*s = matches[mi-1]
			return nil
		}
	} else {
		if mi > *matchGroup {
			*s = matches[*matchGroup]
			return nil
		}
	}
	//Fallback
	filterUrlAppendix(s)
	return fmt.Errorf("Mismatch Regex (Url Filter)")
}

func filterUrlAppendix(s *string) error {
	if s != nil {
		i := strings.Index(*s, "?")
		if i > -1 {
			*s = (*s)[:i]
		}
	}
	return nil
}

func filterUrlPrefix(s *string) error {
	if s != nil {
		*s = strings.ToLower(*s)
		i := strings.Index(*s, "https://")
		if i > -1 {
			*s = (*s)[i+6:]
			return nil
		}
		i = strings.Index(*s, "http://")
		if i > -1 {
			*s = (*s)[i+5:]
		}
	}
	return nil
}

func cleanInterfaceString(i interface{}) error {
	s := &i
	if temp, ok := (*s).(string); ok {
		*s = strings.ToLower(strings.TrimSpace(temp))
	}
	return nil
}

func ensureInterfaceString(i interface{}) error {
	s := &i
	if _, ok := (*s).(string); !ok {
		if s != nil {
			*s = fmt.Sprintf("%v", *s)
		}
	}
	return nil
}

func cleanString(s *string) error {
	if s != nil && *s != "" {
		*s = strings.ToLower(strings.TrimSpace(*s))
	}
	return nil
}

func upperString(s *string) error {
	if s != nil && *s != "" {
		*s = strings.ToUpper(strings.TrimSpace(*s))
	}
	return nil
}

func FixedLengthNumberString(length int, str string) string {
	verb := fmt.Sprintf("%%%d.%ds", length, length)
	return strings.Replace(fmt.Sprintf(verb, str), " ", "0", -1)
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
		var err error
		if ip, _, err = net.SplitHostPort(r.RemoteAddr); err != nil {
			ip = r.RemoteAddr
		}
	}
	return cleanIP(ip)
}

func cleanIP(ip string) string {
	ipa := strings.Split(ip, ",")
	for i := len(ipa) - 1; i > -1; i-- {
		ipa[i] = strings.TrimSpace(ipa[i])
		if ipp := net.ParseIP(ipa[i]); ipp != nil {
			return ipp.String()
		}
	}
	return ""
}

func getHost(r *http.Request) string {
	if addr, _, err := net.SplitHostPort(r.Host); err != nil {
		return r.Host
	} else {
		return addr
	}
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkRowExpired(row map[string]interface{}, p Prune) bool {
	var created *time.Time
	expired := false
	if ctemp1, ok := row["created"]; ok {
		if ctemp2, okp := ctemp1.(time.Time); okp {
			created = &ctemp2
		}
	}
	if created != nil {
		if p.SkipToTimestamp > 0 && created.Before(time.Unix(p.SkipToTimestamp, 0)) {
			return false
		}
		expired = (*created).Add(time.Second * time.Duration(p.TTL)).Before(time.Now().UTC())
	} else {
		expired = true
	}
	// if fmt.Sprintf("%T", row["cflags"]) != "int64" {
	// 	err = fmt.Errorf("Table %s not supported for pruning (bad cflags type)", p.Table)
	// 	goto tablefailed
	// }
	var ignore int64
	for _, icflag := range p.IgnoreCFlags {
		ignore += icflag
	}
	if cftemp1, ok := row["cflags"]; ok {
		if cftemp2, okp := cftemp1.(int64); okp {
			if cftemp2&ignore > 0 {
				expired = false
			}
		}
	}
	return expired
}

func checkIdExpired(uuid *gocql.UUID, ttl int) bool {

	//If the id is incorrectly formatted expire it
	if uuid == nil || uuid.Version() != 1 {
		return true
	}

	created := uuid.Time()

	return created.Add(time.Second * time.Duration(ttl)).Before(time.Now().UTC())

}
