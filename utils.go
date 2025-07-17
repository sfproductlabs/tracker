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
	"crypto/sha256"
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
	"github.com/google/uuid"
	goja4h "github.com/lum8rjack/go-ja4h"
)

// //////////////////////////////////////
// hash
// //////////////////////////////////////
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

func shasum256(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// //////////////////////////////////////
// filterUrl
// matchGroup is a 1 indexed array (0 is default last)
// returns last match if no group, or group
// //////////////////////////////////////
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

// //////////////////////////////////////
// cacheDir in /tmp for SSL
// //////////////////////////////////////
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

// getRequestHostInfo returns comprehensive host information from various request headers
func getRequestHostInfo(r *http.Request) map[string]string {
	hostInfo := map[string]string{
		"request_host":   r.Header.Get("host"),
		"x_host":         r.Header.Get("x-host"),
		"forwarded_host": r.Header.Get("x-forwarded-host"),
		"original_host":  r.Header.Get("x-original-host"),
		"client_ip":      r.Header.Get("x-real-ip"),
		"server_ip":      r.Header.Get("server-ip"),
		"forwarded_for":  r.Header.Get("x-forwarded-for"),
	}

	// Filter out empty values
	filtered := make(map[string]string)
	for k, v := range hostInfo {
		if v != "" {
			filtered[k] = v
		}
	}
	return filtered
}

// getPrimaryHost returns the most reliable host identifier for hhash calculation
// Order of precedence:
// 1. x-original-host (original client request host before any proxying)
// 2. host (standard host header)
// 3. x-forwarded-host (if behind a trusted proxy)
// 4. x-host (fallback custom header)
// 5. x-real-ip (if no host headers available)
// 6. x-forwarded-for (last resort, first client IP in chain)
// 7. server-ip (absolute last resort)
func getPrimaryHost(r *http.Request) string {
	headers := r.Header

	// Try each header in order of reliability
	host := headers.Get("x-original-host")
	if host != "" {
		return cleanHost(host)
	}

	host = headers.Get("host")
	if host != "" {
		return cleanHost(host)
	}

	host = headers.Get("x-forwarded-host")
	if host != "" {
		return cleanHost(host)
	}

	host = headers.Get("x-host")
	if host != "" {
		return cleanHost(host)
	}

	host = headers.Get("x-real-ip")
	if host != "" {
		return cleanHost(host)
	}

	// Handle x-forwarded-for (take first IP in chain)
	if forwardedFor := headers.Get("x-forwarded-for"); forwardedFor != "" {
		if ips := strings.Split(forwardedFor, ","); len(ips) > 0 {
			firstIP := strings.TrimSpace(ips[0])
			if firstIP != "" {
				return cleanHost(firstIP)
			}
		}
	}

	host = headers.Get("server-ip")
	if host != "" {
		return cleanHost(host)
	}

	return ""
}

// cleanHost removes port from host if present and returns clean hostname/IP
func cleanHost(host string) string {
	if addr, _, err := net.SplitHostPort(host); err != nil {
		return host
	} else {
		return addr
	}
}

func getIP(r *http.Request) string {
	// Enhanced IP detection with multiple header fallbacks
	// Order of precedence:
	// 1. x-real-ip (most reliable, set by trusted proxy)
	// 2. x-forwarded-for (first IP in chain)
	// 3. x-client-ip (some proxies use this)
	// 4. cf-connecting-ip (Cloudflare)
	// 5. x-forwarded (standard format)
	// 6. RemoteAddr (fallback)

	headers := r.Header

	// Try x-real-ip first (most reliable)
	if ip := headers.Get("x-real-ip"); ip != "" {
		return cleanIP(ip)
	}

	// Try x-forwarded-for (take first IP in chain)
	if forwardedFor := headers.Get("x-forwarded-for"); forwardedFor != "" {
		if ips := strings.Split(forwardedFor, ","); len(ips) > 0 {
			firstIP := strings.TrimSpace(ips[0])
			if firstIP != "" {
				return cleanIP(firstIP)
			}
		}
	}

	// Try x-client-ip
	if ip := headers.Get("x-client-ip"); ip != "" {
		return cleanIP(ip)
	}

	// Try Cloudflare header
	if ip := headers.Get("cf-connecting-ip"); ip != "" {
		return cleanIP(ip)
	}

	// Try x-forwarded header
	if forwarded := headers.Get("x-forwarded"); forwarded != "" {
		// Parse "for=client,for=proxy" format
		if strings.Contains(forwarded, "for=") {
			parts := strings.Split(forwarded, ",")
			for _, part := range parts {
				if strings.HasPrefix(strings.TrimSpace(part), "for=") {
					client := strings.TrimSpace(strings.TrimPrefix(part, "for="))
					if client != "" && client != "unknown" {
						return cleanIP(client)
					}
				}
			}
		}
	}

	// Fallback to RemoteAddr
	if r.RemoteAddr != "" {
		if ip, _, err := net.SplitHostPort(r.RemoteAddr); err != nil {
			return cleanIP(r.RemoteAddr)
		} else {
			return cleanIP(ip)
		}
	}

	return ""
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
	host := getPrimaryHost(r)
	if host == "" {
		// Fallback to r.Host if no headers found
		return r.Host
	}
	return host
}

// getFullURL returns the full URL for redirect lookups, using enhanced host detection
func getFullURL(r *http.Request) string {
	host := getPrimaryHost(r)
	if host == "" {
		// Fallback to original host if enhanced detection fails
		host = r.Host
	}
	return fmt.Sprintf("%s%s", host, r.URL.Path)
}

// getJA4H generates a JA4H fingerprint from the HTTP request
func getJA4H(r *http.Request) string {
	return goja4h.JA4H(r)
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

func checkRowExpired(row map[string]interface{}, meta *gocql.TableMetadata, p Prune, pruneSkipToTimestamp int64) (bool, *time.Time) {
	var created *time.Time
	expired := false
	if ctemp1, ok := row["created"]; ok {
		if ctemp2, okp := ctemp1.(time.Time); okp {
			created = &ctemp2
		}
	}
	if meta != nil && len(meta.PartitionKey) == 1 {
		if tuuid, tok := row[meta.PartitionKey[0].Name]; tok {
			if uuid, ok := tuuid.(gocql.UUID); ok {
				if uuid.Version() == 1 {
					c := uuid.Time()
					if c.Before(*created) {
						created = &c
					}
				}
			}
		}
	}

	if created != nil {
		if created.Before(time.Unix(pruneSkipToTimestamp, 0)) {
			return false, created
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
	for _, icflag := range p.CFlagsIgnore {
		ignore += icflag
	}
	if cftemp1, ok := row["cflags"]; ok {
		if cftemp2, okp := cftemp1.(int64); okp {
			if cftemp2&ignore > 0 {
				expired = false
			}
		}
	}
	return expired, created
}

func checkIdExpired(uuid *gocql.UUID, ttl int) bool {

	//If the id is incorrectly formatted expire it
	if uuid == nil || uuid.Version() != 1 {
		return true
	}

	created := uuid.Time()

	return created.Add(time.Second * time.Duration(ttl)).Before(time.Now().UTC())

}

func checkUUIDExpired(uuid *uuid.UUID, ttl int) bool {
	//If the id is incorrectly formatted expire it
	if uuid == nil || uuid.Version() != 1 {
		return true
	}

	// Convert UUID timestamp to time.Time
	// UUID v1 timestamp is 100-nanosecond intervals since UUID epoch (15 Oct 1582)
	uuidTime := time.Unix(0, int64((uuid.Time()-0x01B21DD213814000)*100))

	return uuidTime.Add(time.Second * time.Duration(ttl)).Before(time.Now().UTC())
}

func SetValueInJSON(iface interface{}, path string, value interface{}) interface{} {
	m := iface.(map[string]interface{})
	split := strings.Split(path, ".")
	for k, v := range m {
		if strings.EqualFold(k, split[0]) {
			if len(split) == 1 {
				m[k] = value
				return m
			}
			switch v.(type) {
			case map[string]interface{}:
				return SetValueInJSON(v, strings.Join(split[1:], "."), value)
			default:
				return m
			}
		}
	}
	// path not found -> create
	if len(split) == 1 {
		m[split[0]] = value
	} else {
		newMap := make(map[string]interface{})
		newMap[split[len(split)-1]] = value
		for i := len(split) - 2; i > 0; i-- {
			mTmp := make(map[string]interface{})
			mTmp[split[i]] = newMap
			newMap = mTmp
		}
		m[split[0]] = newMap
	}
	return m
}

// //////////////////////////////////////
// Mathematical utility functions
// //////////////////////////////////////

// maxInt returns the larger of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// //////////////////////////////////////
// String/Type conversion utilities
// //////////////////////////////////////

// getStringValue converts interface{} to string with nil safety
func getStringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if str, ok := v.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", v)
}

// getStringPtr converts interface{} to *string with nil safety
func getStringPtr(v interface{}) *string {
	if v == nil {
		return nil
	}
	str := getStringValue(v)
	if str == "" {
		return nil
	}
	return &str
}

// parseUUID converts interface{} to *uuid.UUID with error handling
func parseUUID(v interface{}) interface{} {
	str := getStringValue(v)
	if str == "" {
		return nil
	}
	if parsed, err := uuid.Parse(str); err == nil {
		return parsed // Return UUID value, not pointer
	}
	return nil
}

// //////////////////////////////////////
// Metrics manipulation utilities
// //////////////////////////////////////

// incrementMetric increments a metric value in a map by 1, handling type conversion
func incrementMetric(metrics map[string]interface{}, key string) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue + 1
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue) + 1
		}
		if intValue, ok := value.(int64); ok {
			return float64(intValue) + 1
		}
	}
	return 1
}

// addToMetric adds a value to a metric in a map, handling type conversion
func addToMetric(metrics map[string]interface{}, key string, addition float64) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue + addition
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue) + addition
		}
		if intValue, ok := value.(int64); ok {
			return float64(intValue) + addition
		}
	}
	return addition
}

// getMetricValue gets a metric value from a map as float64, handling type conversion
func getMetricValue(metrics map[string]interface{}, key string) float64 {
	if value, exists := metrics[key]; exists {
		if floatValue, ok := value.(float64); ok {
			return floatValue
		}
		if intValue, ok := value.(int); ok {
			return float64(intValue)
		}
		if intValue, ok := value.(int64); ok {
			return float64(intValue)
		}
	}
	return 0
}
