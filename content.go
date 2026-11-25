// content.go — tr-cluster content fallback (phase D).
//
// Serves the canonical CMS content from the CDN (the immutable content
// backbone) + the site config from ClickHouse, so the tr cluster (ch1-3) can
// serve content when x-1 (the x cluster / source of truth) is down. This is the
// fallback for the secops/actgreen content routers: they try x-1 first, then
// this route (on the tr cluster).
//
// The route: GET /tr/{apiVersion}/content/resolve/{path}
//   - Resolves the host -> site config (from the ~5-min CH cache, incl. cdn_cname).
//   - Fetches the CDN index ({cdn}/{org}/cms/{org}/index.json) -> canonical_arm_id.
//   - Fetches the canonical fragment ({cdn}/{org}/cms/{org}/items/{path}/arms/{arm}.html).
//   - Returns the §3 common-content contract (JSON).
//
// Fail-open: any error -> 404 (the secops router then falls back to its local
// secops-content submodule + branded 404). Never 500 the site.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// contentIndexItem is one entry in the CDN index.json.
type contentIndexItem struct {
	ContentPath     string `json:"content_path"`
	FullPath        string `json:"full_path"`
	CanonicalURL    string `json:"canonical_url"`
	XID             string `json:"xid"`
	Title           string `json:"title"`
	CanonicalArmID  string `json:"canonical_arm_id"`
	UpdatedAt       string `json:"updated_at"`
	Taxonomy        struct {
		Category string   `json:"category"`
		Persona  string   `json:"persona"`
		Tags     []string `json:"tags"`
	} `json:"taxonomy"`
}

// apiVersion is set in main ("v" + configuration.ApiVersion) and shared with the
// content route (the /tr/{apiVersion}/content/resolve/ prefix).
var apiVersion string

// contentCDNClient is a small HTTP client for fetching the CDN content.
var contentCDNClient = &http.Client{Timeout: 8 * time.Second}

// contentResolveHandler serves the canonical CMS content from the CDN + CH.
func contentResolveHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// 1) Resolve the host -> site config (from the ~5-min CH cache).
	host := r.Host
	if h := r.Header.Get("X-Forwarded-Host"); h != "" {
		host = h
	}
	if gTlsCertMgr == nil {
		http.Error(w, `{"status":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	site, ok := gTlsCertMgr.ResolveHost(host)
	if !ok || site.CdnCname == "" {
		// No site config or no CDN configured -> 404 (the secops router falls
		// back to its local content + branded 404).
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"status":"not_found","served_by":"tr_cluster","reason":"no_site_or_cdn"}`)
		return
	}

	// 2) Extract the content path (after the /resolve/ prefix).
	prefix := "/tr/" + apiVersion + "/content/resolve/"
	contentPath := strings.TrimPrefix(r.URL.Path, prefix)
	contentPath = strings.Trim(contentPath, "/")
	if contentPath == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"status":"bad_request","reason":"missing_path"}`)
		return
	}

	// 3) Fetch the CDN index -> find the item + canonical_arm_id.
	cdnBase := strings.TrimSuffix(site.CdnCname, "/")
	org := site.Org
	cmsBase := fmt.Sprintf("%s/%s/cms/%s", cdnBase, org, org)

	indexURL := cmsBase + "/index.json"
	indexBytes, err := contentFetch(indexURL)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"status":"not_found","served_by":"tr_cluster","reason":"cdn_index_unreachable"}`)
		return
	}
	var index []contentIndexItem
	if err := json.Unmarshal(indexBytes, &index); err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"status":"not_found","served_by":"tr_cluster","reason":"cdn_index_parse"}`)
		return
	}
	var item *contentIndexItem
	for i := range index {
		if strings.Trim(index[i].ContentPath, "/") == contentPath {
			item = &index[i]
			break
		}
	}
	if item == nil || item.CanonicalArmID == "" {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"status":"not_found","served_by":"tr_cluster","reason":"path_not_in_index","path":%q}`, contentPath)
		return
	}

	// 4) Fetch the canonical fragment from the CDN.
	fragmentURL := fmt.Sprintf("%s/items/%s/arms/%s.html", cmsBase, contentPath, item.CanonicalArmID)
	fragment, err := contentFetch(fragmentURL)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"status":"not_found","served_by":"tr_cluster","reason":"cdn_fragment_unreachable","path":%q}`, contentPath)
		return
	}

	// 5) Return the §3 common-content contract (canonical mode).
	resp := map[string]interface{}{
		"path":      contentPath,
		"status":    "ok",
		"served_by": "tr_cluster",
		"resolution": map[string]interface{}{
			"mode":      "canonical",
			"arm_id":    item.CanonicalArmID,
			"canonical": true,
		},
		"tracking": map[string]interface{}{
			"xid":         item.XID,
			"arm_id":      item.CanonicalArmID,
			"t_host":      site.TrackerHost,
			"t_path":      "/tr/v1/tr/" + org + "/app",
			"params":      "{}",
			"decision_id": "canonical",
		},
		"arm": map[string]interface{}{
			"id":        item.CanonicalArmID,
			"headline":  item.Title,
			"body_html": string(fragment),
		},
		"meta": map[string]interface{}{
			"title":         item.Title,
			"canonical_url": item.CanonicalURL,
		},
		"cdn": map[string]interface{}{
			"base":    cdnBase,
			"version": item.UpdatedAt,
			"max_age": 300,
		},
		"taxonomy": item.Taxonomy,
	}
	json.NewEncoder(w).Encode(resp)
}

// contentFetch does a GET + returns the body (or an error).
func contentFetch(url string) ([]byte, error) {
	resp, err := contentCDNClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d for %s", resp.StatusCode, url)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB cap
}
