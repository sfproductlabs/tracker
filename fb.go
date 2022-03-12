/*===----------- fb.go - facebook CAPI interface written in go  -------------===
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

func (i *FacebookService) connect() error {
	i.Configuration.Session = i
	if i.Configuration.Context == "" || i.Configuration.Key == "" {
		return fmt.Errorf("You must provide a pixel and token to use the facebook endpoint")
	}
	return nil
}

//////////////////////////////////////// Facebook
// Close
//will terminate the session to the backend, returning error if an issue arises
func (i *FacebookService) close() error {
	return nil
}

//////////////////////////////////////// Facebook
// Write
func (i *FacebookService) write(w *WriteArgs) error {
	//TODO: Facebook services
	// https://graph.facebook.com/{api_ver}/{PIXEL_ID}/events?access_token={TOKEN}
	var err error
	v := *w.Values
	switch w.WriteType {
	case WRITE_EVENT:

		//////////////////////////////////////////////
		//FIX CASE
		//////////////////////////////////////////////
		// delete(v, "cleanIP")
		// cleanString(&(w.Browser))
		// cleanString(&(w.Host))
		//cleanInterfaceString(v["app"])
		var cflags *int64
		if com, ok := v["cflags"].(int64); ok {
			cflags = &com
		} else if com, ok := v["cflags"].(float64); ok {
			temp := int64(com)
			cflags = &temp
		}

		var now int64 = 0
		if temp, ok := v["now"].(string); ok {
			now, _ = strconv.ParseInt(temp, 10, 64)
		} else if temp, ok := v["now"].(int64); ok {
			now = temp
		} else if temp, ok := v["now"].(float64); ok {
			temp := int64(temp)
			now = temp
		}
		if now == 0 {
			now = time.Now().Unix()
		}

		//Only accept marketing level cookies
		if cflags == nil {
			if !i.AppConfig.CFlagsIgnore {
				return nil
			}
		} else if *cflags < i.AppConfig.CFlagsMarketing {
			return nil
		}

		//Only accept events if targeted for fb
		if v["fbx"] != nil || i.Configuration.AttemptAll {

			//[country]
			var country *string
			var region *string
			var city *string
			zip := v["zip"]
			ensureInterfaceString(zip)
			if tz, ok := v["tz"].(string); ok {
				if ct, oktz := countries[tz]; oktz {
					country = &ct
				}
			}
			//[latlon]
			var latlon *geo_point
			latf, oklatf := v["lat"].(float64)
			lonf, oklonf := v["lon"].(float64)
			if oklatf && oklonf {
				//Float
				latlon = &geo_point{}
				latlon.Lat = latf
				latlon.Lon = lonf
			} else {
				//String
				lats, oklats := v["lat"].(string)
				lons, oklons := v["lon"].(string)
				if oklats && oklons {
					latlon = &geo_point{}
					latlon.Lat, _ = strconv.ParseFloat(lats, 64)
					latlon.Lon, _ = strconv.ParseFloat(lons, 64)
				}
			}
			if latlon == nil {
				if gip, err := GetGeoIP(net.ParseIP(w.IP)); err == nil && gip != nil {
					var geoip GeoIP
					if err := json.Unmarshal(gip, &geoip); err == nil && geoip.Latitude != 0 && geoip.Longitude != 0 {
						latlon = &geo_point{}
						latlon.Lat = geoip.Latitude
						latlon.Lon = geoip.Longitude
						if geoip.CountryISO2 != "" {
							country = &geoip.CountryISO2
						}
						if geoip.Region != "" {
							region = &geoip.Region
						}
						if geoip.City != "" {
							city = &geoip.City
						}
						if zip == nil && geoip.Zip != "" {
							zip = &geoip.Zip
						}

					}
				}
			}
			if !i.AppConfig.UseRegionDescriptions {
				//country = nil
				region = nil
				city = nil
			}
			//Self identification of geo_pol overrules geoip
			if ct, ok := v["country"].(string); ok {
				country = &ct
			}
			if r, ok := v["region"].(string); ok {
				region = &r
			}
			if r, ok := v["city"].(string); ok {
				city = &r
			}
			cleanString(country)
			cleanString(region)
			cleanString(city)

			//EventID
			if temp, ok := v["eid"].(string); ok {
				evt, _ := gocql.ParseUUID(temp)
				if evt.Timestamp() != 0 {
					w.EventID = evt
				}
			}
			//Double check
			if w.EventID.Timestamp() == 0 {
				w.EventID = gocql.TimeUUID()
			}

			//[vid] - default
			isNew := false
			if vidstring, ok := v["vid"].(string); !ok {
				v["vid"] = gocql.TimeUUID()
				isNew = true
			} else {
				//Let's override the event id too
				tempvid, _ := gocql.ParseUUID(vidstring)
				if tempvid.Timestamp() == 0 {
					v["vid"] = gocql.TimeUUID()
					isNew = true
				}
			}
			//[uid] - let's overwrite the vid if we have a uid
			if uidstring, ok := v["uid"].(string); ok {
				tempuid, _ := gocql.ParseUUID(uidstring)
				if tempuid.Timestamp() != 0 {
					v["vid"] = v["uid"]
					isNew = false
				}
			}
			//[sid]
			if sidstring, ok := v["sid"].(string); !ok {
				if isNew {
					v["sid"] = v["vid"]
				} else {
					v["sid"] = gocql.TimeUUID()
				}
			} else {
				tempuuid, _ := gocql.ParseUUID(sidstring)
				if tempuuid.Timestamp() == 0 {
					v["sid"] = gocql.TimeUUID()
				} else {
					v["sid"] = tempuuid
				}
			}
			var params *map[string]interface{}
			if ps, ok := v["params"].(string); ok {
				json.Unmarshal([]byte(ps), &params)
			} else if ps, ok := v["params"].(map[string]interface{}); ok {
				params = &ps
			}
			var p map[string]interface{}
			p = make(map[string]interface{})
			if params != nil {
				p = *params
			}

			var ename string
			if temp, ok := p["ename"].(string); ok {
				ename = temp
			} else {
				ename = v["ename"].(string)
			}
			if ename == "" {
				return nil
			}

			httpposturl := "https://graph.facebook.com/v12.0/" + i.Configuration.Context + "/events?access_token=" + i.Configuration.Key

			var userData map[string]interface{}
			userData = make(map[string]interface{})
			var data map[string]interface{}
			data = make(map[string]interface{})
			var j map[string]interface{}
			j = make(map[string]interface{})
			userData["client_user_agent"] = w.Browser
			userData["client_ip_address"] = w.IP
			if temp, ok := v["fbp"].(string); ok {
				userData["fbp"] = temp
			}
			if temp, ok := v["fbc"].(string); ok {
				userData["fbc"] = temp
			}
			if zip != nil {
				if temp, ok := zip.(string); ok {
					userData["zp"] = shasum256(strings.ToLower(strings.TrimSpace(temp)))
				}
			}
			if city != nil {
				userData["ct"] = shasum256(*city)
			}
			if region != nil {
				userData["st"] = shasum256(*region)
			}
			if country != nil {
				userData["country"] = shasum256(*country)
			}
			if v["ehash"] != nil {
				userData["em"] = v["ehash"]
			}
			if v["vid"] != nil {
				if temp, ok := v["vid"].(string); ok {
					userData["external_id"] = shasum256(strings.ToLower(strings.TrimSpace(temp)))
				}
			}
			//overwrite vid with uid if it exists
			if v["uid"] != nil {
				if temp, ok := v["uid"].(string); ok {
					userData["external_id"] = shasum256(strings.ToLower(strings.TrimSpace(temp)))
				}
			}
			if v["value"] != nil {
				data["value"] = v["value"]
			}
			if v["order_id"] != nil {
				data["order_id"] = v["order_id"]
			}
			if v["predicted_ltv"] != nil {
				data["predicted_ltv"] = v["predicted_ltv"]
			}
			if v["content_category"] != nil {
				data["content_category"] = v["content_category"]
			}
			if v["search_string"] != nil {
				data["search_string"] = v["search_string"]
			}
			if v["uri"] != nil {
				data["event_source_url"] = v["uri"]
			}
			data["user_data"] = userData
			data["event_name"] = ename
			data["event_time"] = now
			data["event_id"] = w.EventID
			j["data"] = []interface{}{data}
			if temp, ok := v["test_event_code"].(string); ok {
				j["test_event_code"] = strings.ToUpper(temp)
			}
			if i.AppConfig.Debug {
				fmt.Printf("[REQUEST] Facebook CAPI request payload: %s\n", j)
			}
			if byteArray, e := json.Marshal(j); e == nil {
				ctx, _ := context.WithCancel(context.Background())
				if request, e := http.NewRequest("POST", httpposturl, bytes.NewBuffer(byteArray)); e == nil {
					request.WithContext(ctx)
					request.Header.Set("Content-Type", "application/json; charset=UTF-8")
					client := &http.Client{Timeout: 30 * time.Second}
					if response, e := client.Do(request); e == nil {
						defer response.Body.Close()
						if response.StatusCode < 200 || response.StatusCode > 299 {
							if body, e := ioutil.ReadAll(response.Body); e == nil {
								err = fmt.Errorf("[ERROR] %s", string(body))
							} else {
								err = e
							}
						} else if i.AppConfig.Debug {
							fmt.Printf("[RESPONSE] Facebook CAPI request succeeded. %v\n", response.Status)
						}
					} else {
						err = e
					}
				} else {
					err = e
				}
			} else {
				err = e
			}
		}
	}
	if i.AppConfig.Debug && err != nil {
		fmt.Printf("[ERROR] Facebook CAPI request. %v\n", err)
	}
	return err
}

func (i *FacebookService) prune() error {
	//TODO: Facebook pruning
	return nil
}

func (i *FacebookService) serve(w *http.ResponseWriter, r *http.Request, s *ServiceArgs) error {
	return nil
}

//////////////////////////////////////// Facebook
// Listen
func (i *FacebookService) listen() error {
	return nil
}
