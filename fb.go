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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

////////////////////////////////////////
// Interface Implementations
////////////////////////////////////////

func (i *FacebookService) connect() error {
	i.Configuration.Session = i
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

		httpposturl := "https://graph.facebook.com/v12.0/" + i.Configuration.Context + "/events?access_token=" + i.Configuration.Key

		var userData map[string]interface{}
		userData = make(map[string]interface{})
		var data map[string]interface{}
		data = make(map[string]interface{})
		var j map[string]interface{}
		j = make(map[string]interface{})
		userData["client_user_agent"] = w.Browser
		userData["client_ip_address"] = w.IP
		data["user_data"] = userData
		data["event_name"] = "TestEvent"
		data["event_time"] = time.Now().Unix() - 10000
		j["data"] = []interface{}{data}
		j["test_event_code"] = "TEST76022"
		if byteArray, e := json.Marshal(j); e == nil {
			if request, e := http.NewRequest("POST", httpposturl, bytes.NewBuffer(byteArray)); e == nil {
				request.Header.Set("Content-Type", "application/json; charset=UTF-8")
				client := &http.Client{}
				if response, e := client.Do(request); e == nil {
					defer response.Body.Close()
					if response.StatusCode < 200 || response.StatusCode > 299 {
						if body, e := ioutil.ReadAll(response.Body); e == nil {
							err = fmt.Errorf("[ERROR] %s", string(body))
						} else {
							err = e
						}
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
