package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

func GetGeoIP(pip net.IP) ([]byte, error) {
	if pip == nil {
		return nil, fmt.Errorf("Bad Request (IP)")
	}
	if kv == nil || kv.db == nil {
		return nil, fmt.Errorf("Service Unavailable")
	}
	if pip.To4() != nil {
		//Test, Google DNS - https://localhost:8443/ppi/v1/geoip?ip=8.8.8.8
		ips := strconv.FormatInt(int64(binary.BigEndian.Uint32(pip.To4())), 10)
		ipp := FixedLengthNumberString(10, ips)
		key := IDX_PREFIX_IPV4 + ipp
		var value []byte
		find := func(val []byte) error {
			if len(val) > 0 {
				value = val
				return nil
			} else {
				iter := kv.db.NewIter(kv.ro)
				defer iter.Close()
				for iter.SeekLT([]byte(key)); iteratorIsValid(iter); iter.Next() {
					k := iter.Key()
					val = iter.Value()
					var geoip GeoIP
					err := json.Unmarshal(val, &geoip)
					if err != nil {
						fmt.Println("Error marshalling :", key, string(k), string(val))
						return fmt.Errorf("Server Error (IP)")
					} else {
						if key > string(k) && FixedLengthNumberString(10, geoip.IPEnd) > FixedLengthNumberString(10, ipp) {
							value = val
							return nil
						} else {
							fmt.Println("Not found:", key, string(k), string(val))
							return fmt.Errorf("Not Found (IP)")
						}
					}
					break
				}
			}
			return fmt.Errorf("Not Found (IP)")
		}
		return value, kv.GetValue([]byte(key), find)
	} else if pip.To16() != nil {
		//Test, Google DNS - https://localhost:8443/ppi/v1/geoip?ip=2001:4860:4860::8888
		var hi uint64
		var lo uint64
		buf := bytes.NewReader(pip)
		binary.Read(buf, binary.BigEndian, &hi)
		binary.Read(buf, binary.BigEndian, &lo)
		ips := New(lo, hi).String()
		ipp := FixedLengthNumberString(39, ips)
		key := IDX_PREFIX_IPV6 + ipp
		var value []byte
		find := func(val []byte) error {
			if len(val) > 0 {
				value = val
				return nil
			} else {
				iter := kv.db.NewIter(kv.ro)
				defer iter.Close()
				for iter.SeekLT([]byte(key)); iteratorIsValid(iter); iter.Next() {
					k := iter.Key()
					val = iter.Value()
					var geoip GeoIP
					err := json.Unmarshal(val, &geoip)
					if err != nil {
						fmt.Println("Error marshalling :", key, string(k), string(val))
						return fmt.Errorf("Server Error (IP)")
					} else {
						if key > string(k) && FixedLengthNumberString(39, geoip.IPEnd) > FixedLengthNumberString(39, ipp) {
							value = val
							return nil
						} else {
							fmt.Println("Not found:", key, string(k), string(val))
							return fmt.Errorf("Not Found (IP)")
						}
					}
					break
				}

			}
			return fmt.Errorf("Not Found (IP)")
		}
		return value, kv.GetValue([]byte(key), find)
	} else {
		return nil, fmt.Errorf("Bad Request (IP)")
	}
}
