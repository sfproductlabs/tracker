/*===----------- udt.go - udt for cassandra data types      -------------===
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
	"fmt"

	"github.com/gocql/gocql"
)

type geo_point struct {
	Lat float64 `cql:"lat"`
	Lon float64 `cql:"lon"`
}

// NOTE: due to current implementation details it is not currently possible to use
// a pointer receiver type for the UDTMarshaler interface to handle UDT's
func (p geo_point) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "lat":
		return gocql.Marshal(info, p.Lat)
	case "lon":
		return gocql.Marshal(info, p.Lon)
	default:
		return nil, fmt.Errorf("unknown column for position: %q", name)
	}
}

func (p *geo_point) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "lat":
		return gocql.Unmarshal(info, data, &p.Lat)
	case "lon":
		return gocql.Unmarshal(info, data, &p.Lon)
	default:
		return fmt.Errorf("unknown column for position: %q", name)
	}
}
