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
	"time"

	"github.com/gocql/gocql"
)

type viewport struct {
	W int64 `cql:"w"`
	H int64 `cql:"h"`
}

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

type payment struct {
	InvoiceID       *gocql.UUID `cql:"invid"`
	Invoiced        *time.Time  `cql:"invoiced"`
	Product         *string     `cql:"product"`
	ProductID       *gocql.UUID `cql:"pid"`
	ProductCategory *string     `cql:"pcat"`
	Manufacturer    *string     `cql:"man"`
	Model           *string     `cql:"model"`
	Quantity        *float64    `cql:"qty"`
	Duration        *string     `cql:"duration"`
	Starts          *time.Time  `cql:"starts"`
	Ends            *time.Time  `cql:"ends"`
	Currency        *string     `cql:"currency"`
	Country         *string     `cql:"country"`
	RegionCode      *string     `cql:"rcode"`
	Region          *string     `cql:"region"`
	Price           *float64    `cql:"price"`
	Discount        *float64    `cql:"discount"`
	Revenue         *float64    `cql:"revenue"`
	Margin          *float64    `cql:"margin"`
	Cost            *float64    `cql:"cost"`
	Tax             *float64    `cql:"tax"`
	TaxRate         *float64    `cql:"tax_rate"`
	Commission      *float64    `cql:"commission"`
	Referral        *float64    `cql:"referral"`
	Fees            *float64    `cql:"fees"`
	Subtotal        *float64    `cql:"subtotal"`
	Total           *float64    `cql:"total"`
	Payment         *float64    `cql:"payment"`
	Paid            *time.Time  `cql:"paid"`
}

// NOTE: due to current implementation details it is not currently possible to use
// a pointer receiver type for the UDTMarshaler interface to handle UDT's
func (p payment) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {

	switch name {
	case "invid":
		return gocql.Marshal(info, p.InvoiceID)
	case "invoiced":
		return gocql.Marshal(info, p.Invoiced)
	case "product":
		return gocql.Marshal(info, p.Product)
	case "pid":
		return gocql.Marshal(info, p.ProductID)
	case "pcat":
		return gocql.Marshal(info, p.ProductCategory)
	case "man":
		return gocql.Marshal(info, p.Manufacturer)
	case "model":
		return gocql.Marshal(info, p.Model)
	case "qty":
		return gocql.Marshal(info, p.Quantity)
	case "duration":
		return gocql.Marshal(info, p.Duration)
	case "starts":
		return gocql.Marshal(info, p.Starts)
	case "ends":
		return gocql.Marshal(info, p.Ends)
	case "currency":
		return gocql.Marshal(info, p.Currency)
	case "country":
		return gocql.Marshal(info, p.Country)
	case "rcode":
		return gocql.Marshal(info, p.RegionCode)
	case "region":
		return gocql.Marshal(info, p.Region)
	case "price":
		return gocql.Marshal(info, p.Price)
	case "discount":
		return gocql.Marshal(info, p.Discount)
	case "revenue":
		return gocql.Marshal(info, p.Revenue)
	case "margin":
		return gocql.Marshal(info, p.Margin)
	case "cost":
		return gocql.Marshal(info, p.Cost)
	case "tax":
		return gocql.Marshal(info, p.Tax)
	case "tax_rate":
		return gocql.Marshal(info, p.TaxRate)
	case "commission":
		return gocql.Marshal(info, p.Commission)
	case "referral":
		return gocql.Marshal(info, p.Referral)
	case "fees":
		return gocql.Marshal(info, p.Fees)
	case "subtotal":
		return gocql.Marshal(info, p.Subtotal)
	case "total":
		return gocql.Marshal(info, p.Total)
	case "payment":
		return gocql.Marshal(info, p.Payment)
	case "paid":
		return gocql.Marshal(info, p.Paid)
	default:
		return nil, fmt.Errorf("unknown column for position: %q", name)
	}
}

func (p *payment) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "invid":
		return gocql.Unmarshal(info, data, &p.InvoiceID)
	case "invoiced":
		return gocql.Unmarshal(info, data, &p.Invoiced)
	case "product":
		return gocql.Unmarshal(info, data, &p.Product)
	case "pid":
		return gocql.Unmarshal(info, data, &p.ProductID)
	case "pcat":
		return gocql.Unmarshal(info, data, &p.ProductCategory)
	case "man":
		return gocql.Unmarshal(info, data, &p.Manufacturer)
	case "model":
		return gocql.Unmarshal(info, data, &p.Model)
	case "qty":
		return gocql.Unmarshal(info, data, &p.Quantity)
	case "duration":
		return gocql.Unmarshal(info, data, &p.Duration)
	case "starts":
		return gocql.Unmarshal(info, data, &p.Starts)
	case "ends":
		return gocql.Unmarshal(info, data, &p.Ends)
	case "currency":
		return gocql.Unmarshal(info, data, &p.Currency)
	case "country":
		return gocql.Unmarshal(info, data, &p.Country)
	case "rcode":
		return gocql.Unmarshal(info, data, &p.RegionCode)
	case "region":
		return gocql.Unmarshal(info, data, &p.Region)
	case "price":
		return gocql.Unmarshal(info, data, &p.Price)
	case "discount":
		return gocql.Unmarshal(info, data, &p.Discount)
	case "revenue":
		return gocql.Unmarshal(info, data, &p.Revenue)
	case "margin":
		return gocql.Unmarshal(info, data, &p.Margin)
	case "cost":
		return gocql.Unmarshal(info, data, &p.Cost)
	case "tax":
		return gocql.Unmarshal(info, data, &p.Tax)
	case "tax_rate":
		return gocql.Unmarshal(info, data, &p.TaxRate)
	case "commission":
		return gocql.Unmarshal(info, data, &p.Commission)
	case "referral":
		return gocql.Unmarshal(info, data, &p.Referral)
	case "fees":
		return gocql.Unmarshal(info, data, &p.Fees)
	case "subtotal":
		return gocql.Unmarshal(info, data, &p.Subtotal)
	case "total":
		return gocql.Unmarshal(info, data, &p.Total)
	case "payment":
		return gocql.Unmarshal(info, data, &p.Payment)
	case "paid":
		return gocql.Unmarshal(info, data, &p.Paid)
	default:
		return fmt.Errorf("unknown column for position: %q", name)
	}
}
