/*===----------- cql.go - c* utility written in go  -------------===
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
	"context"
	"fmt"
	"os"
	"reflect"
	"text/tabwriter"

	"github.com/gocql/gocql"
)

func printCStarQuery(ctx context.Context, session *gocql.Session, stmt string, values ...interface{}) error {
	iter := session.Query(stmt, values...).WithContext(ctx).Iter()
	fmt.Println(stmt)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ',
		0)
	for i, columnInfo := range iter.Columns() {
		if i > 0 {
			fmt.Fprint(w, "\t| ")
		}
		fmt.Fprintf(w, "%s (%s)", columnInfo.Name, columnInfo.TypeInfo)
	}

	for {
		rd, err := iter.RowData()
		if err != nil {
			return err
		}
		if !iter.Scan(rd.Values...) {
			break
		}
		fmt.Fprint(w, "\n")
		for i, val := range rd.Values {
			if i > 0 {
				fmt.Fprint(w, "\t| ")
			}

			fmt.Fprint(w, reflect.Indirect(reflect.ValueOf(val)).Interface())
		}
	}

	fmt.Fprint(w, "\n")
	w.Flush()
	fmt.Println()

	return iter.Close()
}
