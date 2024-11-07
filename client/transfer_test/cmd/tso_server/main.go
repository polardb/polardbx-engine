/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"transfer/pkg/transfer"
)

func main() {
	port := flag.Int("port", 6789, "Port")

	tso := transfer.NewTSO()

	http.HandleFunc("/current", func(w http.ResponseWriter, r *http.Request) {
		current := tso.Next()
		w.Write([]byte(strconv.FormatInt(current, 10) + "\n"))
	})
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		start := tso.Start()
		w.Write([]byte(strconv.FormatInt(start, 10) + "\n"))
	})
	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("Serve on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}
