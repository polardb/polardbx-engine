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


package transfer

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type TSO interface {
	Start() int64
	Next() int64
}

func IncTs(old int64) int64 {
	return old + (1 << TsReservedBits)
}

// TSO generates increasing timestamp.
type localTSO struct {
	clock int64
	start int64
}

// Timestamp format: physical time in milliseconds (42 bits) and logical clock (22 bits)
const TsPhysicalBits = 42
const TsLogicalBits = 16
const TsReservedBits = 6

// NewTSO create a TSO instance with current time.
func NewTSO() *localTSO {
	return &localTSO{
		start: (time.Now().UnixNano() / 1000000) << (TsLogicalBits + TsReservedBits),
	}
}

func (tso *localTSO) Start() int64 {
	return tso.start
}

// Next generate next timestamp
func (tso *localTSO) Next() int64 {
	// Make sure clock is beyond the current time
	threshold := (time.Now().UnixNano() / 1000000) << (TsLogicalBits + TsReservedBits)
	last := atomic.LoadInt64(&tso.clock)
	for last < threshold && atomic.CompareAndSwapInt64(&tso.clock, last, threshold) {
		last = atomic.LoadInt64(&tso.clock)
	}
	// Do allocate a timestamp
	return atomic.AddInt64(&tso.clock, 1<<TsReservedBits)
}

func TSOHandler(tso TSO) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/current", func(w http.ResponseWriter, r *http.Request) {
		current := tso.Next()
		w.Write([]byte(strconv.FormatInt(current, 10) + "\n"))
	})
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		start := tso.Start()
		w.Write([]byte(strconv.FormatInt(start, 10) + "\n"))
	})

	return mux
}

type clusterTSO struct {
	baseURL string
}

// TODO: return err
func (tso *clusterTSO) Next() int64 {
	res, err := http.Get(tso.baseURL + "/current")
	if err != nil {
		// TODO
		panic(err)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	next, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil {
		panic(err)
	}
	return next
}

// TODO: return err.
// TODO: cache start.
func (tso *clusterTSO) Start() int64 {
	res, err := http.Get(tso.baseURL + "/start")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	start, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil {
		panic(err)
	}
	return start
}

func NewClusterTSO(baseURL string) TSO {
	return &clusterTSO{
		baseURL: baseURL,
	}
}
