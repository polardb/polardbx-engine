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
