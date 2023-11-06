package main

import (
	"context"
	"database/sql"
	"flag"
	"os"

	"transfer/pkg/transfer"
)

func main() {
	var dsn = flag.String("dsn", os.Getenv("MYSQL_TRANSFER_DSN"), "MySQL Data Source Name")

	flag.Parse()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		panic(err)
	}
	if err := transfer.RecoverAll(context.Background(), db); err != nil {
		panic(err)
	}
}
