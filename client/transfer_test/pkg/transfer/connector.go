package transfer

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type Connector interface {
	Get(ctx context.Context) (*sql.Conn, error)
	Raw() *sql.DB
}

type dbWrapper struct {
	db *sql.DB
}

func (c *dbWrapper) Raw() *sql.DB {
	return c.db
}

func (c *dbWrapper) Get(ctx context.Context) (*sql.Conn, error) {
	return c.db.Conn(ctx)
}

func NewConnector(db *sql.DB) Connector {
	return &dbWrapper{db}
}
