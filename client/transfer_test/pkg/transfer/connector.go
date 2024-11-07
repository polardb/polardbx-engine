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
