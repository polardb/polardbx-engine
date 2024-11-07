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
	"fmt"
	"strings"
)

const (
	tablePrefix = "accounts"
	indexPrefix = "index_balance"
)

func PrepareData(ctx context.Context, db *sql.DB, conf *Config) (err error) {
	_, err = db.ExecContext(ctx,
		`DROP TABLE IF EXISTS accounts`,
	)
	if err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}

	type ddlGenSpec struct {
		tableName    string
		indexName    string
		createSuffix string
	}

	var specs []ddlGenSpec
	if conf.Paritions == 0 {
		specs = append(specs, ddlGenSpec{
			tableName:    tablePrefix,
			indexName:    indexPrefix,
			createSuffix: conf.CreateTableSuffix,
		})
	} else {
		for i := 0; i < conf.Paritions; i++ {
			specs = append(specs, ddlGenSpec{
				tableName: fmt.Sprintf("%s_%06d", tablePrefix, i),
				indexName: fmt.Sprintf("%s_%06d", indexPrefix, i),
			})
		}
	}
	var ddls []string
	for _, spec := range specs {
		ddls = append(ddls, fmt.Sprintf("DROP TABLE IF EXISTS %s", spec.tableName))

		var ddl string
		if conf.DbType == "postgres" {
			ddl = fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, balance INT NOT NULL, version INT NOT NULL DEFAULT '0') %s", spec.tableName, spec.createSuffix)
		} else {
			ddl = fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, balance INT NOT NULL, version INT NOT NULL DEFAULT '0', gmt_modified TIMESTAMP DEFAULT NOW() ON UPDATE NOW()) %s", spec.tableName, spec.createSuffix)
			ddl += " COMMENT 'Created by transfer-test'"
		}
		ddls = append(ddls, ddl)

		ddl = fmt.Sprintf("CREATE INDEX %s ON %s (balance)", spec.indexName, spec.tableName)
		if conf.DbType == "mysql" {
			ddl += " COMMENT 'Created by transfer-test'"
		}
		ddls = append(ddls, ddl)
	}

	for _, ddl := range ddls {
		fmt.Println(ddl)
		_, err = db.ExecContext(ctx, ddl)
		if err != nil {
			return fmt.Errorf("DDL failed: %w", err)
		}
	}

	dmlTuples := make(map[string][]string)
	for i := 0; i < conf.RowCount; i++ {
		id := i
		tableName := RoutePoint(conf)(id)
		dmlTuples[tableName] = append(dmlTuples[tableName],
			fmt.Sprintf("(%v, %v)", i, conf.InitialBalance),
		)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("create conn failed: %w", err)
	}
	defer conn.Close()
	conn.ExecContext(ctx, "BEGIN")
	success := false
	defer func() {
		if !success {
			conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	for tableName, tuples := range dmlTuples {
		n := len(tuples)
		const STEP = 1000
		for i := 0; i < n; i += STEP {
			var j int
			if i+STEP >= n {
				j = n
			} else {
				j = i + STEP
			}
			dml := "INSERT INTO " + tableName + " (id, balance) VALUES " + strings.Join(tuples[i:j], ", ")
			_, err = conn.ExecContext(ctx, dml)
		}
		if err != nil {
			return fmt.Errorf("DML failed: %w", err)
		}
	}

	_, err = conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		return err
	} else {
		success = true
	}

	return nil
}

func SetGlobalIsolationLevel(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	return err
}

func RecoverAll(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "XA RECOVER")
	if err != nil {
		return fmt.Errorf("XA RECOVER failed: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v1, gtridLen, bqualLen int
		var data string
		err = rows.Scan(&v1, &gtridLen, &bqualLen, &data)
		if err != nil {
			return fmt.Errorf("scan XA RECOVER failed: %w", err)
		}
		var sql string
		if bqualLen != 0 {
			sql = fmt.Sprintf("XA ROLLBACK '%v','%v'", data[:gtridLen], data[gtridLen:gtridLen+bqualLen])
		} else {
			sql = fmt.Sprintf("XA ROLLBACK '%v'", data)
		}
		_, err = db.ExecContext(ctx, sql)
		if err != nil {
			return fmt.Errorf("%v failed: %w", sql, err)
		}
	}
	return nil
}
