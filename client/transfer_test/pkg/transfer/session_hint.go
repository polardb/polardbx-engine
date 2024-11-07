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
	"fmt"
	"log"
	"strings"
)

type SessionHint struct {
	partitionNames []string
}

func NewSessionHint() *SessionHint {
	return &SessionHint{
		partitionNames: []string{},
	}
}

func (s *SessionHint) Init(ctx context.Context, conf *Config, connector Connector) (err error) {
	if conf.ForXDB {
		return fmt.Errorf("not supportted")
	}

	db := connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("create conn failed: %w", err)
	}
	defer conn.Close()

	// Get all group names.
	tableName := RouteScan(conf)()[0]
	rows, err := conn.QueryContext(ctx, "show topology from "+tableName)
	if err != nil {
		return fmt.Errorf("failed to query on %s: %w", tableName, err)
	}
	defer rows.Close()

	m := make(map[string]int)
	updateMap := func(m map[string]int, key string) {
		if _, ok := m[key]; !ok {
			m[key] = 0
		}
		m[key]++
	}

	auto := false
	if columnNames, err := rows.Columns(); len(columnNames) == 6 {
		if err != nil {
			return fmt.Errorf("failed to scan: %w", err)
		}
		auto = true
	}

	for rows.Next() {
		var id, groupName, tbName, partitionName, physicalName, dnId string
		if auto {
			err = rows.Scan(&id, &groupName, &tbName, &partitionName, &physicalName, &dnId)
		} else {
			err = rows.Scan(&id, &groupName, &tbName, &partitionName)
		}
		if err != nil {
			return fmt.Errorf("failed to scan: %w", err)
		}
		if auto {
			s.partitionNames = append(s.partitionNames, partitionName)
		} else {
			updateMap(m, groupName)
		}
	}

	// Generate partition names.
	if !auto {
		for groupName, cnt := range m {
			for i := 0; i < cnt; i++ {
				s.partitionNames = append(s.partitionNames, fmt.Sprintf(groupName+":%d", i))
			}
		}
	}

	log.Print("all partitions: " + strings.Join(s.partitionNames, ", "))

	return nil
}
