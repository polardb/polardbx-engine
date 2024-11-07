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


package ssot

import (
	"fmt"
	"testing"
)

type op interface {
	fmt.Stringer
}

type opTransfer struct {
	src, dst, amount int
	commitTs         int64
}

func (o opTransfer) String() string {
	return fmt.Sprintf("transfer[%v] %v -> %v (%v)", o.commitTs, o.src, o.dst, o.amount)
}

type opQuery struct {
	id            int
	snapshotTs    int64
	expectBalance int
	expectVersion int
}

func (o opQuery) String() string {
	return fmt.Sprintf("query[%v](%v) -> Record(%v, %v)", o.snapshotTs, o.id, o.expectBalance, o.expectVersion)
}

func TestSsot(t *testing.T) {
	var initRecords []Record
	for i := 0; i < 100; i++ {
		initRecords = append(initRecords, Record{
			Version: 0,
			Balance: 100,
		})
	}
	s := NewMemSsot(initRecords)

	ops := []op{
		opQuery{
			10,
			0,
			100,
			0,
		},
		opTransfer{
			1, 2, 1,
			10,
		},
		opQuery{
			1,
			5,
			100,
			0,
		},
		opQuery{
			1,
			15,
			99,
			1,
		},
		opQuery{
			2,
			15,
			101,
			1,
		},
		opTransfer{
			3, 2, 1,
			20,
		},
		opQuery{
			2,
			15,
			101,
			1,
		},
		opQuery{
			2,
			20,
			102,
			2,
		},
		opQuery{
			2,
			25,
			102,
			2,
		},
		opQuery{
			3,
			25,
			99,
			1,
		},
	}

	for _, opi := range ops {
		fmt.Println(opi.String())
		switch op := opi.(type) {
		case opTransfer:

			err := s.Transfer(op.src, op.dst, op.amount, op.commitTs)
			if err != nil {
				t.Fatal(err)
			}

		case opQuery:

			rec, err := s.Query(op.id, op.snapshotTs)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Balance != op.expectBalance || rec.Version != op.expectVersion {
				t.Fatalf("Expected: Record(balance=%v, version=%v), Actual: Record(balance=%v, version=%v)",
					op.expectBalance, op.expectVersion, rec.Balance, rec.Version)
			}
		}
	}
}
