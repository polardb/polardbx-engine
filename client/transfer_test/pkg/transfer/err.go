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
	"fmt"
	"strings"
)

type TotalNotMatchError struct {
	Ts       int64
	Accounts []Account
	Sum      int
	Expect   int
}

func (e *TotalNotMatchError) Error() string {
	var strs []string
	strs = append(strs, "Inconsistency Detected!")
	for _, account := range e.Accounts {
		strs = append(strs, fmt.Sprintf("%v: %v", account.ID, account.Balance))
	}
	strs = append(strs, fmt.Sprintf("Read with ts: %v, expect: %v, actual: %v", e.Ts, e.Expect, e.Sum))
	return strings.Join(strs, "\n")
}

type SnapshotTooOldError struct {
	Ts int64
}

func (e *SnapshotTooOldError) Error() string {
	return fmt.Sprintf("Snapshot too old: %v", e.Ts)
}
