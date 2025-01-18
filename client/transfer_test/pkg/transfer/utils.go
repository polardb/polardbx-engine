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
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"time"
)

func randomString() string {
	length := rand.Intn(500) + 10
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomId(lowerBound int) int {
	var id int
	id = rand.Intn(math.MaxInt32-lowerBound+1) + lowerBound
	return id
}

func rand2(upperbound int) (int, int) {
	var a1, a2 int
	a1 = rand.Intn(upperbound)
	for {
		a2 = rand.Intn(upperbound)
		if a2 != a1 {
			break
		}
	}
	return a1, a2
}

func maxInt64(val1 int64, vals ...int64) int64 {
	res := val1
	for _, val := range vals {
		if val > res {
			res = val
		}
	}
	return res
}

func getUserByID(trx Trx, id int) string {
	var user string
	err := trx.QueryRow(fmt.Sprintf("SELECT user FROM accounts WHERE id = %d", id)).Scan(&user)
	if err != nil {
		return ""
	}
	return user
}

func getRandomID(trx Trx) (int, error) {
	var id int

	for {
		err := trx.QueryRow("SELECT id FROM accounts ORDER BY RAND() LIMIT 1").Scan(&id)
		if err != nil {
			return 0, err
		}

		err = trx.QueryRow(fmt.Sprintf("SELECT id FROM accounts WHERE id = %d FOR UPDATE", id)).Scan(&id)
		if err == nil {
			return id, nil
		}

		if err == sql.ErrNoRows {
			continue
		} else {
			return 0, err
		}
	}
}

func getUserPair(trx1 Trx, trx2 Trx) (string, string, error) {
	const maxRetries = 5
	var users []string
	retries := 0

	for retries < maxRetries {
		retries++
		rows, err := trx1.Query("SELECT user FROM accounts WHERE user IS NOT NULL ORDER BY RAND() LIMIT 2")
		if err == sql.ErrNoRows {
			continue
		} else if err != nil {
			return "", "", err
		}
		defer rows.Close()

		for rows.Next() {
			var user string
			if err := rows.Scan(&user); err != nil {
				return "", "", err
			}
			users = append(users, user)
		}

		if len(users) < 2 {
			users = nil
			continue // Not enough users found, retry
		}

		// We now have two users
		lockFirst, lockSec := users[0], users[1]
		if lockFirst > lockSec {
			lockFirst, lockSec = lockSec, lockFirst // Ensure consistent locking order
		}

		err = trx1.QueryRow(fmt.Sprintf("SELECT user FROM accounts WHERE user = '%s' FOR UPDATE", lockFirst)).Scan(&lockFirst)
		if err == sql.ErrNoRows {
			continue
		} else if err != nil {
			return "", "", err
		}

		err = trx2.QueryRow(fmt.Sprintf("SELECT user FROM accounts WHERE user = '%s' FOR UPDATE", lockSec)).Scan(&lockSec)
		if err == nil {
			return lockFirst, lockSec, nil
		} else if err == sql.ErrNoRows {
			return "", "", nil
		} else {
			return "", "", err
		}
	}
	return "", "", nil
}

func getRandomUser(trx Trx) (string, error) {
	const maxRetries = 5
	var user string
	retries := 0

	for retries < maxRetries {
		retries++
		err := trx.QueryRow("SELECT user FROM accounts WHERE user IS NOT NULL ORDER BY RAND() LIMIT 1").Scan(&user)
		if err == sql.ErrNoRows {
			continue
		} else if err != nil {
			return "", err
		}

		err = trx.QueryRow(fmt.Sprintf("SELECT user FROM accounts WHERE user = '%s' FOR UPDATE", user)).Scan(&user)
		if err == nil {
			return user, nil
		} else if err == sql.ErrNoRows {
			continue
		} else {
			return "", err
		}
	}
	return "", nil
}
