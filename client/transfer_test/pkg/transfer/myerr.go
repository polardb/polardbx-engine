package transfer

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

func isMySQLError(err error, codes ...uint16) bool {
	var myerr *mysql.MySQLError
	if !errors.As(err, &myerr) {
		return false
	}
	for _, code := range codes {
		if myerr.Number == code {
			return true
		}
	}
	return false
}
