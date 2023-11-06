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
