package transfer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"transfer/pkg/logutils"

	"go.uber.org/zap"
)

func check(ctx context.Context, p basePlugin, ts int64, useCts bool, id string, useSecIdx bool, useCurrSnapshot bool) (err error) {
	db := p.connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create conn failed.")
		return nil
	}
	defer conn.Close()

	// Use read-only transactions with a probability of 50%.
	readOnly := !(rand.Intn(2) < 1)
	if readOnly {
		_, err := conn.ExecContext(ctx, "START TRANSACTION READ ONLY ")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("START TRANSACTION READ ONLY failed.")
			return nil
		}
	} else {
		_, err := conn.ExecContext(ctx, "BEGIN")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("BEGIN failed.")
			return nil
		}
	}
	defer func() {
		_, err := conn.ExecContext(ctx, "ROLLBACK")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("ROLLBACK failed.")
			return
		}
	}()
	if useCts {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("SET innodb_snapshot_seq = %v", ts))
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("SET innodb_snapshot_seq failed.")
			return fmt.Errorf("failed to query: %w", err)
		}
	}
	if useCurrSnapshot {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("SET innodb_current_snapshot_seq = on"))
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("SET innodb_current_snapshot_seq failed.")
			return fmt.Errorf("failed to query: %w", err)
		}
	}
	if err := checkInternal(ctx, conn, p, ts, id, useSecIdx); err != nil {
		return err
	}

	return nil
}

func checkSessionHint(ctx context.Context, p basePlugin, ts int64, id string) (err error) {
	db := p.connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create conn failed.")
		return nil
	}
	defer conn.Close()

	// Use read-only transactions with a probability of 50%.
	readOnly := !(rand.Intn(2) < 1)
	if readOnly {
		_, err := conn.ExecContext(ctx, "START TRANSACTION READ ONLY ")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("START TRANSACTION READ ONLY failed.")
			return nil
		}
	} else {
		_, err := conn.ExecContext(ctx, "BEGIN")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("BEGIN failed.")
			return nil
		}
	}
	defer func() {
		_, err := conn.ExecContext(ctx, "ROLLBACK")
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Error("ROLLBACK failed.")
			return
		}
	}()

	if err := checkSessionHintInternal(ctx, conn, p, ts, id); err != nil {
		return fmt.Errorf("[session hint] check inconsistency failed, readonly: %t, %w", readOnly, err)
	}

	return nil
}

func checkFlashback(ctx context.Context, p basePlugin, ts int64, id string) (err error) {
	db := p.connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("create conn failed: %w", err)
	}
	defer conn.Close()

	// For flashback query, no need to start a trx.
	err = checkFlashbackInternal(ctx, conn, p, ts, id)
	if err != nil {
		return err
	}
	return nil
}

func checkFlashbackSessionHint(ctx context.Context, p basePlugin, ts int64, id string) (err error) {
	db := p.connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("create conn failed: %w", err)
	}
	defer conn.Close()

	// For flashback query, no need to start a trx.
	err = checkFlashbackSessionHintInternal(ctx, conn, p, ts, id)
	if err != nil {
		return err
	}
	return nil
}

func checkPeriodically(ctx context.Context, p basePlugin, ts int64, interval, total time.Duration, useCts bool, id string, useSecIdx bool) (err error) {
	db := p.connector.Raw()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("create conn failed: %w", err)
	}
	defer conn.Close()

	trx, err := conn.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}
	if useCts {
		_, err = trx.ExecContext(ctx, fmt.Sprintf("SET innodb_snapshot_seq = %v", ts))
		if err != nil {
			return fmt.Errorf("failed to query: %w", err)
		}
	}
	totalTm := time.After(total)
	for {
		if err := checkInternal(ctx, conn, p, ts, id, useSecIdx); err != nil {
			logutils.FromContext(ctx).Error("check internal failed.", zap.Error(err))
			if err := trx.Rollback(); err != nil {
				logutils.FromContext(ctx).Error("Rollback failed.", zap.Error(err))
			}
			return err
		}
		intervalTm := time.After(interval)
		select {
		case <-totalTm:
			if err := trx.Commit(); err != nil {
				return err
			}
			return
		case <-intervalTm:
			continue
		case <-ctx.Done():
			if err := trx.Rollback(); err != nil {
				logutils.FromContext(ctx).Error("Rollback failed.", zap.Error(err))
			}
			return ctx.Err()
		}
	}
}

type Account struct {
	ID      int
	Balance int
	Version int
}

func GetAccounts(ctx context.Context, conn *sql.Conn, tables []string, hint string, sessionVar string, useSecIdx bool) ([]Account, error) {
	if sessionVar != "" {
		_, err := conn.ExecContext(ctx, sessionVar)
		if err != nil {
			return nil, err
		}
	}

	var records []Account
	for _, tableName := range tables {
		if partials, err := func(tableName string) (partialRecods []Account, err error) {
			var query = "SELECT id, balance, version FROM " + tableName + " ORDER BY balance"
			if useSecIdx {
				query = "SELECT id, balance, version FROM " + tableName + " force index(index_balance) ORDER BY balance"
			}
			rows, err := conn.QueryContext(ctx, hint+query)
			if err != nil {
				return nil, fmt.Errorf("failed to query on %s: %w", tableName, err)
			}
			defer rows.Close()
			for rows.Next() {
				var rec Account
				err = rows.Scan(&rec.ID, &rec.Balance, &rec.Version)
				if err != nil {
					return nil, fmt.Errorf("failed to scan: %w", err)
				}
				partialRecods = append(partialRecods, rec)
			}
			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to scan: %w", err)
			}
			return
		}(tableName); err != nil {
			return nil, err
		} else {
			records = append(records, partials...)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
	return records, nil
}

func GetAccountsWithSessionHint(ctx context.Context, conn *sql.Conn, tables []string, hint string,
	sessionVar string, sessionHint *SessionHint) ([]Account, error) {
	var records []Account
	defer func() {
		_, err := conn.ExecContext(ctx, "SET PARTITION_HINT = ''")
		if err != nil {
			return
		}
	}()

	if sessionVar != "" {
		_, err := conn.ExecContext(ctx, sessionVar)
		if err != nil {
			return nil, err
		}
	}

	for _, tableName := range tables {
		if partials, err := func(tableName string) (partialRecods []Account, err error) {
			for _, partitionName := range sessionHint.partitionNames {
				// Set partition hint.
				conn.ExecContext(ctx, fmt.Sprintf("SET PARTITION_HINT = '%s'", partitionName))
				// Select from this partition.
				rows, err := conn.QueryContext(ctx, hint+"SELECT id, balance, version FROM "+tableName+" ORDER BY balance")
				if err != nil {
					return nil, fmt.Errorf("[session hint]failed to query on %s: %w", tableName, err)
				}
				defer rows.Close()
				for rows.Next() {
					var rec Account
					err = rows.Scan(&rec.ID, &rec.Balance, &rec.Version)
					if err != nil {
						return nil, fmt.Errorf("[session hint]failed to scan: %w", err)
					}
					partialRecods = append(partialRecods, rec)
				}
				if err := rows.Err(); err != nil {
					return nil, fmt.Errorf("[session hint]failed to scan: %w", err)
				}
			}
			return
		}(tableName); err != nil {
			return nil, err
		} else {
			records = append(records, partials...)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
	return records, nil
}

func GetAccountsWithFlashbackQuery(ctx context.Context, conn *sql.Conn, tables []string, hint string,
	sessionVar string, min int, max int) ([]Account, error) {
	// Get a random timestamp.
	randomSeconds := rand.Intn(max-min) + min
	t := time.Now().Add(-1 * time.Duration(randomSeconds) * time.Second)
	flashbackTimestamp := fmt.Sprintf(" as of timestamp '%s' ", t.Format("2006-01-02 15:04:05"))

	if sessionVar != "" {
		_, err := conn.ExecContext(ctx, sessionVar)
		if err != nil {
			return nil, err
		}
	}

	var records []Account
	for _, tableName := range tables {
		if partials, err := func(tableName string) (partialRecods []Account, err error) {
			query := hint + "SELECT id, balance, version FROM " + tableName + flashbackTimestamp + " ORDER BY balance"
			rows, err := conn.QueryContext(ctx, query)
			if err != nil {
				return nil, fmt.Errorf("failed to query on %s, query: %s, err: %w", tableName, query, err)
			}
			defer rows.Close()
			for rows.Next() {
				var rec Account
				err = rows.Scan(&rec.ID, &rec.Balance, &rec.Version)
				if err != nil {
					return nil, fmt.Errorf("failed to scan: %w", err)
				}
				partialRecods = append(partialRecods, rec)
			}
			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to scan: %w", err)
			}
			return
		}(tableName); err != nil {
			return nil, err
		} else {
			records = append(records, partials...)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
	return records, nil
}

func GetAccountsWithFlashbackQueryAndSessionHint(ctx context.Context, conn *sql.Conn, tables []string, hint string, sessionHint *SessionHint) ([]Account, error) {
	randomSeconds := rand.Intn(10) + 10
	t := time.Now().Add(-1 * time.Duration(randomSeconds) * time.Second)
	flashbackTimestamp := fmt.Sprintf(" as of timestamp '%s' ", t.Format("2006-01-02 15:04:05"))

	var records []Account
	defer conn.ExecContext(ctx, "SET PARTITION_HINT = ''")
	for _, tableName := range tables {
		if partials, err := func(tableName string) (partialRecods []Account, err error) {
			for _, partitionName := range sessionHint.partitionNames {
				// Set partition hint.
				conn.ExecContext(ctx, fmt.Sprintf("SET PARTITION_HINT = '%s'", partitionName))
				// Select from this partition.
				rows, err := conn.QueryContext(ctx, hint+"SELECT id, balance, version FROM "+tableName+flashbackTimestamp+" ORDER BY balance")
				if err != nil {
					return nil, fmt.Errorf("[flashback + session hint]failed to query on %s: %w", tableName, err)
				}
				defer rows.Close()
				for rows.Next() {
					var rec Account
					err = rows.Scan(&rec.ID, &rec.Balance, &rec.Version)
					if err != nil {
						return nil, fmt.Errorf("[flashback + session hint]failed to scan: %w", err)
					}
					partialRecods = append(partialRecods, rec)
				}
				if err := rows.Err(); err != nil {
					return nil, fmt.Errorf("[flashback + session hint]failed to scan: %w", err)
				}
			}
			return
		}(tableName); err != nil {
			return nil, err
		} else {
			records = append(records, partials...)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
	return records, nil
}

func GetAccount(ctx context.Context, conn *sql.Conn, tableName string, id int, hint string) (acc Account, err error) {
	rows, err := conn.QueryContext(ctx, hint+fmt.Sprintf("SELECT id, balance, version FROM %s WHERE id = %d", tableName, id))
	if err != nil {
		return acc, fmt.Errorf("failed to query on %s: %w", tableName, err)
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&acc.ID, &acc.Balance, &acc.Version)
		if err != nil {
			return acc, fmt.Errorf("failed to find account %d: %w", id, err)
		}
		return acc, nil
	} else if err = rows.Err(); err != nil {
		return acc, fmt.Errorf("failed to find account %d: %w", id, err)
	} else {
		return acc, fmt.Errorf("account %d not found on %s", id, tableName)
	}
}

func checkInternal(ctx context.Context, conn *sql.Conn, p basePlugin, ts int64, id string, useSecIdx bool) (err error) {
	defer func() {
		if isMySQLError(err, 7510) {
			err = &SnapshotTooOldError{Ts: ts}
		} else if isMySQLError(err, 7527) {
			// global query timeout error
			err = nil
		}
	}()

	accounts, err := GetAccounts(ctx, conn, RouteScan(p.conf)(), fmt.Sprintf("/*%s*/", id), "", useSecIdx)

	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Info("GetAccounts failed.")
		return err
	}

	err = checkAccounts(accounts, p, ts, false)
	if err != nil {
		return fmt.Errorf("[%s]check failed : %w", id, err)
	}

	return nil
}

func checkSessionHintInternal(ctx context.Context, conn *sql.Conn, p basePlugin, ts int64, id string) (err error) {
	defer func() {
		if isMySQLError(err, 7510) {
			err = &SnapshotTooOldError{Ts: ts}
		}
	}()

	accounts, err := GetAccountsWithSessionHint(ctx, conn, RouteScan(p.conf)(), fmt.Sprintf("/*%s*/", id),
		"", p.sessionHint)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("GetAccountsWithSessionHint failed.")
		return
	}

	err = checkAccounts(accounts, p, ts, false)
	if err != nil {
		return fmt.Errorf("[%s]check with session hint failed : %w", id, err)
	}

	return nil
}

func checkFlashbackInternal(ctx context.Context, conn *sql.Conn, p basePlugin, ts int64, id string) (err error) {
	defer func() {
		if isMySQLError(err, 7510) {
			err = &SnapshotTooOldError{Ts: ts}
		}
	}()

	tables := RouteScan(p.conf)()

	accounts, err := GetAccountsWithFlashbackQuery(ctx, conn, tables, fmt.Sprintf("/*%s*/", id),
		"", p.conf.CheckFlashback.MinSeconds, p.conf.CheckFlashback.MaxSeconds)
	if err != nil {
		// Snapshot too old.
		if strings.Contains(err.Error(), "The snapshot to find is out of range, please adjust scn history configuration") {
			return nil
		}
		if strings.Contains(err.Error(), "Snapshot too old") {
			return nil
		}
		// Table has been changed in the flashback time, e.g. table has not been created at that time.
		if strings.Contains(err.Error(), " The definition of the table required by the flashback query has changed") {
			return nil
		}
		if strings.Contains(err.Error(), "Table definition has changed, please retry transaction") {
			return nil
		}
		logutils.FromContext(ctx).With(zap.Error(err)).Error("GetAccountsWithFlashbackQuery failed.")
		return nil
	}

	err = checkAccounts(accounts, p, ts, true)
	if err != nil {
		return fmt.Errorf("[%s]check with flashback query failed, err: %w", id, err)
	}

	return nil
}

func checkFlashbackSessionHintInternal(ctx context.Context, conn *sql.Conn, p basePlugin, ts int64, id string) (err error) {
	defer func() {
		if isMySQLError(err, 7510) {
			err = &SnapshotTooOldError{Ts: ts}
		}
	}()

	tables := RouteScan(p.conf)()

	accounts, err := GetAccountsWithFlashbackQueryAndSessionHint(ctx, conn, tables, fmt.Sprintf("/*%s*/", id), p.sessionHint)
	if err != nil {
		// Snapshot too old.
		if strings.Contains(err.Error(), "The snapshot to find is out of range, please adjust scn history configuration") {
			return nil
		}
		if strings.Contains(err.Error(), "Snapshot too old") {
			return nil
		}
		// Table has been changed in the flashback time, e.g. table has not been created at that time.
		if strings.Contains(err.Error(), " The definition of the table required by the flashback query has changed") {
			return nil
		}
		if strings.Contains(err.Error(), "Table definition has changed, please retry transaction") {
			return nil
		}
		logutils.FromContext(ctx).With(zap.Error(err)).Error("GetAccountsWithFlashbackQueryAndSessionHint failed.")
		return nil
	}

	err = checkAccounts(accounts, p, ts, true)
	if err != nil {
		return fmt.Errorf("[%s]check with session hint + flashback query failed : %w", id, err)
	}
	return nil
}

func checkAccounts(accounts []Account, p basePlugin, ts int64, flashback bool) error {
	sum := 0
	for _, account := range accounts {
		if !flashback && p.conf.EnableSsot {
			rec, err := p.sourceTruth.Query(account.ID, ts)
			if err != nil {
				return fmt.Errorf("query ssot failed: %w", err)
			}
			if rec.Balance != account.Balance {
				return fmt.Errorf("check ssot failed: ts %v, id %v, %v != %v", ts, account.ID, account.Balance, rec.Balance)
				// logutils.FromContext(ctx).Sugar().Warnf("Check ssot failed: id %v, %v != %v", account.ID, account.Balance, rec.Balance)
			}
		}
		sum += account.Balance
	}
	expected := p.conf.RowCount * p.conf.InitialBalance
	// Expect sum == expected or sum == 0 when flashback to the initial state.
	if sum != expected && !(flashback && sum == 0) {
		return &TotalNotMatchError{
			Ts:       ts,
			Sum:      sum,
			Expect:   expected,
			Accounts: accounts,
		}
	}
	return nil
}

type CheckBalancePlugin struct {
	basePlugin
}

func (*CheckBalancePlugin) Name() string {
	return "check_balance"
}

func (p *CheckBalancePlugin) Round(ctx context.Context, id string) error {
	current := p.tso.Next()

	return check(ctx, p.basePlugin, current, p.conf.EnableCts, id, false, false)
}

func (b PluginBuilder) BuildCheckBalance() Plugin {
	return &CheckBalancePlugin{
		basePlugin: b.basePlugin,
	}
}

type CheckSessionHintPlugin struct {
	basePlugin
}

func (*CheckSessionHintPlugin) Name() string {
	return "session_hint"
}

func (p *CheckSessionHintPlugin) Round(ctx context.Context, id string) error {
	current := p.tso.Next()

	return checkSessionHint(ctx, p.basePlugin, current, id)
}

func (b PluginBuilder) BuildCheckSessionHint() Plugin {
	return &CheckSessionHintPlugin{
		basePlugin: b.basePlugin,
	}
}

type CheckFlashbackPlugin struct {
	basePlugin
	wait bool
}

func (*CheckFlashbackPlugin) Name() string {
	return "flashback_query"
}

func (p *CheckFlashbackPlugin) Round(ctx context.Context, id string) error {
	if p.wait {
		time.Sleep(time.Duration(p.conf.CheckFlashback.MaxSeconds) * time.Second)
		p.wait = false
	}
	current := p.tso.Next()

	return checkFlashback(ctx, p.basePlugin, current, id)
}

func (b PluginBuilder) BuildCheckFlashback() Plugin {
	return &CheckFlashbackPlugin{
		basePlugin: b.basePlugin,
		wait:       true,
	}
}

type CheckFlashbackSessionHintPlugin struct {
	basePlugin
	wait bool
}

func (*CheckFlashbackSessionHintPlugin) Name() string {
	return "flashback_session_hint"
}

func (p *CheckFlashbackSessionHintPlugin) Round(ctx context.Context, id string) error {
	if p.wait {
		time.Sleep(20 * time.Second)
		p.wait = false
	}
	current := p.tso.Next()

	return checkFlashbackSessionHint(ctx, p.basePlugin, current, id)
}

func (b PluginBuilder) BuildCheckFlashbackSessionHint() Plugin {
	return &CheckFlashbackSessionHintPlugin{
		basePlugin: b.basePlugin,
		wait:       true,
	}
}

type ReadSnapshotPlugin struct {
	basePlugin
}

func (*ReadSnapshotPlugin) Name() string {
	return "read_snapshot"
}

func (p *ReadSnapshotPlugin) Round(ctx context.Context, id string) error {
	logger := logutils.FromContext(ctx)
	base := p.globals.SnapshotLowerbound()
	current := p.tso.Next()
	if current < base {
		logger.Fatal("current < base",
			zap.Int64("current", current),
			zap.Int64("base", base),
		)
	}
	if base == 0 {
		base = p.tso.Start()
	}
	ts := rand.Int63n(current-base) + base
	err := check(ctx, p.basePlugin, ts, p.conf.EnableCts, id, false, false)
	var serr *SnapshotTooOldError
	if errors.As(err, &serr) {
		ts := serr.Ts
		p.globals.TryUpdateSnapshotLowerbound(ctx, ts)
		return nil
	}
	return err
}

func (b PluginBuilder) BuildReadSnapshot() Plugin {
	return &ReadSnapshotPlugin{
		basePlugin: b.basePlugin,
	}
}

type ReadTooOldSnapshotPlugin struct {
	basePlugin
}

func (*ReadTooOldSnapshotPlugin) Name() string {
	return "read_too_old_snapshot"
}

func (p *ReadTooOldSnapshotPlugin) Round(ctx context.Context, id string) error {
	base := p.globals.SnapshotLowerbound()
	if base == 0 {
		time.Sleep(time.Second * 3)
		return nil
	}
	start := p.tso.Start()
	ts := rand.Int63n(base-start) + start
	err := check(ctx, p.basePlugin, ts, p.conf.EnableCts, id, false, false)
	var serr *SnapshotTooOldError
	if err == nil || !errors.As(err, &serr) {
		return errors.New("snapshot should be too old")
	}
	return nil
}

func (b PluginBuilder) BuildReadTooOldSnapshot() Plugin {
	return &ReadTooOldSnapshotPlugin{
		basePlugin: b.basePlugin,
	}
}

type ReadLongSnapshotPlugin struct {
	basePlugin
	interval time.Duration
	total    time.Duration
}

func (*ReadLongSnapshotPlugin) Name() string {
	return "read_long"
}

func (p *ReadLongSnapshotPlugin) Round(ctx context.Context, id string) (err error) {
	ts := p.tso.Next()
	logger := logutils.FromContext(ctx).With(zap.Int64("ts", ts))
	logger.Info("Read long start.")
	defer func() {
		if err == nil {
			logger.Info("Read long success.")
		} else if !errors.Is(err, context.Canceled) {
			logger.Error("Read long failed.", zap.Error(err))
		}
	}()
	return checkPeriodically(ctx, p.basePlugin, ts, p.interval, p.total, p.conf.EnableCts, id, false)
}

func (b PluginBuilder) BuildReadLong(interval, total time.Duration) Plugin {
	return &ReadLongSnapshotPlugin{
		basePlugin: b.basePlugin,
		interval:   interval,
		total:      total,
	}
}

type CheckSecIdxPlugin struct {
	basePlugin
}

func (*CheckSecIdxPlugin) Name() string {
	return "check_secondary_index"
}

func (p *CheckSecIdxPlugin) Round(ctx context.Context, id string) error {
	current := p.tso.Next()

	return check(ctx, p.basePlugin, current, p.conf.EnableCts, id, true, false)
}

func (b PluginBuilder) BuildCheckSecIdx() Plugin {
	return &CheckSecIdxPlugin{
		basePlugin: b.basePlugin,
	}
}

type ReadCurrentSnapshotPlugin struct {
	basePlugin
}

func (*ReadCurrentSnapshotPlugin) Name() string {
	return "read_current_snapshot"
}

func (p *ReadCurrentSnapshotPlugin) Round(ctx context.Context, id string) error {

	current := p.tso.Next()

	return check(ctx, p.basePlugin, current, false, id, false, true)
}

func (b PluginBuilder) BuildReadCurrentSnapshot() Plugin {
	return &ReadCurrentSnapshotPlugin{
		basePlugin: b.basePlugin,
	}
}
