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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"transfer/pkg/logutils"

	"go.uber.org/zap"
)

type ReplicaReadPlugin struct {
	basePlugin
	replica Connector
}

func (*ReplicaReadPlugin) Name() string {
	return "replica_read"
}

func (p *ReplicaReadPlugin) Round(ctx context.Context, id string) error {
	master := p.connector.Raw()
	slave := p.replica.Raw()
	err := checkReplica(ctx, p.tso, master, slave, p.conf, id)
	if p.conf.IgnoreReadError && err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("Check replica consistent failed.")
	} else {
		return err
	}

	return nil
}

func (b PluginBuilder) BuildReplicaReadPlugin(replica Connector) Plugin {
	return &ReplicaReadPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
	}
}

func checkReplica(ctx context.Context, tso TSO, master *sql.DB, slave *sql.DB, conf *Config, id string) (err error) {
	masterConn, err := master.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create trx failed.")
		return nil
	}
	defer masterConn.Close()

	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create trx failed.")
		return nil
	}
	defer slaveConn.Close()

	var ts int64

	if conf.EnableCts {
		ts = tso.Next()

		defer func() {
			if isMySQLError(err, 7510) {
				err = &SnapshotTooOldError{Ts: ts}
			}
		}()

		masterTrx, err := masterConn.BeginTx(ctx, &sql.TxOptions{
			ReadOnly: true,
		})
		if err != nil {
			return fmt.Errorf("begin transaction failed: %w", err)
		}
		defer masterTrx.Rollback()

		_, err = masterConn.ExecContext(ctx, "SET innodb_snapshot_seq = ?", ts)
		if err != nil {
			return fmt.Errorf("set master snapshot_seq failed: %w", err)
		}

		slaveTrx, err := slaveConn.BeginTx(ctx, &sql.TxOptions{
			ReadOnly: true,
		})
		if err != nil {
			return fmt.Errorf("begin transaction failed: %w", err)
		}
		defer slaveTrx.Rollback()

		_, err = slaveConn.ExecContext(ctx, "SET innodb_snapshot_seq = ?", ts)
		if err != nil {
			return fmt.Errorf("set slave snapshot_seq failed: %w", err)
		}

		getLsn := func(conn *sql.Conn) (int64, error) {
			rows, err := conn.QueryContext(ctx, "SHOW GLOBAL STATUS LIKE 'Applied_index'")
			if err != nil {
				return 0, err
			}
			defer rows.Close()
			if !rows.Next() {
				return 0, errors.New("empty result")
			}
			var ignored string
			var lsn int64
			if err := rows.Scan(&ignored, &lsn); err != nil {
				return 0, err
			}
			if err := rows.Err(); err != nil {
				return 0, err
			}
			return lsn, nil
		}

		masterLsn, err := getLsn(masterConn)
		if err != nil {
			return fmt.Errorf("master get lsn failed: %w", err)
		}

		if _, err := slaveConn.ExecContext(ctx, "SET read_lsn = ?", masterLsn); err != nil {
			return fmt.Errorf("slave wait lsn failed: %w", err)
		}
	}

	tables := RouteScan(conf)()

	masterAccounts, err := GetAccounts(ctx, masterConn, tables, fmt.Sprintf("/*%s*/", id), "", false)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("read leader failed.")
		return nil
	}

	slaveAccounts, err := GetAccounts(ctx, slaveConn, tables,
		fmt.Sprintf("/*%s*/", id)+conf.ReplicaRead.ReplicaReadHint, conf.ReplicaRead.SessionVar, false)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("read follower failed.")
		return nil
	}

	if len(slaveAccounts) != len(masterAccounts) {
		return fmt.Errorf("inconsistency: result size not match (master: %d, slave: %d, ts: %d)",
			len(masterAccounts), len(slaveAccounts), ts)
	}

	masterTotal := 0
	slaveTotal := 0
	n := len(slaveAccounts)
	for i := 0; i < n; i++ {
		if slaveAccounts[i].ID != masterAccounts[i].ID {
			return fmt.Errorf("inconsistency: account id not matched (master id = %d, slave id = %d, ts: %d)",
				masterAccounts[i].ID, slaveAccounts[i].ID, ts)
		}
		// When CTS is enabled, record in master/slave must be exactly matched
		// otherwise the record in slave can be newer, but not allowed to be older then master
		if conf.EnableCts || slaveAccounts[i].Version == masterAccounts[i].Version {
			// version equals, check balances
			if slaveAccounts[i].Balance != masterAccounts[i].Balance {
				return fmt.Errorf("inconsistency: balance not match (id = %d, master balance = %d, slave balance = %d, ts: %d)",
					i, masterAccounts[i].Balance, slaveAccounts[i].Balance, ts)
			}
		}
		if conf.ReplicaStrongConsistency && slaveAccounts[i].Version < masterAccounts[i].Version {
			// slave version is earlier, not allowed
			return fmt.Errorf("inconsistency: slave version is earlier than master (id = %d, master version = %d, slave version = %d, ts: %d)",
				i, masterAccounts[i].Version, slaveAccounts[i].Version, ts)
		}

		masterTotal += masterAccounts[i].Balance
		slaveTotal += slaveAccounts[i].Balance
	}

	if expectTotal := conf.RowCount * conf.InitialBalance; expectTotal != slaveTotal || expectTotal != masterTotal {
		// Total balances not match.
		return fmt.Errorf("[%s]inconsistency: total balances not match (expect total balance = %d, master total balance = %d, slave total balance = %d)",
			id, expectTotal, masterTotal, slaveTotal)
	}

	return nil
}

type ReplicaSessionHintPlugin struct {
	basePlugin
	replica Connector
}

func (*ReplicaSessionHintPlugin) Name() string {
	return "replica_session_hint"
}

func (p *ReplicaSessionHintPlugin) Round(ctx context.Context, id string) error {
	master := p.connector.Raw()
	slave := p.replica.Raw()

	err := checkReplicaWithSessionHint(ctx, master, slave, p.conf, p.sessionHint, id)
	if p.conf.IgnoreReadError && err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]Check replica consistent failed.")
	} else {
		return err
	}

	return nil
}

func (b PluginBuilder) BuildReplicaSessionHintPlugin(replica Connector) Plugin {
	return &ReplicaSessionHintPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
	}
}

func checkReplicaWithSessionHint(ctx context.Context, master *sql.DB, slave *sql.DB, conf *Config, sessionHint *SessionHint, id string) (err error) {
	masterConn, err := master.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]create trx failed.")
		return nil
	}
	defer masterConn.Close()

	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]create trx failed.")
		return nil
	}
	defer slaveConn.Close()

	masterTrx, err := masterConn.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]create trx failed.")
		return nil
	}
	defer func() {
		err := masterTrx.Rollback()
		if err != nil {
			return
		}
	}()

	slaveTrx, err := slaveConn.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]create trx failed.")
		return nil
	}
	defer func() {
		err := slaveTrx.Rollback()
		if err != nil {
			return
		}
	}()

	tables := RouteScan(conf)()

	masterAccounts, err := GetAccountsWithSessionHint(ctx, masterConn, tables, fmt.Sprintf("/*%s*/", id), "", sessionHint)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]read leader failed.")
		return nil
	}

	slaveAccounts, err := GetAccountsWithSessionHint(ctx, slaveConn, tables,
		fmt.Sprintf("/*%s*/", id)+conf.ReplicaSessionHint.ReplicaReadHint, conf.ReplicaSessionHint.SessionVar, sessionHint)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint]read follower failed.")
		return nil
	}

	if len(slaveAccounts) != len(masterAccounts) {
		return fmt.Errorf("[session hint]inconsistency: result size not match (master: %d, slave: %d)",
			len(masterAccounts), len(slaveAccounts))
	}

	masterTotal := 0
	slaveTotal := 0
	n := len(slaveAccounts)
	for i := 0; i < n; i++ {
		if slaveAccounts[i].ID != masterAccounts[i].ID {
			return fmt.Errorf("[session hint]inconsistency: account id not matched (master id = %d, slave id = %d)",
				masterAccounts[i].ID, slaveAccounts[i].ID)
		}
		// When CTS is enabled, record in master/slave must be exactly matched
		// otherwise the record in slave can be newer, but not allowed to be older then master
		if conf.EnableCts || slaveAccounts[i].Version == masterAccounts[i].Version {
			// version equals, check balances
			if slaveAccounts[i].Balance != masterAccounts[i].Balance {
				return fmt.Errorf("[session hint]inconsistency: balance not match (id = %d, master balance = %d, slave balance = %d)",
					i, masterAccounts[i].Balance, slaveAccounts[i].Balance)
			}
		}
		if conf.ReplicaStrongConsistency && slaveAccounts[i].Version < masterAccounts[i].Version {
			// slave version is earlier, not allowed
			return fmt.Errorf("[session hint]inconsistency: slave version is earlier than master (id = %d, master version = %d, slave version = %d)",
				i, masterAccounts[i].Version, slaveAccounts[i].Version)
		}
		masterTotal += masterAccounts[i].Balance
		slaveTotal += slaveAccounts[i].Balance
	}

	if expectTotal := conf.RowCount * conf.InitialBalance; expectTotal != slaveTotal || expectTotal != masterTotal {
		// Total balances not match.
		return fmt.Errorf("[%s][session hint]inconsistency: total balances not match (expect total balance = %d, master total balance = %d, slave total balance = %d)",
			id, expectTotal, masterTotal, slaveTotal)
	}

	return nil
}

type ReplicaFlashbackPlugin struct {
	basePlugin
	replica Connector
	wait    bool
}

func (*ReplicaFlashbackPlugin) Name() string {
	return "replica_flash_back"
}

func (p *ReplicaFlashbackPlugin) Round(ctx context.Context, id string) error {
	if p.wait {
		time.Sleep(time.Duration(p.conf.CheckFlashback.MaxSeconds) * time.Second)
		p.wait = false
	}
	slave := p.replica.Raw()

	err := checkReplicaWithFlashbackQuery(ctx, slave, p.conf, id)
	if p.conf.IgnoreReadError && err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[flashback query]Check replica consistent failed.")
	} else {
		return err
	}

	return nil
}

func (b PluginBuilder) BuildReplicaFlashbackPlugin(replica Connector) Plugin {
	return &ReplicaFlashbackPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
		wait:       true,
	}
}

func checkReplicaWithFlashbackQuery(ctx context.Context, slave *sql.DB, conf *Config, id string) (err error) {
	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[flashback query]create trx failed.")
		return nil
	}
	defer slaveConn.Close()

	tables := RouteScan(conf)()

	slaveAccounts, err := GetAccountsWithFlashbackQuery(ctx, slaveConn, tables,
		fmt.Sprintf("/*%s*/", id)+conf.ReplicaFlashback.ReplicaReadHint,
		conf.ReplicaFlashback.SessionVar, conf.ReplicaFlashback.MinSeconds, conf.ReplicaFlashback.MaxSeconds)
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
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[flashback query]GetAccountsWithFlashbackQuery failed.")
		return nil
	}

	if len(slaveAccounts) != conf.RowCount {
		return fmt.Errorf("[flashback query]inconsistency: result size not match (slave: %d)",
			len(slaveAccounts))
	}

	slaveTotal := 0
	n := len(slaveAccounts)
	for i := 0; i < n; i++ {
		slaveTotal += slaveAccounts[i].Balance
	}

	if expectTotal := conf.RowCount * conf.InitialBalance; expectTotal != slaveTotal {
		// Total balances not match.
		return fmt.Errorf("[%s][flashback query]inconsistency: total balances not match "+
			"(expect total balance = %d, slave total balance = %d)",
			id, expectTotal, slaveTotal)
	}

	return nil
}

type ReplicaFlashbackSessionHintPlugin struct {
	basePlugin
	replica Connector
	wait    bool
}

func (*ReplicaFlashbackSessionHintPlugin) Name() string {
	return "replica_flashback_session_hint"
}

func (p *ReplicaFlashbackSessionHintPlugin) Round(ctx context.Context, id string) error {
	if p.wait {
		time.Sleep(20 * time.Second)
		p.wait = false
	}
	slave := p.replica.Raw()
	err := checkReplicaWithFlashbackQueryAndSessionHint(ctx, slave, p.conf, p.sessionHint, id)
	if p.conf.IgnoreReadError && err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("[session hint + flashback query]Check replica consistent failed.")
	} else {
		return err
	}

	return nil
}

func (b PluginBuilder) BuildReplicaFlashbackSessionHintPlugin(replica Connector) Plugin {
	return &ReplicaFlashbackSessionHintPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
		wait:       true,
	}
}

func checkReplicaWithFlashbackQueryAndSessionHint(ctx context.Context, slave *sql.DB, conf *Config,
	sessionHint *SessionHint, id string) (err error) {
	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		return fmt.Errorf("[flashback query]create trx failed: %w", err)
	}
	defer slaveConn.Close()

	tables := RouteScan(conf)()

	slaveAccounts, err := GetAccountsWithFlashbackQueryAndSessionHint(ctx, slaveConn, tables, fmt.Sprintf("/*%s*/", id)+conf.ReplicaRead.ReplicaReadHint, sessionHint)
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

	if len(slaveAccounts) != conf.RowCount {
		return fmt.Errorf("[flashback query]inconsistency: result size not match (slave: %d)",
			len(slaveAccounts))
	}

	slaveTotal := 0
	n := len(slaveAccounts)
	for i := 0; i < n; i++ {
		slaveTotal += slaveAccounts[i].Balance
	}

	if expectTotal := conf.RowCount * conf.InitialBalance; expectTotal != slaveTotal {
		// Total balances not match.
		return fmt.Errorf("[flashback query]inconsistency: total balances not match (expect total balance = %d, slave total balance = %d)",
			expectTotal, slaveTotal)
	}

	return nil
}

type CheckVersionPlugin struct {
	basePlugin
	replica Connector
}

func (*CheckVersionPlugin) Name() string {
	return "check_version"
}

func (p *CheckVersionPlugin) Round(ctx context.Context, traceId string) error {
	if p.conf.ForXDB {
		return nil
	}

	id := rand.Intn(p.conf.RowCount)

	table := RoutePoint(p.conf)(id)

	master := p.connector.Raw()
	slave := p.replica.Raw()
	masterConn, err := master.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create trx failed.")
		return nil
	}
	defer masterConn.Close()
	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create trx failed.")
		return nil
	}
	defer slaveConn.Close()

	var hint string
	masterAccount, err := GetAccount(ctx, masterConn, table, id, hint)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("GetAccount master failed.")
		return nil
	}
	hint = "/*+TDDL:SLAVE()*/"
	slaveAccount, err := GetAccount(ctx, slaveConn, table, id, hint)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("GetAccount slave failed.")
		return nil
	}
	if masterAccount.ID != slaveAccount.ID {
		return fmt.Errorf("id not match: %d vs %d", masterAccount.ID, slaveAccount.ID)
	}
	if slaveAccount.Version < masterAccount.Version {
		return fmt.Errorf("inconsistency: slave version is earlier than master (id = %d, master version = %d, slave version = %d)",
			id, masterAccount.Version, slaveAccount.Version)
	}
	return nil
}

func (b PluginBuilder) BuildCheckVersionPlugin(replica Connector) Plugin {
	return &CheckVersionPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
	}
}

type CheckCdcPlugin struct {
	basePlugin
	replica Connector
}

func (*CheckCdcPlugin) Name() string {
	return "check_cdc"
}

func (p *CheckCdcPlugin) Round(ctx context.Context, id string) error {
	slave := p.replica.Raw()

	slaveConn, err := slave.Conn(ctx)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("create trx failed.")
		return nil
	}
	defer slaveConn.Close()

	tables := RouteScan(p.conf)()

	slaveAccounts, err := GetAccounts(ctx, slaveConn, tables, "", "", false)
	if err != nil {
		logutils.FromContext(ctx).With(zap.Error(err)).Error("read downstream DB failed.")
		return nil
	}

	if len(slaveAccounts) != p.conf.RowCount {
		return fmt.Errorf("inconsistency: result size not match (expected: %d, actual: %d)",
			p.conf.RowCount, len(slaveAccounts))
	}

	slaveTotal := 0
	n := len(slaveAccounts)
	for i := 0; i < n; i++ {
		slaveTotal += slaveAccounts[i].Balance
	}

	if expectTotal := p.conf.RowCount * p.conf.InitialBalance; expectTotal != slaveTotal {
		// Total balances not match.
		return fmt.Errorf("[check cdc]inconsistency: total balances not match (expect total balance = %d, slave total balance = %d)",
			expectTotal, slaveTotal)
	}

	time.Sleep(30 * time.Millisecond)

	return nil
}

func (b PluginBuilder) BuildCheckCdcPlugin(replica Connector) Plugin {
	return &CheckCdcPlugin{
		basePlugin: b.basePlugin,
		replica:    replica,
	}
}
