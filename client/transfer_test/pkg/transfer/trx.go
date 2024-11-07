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
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"transfer/pkg/logutils"

	"go.uber.org/zap"
)

// We support 2 kinds of transactions:
// * XA Trx - Started with 'XA START <XID>'. Optional with timestamp and branch
// * Simple Trx - Started with 'BEGIN'. Optional with timestamp

type Trx interface {
	fmt.Stringer

	Start() error
	StartWithTs(ts int64) error
	CommitOnePhase() error
	Rollback() error

	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type baseTrx struct {
	ctx            context.Context
	conn           *sql.Conn
	useAsyncCommit bool
	useCts         bool
	tso            TSO
	startTs        int64
	commitTs       int64
}

func (trx *baseTrx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return trx.conn.ExecContext(trx.ctx, query, args...)
}

func (trx *baseTrx) QueryRow(query string, args ...interface{}) *sql.Row {
	return trx.conn.QueryRowContext(trx.ctx, query, args...)
}

func (trx *baseTrx) setSnapshotTs(ts int64) error {
	_, err := trx.conn.ExecContext(trx.ctx, fmt.Sprintf("SET innodb_snapshot_seq = %v", ts))
	return err
}

func (trx *baseTrx) setPrepareTs(ts int64) error {
	_, err := trx.conn.ExecContext(trx.ctx, fmt.Sprintf("SET innodb_prepare_seq = %v", ts))
	return err
}

func (trx *baseTrx) setCommitTs(ts int64) error {
	_, err := trx.conn.ExecContext(trx.ctx, fmt.Sprintf("SET innodb_commit_seq = %v", ts))
	return err
}

func (trx *baseTrx) setCurrentSnapshotTs(ts int64) error {
	_, err := trx.conn.ExecContext(trx.ctx, fmt.Sprintf("SET innodb_current_snapshot_seq = on", ts))
	return err
}

type XATrx struct {
	baseTrx
	branch int
}

func NewXATrx(ctx context.Context, tso TSO, conn *sql.Conn, conf *Config) *XATrx {
	return NewXATrxWithBranch(ctx, tso, conn, conf, 0)
}

func NewXATrxWithBranch(ctx context.Context, tso TSO, conn *sql.Conn, conf *Config, branch int) *XATrx {
	return &XATrx{
		baseTrx: baseTrx{
			ctx:            ctx,
			conn:           conn,
			useCts:         conf.EnableCts,
			useAsyncCommit: conf.EnableAsyncCommit,
			tso:            tso,
		},
		branch: branch,
	}
}

func (trx *XATrx) xid() string {
	return fmt.Sprintf("'XID_%d','%d'", trx.startTs, trx.branch)
}

func (trx *XATrx) String() string {
	return fmt.Sprintf("[XATrx: %s]", trx.xid())
}

func (trx *XATrx) StartWithTs(ts int64) error {
	trx.startTs = ts
	var err error
	_, err = trx.conn.ExecContext(trx.ctx, fmt.Sprintf("XA START %v", trx.xid()))
	if err != nil {
		return err
	}
	if trx.useCts {
		err = trx.setSnapshotTs(ts)
		if err != nil {
			return err
		}
	}
	return err
}

func (trx *XATrx) Start() error {
	return trx.StartWithTs(trx.tso.Next())
}

func (trx *XATrx) Rollback() (err error) {
	defer func() {
		if err != nil && (isMySQLError(err, 1614) ||
			strings.Contains(err.Error(), "connection is already closed") ||
			errors.Is(err, driver.ErrBadConn)) {
			err = nil
		}
	}()
	_, err = trx.conn.ExecContext(trx.ctx, fmt.Sprintf("XA END %v", trx.xid()))

	if err != nil {
		logutils.FromContext(trx.ctx).Error("XA END in rollback failed.", zap.Error(err), zap.Stringer("trx", trx))
		return fmt.Errorf("XA END in rollback failed: %w", err)
	}
	_, err = trx.conn.ExecContext(trx.ctx, fmt.Sprintf("XA ROLLBACK %v", trx.xid()))

	if err != nil {
		logutils.FromContext(trx.ctx).Error("XA ROLLBACK failed.", zap.Error(err), zap.Stringer("trx", trx))
		return fmt.Errorf("XA ROLLBACK failed: %w", err)
	}
	return nil
}

func (trx *XATrx) execf(perr *error, sql string, args ...interface{}) {
	if *perr != nil {
		return
	}
	sql = fmt.Sprintf(sql, args...)
	_, *perr = trx.conn.ExecContext(trx.ctx, sql)
	if *perr != nil {
		*perr = fmt.Errorf("%v failed: %w", sql, *perr)
	}
}

func (trx *XATrx) CommitOnePhase() (err error) {
	trx.execf(&err, "XA END %v", trx.xid())
	trx.execf(&err, "XA COMMIT %v ONE PHASE", trx.xid())
	return
}

func (trx *XATrx) PrepareWithTs(ts int64) (minCommitTs int64, err error) {
	if !trx.useAsyncCommit {
		return 0, errors.New("async commit only")
	}
	if _, err = trx.conn.ExecContext(trx.ctx, fmt.Sprintf("XA END %v", trx.xid())); err != nil {
		return 0, fmt.Errorf("XA END failed: %w", err)
	}
	if err = trx.setPrepareTs(trx.tso.Next()); err != nil {
		return 0, fmt.Errorf("set prepare_ts failed: %v %w", ts, err)
	}
	row := trx.conn.QueryRowContext(trx.ctx, fmt.Sprintf("XA PREPARE %v", trx.xid()))
	if err := row.Scan(&minCommitTs); err != nil {
		return 0, fmt.Errorf("get XA PREPARE result failed: %w", err)
	}
	if minCommitTs == 0 {
		return 0, errors.New("invalid min_commit_ts")
	}
	return minCommitTs, err
}

func (trx *XATrx) Prepare() (err error) {
	trx.execf(&err, "XA END %v", trx.xid())
	trx.execf(&err, "XA PREPARE %v", trx.xid())
	return
}

func (trx *XATrx) Commit(ts int64) (err error) {
	trx.commitTs = ts
	if trx.useCts {
		err = trx.setCommitTs(ts)
	}
	trx.execf(&err, "XA COMMIT %v", trx.xid())
	return
}

func (trx *XATrx) prepareAndCommitAsync(hook func(commitTs int64) error) error {
	prepareTs := trx.tso.Next()
	minCommitTs, err := trx.PrepareWithTs(prepareTs)
	if err != nil {
		return err
	}
	commitTs := IncTs(minCommitTs)
	if err := hook(commitTs); err != nil {
		return err
	}
	return trx.Commit(commitTs)
}

func (trx *XATrx) prepareAndCommitSync(hook func(commitTs int64) error) error {
	if err := trx.Prepare(); err != nil {
		return err
	}
	commitTs := trx.tso.Next()
	if err := hook(commitTs); err != nil {
		return err
	}
	return trx.Commit(commitTs)
}

func (trx *XATrx) PrepareAndCommit(hook func(commitTs int64) error) (err error) {
	if trx.useAsyncCommit {
		return trx.prepareAndCommitAsync(hook)
	} else {
		return trx.prepareAndCommitSync(hook)
	}
}

type SimpleTrx struct {
	baseTrx
}

func (trx *SimpleTrx) String() string {
	return "[SimpleTrx]"
}

func NewSimpleTrx(ctx context.Context, tso TSO, conn *sql.Conn, conf *Config) *SimpleTrx {
	return &SimpleTrx{
		baseTrx: baseTrx{
			ctx:    ctx,
			conn:   conn,
			useCts: conf.EnableCts,
			tso:    tso,
		},
	}
}

func (trx *SimpleTrx) StartWithTs(ts int64) error {
	trx.startTs = ts
	var err error
	_, err = trx.conn.ExecContext(trx.ctx, "BEGIN")
	if err != nil {
		return err
	}
	if trx.useCts {
		err = trx.setSnapshotTs(ts)
		if err != nil {
			return err
		}
	}
	return err
}

func (trx *SimpleTrx) Start() error {
	return trx.StartWithTs(trx.tso.Next())
}

func (trx *SimpleTrx) CommitOnePhase() error {
	_, err := trx.conn.ExecContext(trx.ctx, "COMMIT")
	return err
}

func (trx *SimpleTrx) Rollback() error {
	_, err := trx.conn.ExecContext(trx.ctx, "ROLLBACK")
	return err
}
