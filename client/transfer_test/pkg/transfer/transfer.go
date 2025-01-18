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
	"errors"
	"fmt"
	"math/rand"

	"transfer/pkg/logutils"

	"go.uber.org/zap"
)

type TransferPlugin struct {
	basePlugin
}

func (*TransferPlugin) Name() string {
	return "transfer_basic"
}

func (p *TransferPlugin) Round(ctx context.Context, id string) (err error) {
	a1, a2 := rand2(p.conf.RowCount)
	return transfer(ctx, &p.basePlugin, a1, a2, 1, false)
}

type TransferOnePhasePlugin struct {
	basePlugin
}

func (*TransferOnePhasePlugin) Name() string {
	return "transfer_one_phase"
}

func (p *TransferOnePhasePlugin) Round(ctx context.Context, id string) (err error) {
	a1, a2 := rand2(p.conf.RowCount)
	return transfer(ctx, &p.basePlugin, a1, a2, 1, true)
}

type TransferTwoXAPlugin struct {
	basePlugin
}

func (*TransferTwoXAPlugin) Name() string {
	return "transfer_two_xa"
}

func (p *TransferTwoXAPlugin) Round(ctx context.Context, id string) error {
	a1, a2 := rand2(p.conf.RowCount)
	return transferTwoXA(ctx, &p.basePlugin, a1, a2, 1)
}

func transfer(ctx context.Context, p *basePlugin, src, dst, amount int, isOnePhase bool) error {
	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("new connection failed: %w", err)
	}
	defer conn.Close()
	trx := NewXATrx(ctx, p.tso, conn, p.conf)
	err = trx.Start()
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}

	rollbackNeeded := true
	defer func() {
		if rollbackNeeded {
			_ = trx.Rollback() // Rollback if there was an issue
		}
	}()

	if p.conf.EnableUK {
		var srcUser, dstUser string
		srcUser, dstUser, err = getUserPair(trx, trx)
		if srcUser == "" || dstUser == "" {
			if err != nil {
				logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			}
			return nil // No avaiable user.
		}

		if ok, err := transferInternalByUser(ctx,
			trx, trx, srcUser, dstUser, amount,
		); err != nil || !ok {
			return err
		}
	} else {
		src, err = getRandomID(trx)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}
		dst, err = getRandomID(trx)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}

		if ok, err := transferInternal(ctx,
			trx, trx, src, dst, amount, src > dst,
			RoutePoint(p.conf),
		); err != nil || !ok {
			return err
		}
	}

	if isOnePhase {
		if p.conf.EnableSsot {
			// TODO: may cause inconsistent between backend and ssot.
			err = p.sourceTruth.Transfer(src, dst, amount, p.tso.Next())
			if err != nil {
				return fmt.Errorf("failed to update ssot: %w", err)
			}
		}
		err = trx.CommitOnePhase()
	} else {
		err = trx.PrepareAndCommit(func(commitTs int64) error {
			if p.conf.EnableSsot {
				return p.sourceTruth.Transfer(src, dst, amount, commitTs)
			} else {
				return nil
			}
		})
	}

	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	rollbackNeeded = false
	return nil
}

func transferTwoXA(ctx context.Context, p *basePlugin, src, dst, amount int) error {
	conn1, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("new connection failed: %w", err)
	}
	defer conn1.Close()
	conn2, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("new connection failed: %w", err)
	}
	defer conn2.Close()
	trx1 := NewXATrxWithBranch(ctx, p.tso, conn1, p.conf, 1)
	trx2 := NewXATrxWithBranch(ctx, p.tso, conn2, p.conf, 2)
	startTs := p.tso.Next()
	err = trx1.StartWithTs(startTs)
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}
	err = trx2.StartWithTs(startTs)
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}

	rollbackNeeded := true
	defer func() {
		if rollbackNeeded {
			_ = trx1.Rollback()
			_ = trx2.Rollback()
		}
	}()

	if p.conf.EnableUK {
		var srcUser, dstUser string
		srcUser, dstUser, err = getUserPair(trx1, trx2)
		if srcUser == "" || dstUser == "" {
			if err != nil {
				logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			}
			return nil // No avaiable user.
		}

		if ok, err := transferInternalByUser(ctx,
			trx1, trx2, srcUser, dstUser, amount,
		); err != nil || !ok {
			return err
		}
	} else {
		src, err = getRandomID(trx1)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}
		dst, err = getRandomID(trx2)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}

		if ok, err := transferInternal(ctx,
			trx1, trx2,
			src, dst, amount,
			src > dst,
			RoutePoint(p.conf),
		); err != nil || !ok {
			return err
		}
	}

	var commitTs int64
	if p.conf.EnableAsyncCommit {
		prepareTs := p.tso.Next()
		minCommitTs1, err := trx1.PrepareWithTs(prepareTs)
		if err != nil {
			return fmt.Errorf("failed to prepare: %w", err)
		}
		minCommitTs2, err := trx2.PrepareWithTs(prepareTs)
		if err != nil {
			return fmt.Errorf("failed to prepare: %w", err)
		}
		commitTs = IncTs(maxInt64(minCommitTs1, minCommitTs2))
	} else {
		err = trx1.Prepare()
		if err != nil {
			return fmt.Errorf("failed to prepare: %w", err)
		}
		err = trx2.Prepare()
		if err != nil {
			return fmt.Errorf("failed to prepare: %w", err)
		}
		commitTs = p.tso.Next()
	}

	if p.conf.EnableSsot {
		// Update Ssoc during Prepare and Commit.
		err = p.sourceTruth.Transfer(src, dst, 1, commitTs)
		if err != nil {
			return fmt.Errorf("failed to update ssot: %w", err)
		}
	}

	err = trx1.Commit(commitTs)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	err = trx2.Commit(commitTs)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	rollbackNeeded = false
	return nil
}

func transferInternal(ctx context.Context,
	trx1, trx2 Trx,
	src, dst, amount int,
	acquireLock2First bool,
	router pointRouter,
) (ok bool, err error) {
	defer func() {
		if errors.Is(err, context.Canceled) {
			return
		}
		if isMySQLError(err, 1213) {
			// Deadlock error.
			err = nil
		}
		if err != nil || !ok {
			logutils.FromContext(ctx).Info("Will rollback trx.", zap.Error(err), zap.Int("src", src), zap.Int("dst", dst))
			for trx := range map[Trx]struct{}{
				trx1: {},
				trx2: {},
			} {
				if rollbackErr := trx.Rollback(); rollbackErr != nil {
					logutils.FromContext(ctx).Error("Rollback trx failed.", zap.Error(rollbackErr), zap.Stringer("trx", trx))
					err = rollbackErr
				}
			}
			if isMySQLError(err, 1213) || isMySQLError(err, 1205) {
				// Deadlock error or Lockwait timeout error
				err = nil
			}
		}
	}()
	if acquireLock2First {
		if err := updateBalance(trx2, amount, dst, router); err != nil {
			return false, err
		}
	}
	row := trx1.QueryRow(fmt.Sprintf("SELECT balance FROM %s WHERE id = %d FOR UPDATE", router(src), src))
	var balance int
	err = row.Scan(&balance)
	if err != nil {
		return false, fmt.Errorf("read failed: %w", err)
	}
	if balance < amount {
		logutils.FromContext(ctx).Info("insufficient balance", zap.Int("balance", balance), zap.Int("required", amount))
		return false, nil
	}
	if err := updateBalance(trx1, -amount, src, router); err != nil {
		return false, err
	}
	if !acquireLock2First {
		if err := updateBalance(trx2, amount, dst, router); err != nil {
			return false, err
		}
	}
	return true, nil
}

func updateBalance(trx Trx, amount, id int, router pointRouter) error {
	_, err := trx.Exec(fmt.Sprintf("UPDATE %s SET balance = balance + %d, version = version + 1 where id = %d", router(id), amount, id))
	if err != nil {
		return fmt.Errorf("update balance failed: %w", err)
	}
	return nil
}

func (b PluginBuilder) BuildTransferBasic() Plugin {
	return &TransferPlugin{
		basePlugin: b.basePlugin,
	}
}

func (b PluginBuilder) BuildTransferOnePhase() Plugin {
	return &TransferOnePhasePlugin{
		basePlugin: b.basePlugin,
	}
}

func (b PluginBuilder) BuildTransferTwoXA() Plugin {
	return &TransferTwoXAPlugin{
		basePlugin: b.basePlugin,
	}
}

// FIXME
func (b PluginBuilder) BuildTransferLarge(count int) Plugin {
	return &TransferPlugin{
		basePlugin: b.basePlugin,
	}
}

type TransferSimplePlugin struct {
	basePlugin
}

func (*TransferSimplePlugin) Name() string {
	return "transfer_simple"
}

func (p *TransferSimplePlugin) Round(ctx context.Context, id string) error {
	a1, a2 := rand2(p.conf.RowCount)
	return transferSimple(ctx, &p.basePlugin, a1, a2, 1)
}

func transferSimple(ctx context.Context, p *basePlugin, src, dst, amount int) error {
	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("new connection failed: %w", err)
	}
	defer conn.Close()
	trx := NewSimpleTrx(ctx, p.tso, conn, p.conf)
	err = trx.Start()
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}

	if p.conf.EnableUK {
		var srcUser, dstUser string
		srcUser, dstUser, err = getUserPair(trx, trx)
		if srcUser == "" || dstUser == "" {
			if err != nil {
				logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			}
			return nil // No avaiable user.
		}

		if ok, err := transferInternalByUser(ctx,
			trx, trx, srcUser, dstUser, amount,
		); err != nil || !ok {
			return err
		}
	} else {
		src, err = getRandomID(trx)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}
		dst, err = getRandomID(trx)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
			return nil // No avaiable user.
		}

		if ok, err := transferInternal(ctx,
			trx, trx, src, dst, amount, src > dst,
			RoutePoint(p.conf),
		); err != nil || !ok {
			return err
		}
	}

	err = trx.CommitOnePhase()
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

func (b PluginBuilder) BuildTransferSimple() Plugin {
	return &TransferSimplePlugin{
		basePlugin: b.basePlugin,
	}
}

func updateUserBalance(trx Trx, user string, amount int) error {
	_, err := trx.Exec(fmt.Sprintf("UPDATE accounts SET balance = balance + %d, version = version + 1 where user = '%s'", amount, user))
	if err != nil {
		return fmt.Errorf("update balance failed: %w", err)
	}
	return nil
}

func transferInternalByUser(ctx context.Context,
	trx1, trx2 Trx,
	src, dst string,
	amount int,
) (ok bool, err error) {
	defer func() {
		if errors.Is(err, context.Canceled) {
			return
		}
		if isMySQLError(err, 1213) {
			// Deadlock error.
			err = nil
		}
		if err != nil || !ok {
			logutils.FromContext(ctx).Info("Will rollback trx.", zap.Error(err), zap.String("src", src), zap.String("dst", dst))
			for trx := range map[Trx]struct{}{
				trx1: {},
				trx2: {},
			} {
				if rollbackErr := trx.Rollback(); rollbackErr != nil {
					logutils.FromContext(ctx).Error("Rollback trx failed.", zap.Error(rollbackErr), zap.Stringer("trx", trx))
					err = rollbackErr
				}
			}
			if isMySQLError(err, 1213) || isMySQLError(err, 1205) {
				// Deadlock error or Lockwait timeout error
				err = nil
			}
		}
	}()

	row := trx1.QueryRow(fmt.Sprintf("SELECT balance FROM accounts WHERE user = '%s' FOR UPDATE", src))
	var balance int
	err = row.Scan(&balance)
	if err != nil {
		return false, fmt.Errorf("read balance failed: %w", err)
	}
	if balance < amount {
		logutils.FromContext(ctx).Info("insufficient balance", zap.Int("balance", balance), zap.Int("required", amount))
		return false, nil
	}

	if err := updateUserBalance(trx1, src, -amount); err != nil {
		return false, err
	}
	if err := updateUserBalance(trx2, dst, amount); err != nil {
		return false, err
	}

	return true, nil
}

type UserManagePlugin struct {
	basePlugin
}

func (*UserManagePlugin) Name() string {
	return "user_manage"
}

func (p *UserManagePlugin) Round(ctx context.Context, id string) (err error) {
	return manageUser(ctx, &p.basePlugin, p.conf.EnableCts)
}

func (b PluginBuilder) BuildUserManage() Plugin {
	return &UserManagePlugin{
		basePlugin: b.basePlugin,
	}
}

func updateUserName(trx Trx, user string) error {
	var query string
	if rand.Intn(100000) == 0 {
		query = fmt.Sprintf("UPDATE accounts SET user = NULL WHERE user = '%s'", user)
	} else {
		newName := randomString()
		query = fmt.Sprintf("UPDATE accounts SET user = '%s' WHERE user = '%s'", newName, user)
	}

	_, err := trx.Exec(query)
	if err != nil {
		return fmt.Errorf("Update user failed: %w", err)
	}
	return nil
}

func updateUserId(trx Trx, user string) error {
	newId := randomId(1000)
	_, err := trx.Exec(fmt.Sprintf("UPDATE accounts SET id = %d where user = '%s'", newId, user))
	if err != nil {
		return fmt.Errorf("Update user failed: %w", err)
	}
	return nil
}

func manageUser(ctx context.Context, p *basePlugin, useCts bool) error {
	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("new connection failed: %w", err)
	}
	defer conn.Close()

	var trx Trx
	if useCts {
		trx = NewXATrx(ctx, p.tso, conn, p.conf)
	} else {
		trx = NewSimpleTrx(ctx, p.tso, conn, p.conf)
	}

	err = trx.Start()
	if err != nil {
		return fmt.Errorf("create trx failed: %w", err)
	}

	rollbackNeeded := true
	defer func() {
		if rollbackNeeded {
			_ = trx.Rollback() // Rollback if there was an issue
		}
	}()

	user, err := getRandomUser(trx)
	if user == "" {
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Get user failed.")
		}
		return nil // No avaiable user.
	}

	updateName := !(rand.Intn(2) < 1)
	if updateName {
		err = updateUserName(trx, user)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Manage user failed.")
			if isMySQLError(err, 1062) {
				// Duplicate entry
				err = nil
			}
			return err
		}
	} else {
		err = updateUserId(trx, user)
		if err != nil {
			logutils.FromContext(ctx).With(zap.Error(err)).Info("Manage user failed.")
			if isMySQLError(err, 1062) {
				// Duplicate entry
				err = nil
			}
			return err
		}
	}

	err = trx.CommitOnePhase()
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	rollbackNeeded = false
	return nil
}
