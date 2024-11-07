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
	"sync/atomic"

	"go.uber.org/zap"

	"transfer/pkg/logutils"
)

type Globals struct {
	snapshotLowerbound int64
}

func NewGlobals() *Globals {
	return &Globals{
		snapshotLowerbound: 0,
	}
}

func (p *Globals) SnapshotLowerbound() int64 {
	return atomic.LoadInt64(&p.snapshotLowerbound)
}

func (g *Globals) TryUpdateSnapshotLowerbound(ctx context.Context, updated int64) {
	logger := logutils.FromContext(ctx)
	for {
		old := atomic.LoadInt64(&g.snapshotLowerbound)
		if old >= updated {
			return
		}
		if atomic.CompareAndSwapInt64(&g.snapshotLowerbound, old, updated) {
			logger.Info("Update snapshot_lower_bound", zap.Int64("from", old), zap.Int64("to", updated))
			return
		}
	}
}
