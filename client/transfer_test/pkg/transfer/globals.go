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
