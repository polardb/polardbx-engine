package transfer

import (
	"context"
	"fmt"
	"time"

	"transfer/pkg/logutils"

	"go.uber.org/zap"
)

type EvictPlugin struct {
	basePlugin
	interval time.Duration
}

func (*EvictPlugin) Name() string {
	return "run_evict"
}

func (p *EvictPlugin) Round(ctx context.Context, id string) error {
	logger := logutils.FromContext(ctx)

	time.Sleep(p.interval)

	logger.Info("Run evict commit cache")

	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "SET GLOBAL innodb_evict_run_now = ON")
	if err != nil {
		return fmt.Errorf("failed to run evict: %w", err)
	}
	return nil
}

func (b PluginBuilder) BuildEvictPlugin(interval time.Duration) Plugin {
	return &EvictPlugin{
		basePlugin: b.basePlugin,
		interval:   interval,
	}
}

type PurgePlugin struct {
	basePlugin
	interval time.Duration
}

func (*PurgePlugin) Name() string {
	return "run_purge"
}

func (p *PurgePlugin) Round(ctx context.Context, id string) error {
	logger := logutils.FromContext(ctx)

	time.Sleep(p.interval)

	logger.Info("Run purge undo logs")

	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "SET GLOBAL innodb_purge_run_now = ON")
	if err != nil {
		return fmt.Errorf("failed to run purge: %w", err)
	}
	return nil
}

func (b PluginBuilder) BuildPurgePlugin(interval time.Duration) Plugin {
	return &PurgePlugin{
		basePlugin: b.basePlugin,
		interval:   interval,
	}
}

type ShowStatusPlugin struct {
	basePlugin
	interval time.Duration
}

func (*ShowStatusPlugin) Name() string {
	return "show_status"
}

func (p *ShowStatusPlugin) Round(ctx context.Context, id string) error {
	time.Sleep(p.interval)

	logger := logutils.FromContext(ctx)

	conn, err := p.connector.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}
	defer conn.Close()

	for _, name := range []string{
		"Innodb_evicted_map_size",
		"Innodb_prepared_map_size",
		"Innodb_purge_trx_id_age",
	} {
		func(name string) {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW GLOBAL STATUS LIKE "%v"`, name))
			logger := logger.With(zap.String("status", name))
			if err != nil {
				logger.Warn("Query status failed.", zap.Error(err))
				return
			}
			defer rows.Close()
			if !rows.Next() {
				logger.Warn("Status not found.", zap.Error(err))
				return
			}
			var ignored string
			var val uint64
			if err := rows.Scan(&ignored, &val); err != nil {
				logger.Warn("Scan failed.", zap.Error(err))
				return
			}
			if err := rows.Err(); err != nil {
				logger.Warn("Query failed.", zap.Error(err))
			}
			logger.Info("Status.", zap.Uint64("val", val))
		}(name)
	}

	return nil
}

func (b PluginBuilder) BuildShowStatusPlugin(interval time.Duration) Plugin {
	return &ShowStatusPlugin{
		basePlugin: b.basePlugin,
		interval:   interval,
	}
}

type HeartbeatPlugin struct {
	basePlugin
	interval time.Duration
}

func (*HeartbeatPlugin) Name() string {
	return "heartbeat"
}

func (p *HeartbeatPlugin) Round(ctx context.Context, id string) error {
	time.Sleep(p.interval)
	ts := p.tso.Next()
	_, err := p.connector.Raw().ExecContext(ctx, "SET GLOBAL innodb_heartbeat_seq = ?", ts)
	return err
}

func (b PluginBuilder) BuildHeartbeatPlugin(interval time.Duration) Plugin {
	return &HeartbeatPlugin{basePlugin: b.basePlugin, interval: interval}
}

type SsotGcPlugin struct {
	basePlugin

	interval time.Duration

	lastTs int64
}

func (*SsotGcPlugin) Name() string {
	return "ssot_gc"
}

func (p *SsotGcPlugin) Round(ctx context.Context, id string) error {
	if p.lastTs != 0 {
		if err := p.sourceTruth.GC(p.lastTs); err != nil {
			return err
		}
	}

	p.lastTs = p.tso.Next()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.interval):
	}

	return nil
}

func (b PluginBuilder) BuildSsotGcPlugin(interval time.Duration) *SsotGcPlugin {
	return &SsotGcPlugin{basePlugin: b.basePlugin, interval: interval}
}
