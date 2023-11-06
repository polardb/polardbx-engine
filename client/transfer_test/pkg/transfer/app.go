package transfer

import (
	"context"

	"transfer/pkg/logutils"

	"github.com/rs/xid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type App struct {
	conf    *Config
	metrics *Metrics
	plugins []Plugin
}

func NewApp(conf *Config, metrics *Metrics) *App {
	return &App{conf: conf, metrics: metrics}
}

func (app *App) Register(plugin Plugin) {
	app.metrics.Register(plugin.Name())
	app.plugins = append(app.plugins, plugin)
}

func (app *App) Run(ctx context.Context) error {
	logger := logutils.FromContext(ctx)
	logger.Info("App start")
	g, ctx := errgroup.WithContext(ctx)
	for _, plugin := range app.plugins {
		plugin := plugin
		g.Go(func() error {
			id := xid.New()
			logger := logger.With(
				zap.String("plugin", plugin.Name()),
				zap.String("trace_id", id.String()),
			)
			ctx := logutils.WithLogger(ctx, logger)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				err := plugin.Round(ctx, id.String())
				if err != nil {
					return err
				}
				app.metrics.Record(plugin.Name())
			}
		})
	}
	return g.Wait()
}
