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
