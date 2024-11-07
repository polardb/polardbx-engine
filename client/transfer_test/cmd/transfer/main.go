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


package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"transfer/pkg/logutils"
	"transfer/pkg/ssot"
	"transfer/pkg/transfer"
	"transfer/pkg/version"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	_ "net/http/pprof"

	_ "github.com/lib/pq"
)

type SubCmd int

const (
	Help = iota
	Run
	Prepare
)

func main() {
	flags := flag.NewFlagSet("general", flag.ExitOnError)
	var (
		dsn        = flags.String("dsn", os.Getenv("MYSQL_TRANSFER_DSN"), "MySQL Data Source Name")
		confPath   = flags.String("config", os.Getenv("MYSQL_TRANSFER_CONFIG"), "Config file")
		replicaDsn = flags.String("replica-dsn", os.Getenv("MYSQL_TRANSFER_REPLICA_DSN"), "MySQL Replica Data Source Name")
	)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(-1)
	}

	var cmd SubCmd
	switch strings.ToLower(os.Args[1]) {
	case "prepare":
		cmd = Prepare
	case "run":
		cmd = Run
	case "version":
		fmt.Println(version.V)
		os.Exit(0)
	case "help":
		cmd = Help
		os.Exit(0)
	default:
		printUsage()
		os.Exit(-1)
	}

	err := flags.Parse(os.Args[2:])
	if err != nil {
		printUsage()
		os.Exit(-1)
	}

	var conf *transfer.Config
	if _, err := toml.DecodeFile(*confPath, &conf); err != nil {
		log.Fatal("read conf file failed.", zap.Error(err))
	}

	conf.Normalize()
	if err := toml.NewEncoder(os.Stdout).Encode(conf); err != nil {
		fmt.Println("Start transfer test with config:")
	}

	ctx := context.Background()

	logger, _ := zap.NewDevelopment()
	ctx = logutils.WithLogger(ctx, logger)

	var d string
	if len(*dsn) > 0 {
		d = *dsn
	} else if len(conf.Dsn) > 0 {
		d = conf.Dsn
	} else {
		log.Fatal("missing dsn (-dsn='...')")
	}
	db, err := sql.Open(conf.DbType, d)
	if err != nil {
		logger.Fatal("Connect failed.", zap.Error(err))
	}
	db.SetMaxIdleConns(1024)
	db.SetMaxOpenConns(1024)
	defer db.Close()
	tso := transfer.NewTSO()

	if conf.ForXDB {
		if err := transfer.SetGlobalIsolationLevel(ctx, db); err != nil {
			logger.Warn("Set transaction level failed.", zap.Error(err))
		}
		logger.Info("Recover all hanging XA transactions")
		err = transfer.RecoverAll(ctx, db)
		if err != nil {
			logger.Warn("XA recover failed.", zap.Error(err))
		}
	}

	if cmd == Prepare {
		err = transfer.PrepareData(ctx, db, conf)
		if err != nil {
			log.Fatal("Prepare failed.", zap.Error(err))
		}
		logger.Info("Table and data prepared")
		return
	}

	var (
		globals     = transfer.NewGlobals()
		metrics     = transfer.NewMetrics()
		sessionHint = transfer.NewSessionHint()

		app = transfer.NewApp(conf, metrics)
	)

	connector := transfer.NewConnector(db)

	conn, err := connector.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	accs, err := transfer.GetAccounts(ctx, conn, transfer.RouteScan(conf)(), "", "", false)
	if err != nil {
		log.Fatal(err)
	}
	var recs []ssot.Record
	for _, acc := range accs {
		recs = append(recs, ssot.Record{
			Balance: acc.Balance,
			Version: acc.Version,
		})
	}

	// Init session hint.
	if conf.CheckSessionHint.Enabled || conf.ReplicaSessionHint.Enabled {
		err := sessionHint.Init(ctx, conf, connector)
		if err != nil {
			log.Fatal(err)
		}
	}

	builder := transfer.PluginBuilder{}
	builder = builder.
		Conf(conf).
		TSO(tso).
		Globals(globals).
		Metrics(metrics).
		Connector(connector).
		Ssot(ssot.NewMemSsot(recs)).
		SessionHint(sessionHint)

	if conf.CheckBalance.Enabled {
		for i := 0; i < conf.CheckBalance.Threads; i++ {
			app.Register(builder.BuildCheckBalance())
		}
	}

	if conf.CheckSessionHint.Enabled {
		for i := 0; i < conf.CheckSessionHint.Threads; i++ {
			app.Register(builder.BuildCheckSessionHint())
		}
	}

	if conf.CheckFlashback.Enabled {
		for i := 0; i < conf.CheckFlashback.Threads; i++ {
			app.Register(builder.BuildCheckFlashback())
		}
	}

	if conf.CheckSessionHint.Enabled && conf.CheckFlashback.Enabled {
		for i := 0; i < conf.CheckSessionHint.Threads; i++ {
			app.Register(builder.BuildCheckFlashbackSessionHint())
		}
	}

	if conf.ReadSnapshot.Enabled {
		for i := 0; i < conf.ReadSnapshot.Threads; i++ {
			app.Register(builder.BuildReadSnapshot())
		}
	}

	if conf.ReadTooOldSnapshot.Enabled {
		for i := 0; i < conf.ReadTooOldSnapshot.Threads; i++ {
			app.Register(builder.BuildReadTooOldSnapshot())
		}
	}

	if conf.ReadLong.Enabled {
		for i := 0; i < conf.ReadLong.Threads; i++ {
			app.Register(builder.BuildReadLong(
				time.Duration(conf.ReadLong.Interval),
				time.Duration(conf.ReadLong.Total),
			))
		}
	}

	if conf.TransferBasic.Enabled {
		for i := 0; i < conf.TransferBasic.Threads; i++ {
			app.Register(builder.BuildTransferBasic())
		}
	}

	if conf.TransferOnePhase.Enabled {
		for i := 0; i < conf.TransferOnePhase.Threads; i++ {
			app.Register(builder.BuildTransferOnePhase())
		}
	}

	if conf.TransferTwoXA.Enabled {
		for i := 0; i < conf.TransferTwoXA.Threads; i++ {
			app.Register(builder.BuildTransferTwoXA())
		}
	}

	if conf.TransferLarge.Enabled {
		for i := 0; i < conf.TransferLarge.Threads; i++ {
			app.Register(builder.BuildTransferLarge(conf.TransferLarge.Count))
		}
	}

	if conf.TransferSimple.Enabled {
		for i := 0; i < conf.TransferSimple.Threads; i++ {
			app.Register(builder.BuildTransferSimple())
		}
	}

	if conf.ReplicaRead.Enabled || conf.CheckVersion.Enabled ||
		conf.ReplicaSessionHint.Enabled || conf.ReplicaFlashback.Enabled {
		var d string
		if len(*replicaDsn) != 0 {
			d = *replicaDsn
		} else if len(conf.ReplicaRead.ReplicaDsn) != 0 {
			d = conf.ReplicaRead.ReplicaDsn
		} else {
			log.Fatal("missing replica dsn (-replica-dsn='...')")
		}
		replicaDB, err := sql.Open(conf.DbType, d)
		if err != nil {
			logger.Fatal("Connect to replica DB failed.", zap.Error(err))
		}
		replicaDB.SetMaxIdleConns(1024)
		replicaDB.SetMaxOpenConns(1024)
		defer replicaDB.Close()
		replicaConnector := transfer.NewConnector(replicaDB)

		if conf.ReplicaRead.Enabled {
			for i := 0; i < conf.ReplicaRead.Threads; i++ {
				app.Register(builder.BuildReplicaReadPlugin(replicaConnector))
			}
		}

		if conf.ReplicaSessionHint.Enabled {
			for i := 0; i < conf.ReplicaSessionHint.Threads; i++ {
				app.Register(builder.BuildReplicaSessionHintPlugin(replicaConnector))
			}
		}

		if conf.ReplicaFlashback.Enabled {
			for i := 0; i < conf.ReplicaFlashback.Threads; i++ {
				app.Register(builder.BuildReplicaFlashbackPlugin(replicaConnector))
			}
		}

		if conf.ReplicaSessionHint.Enabled && conf.ReplicaFlashback.Enabled {
			for i := 0; i < conf.ReplicaSessionHint.Threads; i++ {
				app.Register(builder.BuildReplicaFlashbackSessionHintPlugin(replicaConnector))
			}
		}

		if conf.CheckVersion.Enabled {
			for i := 0; i < conf.CheckVersion.Threads; i++ {
				app.Register(builder.BuildCheckVersionPlugin(replicaConnector))
			}
		}
	}

	if conf.CheckCdc.Enabled {
		downstreamDSN := conf.CheckCdc.DownstreamDsn
		downstreamDB, err := sql.Open(conf.DbType, downstreamDSN)
		if err != nil {
			logger.Fatal("Connect to replica DB failed.", zap.Error(err))
		}
		downstreamDB.SetMaxIdleConns(1024)
		downstreamDB.SetMaxOpenConns(1024)
		defer downstreamDB.Close()
		downstreamConnector := transfer.NewConnector(downstreamDB)
		for i := 0; i < conf.CheckCdc.Threads; i++ {
			app.Register(builder.BuildCheckCdcPlugin(downstreamConnector))
		}
	}

	if conf.RunEvict.Enabled {
		app.Register(builder.BuildEvictPlugin(time.Duration(conf.RunEvict.Interval)))
	}
	if conf.RunPurge.Enabled {
		app.Register(builder.BuildPurgePlugin(time.Duration(conf.RunPurge.Interval)))
	}
	if conf.ShowStatus.Enabled {
		app.Register(builder.BuildShowStatusPlugin(time.Duration(conf.ShowStatus.Interval)))
	}
	if conf.Heartbeat.Enabled {
		app.Register(builder.BuildHeartbeatPlugin(time.Duration(conf.Heartbeat.Interval)))
	}
	if conf.EnableSsot {
		app.Register(builder.BuildSsotGcPlugin(time.Second * 60))
	}

	if conf.ReadCurrentSnapshot.Enabled {
		for i := 0; i < conf.ReadCurrentSnapshot.Threads; i++ {
			app.Register(builder.BuildReadCurrentSnapshot())
		}
	}

	if conf.CheckSecIdx.Enabled {
		for i := 0; i < conf.CheckSecIdx.Threads; i++ {
			app.Register(builder.BuildCheckSecIdx())
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return app.Run(ctx)
	})

	g.Go(func() error {
		last := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			var t time.Time
			select {
			case <-ctx.Done():
				return nil
			case t = <-ticker.C:
			}
			dur := t.Sub(last)
			last = t
			data := metrics.CopyAndReset()
			var labels []string
			for label := range data {
				labels = append(labels, label)
			}
			sort.Strings(labels)
			for _, label := range labels {
				count := data[label]
				logger.Sugar().Infof("TPS %v: %.2f", label, float64(count)/dur.Seconds())
			}
		}
	})

	err = g.Wait()
	if err != nil {
		log.Fatal("Test failed.", zap.Error(err))
	}
}

const usageText = `Usage: 
  transfer run -config=<path_to_config_file> -dsn=<dsn> [-replica-dsn=<replica-dsn>]
  transfer prepare -config=<path_to_config_file> -dsn=<dsn> [-replica-dsn=<replica-dsn>]

DSN format:
  <username>:<password>@tcp(<address>:<port>)/<database>
e.g.
  -dsn='polarx:123456@tcp(11.167.60.147:3306)/test'
  -dsn='polardbx_root:123456@tcp(127.0.0.1:8508)/drds_polarx1_qatest_app'
`

func printUsage() {
	fmt.Fprint(os.Stderr, usageText)
}
