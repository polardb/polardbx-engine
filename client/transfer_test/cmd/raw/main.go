package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"transfer/pkg/logutils"
	"transfer/pkg/transfer"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	flags := flag.NewFlagSet("general", flag.ExitOnError)
	var (
		dsn        = flags.String("dsn", "", "PolarDB-X Master DSN")
		replicaDsn = flags.String("replica-dsn", "", "PolarDB-X Replica DSN")
		threads    = flags.Int("threads", 10, "Number of threads")
	)

	ctx := context.Background()

	logger, _ := zap.NewDevelopment()
	ctx = logutils.WithLogger(ctx, logger)

	err := flags.Parse(os.Args[1:])
	if err != nil {
		logger.Fatal("Connect to master database failed", zap.Error(err))
	}

	if len(*dsn) == 0 || len(*replicaDsn) == 0 {
		logger.Fatal("missing -dsn or -replica-dsn")
	}

	masterDB, err := sql.Open("mysql", *dsn)
	if err != nil {
		logger.Fatal("Connect to master database failed", zap.Error(err))
	}
	defer masterDB.Close()

	slaveDB, err := sql.Open("mysql", *replicaDsn)
	if err != nil {
		logger.Fatal("Connect to replica database failed", zap.Error(err))
	}
	defer slaveDB.Close()

	err = PrepareData(ctx, masterDB, *threads)
	if err != nil {
		logger.Fatal("Prepare data failed", zap.Error(err))
	}

	metrics := transfer.NewMetrics()
	metrics.Register("ReadAfterWriteCheck")

	g, ctx := errgroup.WithContext(ctx)

	// Run checker threads
	for i := 0; i < *threads; i++ {
		id := i
		g.Go(func() error {
			round := 0
			for {
				err := Check(ctx, id, round, masterDB, slaveDB)
				if err != nil {
					return err
				}
				metrics.Record("ReadAfterWriteCheck")
				round++
			}
		})
	}

	// Run metrics reporter thread
	g.Go(func() error {
		last := time.Now()
		ticker := time.NewTicker(5 * time.Second)
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
			count := data["ReadAfterWriteCheck"]
			logger.Sugar().Infof("TPS: %.2f", float64(count)/dur.Seconds())
		}
	})

	err = g.Wait()
	if err != nil {
		log.Fatal("Test failed. ", zap.Error(err))
	}
}

func Check(ctx context.Context, id, value int, master *sql.DB, replica *sql.DB) error {
	// Write to Master
	result, err := master.ExecContext(ctx, "UPDATE raw_test SET value = ? WHERE id = ?", value, id)
	if err != nil {
		return errors.Wrap(err, "failed to run query")
	}
	_, err = result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows")
	}

	// Read from Replica
	var valueInReplica int
	err = replica.QueryRowContext(ctx, "SELECT value FROM raw_test WHERE id = ?", id).Scan(&valueInReplica)
	if err != nil {
		return errors.Wrap(err, "failed to read value from replica")
	}

	if valueInReplica != value {
		return fmt.Errorf("inconsistency: value not match (master: %d, replica: %d)", value, valueInReplica)
	}

	return nil
}

func PrepareData(ctx context.Context, db *sql.DB, threads int) (err error) {
	_, err = db.ExecContext(ctx,
		`DROP TABLE IF EXISTS raw_test`,
	)
	if err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}

	ddl := `CREATE TABLE raw_test (
  id INT PRIMARY KEY,
  value INT NOT NULL
) DBPARTITION BY HASH(id)`

	_, err = db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	tuples := make([]string, 0, threads)
	for i := 0; i < threads; i++ {
		tuples = append(tuples,
			fmt.Sprintf("(%v, 0)", i),
		)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO raw_test VALUES "+strings.Join(tuples, ", "))
	if err != nil {
		return fmt.Errorf("prepare data failed: %w", err)
	}
	return err
}
