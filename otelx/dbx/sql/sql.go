package sql

import (
	"context"
	stdsql "database/sql"

	"github.com/ValerySidorin/corex/dbx/impl/sql"
	"github.com/ValerySidorin/corex/errx"
	"github.com/XSAM/otelsql"
)

func DBOpener(opts ...otelsql.Option) sql.DBOpener {
	return func(ctx context.Context, driverName, dsn string) (*stdsql.DB, error) {
		db, err := DBOpenerWithTracer(opts...)(ctx, driverName, dsn)
		if err != nil {
			return nil, errx.Wrap("open db with tracer", err)
		}

		if err := DBOpenerWithMetricsCallback(opts...)(db); err != nil {
			return nil, errx.Wrap("exec callback with metrics", err)
		}

		return db, nil
	}
}

func DBOpenerWithTracer(opts ...otelsql.Option) sql.DBOpener {
	return func(ctx context.Context, driverName, dsn string) (*stdsql.DB, error) {
		db, err := otelsql.Open(driverName, dsn, opts...)
		if err != nil {
			return nil, errx.Wrap("open otelsql db", err)
		}

		return db, nil
	}
}

func DBOpenerWithMetricsCallback(opts ...otelsql.Option) func(*stdsql.DB) error {
	return func(db *stdsql.DB) error {
		err := otelsql.RegisterDBStatsMetrics(db, opts...)
		if err != nil {
			return errx.Wrap("register otelsql db stats metrics", err)
		}
		return nil
	}
}
