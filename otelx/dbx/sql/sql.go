package sql

import (
	"context"
	stdsql "database/sql"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/dbx/impl/sql"
	"github.com/ValerySidorin/corex/errx"
	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
)

var (
	defaultDbOpenerOptBuilder = func(dsn string) []otelsql.Option {
		return []otelsql.Option{
			otelsql.WithAttributes(
				GetHostAttribute(dsn),
				GetDBNameAttribute(dsn),
			),
		}
	}

	GetHostAttribute = func(dsn string) attribute.KeyValue {
		host, _ := dbx.GetHost(dsn)
		return attribute.String("host", host)
	}

	GetDBNameAttribute = func(dsn string) attribute.KeyValue {
		dbname, _ := dbx.GetDatabase(dsn)
		return attribute.String("database", dbname)
	}
)

type DBOpenerConfig struct {
	DBOpenerOptBuilder func(string) []otelsql.Option
}

func newDBOpenerConfig() *DBOpenerConfig {
	return &DBOpenerConfig{
		DBOpenerOptBuilder: defaultDbOpenerOptBuilder,
	}
}

func DBOpener(opts ...func(*DBOpenerConfig)) sql.DBOpener {
	return func(ctx context.Context, driverName, dsn string) (*stdsql.DB, error) {
		cfg := newDBOpenerConfig()

		for _, opt := range opts {
			opt(cfg)
		}

		db, err := DBOpenerWithTracer(opts...)(ctx, driverName, dsn)
		if err != nil {
			return nil, errx.Wrap("open db with tracer", err)
		}

		if err := DBOpenerWithMetricsCallback(opts...)(dsn, db); err != nil {
			return nil, errx.Wrap("exec callback with metrics", err)
		}

		return db, nil
	}
}

func DBOpenerWithTracer(opts ...func(*DBOpenerConfig)) sql.DBOpener {
	return func(ctx context.Context, driverName, dsn string) (*stdsql.DB, error) {
		cfg := newDBOpenerConfig()

		for _, opt := range opts {
			opt(cfg)
		}

		db, err := otelsql.Open(driverName, dsn, cfg.DBOpenerOptBuilder(dsn)...)
		if err != nil {
			return nil, errx.Wrap("open otelsql db", err)
		}

		return db, nil
	}
}

func DBOpenerWithMetricsCallback(opts ...func(*DBOpenerConfig)) func(string, *stdsql.DB) error {
	return func(dsn string, db *stdsql.DB) error {
		cfg := newDBOpenerConfig()

		for _, opt := range opts {
			opt(cfg)
		}

		err := otelsql.RegisterDBStatsMetrics(db, cfg.DBOpenerOptBuilder(dsn)...)
		if err != nil {
			return errx.Wrap("register otelsql db stats metrics", err)
		}
		return nil
	}
}

func WithOtelSqlOptBuilder(b func(dsn string) []otelsql.Option) func(*DBOpenerConfig) {
	return func(doc *DBOpenerConfig) {
		doc.DBOpenerOptBuilder = b
	}
}
