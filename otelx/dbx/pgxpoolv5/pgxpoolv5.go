package pgxpoolv5

import (
	"context"
	"time"

	"github.com/ValerySidorin/corex/dbx/impl/pgxpoolv5"
	"github.com/ValerySidorin/corex/errx"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type PoolOpenerConfig struct {
	OtelPgxPoolConfigOptBuilder func(poolCfg *pgxpool.Config) []otelpgx.Option
}

type PoolOpenerWithTracerConfig struct {
	OtelPgxPoolOptBuilder func(pool *pgxpool.Pool) []otelpgx.Option
}

func newPoolOpenerConfig() *PoolOpenerConfig {
	return &PoolOpenerConfig{
		OtelPgxPoolConfigOptBuilder: defaultOtelPgxPoolConfigOptBuilder,
	}
}

func newPoolOpenerWithTracerConfig() *PoolOpenerWithTracerConfig {
	return &PoolOpenerWithTracerConfig{
		OtelPgxPoolOptBuilder: defaultOtelPgxPoolOptBuilder,
	}
}

func PoolOpener(opts ...func(*PoolOpenerConfig)) pgxpoolv5.PoolOpener {
	cfg := newPoolOpenerConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
		pConf, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return nil, errx.Wrap("parse pgx conn config", err)
		}

		pConf.ConnConfig.Tracer = otelpgx.NewTracer(cfg.OtelPgxPoolConfigOptBuilder(pConf)...)

		pool, err := pgxpool.NewWithConfig(ctx, pConf)
		if err != nil {
			return nil, errx.Wrap("open pgx pool", err)
		}

		pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		if err := pool.Ping(pingCtx); err != nil {
			return nil, errx.Wrap("ping db", err)
		}

		return pool, nil
	}
}

func PoolOpenerWithTracerCallback(opts ...func(*PoolOpenerWithTracerConfig)) func(*pgxpool.Pool) error {
	cfg := newPoolOpenerWithTracerConfig()

	return func(pool *pgxpool.Pool) error {
		tracer := otelpgx.NewTracer(cfg.OtelPgxPoolOptBuilder(pool)...)
		pool.Config().ConnConfig.Tracer = tracer
		return nil
	}
}

func WithOtelPgxOptBuilder(b func(poolCfg *pgxpool.Config) []otelpgx.Option) func(*PoolOpenerConfig) {
	return func(poc *PoolOpenerConfig) {
		poc.OtelPgxPoolConfigOptBuilder = b
	}
}

func WithOtelPgxOptBuilder2(b func(pool *pgxpool.Pool) []otelpgx.Option) func(*PoolOpenerWithTracerConfig) {
	return func(poc *PoolOpenerWithTracerConfig) {
		poc.OtelPgxPoolOptBuilder = b
	}
}

var defaultOtelPgxPoolConfigOptBuilder func(*pgxpool.Config) []otelpgx.Option = func(poolCfg *pgxpool.Config) []otelpgx.Option {
	return []otelpgx.Option{
		otelpgx.WithAttributes(
			semconv.DBSystemPostgreSQL,
			attribute.String("host", poolCfg.ConnConfig.Host),
			attribute.String("database", poolCfg.ConnConfig.Database),
		),
	}
}

var defaultOtelPgxPoolOptBuilder func(*pgxpool.Pool) []otelpgx.Option = func(pool *pgxpool.Pool) []otelpgx.Option {
	return []otelpgx.Option{
		otelpgx.WithAttributes(
			semconv.DBSystemPostgreSQL,
			attribute.String("host", pool.Config().ConnConfig.Host),
			attribute.String("database", pool.Config().ConnConfig.Database),
		),
	}
}
