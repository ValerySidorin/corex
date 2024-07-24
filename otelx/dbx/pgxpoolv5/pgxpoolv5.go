package pgxpoolv5

import (
	"context"

	"github.com/ValerySidorin/corex/errx"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func OpenPoolWithTracer() func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
		pConf, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return nil, errx.Wrap("parse pgx conn config", err)
		}

		tracer := otelpgx.NewTracer(otelpgx.WithAttributes(
			semconv.DBSystemPostgreSQL,
			attribute.String("host", pConf.ConnConfig.Host)))

		pConf.ConnConfig.Tracer = tracer

		pool, err := pgxpool.NewWithConfig(ctx, pConf)
		if err != nil {
			return nil, errx.Wrap("open pgx pool", err)
		}

		return pool, nil
	}
}
