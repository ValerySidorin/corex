package pgxpoolv5

import (
	"context"

	"github.com/IBM/pgxpoolprometheus"
	"github.com/ValerySidorin/corex/dbx/impl/pgxpoolv5"
	"github.com/ValerySidorin/corex/errx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type PoolOpenerConfig struct {
	PromLabelsBuilder func(pool *pgxpool.Pool) map[string]string
}

func newPoolOpenerConfig(options ...func(cfg *PoolOpenerConfig)) *PoolOpenerConfig {
	cfg := &PoolOpenerConfig{
		PromLabelsBuilder: defaultPromLabelsBuilder,
	}

	for _, opt := range options {
		opt(cfg)
	}

	return cfg
}

func WithPromLabelsBuilder(builder func(pool *pgxpool.Pool) map[string]string) func(cfg *PoolOpenerConfig) {
	return func(cfg *PoolOpenerConfig) {
		cfg.PromLabelsBuilder = builder
	}
}

func OpenPool(ctx context.Context, dsn string,
	poolOpener pgxpoolv5.PoolOpener, options ...func(cfg *PoolOpenerConfig)) (*pgxpool.Pool, error) {
	pool, err := poolOpener(ctx, dsn)
	if err != nil {
		return nil, errx.Wrap("open pool", err)
	}

	cfg := newPoolOpenerConfig(options...)

	collector := pgxpoolprometheus.NewCollector(pool, cfg.PromLabelsBuilder(pool))
	prometheus.MustRegister(collector)

	return pool, nil
}

func defaultPromLabelsBuilder(pool *pgxpool.Pool) map[string]string {
	return map[string]string{
		"database": pool.Config().ConnConfig.Database,
		"host":     pool.Config().ConnConfig.Host,
	}
}
