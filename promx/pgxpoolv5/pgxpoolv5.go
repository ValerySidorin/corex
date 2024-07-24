package pgxpoolv5

import (
	"github.com/IBM/pgxpoolprometheus"
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

func PoolOpenerCallback(options ...func(cfg *PoolOpenerConfig)) func(*pgxpool.Pool) error {
	return func(pool *pgxpool.Pool) error {
		cfg := newPoolOpenerConfig(options...)

		collector := pgxpoolprometheus.NewCollector(pool, cfg.PromLabelsBuilder(pool))
		prometheus.MustRegister(collector)
		return nil
	}
}

func defaultPromLabelsBuilder(pool *pgxpool.Pool) map[string]string {
	return map[string]string{
		"database": pool.Config().ConnConfig.Database,
		"host":     pool.Config().ConnConfig.Host,
	}
}
