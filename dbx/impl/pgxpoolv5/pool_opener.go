package pgxpoolv5

import (
	"context"

	"github.com/ValerySidorin/corex/errx"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolOpener func(ctx context.Context, dsn string) (*pgxpool.Pool, error)

func DefaultPoolOpener() PoolOpener {
	return func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
		pConf, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return nil, errx.Wrap("parse pgx conn config", err)
		}

		pool, err := pgxpool.NewWithConfig(ctx, pConf)
		if err != nil {
			return nil, errx.Wrap("open pgx pool", err)
		}

		pingCtx, cancel := context.WithTimeout(ctx, DefaultPingTimeout)
		defer cancel()

		if err := pool.Ping(pingCtx); err != nil {
			return nil, errx.Wrap("ping db", err)
		}

		return pool, nil
	}
}

func CustomPoolOpener(poolOpener PoolOpener, callbacks ...func(*pgxpool.Pool) error) PoolOpener {
	return func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
		pool, err := poolOpener(ctx, dsn)
		if err != nil {
			return nil, errx.Wrap("open pool", err)
		}

		for _, callback := range callbacks {
			err = callback(pool)
			if err != nil {
				return nil, errx.Wrap("exec callback", err)
			}
		}

		return pool, nil
	}
}
