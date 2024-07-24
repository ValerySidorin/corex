package pgxpoolv5

import (
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

func PoolOpenerWithTracerCallback(opts ...otelpgx.Option) func(*pgxpool.Pool) error {
	return func(pool *pgxpool.Pool) error {
		tracer := otelpgx.NewTracer(opts...)
		pool.Config().ConnConfig.Tracer = tracer
		return nil
	}
}
