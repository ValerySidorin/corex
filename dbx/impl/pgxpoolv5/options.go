package pgxpoolv5

import (
	"time"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Option func(*DB)

func WithInitPingTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.initPingTimeout = timeout
	}
}

func WithGenericOptions(options ...dbx.Option[*pgxpool.Pool]) Option {
	return func(db *DB) {
		db.genericOpts = append(db.genericOpts, options...)
	}
}

func WithPoolOpener(poolOpener PoolOpener) Option {
	return func(db *DB) {
		db.poolOpener = poolOpener
	}
}
