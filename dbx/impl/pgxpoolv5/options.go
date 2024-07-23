package pgxpoolv5

import (
	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Option func(*DB)

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

func WithNodeChecker(nodeChecker cluster.NodeChecker[*pgxpool.Pool]) Option {
	return func(db *DB) {
		db.nodeChecker = nodeChecker
	}
}

func WithPoolCloser(poolCloser cluster.ConnCloser[*pgxpool.Pool]) Option {
	return func(db *DB) {
		db.poolCloser = poolCloser
	}
}
