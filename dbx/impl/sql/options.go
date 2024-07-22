package sql

import (
	"database/sql"
	"time"

	"github.com/ValerySidorin/corex/dbx"
)

type Option func(db *DB)

func WithInitPingTimeout(timeout time.Duration) Option {
	return func(db *DB) {
		db.initPingTimeout = timeout
	}
}

func WithGenericOptions(options ...dbx.Option[*sql.DB]) Option {
	return func(db *DB) {
		db.genericOpts = append(db.genericOpts, options...)
	}
}

func WithDBOpener(dbOpener func(driverName, dsn string) (*sql.DB, error)) Option {
	return func(db *DB) {
		db.dbOpener = dbOpener
	}
}
