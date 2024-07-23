package sql

import (
	"database/sql"

	"github.com/ValerySidorin/corex/dbx"
)

type Option func(db *DB)

func WithGenericOptions(options ...dbx.Option[*sql.DB]) Option {
	return func(db *DB) {
		db.genericOpts = append(db.genericOpts, options...)
	}
}

func WithDBOpener(dbOpener DBOpener) Option {
	return func(db *DB) {
		db.dbOpener = dbOpener
	}
}
