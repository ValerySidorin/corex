package sql

import (
	"context"
	"database/sql"

	"github.com/ValerySidorin/corex/errx"
)

type DBOpener func(ctx context.Context, driverName, dsn string) (*sql.DB, error)

var DefaultDBOpener DBOpener = func(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, errx.Wrap("open db", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), DefaultPingTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		return nil, errx.Wrap("ping db", err)
	}

	return db, nil
}

func CustomDBOpener(dbOpener DBOpener, callbacks ...func(dsn string, db *sql.DB) error) DBOpener {
	return func(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
		db, err := dbOpener(ctx, driverName, dsn)
		if err != nil {
			return nil, errx.Wrap("open db", err)
		}

		for _, callback := range callbacks {
			if err := callback(dsn, db); err != nil {
				return nil, errx.Wrap("exec callback", err)
			}
		}

		return db, nil
	}
}
