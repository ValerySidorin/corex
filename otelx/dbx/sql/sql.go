package sql

import (
	"database/sql"

	"github.com/ValerySidorin/corex/errx"
	"github.com/XSAM/otelsql"
)

func DBOpener(opts ...otelsql.Option) func(string, string) (*sql.DB, error) {
	return func(driverName, dsn string) (*sql.DB, error) {
		db, err := otelsql.Open(driverName, dsn, opts...)
		if err != nil {
			return nil, errx.Wrap("open otelsql db", err)
		}

		err = otelsql.RegisterDBStatsMetrics(db, opts...)
		if err != nil {
			return nil, errx.Wrap("register otelsql db stats metrics", err)
		}

		return db, nil
	}
}
