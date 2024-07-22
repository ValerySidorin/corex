package pgxpoolv5

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type errRow struct {
	err error
}

func (r *errRow) Scan(dest ...any) error {
	return r.err
}

type errBatchResults struct {
	err error
}

func (r *errBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, r.err
}

func (r *errBatchResults) Query() (pgx.Rows, error) {
	return nil, r.err
}

func (r *errBatchResults) QueryRow() pgx.Row {
	return &errRow{
		err: r.err,
	}
}

func (r *errBatchResults) Close() error {
	return r.err
}
