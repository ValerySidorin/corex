package sql

import "github.com/ValerySidorin/corex/dbx"

type nopResult struct{}

func (r *nopResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *nopResult) RowsAffected() (int64, error) {
	return 0, nil
}

type ErrRow struct {
	err error
}

func newErrRow(err error) dbx.Row {
	return &ErrRow{
		err: err,
	}
}

func (r *ErrRow) Err() error {
	return r.err
}

func (r *ErrRow) Scan(dest ...any) error {
	return r.err
}
