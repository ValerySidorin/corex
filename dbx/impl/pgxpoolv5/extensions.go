package pgxpoolv5

import (
	"context"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/errx"
)

// Query is a generic query helper with context.
func Query[T any](ctx context.Context, db *DB, query string,
	pointers func(*T) []interface{}, args ...any) ([]T, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, errx.Wrap("generic query context", err)
	}
	defer rows.Close()

	res, err := dbx.Scan(rows, pointers)
	return res, errx.Wrap("scan", err)
}
