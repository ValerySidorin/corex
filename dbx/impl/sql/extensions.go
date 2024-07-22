package sql

import (
	"context"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/errx"
)

// Query is a generic query helper.
func Query[T any](db *DB, query string,
	pointers func(*T) []interface{}, args ...any) ([]T, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, errx.Wrap("generic query", err)
	}
	defer rows.Close()

	res, err := dbx.Scan(rows, pointers)
	return res, errx.Wrap("scan", err)
}

// QueryContext is a generic query helper with context.
func QueryContext[T any](ctx context.Context, db *DB, query string,
	pointers func(*T) []interface{}, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errx.Wrap("generic query context", err)
	}
	defer rows.Close()

	res, err := dbx.Scan(rows, pointers)
	return res, errx.Wrap("scan", err)
}
