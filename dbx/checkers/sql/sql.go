package sql

import (
	"context"
	"database/sql"
)

func NopCheck(ctx context.Context, db *sql.DB) (bool, error) {
	return true, nil
}

func PostgreSQL(ctx context.Context, db *sql.DB) (bool, error) {
	row := db.QueryRowContext(ctx, "SELECT NOT pg_is_in_recovery()")
	var primary bool
	if err := row.Scan(&primary); err != nil {
		return false, err
	}

	return primary, nil
}
