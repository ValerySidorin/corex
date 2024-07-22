package pgxpoolv5

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Default checks whether PostgreSQL server is primary or not.
func Check(ctx context.Context, db *pgxpool.Pool) (bool, error) {
	row := db.QueryRow(ctx, "SELECT NOT pg_is_in_recovery()")
	var primary bool
	if err := row.Scan(&primary); err != nil {
		return false, err
	}

	return primary, nil
}
