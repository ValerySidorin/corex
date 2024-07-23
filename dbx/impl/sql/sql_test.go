package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ValerySidorin/corex/dbx"
	"github.com/stretchr/testify/assert"
)

func TestWithCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDB("sqlite3", []string{"first"}, nopNodeChecker,
		WithGenericOptions(dbx.WithCtx[*sql.DB](ctx)),
		WithDBOpener(func(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
			db, _, err := sqlmock.New()
			return db, err
		}))
	assert.Nil(t, err)

	newDB := db.WithCtx(context.Background())
	cancel()

	assert.NotNil(t, db.Ctx.Err())
	assert.Nil(t, newDB.Ctx.Err())
}

func nopNodeChecker(ctx context.Context, db *sql.DB) (bool, error) {
	return true, nil
}
