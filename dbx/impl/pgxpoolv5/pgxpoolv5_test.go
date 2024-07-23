package pgxpoolv5

import (
	"context"
	"testing"
	"time"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDB([]string{"host=localhost"},
		WithGenericOptions(
			dbx.WithCtx[*pgxpool.Pool](ctx),
		),
		WithNodeChecker(nopNodeChecker),
	)
	assert.Nil(t, err)

	cancel()

	newCtx := context.Background()
	newDB := db.WithCtx(newCtx)

	assert.NotNil(t, db.Ctx.Err())
	assert.Nil(t, newDB.Ctx.Err())
}

func TestWithNodeWaitTimeout(t *testing.T) {
	db, err := NewDB([]string{"host=localhost"},
		WithGenericOptions(dbx.WithNodeWaitTimeout[*pgxpool.Pool](10*time.Second)),
		WithNodeChecker(nopNodeChecker),
	)

	assert.Nil(t, err)

	newDB := db.WithNodeWaitTimeout(60 * time.Second)

	assert.Equal(t, 10*time.Second, db.NodeWaitTimeout)
	assert.Equal(t, 60*time.Second, newDB.NodeWaitTimeout)
}

func nopNodeChecker(ctx context.Context, db *pgxpool.Pool) (bool, error) {
	return true, nil
}
