package pgxpoolv5

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ValerySidorin/corex/dbx"
	checkers "github.com/ValerySidorin/corex/dbx/checkers/pgxpoolv5"
	closers "github.com/ValerySidorin/corex/dbx/closers/pgxpoolv5"
	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/ValerySidorin/corex/errx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultPingTimeout = 15 * time.Second

type PoolOpener func(ctx context.Context, dsn string) (*pgxpool.Pool, error)

type DB struct {
	*dbx.DB[*pgxpool.Pool]
	genericOpts []dbx.Option[*pgxpool.Pool]

	poolOpener PoolOpener

	initPingTimeout time.Duration
	tx              pgx.Tx
}

func NewDB(dsns []string, options ...Option) (*DB, error) {
	resDB := newDB()

	for _, opt := range options {
		opt(resDB)
	}

	var err error
	resDB.DB, err = dbx.NewDB("pgx", dsns,
		func(ctx context.Context, driverName, dsn string) (*pgxpool.Pool, error) {
			return resDB.poolOpener(ctx, dsn)
		},
		closers.Close,
		checkers.Check,
		resDB.genericOpts...,
	)

	return resDB, errx.Wrap("init generic db", err)
}

func (db *DB) WithWriteToNode(criteria cluster.NodeStateCriteria) *DB {
	resDB := db.copyWithCtx(db.Ctx)
	resDB.WriteToNode = criteria
	return resDB
}

func (db *DB) WithReadFromNode(criteria cluster.NodeStateCriteria) *DB {
	resDB := db.copyWithCtx(db.Ctx)
	resDB.ReadFromNode = criteria
	return resDB
}

func (db *DB) WithDefaultNode(criteria cluster.NodeStateCriteria) *DB {
	resDB := db.copyWithCtx(db.Ctx)
	resDB.DefaultNode = criteria
	return resDB
}

func (db *DB) DoTx(f func(db dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions]) error, opts pgx.TxOptions) error {
	newDB, err := db.withTx(context.Background(), opts)
	if err != nil {
		return errx.Wrap("with tx", err)
	}
	defer func() {
		_ = newDB.tx.Rollback(context.Background())
	}()

	err = f(newDB)
	if err != nil {
		return errx.Wrap("exec func in tx", err)
	}

	if err := newDB.tx.Commit(context.Background()); err != nil {
		return errx.Wrap("commit", err)
	}

	return nil
}

func (db *DB) DoTxContext(
	ctx context.Context,
	f func(ctx context.Context, db dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions]) error,
	opts pgx.TxOptions) error {
	newDB, err := db.withTx(ctx, opts)
	if err != nil {
		return errx.Wrap("with tx", err)
	}
	defer func() {
		_ = newDB.tx.Rollback(ctx)
	}()

	err = f(ctx, newDB)
	if err != nil {
		return errx.Wrap("exec func in tx", err)
	}

	if err := newDB.tx.Commit(ctx); err != nil {
		return errx.Wrap("commit", err)
	}

	return nil
}

func (db *DB) Tx() (pgx.Tx, error) {
	if db.tx == nil {
		return nil, errors.New("no pgx tx")
	}

	return db.tx, nil
}

func (db *DB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if db.tx != nil {
		res, err := db.tx.Exec(ctx, sql, arguments...)
		return res, errx.Wrap("exec in tx", err)
	}

	pool, err := db.WaitForWriteToConn(ctx)
	if err != nil {
		return pgconn.CommandTag{}, errx.Wrap("wait for write to conn", err)
	}

	res, err := pool.Exec(ctx, sql, arguments...)
	return res, errx.Wrap("exec", err)
}

func (db *DB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if db.tx != nil {
		res, err := db.tx.Query(ctx, sql, args...)
		return res, errx.Wrap("query in tx", err)
	}

	var (
		pool *pgxpool.Pool
		err  error
	)

	if isSelectWithLock(sql) {
		pool, err = db.WaitForWriteToConn(ctx)
	} else {
		pool, err = db.WaitForReadFromConn(ctx)
	}
	if err != nil {
		return nil, errx.Wrap("wait for conn", err)
	}

	res, err := pool.Query(ctx, sql, args...)
	return res, errx.Wrap("query", err)
}

func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if db.tx != nil {
		return db.tx.QueryRow(ctx, sql, args...)
	}

	var (
		pool *pgxpool.Pool
		err  error
	)

	if isSelectWithLock(sql) {
		pool, err = db.WaitForWriteToConn(ctx)
	} else {
		pool, err = db.WaitForReadFromConn(ctx)
	}
	if err != nil {
		return &errRow{
			err: errx.Wrap("wait for read from conn", err),
		}
	}

	return pool.QueryRow(ctx, sql, args...)
}

func (db *DB) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if db.tx != nil {
		return db.tx.SendBatch(ctx, b)
	}

	pool, err := db.WaitForDefaultConn(ctx)
	if err != nil {
		return &errBatchResults{
			err: errx.Wrap("wait for default conn", err),
		}
	}

	return pool.SendBatch(ctx, b)
}

func (db *DB) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if db.tx != nil {
		res, err := db.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
		return res, errx.Wrap("copy from in tx", err)
	}

	pool, err := db.WaitForWriteToConn(ctx)
	if err != nil {
		return 0, errx.Wrap("wait for write to conn", err)
	}

	res, err := pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
	return res, errx.Wrap("copy from", err)
}

func (db *DB) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	if db.tx != nil {
		res, err := db.tx.Prepare(ctx, name, sql)
		return res, errx.Wrap("prepare in tx", err)
	}

	pool, err := db.WaitForDefaultConn(ctx)
	if err != nil {
		return nil, errx.Wrap("wait for write to conn", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, errx.Wrap("acquire conn", err)
	}

	res, err := conn.Conn().Prepare(ctx, name, sql)
	return res, errx.Wrap("prepare", err)
}

func DefaultPoolOpener(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pConf, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errx.Wrap("parse pgx conn config", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pConf)
	if err != nil {
		return nil, errx.Wrap("open pgx pool", err)
	}

	return pool, nil
}

func newDB() *DB {
	return &DB{
		poolOpener:      DefaultPoolOpener,
		initPingTimeout: DefaultPingTimeout,
	}
}

// withTx returns a copied version of *DB with new transaction.
func (db *DB) withTx(ctx context.Context, opts pgx.TxOptions) (*DB, error) {
	var (
		conn *pgxpool.Pool
		err  error
	)

	newDB := db.copyWithCtx(ctx)

	if newDB.tx != nil {
		txConn := newDB.tx.Conn()

		tx, err := txConn.BeginTx(ctx, opts)
		if err != nil {
			return nil, errx.Wrap("begin nested tx", err)
		}

		newDB.tx = tx
		return newDB, nil
	}

	if opts.AccessMode == pgx.ReadWrite {
		conn, err = newDB.WaitForWriteToConn(ctx)
	} else {
		conn, err = newDB.WaitForReadFromConn(ctx)
	}

	if err != nil {
		return nil, errx.Wrap("wait for conn", err)
	}

	tx, err := conn.BeginTx(newDB.Ctx, opts)
	if err != nil {
		return nil, errx.Wrap("begin tx", err)
	}

	newDB.tx = tx
	return newDB, nil
}

func (db *DB) copyWithCtx(ctx context.Context) *DB {
	resDB := &DB{
		DB:              db.DB,
		genericOpts:     db.genericOpts,
		poolOpener:      db.poolOpener,
		initPingTimeout: db.initPingTimeout,
		tx:              db.tx,
	}
	resDB.Ctx = ctx

	return resDB
}

func _() dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions] {
	return &DB{}
}

func isSelectWithLock(sql string) bool {
	sql = strings.ToLower(sql)
	return strings.Contains(sql, "for update") ||
		strings.Contains(sql, "for no key update") ||
		strings.Contains(sql, "for share") ||
		strings.Contains(sql, "for key share")
}