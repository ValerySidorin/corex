package sql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ValerySidorin/corex/dbx"
	closers "github.com/ValerySidorin/corex/dbx/closers/sql"
	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/ValerySidorin/corex/errx"
)

const DefaultPingTimeout = 15 * time.Second

type queryWithLockChecker func(query string) bool

type DB struct {
	*dbx.DB[*sql.DB]
	genericOpts []dbx.Option[*sql.DB]

	dbOpener             DBOpener
	queryWithLockChecker queryWithLockChecker
	tx                   *sql.Tx
}

// NewDB returns an instance of *DB.
func NewDB(driverName string, dsns []string,
	checker cluster.NodeChecker[*sql.DB], options ...Option) (*DB, error) {

	resDB := newDB()

	for _, opt := range options {
		opt(resDB)
	}

	resDB.queryWithLockChecker = getQueryWithLockChecker(driverName)

	var err error
	resDB.DB, err = dbx.NewDB(driverName, dsns,
		func(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
			return resDB.dbOpener(ctx, driverName, dsn)
		},
		closers.Close,
		checker,
		resDB.genericOpts...,
	)

	return resDB, errx.Wrap("init generic db", err)
}

func (db *DB) WithCtx(ctx context.Context) *DB {
	resDB := db.copy()
	resDB.Ctx = ctx
	return resDB
}

func (db *DB) WithNodeWaitTimeout(timeout time.Duration) *DB {
	resDB := db.copy()
	resDB.NodeWaitTimeout = timeout
	return resDB
}

func (db *DB) WithWriteToNodeStrategy(strategy dbx.GetNodeStragegy) *DB {
	resDB := db.copy()
	resDB.WriteToNodeStrategy = strategy
	return resDB
}

func (db *DB) WithReadFromNodeStrategy(strategy dbx.GetNodeStragegy) *DB {
	resDB := db.copy()
	resDB.ReadFromNodeStrategy = strategy
	return resDB
}

func (db *DB) WithDefaultNodeStrategy(strategy dbx.GetNodeStragegy) *DB {
	resDB := db.copy()
	resDB.DefaultNodeStrategy = strategy
	return resDB
}

// DoTx executes passed function in transaction.
func (db *DB) DoTx(f func(db dbx.DBxer[*sql.DB, *sql.Tx, *sql.TxOptions]) error, opts *sql.TxOptions) error {
	newDB, err := db.withTx(context.Background(), opts)
	if err != nil {
		return errx.Wrap("with tx", err)
	}
	defer func() {
		_ = newDB.tx.Rollback()
	}()

	err = f(newDB)
	if err != nil {
		return errx.Wrap("exec func in tx", err)
	}

	if err := newDB.tx.Commit(); err != nil {
		return errx.Wrap("commit", err)
	}

	return nil
}

// DoTxContext executes passed function in transaction.
func (db *DB) DoTxContext(
	ctx context.Context,
	f func(ctx context.Context, db dbx.DBxer[*sql.DB, *sql.Tx, *sql.TxOptions]) error,
	opts *sql.TxOptions) error {
	newDB, err := db.withTx(ctx, opts)
	if err != nil {
		return errx.Wrap("with tx", err)
	}
	defer func() {
		_ = newDB.tx.Rollback()
	}()

	err = f(ctx, newDB)
	if err != nil {
		return errx.Wrap("exec func in tx", err)
	}

	if err := newDB.tx.Commit(); err != nil {
		return errx.Wrap("commit", err)
	}

	return nil
}

func (db *DB) Tx() (*sql.Tx, error) {
	if db.tx == nil {
		return nil, errors.New("no sql tx")
	}

	return db.tx, nil
}

// Exec executes query.
func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	if db.tx != nil {
		res, err := db.tx.Exec(query, args...)
		return res, errx.Wrap("exec in tx", err)
	}

	conn, err := db.GetWriteToConn(db.Ctx)
	if err != nil {
		return &nopResult{}, errx.Wrap("wait for write to conn", err)
	}

	res, err := conn.Exec(query, args...)
	return res, errx.Wrap("exec", err)
}

// ExecContext executes query with context.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if db.tx != nil {
		res, err := db.tx.ExecContext(ctx, query, args...)
		return res, errx.Wrap("exec context in tx", err)
	}

	conn, err := db.GetWriteToConn(ctx)
	if err != nil {
		return &nopResult{}, errx.Wrap("wait for write to conn", err)
	}

	res, err := conn.ExecContext(ctx, query, args...)
	return res, errx.Wrap("exec context", err)
}

// Prepare prepares query.
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	if db.tx != nil {
		res, err := db.tx.Prepare(query)
		return res, errx.Wrap("prepare in tx", err)
	}

	conn, err := db.GetDefaultConn(db.Ctx)
	if err != nil {
		return nil, errx.Wrap("wait for default conn", err)
	}

	res, err := conn.Prepare(query)
	return res, errx.Wrap("prepare", err)
}

// PrepareContext prepares query with context.
func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if db.tx != nil {
		res, err := db.tx.PrepareContext(ctx, query)
		return res, errx.Wrap("prepare context in tx", err)
	}

	conn, err := db.GetDefaultConn(ctx)
	if err != nil {
		return nil, errx.Wrap("wait for default conn", err)
	}

	res, err := conn.PrepareContext(ctx, query)
	return res, errx.Wrap("prepare context", err)
}

// Query queries underlying cluster.
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext queries underlying cluster with context.
func (db *DB) QueryContext(ctx context.Context,
	query string, args ...any) (*sql.Rows, error) {
	if db.tx != nil {
		res, err := db.tx.QueryContext(ctx, query, args...)
		return res, errx.Wrap("query context in tx", err)
	}

	var (
		conn *sql.DB
		err  error
	)

	if db.queryWithLockChecker(query) {
		conn, err = db.GetReadFromConn(ctx)
	} else {
		conn, err = db.GetReadFromConn(ctx)
	}
	if err != nil {
		return nil, errx.Wrap("wait for conn", err)
	}

	res, err := conn.QueryContext(ctx, query, args...)
	return res, errx.Wrap("query context", err)
}

// QueryRow queries row from underlying cluster.
func (db *DB) QueryRow(query string, args ...any) dbx.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext queries row from underlying cluster with context.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) dbx.Row {
	if db.tx != nil {
		return db.tx.QueryRowContext(ctx, query)
	}

	var (
		conn *sql.DB
		err  error
	)

	if db.queryWithLockChecker(query) {
		conn, err = db.GetReadFromConn(ctx)
	} else {
		conn, err = db.GetReadFromConn(ctx)
	}

	if err != nil {
		return newErrRow(errx.Wrap("wait for conn", err))
	}

	return conn.QueryRowContext(ctx, query, args...)
}

func newDB() *DB {
	return &DB{
		dbOpener: DefaultDBOpener,
	}
}

// withTx returns a copied version of *DB with new transaction.
func (db *DB) withTx(ctx context.Context, opts *sql.TxOptions) (*DB, error) {
	if db.tx != nil {
		return nil, errors.New("nested transactions are not supported by database/sql package")
	}

	var (
		conn *sql.DB
		err  error
	)

	newDB := db.copy()
	newDB.Ctx = ctx

	if opts == nil || !opts.ReadOnly {
		conn, err = newDB.GetWriteToConn(ctx)
	} else {
		conn, err = newDB.GetReadFromConn(ctx)
	}

	if err != nil {
		return nil, errx.Wrap("wait for conn", err)
	}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, errx.Wrap("begin tx", err)
	}

	newDB.tx = tx
	return newDB, nil
}

func (db *DB) copy() *DB {
	return &DB{
		DB:                   db.DB.Copy(),
		genericOpts:          db.genericOpts,
		dbOpener:             db.dbOpener,
		queryWithLockChecker: db.queryWithLockChecker,
		tx:                   db.tx,
	}
}

func _() dbx.DBxer[*sql.DB, *sql.Tx, *sql.TxOptions] {
	return &DB{}
}

func getQueryWithLockChecker(driverName string) queryWithLockChecker {
	switch driverName {
	case "postgres", "pgx":
		return plsqlQueryWithLockCheck
	case "mysql":
		return mysqlQueryWithLockCheck
	case "sqlserver":
		return tsqlQueryWithLockCheck
	default:
		return func(query string) bool {
			return false
		}
	}
}
