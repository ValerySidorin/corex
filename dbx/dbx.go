package dbx

import (
	"context"
	"fmt"
	"time"

	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/ValerySidorin/corex/errx"
)

type Row interface {
	Err() error
	Scan(dest ...any) error
}

type Rows interface {
	Row
	Next() bool
}

type DBxer[TConn any, TTx any, TTxOptions any] interface {
	DoTx(f func(db DBxer[TConn, TTx, TTxOptions]) error, opts TTxOptions) error
	DoTxContext(
		ctx context.Context,
		f func(ctx context.Context, db DBxer[TConn, TTx, TTxOptions]) error,
		opts TTxOptions) error
	Tx() (TTx, error)
	WaitForConn(ctx context.Context, criteria cluster.NodeStateCriteria) (TConn, error)
	WaitForWriteToConn(ctx context.Context) (TConn, error)
	WaitForReadFromConn(ctx context.Context) (TConn, error)
	WaitForDefaultConn(ctx context.Context) (TConn, error)
}

type ConnOpener[T any] func(ctx context.Context, driverName string, dsn string) (T, error)

type DB[T any] struct {
	Cluster     *cluster.Cluster[T]
	clusterOpts []cluster.ClusterOption[T]

	NodeWaitTimeout time.Duration

	WriteToNode  cluster.NodeStateCriteria // This is used, when we can clearly guess, that query is a write query (for example, Exec())
	ReadFromNode cluster.NodeStateCriteria // This is used, when we can clearly guess, that query is a read query (for example, Query())
	DefaultNode  cluster.NodeStateCriteria // This is used, when we can not figure out, what type of request is formed (like Prepare())

	Ctx context.Context
}

func NewDB[T any](driverName string, dsns []string,
	connOpener ConnOpener[T],
	connCloser cluster.ConnCloser[T],
	nodeChecker cluster.NodeChecker[T],
	options ...Option[T]) (*DB[T], error) {
	resDB := newDB[T]()

	for _, opt := range options {
		opt(resDB)
	}

	nodes := make([]cluster.Node[T], 0, len(dsns))

	for _, dsn := range dsns {
		conn, err := connOpener(resDB.Ctx, driverName, dsn)
		if err != nil {
			return nil, errx.Wrap("open conn", err)
		}

		safeAddr, err := getSafeAddr(dsn)
		if err != nil {
			return nil, errx.Wrap("parse db url", err)
		}

		nodeAddr := safeAddr.host
		if nodeAddr == "" {
			nodeAddr = safeAddr.String()
		}

		nodes = append(nodes, cluster.NewNode(nodeAddr, conn))
	}

	cl, err := cluster.NewCluster(
		nodes, nodeChecker, connCloser, resDB.clusterOpts...)
	if err != nil {
		return nil, errx.Wrap("init cluster", err)
	}
	resDB.Cluster = cl

	return resDB, nil
}

func (db *DB[T]) WaitForConn(ctx context.Context, criteria cluster.NodeStateCriteria) (T, error) {
	return db.waitForDB(ctx, criteria)
}

func (db *DB[T]) WaitForWriteToConn(ctx context.Context) (T, error) {
	return db.waitForDB(ctx, db.WriteToNode)
}

func (db *DB[T]) WaitForReadFromConn(ctx context.Context) (T, error) {
	return db.waitForDB(ctx, db.ReadFromNode)
}

func (db *DB[T]) WaitForDefaultConn(ctx context.Context) (T, error) {
	return db.waitForDB(ctx, db.DefaultNode)
}

// Close closes all nodes in cluster.
func (db *DB[T]) Close() {
	db.Cluster.Close()
}

func newDB[T any]() *DB[T] {
	return &DB[T]{
		Ctx:             context.Background(),
		NodeWaitTimeout: 5 * time.Second,
		WriteToNode:     cluster.Primary,
		ReadFromNode:    cluster.PreferPrimary,
		DefaultNode:     cluster.Primary,
	}
}

func (db *DB[T]) waitForDB(ctx context.Context, criteria cluster.NodeStateCriteria) (T, error) {
	waitCtx, cancel := context.WithTimeout(ctx, db.NodeWaitTimeout)
	defer cancel()

	var t T
	node, err := db.Cluster.WaitForNode(waitCtx, criteria)
	if err != nil {
		return t, fmt.Errorf("wait for node (%s): %w", criteria, err)
	}

	return node.DB(), nil
}

func Scan[T any](rows Rows, pointers func(*T) []interface{}) ([]T, error) {
	var res = []T{}

	for rows.Next() {
		var elem T
		if err := rows.Scan(pointers(&elem)...); err != nil {
			return nil, errx.Wrap("row scan", err)
		}

		res = append(res, elem)
	}

	if rows.Err() != nil {
		return nil, errx.Wrap("rows err", rows.Err())
	}

	return res, nil
}