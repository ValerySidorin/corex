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
	GetConn(ctx context.Context, strategy GetNodeStragegy) (TConn, error)
	GetWriteToConn(ctx context.Context) (TConn, error)
	GetReadFromConn(ctx context.Context) (TConn, error)
	GetDefaultConn(ctx context.Context) (TConn, error)
	Close()
}

type ConnOpener[T any] func(ctx context.Context, driverName string, dsn string) (T, error)

type DB[T any] struct {
	Cluster     *cluster.Cluster[T]
	clusterOpts []cluster.ClusterOption[T]

	NodeWaitTimeout time.Duration

	WriteToNodeStrategy  GetNodeStragegy // This is used, when we can clearly guess, that query is a write query (for example, Exec())
	ReadFromNodeStrategy GetNodeStragegy // This is used, when we can clearly guess, that query is a read query (for example, Query())
	DefaultNodeStrategy  GetNodeStragegy // This is used, when we can not figure out, what type of request is formed (like Prepare())

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
		node, err := nodeFromConn(resDB.Ctx, driverName, dsn, connOpener)
		if err != nil {
			for _, n := range nodes {
				connCloser(n.DB())
			}
			return nil, errx.Wrap("create node from conn", err)
		}

		nodes = append(nodes, node)
	}

	cl, err := cluster.NewCluster(
		nodes, nodeChecker, connCloser, resDB.clusterOpts...)
	if err != nil {
		return nil, errx.Wrap("init cluster", err)
	}
	resDB.Cluster = cl

	return resDB, nil
}

func (db *DB[T]) Copy() *DB[T] {
	return &DB[T]{
		Cluster:              db.Cluster,
		clusterOpts:          db.clusterOpts,
		NodeWaitTimeout:      db.NodeWaitTimeout,
		WriteToNodeStrategy:  db.WriteToNodeStrategy,
		ReadFromNodeStrategy: db.ReadFromNodeStrategy,
		DefaultNodeStrategy:  db.DefaultNodeStrategy,
		Ctx:                  db.Ctx,
	}
}

func (db *DB[T]) GetConn(ctx context.Context, strategy GetNodeStragegy) (T, error) {
	var t T

	if !strategy.Wait {
		node := db.Cluster.Node(strategy.Criteria)
		if node == nil {
			return t, fmt.Errorf("node (%s) not found", strategy.Criteria)
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, db.NodeWaitTimeout)
	defer cancel()

	node, err := db.Cluster.WaitForNode(waitCtx, strategy.Criteria)
	if err != nil {
		return t, fmt.Errorf("wait for node (%s): %w", strategy.Criteria, err)
	}

	return node.DB(), nil
}

func (db *DB[T]) GetWriteToConn(ctx context.Context) (T, error) {
	return db.GetConn(ctx, db.WriteToNodeStrategy)
}

func (db *DB[T]) GetReadFromConn(ctx context.Context) (T, error) {
	return db.GetConn(ctx, db.ReadFromNodeStrategy)
}

func (db *DB[T]) GetDefaultConn(ctx context.Context) (T, error) {
	return db.GetConn(ctx, db.DefaultNodeStrategy)
}

// Close closes all nodes in cluster.
func (db *DB[T]) Close() {
	db.Cluster.Close()
}

func newDB[T any]() *DB[T] {
	return &DB[T]{
		Ctx:             context.Background(),
		NodeWaitTimeout: 5 * time.Second,
		WriteToNodeStrategy: GetNodeStragegy{
			Criteria: cluster.Primary,
			Wait:     true,
		},
		ReadFromNodeStrategy: GetNodeStragegy{
			Criteria: cluster.PreferStandby,
			Wait:     true,
		},
		DefaultNodeStrategy: GetNodeStragegy{
			Criteria: cluster.Primary,
			Wait:     true,
		},
	}
}

func nodeFromConn[T any](ctx context.Context, driverName, dsn string, connOpener ConnOpener[T]) (cluster.Node[T], error) {
	conn, err := connOpener(ctx, driverName, dsn)
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

	return cluster.NewNode(nodeAddr, conn), nil
}
