package dbx

import (
	"context"
	"time"

	"github.com/ValerySidorin/corex/dbx/cluster"
)

type Option[T any] func(*DB[T])

func WithCtx[T any](ctx context.Context) Option[T] {
	return func(db *DB[T]) {
		db.Ctx = ctx
	}
}

func WithWriteToNodeStrategy[T any](strategy GetNodeStragegy) Option[T] {
	return func(db *DB[T]) {
		db.WriteToNodeStrategy = strategy
	}
}

func WithReadFromNodeStrategy[T any](strategy GetNodeStragegy) Option[T] {
	return func(db *DB[T]) {
		db.ReadFromNodeStrategy = strategy
	}
}

func WithDefaultNodeStrategy[T any](strategy GetNodeStragegy) Option[T] {
	return func(db *DB[T]) {
		db.DefaultNodeStrategy = strategy
	}
}

func WithNodeWaitTimeout[T any](timeout time.Duration) Option[T] {
	return func(db *DB[T]) {
		db.NodeWaitTimeout = timeout
	}
}

func WithClusterOptions[T any](options ...cluster.ClusterOption[T]) Option[T] {
	return func(db *DB[T]) {
		db.clusterOpts = append(db.clusterOpts, options...)
	}
}
