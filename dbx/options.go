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

func WithWriteToNode[T any](criteria cluster.NodeStateCriteria) Option[T] {
	return func(db *DB[T]) {
		db.WriteToNode = criteria
	}
}

func WithReadFromNode[T any](criteria cluster.NodeStateCriteria) Option[T] {
	return func(db *DB[T]) {
		db.ReadFromNode = criteria
	}
}

func WithDefaultNode[T any](criteria cluster.NodeStateCriteria) Option[T] {
	return func(db *DB[T]) {
		db.DefaultNode = criteria
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
