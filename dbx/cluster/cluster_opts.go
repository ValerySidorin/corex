package cluster

import "time"

// ClusterOption is a functional option type for Cluster constructor
type ClusterOption[T any] func(*Cluster[T])

// WithUpdateInterval sets interval between cluster node updates
func WithUpdateInterval[T any](d time.Duration) ClusterOption[T] {
	return func(cl *Cluster[T]) {
		cl.updateInterval = d
	}
}

// WithUpdateTimeout sets ping timeout for update of each node in cluster
func WithUpdateTimeout[T any](d time.Duration) ClusterOption[T] {
	return func(cl *Cluster[T]) {
		cl.updateTimeout = d
	}
}

// WithNodePicker sets algorithm for node selection (e.g. random, round robin etc)
func WithNodePicker[T any](picker NodePicker[T]) ClusterOption[T] {
	return func(cl *Cluster[T]) {
		cl.picker = picker
	}
}

// WithTracer sets tracer for actions happening in the background
func WithTracer[T any](tracer Tracer[T]) ClusterOption[T] {
	return func(cl *Cluster[T]) {
		cl.tracer = tracer
	}
}
