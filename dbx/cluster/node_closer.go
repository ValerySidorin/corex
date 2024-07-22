package cluster

type ConnCloser[T any] func(T) error
