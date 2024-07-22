package cluster

import (
	"context"
	"fmt"
)

// Node of single cluster
type Node[T any] interface {
	fmt.Stringer

	Addr() string
	DB() T
}

type node[T any] struct {
	addr string
	db   T
}

var _ Node[string] = &node[string]{}

// NewNode constructs node from pgxpool v5
func NewNode[T any](addr string, db T) Node[T] {
	return &node[T]{addr: addr, db: db}
}

func (n *node[T]) Addr() string {
	return n.addr
}

func (n *node[T]) DB() T {
	return n.db
}

func (n *node[T]) String() string {
	return n.addr
}

// NodeStateCriteria for choosing a node
type NodeStateCriteria int

const (
	// Alive for choosing any alive node
	Alive NodeStateCriteria = iota + 1
	// Primary for choosing primary node
	Primary
	// Standby for choosing standby node
	Standby
	// PreferPrimary for choosing primary or any alive node
	PreferPrimary
	// PreferStandby for choosing standby or any alive node
	PreferStandby
)

func (c NodeStateCriteria) String() string {
	switch c {
	case Alive:
		return "alive"
	case Primary:
		return "primary"
	case Standby:
		return "standby"
	case PreferPrimary:
		return "prefer primary"
	case PreferStandby:
		return "prefer standby"
	default:
		return "unknown"
	}
}

type NodeChecker[T any] func(ctx context.Context, db T) (bool, error)

type NodePicker[T any] func(nodes []Node[T]) Node[T]
