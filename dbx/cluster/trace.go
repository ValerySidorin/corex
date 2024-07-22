package cluster

// Tracer is a set of hooks to run at various stages of background nodes status update.
// Any particular hook may be nil. Functions may be called concurrently from different goroutines.
type Tracer[T any] struct {
	// UpdateNodes is called when before updating nodes status.
	UpdateNodes func()
	// UpdatedNodes is called after all nodes are updated. The nodes is a list of currently alive nodes.
	UpdatedNodes func(nodes AliveNodes[T])
	// NodeDead is called when it is determined that specified node is dead.
	NodeDead func(node Node[T], err error)
	// NodeAlive is called when it is determined that specified node is alive.
	NodeAlive func(node Node[T])
	// NotifiedWaiters is called when all callers of 'WaitFor*' functions have been notified.
	NotifiedWaiters func()
}
