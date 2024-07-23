package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Default values for cluster config
const (
	DefaultUpdateInterval = time.Second * 5
	DefaultUpdateTimeout  = time.Second
)

type nodeWaiter[T any] struct {
	Ch            chan Node[T]
	StateCriteria NodeStateCriteria
}

// AliveNodes of Cluster
type AliveNodes[T any] struct {
	Alive     []Node[T]
	Primaries []Node[T]
	Standbys  []Node[T]
}

// Cluster consists of number of 'nodes' of a single SQL database.
// Background goroutine periodically checks nodes and updates their status.
type Cluster[T any] struct {
	tracer Tracer[T]

	// Configuration
	updateInterval time.Duration
	updateTimeout  time.Duration
	checker        NodeChecker[T]
	picker         NodePicker[T]
	closer         ConnCloser[T]

	// Status
	updateStopper chan struct{}
	aliveNodes    atomic.Value
	nodes         []Node[T]
	errCollector  errorsCollector

	// Notification
	muWaiters sync.Mutex
	waiters   []nodeWaiter[T]
}

// NewCluster constructs cluster object representing a single 'cluster' of SQL database.
// Close function must be called when cluster is not needed anymore.
func NewCluster[T any](nodes []Node[T],
	checker NodeChecker[T], closer ConnCloser[T],
	opts ...ClusterOption[T]) (*Cluster[T], error) {
	// Validate nodes
	if len(nodes) == 0 {
		return nil, errors.New("no nodes provided")
	}

	for i, node := range nodes {
		if node.Addr() == "" {
			return nil, fmt.Errorf("node %d has no address", i)
		}

		// TODO: somehow check if node.DB() is nil
	}

	cl := &Cluster[T]{
		updateStopper:  make(chan struct{}),
		updateInterval: DefaultUpdateInterval,
		updateTimeout:  DefaultUpdateTimeout,
		checker:        checker,
		picker:         PickNodeRandom[T](),
		closer:         closer,
		nodes:          nodes,
		errCollector:   newErrorsCollector(),
	}

	// Apply options
	for _, opt := range opts {
		opt(cl)
	}

	// Store initial nodes state
	cl.aliveNodes.Store(AliveNodes[T]{})

	// Start update routine
	go cl.backgroundNodesUpdate()
	return cl, nil
}

// Close databases and stop node updates.
func (cl *Cluster[T]) Close() error {
	close(cl.updateStopper)

	var err error
	for _, node := range cl.nodes {
		if err := cl.closer(node.DB()); err != nil {
			return err
		}
	}

	return err
}

// Nodes returns list of all nodes
func (cl *Cluster[T]) Nodes() []Node[T] {
	return cl.nodes
}

func (cl *Cluster[T]) nodesAlive() AliveNodes[T] {
	return cl.aliveNodes.Load().(AliveNodes[T])
}

func (cl *Cluster[T]) addUpdateWaiter(criteria NodeStateCriteria) <-chan Node[T] {
	// Buffered channel is essential.
	// Read WaitForNode function for more information.
	ch := make(chan Node[T], 1)
	cl.muWaiters.Lock()
	defer cl.muWaiters.Unlock()
	cl.waiters = append(cl.waiters, nodeWaiter[T]{Ch: ch, StateCriteria: criteria})
	return ch
}

// WaitForAlive node to appear or until context is canceled
func (cl *Cluster[T]) WaitForAlive(ctx context.Context) (Node[T], error) {
	return cl.WaitForNode(ctx, Alive)
}

// WaitForPrimary node to appear or until context is canceled
func (cl *Cluster[T]) WaitForPrimary(ctx context.Context) (Node[T], error) {
	return cl.WaitForNode(ctx, Primary)
}

// WaitForStandby node to appear or until context is canceled
func (cl *Cluster[T]) WaitForStandby(ctx context.Context) (Node[T], error) {
	return cl.WaitForNode(ctx, Standby)
}

// WaitForPrimaryPreferred node to appear or until context is canceled
func (cl *Cluster[T]) WaitForPrimaryPreferred(ctx context.Context) (Node[T], error) {
	return cl.WaitForNode(ctx, PreferPrimary)
}

// WaitForStandbyPreferred node to appear or until context is canceled
func (cl *Cluster[T]) WaitForStandbyPreferred(ctx context.Context) (Node[T], error) {
	return cl.WaitForNode(ctx, PreferStandby)
}

// WaitForNode with specified status to appear or until context is canceled
func (cl *Cluster[T]) WaitForNode(ctx context.Context, criteria NodeStateCriteria) (Node[T], error) {
	// Node already exists?
	node := cl.Node(criteria)
	if node != nil {
		return node, nil
	}

	ch := cl.addUpdateWaiter(criteria)

	// Node might have appeared while we were adding waiter, recheck
	node = cl.Node(criteria)
	if node != nil {
		return node, nil
	}

	// If channel is unbuffered and we are right here when nodes are updated,
	// the update code won't be able to write into channel and will 'forget' it.
	// Then we will report nil to the caller, either because update code
	// closes channel or because context is canceled.
	//
	// In both cases its not what user wants.
	//
	// We can solve it by doing cl.Node(ns) if/when we are about to return nil.
	// But if another update runs between channel read and cl.Node(ns) AND no
	// nodes have requested status, we will still return nil.
	//
	// Also code becomes more complex.
	//
	// Wait for node to appear...
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case node := <-ch:
		return node, nil
	}
}

// Alive returns node that is considered alive
func (cl *Cluster[T]) Alive() Node[T] {
	return cl.alive(cl.nodesAlive())
}

func (cl *Cluster[T]) alive(nodes AliveNodes[T]) Node[T] {
	if len(nodes.Alive) == 0 {
		return nil
	}

	return cl.picker(nodes.Alive)
}

// Primary returns first available node that is considered alive and is primary (able to execute write operations)
func (cl *Cluster[T]) Primary() Node[T] {
	return cl.primary(cl.nodesAlive())
}

func (cl *Cluster[T]) primary(nodes AliveNodes[T]) Node[T] {
	if len(nodes.Primaries) == 0 {
		return nil
	}

	return cl.picker(nodes.Primaries)
}

// Standby returns node that is considered alive and is standby (unable to execute write operations)
func (cl *Cluster[T]) Standby() Node[T] {
	return cl.standby(cl.nodesAlive())
}

func (cl *Cluster[T]) standby(nodes AliveNodes[T]) Node[T] {
	if len(nodes.Standbys) == 0 {
		return nil
	}

	// select one of standbys
	return cl.picker(nodes.Standbys)
}

// PrimaryPreferred returns primary node if possible, standby otherwise
func (cl *Cluster[T]) PrimaryPreferred() Node[T] {
	return cl.primaryPreferred(cl.nodesAlive())
}

func (cl *Cluster[T]) primaryPreferred(nodes AliveNodes[T]) Node[T] {
	node := cl.primary(nodes)
	if node == nil {
		node = cl.standby(nodes)
	}

	return node
}

// StandbyPreferred returns standby node if possible, primary otherwise
func (cl *Cluster[T]) StandbyPreferred() Node[T] {
	return cl.standbyPreferred(cl.nodesAlive())
}

func (cl *Cluster[T]) standbyPreferred(nodes AliveNodes[T]) Node[T] {
	node := cl.standby(nodes)
	if node == nil {
		node = cl.primary(nodes)
	}

	return node
}

// Node returns cluster node with specified status.
func (cl *Cluster[T]) Node(criteria NodeStateCriteria) Node[T] {
	return cl.node(cl.nodesAlive(), criteria)
}

func (cl *Cluster[T]) node(nodes AliveNodes[T], criteria NodeStateCriteria) Node[T] {
	switch criteria {
	case Alive:
		return cl.alive(nodes)
	case Primary:
		return cl.primary(nodes)
	case Standby:
		return cl.standby(nodes)
	case PreferPrimary:
		return cl.primaryPreferred(nodes)
	case PreferStandby:
		return cl.standbyPreferred(nodes)
	default:
		panic(fmt.Sprintf("unknown node state criteria: %d", criteria))
	}
}

// Err returns the combined error including most recent errors for all nodes.
// This error is CollectedErrors or nil.
func (cl *Cluster[T]) Err() error {
	return cl.errCollector.Err()
}

// backgroundNodesUpdate periodically updates list of live db nodes
func (cl *Cluster[T]) backgroundNodesUpdate() {
	// Initial update
	cl.updateNodes()

	ticker := time.NewTicker(cl.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cl.updateStopper:
			return
		case <-ticker.C:
			cl.updateNodes()
		}
	}
}

// updateNodes pings all db nodes and stores alive ones in a separate slice
func (cl *Cluster[T]) updateNodes() {
	if cl.tracer.UpdateNodes != nil {
		cl.tracer.UpdateNodes()
	}

	ctx, cancel := context.WithTimeout(context.Background(), cl.updateTimeout)
	defer cancel()

	alive := checkNodes(ctx, cl.nodes, checkExecutor(cl.checker), cl.tracer, &cl.errCollector)
	cl.aliveNodes.Store(alive)

	if cl.tracer.UpdatedNodes != nil {
		cl.tracer.UpdatedNodes(alive)
	}

	cl.notifyWaiters(alive)

	if cl.tracer.NotifiedWaiters != nil {
		cl.tracer.NotifiedWaiters()
	}
}

func (cl *Cluster[T]) notifyWaiters(nodes AliveNodes[T]) {
	cl.muWaiters.Lock()
	defer cl.muWaiters.Unlock()

	if len(cl.waiters) == 0 {
		return
	}

	var nodelessWaiters []nodeWaiter[T]
	// Notify all waiters
	for _, waiter := range cl.waiters {
		node := cl.node(nodes, waiter.StateCriteria)
		if node == nil {
			// Put waiter back
			nodelessWaiters = append(nodelessWaiters, waiter)
			continue
		}

		// We won't block here, read addUpdateWaiter function for more information
		waiter.Ch <- node
		// No need to close channel since we write only once and forget it so does the 'client'
	}

	cl.waiters = nodelessWaiters
}
