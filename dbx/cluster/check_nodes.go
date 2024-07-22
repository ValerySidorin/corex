package cluster

import (
	"context"
	"sort"
	"sync"
	"time"
)

type checkedNode[T any] struct {
	Node    Node[T]
	Latency time.Duration
}

type checkedNodesList[T any] []checkedNode[T]

var _ sort.Interface = checkedNodesList[sort.Interface]{}

func (list checkedNodesList[T]) Len() int {
	return len(list)
}

func (list checkedNodesList[T]) Less(i, j int) bool {
	return list[i].Latency < list[j].Latency
}

func (list checkedNodesList[T]) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list checkedNodesList[T]) Nodes() []Node[T] {
	res := make([]Node[T], 0, len(list))
	for _, node := range list {
		res = append(res, node.Node)
	}

	return res
}

type groupedCheckedNodes[T any] struct {
	Primaries checkedNodesList[T]
	Standbys  checkedNodesList[T]
}

// Alive returns merged primaries and standbys sorted by latency. Primaries and standbys are expected to be
// sorted beforehand.
func (nodes groupedCheckedNodes[T]) Alive() []Node[T] {
	res := make([]Node[T], len(nodes.Primaries)+len(nodes.Standbys))

	var i int
	for len(nodes.Primaries) > 0 && len(nodes.Standbys) > 0 {
		if nodes.Primaries[0].Latency < nodes.Standbys[0].Latency {
			res[i] = nodes.Primaries[0].Node
			nodes.Primaries = nodes.Primaries[1:]
		} else {
			res[i] = nodes.Standbys[0].Node
			nodes.Standbys = nodes.Standbys[1:]
		}

		i++
	}

	for j := 0; j < len(nodes.Primaries); j++ {
		res[i] = nodes.Primaries[j].Node
		i++
	}

	for j := 0; j < len(nodes.Standbys); j++ {
		res[i] = nodes.Standbys[j].Node
		i++
	}

	return res
}

type checkExecutorFunc[T any] func(ctx context.Context, node Node[T]) (bool, time.Duration, error)

// checkNodes takes slice of nodes, checks them in parallel and returns the alive ones.
// Accepts customizable executor which enables time-independent tests for node sorting based on 'latency'.
func checkNodes[T any](ctx context.Context, nodes []Node[T], executor checkExecutorFunc[T], tracer Tracer[T], errCollector *errorsCollector) AliveNodes[T] {
	checkedNodes := groupedCheckedNodes[T]{
		Primaries: make(checkedNodesList[T], 0, len(nodes)),
		Standbys:  make(checkedNodesList[T], 0, len(nodes)),
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node Node[T], wg *sync.WaitGroup) {
			defer wg.Done()

			primary, duration, err := executor(ctx, node)
			if err != nil {
				if tracer.NodeDead != nil {
					tracer.NodeDead(node, err)
				}
				if errCollector != nil {
					errCollector.Add(node.Addr(), err, time.Now())
				}
				return
			}
			if errCollector != nil {
				errCollector.Remove(node.Addr())
			}

			if tracer.NodeAlive != nil {
				tracer.NodeAlive(node)
			}

			nl := checkedNode[T]{Node: node, Latency: duration}

			mu.Lock()
			defer mu.Unlock()
			if primary {
				checkedNodes.Primaries = append(checkedNodes.Primaries, nl)
			} else {
				checkedNodes.Standbys = append(checkedNodes.Standbys, nl)
			}
		}(node, &wg)
	}
	wg.Wait()

	sort.Sort(checkedNodes.Primaries)
	sort.Sort(checkedNodes.Standbys)

	return AliveNodes[T]{
		Alive:     checkedNodes.Alive(),
		Primaries: checkedNodes.Primaries.Nodes(),
		Standbys:  checkedNodes.Standbys.Nodes(),
	}
}

// checkExecutor returns checkExecutorFunc which can execute supplied check.
func checkExecutor[T any](checker NodeChecker[T]) checkExecutorFunc[T] {
	return func(ctx context.Context, node Node[T]) (bool, time.Duration, error) {
		ts := time.Now()
		primary, err := checker(ctx, node.DB())
		d := time.Since(ts)
		if err != nil {
			return false, d, err
		}

		return primary, d, nil
	}
}
