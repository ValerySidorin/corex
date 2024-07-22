package cluster

import (
	"math/rand"
	"sync/atomic"
)

// PickNodeRandom returns random node from nodes set
func PickNodeRandom[T any]() NodePicker[T] {
	return func(nodes []Node[T]) Node[T] {
		return nodes[rand.Intn(len(nodes))]
	}
}

// PickNodeRoundRobin returns next node based on Round Robin algorithm
func PickNodeRoundRobin[T any]() NodePicker[T] {
	var nodeIdx uint32
	return func(nodes []Node[T]) Node[T] {
		n := atomic.AddUint32(&nodeIdx, 1)
		return nodes[(int(n)-1)%len(nodes)]
	}
}

// PickNodeClosest returns node with least latency
func PickNodeClosest[T any]() NodePicker[T] {
	return func(nodes []Node[T]) Node[T] {
		return nodes[0]
	}
}
