package main

import (
	"context"
	"log"

	checkers "github.com/ValerySidorin/corex/dbx/checkers/pgxpoolv5"
	closers "github.com/ValerySidorin/corex/dbx/closers/pgxpoolv5"
	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/jackc/pgx/v5/pgxpool"
)

func checkPgxpool() {
	ctx := context.Background()

	// Change hosts to your cluster nodes
	// Postgres must run in cluster mode, under Patroni control for example
	dsns := []string{"example.host1.com", "example.host2.com"}

	var nodes []cluster.Node[*pgxpool.Pool]

	for _, dsn := range dsns {
		db, err := pgxpool.New(ctx, dsn)
		if err != nil {
			log.Fatal("new pgxpool: ", err)
		}

		node := cluster.NewNode(dsn, db)
		nodes = append(nodes, node)
	}

	cl, err := cluster.NewCluster(nodes,
		checkers.Check, closers.Close,
		cluster.WithNodePicker(cluster.PickNodeRandom[*pgxpool.Pool]()),
	)
	if err != nil {
		log.Fatal("new cluster: ", err)
	}

	node, err := cl.WaitForPrimary(ctx)
	if err != nil {
		log.Fatal("wait for primary: ", err)
	}

	if err := node.DB().Ping(ctx); err != nil {
		log.Fatal("ping: ", err)
	}
}
