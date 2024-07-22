package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	checkers "github.com/ValerySidorin/corex/dbx/checkers/sql"
	closers "github.com/ValerySidorin/corex/dbx/closers/sql"
	"github.com/ValerySidorin/corex/dbx/cluster"
	_ "github.com/mattn/go-sqlite3"
)

func checkSqlite3() {
	os.Remove("./foo.db")
	ctx := context.Background()

	dsns := []string{"./foo.db"}

	var nodes []cluster.Node[*sql.DB]

	for _, dsn := range dsns {
		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			log.Fatal("new pgxpool: ", err)
		}

		node := cluster.NewNode(dsn, db)
		nodes = append(nodes, node)
	}

	cl, err := cluster.NewCluster(nodes,
		checkers.NopCheck, closers.Close,
		cluster.WithNodePicker(cluster.PickNodeRandom[*sql.DB]()),
	)
	if err != nil {
		log.Fatal("new cluster: ", err)
	}

	node, err := cl.WaitForPrimary(ctx)
	if err != nil {
		log.Fatal("wait for primary: ", err)
	}

	if err := node.DB().Ping(); err != nil {
		log.Fatal("ping: ", err)
	}
}
