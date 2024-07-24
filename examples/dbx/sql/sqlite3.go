package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	stdsql "database/sql"

	"github.com/ValerySidorin/corex/dbx"
	checkers "github.com/ValerySidorin/corex/dbx/checkers/sql"
	"github.com/ValerySidorin/corex/dbx/cluster"
	"github.com/ValerySidorin/corex/dbx/impl/sql"
	"github.com/ValerySidorin/corex/otelx"
	otelxsql "github.com/ValerySidorin/corex/otelx/dbx/sql"
	"github.com/XSAM/otelsql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
)

// This example is kindly stolen from mattn/go-sqlite3 repo
func main() {
	os.Remove("./foo.db")

	ctx := context.Background()

	otelResource := otelx.InitResource(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("example_service"),
		))

	err := otelx.Init(
		otelx.WithResource(otelResource),
		otelx.WithTracingStdout(),
		otelx.WithMetricsPrometheus(),
	)
	if err != nil {
		log.Fatal("init otel: ", err)
	}

	db, err := sql.NewDB("sqlite3", []string{"./foo.db"}, checkers.NopCheck,
		sql.WithGenericOptions(
			dbx.WithCtx[*stdsql.DB](ctx),
			dbx.WithClusterOptions(
				cluster.WithUpdateInterval[*stdsql.DB](2*time.Second),
			),
		),
		sql.WithDBOpener(
			otelxsql.DBOpener(
				otelxsql.WithOtelSqlOptBuilder(
					func(dsn string) []otelsql.Option {
						return []otelsql.Option{
							otelsql.WithAttributes(
								otelxsql.GetHostAttribute(dsn),
							),
						}
					},
				),
			),
		),
	)
	if err != nil {
		log.Fatal("new db: ", err)
	}
	defer db.Close()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":9999", nil))
	}()

	sqlStmt := `
		create table foo (id integer not null primary key, name text);
		delete from foo;
		`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}

	if err := db.DoTxContext(ctx, func(ctx context.Context, db dbx.DBxer[*stdsql.DB, *stdsql.Tx, *stdsql.TxOptions]) error {
		sqlDB := db.(*sql.DB) // In case you want to use implementation inside
		stmt, err := sqlDB.PrepareContext(ctx, "insert into foo(id, name) values(?, ?)")
		if err != nil {
			log.Fatal("prepare in tx", err)
		}
		defer stmt.Close()
		for i := range 100 {
			_, err = stmt.ExecContext(ctx, i, fmt.Sprintf("こんにちは世界%03d", i))
			if err != nil {
				log.Fatal("exec in tx", err)
			}
		}

		return nil
	}, nil); err != nil {
		log.Fatal("do tx: ", err)
	}

	type res struct {
		id   int
		name string
	}

	records, err := sql.Query(db, "select id, name from foo",
		func(r *res) []interface{} {
			return []interface{}{&r.id, &r.name}
		})
	if err != nil {
		log.Fatal("generic query: ", err)
	}

	for _, r := range records {
		fmt.Printf("id: %d, name: %s\n", r.id, r.name)
	}

	stmt, err := db.
		WithDefaultNodeStrategy(dbx.WaitForStandbyPreferred()).
		WithNodeWaitTimeout(5 * time.Second).
		Prepare("select name from foo where id = ?")
	if err != nil {
		log.Fatal("prepare", err)
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if err != nil {
		log.Fatal("query row", err)
	}
	fmt.Println(name)

	_, err = db.Exec("delete from foo")
	if err != nil {
		log.Fatal("delete", err)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		log.Fatal("insert", err)
	}

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		log.Fatal("query: ", err)
	}
	defer rows.Close()
	for rows.Next() {
		r := &res{}

		err = rows.Scan(&r.id, &r.name)
		if err != nil {
			log.Fatal("scan", err)
		}
		fmt.Println("id:", r.id, "name:", r.name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal("rows err: ", err)
	}

	metricsResp, err := http.Get("http://localhost:9999/metrics")
	if err != nil {
		log.Fatal("get metrics: ", err)
	}
	defer metricsResp.Body.Close()

	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		log.Fatal("read body: ", err)
	}
	fmt.Println(string(body))
}
