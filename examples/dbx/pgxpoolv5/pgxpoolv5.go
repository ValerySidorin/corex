package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/IBM/pgxpoolprometheus"
	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/dbx/impl/pgxpoolv5"
	"github.com/ValerySidorin/corex/errx"
	"github.com/ValerySidorin/corex/otelx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	ctx := context.Background()

	otelResource := otelx.InitResource(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("example_service"),
		))

	err := otelx.Init(
		otelx.WithResource(otelResource),
		otelx.WithTracingStdout(),
	)
	if err != nil {
		log.Fatal("init otel: ", err)
	}

	var db dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions]

	db, err = pgxpoolv5.NewDB(
		[]string{"postgres://postgres:password@localhost:5432/test"},
		pgxpoolv5.WithGenericOptions(
			dbx.WithCtx[*pgxpool.Pool](ctx),
		),
		pgxpoolv5.WithInitPingTimeout(5*time.Second),
		pgxpoolv5.WithPoolOpener(func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
			pool, err := pgxpoolv5.DefaultPoolOpener(ctx, dsn)
			if err != nil {
				return nil, errx.Wrap("open pool", err)
			}

			collector := pgxpoolprometheus.NewCollector(pool,
				map[string]string{
					"db_name": "test",
					"host":    pool.Config().ConnConfig.Host,
				})
			prometheus.MustRegister(collector)

			return pool, nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":9999", nil))
	}()

	pool, err := db.GetWriteToConn(ctx)
	if err != nil {
		log.Fatal(err)
	}

	pool.Exec(ctx, `DROP TABLE IF EXISTS employees;`)

	sqlStmt := `
	CREATE TABLE employees (
		id SERIAL PRIMARY KEY,
		first_name VARCHAR(50) NOT NULL,
		last_name VARCHAR(50) NOT NULL,
		email VARCHAR(100) UNIQUE NOT NULL,
		hire_date DATE NOT NULL,
		salary NUMERIC(10, 2)
	);
		`

	_, err = pool.Exec(ctx, sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}

	type res struct {
		id        int
		firstName string
	}

	if err := db.DoTxContext(ctx, func(ctx context.Context, db dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions]) error {
		tx, _ := db.Tx()
		_, err := tx.Prepare(ctx, "stmt", `
		INSERT INTO employees (first_name, last_name, email, hire_date, salary)
		VALUES ($1, $2, $3, $4, $5);`)
		if err != nil {
			log.Fatal("prepare in tx ", err)
		}

		for i := range 100 {
			_, err = tx.Exec(ctx, "stmt",
				"John"+strconv.Itoa(i),
				"Doe"+strconv.Itoa(i),
				"johndoe"+strconv.Itoa(i)+"@somecompany.com",
				time.Now(),
				50000.00,
			)
			if err != nil {
				log.Fatal("exec in tx", err)
			}
		}

		if err := db.DoTxContext(ctx, func(ctx context.Context, db dbx.DBxer[*pgxpool.Pool, pgx.Tx, pgx.TxOptions]) error {
			pgxpoolDB := db.(*pgxpoolv5.DB)
			var r res
			if err := pgxpoolDB.QueryRow(ctx, `SELECT id, first_name FROM employees`).Scan(&r.id, &r.firstName); err != nil {
				return errx.Wrap("query row in nested tx", err)
			}

			fmt.Println("id:", r.id, "firstName:", r.firstName)
			return nil

		}, pgx.TxOptions{}); err != nil {
			log.Fatal("do nested tx: ", err)
		}

		return nil
	}, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	}); err != nil {
		log.Fatal("do tx: ", err)
	}

	rows, err := pool.Query(ctx, "SELECT id, first_name FROM employees")
	if err != nil {
		log.Fatal("query: ", err)
	}
	defer rows.Close()
	for rows.Next() {
		r := &res{}

		err = rows.Scan(&r.id, &r.firstName)
		if err != nil {
			log.Fatal("scan", err)
		}
		fmt.Println("id:", r.id, "firstName:", r.firstName)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal("rows err: ", err)
	}

	_, err = pool.Exec(ctx, "DELETE FROM employees;")
	if err != nil {
		log.Fatal("delete", err)
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
