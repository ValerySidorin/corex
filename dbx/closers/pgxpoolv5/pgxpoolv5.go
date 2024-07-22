package pgxpoolv5

import "github.com/jackc/pgx/v5/pgxpool"

func Close(p *pgxpool.Pool) error {
	p.Close()
	return nil
}
