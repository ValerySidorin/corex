package sql

import "database/sql"

func Close(db *sql.DB) error {
	return db.Close()
}
