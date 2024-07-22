package main

import (
	stdsql "database/sql"

	"github.com/ValerySidorin/corex/dbx"
	"github.com/ValerySidorin/corex/dbx/impl/sql"
	"github.com/ValerySidorin/corex/errx"
)

/*
This is a typical ideal repo. Usage:
	var db *sql.DB // initialized as nil to simplify example
	r := New(db)

	err := r.CreateSomething("val")
	if err != nil {
		return errx.Wrap("create something", err)
	}

	_ = r.DoTx(func(s Storer) error {
		err := s.CreateSomething("val1")
		if err != nil {
			return errx.Wrap("create something in tx", err)
		}

		val, err := s.GetSomething()
		if err != nil {
			return errx.Wrap("get something in tx", err)
		}

		fmt.Println(val)

		return nil
	}, true)
*/

type Storer interface {
	DoTx(f func(s Storer) error, readonly bool) error

	GetSomething() (string, error)
	CreateSomething(val string) error
}

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

func (st *Store) DoTx(f func(s Storer) error, readonly bool) error {
	err := st.db.DoTx(func(db dbx.DBxer[*stdsql.DB, *stdsql.Tx, *stdsql.TxOptions]) error {
		err := f(New(db.(*sql.DB)))
		return errx.Wrap("call f", err)
	}, &stdsql.TxOptions{
		ReadOnly: readonly,
	})

	return errx.Wrap("do tx", err)
}

func (st *Store) GetSomething() (string, error) {
	var res string
	err := st.db.QueryRow("").Scan(&res)
	return res, errx.Wrap("query row", err)
}

func (st *Store) CreateSomething(val string) error {
	_, err := st.db.Exec("")
	return errx.Wrap("exec", err)
}
