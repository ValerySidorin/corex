package dbx

import "github.com/ValerySidorin/corex/errx"

func Scan[T any](rows Rows, pointers func(*T) []interface{}) ([]T, error) {
	var res = []T{}

	for rows.Next() {
		var elem T
		if err := rows.Scan(pointers(&elem)...); err != nil {
			return nil, errx.Wrap("row scan", err)
		}

		res = append(res, elem)
	}

	if rows.Err() != nil {
		return nil, errx.Wrap("rows err", rows.Err())
	}

	return res, nil
}
