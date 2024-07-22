package sql

import "strings"

func plsqlQueryWithLockCheck(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "for update") ||
		strings.Contains(query, "for no key update") ||
		strings.Contains(query, "for share") ||
		strings.Contains(query, "for key share")
}

func mysqlQueryWithLockCheck(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "for update") ||
		strings.Contains(query, "for share")
}

func tsqlQueryWithLockCheck(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "with") &&
		strings.Contains(query, "lock")
}
