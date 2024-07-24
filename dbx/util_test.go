package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSafeAddr(t *testing.T) {
	t.Run("url", func(t *testing.T) {
		url := "https://user:password@localhost:1111/mydb?param=1"
		safeAddr, err := getSafeAddr(url)
		assert.Nil(t, err)

		assert.Equal(t, "localhost:1111", safeAddr.host)
		assert.Equal(t, "mydb", safeAddr.dbname)
	})

	t.Run("dsn", func(t *testing.T) {
		dsn := "host=localhost:9999 dbname=mydb2"
		safeAddr, err := getSafeAddr(dsn)
		assert.Nil(t, err)

		assert.Equal(t, "localhost:9999", safeAddr.host)
		assert.Equal(t, "mydb2", safeAddr.dbname)
	})
}
