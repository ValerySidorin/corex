package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSafeAddr(t *testing.T) {
	url := "https://user:password@localhost:9999/mydb?param=1"
	safeAddr, err := getSafeAddr(url)
	assert.Nil(t, err)

	assert.Equal(t, "localhost:9999", safeAddr.host)
	assert.Equal(t, "/mydb", safeAddr.path)
}
