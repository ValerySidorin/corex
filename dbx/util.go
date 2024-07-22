package dbx

import (
	"net/url"

	"github.com/ValerySidorin/corex/errx"
)

type safeAddr struct {
	host string
	path string
}

// getSafeAddr returns host+path from dsn.
func getSafeAddr(dsn string) (*safeAddr, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errx.Wrap("parse url", err)
	}
	u.User = nil
	u.Scheme = ""
	return &safeAddr{
		host: u.Host,
		path: u.Path,
	}, nil
}

func (a *safeAddr) String() string {
	return a.host + a.path
}
