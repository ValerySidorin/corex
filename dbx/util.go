package dbx

import (
	"errors"
	"net/url"
	"strings"
)

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

type safeAddr struct {
	host   string
	dbname string
}

func GetHost(dsn string) (string, error) {
	settings, kvErr := parseKeywordValueSettings(dsn)
	u, uErr := url.Parse(dsn)

	if uErr != nil {
		if kvErr != nil {
			return "", errors.Join(kvErr, uErr)
		}

		return settings["host"], nil
	}

	return u.Host, nil
}

func GetDatabase(dsn string) (string, error) {
	settings, kvErr := parseKeywordValueSettings(dsn)
	u, uErr := url.Parse(dsn)

	if uErr != nil {
		if kvErr != nil {
			return "", errors.Join(kvErr, uErr)
		}

		return settings["database"], nil
	}

	return u.Path[1:], nil
}

// getSafeAddr returns host+path from dsn.
func getSafeAddr(dsn string) (*safeAddr, error) {
	settings, kvErr := parseKeywordValueSettings(dsn)
	u, uErr := url.Parse(dsn)

	if uErr != nil {
		if kvErr != nil {
			return nil, errors.Join(kvErr, uErr)
		}

		return &safeAddr{
			host:   settings["host"],
			dbname: settings["database"],
		}, nil
	}

	return &safeAddr{
		host:   u.Host,
		dbname: u.Path[1:],
	}, nil
}

func (a *safeAddr) String() string {
	return a.host + "/" + a.dbname
}

func parseKeywordValueSettings(s string) (map[string]string, error) {
	settings := make(map[string]string)

	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return nil, errors.New("invalid keyword/value")
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		if len(s) == 0 {
		} else if s[0] != '\'' {
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, errors.New("invalid backslash")
					}
				}
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		} else { // quoted string
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return nil, errors.New("unterminated quoted string in connection info string")
			}
			val = strings.Replace(strings.Replace(s[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(s) {
				s = ""
			} else {
				s = s[end+1:]
			}
		}

		if k, ok := nameMap[key]; ok {
			key = k
		}

		if key == "" {
			return nil, errors.New("invalid keyword/value")
		}

		settings[key] = val
	}

	return settings, nil
}
