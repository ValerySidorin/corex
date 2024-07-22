package errx

import "fmt"

func Wrap(msg string, err error) error {
	if err != nil {
		return fmt.Errorf(msg+": %w", err)
	}

	return nil
}
