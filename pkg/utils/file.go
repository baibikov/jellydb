package utils

import (
	"os"

	"github.com/pkg/errors"
)

func CreateFileIfNotExists(path string) error {
	_, err := os.Stat(path)
	if !os.IsNotExist(err) {
		return errors.Wrap(err, "stating dir")
	}

	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "mkdir file")
	}

	return nil
}
