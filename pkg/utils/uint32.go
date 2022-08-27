package utils

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

func Uint32FromReaderAt(reader io.ReaderAt, off, l int64) (uint32, error) {
	bytes := make([]byte, l)
	_, err := reader.ReadAt(bytes, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	return binary.LittleEndian.Uint32(bytes), nil
}

func Uint32ToWriter(writer io.Writer, l int64, v uint32) error {
	bytes := make([]byte, l)
	binary.LittleEndian.PutUint32(bytes, v)
	_, err := writer.Write(bytes)
	return err
}
