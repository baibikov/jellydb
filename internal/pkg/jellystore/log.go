// Package jellystore
/*
   Copyright 2022 Jellydb in-memory database
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package jellystore

import (
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/utils"
)

type log struct {
	file *os.File
}

func openLog(path string) (*log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "open logfile by path - %s", path)
	}

	return &log{
		file: file,
	}, nil
}

func (l *log) Close() error {
	return l.file.Close()
}

func (l *log) readAt(b []byte, off int64) (n int, err error) {
	_, err = l.file.ReadAt(b, off)
	if errors.Is(err, io.EOF) {
		return 0, io.EOF
	}

	return 0, err
}

func (l *log) write(bb []byte) error {
	err := utils.Uint32ToWriter(l.file, messageLen, uint32(len(bb)))
	if err != nil {
		return errors.Wrap(err, "write message-len")
	}

	mb := make([]byte, maxMessageSize)
	copy(mb, bb)
	_, err = l.file.Write(mb)
	return errors.Wrap(err, "write message")
}
