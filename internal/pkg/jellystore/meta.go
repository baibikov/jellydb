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
	"os"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/utils"
)

type meta struct {
	file      *os.File
	written   *written
	committed *committed
}

func openMeta(path string) (*meta, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, errors.Wrapf(err, "open metafile by path - %s", path)
	}

	return &meta{
		file: file,
		committed: &committed{
			file: file,
		},
		written: &written{
			file: file,
		},
	}, nil
}

func (m *meta) Close() error {
	return m.file.Close()
}

type written struct {
	file *os.File
	last uint32
}

type offsetType uint32

func (o offsetType) uint32() uint32 {
	return uint32(o)
}

func (o offsetType) equal(u offsetType) bool {
	return o == u
}

func (o offsetType) int64() int64 {
	return int64(o)
}

const (
	writtenReaderOffset   = 0
	committedReaderOffset = 4
)

func (w *written) offset() (offsetType, error) {
	off, err := utils.Uint32FromReaderAt(w.file, writtenReaderOffset, messageLen)
	if err != nil {
		return 0, errors.Wrap(err, "read written offset")
	}
	w.last = off
	return offsetType(off), nil
}

func (w *written) write(u uint32) error {
	return errors.Wrap(writeWithLatest(w.file, w.last, u), "write written offset")
}

func writeWithLatest(file *os.File, last, new uint32) error {
	wo := last
	if new > last {
		wo = new
	}
	return utils.Uint32ToWriter(file, messageLen, wo)
}

type committed struct {
	file *os.File
	last uint32
}

func (c *committed) offset() (offsetType, error) {
	off, err := utils.Uint32FromReaderAt(c.file, committedReaderOffset, messageLen)
	if err != nil {
		return 0, errors.Wrap(err, "read committed offset")
	}
	c.last = off
	return offsetType(off), nil
}

func (c *committed) write(u uint32) error {
	return errors.Wrap(writeWithLatest(c.file, c.last, u), "write committed offset")
}
