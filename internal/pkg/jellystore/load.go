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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

func (s *Store) Load(ctx context.Context) error {
	return s.load(ctx)
}

func (s *Store) load(ctx context.Context) error {
	entities, err := os.ReadDir(s.config.Path)
	if err != nil {
		return errors.Wrapf(err, "read dir by path - %s", s.config.Path)
	}
	if len(entities) == 0 {
		return nil
	}

	left, right := entities[:len(entities)/2], entities[len(entities)/2:]

	eg := errgroup.Group{}
	eg.Go(func() error {
		return s.loadEntities(ctx, left)
	})

	eg.Go(func() error {
		return s.loadEntities(ctx, right)
	})

	return errors.Wrap(eg.Wait(), "load files by entities")
}

const (
	logFileName  = "log.jelly.db"
	metaFileName = "meta.jelly.format"
)

func (s *Store) loadEntities(ctx context.Context, entities []os.DirEntry) error {
	for _, e := range entities {
		select {
		case <-ctx.Done():
			return errors.New("failed to load all file data")
		default:
			if err := s.loadByFile(e.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	messageLen = 4
)

func (s *Store) loadByFile(key string) (err error) {
	// usable path by db data
	pdata := fmt.Sprintf("%s/%s/%s", s.config.Path, key, logFileName)

	metaInfo, err := openMeta(fmt.Sprintf("%s/%s/%s", s.config.Path, key, metaFileName))
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&err, multierr.Close(metaInfo))

	writtenOffset, err := metaInfo.written.offset()
	if err != nil {
		return err
	}

	committedOffset, err := metaInfo.committed.offset()
	if err != nil {
		return err
	}

	s.setWrittenOffset(key, writtenOffset.int64(), committedOffset.int64())
	if committedOffset.uint32() == writtenOffset.uint32() {
		return nil
	}

	dataFile, err := os.OpenFile(pdata, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "open data file")
	}

	iteration := int64(committedOffset)
	for {
		bb := make([]byte, maxMessageSize+messageLen)
		_, err = dataFile.ReadAt(bb, iteration)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "read messages by key %s from path %s", key, pdata)
		}

		length := binary.LittleEndian.Uint32(bb[:messageLen])
		if messageLen+length > uint32(len(bb)) {
			return errors.New("message slice mismatch for load")
		}

		err = s.Set(key, bb[messageLen:messageLen+length])
		if err != nil {
			return errors.Wrapf(err, "set memorry by key %s from path %s", key, pdata)
		}

		iteration += messageLen + maxMessageSize
	}

	return nil
}
