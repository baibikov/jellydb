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
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/baibikov/jellydb/pkg/utils"
)

func (s *Store) Unload(ctx context.Context) error {
	return s.unload(ctx)
}

func (s *Store) unload(ctx context.Context) error {
	// iterate over all message values that
	// are present and load only unloaded messages
	return s.subject.srange(func(key string, value *message) error {
		select {
		case <-ctx.Done():
			return errors.New("failed to upload all file data")
		default:
			err := s.unloadByFile(key, value)
			if err != nil {
				return errors.Wrapf(err, "unload by key - %s", key)
			}
		}

		return nil
	})
}

func (s *Store) unloadByFile(key string, m *message) (err error) {
	if len(m.queue) == 0 {
		return nil
	}

	dirPath := fmt.Sprintf("%s/%s", s.config.Path, key)
	err = utils.CreateFileIfNotExists(dirPath)
	if err != nil {
		return errors.Wrap(err, "creating file")
	}

	metaInfo, err := openMeta(fmt.Sprintf("%s/%s", dirPath, metaFileName))
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&err, multierr.Close(metaInfo))

	committedOffset, err := metaInfo.committed.offset()
	if err != nil {
		return err
	}

	writtenOffset, err := metaInfo.written.offset()
	if err != nil {
		return err
	}

	logInfo, err := openLog(fmt.Sprintf("%s/%s", dirPath, logFileName))
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&err, multierr.Close(logInfo))

	// convert the committed messages to the final offset
	newCommittedOffset := m.committedOffset
	for i := m.committedIndex; i < m.lastCommitIndex; i++ {
		newCommittedOffset += maxMessageSize + messageLen
	}

	// convert the written messages to the final offset
	newWrittenOffset := m.writtenOffset
	for i := m.writtenIndex; i < m.len(); i++ {
		newWrittenOffset += maxMessageSize + messageLen

		err = logInfo.write(m.queue[i])
		if err != nil {
			return errors.Wrapf(err, "write message by offset %d", newWrittenOffset)
		}
	}

	err = metaInfo.written.write(uint32(newWrittenOffset))
	if err != nil {
		return err
	}

	err = metaInfo.committed.write(uint32(newCommittedOffset))
	if err != nil {
		return err
	}

	m.writtenOffset = newWrittenOffset
	if writtenOffset.int64() > newWrittenOffset {
		m.writtenOffset = writtenOffset.int64()
	}

	m.committedOffset = newCommittedOffset
	if committedOffset.int64() > newCommittedOffset {
		m.committedOffset = committedOffset.int64()
	}

	m.committedIndex = m.committedOffset / (messageLen + maxMessageSize)
	m.writtenIndex = m.writtenOffset / (messageLen + maxMessageSize)

	return nil
}
