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

import "github.com/pkg/errors"

type message struct {
	queue            [][]byte
	firstCommitIndex int64
	lastCommitIndex  int64

	writtenOffset   int64
	committedOffset int64

	writtenIndex   int64
	committedIndex int64
}

func (m *message) len() int64 {
	return int64(len(m.queue))
}

func newMessage() *message {
	return &message{
		queue:            make([][]byte, 0),
		firstCommitIndex: -1,
	}
}

const maxMessageSize = 512

func (m *message) commit(n int64) {
	if n <= 0 {
		return
	}

	m.firstCommitIndex = m.lastCommitIndex

	// if committed once message
	// we add the following only next commit message
	// without batch
	if n-1 == 0 {
		m.lastCommitIndex = m.lastCommitIndex + 1
		return
	}

	// if the batch of messages is greater than the number of messages
	// in the queue, then we substitute the last committed index as the
	// length of the queue itself
	if n-1 > m.len()-1 {
		m.lastCommitIndex = m.len() - 1
		return
	}

	// commit message with next batch
	m.lastCommitIndex = m.lastCommitIndex + 1 + n - 1
}

func (m *message) batch(n int64) [][]byte {
	if n <= 0 {
		return nil
	}

	index := m.lastCommitIndex

	// case of one committed message
	if m.lastCommitIndex == 0 && m.lastCommitIndex == m.firstCommitIndex {
		index += 1
	}

	if index > m.len() {
		return nil
	}

	sliceUp := index + n

	if n > m.len()-1 || sliceUp > m.len() {
		return m.queue[index:]
	}

	return m.queue[index:sliceUp]
}

func (m *message) append(b []byte) error {
	if m.queue == nil {
		m.queue = make([][]byte, 0)
	}
	if len(b) > maxMessageSize {
		return errors.Errorf("transmitted message is larger than allowed")
	}

	m.queue = append(m.queue, b)
	return nil
}
