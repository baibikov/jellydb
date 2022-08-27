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
	"testing"

	"github.com/stretchr/testify/require"
)

const testPath = "./test_path"

var testConfig = &Config{
	Path: "test_path",
}

func TestStore_Commit(t *testing.T) {
	tests := []struct {
		Name      string
		Batch     [][]byte
		Batc2     [][]byte
		Key       string
		Commit    int64
		GetBatch  int64
		GetBatch2 int64
		Want      int64
		Want2     int64
	}{
		{
			Name: "negative-commit",
			Key:  "negative-commit",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   -1,
			GetBatch: 10,
			Want:     10,
		},
		{
			Name: "zero-commit",
			Key:  "zero-commit",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   0,
			GetBatch: 10,
			Want:     10,
		},
		{
			Name: "one-more",
			Key:  "one-more",
			Batch: [][]byte{
				[]byte("message1"),
			},
			Batc2: [][]byte{
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:    1,
			GetBatch:  1,
			GetBatch2: 9,
			Want:      0,
			Want2:     9,
		},
		{
			Name: "two-more",
			Key:  "two-more",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
			},
			Batc2: [][]byte{
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:    2,
			GetBatch:  2,
			GetBatch2: 8,
			Want:      0,
			Want2:     8,
		},
		{
			Name: "all-commit",
			Key:  "all-commit",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   10,
			GetBatch: 10,
			Want:     0,
		},
		{
			Name: "one-up-all",
			Key:  "one-up-all",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   9,
			GetBatch: 10,
			Want:     1,
		},
		{
			Name: "primitive",
			Key:  "primitive-key",
			Batch: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
				[]byte("e"),
				[]byte("f"),
			},
			Commit:   3,
			GetBatch: 3,
			Want:     3,
		},
		{
			Name: "not-committed",
			Key:  "not-committed-key",
			Batch: [][]byte{
				[]byte("message1"),
			},
			Commit:   0,
			GetBatch: 1,
			Want:     1,
		},
		{
			Name: "not-committed-2-messages",
			Key:  "not-committed-2-messages",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
			},
			Commit:   0,
			GetBatch: 2,
			Want:     2,
		},
		{
			Name: "one-committed-message",
			Key:  "one-committed-message",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
			},
			Commit:   1,
			GetBatch: 1,
			Want:     1,
		},
		{
			Name: "50-to-50",
			Key:  "50-to-50",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   5,
			GetBatch: 5,
			Want:     5,
		},
		{
			Name: "99-to-99",
			Key:  "99-to-99",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   1,
			GetBatch: 9,
			Want:     9,
		},
		{
			Name: "80-to-80",
			Key:  "80-to-80",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
			},
			Commit:   8,
			GetBatch: 2,
			Want:     2,
		},
		{
			Name: "tree-committed-message",
			Key:  "tree-committed-message",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
			},
			Commit:   3,
			GetBatch: 1,
			Want:     1,
		},
		{
			Name: "multi-committed-message",
			Key:  "multi-committed-message",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
				[]byte("message6"),
				[]byte("message7"),
				[]byte("message8"),
				[]byte("message9"),
				[]byte("message10"),
				[]byte("message11"),
				[]byte("message12"),
				[]byte("message13"),
				[]byte("message14"),
				[]byte("message15"),
				[]byte("message16"),
			},
			Commit:   10,
			GetBatch: 6,
			Want:     6,
		},
		{
			Name: "one-message",
			Key:  "one-message",
			Batch: [][]byte{
				[]byte("message1"),
			},
			Commit:   1,
			GetBatch: 1,
			Want:     0,
		},
		{
			Name: "two-committed-message",
			Key:  "two-committed-message",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
			},
			Commit:   2,
			GetBatch: 1,
			Want:     1,
		},
		{
			Name: "all-tree-committed-message",
			Key:  "all-tree-committed-message",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
			},
			Commit:   3,
			GetBatch: 0,
			Want:     0,
		},
	}

	s, err := New(testConfig)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			for _, b := range tt.Batch {
				err = s.Set(tt.Key, b)
				require.NoError(t, err)
			}
			err = s.Commit(tt.Key, tt.Commit)
			require.NoError(t, err)
			bb, err := s.Get(tt.Key, tt.GetBatch)
			require.NoError(t, err)
			require.Equal(t, tt.Want, int64(len(bb)))

			if len(tt.Batc2) == 0 {
				return
			}

			for _, b := range tt.Batc2 {
				err = s.Set(tt.Key, b)
				require.NoError(t, err)
			}

			bb, err = s.Get(tt.Key, tt.GetBatch2)
			require.NoError(t, err)
			require.Equal(t, tt.Want2, int64(len(bb)))
		})
	}
}

func TestStore_Get(t *testing.T) {
	tests := []struct {
		Name   string
		Key    string
		Batch  [][]byte
		Want   [][]byte
		Get    int64
		Commit int64
	}{
		{
			Name: "simple",
			Key:  "simple-get",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Want: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get: 5,
		},
		{
			Name: "one-commit",
			Key:  "one-commit-get",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Want: [][]byte{
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Commit: 1,
			Get:    5,
		},
		{
			Name: "all-commit",
			Key:  "all-commit-get",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Want:   [][]byte{},
			Commit: 5,
			Get:    5,
		},
		{
			Name: "99-to-one",
			Key:  "99-to-one-get",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Want: [][]byte{
				[]byte("message5"),
			},
			Commit: 4,
			Get:    5,
		},
	}

	store, err := New(testConfig)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			for _, bb := range tt.Batch {
				err := store.Set(tt.Key, bb)
				require.NoError(t, err)
			}

			err = store.Commit(tt.Key, tt.Commit)
			require.NoError(t, err)

			bb, err := store.Get(tt.Key, tt.Get)
			require.NoError(t, err)
			require.Equal(t, tt.Want, bb)
			require.Equal(t, len(tt.Want), len(bb))
		})
	}

}
