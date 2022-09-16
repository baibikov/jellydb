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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore_Unload(t *testing.T) {
	makeTestPath(t)
	tests := []struct {
		Name             string
		Key              string
		Batch            [][]byte
		Commit           []int64
		WantCommitOffset []int64
		WantWriteOffset  int64
		Wants            []int64
	}{
		{
			Name: "simple",
			Key:  "first-test-key-unload",
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
			Commit: []int64{
				2, 2, 2, 2, 2,
			},
			WantCommitOffset: []int64{
				1032, 2064, 3096, 4128, 5160,
			},
			WantWriteOffset: 5160,
		},
		{
			Name: "iteration",
			Key:  "second-test-key-unload",
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
			Commit: []int64{
				3, 3, 4, 0, 0,
			},
			WantCommitOffset: []int64{
				1548, 3096, 5160, 5160, 5160,
			},
			WantWriteOffset: 5160,
		},
		{
			Name: "all",
			Key:  "all-unload",
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
			Commit: []int64{
				10,
			},
			WantCommitOffset: []int64{
				5160,
			},
			WantWriteOffset: 5160,
		},
		{
			Name: "one-by-all",
			Key:  "one-by-all-unload",
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
			Commit: []int64{
				1,
			},
			WantCommitOffset: []int64{
				516,
			},
			WantWriteOffset: 5160,
		},
		{
			Name: "two-by-all",
			Key:  "two-by-all-unload",
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
			Commit: []int64{
				2,
			},
			WantCommitOffset: []int64{
				1032,
			},
			WantWriteOffset: 5160,
		},
		{
			Name: "50-to-50",
			Key:  "50-to-50-unload",
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
				[]byte("message17"),
				[]byte("message18"),
				[]byte("message19"),
				[]byte("message20"),
			},
			Commit: []int64{
				10, 0,
			},
			WantCommitOffset: []int64{
				5160, 5160,
			},
			WantWriteOffset: 10320,
		},
		{
			Name: "zero-committed",
			Key:  "zero-committed-unload",
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
				[]byte("message17"),
				[]byte("message18"),
				[]byte("message19"),
				[]byte("message20"),
			},
			Commit: []int64{
				0,
			},
			WantCommitOffset: []int64{
				0,
			},
			WantWriteOffset: 10320,
		},
	}

	store, err := New(testConfig)
	require.NoError(t, err)

	for _, tt := range tests {
		err := os.RemoveAll(testPath + "/" + tt.Key)
		require.NoError(t, err)

		t.Run(tt.Name, func(t *testing.T) {
			for _, bb := range tt.Batch {
				err := store.Set(tt.Key, bb)
				require.NoError(t, err)
			}

			err = store.Unload(context.Background())
			require.NoError(t, err)

			for i, c := range tt.Commit {
				err = store.Commit(tt.Key, c)
				require.NoError(t, err)

				err = store.Unload(context.Background())
				require.NoError(t, err)

				m, err := openMeta(testPath + "/" + tt.Key + "/" + metaFileName)
				require.NoError(t, err)

				committed, err := m.committed.offset()
				require.NoError(t, err)

				written, err := m.written.offset()
				require.NoError(t, err)

				require.Equal(t, tt.WantCommitOffset[i], committed.int64())
				require.Equal(t, tt.WantWriteOffset, written.int64())

				require.NoError(t, m.Close())
			}
		})
	}
}
