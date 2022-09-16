// Package store
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

func TestStore_Load(t *testing.T) {
	makeTestPath(t)

	tests := []struct {
		Name   string
		Key    string
		Get    int64
		Commit int64
		Batch  [][]byte
		Want   int
	}{
		{
			Name: "simple",
			Key:  "simple-load",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get:  5,
			Want: 5,
		},
		{
			Name: "all-committed",
			Key:  "all-committed-load",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get:    5,
			Commit: 5,
			Want:   0,
		},
		{
			Name: "one-committed",
			Key:  "one-committed-load",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get:    5,
			Commit: 1,
			Want:   4,
		},
		{
			Name: "two-committed",
			Key:  "two-committed-load",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get:    5,
			Commit: 2,
			Want:   3,
		},
		{
			Name: "99-to-1",
			Key:  "99-to-1-load",
			Batch: [][]byte{
				[]byte("message1"),
				[]byte("message2"),
				[]byte("message3"),
				[]byte("message4"),
				[]byte("message5"),
			},
			Get:    5,
			Commit: 4,
			Want:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			err := os.RemoveAll(testPath + "/" + tt.Key)
			require.NoError(t, err)

			unloadStore, err := New(testConfig)
			require.NoError(t, err)
			for _, bb := range tt.Batch {
				err := unloadStore.Set(tt.Key, bb)
				require.NoError(t, err)
			}

			err = unloadStore.Commit(tt.Key, tt.Commit)
			require.NoError(t, err)

			err = unloadStore.Unload(context.Background())
			require.NoError(t, err)

			loadStore, err := New(testConfig)
			require.NoError(t, err)

			err = loadStore.Load(context.Background())
			require.NoError(t, err)

			bb, err := loadStore.Get(tt.Key, tt.Get)
			require.NoError(t, err)

			require.Equal(t, tt.Want, len(bb))
		})
	}
}
