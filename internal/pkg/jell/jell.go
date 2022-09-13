// Package jell package exists to describe and use stretchable storage concepts for various implementations
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
package jell

import "context"

// Jelly is a generic connection for working with stretch storage.
//
// Multiple goroutines may invoke methods on a Jelly simultaneously.
type Jelly interface {
	// Get getting uncommitted messages from the batch queue
	// the key must be initialized as part of getting data
	// n - parameter must be positive.
	// For example get batch 10 messages after committed (if exists):
	//
	// 	bb, err := store.Get("some-key", 10)
	// 	if err != nil {
	// 	    log.Fatal(err)
	// 	}
	// 	fmt.Println(bb)
	Get(key string, batch int64) ([][]byte, error) // key to get value, batch pull message for get
	// Commit commenting on a batch of messages, messages will be
	// defined as read (which means commented out).
	// Messages will not be displayed during the data retrieval phase.
	// n - parameter must be positive.
	// For example comment 10 messages (if exists):
	//
	//	err := store.Get("some-key")
	//  if err != nil {
	//      log.Fatal(err)
	//  }
	Commit(key string, batch int64) error //  key to set committed, batch pull messages
	// Set adding an entry to the read queue, as soon as the entry
	// occurs, it will be possible to receive this data
	// value has not been nil or len(value) == 0
	// For example:
	//
	//  err := store.Set("some-key", []byte("some-value"))
	//  if err != nil {
	//      log.Fatal(err)
	//  }
	Set(key string, value []byte) error // key to setting current key and value setting information
	// Unloader the concept of unloading values on a stretchable storage
	Unloader
	// Loader the concept of loading values on a stretchable storage
	Loader
}

type Loader interface {
	// Load - loading all parameters/data from storage.
	// Loading data is necessary for fault-tolerant operation of in-memory storage.
	// Loading occurs through the directory specified in the config,
	// upon completion of the context, there is a possibility that
	// not all data can be included in the storage.
	// For example:
	//  err := storage.Load(ctx)
	//  if err != nil {
	//      log.Fatal(err)
	//  }
	//  bb, err := storage.Get("some-key", 1)
	//  if err != nil {
	//      log.Fatal(err)
	//  }
	//  fmt.Println(bb) // bytes by key: "some-key"
	Load(ctx context.Context) error
}

type Unloader interface {
	// Unload - uploading data, allows you to upload all data for all
	// keys to the directory specified in the config
	// to protect data from unexpected drops.
	// At the end of the context, there is a possibility
	// that not all data will be thrown into the storage
	Unload(ctx context.Context) error
}
