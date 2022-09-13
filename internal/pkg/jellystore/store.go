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
	"sync"

	"github.com/pkg/errors"
)

type Store struct {
	mutex  sync.RWMutex
	config *Config

	mpstate map[string]*message
}

func New(config *Config) (*Store, error) {
	if config == nil {
		return nil, errors.New("config has not be empty")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Store{
		config:  config,
		mpstate: map[string]*message{},
	}, nil
}

func (s *Store) Get(key string, n int64) ([][]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, ok := s.mpstate[key]
	if !ok || v == nil {
		return nil, errors.Errorf("store has not key %s", key)
	}

	return v.batch(n), nil
}

func (s *Store) Commit(key string, n int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, ok := s.mpstate[key]
	if !ok || v == nil {
		return errors.Errorf("store has not key %s", key)
	}

	v.commit(n)
	return nil
}

func (s *Store) Set(key string, value []byte) error {
	if len(value) == 0 {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	v, ok := s.mpstate[key]
	if !ok {
		nm := newMessage()
		s.mpstate[key] = nm
		v = s.mpstate[key]
	}

	return v.append(value)
}

func (s *Store) setWrittenOffset(key string, wo, co int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	v, ok := s.mpstate[key]
	if !ok {
		nm := newMessage()
		s.mpstate[key] = nm
		v = nm
	}

	v.writtenOffset = wo
	v.committedOffset = co

	if wo == co {
		v.writtenIndex = 0
		return
	}

	if wo > co {
		// offset shift by commented offset with written offset
		v.writtenIndex = (wo - co) / (messageLen + maxMessageSize)
		return
	}
}
