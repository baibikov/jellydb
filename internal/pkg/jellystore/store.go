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

	subject *subject
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
		subject: new(subject),
	}, nil
}

func (s *Store) Get(key string, n int64) ([][]byte, error) {
	m, err := s.subject.load(key)
	if err != nil {
		return nil, err
	}

	return m.batch(n), nil
}

func (s *Store) Commit(key string, n int64) error {
	m, err := s.subject.load(key)
	if err != nil {
		return err
	}

	m.commit(n)
	return nil
}

func (s *Store) Set(key string, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	return s.subject.store(key).append(value)
}

func (s *Store) setWrittenOffset(key string, wo, co int64) {
	m := s.subject.store(key)
	m.writtenOffset = wo
	m.committedOffset = co

	if wo == co {
		m.writtenIndex = 0
		return
	}

	if wo < co {
		return
	}

	// offset shift by commented offset with written offset
	m.writtenIndex = (wo - co) / (messageLen + maxMessageSize)
}
