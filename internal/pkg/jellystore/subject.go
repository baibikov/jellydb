package jellystore

import (
	"sync"

	"github.com/pkg/errors"
)

type subject struct {
	sync.Map
}

func (s *subject) load(key string) (*message, error) {
	val, ok := s.Load(key)
	if !ok {
		return nil, errors.Errorf("value by %s not found", key)
	}

	m, ok := val.(*message)
	if !ok {
		return nil, errors.Errorf("fatal type assertion to message %[1]T %+[1]v", val)
	}

	return m, nil
}

func (s *subject) srange(f func(key string, value *message) error) (err error) {
	s.Range(func(key, value any) bool {
		kk, _ := key.(string)
		vv, _ := value.(*message)
		if err = f(kk, vv); err != nil {
			return false
		}

		return true
	})

	return err
}

func (s *subject) store(key string) *message {
	val, ok := s.Load(key)
	if ok {
		m, _ := val.(*message)
		return m
	}

	mm := newMessage()
	s.Store(key, mm)
	return mm
}
