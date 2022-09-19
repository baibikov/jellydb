package routing

import "github.com/pkg/errors"

type HandlerFunc func() error

type Routing struct {
	handlers map[interface{}]HandlerFunc
}

func New(handlers map[interface{}]HandlerFunc) *Routing {
	return &Routing{
		handlers: handlers,
	}
}

func (r *Routing) Distribute(k interface{}) error {
	hh, ok := r.handlers[k]
	if !ok {
		return errors.Errorf("undefined key to distribute - %+v", k)
	}

	return hh()
}
