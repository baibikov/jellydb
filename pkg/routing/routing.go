package routing

import "github.com/pkg/errors"

type HandlerFunc func() error

type Routing struct {
	handlers map[comparable]HandlerFunc
}

func New(handlers map[comparable]HandlerFunc) *Routing {
	return &Routing{
		handlers: handlers,
	}
}

func (r *Routing) Distribute(k comparable) error {
	hh, ok := r.handlers[k]
	if !ok {
		return errors.Errorf("undefined key to distribute - %+v", k)
	}

	return hh()
}
