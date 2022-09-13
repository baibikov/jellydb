package tcp

import (
	"net"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/internal/pkg/jell"
)

type Server struct {
	listener net.Listener
	jelly    jell.Jelly
	closed   bool
}

func (s *Server) Close() error {
	s.closed = true
	return s.listener.Close()
}

const (
	tcpNetwork = "tcp"
)

type Config struct {
	Addr string
}

func New(config *Config, jelly jell.Jelly) (*Server, error) {
	if config == nil {
		return nil, errors.New("config has not be empty")
	}

	if config.Addr == "" {
		return nil, errors.New("config addr has not be empty")
	}

	listener, err := net.Listen(tcpNetwork, config.Addr)
	if err != nil {
		return nil, errors.Wrap(err, "listen connection")
	}

	return &Server{
		listener: listener,
		jelly:    jelly,
	}, nil
}
