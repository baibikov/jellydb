package tcp

import (
	"context"
	"io"
	"net"
	"strconv"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/baibikov/jellydb/internal/pkg/jellystore"
	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (s *Server) Broadcast(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := s.listener.Accept()
			if s.closed {
				return nil
			}
			if err != nil {
				return errors.Wrap(err, "accepting client connection")
			}

			go func() {
				if err := newhandler(conn, s.store).do(ctx); err != nil {
					logrus.Error(err)
				}
			}()
		}
	}
}

type handler struct {
	conn  net.Conn
	store *jellystore.Store
}

func newhandler(conn net.Conn, store *jellystore.Store) *handler {
	return &handler{conn: conn, store: store}
}

const (
	pingMessageSize = 1

	setMessageSize    = 128
	getMessageSize    = 128
	commitMessageSize = 128
)

const (
	setMessageType = iota + 1
	getMessageType
	commitMessageType
)

func (h *handler) do(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	defer tryClose(h.conn, "do")

	bb := make([]byte, pingMessageSize)
	n, err := h.conn.Read(bb)
	if err != nil {
		return errors.Wrap(err, "read ping message")
	}
	if n == 0 {
		return errors.New("ping message empty")
	}

	i, err := strconv.Atoi(string(bb))
	if err != nil {
		return errors.Wrapf(err, "string unfolding - %s", string(bb))
	}

	switch i {
	case setMessageType:
		err = h.set(ctx)
	case getMessageType:
		err = h.get(ctx)
	case commitMessageType:
		err = h.commit(ctx)
	default:
		return errors.Errorf("undefined message type - %d", i)
	}

	return err
}

const (
	statusCodeOK  = 20
	StatusCodeBad = 50
)

func tryClose(conn net.Conn, space string) {
	if v := recover(); v != any(nil) {
		logrus.Errorf("space %s rec error: %+v", space, v)
	}

	err := conn.Close()
	if err != nil {
		logrus.Error(space, err)
	}
}

func tryError(err error, space string) {
	if err != nil {
		err = errors.Wrap(err, space)
	}
}

func isSysError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}

func wrapMessageResponse(conn net.Conn, err error) error {
	message := ""
	code := statusCodeOK
	if err != nil {
		message = err.Error()
		code = StatusCodeBad
	}

	err = protomarshal.NewDecoder(conn).Decode(&messages.Response{
		Error: message,
		Code:  int32(code),
	})
	return err
}
