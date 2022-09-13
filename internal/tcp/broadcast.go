package tcp

import (
	"context"
	"io"
	"net"
	"strconv"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/baibikov/jellydb/internal/pkg/jell"
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
				// not error because client has closed
				return nil
			}
			if err != nil {
				return errors.Wrap(err, "accepting client connection")
			}

			go func() {
				if err := newhandler(conn, s.jelly).do(ctx); err != nil {
					logrus.Error(err)
				}
			}()
		}
	}
}

type handler struct {
	conn  net.Conn
	jelly jell.Jelly
}

func newhandler(conn net.Conn, jelly jell.Jelly) *handler {
	return &handler{conn: conn, jelly: jelly}
}

const (
	pingMessageSize = 1

	setMessageSize    = 256
	getMessageSize    = 256
	commitMessageSize = 256
)

const (
	setMessageType = iota + 1
	getMessageType
	commitMessageType
)

func (h *handler) do(ctx context.Context) (err error) {
	defer func() {
		tryClose(h.conn, err, "do")
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		bb := make([]byte, pingMessageSize)
		n, err := h.conn.Read(bb)
		if err != nil {
			return errors.Wrap(err, "read ping message")
		}
		if n == 0 {
			return errors.New("ping message empty")
		}

		typ, err := strconv.Atoi(string(bb))
		if err != nil {
			return errors.Wrapf(err, "string unfolding - %s", string(bb))
		}

		logrus.Debugf("processing message by type - %d", typ)

		switch typ {
		case setMessageType:
			err = h.set()
		case getMessageType:
			err = h.get()
		case commitMessageType:
			err = h.commit()
		default:
			return errors.Errorf("undefined message type - %d", typ)
		}
		if err != nil {
			logrus.Error(err)
		}
	}
}

const (
	statusCodeOK  = 20
	StatusCodeBad = 50
)

func tryClose(conn net.Conn, err error, space string) {
	if v := recover(); v != any(nil) {
		logrus.Errorf("space %s rec error: %+v", space, v)
	}

	if err := conn.Close(); err != nil {
		logrus.Error(err)
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

	return protomarshal.NewDecoder(conn).Decode(&messages.Response{
		Error: message,
		Code:  int32(code),
	})
}
