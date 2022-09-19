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
	"github.com/baibikov/jellydb/pkg/routing"
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
				err := newhandler(conn, s.jelly).do(ctx)
				if isSysError(err) {
					logrus.Info("close connection")
					return
				}
				if err != nil {
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
		tryClose(h.conn, "do")
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := h.distribute(); err != nil {
				logrus.Error(err)
			}
		}
	}
}

func (h *handler) distribute() error {
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
		return errors.Wrapf(err, "string unfolding - %s", bb)
	}

	logrus.Debugf("processing message by type - %d", typ)

	route := routing.New(map[interface{}]routing.HandlerFunc{
		setMessageType:    h.set,
		getMessageType:    h.get,
		commitMessageType: h.commit,
	})

	return route.Distribute(typ)
}

const (
	statusCodeOK  = 20
	StatusCodeBad = 50
)

func tryClose(conn net.Conn, space string) {
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
