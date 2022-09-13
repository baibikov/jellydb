package cli

import (
	"net"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

type settcommand struct {
	conn net.Conn

	key     string
	message []byte
}

func (s *settcommand) validate(params []string) error {
	if len(params) == 0 {
		return ErrNoParams
	}

	if len(params) != 2 {
		return ErrNoAllowedParams
	}

	s.key = params[keyIndex]
	s.message = []byte(params[messageIndex])

	return nil
}

func (s *settcommand) exec() error {
	mm := &messages.SetRequest{
		Key:     s.key,
		Message: s.message,
	}
	err := protomarshal.NewDecoder(s.conn).Decode(mm)
	if err != nil {
		return errors.Wrapf(err, "%s command exec", setCommand)
	}

	err = readResponse(s.conn)
	if err != nil {
		return err
	}

	return nil
}

func (s *settcommand) payload() []string {
	return []string{"👌"}
}

func (s *settcommand) ping() error {
	_, err := s.conn.Write([]byte("1"))
	return errors.Wrapf(err, "%s ping the tcp server", setCommand)
}
