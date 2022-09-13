package cli

import (
	"net"
	"strconv"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

type commitcommand struct {
	conn net.Conn

	key string
	n   int64
}

func (c *commitcommand) validate(params []string) (err error) {
	if len(params) == 0 {
		return ErrNoParams
	}
	if len(params) != 2 {
		return ErrNoAllowedParams
	}

	c.key = params[nIndex]
	c.n, err = strconv.ParseInt(params[nIndex], 10, 64)
	if err != nil {
		return errors.Errorf("%s is not int64", params[nIndex])
	}

	return nil
}

func (c *commitcommand) exec() error {
	mm := &messages.CommitRequest{
		Key: c.key,
		N:   c.n,
	}
	err := protomarshal.NewDecoder(c.conn).Decode(mm)
	if err != nil {
		return errors.Wrapf(err, "%s command exec", commitCommand)
	}

	err = readResponse(c.conn)
	if err != nil {
		return err
	}

	return nil
}

func (c *commitcommand) payload() []string {
	return []string{"👌"}
}

func (c *commitcommand) ping() error {
	_, err := c.conn.Write([]byte("3"))
	return errors.Wrapf(err, "%s ping the tcp server", commitCommand)
}
