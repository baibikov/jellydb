package cli

import (
	"net"
	"strconv"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

const (
	StatusCodeBad = 50
)

const (
	keyIndex     = 0
	messageIndex = 1
	nIndex       = 1

	messageSize = 256
)

type getcommand struct {
	conn net.Conn

	key string
	n   int64

	pp []string
}

func (g *getcommand) validate(params []string) (err error) {
	if len(params) == 0 {
		return ErrNoParams
	}

	if len(params) != 2 {
		return ErrNoAllowedParams
	}

	g.key = params[keyIndex]
	g.n, err = strconv.ParseInt(params[nIndex], 10, 64)
	if err != nil {
		return errors.Errorf("%s is not int64", params[nIndex])
	}

	return nil
}

func (g *getcommand) exec() error {
	err := protomarshal.NewDecoder(g.conn).Decode(&messages.GetRequest{
		Key: g.key,
		N:   g.n,
	})
	if err != nil {
		return errors.Wrapf(err, "%s write message to tcp server", getCommand)
	}

	err = readResponse(g.conn)
	if err != nil {
		return err
	}

	resp := &messages.GetResponse{}
	err = protomarshal.NewEncoder(g.conn, messageSize).Encode(resp)
	if err != nil {
		return errors.Wrapf(err, "%s read data from tcp server", getCommand)
	}

	g.pp = make([]string, len(resp.Messages))
	for i := 0; i < len(resp.Messages); i++ {
		g.pp[i] = string(resp.Messages[i])
	}

	return nil
}

func (g getcommand) payload() []string {
	return g.pp
}

func (g *getcommand) ping() error {
	_, err := g.conn.Write([]byte("2"))
	return errors.Wrapf(err, "%s ping the tcp server", getCommand)
}

func readResponse(conn net.Conn) error {
	mm := &messages.Response{}
	err := protomarshal.NewEncoder(conn, messageSize).Encode(mm)
	if err != nil {
		return errors.Wrapf(err, "%s read message from tcp server", getCommand)
	}
	if mm.Code == StatusCodeBad {
		return errors.Errorf("%s bad request: message %s", getCommand, mm.GetError())
	}

	return nil
}
