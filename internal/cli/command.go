package cli

import (
	"net"

	"github.com/pkg/errors"
)

var (
	ErrNoParams        = errors.New("command params is empty")
	ErrNoAllowedParams = errors.New("the number of parameters exceeds the allowable")
)

type commander interface {
	validate(params []string) error
	ping() error
	exec() error
	payload() []string
}

func newCommand(typ string, conn net.Conn, params []string) (p []string, err error) {
	var cc commander
	switch typ {
	case setCommand:
		cc = &settcommand{
			conn: conn,
		}
	case getCommand:
		cc = &getcommand{
			conn: conn,
		}
	case commitCommand:
		cc = &commitcommand{
			conn: conn,
		}
	default:
		return nil, errors.New("S_ERR: undefined command")
	}

	err = cc.validate(params)
	if err != nil {
		return nil, errors.Wrap(err, "S_ERR")
	}

	err = cc.ping()
	if err != nil {
		return nil, errors.Wrap(err, "E_ERR")
	}

	err = cc.exec()
	if err != nil {
		return nil, errors.Wrap(err, "E_ERR")
	}

	return cc.payload(), nil
}
