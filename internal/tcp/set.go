package tcp

import (
	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (h *handler) set() (err error) {
	req := &messages.SetRequest{}
	err = protomarshal.NewEncoder(h.conn, setMessageSize).Encode(req)
	if err != nil {
		return errors.Wrap(err, "get 'set' state")
	}

	err = wrapMessageResponse(h.conn, h.jelly.Set(req.GetKey(), req.GetMessage()))
	return errors.Wrap(err, "send response message")
}
