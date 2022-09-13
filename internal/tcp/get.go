package tcp

import (
	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (h *handler) get() (err error) {
	req := &messages.GetRequest{}
	err = protomarshal.NewEncoder(h.conn, getMessageSize).Encode(req)
	if err != nil {
		return errors.Wrap(err, "get state")
	}

	bytes, err := h.jelly.Get(req.GetKey(), req.GetN())
	err = wrapMessageResponse(h.conn, err)
	if err != nil {
		return errors.Wrap(err, "send response message")
	}

	err = protomarshal.NewDecoder(h.conn).Decode(&messages.GetResponse{
		Messages: bytes,
	})
	return errors.Wrap(err, "write bytes response")
}
