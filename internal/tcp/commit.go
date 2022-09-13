package tcp

import (
	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (h *handler) commit() (err error) {
	req := &messages.CommitRequest{}
	err = protomarshal.NewEncoder(h.conn, commitMessageSize).Encode(req)
	if err != nil {
		return errors.Wrap(err, "get state")
	}

	err = wrapMessageResponse(h.conn, h.jelly.Commit(req.GetKey(), req.GetN()))
	return errors.Wrap(err, "send response message")
}
