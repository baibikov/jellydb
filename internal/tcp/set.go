package tcp

import (
	"context"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (h *handler) set(ctx context.Context) (err error) {
	defer tryError(err, "set")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			req := &messages.SetRequest{}
			err := protomarshal.NewEncoder(h.conn, setMessageSize).Encode(req)
			if err != nil {
				return errors.Wrap(err, "get 'set' state")
			}

			err = wrapMessageResponse(h.conn, h.store.Set(req.GetKey(), req.GetMessage()))
			if err != nil {
				return errors.Wrap(err, "send response message")
			}
		}
	}
}
