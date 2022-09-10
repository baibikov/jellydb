package tcp

import (
	"context"

	"github.com/pkg/errors"

	"github.com/baibikov/jellydb/pkg/protomarshal"
	"github.com/baibikov/jellydb/protogenerated/messages"
)

func (h *handler) commit(ctx context.Context) (err error) {
	defer tryError(err, "commit")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			req := &messages.CommitRequest{}
			err := protomarshal.NewEncoder(h.conn, commitMessageSize).Encode(req)
			if err != nil {
				return errors.Wrap(err, "get state")
			}

			err = wrapMessageResponse(h.conn, h.store.Commit(req.GetKey(), req.GetN()))
			if err != nil {
				return errors.Wrap(err, "send response message")
			}
		}
	}
}
