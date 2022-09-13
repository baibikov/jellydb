package protomarshal

import (
	"io"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Encoder struct {
	size int
	r    io.Reader
}

func (e *Encoder) Encode(m proto.Message) error {
	bb := make([]byte, e.size)
	n, err := e.r.Read(bb)
	if err != nil {
		return errors.Wrap(err, "read message by reader")
	}
	if n == 0 {
		return errors.New("empty message from reader")
	}

	return errors.Wrapf(proto.Unmarshal(bb[:n], m), "encode message by proto with size %d", e.size)
}

func NewEncoder(reader io.Reader, size int) *Encoder { return &Encoder{r: reader, size: size} }

type Decoder struct {
	w io.Writer
}

func (d *Decoder) Decode(m proto.Message) error {
	bb, err := proto.Marshal(m)
	if err != nil {
		return errors.Wrap(err, "decode message by proto")
	}
	n, err := d.w.Write(bb)
	if err != nil {
		return errors.Wrap(err, "write message by writer")
	}
	if n == 0 {
		return errors.New("empty message write")
	}

	return nil
}

func NewDecoder(writer io.Writer) *Decoder { return &Decoder{w: writer} }
