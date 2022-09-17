package main

import (
	"context"
	"flag"
	"log"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"github.com/baibikov/jellydb/internal/cli"
	"github.com/baibikov/jellydb/pkg/notifyctx"
)

func main() {
	flags, err := parse()
	if err != nil {
		logrus.Error(err)
		return
	}

	if err := runApp(flags); err != nil {
		log.Fatalln(err)
	}
}

type Flags struct {
	addr string
}

func parse() (*Flags, error) {
	var addr string
	flag.StringVar(&addr, "addr", "", "an init")

	flag.Parse()
	if addr == "" {
		return nil, errors.New("addr is required param")
	}
	return &Flags{
		addr: addr,
	}, nil
}

func runApp(f *Flags) (err error) {
	ctx := context.Background()
	ctx, cancel := notifyctx.WrapExitContext(ctx)
	defer cancel()

	c, err := cli.New(&cli.Config{
		Addr: f.addr,
	})
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&err, multierr.Close(c))

	go func() {
		c.Run(ctx)

		// close app CLI after exit
		cancel()
	}()
	<-ctx.Done()
	return nil
}
