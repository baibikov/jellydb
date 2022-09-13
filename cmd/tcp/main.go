package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"github.com/baibikov/jellydb/internal/pkg/jellystore"
	"github.com/baibikov/jellydb/internal/tcp"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func main() {
	flags, err := parse()
	if err != nil {
		logrus.Error(err)
		return
	}

	if err := runApp(flags); err != nil {
		logrus.Fatalln(err)
	}
}

type Flags struct {
	addr string
	path string
}

func parse() (*Flags, error) {
	var addr string
	flag.StringVar(&addr, "addr", "", "an init")

	var path string
	flag.StringVar(&path, "path", "./.data", "")

	flag.Parse()
	if addr == "" {
		return nil, errors.New("addr is required param")
	}
	return &Flags{
		addr: addr,
		path: path,
	}, nil
}

func runApp(f *Flags) (err error) {
	logrus.Info("init app context")
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(
		ctx,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
	)
	defer cancel()

	logrus.Info("init jellystore")
	jellyConfig := &jellystore.Config{
		Path: f.path,
	}
	store, err := jellystore.New(jellyConfig)
	if err != nil {
		return errors.Wrap(err, "init jellystore")
	}

	logrus.Infof("init tcp app on port %s", f.addr)
	tcpConfig := &tcp.Config{
		Addr: f.addr,
	}
	server, err := tcp.New(tcpConfig, store)
	if err != nil {
		return errors.Wrap(err, "init tcp connection")
	}
	defer multierr.AppendInvoke(&err, multierr.Close(server))

	go func() {
		logrus.Info("broadcast the server")
		berr := server.Broadcast(ctx)
		if berr != nil {
			multierr.AppendInto(&err, berr)
			cancel()
		}
	}()

	<-ctx.Done()
	return err
}
