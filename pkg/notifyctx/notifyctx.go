package notifyctx

import (
	"context"
	"os/signal"
	"syscall"
)

func WrapExitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		ctx,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
	)
}
