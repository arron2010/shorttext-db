package utils

import (
	"bytes"
	"context"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
)

func WaitFor() {
	ctx := SignalContext(context.Background())
	select {
	case <-ctx.Done():
		return
	}
}

func SignalContext(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		signal.Stop(sigs)
		close(sigs)
		cancel()
	}()

	return ctx
}

//获取当前运行的协程ID
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
