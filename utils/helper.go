package utils

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"syscall"
	"unsafe"
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

//读取文件，转成文本
func ReadFile(path string) string {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	return string(fd)
}

func ReadBinary(path string) []byte {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	return fd
}

func WriteFile(name, content string) bool {
	data := []byte(content)
	if ioutil.WriteFile(name, data, 0644) == nil {
		return true
	}

	return false
}
func WriteBinaryFile(name string, data []byte) bool {
	if ioutil.WriteFile(name, data, 0644) == nil {
		return true
	}

	return false
}

func IsExist(f string) bool {
	_, err := os.Stat(f)
	if err == nil {
		return true
	}
	result := os.IsExist(err)

	return result
}

func BytesToString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}

	return *(*string)(unsafe.Pointer(&sh))
}

// StringToBytes casts string to slice without copy
func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}

	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}

	return *(*[]byte)(unsafe.Pointer(&bh))
}
