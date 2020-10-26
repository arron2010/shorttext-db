package network

import (
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/cache"
	"time"
)

type AsynCache struct {
	inner  *cache.ExpireCache
	buffer chan *Message
}

func NewMessageCache() *AsynCache {
	m := &AsynCache{}
	inner := cache.NewExpireCache(mqTimeout / 2 * time.Second)
	m.inner = inner
	m.buffer = make(chan *Message)
	go m.run()
	return m
}

func (a *AsynCache) Put(value *Message) {
	select {
	case a.buffer <- value:
		logger.Debugf("消息推入通道[Term:%d From:%d To:%d]\n", value.Term, value.From, value.To)
	}
}

func (a *AsynCache) run() {
	for {
		select {
		case value := <-a.buffer:
			logger.Debugf("消息从通道获取[Term:%d From:%d To:%d]\n", value.Term, value.From, value.To)
			a.combine(value)
		}
	}
}
func (a *AsynCache) combine(value *Message) {
	var batchMessage *BatchMessage
	var ok bool
	batchMessage, ok = a.inner.Get(value.Term).(*BatchMessage)

	if batchMessage == nil {
		batchMessage = NewBatchMessage(value, int(value.Count))
		a.inner.Set(value.Term, batchMessage)
		return
	}
	if !ok {
		return
	}

	if uint32(len(batchMessage.Messages)) < value.Count {
		batchMessage.Messages = append(batchMessage.Messages, value)
	}
}

func (a *AsynCache) Get(key uint64, count int) (*BatchMessage, error) {

	var err error
	var ok bool
	var batchMessage *BatchMessage
	last := time.Now()

	for {
		elapsed := time.Since(last)
		if elapsed.Seconds() > mqTimeout {
			err = errors.New(fmt.Sprintf("获取消息超时[key:%d count:%d]", key, count))
			break
		}
		obj := a.inner.Get(key)
		if obj == nil {
			continue
		}
		batchMessage, ok = obj.(*BatchMessage)
		if !ok {
			err = errors.New(fmt.Sprintf("消息类型无法转换成*BatchMessage [term:%d]\n", key))
			break
		}
		logger.Debugf("消息实际数量:%d 预期数量:%d\n", len(batchMessage.Messages), count)
		if len(batchMessage.Messages) == count {
			break
		}
	}

	return batchMessage, err
}
