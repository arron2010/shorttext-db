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
	inner := cache.NewExpireCache(time.Second * 10)
	m.inner = inner
	m.buffer = make(chan *Message)
	go m.run()
	return m
}

func NewBatchMessage(value *Message, cap int) *BatchMessage {
	var batchMessage *BatchMessage
	batchMessage = &BatchMessage{}
	batchMessage.Term = value.Term
	batchMessage.Messages = make([]*Message, 0, cap)
	batchMessage.Messages = append(batchMessage.Messages, value)

	return batchMessage
}

func (a *AsynCache) Put(value *Message) {
	select {
	case a.buffer <- value:
		logger.Infof("消息推入缓存[Term:%d From:%d To:%d]\n", value.Term, value.From, value.To)
	}
}

func (a *AsynCache) run() {
	for {
		select {
		case value := <-a.buffer:
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

func (a *AsynCache) getBatchMessage(key uint64, count int, value *Message) (*BatchMessage, error) {
	var batchMessage *BatchMessage
	var ok bool
	var obj interface{}

	batchMessage, ok = a.inner.Get(value.Term).(*BatchMessage)
	if batchMessage == nil || !ok {
		batchMessage = NewBatchMessage(value, count)
		a.inner.Set(value.Term, batchMessage)
	} else {
		if value.Term == key && count == len(batchMessage.Messages) {
			logger.Debugf("缓存已经存在此消息 [Term:%d Count:%d]\n", key, count)
			return batchMessage, nil
		}
		batchMessage.Messages = append(batchMessage.Messages, value)
	}
	obj = a.inner.Get(key)
	if obj == nil {
		logger.Errorf("未能从缓存获取消息对象[*BatchMessage] Value Term:%d, Key:%d\n", value.Term, key)
		return nil, errMessageNotFound
	}
	batchMessage, ok = obj.(*BatchMessage)
	if !ok {
		return nil, errors.New(fmt.Sprintf("消息类型无法转换成*BatchMessage Term:%d\n", key))
	}
	logger.Debugf("消息实际数量:%d 预期数量:%d\n", len(batchMessage.Messages), count)
	if ok && len(batchMessage.Messages) == count {
		return batchMessage, nil
	}

	return nil, errMessageCountNotEnough
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
