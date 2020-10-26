package memkv

import (
	"github.com/xp/shorttext-db/easymr"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/easymr/store"
	"github.com/xp/shorttext-db/memkv/proto"
	"sync"
)

var once sync.Once
var emptyItems *proto.DbItems

func initialize(db MemDB) {
	once.Do(func() {
		emptyItems = &proto.DbItems{Items: make([]*proto.DbItem, 0, 0)}
		//加载序列化与反序列化处理器
		store.GetInstance().MessageEncoder = &task.MessageEncoder{NewMsgSerializer()}

		easymr.Set(constants.JOB_HANDLER, NewMemKVJob(), "MemKVJob")
		easymr.Set(constants.MAPPER, NewMemKVMapper(), "MemKVMapper")
		easymr.Set(constants.CONSUMER, NewMemKVConsumer(db), "MemKVConsumer")
		easymr.Set(constants.REDUCER, NewMemKVReducer(), "MemKVReducer")
	})

}
