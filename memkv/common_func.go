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
var emptyItems *proto.DBItems

func initialize(db MemDB) {
	once.Do(func() {
		emptyItems = &proto.DBItems{Items: make([]*proto.DBItem, 0, 0)}
		//加载序列化与反序列化处理器
		store.GetInstance().MessageEncoder = &task.MessageEncoder{NewMsgSerializer()}

		easymr.Set(constants.JOB_HANDLER, NewMemKVJob(), "MemKVJob")
		easymr.Set(constants.MAPPER, NewMemKVMapper(), "MemKVMapper")
		easymr.Set(constants.CONSUMER, NewMemKVConsumer(db), "MemKVConsumer")
		easymr.Set(constants.REDUCER, NewMemKVReducer(), "MemKVReducer")
	})

}

func createDBItem(dbItem *proto.DBItem) *DBItem {
	return &DBItem{Key: dbItem.Key,
		Val:      dbItem.Value,
		StartTS:  dbItem.StartTS,
		CommitTS: dbItem.CommitTS}
}

func createProtoDBItem(dbItem *DBItem) *proto.DBItem {
	return &proto.DBItem{Key: dbItem.Key,
		Value:    dbItem.Val,
		StartTS:  dbItem.StartTS,
		CommitTS: dbItem.CommitTS}
}
func createProtoDBItems(items []*DBItem) *proto.DBItems {
	protoItems := &proto.DBItems{}
	protoItems.Items = make([]*proto.DBItem, 0, len(items))
	l := len(items)
	for i := 0; i < l; i++ {
		protoItems.Items = append(protoItems.Items, createProtoDBItem(items[i]))
	}
	return protoItems
}
