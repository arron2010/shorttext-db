package memkv

import (
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/memkv/proto"
)

type Key []byte
type Value []byte

const KEY_DEFAULT_VERSION = 1

//key的版本为0
const KEY_MIN_VERSION = 2

const MAX_RECORD_COUNT = 1024 * 1024 * 512

var logger = glogger.MustGetLogger("memkv")

type Batch struct {
	addedBuf   []batchItem
	deletedBuf []batchItem
}

type batchItem struct {
	dbItem *proto.DbItem
	ts     uint64
}

func NewBatch() *Batch {

	batch := &Batch{}
	batch.addedBuf = make([]batchItem, 0)
	batch.deletedBuf = make([]batchItem, 0)
	return batch
}
func (b *Batch) Put(key Key, val Value, ts uint64) {
	//writeKey := mvccEncode(key, ts)
	b.addedBuf = append(b.addedBuf, batchItem{dbItem: &proto.DbItem{Key: key, Value: val}, ts: ts})
}
func (b *Batch) Delete(key Key, ts uint64) {
	b.deletedBuf = append(b.deletedBuf, batchItem{dbItem: &proto.DbItem{Key: key, Value: nil}, ts: ts})
}

func NewDbItems() *proto.DbItems {
	instance := &proto.DbItems{}
	instance.Items = make([]*proto.DbItem, 0, 4)
	return instance
}

type DbItems []*DbItem

/*
数据接口
*/
type MemDB interface {
	Put(item *proto.DbItem) (err error)
	Get(key Key) (val *proto.DbItem)
	Delete(key Key) (err error)
	//NewIterator(start Key) (iter Iterator)
	//Find(key Key) *proto.DbItems
	Scan(startKey Key, endKey Key) *proto.DbItems
	RecordCount() int
	LoadDB() error
	PersistDB() error
	SetId(id uint32)
	//Range(start,stop Key)[]*DbItem
	Close() error
}

type KVClient interface {
	//Put(item *DbItem) (err error)
	//Get(key Key) (val *DbItem)
	NewIterator(start Key) Iterator
	NewScanIterator(startKey Key, endKey Key) Iterator
	NewDescendIterator(startKey Key, endKey Key) Iterator
	Write(batch *Batch) error
	Put(item *proto.DbItem, ts uint64) (err error)
	Delete(item *proto.DbItem, ts uint64) (err error)
	Close() error
}

/*
 迭代器接口
*/
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Prev() bool
}
