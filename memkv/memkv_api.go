package memkv

import (
	"bytes"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/memkv/proto"
)

type ValidateFunc func(val *DBItem) bool
type Key []byte
type Value []byte

func (k Key) HasPrefix(prefix Key) bool {
	return bytes.HasPrefix(k, prefix)
}

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
	dbItem *proto.DBItem
	ts     uint64
}

func NewBatch() *Batch {

	batch := &Batch{}
	batch.addedBuf = make([]batchItem, 0)
	batch.deletedBuf = make([]batchItem, 0)
	return batch
}
func (b *Batch) Put(key Key, val Value, ts uint64) {
	if len(key) == 0 {
		return
	}
	//writeKey := mvccEncode(key, ts)
	b.addedBuf = append(b.addedBuf, batchItem{dbItem: &proto.DBItem{Key: key, Value: val}, ts: ts})
}
func (b *Batch) Delete(key Key, ts uint64) {
	if len(key) == 0 {
		return
	}
	b.deletedBuf = append(b.deletedBuf, batchItem{dbItem: &proto.DBItem{Key: key, Value: nil}, ts: ts})
}

func NewDbItems() *proto.DBItems {
	instance := &proto.DBItems{}
	instance.Items = make([]*proto.DBItem, 0, 4)
	return instance
}

type DBItems []*DBItem

/*
数据接口
*/
type MemDB interface {
	Put(item *DBItem) (err error)
	Get(key Key) (val *DBItem)
	Delete(key Key) (err error)
	//NewIterator(start Key) (iter Iterator)
	//Find(key Key) *proto.DBItems
	Ascend(index string,
		iterator func(key Key, value *DBItem) bool) error
	CreateIndex(name, pattern string,
		less ...func(a, b *DBItem) bool) error
	AscendRange(index string, greaterOrEqual, lessThan *DBItem,
		iterator func(key Key, value *DBItem) bool) error
	DescendRange(index string, greaterOrEqual, lessThan *DBItem,
		iterator func(key Key, value *DBItem) bool) error
	AscendGreaterOrEqual(index string, pivot *DBItem,
		iterator func(key Key, value *DBItem) bool) error
	LoadDB() error
	PersistDB() error
	SetId(id uint32)
	//Range(start,stop Key)[]*DBItem
	Close() error
}

type KVClient interface {
	//Put(item *DBItem) (err error)
	//Get(key Key) (val *DBItem)
	FindByKey(finding Key, locked bool) []*DBItem
	Scan(startKey Key, endKey Key, ts uint64, limit int, desc bool, validate ValidateFunc) []*DBItem
	GetByRawKey(key []byte, ts uint64) (result *DBItem, validated bool)
	Put(item *DBItem) (err error)
	Get(key []byte, ts uint64) (item *DBItem, validated bool)
	Delete(key []byte, ts uint64) (err error)
	Close() error
	//GetValues(key []byte) *proto.DBItems
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
