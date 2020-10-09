package memkv

type Key []byte
type Value []byte

type Batch struct {
	addedBuf   []batchItem
	deletedBuf []batchItem
}

type batchItem struct {
	dbItem *DbItem
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
	b.addedBuf = append(b.addedBuf, batchItem{dbItem: &DbItem{key: key, val: val}, ts: ts})
}
func (b *Batch) Delete(key Key, ts uint64) {
	b.deletedBuf = append(b.deletedBuf, batchItem{dbItem: &DbItem{key: key, val: nil}, ts: ts})
}

/*
数据接口
*/
type MemDB interface {
	Put(item *DbItem) (err error)
	Get(key Key) (val *DbItem)
	Delete(key Key) (err error)
	//NewIterator(start Key) (iter Iterator)
	Find(key Key) []*DbItem
	Scan(startKey Key, endKey Key) []*DbItem
	RecordCount() int
	LoadDB() error
	PersistDB() error
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
