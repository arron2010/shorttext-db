package memkv

type Key []byte
type Value []byte

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

/*
 迭代器接口
*/
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
}
