package memkv

import (
	"github.com/xp/shorttext-db/memkv/proto"
)

type LocalDBProxy struct {
	db        MemDB
	sequence  uint64
	readCount uint64
}

func NewLocalDBProxy() *LocalDBProxy {
	var err error
	l := &LocalDBProxy{}
	l.db, err = Open(":memory:")
	if err != nil {
		panic(err)
	}
	l.db.SetId(0)

	return l
}

func (l *LocalDBProxy) NewIterator(key []byte) Iterator {
	l.readCount = l.readCount + 1
	//if l.readCount >400000{
	//
	//	fmt.Println("LocalDBProxy NewIterator---->",	l.readCount," ",string(key) )
	//}

	db := l.db

	start := mvccEncode(key, lockVer)
	stop := mvccEncode(key, 0)
	data := db.Scan(start, stop)
	iter := NewListIterator(data, false)
	return iter
}
func (l *LocalDBProxy) GetValues(key []byte) *proto.DbItems {
	db := l.db
	start := mvccEncode(key, lockVer)
	stop := mvccEncode(key, 0)
	data := db.Scan(start, stop)
	return data
}

func (l *LocalDBProxy) NewScanIterator(startKey []byte, endKey []byte, locked bool, desc bool) Iterator {
	db := l.db
	data := l.scan(db, startKey, endKey)
	iter := NewListIterator(data, false)
	return iter
}

func (l *LocalDBProxy) NewDescendIterator(startKey []byte, endKey []byte) Iterator {
	db := l.db
	data := l.scan(db, startKey, endKey)
	iter := NewListIterator(data, true)
	return iter
}

func (l *LocalDBProxy) Write(batch *Batch) error {
	panic("implement me")
}

func (l *LocalDBProxy) Close() error {
	return l.db.Close()
}

func (l *LocalDBProxy) Put(key []byte, val []byte, ts uint64, locked bool) (err error) {
	db := l.db
	item := &proto.DbItem{Key: key, Value: val}
	item.Key = mvccEncode(item.Key, ts)
	err = db.Put(item)
	//x :=l.generateId()
	//if l.sequence >=2000{
	//	fmt.Println("LocalDBProxy Put------------>",x)
	//}
	return err
}
func (l *LocalDBProxy) Get(key []byte, ts uint64) (val []byte, validated bool) {
	k := mvccEncode(key, ts)
	v := l.db.Get(k).Value
	return v, len(v) != 0
}

func (l *LocalDBProxy) Delete(key []byte, ts uint64, locked bool) (err error) {

	db := l.db
	k := mvccEncode(key, ts)
	return db.Delete(k)
}

func (l *LocalDBProxy) scan(db MemDB, startKey Key, endKey Key) *proto.DbItems {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, 0)
	}
	data := db.Scan(start, stop)
	//k := mvccEncode(startKey,10)
	//obj :=l.db.Get(k)
	//fmt.Println(obj)
	return data
}

func (l *LocalDBProxy) generateId() uint64 {
	//id := atomic.AddUint64(&l.sequence, 1)
	l.sequence = l.sequence + 1
	return l.sequence
}
