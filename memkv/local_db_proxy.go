package memkv

import "github.com/xp/shorttext-db/memkv/proto"

type LocalDBProxy struct {
	db MemDB
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

func (l *LocalDBProxy) NewIterator(key Key) Iterator {
	db := l.db

	start := mvccEncode(key, lockVer)
	stop := mvccEncode(key, 0)
	data := db.Scan(start, stop)
	iter := NewListIterator(data, false)
	return iter
}

func (l *LocalDBProxy) NewScanIterator(startKey Key, endKey Key) Iterator {
	db := l.db
	data := l.scan(db, startKey, endKey)
	iter := NewListIterator(data, false)
	return iter
}

func (l *LocalDBProxy) NewDescendIterator(startKey Key, endKey Key) Iterator {
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

func (l *LocalDBProxy) Put(item *proto.DbItem, ts uint64) (err error) {
	db := l.db
	item.Key = mvccEncode(item.Key, ts)
	err = db.Put(item)
	return err
}
func (l *LocalDBProxy) Delete(item *proto.DbItem, ts uint64) (err error) {
	db := l.db
	item.Key = mvccEncode(item.Key, ts)
	return db.Delete(item.Key)
}

func (l *LocalDBProxy) scan(db MemDB, startKey Key, endKey Key) *proto.DbItems {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, lockVer)
	}
	data := db.Scan(start, stop)
	return data
}
