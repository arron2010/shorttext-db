package memkv

import "github.com/xp/shorttext-db/memkv/proto"

type mockProxy struct {
	dbs      map[int]MemDB
	dbCount  int
	maxCount uint64
}

func newMockProxy(dbCount int, maxCount uint64) (KVClient, error) {
	instance := &mockProxy{}
	instance.dbs = make(map[int]MemDB)
	var err error
	var db MemDB
	instance.dbCount = dbCount
	instance.maxCount = maxCount

	for i := 1; i <= dbCount; i++ {
		db, err = Open(":memory:")
		if err != nil {
			return nil, err
		}
		instance.dbs[i] = db
	}
	return instance, err
}
func (d *mockProxy) Put(item *proto.DbItem, ts uint64) (err error) {
	db := d.dbs[1]
	item.Key = mvccEncode(item.Key, ts)
	err = db.Put(item)
	return err
}

func (d *mockProxy) Delete(item *proto.DbItem, ts uint64) (err error) {
	db := d.dbs[1]
	item.Key = mvccEncode(item.Key, ts)
	return db.Delete(item.Key)
}

func (d *mockProxy) get(key Key) (val *proto.DbItem) {
	db := d.dbs[1]
	val = db.Get(key)
	return val
}

//func (d *DBProxy)Range(start,stop Key)[]*DbItem{
//	db := d.dbs[1]
//	result :=db.Range(start,stop)
//	return result
//}

func (d *mockProxy) Close() error {
	db := d.dbs[1]
	return db.Close()
}

func (d *mockProxy) NewIterator(key Key) Iterator {
	db := d.dbs[1]

	start := mvccEncode(key, lockVer)
	stop := mvccEncode(key, 0)

	data := db.Scan(start, stop)
	iter := NewListIterator(data, false)
	return iter
}

func (d *mockProxy) scan(db MemDB, startKey Key, endKey Key) *proto.DbItems {
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

func (d *mockProxy) NewScanIterator(startKey Key, endKey Key) Iterator {
	db := d.dbs[1]
	data := d.scan(db, startKey, endKey)
	iter := NewListIterator(data, false)
	return iter
}

func (d *mockProxy) NewDescendIterator(startKey Key, endKey Key) Iterator {
	db := d.dbs[1]
	data := d.scan(db, startKey, endKey)
	iter := NewListIterator(data, true)
	return iter
}

func (d *mockProxy) Write(batch *Batch) error {
	var err error
	for _, added := range batch.addedBuf {
		err = d.Put(added.dbItem, added.ts)
		if err != nil {
			return err
		}
	}
	for _, deleted := range batch.deletedBuf {
		err = d.Delete(deleted.dbItem, deleted.ts)
		if err != nil {
			return err
		}
	}
	return err
}
