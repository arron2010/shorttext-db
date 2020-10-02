package memkv

type region struct {
	db          MemDB
	recordCount uint64
}
type Batch struct {
	addedBuf   []*DbItem
	deletedBuf []Key
}

func NewBatch() *Batch {

	batch := &Batch{}
	batch.addedBuf = make([]*DbItem, 0)
	batch.deletedBuf = make([]Key, 0)

	return batch
}
func (b *Batch) Put(key Key, val Value) {
	b.addedBuf = append(b.addedBuf, &DbItem{key: key, val: val})
}
func (b *Batch) Delete(key Key) {
	b.deletedBuf = append(b.deletedBuf, key)
}

type DBProxy struct {
	dbs      map[int]MemDB
	dbCount  int
	maxCount uint64
}

func NewDBProxy(dbCount int, maxCount uint64) (*DBProxy, error) {
	instance := &DBProxy{}
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
func (d *DBProxy) Put(item *DbItem) (err error) {
	db := d.dbs[1]
	err = db.Put(item)
	return err
}

func (d *DBProxy) Get(key Key) (val *DbItem) {
	db := d.dbs[1]
	val = db.Get(key)
	return val
}

func (d *DBProxy) Delete(key Key) (err error) {
	db := d.dbs[1]
	return db.Delete(key)
}

//func (d *DBProxy)Range(start,stop Key)[]*DbItem{
//	db := d.dbs[1]
//	result :=db.Range(start,stop)
//	return result
//}

func (d *DBProxy) Close() {
	db := d.dbs[1]
	db.Close()
}

func (d *DBProxy) NewIterator(start Key) Iterator {
	db := d.dbs[1]
	data := db.Find(start)
	iter := NewListIterator(data)
	return iter
}

func (d *DBProxy) NewScanIterator(startKey Key, endKey Key) Iterator {
	db := d.dbs[1]
	data := db.Scan(startKey, endKey)
	iter := NewListIterator(data)
	return iter
}

func (d *DBProxy) Write(batch *Batch) error {
	var err error
	for _, item := range batch.addedBuf {
		err = d.Put(item)
		if err != nil {
			return err
		}
	}
	for _, key := range batch.deletedBuf {
		err = d.Delete(key)
		if err != nil {
			return err
		}
	}
	return err
}
