package memkv

type DBProxy struct {
	local  KVClient
	remote KVClient
	buffer KVClient
	Locks  map[uint64]int
}

func NewDBProxy() (KVClient, error) {
	instance := &DBProxy{}
	//	instance.remote = NewRemoteDBProxy(server.GetNodeProxy(), collaborator.GetCollaborator())
	instance.local = NewLocalDBProxy(1)
	instance.buffer = NewLocalDBProxy(99)
	instance.Locks = make(map[uint64]int)
	return instance, nil
}
func (d *DBProxy) GetBuffer() KVClient {
	return d.buffer
}
func (d *DBProxy) Close() error {
	err := d.local.Close()
	err = d.remote.Close()
	return err
}

func (d *DBProxy) GetByRawKey(key []byte, ts uint64) (result *DBItem, validated bool) {
	db := d.choose(key, ts)
	return db.GetByRawKey(key, ts)
}
func (d *DBProxy) Scan(startKey Key, endKey Key, ts uint64, limit int, desc bool, validate ValidateFunc) []*DBItem {
	db := d.choose(startKey, ts)
	return db.Scan(startKey, endKey, ts, limit, desc, validate)
}

func (d *DBProxy) FindByKey(finding Key, locked bool) []*DBItem {
	db := d.local
	if locked {
		db = d.buffer
	}
	return db.FindByKey(finding, locked)
}

func (d *DBProxy) Put(item *DBItem) (err error) {
	if len(item.RawKey) == 0 {
		return nil
	}
	db := d.choose(item.RawKey, item.CommitTS)
	//client := d.choose(key, locked)
	//if locked {
	//	d.Locks[ts] = 1 + d.Locks[ts]
	//	//if d.Locks[ts] > 1{
	//	//	xhelper.Print("DBProxy----------->",ts)
	//	//}
	//	ts = lockVer
	//}
	err = db.Put(item)
	return err
}

func (d *DBProxy) Delete(key []byte, ts uint64) (err error) {

	client := d.choose(key, ts)

	//v1,_ := client.Get(key,ts)
	//fmt.Println(len(v1))
	err = client.Delete(key, ts)
	//v2,_ := client.Get(key,ts)
	//fmt.Println(len(v2))
	return err
}

func (d *DBProxy) Get(key []byte, ts uint64) (*DBItem, bool) {
	client := d.choose(key, ts)

	return client.Get(key, ts)
}

//func (d *DBProxy) isLocked(ts uint64) bool {
//	if ts == lockVer {
//		return true
//	}
//	return false
//
//}
func (d *DBProxy) choose(key []byte, ts uint64) KVClient {
	if ts == lockVer {
		return d.buffer
	} else {
		return d.local
	}
}

//func (d *DBProxy) NewIterator(key []byte) Iterator {
//
//	if len(key) == 0 {
//		return NewEmptytIterator()
//	}
//
//	client := d.choose(key, false)
//	iter := client.NewIterator(key)
//	return iter
//}
//func (d *DBProxy) GetValues(key []byte) *proto.DBItems {
//	if len(key) == 0 {
//		return emptyItems
//	}
//
//	return d.local.GetValues(key)
//}
//func (d *DBProxy) NewScanIterator(startKey []byte, endKey []byte, locked bool, desc bool) Iterator {
//	if len(startKey) == 0 {
//		return NewEmptytIterator()
//	}
//	client := d.choose(startKey, locked)
//	var iter Iterator
//	if !desc {
//		iter = client.NewScanIterator(startKey, endKey, locked, desc)
//	} else {
//		iter = client.NewDescendIterator(startKey, endKey)
//	}
//	return iter
//}
//
//func (d *DBProxy) NewDescendIterator(startKey []byte, endKey []byte) Iterator {
//	if len(startKey) == 0 {
//		return NewEmptytIterator()
//	}
//	client := d.choose(startKey, false)
//	iter := client.NewDescendIterator(startKey, endKey)
//	return iter
//}
