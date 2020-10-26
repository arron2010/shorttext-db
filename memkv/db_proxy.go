package memkv

import "github.com/xp/shorttext-db/memkv/proto"

type DBProxy struct {
	local  KVClient
	remote KVClient
	buffer KVClient
	Locks  map[uint64]int
}

func NewDBProxy() (KVClient, error) {
	instance := &DBProxy{}
	//	instance.remote = NewRemoteDBProxy(server.GetNodeProxy(), collaborator.GetCollaborator())
	instance.local = NewLocalDBProxy()
	instance.buffer = NewLocalDBProxy()
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

func (d *DBProxy) NewIterator(key []byte) Iterator {

	if len(key) == 0 {
		return NewEmptytIterator()
	}

	client := d.choose(key, false)
	iter := client.NewIterator(key)
	return iter
}
func (d *DBProxy) GetValues(key []byte) *proto.DbItems {
	if len(key) == 0 {
		return emptyItems
	}

	return d.local.GetValues(key)
}
func (d *DBProxy) NewScanIterator(startKey []byte, endKey []byte, locked bool, desc bool) Iterator {
	if len(startKey) == 0 {
		return NewEmptytIterator()
	}
	client := d.choose(startKey, locked)
	var iter Iterator
	if !desc {
		iter = client.NewScanIterator(startKey, endKey, locked, desc)
	} else {
		iter = client.NewDescendIterator(startKey, endKey)
	}
	return iter
}

func (d *DBProxy) NewDescendIterator(startKey []byte, endKey []byte) Iterator {
	if len(startKey) == 0 {
		return NewEmptytIterator()
	}
	client := d.choose(startKey, false)
	iter := client.NewDescendIterator(startKey, endKey)
	return iter
}

func (d *DBProxy) Write(batch *Batch) error {
	var err error
	//for _, added := range batch.addedBuf {
	//	err = d.Put(added.dbItem, added.ts)
	//	if err != nil {
	//		return err
	//	}
	//}
	//for _, deleted := range batch.deletedBuf {
	//	err = d.Delete(deleted.dbItem, deleted.ts)
	//	if err != nil {
	//		return err
	//	}
	//}
	return err
}

func (d *DBProxy) Put(key []byte, val []byte, ts uint64, locked bool) (err error) {

	client := d.choose(key, locked)
	if locked {
		d.Locks[ts] = 1 + d.Locks[ts]
		//if d.Locks[ts] > 1{
		//	xhelper.Print("DBProxy----------->",ts)
		//}
		ts = lockVer
	}
	err = client.Put(key, val, ts, locked)
	return err
}

func (d *DBProxy) Delete(key []byte, ts uint64, locked bool) (err error) {

	client := d.choose(key, locked)
	if locked {
		v, ok := d.Locks[ts]
		if !ok {
			d.Locks[ts] = 99
		} else {
			d.Locks[ts] = v - 1
		}
		ts = lockVer
	}
	//v1,_ := client.Get(key,ts)
	//fmt.Println(len(v1))
	err = client.Delete(key, ts, locked)
	//v2,_ := client.Get(key,ts)
	//fmt.Println(len(v2))
	return err
}

func (d *DBProxy) Get(key []byte, ts uint64) ([]byte, bool) {
	client := d.choose(key, d.isLocked(ts))

	return client.Get(key, ts)
}
func (d *DBProxy) isLocked(ts uint64) bool {
	if ts == lockVer {
		return true
	}
	return false

}
func (d *DBProxy) choose(key []byte, locked bool) KVClient {
	if locked {
		return d.buffer
	}
	return d.local
	//prefix := string(key[0])
	//if prefix == "m" {
	//	return d.local
	//} else {
	//	return d.remote
	//}
}
