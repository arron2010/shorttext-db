package memkv

import (
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/server"
)

type DBProxy struct {
	local  KVClient
	remote KVClient
}

func NewDBProxy() (KVClient, error) {
	instance := &DBProxy{}
	instance.remote = NewRemoteDBProxy(server.GetNodeProxy(), collaborator.GetCollaborator())
	instance.local = NewLocalDBProxy()
	return instance, nil
}

func (d *DBProxy) Close() error {
	err := d.local.Close()
	err = d.remote.Close()
	return err
}

func (d *DBProxy) NewIterator(key Key) Iterator {
	client := d.choose(key)
	iter := client.NewIterator(key)
	return iter
}

func (d *DBProxy) NewScanIterator(startKey Key, endKey Key) Iterator {
	client := d.choose(startKey)
	iter := client.NewScanIterator(startKey, endKey)
	return iter
}

func (d *DBProxy) NewDescendIterator(startKey Key, endKey Key) Iterator {
	client := d.choose(startKey)
	iter := client.NewDescendIterator(startKey, endKey)
	return iter
}

func (d *DBProxy) Write(batch *Batch) error {
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

func (d *DBProxy) Put(item *proto.DbItem, ts uint64) (err error) {
	client := d.choose(item.Key)
	err = client.Put(item, ts)
	return err
}

func (d *DBProxy) Delete(item *proto.DbItem, ts uint64) (err error) {
	client := d.choose(item.Key)
	err = client.Delete(item, ts)
	return err
}

func (d *DBProxy) choose(key []byte) KVClient {
	prefix := string(key[0])
	if prefix == "m" {
		return d.local
	} else {
		return d.remote
	}
}
