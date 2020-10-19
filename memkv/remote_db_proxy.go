package memkv

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/network/proxy"
	"sync"
)

type RemoteDBProxy struct {
	clbt *collaborator.Collaborator
	n    *proxy.NodeProxy
	c    *chooser
}

var once sync.Once

func NewRemoteDBProxy(n *proxy.NodeProxy, clbt *collaborator.Collaborator) *RemoteDBProxy {
	r := &RemoteDBProxy{}
	c := config.GetCase()
	r.n = n
	r.c = NewChooser()
	r.c.masterId = uint32(n.Id)
	r.clbt = clbt
	r.c.SetBuckets(c.GetCardList())
	r.c.n = n
	initializeMRConfig(nil)

	return r
}

func (r *RemoteDBProxy) NewIterator(key Key) Iterator {
	//var start, stop Key
	//if len(key) > 0{
	//	start = mvccEncode(key, lockVer)
	//	stop = mvccEncode(key, 0)
	//}
	//data := r.scan( start, stop)

	data, err := r.find(&proto.DbItem{Key: key, Value: key})
	if err != nil {
		logger.Errorf("RemoteDBProxy区间查询错误:%v", err)
	}
	iter := NewListIterator(data, true)
	return iter
}

func (r *RemoteDBProxy) NewScanIterator(startKey Key, endKey Key) Iterator {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, lockVer)
	}
	data := r.Scan(start, stop)
	iter := NewListIterator(data, false)
	return iter
}

func (r *RemoteDBProxy) NewDescendIterator(startKey Key, endKey Key) Iterator {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, lockVer)
	}
	data := r.Scan(start, stop)
	iter := NewListIterator(data, true)
	return iter
}
func (r *RemoteDBProxy) Put(item *proto.DbItem, ts uint64) (err error) {
	var to uint64
	var hash uint32
	//var force bool = false
	//if ts == lockVer {
	//	force = true
	//}
	to, hash = r.c.Choose(item.Key, true)
	logger.Infof("插入数据选择区域[%d %d]\n", to, hash)
	item.Key = mvccEncode(item.Key, ts)
	_, err = r.send(item, to, config.MSG_KV_SET)
	if err == nil {
		r.c.UpdateRegion(to, hash, 1)
	}
	return err
}
func (r *RemoteDBProxy) Delete(item *proto.DbItem, ts uint64) (err error) {
	to, hash := r.c.Choose(item.Key, false)
	item.Key = mvccEncode(item.Key, ts)
	_, err = r.send(item, to, config.MSG_KV_DEL)
	if err == nil {
		r.c.UpdateRegion(to, hash, -1)
	}
	return err
}

func (r *RemoteDBProxy) find(item *proto.DbItem) (result *proto.DbItems, err error) {
	to, _ := r.c.Choose(item.Key, false)
	if len(item.Key) > 0 {
		item.Key = mvccEncode(item.Key, lockVer)
		item.Value = mvccEncode(item.Value, 0)
	}
	if to == 0 {

		return NewDbItems(), nil
	}

	result, err = r.send(item, to, config.MSG_KV_FIND)
	if result == nil {
		result = NewDbItems()
	}
	return result, err
}

func (r *RemoteDBProxy) send(item *proto.DbItem, to uint64, op uint32) (items *proto.DbItems, err error) {
	var req, resp []byte
	req, err = marshalDbItem(item)
	if err != nil {
		return nil, err
	}

	resp, err = r.n.SendSingleMsg(to, op, req)
	if op == config.MSG_KV_FIND {
		items = NewDbItems()
		err = unmarshalDbItems(resp, items)
		return items, err
	} else {
		return nil, err
	}
}

func (r *RemoteDBProxy) Write(batch *Batch) error {
	var err error
	for _, added := range batch.addedBuf {
		err = r.Put(added.dbItem, added.ts)
		if err != nil {
			return err
		}
	}
	for _, deleted := range batch.deletedBuf {
		err = r.Delete(deleted.dbItem, deleted.ts)
		if err != nil {
			return err
		}
	}
	return err
}

func (r *RemoteDBProxy) Close() error {
	return r.c.Close()
}

func (r *RemoteDBProxy) Scan(startKey Key, endKey Key) *proto.DbItems {
	jobInfo := interfaces.NewSimpleJobInfo("MemKVJob", false,
		&proto.DbQueryParam{StartKey: startKey, EndKey: endKey})
	context := &task.TaskContext{}
	context.Context = make(map[string]interface{})
	result, err := r.clbt.MapReduce(jobInfo, context)
	if err != nil {
		logger.Errorf("RemoteDBProxy区间查询错误:%v", err)
		return NewDbItems()
	} else {
		return result.Content.(*proto.DbItems)
	}
}
