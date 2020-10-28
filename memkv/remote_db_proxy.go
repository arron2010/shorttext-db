package memkv

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/server"
	"sync"
)

type RemoteDBProxy struct {
	clbt *collaborator.Collaborator
	n    *proxy.NodeProxy
	c    *chooser
}

var remoteOnce sync.Once
var remoteDBProxy *RemoteDBProxy

func GetRemoteDBProxy() *RemoteDBProxy {
	remoteOnce.Do(func() {
		remoteDBProxy = NewRemoteDBProxy(server.GetNodeProxy(), collaborator.GetCollaborator())
	})
	return remoteDBProxy
}
func NewRemoteDBProxy(n *proxy.NodeProxy, clbt *collaborator.Collaborator) *RemoteDBProxy {
	r := &RemoteDBProxy{}
	c := config.GetCase()
	r.n = n
	r.c = NewChooser()
	r.c.masterId = uint32(n.Id)
	r.clbt = clbt
	r.c.SetBuckets(c.GetCardList())
	r.c.n = n
	initialize(nil)

	return r
}

func (r *RemoteDBProxy) NewIterator(key []byte) Iterator {
	//var start, stop Key
	//if len(key) > 0{
	//	start = mvccEncode(key, lockVer)
	//	stop = mvccEncode(key, 0)
	//}
	//data := r.scan( start, stop)

	data, err := r.find(&proto.DBItem{Key: key, Value: key})
	if err != nil {
		logger.Errorf("RemoteDBProxy区间查询错误:%v", err)
	}
	iter := NewListIterator(data, true)
	//debug.PrintStack()

	return iter
}
func (r *RemoteDBProxy) GetValues(key []byte) *proto.DBItems {
	return nil
}
func (r *RemoteDBProxy) NewScanIterator(startKey []byte, endKey []byte, locked bool, desc bool) Iterator {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, lockVer)
	}
	data := r.Scan(start, stop)
	iter := NewListIterator(data, false)
	logger.Info("NewScanIterator多键升序查询:", startKey)

	return iter
}

func (r *RemoteDBProxy) NewDescendIterator(startKey []byte, endKey []byte) Iterator {
	var start, stop Key
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		stop = mvccEncode(endKey, lockVer)
	}
	data := r.Scan(start, stop)
	iter := NewListIterator(data, true)
	logger.Info("NewScanIterator多键降序查询:", startKey)
	return iter
}
func (r *RemoteDBProxy) Put(key []byte, val []byte, ts uint64, locked bool) (err error) {
	var to uint64
	var hash uint32
	item := &proto.DBItem{Key: key, Value: val}
	to, hash = r.c.Choose(item.Key, true)
	//logger.Infof("插入数据选择区域[%d %d]\n", to, hash)
	item.Key = mvccEncode(item.Key, ts)
	_, err = r.send(item, to, config.MSG_KV_SET)
	if err == nil {
		r.c.UpdateRegion(to, hash, 1)
	}
	return err
}
func (r *RemoteDBProxy) Delete(key []byte, ts uint64, locked bool) (err error) {
	item := &proto.DBItem{Key: key}
	to, hash := r.c.Choose(item.Key, false)
	item.Key = mvccEncode(item.Key, ts)
	_, err = r.send(item, to, config.MSG_KV_DEL)
	if err == nil {
		r.c.UpdateRegion(to, hash, -1)
	}
	return err
}
func (r *RemoteDBProxy) Get(key []byte, ts uint64) (val []byte, validated bool) {
	return nil, validated
}

func (r *RemoteDBProxy) find(item *proto.DBItem) (result *proto.DBItems, err error) {
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

func (r *RemoteDBProxy) send(item *proto.DBItem, to uint64, op uint32) (items *proto.DBItems, err error) {
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
	//var err error
	//for _, added := range batch.addedBuf {
	//	err = r.Put(added.dbItem, added.ts)
	//	if err != nil {
	//		return err
	//	}
	//}
	//for _, deleted := range batch.deletedBuf {
	//	err = r.Delete(deleted.dbItem, deleted.ts)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (r *RemoteDBProxy) Close() error {
	return r.c.Close()
}

func (r *RemoteDBProxy) Scan(startKey Key, endKey Key) *proto.DBItems {
	jobInfo := interfaces.NewSimpleJobInfo("MemKVJob", false,
		&proto.DBQueryParam{StartKey: startKey, EndKey: endKey})
	context := &task.TaskContext{}
	context.Context = make(map[string]interface{})
	result, err := r.clbt.MapReduce(jobInfo, context)
	if err != nil {
		logger.Errorf("RemoteDBProxy区间查询错误:%v", err)
		return NewDbItems()
	} else {
		return result.Content.(*proto.DBItems)
	}
}
