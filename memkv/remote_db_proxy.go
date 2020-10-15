package memkv

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/errors"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"sync/atomic"
)

type RemoteDBProxy struct {
	clbt     *collaborator.Collaborator
	n        *proxy.NodeProxy
	c        *chooser
	sequence uint64
}

func NewRemoteDBProxy() *RemoteDBProxy {
	r := &RemoteDBProxy{}
	c := config.GetCase()
	n := proxy.NewNodeProxy(c.GetUrls(), config.GetConfig().LogLevel)
	r.n = n
	r.c = NewChooser()
	r.c.masterId = uint32(n.Id)
	r.c.SetBuckets(c.GetCardList())
	r.sequence = 0
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
	data := r.scan(start, stop)
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
	data := r.scan(start, stop)
	iter := NewListIterator(data, true)
	return iter
}
func (r *RemoteDBProxy) put(item *proto.DbItem, ts uint64) (err error) {
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
func (r *RemoteDBProxy) delete(item *proto.DbItem, ts uint64) (err error) {
	to, hash := r.c.Choose(item.Key, false)
	item.Key = mvccEncode(item.Key, ts)
	_, err = r.send(item, to, config.MSG_KV_DEL)
	if err != nil {
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
	result, err = r.send(item, to, config.MSG_KV_FIND)
	if result == nil {
		result = NewDbItems()
	}
	return result, err
}

func (r *RemoteDBProxy) send(item *proto.DbItem, to uint64, op uint32) (items *proto.DbItems, err error) {

	var result *network.BatchMessage
	var term uint64
	term, err = r.generateId()
	var msg *network.Message
	msg = &network.Message{}
	msg.Term = term
	msg.Count = 1
	msg.Type = op
	msg.Data, err = marshalDbItem(item)
	if err != nil {
		return nil, err
	}
	msg.From = uint64(r.n.Id)
	msg.To = to
	input := &network.BatchMessage{}
	input.Term = msg.Term
	input.Messages = []*network.Message{msg}
	logger.Infof("发送消息 From:%d To:%d Term:%d\n", msg.From, msg.To, msg.Term)
	result, err = r.n.Send(input)
	if len(result.Messages) > 0 {
		if result.Messages[0].ResultCode != config.MSG_KV_RESULT_SUCCESS {
			return nil, errors.New(fmt.Sprintf("数据库服务器返回错误:%s op:%d", result.Messages[0].Text, op))
		} else {
			if op == config.MSG_KV_FIND {
				items = NewDbItems()
				err = unmarshalDbItems(result.Messages[0].Data, items)
				return items, err
			}
		}
	} else {
		return nil, errors.New(fmt.Sprintf("远程操作数据库失败 op:%d", op))
	}
	return nil, err
}

func (r *RemoteDBProxy) generateId() (uint64, error) {

	//node, err := utils.NewNode(0)
	//if err != nil {
	//	return 0, err
	//}
	id := atomic.AddUint64(&r.sequence, 1)
	return id, nil
}

func (r *RemoteDBProxy) Write(batch *Batch) error {
	var err error
	for _, added := range batch.addedBuf {
		err = r.put(added.dbItem, added.ts)
		if err != nil {
			return err
		}
	}
	for _, deleted := range batch.deletedBuf {
		err = r.delete(deleted.dbItem, deleted.ts)
		if err != nil {
			return err
		}
	}
	return err
}

func (r *RemoteDBProxy) scan(startKey Key, endKey Key) *proto.DbItems {
	jobInfo := interfaces.NewSimpleJobInfo("ScanDBHandler", false,
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
