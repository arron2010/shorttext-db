package memkv

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/errors"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/network"
)

type DBServer struct {
	node  *network.StreamServer
	peers []string
	Id    int
	db    MemDB
}

func NewDBServer() *DBServer {
	var err error
	server := &DBServer{}
	c := config.GetCase()
	id := int(c.Local.ID)
	peers := c.GetUrls()
	server.node, err = network.NewStreamServer(id, server, peers...)
	if err != nil {
		panic(err)
	}
	server.db, err = Open(":memory:")
	if err != nil {
		panic(err)
	}
	server.db.SetId(uint32(id))
	server.Id = id
	server.peers = peers

	return server
}

func (s *DBServer) Start() {
	s.node.Start()
}

func (s *DBServer) Process(ctx context.Context, m network.Message) error {
	var err error
	result := network.Message{}
	result.To = m.From
	result.From = m.To
	result.Count = m.Count
	result.Term = m.Term
	result.ResultCode = config.MSG_KV_RESULT_SUCCESS
	result.Index = m.Index
	dbItem := &proto.DbItem{}
	logger.Infof("收到消息 From:%d To:%d Term:%d\n", m.From, m.To, m.Term)
	err = unmarshalDbItem(m.Data, dbItem)
	if err != nil {
		return err
	}
	switch m.Type {
	case config.MSG_KV_SET:
		s.debugOp("Put", dbItem)
		err = s.db.Put(dbItem)
		logger.Infof("数据库[%d]更新数据 消息序号[%d]\n", s.Id, result.Term)
	case config.MSG_KV_FIND:
		s.debugOp("Find", dbItem)
		items := s.db.Scan(dbItem.Key, dbItem.Value)
		result.Data, err = marshalDbItems(items)
	case config.MSG_KV_DEL:
		s.debugOp("Del", dbItem)
		err = s.db.Delete(dbItem.Key)
	default:
		err = errors.New(fmt.Sprintf("数据库[%d]不支持该操作[%d]", s.Id, m.Type))
	}
	logger.Infof("回复消息 From:%d To:%d Term:%d\n", result.From, result.To, result.Term)
	s.node.Send(result)
	return err
}

func (s *DBServer) ReportUnreachable(id uint64) {

}
func (s *DBServer) debugOp(op string, dbItem *proto.DbItem) {
	key, ts, err := mvccDecode(dbItem.Key)
	if err == nil {
		logger.Infof("%s DbItem Key[%s] Ts[%d]\n", op, string(key), ts)
	}
}
