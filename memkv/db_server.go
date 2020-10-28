package memkv

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/server"
)

type MemDBServer struct {
	node *server.Node

	Id int
	db MemDB
}

func NewDBServer(node *server.Node) *MemDBServer {
	var err error
	server := &MemDBServer{}
	c := config.GetCase()
	id := int(c.Local.ID)
	server.db, err = Open(":memory:")
	if err != nil {
		panic(err)
	}
	server.db.SetId(uint32(id))
	server.Id = id
	node.RegisterHandler(server)
	server.node = node
	initialize(server.db)
	return server
}

func (s *MemDBServer) Handle(msgType uint32, data []byte) ([]byte, bool, error) {
	var err error
	var resp []byte

	switch msgType {
	case config.MSG_KV_SET:
		dbItem := &proto.DBItem{}
		err = unmarshalDbItem(data, dbItem)
		if err != nil {
			return nil, true, err
		}
		s.debugOp("Put", dbItem)

		err = s.db.Put(createDBItem(dbItem))
		return nil, true, err

	case config.MSG_KV_FIND:
		dbItem := &proto.DBItem{}
		err = unmarshalDbItem(data, dbItem)
		if err != nil {
			return nil, true, err
		}
		//s.debugOp("Find", dbItem)
		items := []*DBItem{} //s.db.Scan(dbItem.Key, dbItem.Value)
		protoItems := createProtoDBItems(items)
		resp, err = marshalDbItems(protoItems)
		return resp, true, err

	case config.MSG_KV_DEL:
		dbItem := &proto.DBItem{}
		err = unmarshalDbItem(data, dbItem)
		if err != nil {
			return nil, true, err
		}
		//s.debugOp("Del", dbItem)
		err = s.db.Delete(dbItem.Key)
		return nil, true, err
	}

	return resp, false, err
}

func (s *MemDBServer) ReportUnreachable(id uint64) {

}
func (s *MemDBServer) debugOp(op string, dbItem *proto.DBItem) {
	key, ts, err := mvccDecode(dbItem.Key)
	if err == nil {
		logger.Infof("%s DbItem Key[%v] Ts[%d]\n", op, key, ts)
	}
}
