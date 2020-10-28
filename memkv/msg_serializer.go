package memkv

import (
	proto2 "github.com/golang/protobuf/proto"
	"github.com/xp/shorttext-db/memkv/proto"
)

type MsgSerializer struct {
}

func NewMsgSerializer() *MsgSerializer {
	m := &MsgSerializer{}
	return m
}
func (m *MsgSerializer) Serialize(source interface{}) ([]byte, error) {
	var buf []byte
	var err error
	switch source.(type) {
	case *proto.DBQueryParam:
		obj := source.(*proto.DBQueryParam)
		buf, err = proto2.Marshal(obj)
	case *proto.DBItems:
		obj := source.(*proto.DBItems)
		buf, err = proto2.Marshal(obj)
	}
	return buf, err
}

func (m *MsgSerializer) Deserialize(typeName string, payload []byte) (interface{}, error) {
	var err error
	switch typeName {
	case "*proto.DbQueryParam":
		obj := &proto.DBQueryParam{}
		err = proto2.Unmarshal(payload, obj)
		return obj, err
	case "*proto.DbItems":
		obj := &proto.DBItems{}
		err = proto2.Unmarshal(payload, obj)
		return obj, err
	}
	return nil, nil
}
