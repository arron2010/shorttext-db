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
	case *proto.DbQueryParam:
		obj := source.(*proto.DbQueryParam)
		buf, err = proto2.Marshal(obj)
	case *proto.DbItems:
		obj := source.(*proto.DbItems)
		buf, err = proto2.Marshal(obj)
	}
	return buf, err
}

func (m *MsgSerializer) Deserialize(typeName string, payload []byte) (interface{}, error) {
	var err error
	switch typeName {
	case "*proto.DbQueryParam":
		obj := &proto.DbQueryParam{}
		err = proto2.Unmarshal(payload, obj)
		return obj, err
	case "*proto.DbItems":
		obj := &proto.DbItems{}
		err = proto2.Unmarshal(payload, obj)
		return obj, err
	}
	return nil, nil
}
