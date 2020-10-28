package memkv

import (
	proto2 "github.com/golang/protobuf/proto"
	"github.com/xp/shorttext-db/memkv/proto"
)

func marshalDbItem(dbItem *proto.DBItem) ([]byte, error) {
	return proto2.Marshal(dbItem)
}
func unmarshalDbItem(buff []byte, dbItem *proto.DBItem) error {
	return proto2.Unmarshal(buff, dbItem)
}

func unmarshalDbItems(buff []byte, dbItem *proto.DBItems) error {
	return proto2.Unmarshal(buff, dbItem)
}
func marshalDbItems(dbItems *proto.DBItems) ([]byte, error) {
	return proto2.Marshal(dbItems)
}
