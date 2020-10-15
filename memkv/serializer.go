package memkv

import (
	proto2 "github.com/golang/protobuf/proto"
	"github.com/xp/shorttext-db/memkv/proto"
)

func marshalDbItem(dbItem *proto.DbItem) ([]byte, error) {
	return proto2.Marshal(dbItem)
}
func unmarshalDbItem(buff []byte, dbItem *proto.DbItem) error {
	return proto2.Unmarshal(buff, dbItem)
}

func unmarshalDbItems(buff []byte, dbItem *proto.DbItems) error {
	return proto2.Unmarshal(buff, dbItem)
}
func marshalDbItems(dbItems *proto.DbItems) ([]byte, error) {
	return proto2.Marshal(dbItems)
}
