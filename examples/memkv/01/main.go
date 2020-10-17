package main

import (
	"encoding/binary"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/memkv"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/utils"
	"strconv"
)

func encode(data uint32) []byte {
	buf := make([]byte, 4, 4)
	binary.BigEndian.PutUint32(buf, data)
	return buf
}

func decode(buf []byte) uint32 {
	if len(buf) == 0 && len(buf) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(buf)
}
func main() {
	config.LoadSettings("/opt/test/config/test_case1.txt", nil)
	server := memkv.NewGrpcProxyServer()
	server.Handler = func(proxy *memkv.RemoteDBProxy) {
		var batch *memkv.Batch
		for i := 1; i <= 10; i++ {
			batch = memkv.NewBatch()
			str := strconv.Itoa(i)
			batch.Put([]byte(str), []byte(str), uint64(i))
			if i == 19 {
				batch.Delete([]byte(str), uint64(i))
			}

			proxy.Write(batch)
		}

		for i := 1; i <= 10; i++ {
			str := strconv.Itoa(i)
			dbItem := &proto.DbItem{Key: []byte(str)}
			iter := proxy.NewIterator(dbItem.Key)
			val := string(iter.Value())
			fmt.Println(val)
		}

	}
	server.Start()
	utils.WaitFor()
}
