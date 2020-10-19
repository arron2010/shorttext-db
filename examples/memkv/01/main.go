package main

import (
	"encoding/binary"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/memkv"
	"github.com/xp/shorttext-db/utils"
	"strconv"
	"strings"
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
		for i := 1; i <= 9; i++ {
			batch = memkv.NewBatch()
			str := strconv.Itoa(i)
			batch.Put([]byte(str), []byte(str), uint64(i))
			if i == 19 {
				batch.Delete([]byte(str), uint64(i))
			}

			proxy.Write(batch)
		}

		//for i := 1; i <= 10; i++ {
		//	str := strconv.Itoa(i)
		//	dbItem := &proto.DbItem{Key: []byte(str)}
		//	iter := proxy.NewIterator(dbItem.Key)
		//	val := string(iter.Value())
		//	fmt.Println("单个取数:",val)
		//}

		start := memkv.MvccEncode([]byte("1"), uint64(1))
		stop := memkv.MvccEncode([]byte("9"), uint64(9))
		iter := proxy.Scan(start, stop)
		msg := make([]string, 0, 0)
		for _, item := range iter.Items {
			msg = append(msg, string(item.Key))
		}
		fmt.Println("集合:", strings.Join(msg, ","))

	}
	server.Start()
	utils.WaitFor()
}
