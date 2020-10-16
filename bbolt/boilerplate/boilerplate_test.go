package boilerplate

import (
	"fmt"
	"strconv"
	"testing"
)

func TestInitBolt(t *testing.T) {
	dbPath := `/opt/boltdb/test01.db`
	dbName := "test01"
	err := InitBolt(dbPath, []string{dbName})
	if err != nil {
		fmt.Println(err)
	}
	byteName := []byte(dbName)

	//val := "hello"
	const NUM = 10
	for i := 1; i <= NUM; i++ {
		key := strconv.Itoa(10000000 + i)
		Put(byteName, []byte(key), []byte(key))
	}
	key := []byte("10000001")
	result := Get(byteName, []byte(key))
	fmt.Println(string(result))
}
